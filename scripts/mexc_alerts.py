"""mexc_alerts.py — per-minute MEXC bot health monitor with Telegram alerts.

Watches both buckets for adverse events and pings Telegram when thresholds breach.

Alerts:
1. Big SELL loss     — any SELL trade whose realized_delta < -$BIG_SELL_LOSS_USDC.
2. Realized PnL drop — cumulative realized_pnl dropped > $REALIZED_DROP_HOUR_USDC in last 1h.
3. Insufficient cash storm — > INSUF_CASH_WARN_THRESHOLD warnings in last INSUF_CASH_WINDOW_SEC seconds.
4. Drift event      — reference_price dropped > DRIFT_PCT_THRESHOLD% between checks (safety net).

Per-alert cooldowns prevent flood spamming.

Usage:
    # Run once
    env/bin/python scripts/mexc_alerts.py

    # Cron (every minute):
    # * * * * * cd /root/trading_bot && env/bin/python scripts/mexc_alerts.py >> logs/mexc_alerts.log 2>&1
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request
from glob import glob
from typing import Any

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STATE_DIR = os.path.join(REPO_ROOT, "strategies", "pct_ladder", "state")
LOG_DIR   = os.path.join(REPO_ROOT, "logs")
# SHARED telegram.json has 2 recipients: owner (…2650) + …3258.
# RECIPIENT POLICY: …3258 gets LIVE CRYPTO/MEXC messages only. NON-crypto senders (equity, BTST)
# AND paper/backtest crypto senders (fib paper bot, mexc_backtest_compare) must use a dedicated
# owner-only secrets file — NOT this shared one — so …3258 only ever sees LIVE crypto, unless the
# owner explicitly asks otherwise. (This mexc_alerts is live MEXC health → shared file is correct.)
SECRETS   = os.path.join(REPO_ROOT, "strategies", "pct_ladder", "secrets", "telegram.json")

BUCKETS = {
    "bucket1": {
        "state_dir":  os.path.join(STATE_DIR, "bucket1"),
        "log":        os.path.join(LOG_DIR, "mexc_bucket1_runner.log"),
        "label":      "Bucket1",
    },
    "bucket2": {
        "state_dir":  os.path.join(STATE_DIR, "bucket2"),
        "log":        os.path.join(LOG_DIR, "mexc_bucket2_runner.log"),
        "label":      "Bucket2",
    },
    "bucket3": {
        "state_dir":  os.path.join(STATE_DIR, "bucket3"),
        "log":        os.path.join(LOG_DIR, "mexc_bucket3_runner.log"),
        "label":      "Bucket3",
    },
}

# Thresholds (tunable)
BIG_SELL_LOSS_USDC          = 200.0   # any SELL with realized_delta worse than this triggers
REALIZED_DROP_HOUR_USDC     = 500.0   # cumulative realized drop > $500 in 1 hour
INSUF_CASH_WARN_THRESHOLD   = 100     # warnings count in window
INSUF_CASH_WINDOW_SEC       = 600     # 10 min
DRIFT_PCT_THRESHOLD         = 2.0     # ref price moved by 2% (downward) between checks

# Cooldowns (seconds) — don't re-alert within this window
COOLDOWN_SEC = {
    "realized_drop":    3600,   # 1 hour
    "insufficient_cash": 3600,  # 1 hour
    "drift_event":      300,    # 5 min
    # big_sell_loss: no cooldown — every offending trade alerts once via trade-id tracking
}


def _send_telegram(text: str) -> None:
    try:
        with open(SECRETS) as f:
            secrets = json.load(f)
    except FileNotFoundError:
        print(f"[alerts] telegram secrets missing: {SECRETS}", file=sys.stderr)
        return
    token = secrets.get("bot_token")
    chat_ids = secrets.get("chat_id")
    if isinstance(chat_ids, str):
        chat_ids = [chat_ids]
    if not token or not chat_ids:
        return
    for cid in chat_ids:
        data = urllib.parse.urlencode({
            "chat_id": cid,
            "text": text,
            # Plain text — alerts often contain $, -, _ that trip Markdown's parser.
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as e:
            print(f"[alerts] telegram send failed: {e}", file=sys.stderr)


def _load_state(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, path)


def _newest(pattern: str) -> str | None:
    files = [p for p in glob(pattern) if os.path.isfile(p)]
    return max(files, key=os.path.getmtime) if files else None


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_ts(s: str) -> dt.datetime | None:
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d
    except Exception:
        return None


def _on_cooldown(state: dict, key: str, now: dt.datetime) -> bool:
    last_ts = state.get("last_alert_ts", {}).get(key)
    if not last_ts:
        return False
    last = _parse_ts(last_ts)
    if not last:
        return False
    cd = COOLDOWN_SEC.get(key, 3600)
    return (now - last).total_seconds() < cd


def _mark_alert(state: dict, key: str, now: dt.datetime) -> None:
    state.setdefault("last_alert_ts", {})[key] = now.isoformat()


def _read_trades_since(trades_path: str, last_seen_oid: str | None) -> list[dict]:
    """Return new FILL trades since the last seen order_id."""
    if not trades_path or not os.path.exists(trades_path):
        return []
    rows: list[dict] = []
    started = (last_seen_oid is None)
    try:
        with open(trades_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("event") != "FILL":
                    continue
                if not started:
                    if r.get("order_id") == last_seen_oid:
                        started = True
                    continue
                rows.append(r)
    except Exception:
        return []
    return rows


def _count_insufficient_cash_warnings(log_path: str, window_sec: int, now: dt.datetime) -> int:
    """Count 'insufficient cash' warnings in the last window_sec by reading the tail of the log."""
    if not os.path.exists(log_path):
        return 0
    cutoff = now - dt.timedelta(seconds=window_sec)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    count = 0
    try:
        # Tail approach: read last ~5MB only
        size = os.path.getsize(log_path)
        offset = max(0, size - 5 * 1024 * 1024)
        with open(log_path, "rb") as f:
            f.seek(offset)
            if offset > 0:
                f.readline()  # discard partial
            for raw in f:
                try:
                    line = raw.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                if "insufficient cash" not in line:
                    continue
                # Line starts with "YYYY-MM-DD HH:MM:SS ..."
                if len(line) < 19:
                    continue
                if line[:19] >= cutoff_str:
                    count += 1
    except Exception:
        pass
    return count


def _check_bucket(name: str, paths: dict, now: dt.datetime) -> None:
    label    = paths["label"]
    state_dir = paths["state_dir"]
    log_path  = paths["log"]
    alerts_state_path = os.path.join(state_dir, "alerts_state.json")
    state_file = _newest(os.path.join(state_dir, "state_*_v1.json"))
    trades_file = _newest(os.path.join(state_dir, "trades_*_v1.jsonl"))

    if not state_file:
        return

    bot_state = _load_state(state_file)
    ss = (bot_state.get("symbol_states") or {}).get("ETHUSDC") or {}
    cur_realized = float(ss.get("realized_pnl") or 0.0)
    cur_ref = float(ss.get("reference_price") or 0.0)

    alerts_state = _load_state(alerts_state_path)
    last_realized = alerts_state.get("realized_baseline")
    last_realized_ts = alerts_state.get("realized_baseline_ts")
    last_seen_oid = alerts_state.get("last_seen_oid")
    last_ref = alerts_state.get("last_ref")

    # --- Alert 1: Big SELL loss on any new trade ---
    # On first run (last_seen_oid is None), skip alerts — just record the last seen trade.
    # This avoids alerting on historical trades when the script is first deployed.
    is_first_run = last_seen_oid is None
    new_trades = _read_trades_since(trades_file, last_seen_oid) if trades_file else []
    if is_first_run:
        # Find the last trade and record it without alerting
        last_oid = None
        if trades_file and os.path.exists(trades_file):
            with open(trades_file) as f:
                for line in f:
                    try:
                        r = json.loads(line.strip())
                        if r.get("event") == "FILL" and r.get("order_id"):
                            last_oid = r["order_id"]
                    except Exception:
                        continue
        if last_oid:
            alerts_state["last_seen_oid"] = last_oid
    else:
        triggered_big_loss = []
        for t in new_trades:
            if str(t.get("side")) != "SELL":
                continue
            try:
                delta = float(t.get("realized_delta") or 0.0)
            except Exception:
                continue
            if delta < -BIG_SELL_LOSS_USDC:
                triggered_big_loss.append(t)
        for t in triggered_big_loss:
            msg = (
                f"🔴 {label}: big SELL loss\n"
                f"qty={t.get('qty')} ETH @ ${float(t.get('price') or 0):,.2f}\n"
                f"realized_delta=${float(t.get('realized_delta') or 0):,.2f}\n"
                f"ts={t.get('ts')}"
            )
            _send_telegram(msg)
        if new_trades:
            alerts_state["last_seen_oid"] = new_trades[-1].get("order_id")

    # --- Alert 2: Cumulative realized PnL drop in the last hour ---
    if last_realized is not None and last_realized_ts:
        baseline_dt = _parse_ts(last_realized_ts)
        if baseline_dt and (now - baseline_dt).total_seconds() >= 3600:
            drop = float(last_realized) - cur_realized
            if drop > REALIZED_DROP_HOUR_USDC and not _on_cooldown(alerts_state, "realized_drop", now):
                msg = (
                    f"⚠️ {label}: realized PnL drop\n"
                    f"-${drop:,.2f} in 1h\n"
                    f"now=${cur_realized:,.2f}, was ${float(last_realized):,.2f}"
                )
                _send_telegram(msg)
                _mark_alert(alerts_state, "realized_drop", now)
            # rotate baseline every hour
            alerts_state["realized_baseline"] = cur_realized
            alerts_state["realized_baseline_ts"] = now.isoformat()
    else:
        alerts_state["realized_baseline"] = cur_realized
        alerts_state["realized_baseline_ts"] = now.isoformat()

    # --- Alert 3: Insufficient cash storm ---
    n_warn = _count_insufficient_cash_warnings(log_path, INSUF_CASH_WINDOW_SEC, now)
    if n_warn > INSUF_CASH_WARN_THRESHOLD and not _on_cooldown(alerts_state, "insufficient_cash", now):
        msg = (
            f"⚠️ {label}: BUY storm of insufficient cash\n"
            f"{n_warn} warnings in last {INSUF_CASH_WINDOW_SEC // 60} min\n"
            f"grid downside is paralyzed — check cash + config"
        )
        _send_telegram(msg)
        _mark_alert(alerts_state, "insufficient_cash", now)

    # --- Alert 4: Drift event (ref dropped between checks) ---
    if last_ref and cur_ref > 0:
        try:
            last_ref_f = float(last_ref)
            if last_ref_f > 0:
                pct_change = (cur_ref - last_ref_f) / last_ref_f * 100.0
                if pct_change < -DRIFT_PCT_THRESHOLD and not _on_cooldown(alerts_state, "drift_event", now):
                    msg = (
                        f"⚠️ {label}: reference price dropped\n"
                        f"${last_ref_f:,.2f} → ${cur_ref:,.2f} ({pct_change:+.2f}%)\n"
                        f"check pro_drift_recenter setting if this was unexpected"
                    )
                    _send_telegram(msg)
                    _mark_alert(alerts_state, "drift_event", now)
        except Exception:
            pass
    alerts_state["last_ref"] = cur_ref
    alerts_state["last_check_ts"] = now.isoformat()
    _save_state(alerts_state_path, alerts_state)


def main() -> None:
    now = _now_utc()
    for name, paths in BUCKETS.items():
        try:
            _check_bucket(name, paths, now)
        except Exception as e:
            print(f"[alerts] {name} check failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
