#!/usr/bin/env python3
"""
mexc_monthly_report.py — how much the bot EARNED (banked realized profit) in a month.

"Earned" = realized profit from completed grid cycles whose SELL closed inside the
month. Crucially it is computed with the SAME LIFO engine the 8-hourly Telegram
report uses (`compute_metrics` in mexc_telegram_report.py) — including seed-cost
for funded buckets and each bucket's baseline cutoff — so the monthly total ALWAYS
reconciles with the per-cycle `R` numbers your partner already sees. It does NOT
read raw `realized_delta` from the log (those predate the bucket-3 seed re-cost and
would overstate) — it re-derives realized from the corrected cost basis.

Unrealized (open-position) PnL is not counted — not "earned" until a cycle closes.

    env/bin/python scripts/mexc_monthly_report.py                 # previous calendar month
    env/bin/python scripts/mexc_monthly_report.py --month 2026-07 # a specific month
    env/bin/python scripts/mexc_monthly_report.py --this-month    # month-to-date
    env/bin/python scripts/mexc_monthly_report.py --send          # also send to Telegram
"""
import sys, os, json, glob, argparse, datetime, urllib.request, urllib.parse
from decimal import Decimal

sys.path.insert(0, "/root/trading_bot/scripts")
sys.path.insert(0, "/root/trading_bot")
from mexc_telegram_report import load_config, load_trades, compute_metrics  # reuse the exact engine

STATE_DIR = "/root/trading_bot/strategies/pct_ladder"
TG_SECRET = "/root/trading_bot/strategies/pct_ladder/secrets/telegram.json"
UTC = datetime.timezone.utc
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))  # bare `since` is IST (matches the 8h report)

# Per bucket: config, label, baseline cutoff (mirrors mexc_telegram_cron.sh), seed inventory.
BUCKETS = {
    "bucket1": dict(label="3% grid",  config=f"{STATE_DIR}/config.mexc.bucket1.json",
                    since="2026-06-08T10:44:47", seed_eth=0.0, seed_cost=0.0),
    "bucket2": dict(label="10% wide", config=f"{STATE_DIR}/config.mexc.bucket2.json",
                    since=None, seed_eth=0.0, seed_cost=0.0),
    "bucket3": dict(label="2% tight", config=f"{STATE_DIR}/config.mexc.bucket3.json",
                    since=None, seed_eth=19.7094, seed_cost=1939.5),
}


def _trade_files(b):
    return sorted(glob.glob(f"{STATE_DIR}/state/{b}/trades_*.jsonl"))


def _ts(r):
    try:
        d = datetime.datetime.fromisoformat(r.get("ts", ""))
        return d if d.tzinfo else d.replace(tzinfo=UTC)
    except Exception:
        return None


def _month_bounds(month: str):
    y, m = int(month[:4]), int(month[5:7])
    start = datetime.datetime(y, m, 1, tzinfo=UTC)
    end = datetime.datetime(y + (m == 12), (m % 12) + 1, 1, tzinfo=UTC)
    return start, end


def earned_for_bucket(b, cfg, start, end):
    """Realized profit (corrected LIFO) from sells in [start, end), + cycle count."""
    fills = load_trades(_trade_files(b))
    if not fills:
        return Decimal("0"), 0
    # 1) apply the bucket's baseline cutoff (same as the Telegram report)
    if cfg["since"]:
        base = datetime.datetime.fromisoformat(cfg["since"])
        base = (base.replace(tzinfo=IST) if base.tzinfo is None else base).astimezone(UTC)
        fills = [r for r in fills if (_ts(r) is not None and _ts(r) >= base)]
    # 2) window to fills BEFORE month end, so LIFO state == end-of-month state
    fills = [r for r in fills if (_ts(r) is not None and _ts(r) < end)]
    if not fills:
        return Decimal("0"), 0
    strat = {}
    if cfg["seed_eth"] > 0 and cfg["seed_cost"] > 0:
        strat["_initial_eth"] = str(cfg["seed_eth"])
        strat["_initial_cost"] = str(cfg["seed_cost"])
    # since = month start → period_pnl counts only sells that closed inside the month
    m = compute_metrics(fills, start, strat)
    return Decimal(str(m["period_pnl"])), int(m["cycles_completed"])


def build_message(month: str, partial: bool) -> str:
    start, end = _month_bounds(month)
    total = Decimal("0")
    rows = []
    for b, cfg in BUCKETS.items():
        earned, cycles = earned_for_bucket(b, cfg, start, end)
        total += earned
        rows.append((f"{b[-1]} ({cfg['label']})", earned, cycles))

    L = []
    tag = " (MTD)" if partial else ""
    L.append(f"*MEXC Monthly Earnings — {start.strftime('%B %Y')}{tag}*")
    L.append("_Banked profit from completed grid cycles_")
    L.append("```")
    L.append(f"{'Bucket':16s}{'Earned':>11s}{'Cycles':>8s}")
    for label, earned, cycles in rows:
        L.append(f"{label:16s}{('$'+format(float(earned), ',.0f')):>11s}{cycles:>8d}")
    L.append("-" * 35)
    L.append(f"{'TOTAL EARNED':16s}{('$'+format(float(total), ',.0f')):>11s}")
    L.append("```")
    L.append("_Realized only — reconciles with the 8h report's R numbers._")
    return "\n".join(L)


def send(text: str):
    s = json.load(open(TG_SECRET))
    token = s["bot_token"]; chats = s["chat_id"]
    if isinstance(chats, str):
        chats = [chats]
    for c in chats:
        data = urllib.parse.urlencode({"chat_id": c, "text": text[:4000],
                                       "parse_mode": "Markdown"}).encode()
        try:
            urllib.request.urlopen(urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data, method="POST"), timeout=15)
        except Exception as e:
            print("telegram failed:", e, file=sys.stderr)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM (default: previous calendar month)")
    ap.add_argument("--this-month", action="store_true", help="current month-to-date")
    ap.add_argument("--send", action="store_true", help="send to Telegram")
    args = ap.parse_args()

    now = datetime.datetime.now(UTC)
    if args.month:
        month = args.month
        partial = (month == now.strftime("%Y-%m"))
    elif args.this_month:
        month = now.strftime("%Y-%m"); partial = True
    else:
        prev = now.replace(day=1) - datetime.timedelta(days=1)
        month = prev.strftime("%Y-%m"); partial = False

    msg = build_message(month, partial)
    print(msg)
    if args.send:
        send(msg)
        print("\n[sent to Telegram]")
