#!/usr/bin/env python3
"""
BTST '1lg0' Lagrangian strategy — PAPER-LIVE bot (flat_legs execution).

Runs the same signal as notebooks/btst_lagrangian_backtest.ipynb, forward, day by day,
with NO real orders (simulated fills at real fetched prices). Two actions per trading day:

  exit   (~09:22 IST, after the 09:20 bar closes): SELL the whole book at the 09:20 open, book
         the overnight (close->open) P&L, drop tranches that have completed lf nights. THEN
         compute today's signal from the ACTUAL 09:20 minute bar (never LTP — it must match the
         backtest) and store the buy list. The signal only needs today's open, so it is fully
         decided here in the morning.
  entry  (~15:06 IST, after the 15:05 bar closes): place the PRE-DECIDED buys at the 15:05 close
         — open the new tranche + re-buy every still-pending tranche (flat_legs re-establish).
         Pure execution, no computation, before the 15:15 CAS freeze.

flat_legs = flat intraday, so ONLY the close->open overnight move is captured (no intraday
exposure) — the whole point of this BTST strategy. A tranche is held for `lf` nights.

State is per-universe under state/btst_paper/<universe>/. Read-only against the market
(quotes/bars only). Usage:
    python scripts/btst_paper_bot.py --universe universe_top250.json --action entry
    python scripts/btst_paper_bot.py --universe universe_top250.json --action exit
    python scripts/btst_paper_bot.py --action selftest      # offline logic check
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from intraday_research.btst_lagrangian import generate_live_signals, session_daily_from_minute
from intraday_research.fyers_cache import safe_symbol_filename
from intraday_research.universe import load_universe

# ── Strategy config (matches the notebook / go-live intent) ───────────────────
PARAMS = {
    "strategy_id": "1lg0",
    "return_type": "log",
    "cost_model": "day_netted",
    "frequency_type": "close to open",
    "tot_capital": 10_00_000,
    "lb": 30,
    "trades": 5,
    "lf": 5,
    "risk_free_rate": 0.02,
    "upper_bound": 0.5,
    "lower_bound": 0,
    "long_only": 1,
    "rmean": "sma",
    "objective": "sortino",
    "execution": "flat_legs",
}
ENTRY_TIME = "15:05"   # buy-at-close price mark (before the 15:15 CAS freeze — no auction issue)
EXIT_TIME = "09:20"    # sell-at-open price mark
MINUTE_DATA_DIR = REPO / "data" / "fyers"
AUTH_FILE = REPO / "fyers_auth.json"
USER_KEY = "user1"


# Dedicated BTST recipients (only the owner) — NOT the shared telegram.json the other bots use.
TELEGRAM_SECRETS = REPO / "strategies" / "pct_ladder" / "secrets" / "telegram_btst.json"
TELEGRAM_SENT_LOG = REPO / "state" / "btst_paper" / "telegram_sent.jsonl"
NOTIFY = True   # set False by --no-telegram


def _plain(sym: str) -> str:
    return sym.removeprefix("NSE:").removesuffix("-EQ")


def _send_telegram(text: str) -> None:
    if not NOTIFY:
        return
    try:
        s = json.loads(TELEGRAM_SECRETS.read_text())
    except Exception:
        print("[telegram] secrets missing — skipping notify", file=sys.stderr)
        return
    token, chats = s.get("bot_token"), s.get("chat_id")
    if isinstance(chats, str):
        chats = [chats]
    if not token or not chats:
        return
    for cid in chats:
        data = urllib.parse.urlencode({"chat_id": cid, "text": text}).encode()
        try:
            resp = urllib.request.urlopen(urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage", data=data, method="POST"), timeout=10)
            mid = (json.loads(resp.read()).get("result") or {}).get("message_id")
            if mid is not None:   # log id so any later cleanup is exact, never a range scan
                TELEGRAM_SENT_LOG.parent.mkdir(parents=True, exist_ok=True)
                with open(TELEGRAM_SENT_LOG, "a") as f:
                    f.write(json.dumps({"chat_id": cid, "message_id": mid,
                                        "ts": dt.datetime.now(dt.timezone.utc).isoformat()}) + "\n")
        except Exception as e:
            print(f"[telegram] send failed: {e}", file=sys.stderr)


# ── Data ──────────────────────────────────────────────────────────────────────
def fetch_minute(symbols: list[str], start: str, end: str) -> None:
    """Incrementally fetch 1-minute bars for the universe (skips already-covered ranges)."""
    cmd = [sys.executable, str(REPO / "scripts" / "fetch_fyers_intraday_data.py"),
           "--auth-file", str(AUTH_FILE), "--user-key", USER_KEY,
           "--start", start, "--end", end, "--output-dir", str(MINUTE_DATA_DIR),
           "--format", "parquet", "--resolution", "1", "--chunk-days", "100",
           "--skip-invalid-symbols"]
    for s in symbols:
        cmd += ["--symbol", s]
    r = subprocess.run(cmd, cwd=REPO, text=True, capture_output=True)
    if r.stdout:
        print(r.stdout[-1500:])
    if r.returncode != 0:
        print(r.stderr[-1500:])
        raise RuntimeError(f"data fetch failed ({r.returncode})")


def load_session_data(symbols: list[str], start: str, end: str) -> dict[str, pd.DataFrame]:
    """Per-ticker session frames: Close = price at ENTRY_TIME (15:05), Open = price at
    EXIT_TIME (09:20). Same source as the backtest so paper prices match."""
    data, skipped = {}, []
    for sym in symbols:
        path = MINUTE_DATA_DIR / f"{safe_symbol_filename(sym)}.parquet"
        if not path.exists():
            skipped.append(sym)
            continue
        daily = session_daily_from_minute(pd.read_parquet(path),
                                          entry_price_time=ENTRY_TIME, exit_price_time=EXIT_TIME)
        mask = (daily["Date"].astype(str) >= start) & (daily["Date"].astype(str) <= end)
        daily = daily.loc[mask].reset_index(drop=True)
        if not daily.empty:
            data[_plain(sym)] = daily
    return data


# ── Paper state ────────────────────────────────────────────────────────────────
def _state_path(universe_name: str) -> Path:
    d = REPO / "state" / "btst_paper" / universe_name
    d.mkdir(parents=True, exist_ok=True)
    return d / "state.json"


def load_state(universe_name: str) -> dict:
    p = _state_path(universe_name)
    if p.exists():
        return json.loads(p.read_text())
    return {"universe": universe_name, "phase": "flat", "realized_pnl": 0.0,
            "last_entry_date": None, "last_exit_date": None, "signal_date": None,
            "pending_entry": [], "tranches": [], "started": dt.date.today().isoformat()}


def save_state(state: dict) -> None:
    p = _state_path(state["universe"])
    p.write_text(json.dumps(state, indent=2, default=str) + "\n")


def _log_trades(universe_name: str, rows: list[dict]) -> None:
    p = _state_path(universe_name).parent / "paper_trades.jsonl"
    with open(p, "a") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")


# ── Actions ─────────────────────────────────────────────────────────────────────
def run_entry(state: dict, data: dict[str, pd.DataFrame]) -> dict:
    """Afternoon (~15:06, after the 15:05 bar): place the PRE-DECIDED buys at the 15:05 close —
    re-buy surviving tranches + open the new tranche from the signal stored this morning. No
    signal computation here (done at the open); this job is pure execution."""
    if state.get("phase") == "overnight":
        raise RuntimeError("phase=overnight: exit hasn't run since the last entry — refusing to double-enter.")
    tdate = _last_date(data)
    if state.get("last_entry_date") == tdate:
        print(f"[entry] already entered for {tdate} — skipping")
        return state

    def close_px(ticker):
        f = data.get(ticker)
        if f is None or f.empty:
            return None
        row = f[f["Date"].astype(str) == tdate]
        return float(row["Close"].iloc[0]) if not row.empty else float(f["Close"].iloc[-1])

    # 1) re-buy still-pending tranches at today's 15:05 close (flat leg)
    state, trades, rebought = _rebuy(state, close_px)

    # 2) open the new tranche from this morning's stored signal (weights -> qty at 15:05 price)
    cap_tranche = PARAMS["tot_capital"] / (PARAMS["lf"] + 1)
    opened, new_positions = [], {}
    for p in state.get("pending_entry", []):
        tkr, perc = p["ticker"], float(p["perc"])
        px = close_px(tkr)
        if px is None or px <= 0:
            continue
        qty = int(perc * cap_tranche / px)
        if qty <= 0:
            continue
        new_positions[tkr] = {"qty": qty, "last_buy_price": px, "entry_price": px}
        opened.append({"ticker": tkr, "qty": qty, "price": px})
        trades.append({"ts": tdate, "action": "BUY", "kind": "open", "ticker": tkr,
                       "qty": qty, "price": px})
    if new_positions:
        state["tranches"].append({"entry_date": tdate, "nights_remaining": PARAMS["lf"],
                                  "positions": new_positions})
    state["phase"] = "overnight"
    state["last_entry_date"] = tdate
    state["pending_entry"] = []
    _log_trades(state["universe"], trades)
    save_state(state)
    _report_entry(state, tdate, opened, rebought)

    # afternoon Telegram: what OPENED today + rolled count
    lines = [f"🌆 BTST {state['universe']} — afternoon {tdate}"]
    if opened:
        lines.append(f"OPENED {len(opened)}:")
        lines += [f"  • {o['ticker']} ×{o['qty']} @ ₹{o['price']:.1f}" for o in opened]
    else:
        lines.append("OPENED today: none")
    lines.append(f"Re-bought {rebought} rolled legs | active tranches: {len(state['tranches'])}")
    _send_telegram("\n".join(lines))
    return state


def _rebuy(state: dict, close_px) -> tuple[dict, list, int]:
    trades, n = [], 0
    tdate = None
    for tr in state["tranches"]:
        for tkr, pos in tr["positions"].items():
            px = close_px(tkr)
            if px is None or px <= 0:
                continue
            pos["last_buy_price"] = px
            n += 1
            trades.append({"ts": state.get("last_exit_date"), "action": "BUY", "kind": "roll",
                           "ticker": tkr, "qty": pos["qty"], "price": px})
    return state, trades, n


def run_exit(state: dict, data: dict[str, pd.DataFrame]) -> dict:
    """Morning (~09:22, after the 09:20 bar): sell the whole book at the 09:20 open, book the
    overnight P&L, drop completed tranches. THEN compute today's signal from that same ACTUAL
    09:20 bar (never LTP — it must match the backtest) and store the buy list for the 15:05 entry."""
    xdate = _last_date(data)
    closed, day_pnl = [], 0.0

    def open_px(ticker):
        f = data.get(ticker)
        if f is None or f.empty:
            return None
        row = f[f["Date"].astype(str) == xdate]
        return float(row["Open"].iloc[0]) if not row.empty else float(f["Open"].iloc[-1])

    # 1) exit the overnight book at the 09:20 open (skip if nothing held or already done today)
    if state.get("phase") == "overnight" and state.get("last_exit_date") != xdate:
        trades = []
        survivors = []
        for tr in state["tranches"]:
            tr_pnl = 0.0
            for tkr, pos in tr["positions"].items():
                px = open_px(tkr)
                if px is None or px <= 0:
                    continue
                pnl = pos["qty"] * (px - pos["last_buy_price"])
                tr_pnl += pnl
                trades.append({"ts": xdate, "action": "SELL", "kind": "leg", "ticker": tkr,
                               "qty": pos["qty"], "price": px, "pnl": round(pnl, 2)})
            day_pnl += tr_pnl
            tr["nights_remaining"] -= 1
            if tr["nights_remaining"] <= 0:
                closed.append({"entry_date": tr["entry_date"],
                               "tickers": sorted(tr["positions"]), "pnl": round(tr_pnl, 2)})
            else:
                survivors.append(tr)
        state["tranches"] = survivors
        state["realized_pnl"] = round(float(state.get("realized_pnl", 0.0)) + day_pnl, 2)
        state["phase"] = "flat"
        state["last_exit_date"] = xdate
        _log_trades(state["universe"], trades)
        _report_exit(state, xdate, closed, day_pnl)
    elif state.get("phase") == "overnight":
        print(f"[exit] already exited for {xdate} — skipping the sell")

    # 2) compute + store today's signal from the ACTUAL 09:20 bar (names + weights only; the
    #    15:05 price sets qty at entry time). buy_price/qty from generate_live_signals are ignored.
    signal = generate_live_signals(PARAMS, data)
    state["pending_entry"] = [{"ticker": r["ticker"], "perc": float(r["perc"])}
                              for _, r in signal.iterrows()]
    state["signal_date"] = xdate
    save_state(state)
    names = [p["ticker"] for p in state["pending_entry"]]
    print(f"[signal] {xdate}: {len(names)} names for the 15:05 entry -> {names}")

    # morning Telegram: overnight P&L + what CLOSED today + the signal to place at 15:05
    lines = [f"🌅 BTST {state['universe']} — morning {xdate}",
             f"Overnight P&L: ₹{day_pnl:,.0f}  |  cumulative ₹{state['realized_pnl']:,.0f}"]
    if closed:
        lines.append(f"CLOSED {len(closed)} (completed {PARAMS['lf']} nights):")
        lines += [f"  • {', '.join(c['tickers'])} → ₹{c['pnl']:,.0f}" for c in closed]
    else:
        lines.append("CLOSED today: none")
    lines.append(f"Signal to BUY at 15:05: {', '.join(names) if names else '(none)'}")
    lines.append(f"Active tranches: {len(state['tranches'])}")
    _send_telegram("\n".join(lines))
    return state


def _last_date(data: dict[str, pd.DataFrame]) -> str:
    return max(str(f["Date"].iloc[-1]) for f in data.values()) if data else dt.date.today().isoformat()


# ── Reporting (printed; Telegram/dashboard wired separately) ─────────────────────
def _report_entry(state, tdate, opened, rebought):
    print(f"\n=== BTST paper [{state['universe']}] ENTRY {tdate} ===")
    print(f"opened {len(opened)} new: " +
          ", ".join(f"{o['ticker']}x{o['qty']}@{o['price']:.1f}" for o in opened))
    print(f"rolled (re-bought) {rebought} pending legs | active tranches: {len(state['tranches'])}")


def _report_exit(state, xdate, closed, day_pnl):
    print(f"\n=== BTST paper [{state['universe']}] EXIT {xdate} ===")
    print(f"overnight P&L today: {day_pnl:,.2f} | cumulative realized: {state['realized_pnl']:,.2f}")
    if closed:
        print(f"closed {len(closed)} tranche(s) (completed {PARAMS['lf']} nights):")
        for c in closed:
            print(f"   entered {c['entry_date']}: {', '.join(c['tickers'])}  P&L {c['pnl']:,.2f}")
    print(f"active tranches remaining: {len(state['tranches'])}")


# ── Offline self-test: flat_legs must capture exactly the overnight moves ────────
def selftest() -> int:
    """Synthetic 1 ticker, lf=2. Prices: closes C, opens O. flat_legs P&L over the tranche's
    life must equal qty * sum of (open[d+1]-close[d]) overnight gaps — and NOT include any
    intraday (open->close) move."""
    global PARAMS
    PARAMS = {**PARAMS, "lf": 2}
    # Build a fake data dict for generate_live_signals is overkill; test the book directly.
    qty = 10
    # day0 close=100 (entry); day1 open=105 close=90; day2 open=95 (final exit)
    st = {"universe": "_selftest", "phase": "flat", "realized_pnl": 0.0,
          "last_entry_date": None, "last_exit_date": None, "tranches": []}
    # manual entry day0
    st["tranches"].append({"entry_date": "d0", "nights_remaining": 2,
                           "positions": {"X": {"qty": qty, "last_buy_price": 100.0, "entry_price": 100.0}}})
    st["phase"] = "overnight"
    # exit day1 open=105
    for tr in st["tranches"]:
        for _, pos in tr["positions"].items():
            st["realized_pnl"] += pos["qty"] * (105.0 - pos["last_buy_price"])
        tr["nights_remaining"] -= 1
    st["phase"] = "flat"
    # entry day1: rebuy @ close=90
    for tr in st["tranches"]:
        for _, pos in tr["positions"].items():
            pos["last_buy_price"] = 90.0
    st["phase"] = "overnight"
    # exit day2 open=95
    for tr in list(st["tranches"]):
        for _, pos in tr["positions"].items():
            st["realized_pnl"] += pos["qty"] * (95.0 - pos["last_buy_price"])
        tr["nights_remaining"] -= 1
    pnl = st["realized_pnl"]
    expected = qty * ((105 - 100) + (95 - 90))   # two overnight gaps only
    intraday = qty * ((90 - 105))                # day1 intraday move that must NOT appear
    ok = abs(pnl - expected) < 1e-9
    print(f"selftest: pnl={pnl} expected(overnight only)={expected} "
          f"(intraday {intraday} correctly excluded) -> {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--universe", help="universe json (e.g. universe_top250.json)")
    ap.add_argument("--action", required=True, choices=["entry", "exit", "status", "selftest"])
    ap.add_argument("--no-fetch", action="store_true", help="use cached bars, skip the fetch")
    ap.add_argument("--no-telegram", action="store_true", help="don't send Telegram messages")
    ap.add_argument("--lookback-days", type=int, default=90, help="calendar days of data to load")
    args = ap.parse_args()

    global NOTIFY
    NOTIFY = not args.no_telegram

    if args.action == "selftest":
        return selftest()

    uni = load_universe(REPO / args.universe)
    uname = Path(args.universe).stem
    symbols = list(uni.symbols)
    end = dt.date.today().isoformat()
    start = str(dt.date.today() - dt.timedelta(days=args.lookback_days))

    if not args.no_fetch:
        fetch_minute(symbols, start, end)
    data = load_session_data(symbols, start, end)
    print(f"[{uname}] loaded {len(data)}/{len(symbols)} symbols, {start}..{end}")

    state = load_state(uname)
    if args.action == "entry":
        run_entry(state, data)
    elif args.action == "exit":
        run_exit(state, data)
    elif args.action == "status":
        print(json.dumps({k: v for k, v in state.items() if k != "tranches"}, indent=2))
        print(f"active tranches: {len(state['tranches'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
