#!/usr/bin/env python3
"""
mexc_telegram_report.py — Send 8-hour trading summary to Telegram.

Metrics (for the configured look-back window):
  - cycles_completed : non-rebalance fills in the period (each fill = cycle leg completed)
  - avg_ladder_size  : avg cum_quote_qty of non-rebalance fills in period
  - current_ladder_size : buy_quote from config
  - rebalance_qty    : sum of abs(qty) of rebalance fills in the period
  - total_bot_pnl    : all-time formula PnL  (sum of cum_quote_qty × pct for cycle sells)

Telegram credentials: strategies/pct_ladder/secrets/telegram.json
  { "bot_token": "...", "chat_id": "..." }

Usage:
    python3 scripts/mexc_telegram_report.py \\
        --config  strategies/pct_ladder/config.mexc.json \\
        --trades  strategies/pct_ladder/state/mexc_trades.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl \\
        --hours   8
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import urllib.request
import urllib.parse
from decimal import Decimal
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _is_rebalance(reason: str) -> bool:
    return bool(re.search(r'rebalance|rebal', reason, re.IGNORECASE))


def _is_cycle(reason: str) -> bool:
    return bool(re.search(r'ltp[<>]=ref[+\-]', reason))


def _parse_pct(reason: str) -> Decimal:
    m = re.search(r'[+\-](\d+\.?\d*)%', reason)
    return Decimal(m.group(1)) / Decimal("100") if m else D0


def _fmt_ist(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M IST")


def portfolio_verify(fills: list[dict], manual_positions: list[dict],
                     cash: Decimal, broker_eth: Decimal, cmp: Decimal,
                     invested: Decimal, cycle_pnl: Decimal) -> str:
    """
    Compute true P&L via FIFO (avg buy vs avg sell method) and reconcile
    against current portfolio value.  Returns a printable report string.
    """
    # Build initial lots from manual positions
    lots: list[list] = []
    for mp in manual_positions:
        q = _dec(mp.get("qty", 0))
        p = _dec(mp.get("buy_price", 0))
        if q > D0 and p > D0:
            lots.append([q, p])

    total_buy_qty  = _dec(sum(mp.get("qty", 0) for mp in manual_positions))
    total_buy_cost = sum(_dec(mp.get("qty", 0)) * _dec(mp.get("buy_price", 0))
                         for mp in manual_positions)
    total_sell_qty  = D0
    total_sell_cost = D0   # proceeds
    gross_gain      = D0
    gross_loss      = D0

    for r in fills:
        side  = str(r.get("side") or "").upper()
        qty   = _dec(r.get("qty") or "0")
        price = _dec(r.get("price") or "0")
        cqq   = _dec(r.get("cum_quote_qty") or "0")
        if qty <= D0 or price <= D0:
            continue
        if side == "BUY":
            lots.append([qty, price])
            total_buy_qty  += qty
            total_buy_cost += (cqq if cqq > D0 else qty * price)
        elif side == "SELL":
            total_sell_qty  += qty
            total_sell_cost += (cqq if cqq > D0 else qty * price)
            rem = qty
            while rem > D0 and lots:
                take   = min(rem, lots[0][0])
                buy_p  = lots[0][1]
                pnl    = take * (price - buy_p)
                if pnl >= D0:
                    gross_gain += pnl
                else:
                    gross_loss += pnl
                rem        -= take
                lots[0][0] -= take
                if lots[0][0] <= D0:
                    lots.pop(0)

    avg_buy  = total_buy_cost  / total_buy_qty  if total_buy_qty  > D0 else D0
    avg_sell = total_sell_cost / total_sell_qty if total_sell_qty > D0 else D0

    true_realized  = total_sell_qty * (avg_sell - avg_buy)
    open_qty       = sum(l[0] for l in lots)
    open_cost      = sum(l[0] * l[1] for l in lots)
    open_avg       = open_cost / open_qty if open_qty > D0 else D0
    unrealized     = open_qty * (cmp - open_avg)   # open lots at avg cost vs CMP
    hidden_losses  = cycle_pnl - true_realized      # LIFO shows more than true realized

    pv          = cash + broker_eth * cmp
    pv_gain     = pv - invested
    breakeven   = (invested - cash) / broker_eth if broker_eth > D0 else D0

    lines = [
        "=== Portfolio Verify ===",
        f"Avg buy:  {float(avg_buy):.2f}  |  Avg sell: {float(avg_sell):.2f}  |  Spread: {float(avg_sell-avg_buy):+.2f}/ETH",
        f"",
        f"Cycle PnL (LIFO):   {float(cycle_pnl):+.2f}",
        f"True realized:      {float(true_realized):+.2f}  ({float(total_sell_qty):.2f} ETH × {float(avg_sell-avg_buy):.2f})",
        f"Hidden losses:      {float(hidden_losses):-,.2f}  (LIFO hides sells below cost)",
        f"  Gross gains:      {float(gross_gain):+,.2f}",
        f"  Gross losses:     {float(gross_loss):+,.2f}",
        f"",
        f"Open ETH:           {float(open_qty):.4f} @ avg {float(open_avg):.2f}",
        f"Unrealized PnL:     {float(unrealized):+.2f}  (CMP {float(cmp):.2f} vs avg {float(open_avg):.2f})",
        f"",
        f"Net PnL:            {float(true_realized + unrealized):+.2f}",
        f"Invested:           {float(invested):.2f}",
        f"Portfolio Value:    {float(pv):.2f}  (gain: {float(pv_gain):+.2f})",
        f"Breakeven ETH:      {float(breakeven):.2f}",
    ]
    return "\n".join(lines)


def load_config(config_path: str) -> dict:
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


def load_trades(trades_paths: list[str]) -> list[dict]:
    """Load and deduplicate all FILL events, sorted by timestamp."""
    seen: set[str] = set()
    events: list[dict] = []
    for path in trades_paths:
        try:
            with open(path, encoding="utf-8") as f:
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
                    oid = str(r.get("order_id") or "")
                    if oid and oid in seen:
                        continue
                    if oid:
                        seen.add(oid)
                    if _dec(r.get("qty") or "0") <= D0:
                        continue
                    events.append(r)
        except FileNotFoundError:
            pass
    events.sort(key=lambda r: r.get("ts", ""))
    return events



def compute_metrics(fills: list[dict], since: datetime.datetime, cfg_strategy: dict) -> dict:
    buy_quote = _dec(cfg_strategy.get("buy_quote", 0))
    upper_pct = _dec(cfg_strategy.get("upper_pct", 0))

    # LIFO stack — each entry: [remaining_qty, price, is_rebal]
    # BUYs are pushed to the end (top); SELLs match from top (latest buy first).
    open_buys: list[list] = []

    all_time_pnl        = D0
    all_time_rebal_pnl  = D0
    all_time_cycles     = 0
    all_time_rebal_cyc  = 0
    period_pnl          = D0
    period_rebal_pnl    = D0
    period_cycles       = 0
    period_rebal_cycles = 0
    rebalance_qty       = D0
    period_ladder_values: list[Decimal] = []
    period_fills_seq: list[tuple] = []   # (side, qty, price) for stock-PnL segment walk
    # Average-cost realized PnL: tracks running avg cost of all bot-bought ETH.
    # For each sell: realized_pnl = qty × (sell_price − avg_cost_at_that_moment).
    # This is the "real" PnL — what the sell actually earned vs what was paid for that ETH.
    avg_cost_qty  = D0   # running ETH qty (from all buys, used to maintain avg cost)
    avg_cost_cost = D0   # running total cost paid for all bought ETH
    period_rebal_avg_pnl  = D0   # realized PnL (avg-cost) for rebalance sells in period
    period_ladder_avg_pnl = D0   # realized PnL (avg-cost) for ladder sells in period

    def process_trade(side, qty, price, is_rebal, in_period):
        nonlocal all_time_pnl, all_time_rebal_pnl, all_time_cycles, all_time_rebal_cyc
        nonlocal period_pnl, period_rebal_pnl, period_cycles, period_rebal_cycles

        if side == "BUY":
            # BUY always opens a new long position on the stack.
            open_buys.append([qty, price, is_rebal])
            return

        # SELL: find the most recent buy lot BELOW the sell price (one lot only).
        # Matching only one lot avoids cross-lot spillage that creates artificial losses
        # when a sell partially consumes a cheap lot then spills into an expensive one.
        # Remaining unmatched sell qty is discarded (spot-only bot; no shorting).
        i = len(open_buys) - 1
        while i >= 0:
            entry = open_buys[i]
            if price > entry[1]:  # profitable match found
                take = min(qty, entry[0])
                cycle_pnl = take * (price - entry[1])
                any_rebal = is_rebal or entry[2]
                entry[0] -= take
                if entry[0] <= D0:
                    open_buys.pop(i)
                # Count 1 cycle for this sell
                if any_rebal:
                    all_time_rebal_pnl += cycle_pnl
                    all_time_rebal_cyc += 1
                    if in_period:
                        period_rebal_pnl    += cycle_pnl
                        period_rebal_cycles += 1
                else:
                    all_time_pnl    += cycle_pnl
                    all_time_cycles += 1
                    if in_period:
                        period_pnl    += cycle_pnl
                        period_cycles += 1
                return
            i -= 1
        # No profitable buy found — sell discarded (initial inventory depletion)

    for r in fills:
        ts_str = r.get("ts", "")
        try:
            ts = datetime.datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
        except Exception:
            ts = None

        reason = str(r.get("reason") or "")
        side   = str(r.get("side") or "").upper()
        qty    = _dec(r.get("qty") or "0")
        price  = _dec(r.get("price") or "0")
        cqq    = _dec(r.get("cum_quote_qty") or "0")
        is_reb = _is_rebalance(reason)
        in_period = ts is not None and ts >= since

        if qty <= D0 or price <= D0:
            continue

        if is_reb and in_period:
            rebalance_qty += qty

        if side in ("BUY", "SELL"):
            # Maintain running average cost (all bot buys, regardless of period)
            notional = cqq if cqq > D0 else qty * price
            if side == "BUY":
                avg_cost_qty  += qty
                avg_cost_cost += notional
            elif side == "SELL" and avg_cost_qty > D0:
                avg_cost  = avg_cost_cost / avg_cost_qty
                avg_pnl   = qty * (price - avg_cost)
                # Reduce cost basis by the sold portion
                sell_qty  = min(qty, avg_cost_qty)
                avg_cost_cost -= sell_qty * avg_cost
                avg_cost_qty  -= sell_qty
                if in_period:
                    if is_reb:
                        period_rebal_avg_pnl  += avg_pnl
                    else:
                        period_ladder_avg_pnl += avg_pnl

            process_trade(side, qty, price, is_reb, in_period)
            if in_period:
                period_fills_seq.append((side, qty, price, is_reb))

        if in_period and not is_reb and cqq > D0:
            period_ladder_values.append(cqq)

    avg_ladder = (sum(period_ladder_values) / len(period_ladder_values)
                  if period_ladder_values else buy_quote)

    return {
        "cycles_completed":     period_cycles,
        "avg_ladder_size":      avg_ladder,
        "current_ladder_size":  buy_quote,
        "upper_pct":            upper_pct,
        "rebalance_qty":        rebalance_qty,
        "rebal_cycles":         period_rebal_cycles,
        "rebal_pnl":            period_rebal_pnl,           # theoretical LIFO (unused in message now)
        "rebal_avg_pnl":        period_rebal_avg_pnl,    # realized PnL: rebal sells vs avg cost
        "ladder_avg_pnl":       period_ladder_avg_pnl,  # realized PnL: ladder sells vs avg cost
        "avg_cost":             avg_cost_cost / avg_cost_qty if avg_cost_qty > D0 else D0,
        "total_bot_pnl":        all_time_pnl,
        "period_pnl":           period_pnl,
        "total_rebal_pnl":      all_time_rebal_pnl,
        "period_fills_seq":     period_fills_seq,
    }


def _fmt_short(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%m-%d %H:%M")


def build_message(metrics: dict, since: datetime.datetime, now: datetime.datetime,
                  hours: int, state_path: str | None, symbol: str,
                  last_report_path: str | None = None) -> tuple[str, dict | None]:
    """
    Returns (message_text, pv_snapshot_to_save).
    pv_snapshot_to_save is None if state could not be read.
    """
    m = metrics
    s   = float(m['current_ladder_size'])
    pct = float(m['upper_pct'])
    s_str   = f"{int(s)}"   if s   == int(s)   else f"{s}"
    pct_str = f"{int(pct)}" if pct == int(pct) else f"{pct}"

    def _sgn(v, decimals=0):
        fmt = f"{{:+.{decimals}f}}"
        return fmt.format(v)

    # Cycle PnL (theoretical LIFO spread — the only non-real number)
    pp = float(m['period_pnl'])
    pp_str = _sgn(pp, 0)

    # Rebalance realized PnL: (sell_price − avg_buy_cost) × qty for rebal sells in period
    rc = float(m['rebal_avg_pnl'])
    rc_str = _sgn(rc, 0)

    # Ladder realized PnL: (sell_price − avg_buy_cost) × qty for ladder sells in period
    # Positive = sold above average cost (true profit)
    # Negative = sold below average cost (selling into a falling market)
    lc = float(m['ladder_avg_pnl'])
    lc_str = _sgn(lc, 0)

    # Portfolio value + compound step size from state
    pv_str = ""
    compound_s_str = None
    pv_snapshot = None

    if state_path:
        try:
            with open(state_path, encoding="utf-8") as f:
                state = json.load(f)
            price      = _dec(state.get("last_prices", {}).get(symbol) or "0")
            cash       = _dec(state.get("cash") or "0")
            broker_eth = _dec(state.get("extras", {}).get(f"broker_base_qty_{symbol}") or "0")
            if broker_eth <= D0:
                ss         = (state.get("symbol_states") or {}).get(symbol) or {}
                broker_eth = _dec(ss.get("traded_qty") or "0")
            pv     = cash + broker_eth * price
            pv_str = f", PV{int(pv)}"

            pv_snapshot = {"ts": now.isoformat(), "pv": float(pv), "price": float(price)}

            cbq = _dec(state.get("extras", {}).get("compound_buy_quote") or "0")
            if cbq > D0:
                cbq_f = float(cbq)
                compound_s_str = f"{int(cbq_f)}" if cbq_f == int(cbq_f) else f"{cbq_f:.2f}"
        except Exception:
            pass

    effective_s_str = compound_s_str if compound_s_str is not None else s_str

    # PV with S (ETH price MTM on full holding) and B (ladder avg-cost realized PnL)
    if pv_snapshot and last_report_path and broker_eth > D0:
        try:
            with open(last_report_path, encoding="utf-8") as f:
                last = json.load(f)
            last_price = _dec(last.get("price", 0))
            if last_price > D0:
                dP      = float(_dec(pv_snapshot["price"])) - float(last_price)
                eth_mtm = float(broker_eth) * dP
                pv_str  = f", PV{int(pv_snapshot['pv'])}(S{_sgn(eth_mtm, 0)},B{lc_str})"
        except Exception:
            pass

    line1 = f"{_fmt_short(since)} -> {_fmt_short(now)}"
    line2 = (
        f"C{m['cycles_completed']}({pp_str}), "
        f"A{int(float(m['avg_ladder_size']))}, "
        f"S{effective_s_str}({pct_str}%), "
        f"R{m['rebal_cycles']}({rc_str}), "
        f"P{float(m['total_bot_pnl']):.2f}"
        f"{pv_str}"
    )
    return f"{line1}\n{line2}", pv_snapshot


def send_telegram(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "Markdown",
    }).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        result = json.loads(resp.read())
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error: {result}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config",       required=True, help="Strategy config JSON")
    ap.add_argument("--trades",       required=True, nargs="+", help="Trade .jsonl files")
    ap.add_argument("--hours",        type=int, default=8, help="Look-back window in hours")
    ap.add_argument("--initial-eth",  type=float, default=0,
                    help="Initial ETH holding before first trade (to seed FIFO queue)")
    ap.add_argument("--initial-cost", type=float, default=0,
                    help="Average cost of initial ETH holding")
    ap.add_argument("--secrets",      default=None,
                    help="Telegram secrets JSON (default: <config_dir>/secrets/telegram.json)")
    ap.add_argument("--dry-run",      action="store_true", help="Print message, don't send")
    ap.add_argument("--verify",       action="store_true", help="Print full portfolio P&L verification and exit")
    args = ap.parse_args()

    cfg      = load_config(args.config)
    strategy = cfg.get("strategy", {})
    symbol   = (strategy.get("symbols") or ["ETHUSDC"])[0]
    state_path = None
    paths = cfg.get("paths", {})
    if paths.get("state_path"):
        base = os.path.dirname(os.path.abspath(args.config))
        state_path = os.path.join(base, paths["state_path"])

    # Pass initial inventory into strategy dict for compute_metrics seeding
    if args.initial_eth > 0 and args.initial_cost > 0:
        strategy["_initial_eth"]  = str(args.initial_eth)
        strategy["_initial_cost"] = str(args.initial_cost)

    now   = datetime.datetime.now(tz=UTC)
    since = now - datetime.timedelta(hours=args.hours)

    # Sidecar file storing PV+price from the previous report (for accurate T/S calc)
    last_report_path = None
    if state_path:
        last_report_path = os.path.join(os.path.dirname(state_path), "telegram_last_report.json")

    fills   = load_trades(args.trades)
    metrics = compute_metrics(fills, since, strategy)
    msg, pv_snapshot = build_message(metrics, since, now, args.hours, state_path, symbol,
                                     last_report_path=last_report_path)

    if args.verify:
        # build_message now returns a tuple; re-invoke cleanly for verify path
        # Load capital flows + manual positions for full portfolio reconciliation
        base = os.path.dirname(os.path.abspath(args.config))
        cap_flows_path  = os.path.join(base, "state", "capital_flows_2026_03_05_v1.json")
        man_pos_path    = os.path.join(base, "state", "manual_positions_2026_03_05_v1.json")
        invested = D0
        manual_positions = []
        try:
            with open(cap_flows_path, encoding="utf-8") as f:
                for flow in json.load(f):
                    invested += _dec(flow.get("delta", 0))
        except FileNotFoundError:
            pass
        try:
            with open(man_pos_path, encoding="utf-8") as f:
                manual_positions = json.load(f)
        except FileNotFoundError:
            pass

        cash = broker_eth_qty = cmp_price = D0
        if state_path:
            try:
                with open(state_path, encoding="utf-8") as f:
                    st = json.load(f)
                cmp_price    = _dec(st.get("last_prices", {}).get(symbol) or "0")
                cash         = _dec(st.get("cash") or "0")
                broker_eth_qty = _dec(st.get("extras", {}).get(f"broker_base_qty_{symbol}") or "0")
                if broker_eth_qty <= D0:
                    ss = (st.get("symbol_states") or {}).get(symbol) or {}
                    broker_eth_qty = _dec(ss.get("traded_qty") or "0")
            except Exception:
                pass

        cycle_pnl = _dec(metrics["total_bot_pnl"])
        report = portfolio_verify(fills, manual_positions, cash, broker_eth_qty,
                                  cmp_price, invested, cycle_pnl)
        print(report)
        return

    print(msg)
    print()

    if args.dry_run:
        print("[dry-run] Message not sent.")
        return

    def _save_snapshot():
        if pv_snapshot and last_report_path:
            try:
                with open(last_report_path, "w", encoding="utf-8") as f:
                    json.dump(pv_snapshot, f)
            except Exception as e:
                print(f"Warning: could not save last-report snapshot: {e}")

    # Load Telegram secrets
    secrets_path = args.secrets
    if not secrets_path:
        config_dir   = os.path.dirname(os.path.abspath(args.config))
        secrets_path = os.path.join(config_dir, "secrets", "telegram.json")

    try:
        with open(secrets_path, encoding="utf-8") as f:
            tg = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Telegram secrets not found at {secrets_path}")
        print("Create it with: { \"bot_token\": \"...\", \"chat_id\": \"...\" }")
        raise SystemExit(1)

    token    = tg["bot_token"]
    chat_ids = tg["chat_id"]
    if isinstance(chat_ids, str):
        chat_ids = [chat_ids]
    for chat_id in chat_ids:
        send_telegram(token, str(chat_id), msg)
        print(f"Sent to Telegram chat {chat_id}")
    _save_snapshot()


if __name__ == "__main__":
    main()
