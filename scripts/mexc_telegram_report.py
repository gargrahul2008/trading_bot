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


def _lifo_realized(trades_seq) -> float:
    """Separate-stack LIFO matching. Returns total realized PnL from matched pairs."""
    buy_stack: list = []
    sell_stack: list = []
    realized = 0.0
    for side, qty, px in trades_seq:
        if qty < 1e-9:
            continue
        if side == "BUY":
            remaining = qty
            while remaining > 1e-9 and sell_stack:
                take = min(remaining, sell_stack[-1][0])
                realized += take * (sell_stack[-1][1] - px)
                remaining -= take
                sell_stack[-1][0] -= take
                if sell_stack[-1][0] < 1e-9:
                    sell_stack.pop()
            if remaining > 1e-9:
                buy_stack.append([remaining, px])
        elif side == "SELL":
            remaining = qty
            while remaining > 1e-9 and buy_stack:
                take = min(remaining, buy_stack[-1][0])
                realized += take * (px - buy_stack[-1][1])
                remaining -= take
                buy_stack[-1][0] -= take
                if buy_stack[-1][0] < 1e-9:
                    buy_stack.pop()
            if remaining > 1e-9:
                sell_stack.append([remaining, px])
    return realized


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

    # Seed opening inventory (ETH a bucket was funded with, not bought via grid trades).
    # Without this, sells of that seed have no buy to match → they're skipped (no PnL shown).
    _init_eth  = _dec(cfg_strategy.get("_initial_eth")  or 0)
    _init_cost = _dec(cfg_strategy.get("_initial_cost") or 0)

    # Unified LIFO stack for period cycle counting — [remaining_qty, price, is_rebal]
    open_buys: list[list] = []
    if _init_eth > D0 and _init_cost > D0:
        open_buys.append([_init_eth, _init_cost, False])

    all_time_pnl        = D0
    period_pnl          = D0
    period_rebal_pnl    = D0
    period_cycles       = 0
    period_rebal_cycles = 0
    # All-time grid (non-rebalance) completed-cycle counters
    at_cycles           = 0   # total matched grid sells (completed cycles)
    at_cycle_wins       = 0   # cycles closed at a profit
    at_cycle_losses     = 0   # cycles closed at a loss
    at_cycle_pnl        = D0  # sum of those cycles' realized PnL (LIFO)
    rebalance_qty       = D0
    period_ladder_values: list[Decimal] = []

    # Flow tracking (all-time)
    grid_net_usdc  = D0
    grid_net_eth   = D0
    rebal_net_usdc = D0
    rebal_net_eth  = D0
    total_net_usdc = D0
    total_net_eth  = D0

    # Trade sequences for separate-stack LIFO (all-time)
    grid_seq:  list[tuple] = []
    rebal_seq: list[tuple] = []
    if _init_eth > D0 and _init_cost > D0:
        grid_seq.append(("BUY", float(_init_eth), float(_init_cost)))

    def process_trade(side, qty, price, is_rebal, in_period):
        """Unified LIFO for period cycle counting. Standard matching (all sells, not just profitable)."""
        nonlocal all_time_pnl, period_cycles, period_rebal_cycles
        nonlocal period_pnl, period_rebal_pnl
        nonlocal at_cycles, at_cycle_wins, at_cycle_losses, at_cycle_pnl

        if side == "BUY":
            open_buys.append([qty, price, is_rebal])
            return

        # SELL: consume from top of stack (standard LIFO)
        rem = qty
        sell_pnl = D0
        any_rebal = is_rebal
        while rem > D0 and open_buys:
            entry = open_buys[-1]
            take = min(rem, entry[0])
            sell_pnl += take * (price - entry[1])
            any_rebal = any_rebal or entry[2]
            entry[0] -= take
            if entry[0] <= D0:
                open_buys.pop()
            rem -= take

        if sell_pnl != D0 or rem < qty:
            all_time_pnl += sell_pnl
            if not any_rebal:
                # All-time completed grid cycle (this matched sell closed inventory)
                at_cycles += 1
                at_cycle_pnl += sell_pnl
                if sell_pnl >= D0:
                    at_cycle_wins += 1
                else:
                    at_cycle_losses += 1
            if in_period:
                if any_rebal:
                    period_rebal_pnl += sell_pnl
                    period_rebal_cycles += 1
                else:
                    period_pnl += sell_pnl
                    period_cycles += 1

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
            notional = cqq if cqq > D0 else qty * price

            # Flow tracking
            if side == "BUY":
                total_net_usdc -= notional
                total_net_eth  += qty
                if is_reb:
                    rebal_net_usdc -= notional
                    rebal_net_eth  += qty
                else:
                    grid_net_usdc -= notional
                    grid_net_eth  += qty
            else:
                total_net_usdc += notional
                total_net_eth  -= qty
                if is_reb:
                    rebal_net_usdc += notional
                    rebal_net_eth  -= qty
                else:
                    grid_net_usdc += notional
                    grid_net_eth  -= qty

            # Build sequences for separate-stack LIFO
            entry = (side, float(qty), float(price))
            if is_reb:
                rebal_seq.append(entry)
            else:
                grid_seq.append(entry)

            process_trade(side, qty, price, is_reb, in_period)

        if in_period and not is_reb and cqq > D0:
            period_ladder_values.append(cqq)

    avg_ladder = (sum(period_ladder_values) / len(period_ladder_values)
                  if period_ladder_values else buy_quote)

    return {
        "cycles_completed":    period_cycles,
        "period_pnl":          period_pnl,
        "avg_ladder_size":     avg_ladder,
        "current_ladder_size": buy_quote,
        "upper_pct":           upper_pct,
        "rebalance_qty":       rebalance_qty,
        "rebal_cycles":        period_rebal_cycles,
        "period_rebal_pnl":   period_rebal_pnl,
        "total_bot_pnl":       all_time_pnl,
        "at_cycles":           at_cycles,
        "at_cycle_wins":       at_cycle_wins,
        "at_cycle_losses":     at_cycle_losses,
        "at_cycle_pnl":        at_cycle_pnl,
        "grid_net_usdc":      float(grid_net_usdc),
        "grid_net_eth":       float(grid_net_eth),
        "rebal_net_usdc":     float(rebal_net_usdc),
        "rebal_net_eth":      float(rebal_net_eth),
        "total_net_usdc":     float(total_net_usdc),
        "total_net_eth":      float(total_net_eth),
        "grid_seq":           grid_seq,
        "rebal_seq":          rebal_seq,
    }


def _fmt_short(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%m-%d %H:%M")


def build_message(metrics: dict, since: datetime.datetime, now: datetime.datetime,
                  hours: int, state_path: str | None, symbol: str,
                  last_report_path: str | None = None,
                  extra_eth: float = 0.0) -> tuple[str, dict | None]:
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

    pp_str = _sgn(float(m['period_pnl']), 0)
    rp_str = _sgn(float(m['period_rebal_pnl']), 0)

    # Separate-stack LIFO for all-time realized PnL
    grid_realized  = _lifo_realized(m['grid_seq'])
    rebal_realized = _lifo_realized(m['rebal_seq'])

    # Flow totals from compute_metrics
    grid_net_usdc  = m['grid_net_usdc']
    grid_net_eth   = m['grid_net_eth']
    rebal_net_usdc = m['rebal_net_usdc']
    rebal_net_eth  = m['rebal_net_eth']
    total_net_usdc = m['total_net_usdc']
    total_net_eth  = m['total_net_eth']

    # Read state for portfolio data
    price = D0
    pv = D0
    pv_snapshot = None
    compound_s_str = None
    eth_holding_pnl = None
    net_pnl = None
    held_str = None

    if state_path:
        try:
            with open(state_path, encoding="utf-8") as f:
                state = json.load(f)
            price      = _dec(state.get("last_prices", {}).get(symbol) or "0")
            cash       = _dec(state.get("cash") or "0")
            broker_eth = _dec(state.get("extras", {}).get(f"broker_base_qty_{symbol}") or "0")
            if broker_eth <= D0:
                ss = (state.get("symbol_states") or {}).get(symbol) or {}
                broker_eth = _dec(ss.get("traded_qty") or "0")

            start_pv = float(_dec(state.get("extras", {}).get("portfolio_start_value") or "0"))

            # Prefer pnl_summary.json for balances
            try:
                summary_path = os.path.join(os.path.dirname(state_path), "pnl_summary.json")
                with open(summary_path, encoding="utf-8") as f:
                    summary = json.load(f)
                holdings = summary.get("holdings") or {}
                quote_total = _dec(holdings.get("quote_total") or "0")
                sym_data = (holdings.get("per_symbol") or {}).get(symbol) or {}
                base_total = _dec(sym_data.get("base_total") or "0")
                px = _dec(sym_data.get("px") or "0")
                if quote_total > D0:
                    cash = quote_total
                if base_total > D0:
                    broker_eth = base_total
                if px > D0:
                    price = px
            except Exception:
                pass

            # Include untracked HODL ETH in the displayed PV so the bucket's total holdings show up,
            # even if the bot itself only tracks the trading slice.
            extra_eth_dec = _dec(str(extra_eth)) if extra_eth else D0
            pv = cash + (broker_eth + extra_eth_dec) * price
            pv_snapshot = {"ts": now.isoformat(), "pv": float(pv), "price": float(price)}
            # Held ETH + cash for THIS bucket (tracked slice) — shown on every message.
            held_str = f"Held: {float(broker_eth):.4f} ETH + ${float(cash):,.0f} cash"

            cbq = _dec(state.get("extras", {}).get("compound_buy_quote") or "0")
            if cbq > D0:
                cbq_f = float(cbq)
                compound_s_str = f"{int(cbq_f)}" if cbq_f == int(cbq_f) else f"{cbq_f:.2f}"
            wpct = state.get("extras", {}).get("weekend_upper_pct")
            if wpct:
                pct_str = f"{float(wpct):g}"

            # Compute ETH holding PnL from start state. Compute even with ZERO trades —
            # a bucket that just holds ETH still has mark-to-market PnL (was previously
            # guarded by total_net_eth != 0, which made no-trade buckets show Net+0).
            fp = float(price)
            if fp > 0 and start_pv > 0:
                start_eth  = float(broker_eth) - total_net_eth
                start_usdc = float(cash) - total_net_usdc
                if start_eth > 0.1:
                    start_price = (start_pv - start_usdc) / start_eth
                    eth_holding_pnl = start_eth * (fp - start_price)
        except Exception:
            pass

    effective_s_str = compound_s_str if compound_s_str is not None else s_str
    fp = float(price)

    # 4-bucket PnL: flow-based totals, separate-stack realized, derived unrealized
    grid_total  = grid_net_usdc  + grid_net_eth  * fp if fp > 0 else 0.0
    rebal_total = rebal_net_usdc + rebal_net_eth * fp if fp > 0 else 0.0
    grid_unrealized  = grid_total  - grid_realized
    rebal_unrealized = rebal_total - rebal_realized
    trading_total = grid_total + rebal_total
    net_pnl = (eth_holding_pnl or 0.0) + trading_total

    # Format message
    line1 = f"{_fmt_short(since)} -> {_fmt_short(now)}"
    line2 = (
        f"C{m['cycles_completed']}({pp_str}), "
        f"S{effective_s_str}({pct_str}%), "
        f"Rb{m['rebal_cycles']}({rp_str})"
    )
    line3 = (
        f"Bot{_sgn(grid_realized, 0)}(u{_sgn(grid_unrealized, 0)}), "
        f"Rbl{_sgn(rebal_realized, 0)}(u{_sgn(rebal_unrealized, 0)})"
    )
    # All-time completed grid cycles: total, win/loss split, and their realized PnL (LIFO)
    line_cyc = (
        f"Cyc{m['at_cycles']}: {m['at_cycle_wins']}W/{m['at_cycle_losses']}L, "
        f"R{_sgn(float(m['at_cycle_pnl']), 0)}"
    )
    parts4 = []
    if eth_holding_pnl is not None:
        parts4.append(f"H{_sgn(eth_holding_pnl, 0)}")
    if net_pnl is not None:
        parts4.append(f"Net{_sgn(net_pnl, 0)}")
    if float(pv) > 0:
        parts4.append(f"PV{int(float(pv))}")
    line4 = ", ".join(parts4)

    # Optional line5: show HODL/extra-ETH breakdown so the message reflects total bucket holdings.
    msg = f"{line1}\n{line2}\n{line_cyc}\n{line3}\n{line4}"
    if held_str:
        msg += f"\n{held_str}"
    if extra_eth > 0:
        bot_eth = float(broker_eth)
        msg += f"\nETH bot {bot_eth:.2f} + HODL {extra_eth:.2f} = {bot_eth + extra_eth:.2f} @ ${float(price):,.0f}"
    return msg, pv_snapshot


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
    ap.add_argument("--extra-eth",    type=float, default=0,
                    help="Untracked ETH held outside the bot's view (e.g., HODL stack). "
                         "Added to bot ETH for PV display so the message reflects total bucket holdings.")
    ap.add_argument("--since",        default=None,
                    help="Baseline cutoff: ignore all fills before this time. "
                         "Accepts 'YYYY-MM-DD HH:MM:SS' (assumed IST) or ISO-8601 with tz. "
                         "Scopes cycle/PnL accounting to trades on/after this instant.")
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

    # Baseline cutoff: drop fills before --since so all accounting starts from that instant.
    if args.since:
        try:
            _cut = datetime.datetime.fromisoformat(args.since)
        except ValueError:
            _cut = datetime.datetime.strptime(args.since, "%Y-%m-%d %H:%M:%S")
        if _cut.tzinfo is None:
            _cut = _cut.replace(tzinfo=IST)   # bare timestamps interpreted as IST
        _cut_utc = _cut.astimezone(UTC)
        def _ts(r):
            try:
                d = datetime.datetime.fromisoformat(r.get("ts", ""))
                return d if d.tzinfo else d.replace(tzinfo=UTC)
            except Exception:
                return None
        fills = [r for r in fills if (_ts(r) is not None and _ts(r) >= _cut_utc)]

    metrics = compute_metrics(fills, since, strategy)
    msg, pv_snapshot = build_message(metrics, since, now, args.hours, state_path, symbol,
                                     last_report_path=last_report_path,
                                     extra_eth=args.extra_eth)

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
