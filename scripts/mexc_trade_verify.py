#!/usr/bin/env python3
"""
mexc_trade_verify.py — Per-trade portfolio verification table.

Starts from a known opening balance (last trade of a given date in IST) and
produces one CSV row per trade showing:

    pv_delta = Q_before × price_change   (always, by algebra)

The `check` column should always be ~0 for normal trades.
A non-zero check flags a cash/ETH accounting discrepancy.
Capital flows (deposits/withdrawals) appear as DEPOSIT/WITHDRAW rows
where check = deposit amount (expected — not a bug).

Modes:
  First run (CSV does not exist):
    Replays all trades from --opening-date and writes the full CSV.

  Subsequent runs (CSV already exists) — APPEND MODE:
    Reads the last row for current state (Q, A, cash, price, pv, ts).
    Only appends rows for trades newer than that timestamp.
    Safe to run every minute via cron — no duplicates.

Columns:
    ts_ist, event, reason, qty, price, price_change,
    Q_before, Q_after, avg_cost,
    cash_before, cash_after, trade_pnl,
    pv, pv_delta, expected_delta, check,
    is_cycle

Usage:
    python3 scripts/mexc_trade_verify.py \\
        --trades  strategies/pct_ladder/state/mexc_trades.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_02.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_03.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_03_v1.jsonl \\
                  strategies/pct_ladder/state/mexc_trades_2026_03_05_v1.jsonl \\
        --manual  strategies/pct_ladder/state/manual_positions_2026_03_05_v1.json \\
        --capital strategies/pct_ladder/state/capital_flows_2026_03_05_v1.json \\
        --opening-date 2026-03-18 \\
        --out     strategies/pct_ladder/state/mexc_trade_verify.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = datetime.timezone.utc
D0  = Decimal("0")

HEADERS = [
    "ts_ist",
    "event",
    "reason",
    "qty",
    "price",
    "price_change",
    "Q_before",
    "Q_after",
    "avg_cost",
    "cash_before",
    "cash_after",
    "trade_pnl",
    "pv",
    "pv_delta",
    "expected_delta",
    "check",
    "is_cycle",
    "cycle_stock_pnl",
    "cycle_bot_pnl",
    "cycle_formula_pnl",
    "cycle_retained_eth",
    "cum_retained_eth",
    "cum_retained_eth_value",
    "strat_net_usdt",
    "strat_net_eth",
    "strat_pnl",
    "cum_stock_pnl",
    "cum_bot_pnl",
    "net_pnl",
    "unrealized_pnl",
    "order_id",
]


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


def _r(x, places: int = 2) -> str:
    try:
        q = Decimal("0." + "0" * places)
        return str(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))
    except Exception:
        return str(x)


def _parse_ts(s: str) -> datetime.datetime:
    try:
        dt = datetime.datetime.fromisoformat(s)
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    except Exception:
        return datetime.datetime.fromtimestamp(0, tz=UTC)


def _fmt_ist(dt: datetime.datetime) -> str:
    return dt.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S IST")


def _is_rebalance(reason: str) -> bool:
    return bool(re.search(r'rebalance|rebal', reason, re.IGNORECASE))


def load_all_events(trades_paths: list[str],
                    manual_path: str | None,
                    capital_path: str | None) -> list[tuple]:
    """
    Returns sorted list of (ts_utc, event_type, qty, price, reason, order_id).
    event_type: BUY / SELL / DEPOSIT / WITHDRAW
    order_id is "" for manual positions and capital flows.
    """
    events: list[tuple] = []

    # Manual ETH positions → MANUAL_BUY events (clearly marked, same accounting as BUY)
    if manual_path:
        try:
            for m in json.load(open(manual_path, encoding="utf-8")):
                qty   = _dec(m.get("qty", 0))
                price = _dec(m.get("buy_price", 0))
                if qty <= D0 or price <= D0:
                    continue
                ts = _parse_ts(str(m.get("ts", "")))
                events.append((ts, "MANUAL_BUY", qty, price, "manual_eth_purchase", ""))
        except FileNotFoundError:
            pass

    # Capital flows → DEPOSIT / WITHDRAW events
    if capital_path:
        try:
            for f in json.load(open(capital_path, encoding="utf-8")):
                delta = _dec(f.get("delta", 0))
                if delta == D0:
                    continue
                ts     = _parse_ts(str(f.get("ts", "")))
                etype  = "DEPOSIT" if delta > D0 else "WITHDRAW"
                events.append((ts, etype, abs(delta), D0, f.get("note", ""), ""))
        except FileNotFoundError:
            pass

    # Bot fills
    seen: set[str] = set()
    for path in trades_paths:
        try:
            for line in open(path, encoding="utf-8"):
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
                qty = _dec(r.get("qty") or "0")
                if qty <= D0:
                    continue
                ts     = _parse_ts(r.get("ts") or "")
                side   = str(r.get("side") or "").upper()
                price  = _dec(r.get("price") or "0")
                reason = str(r.get("reason") or "")
                events.append((ts, side, qty, price, reason, oid))
        except FileNotFoundError:
            pass

    events.sort(key=lambda e: e[0])
    return events


def replay_to(events: list[tuple],
              cutoff: datetime.datetime) -> tuple[Decimal, Decimal, Decimal, datetime.datetime, Decimal]:
    """
    Replay all events up to and INCLUDING cutoff.
    Returns (Q, A, cash, last_ts, last_price).
    """
    Q = D0; A = D0; cash = D0
    last_ts    = cutoff
    last_price = D0

    for ts, etype, qty, price, _reason, _oid in events:
        if ts > cutoff:
            break
        if etype in ("DEPOSIT", "WITHDRAW"):
            cash += qty if etype == "DEPOSIT" else -qty
        elif etype in ("BUY", "MANUAL_BUY"):
            new_Q = Q + qty
            A     = (Q * A + qty * price) / new_Q if new_Q > D0 else price
            Q     = new_Q
            cash -= qty * price
            last_ts    = ts
            last_price = price
        elif etype == "SELL":
            Q     -= qty
            cash  += qty * price
            last_ts    = ts
            last_price = price

    return Q, A, cash, last_ts, last_price


def build_rows(events: list[tuple],
               after: datetime.datetime,
               open_Q: Decimal, open_A: Decimal,
               open_cash: Decimal, open_ts: datetime.datetime,
               open_price: Decimal,
               pct: Decimal = Decimal("0.004"),
               strategy_start: datetime.datetime | None = None) -> list[dict]:  # noqa: C901

    rows: list[dict] = []

    # Opening row
    open_pv = open_Q * open_price + open_cash
    rows.append({
        "ts_ist":             _fmt_ist(open_ts),
        "event":              "OPEN",
        "reason":             "opening balance",
        "qty":                "",
        "price":              _r(open_price, 2),
        "price_change":       "0.00",
        "Q_before":           "",
        "Q_after":            _r(open_Q, 5),
        "avg_cost":           _r(open_A, 2),
        "cash_before":        "",
        "cash_after":         _r(open_cash, 2),
        "trade_pnl":          "0.00",
        "pv":                 _r(open_pv, 2),
        "pv_delta":           "0.00",
        "expected_delta":     "0.00",
        "check":              "0.00",
        "is_cycle":               "0",
        "cycle_stock_pnl":        "",
        "cycle_bot_pnl":          "",
        "cycle_formula_pnl":      "",
        "cycle_retained_eth":     "",
        "cum_retained_eth":       "0.000000",
        "cum_retained_eth_value": "0.00",
        "strat_net_usdt":         "0.00",
        "strat_net_eth":          "0.000000",
        "strat_pnl":              "0.00",
        "cum_stock_pnl":          "0.00",
        "cum_bot_pnl":            "0.00",
        "net_pnl":                "0.00",
        "unrealized_pnl":         _r(open_Q * (open_price - open_A), 2),
        "order_id":               "",
    })

    Q             = open_Q
    A             = open_A
    cash          = open_cash
    prev_pv       = open_pv
    prev_price    = open_price
    cum_ret_eth   = D0
    cum_stock     = D0
    cum_bot       = D0
    last_buy_qty  = D0
    strat_net_usdt = D0
    strat_net_eth  = D0

    for ts, etype, qty, price, reason, oid in events:
        if ts <= after:
            continue

        Q_before    = Q
        A_before    = A
        cash_before = cash
        trade_pnl   = D0
        is_cycle    = 0

        if etype in ("BUY", "MANUAL_BUY"):
            if etype == "BUY" and not _is_rebalance(reason):
                last_buy_qty = qty
            new_Q = Q + qty
            A     = (Q * A + qty * price) / new_Q if new_Q > D0 else price
            Q     = new_Q
            cash -= qty * price
            if etype == "BUY" and (strategy_start is None or ts >= strategy_start):
                strat_net_usdt -= qty * price
                strat_net_eth  += qty

        elif etype == "SELL":
            trade_pnl = (price - A_before) * qty
            Q        -= qty
            cash     += qty * price
            if not _is_rebalance(reason):
                is_cycle = 1
            if strategy_start is None or ts >= strategy_start:
                strat_net_usdt += qty * price
                strat_net_eth  -= qty

        elif etype in ("DEPOSIT", "WITHDRAW"):
            cash_delta = qty if etype == "DEPOSIT" else -qty
            cash += cash_delta
            pv        = Q * prev_price + cash
            pv_delta  = pv - prev_pv
            rows.append({
                "ts_ist":             _fmt_ist(ts),
                "event":              etype,
                "reason":             reason,
                "qty":                _r(qty, 2),
                "price":              _r(prev_price, 2),
                "price_change":       "0.00",
                "Q_before":           _r(Q_before, 5),
                "Q_after":            _r(Q, 5),
                "avg_cost":           _r(A, 2),
                "cash_before":        _r(cash_before, 2),
                "cash_after":         _r(cash, 2),
                "trade_pnl":          "0.00",
                "pv":                 _r(pv, 2),
                "pv_delta":           _r(pv_delta, 2),
                "expected_delta":     "0.00",
                "check":              _r(pv_delta, 2),
                "is_cycle":               "0",
                "cycle_stock_pnl":        "",
                "cycle_bot_pnl":          "",
                "cycle_formula_pnl":      "",
                "cycle_retained_eth":     "",
                "cum_retained_eth":       _r(cum_ret_eth, 6),
                "cum_retained_eth_value": _r(cum_ret_eth * prev_price, 2),
                "strat_net_usdt":         _r(strat_net_usdt, 2),
                "strat_net_eth":          _r(strat_net_eth, 6),
                "strat_pnl":              _r(strat_net_usdt + strat_net_eth * prev_price, 2),
                "cum_stock_pnl":          _r(cum_stock, 2),
                "cum_bot_pnl":            _r(cum_bot, 2),
                "net_pnl":                _r(cum_stock + cum_bot, 2),
                "unrealized_pnl":         _r(Q * (prev_price - A), 2),
                "order_id":               "",
            })
            prev_pv = pv
            continue

        # Trade row (BUY or SELL)
        price_change = price - prev_price
        pv           = Q * price + cash
        pv_delta     = pv - prev_pv
        exp_delta    = Q_before * price_change
        check_val    = pv_delta - exp_delta

        # Attribution columns — stock_pnl for every trade, bot/formula only for sells.
        # SELL: stock = Q_after × price_change,  bot = qty × price_change ≈ formula
        # BUY:  stock = Q_before × price_change, bot/formula = "" (pv_delta = Q_before × price_change exactly)
        c_stock = c_bot = c_formula = c_ret_eth = ""
        stock_dec = bot_dec = D0
        if etype == "SELL":
            bot_pnl_dec = pv - prev_pv - Q * price_change
            stock_dec   = Q * price_change
            bot_dec     = bot_pnl_dec
            c_stock     = _r(stock_dec, 2)
            c_bot       = _r(bot_dec, 2)
            if is_cycle:
                c_formula   = _r(qty * price * pct, 2)
                ret_eth_dec = last_buy_qty - qty
                c_ret_eth   = _r(ret_eth_dec, 6)
                cum_ret_eth += ret_eth_dec
            else:
                cum_ret_eth -= qty
        elif etype in ("BUY", "MANUAL_BUY"):
            stock_dec   = Q_before * price_change
            c_stock     = _r(stock_dec, 2)
            if etype == "BUY" and _is_rebalance(reason):
                cum_ret_eth += qty
        cum_stock += stock_dec
        cum_bot   += bot_dec

        rows.append({
            "ts_ist":                 _fmt_ist(ts),
            "event":                  etype,
            "reason":                 reason,
            "qty":                    _r(qty, 5),
            "price":                  _r(price, 2),
            "price_change":           _r(price_change, 2),
            "Q_before":               _r(Q_before, 5),
            "Q_after":                _r(Q, 5),
            "avg_cost":               _r(A, 2),
            "cash_before":            _r(cash_before, 2),
            "cash_after":             _r(cash, 2),
            "trade_pnl":              _r(trade_pnl, 2),
            "pv":                     _r(pv, 2),
            "pv_delta":               _r(pv_delta, 2),
            "expected_delta":         _r(exp_delta, 2),
            "check":                  _r(check_val, 2),
            "is_cycle":               str(is_cycle),
            "cycle_stock_pnl":        c_stock,
            "cycle_bot_pnl":          c_bot,
            "cycle_formula_pnl":      c_formula,
            "cycle_retained_eth":     c_ret_eth,
            "cum_retained_eth":       _r(cum_ret_eth, 6),
            "cum_retained_eth_value": _r(cum_ret_eth * price, 2),
            "strat_net_usdt":         _r(strat_net_usdt, 2),
            "strat_net_eth":          _r(strat_net_eth, 6),
            "strat_pnl":              _r(strat_net_usdt + strat_net_eth * price, 2),
            "cum_stock_pnl":          _r(cum_stock, 2),
            "cum_bot_pnl":            _r(cum_bot, 2),
            "net_pnl":                _r(cum_stock + cum_bot, 2),
            "unrealized_pnl":         _r(Q * (price - A), 2),
            "order_id":               oid,
        })

        prev_pv    = pv
        prev_price = price

    return rows


def get_last_state(csv_path: str) -> tuple | None:
    """
    Read existing CSV to resume from.
    Returns (last_ts_utc, Q, A, cash, price, pv, seen_order_ids,
             cum_ret_eth, cum_stock, cum_bot, strat_net_usdt, strat_net_eth) or None.
    seen_order_ids: set of all order_ids already written (for deduplication).
    """
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return None
    try:
        last_row     = None
        seen_oids: set[str] = set()
        with open(csv_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                last_row = row
                oid = row.get("order_id", "").strip()
                if oid:
                    seen_oids.add(oid)
        if last_row is None:
            return None
        ts_str = last_row["ts_ist"].replace(" IST", "+05:30")
        ts_utc = datetime.datetime.fromisoformat(ts_str).astimezone(UTC)
        Q     = _dec(last_row.get("Q_after")    or "0")
        A     = _dec(last_row.get("avg_cost")   or "0")
        cash  = _dec(last_row.get("cash_after") or "0")
        price        = _dec(last_row.get("price")             or "0")
        pv           = _dec(last_row.get("pv")                or "0")
        cum_ret_eth  = _dec(last_row.get("cum_retained_eth")  or "0")
        cum_stock    = _dec(last_row.get("cum_stock_pnl")     or "0")
        cum_bot      = _dec(last_row.get("cum_bot_pnl")       or "0")
        strat_net_usdt = _dec(last_row.get("strat_net_usdt")  or "0")
        strat_net_eth  = _dec(last_row.get("strat_net_eth")   or "0")
        return ts_utc, Q, A, cash, price, pv, seen_oids, cum_ret_eth, cum_stock, cum_bot, strat_net_usdt, strat_net_eth
    except Exception:
        return None


def append_rows(csv_path: str, events: list[tuple],
                last_ts: datetime.datetime, Q: Decimal, A: Decimal,
                cash: Decimal, prev_price: Decimal, prev_pv: Decimal,
                seen_oids: set[str],
                pct: Decimal = Decimal("0.004"),
                cum_ret_eth: Decimal = D0,
                cum_stock: Decimal = D0,
                cum_bot: Decimal = D0,
                strat_net_usdt: Decimal = D0,
                strat_net_eth: Decimal = D0,
                strategy_start: datetime.datetime | None = None) -> int:
    """
    Process events strictly after last_ts and append new rows to csv_path.
    Returns count of rows appended.
    """
    new_rows: list[dict] = []
    last_buy_qty = D0

    for ts, etype, qty, price, reason, oid in events:
        # Skip if already written:
        #   - order_id already in CSV (handles same-second microsecond edge cases)
        #   - OR timestamp at/before last row (handles old historical trades not in CSV)
        if oid and oid in seen_oids:
            continue
        if ts <= last_ts:
            continue

        Q_before    = Q
        A_before    = A
        cash_before = cash
        trade_pnl   = D0
        is_cycle    = 0

        if etype in ("BUY", "MANUAL_BUY"):
            if etype == "BUY" and not _is_rebalance(reason):
                last_buy_qty = qty
            new_Q = Q + qty
            A     = (Q * A + qty * price) / new_Q if new_Q > D0 else price
            Q     = new_Q
            cash -= qty * price
            if etype == "BUY" and (strategy_start is None or ts >= strategy_start):
                strat_net_usdt -= qty * price
                strat_net_eth  += qty

        elif etype == "SELL":
            trade_pnl = (price - A_before) * qty
            Q        -= qty
            cash     += qty * price
            if not _is_rebalance(reason):
                is_cycle = 1
            if strategy_start is None or ts >= strategy_start:
                strat_net_usdt += qty * price
                strat_net_eth  -= qty

        elif etype in ("DEPOSIT", "WITHDRAW"):
            cash_delta = qty if etype == "DEPOSIT" else -qty
            cash += cash_delta
            pv        = Q * prev_price + cash
            pv_delta  = pv - prev_pv
            new_rows.append({
                "ts_ist":             _fmt_ist(ts),
                "event":              etype,
                "reason":             reason,
                "qty":                _r(qty, 2),
                "price":              _r(prev_price, 2),
                "price_change":       "0.00",
                "Q_before":           _r(Q_before, 5),
                "Q_after":            _r(Q, 5),
                "avg_cost":           _r(A, 2),
                "cash_before":        _r(cash_before, 2),
                "cash_after":         _r(cash, 2),
                "trade_pnl":          "0.00",
                "pv":                 _r(pv, 2),
                "pv_delta":           _r(pv_delta, 2),
                "expected_delta":     "0.00",
                "check":              _r(pv_delta, 2),
                "is_cycle":               "0",
                "cycle_stock_pnl":        "",
                "cycle_bot_pnl":          "",
                "cycle_formula_pnl":      "",
                "cycle_retained_eth":     "",
                "cum_retained_eth":       _r(cum_ret_eth, 6),
                "cum_retained_eth_value": _r(cum_ret_eth * prev_price, 2),
                "strat_net_usdt":         _r(strat_net_usdt, 2),
                "strat_net_eth":          _r(strat_net_eth, 6),
                "strat_pnl":              _r(strat_net_usdt + strat_net_eth * prev_price, 2),
                "cum_stock_pnl":          _r(cum_stock, 2),
                "cum_bot_pnl":            _r(cum_bot, 2),
                "net_pnl":                _r(cum_stock + cum_bot, 2),
                "unrealized_pnl":         _r(Q * (prev_price - A), 2),
                "order_id":               "",
            })
            prev_pv = pv
            if oid:
                seen_oids.add(oid)
            continue

        price_change = price - prev_price
        pv           = Q * price + cash
        pv_delta     = pv - prev_pv
        exp_delta    = Q_before * price_change
        check_val    = pv_delta - exp_delta

        c_stock = c_bot = c_formula = c_ret_eth = ""
        stock_dec = bot_dec = D0
        if etype == "SELL":
            bot_pnl_dec = pv - prev_pv - Q * (price - prev_price)
            stock_dec   = Q * (price - prev_price)
            bot_dec     = bot_pnl_dec
            c_stock     = _r(stock_dec, 2)
            c_bot       = _r(bot_dec, 2)
            if is_cycle:
                c_formula   = _r(qty * price * pct, 2)
                ret_eth_dec = last_buy_qty - qty
                c_ret_eth   = _r(ret_eth_dec, 6)
                cum_ret_eth += ret_eth_dec
            else:
                cum_ret_eth -= qty
        elif etype in ("BUY", "MANUAL_BUY"):
            stock_dec = Q_before * (price - prev_price)
            c_stock   = _r(stock_dec, 2)
            if etype == "BUY" and _is_rebalance(reason):
                cum_ret_eth += qty
        cum_stock += stock_dec
        cum_bot   += bot_dec

        new_rows.append({
            "ts_ist":                 _fmt_ist(ts),
            "event":                  etype,
            "reason":                 reason,
            "qty":                    _r(qty, 5),
            "price":                  _r(price, 2),
            "price_change":           _r(price_change, 2),
            "Q_before":               _r(Q_before, 5),
            "Q_after":                _r(Q, 5),
            "avg_cost":               _r(A, 2),
            "cash_before":            _r(cash_before, 2),
            "cash_after":             _r(cash, 2),
            "trade_pnl":              _r(trade_pnl, 2),
            "pv":                     _r(pv, 2),
            "pv_delta":               _r(pv_delta, 2),
            "expected_delta":         _r(exp_delta, 2),
            "check":                  _r(check_val, 2),
            "is_cycle":               str(is_cycle),
            "cycle_stock_pnl":        c_stock,
            "cycle_bot_pnl":          c_bot,
            "cycle_formula_pnl":      c_formula,
            "cycle_retained_eth":     c_ret_eth,
            "cum_retained_eth":       _r(cum_ret_eth, 6),
            "cum_retained_eth_value": _r(cum_ret_eth * price, 2),
            "strat_net_usdt":         _r(strat_net_usdt, 2),
            "strat_net_eth":          _r(strat_net_eth, 6),
            "strat_pnl":              _r(strat_net_usdt + strat_net_eth * price, 2),
            "cum_stock_pnl":          _r(cum_stock, 2),
            "cum_bot_pnl":            _r(cum_bot, 2),
            "net_pnl":                _r(cum_stock + cum_bot, 2),
            "unrealized_pnl":         _r(Q * (price - A), 2),
            "order_id":               oid,
        })

        prev_pv    = pv
        prev_price = price
        if oid:
            seen_oids.add(oid)

    if new_rows:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=HEADERS).writerows(new_rows)

    return len(new_rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades",       required=True, nargs="+")
    ap.add_argument("--manual",       default=None,  help="manual_positions JSON")
    ap.add_argument("--capital",      default=None,  help="capital_flows JSON")
    ap.add_argument("--opening-date", default=None,
                    help="Use last trade of this IST date as opening balance (YYYY-MM-DD). "
                         "Omit or use --from-start to start from the very first event.")
    ap.add_argument("--from-start",   action="store_true",
                    help="Start from the very first event (Q=0, cash=0). "
                         "Overrides --opening-date.")
    ap.add_argument("--pct",          type=float, default=0.004,
                    help="Ladder step %% as decimal (default 0.004 = 0.4%%). "
                         "Used for cycle_formula_pnl = qty × price × pct.")
    ap.add_argument("--strategy-start", default="2026-03-05",
                    help="Date from which strategy P&L is tracked (YYYY-MM-DD)")
    ap.add_argument("--out",          required=True, help="Output CSV path")
    args = ap.parse_args()
    pct = Decimal(str(args.pct))
    strategy_start_dt = datetime.datetime.strptime(args.strategy_start, '%Y-%m-%d').replace(tzinfo=UTC)

    events = load_all_events(args.trades, args.manual, args.capital)

    # ── APPEND MODE: CSV already exists ──────────────────────────────────────
    last_state = get_last_state(args.out)
    if last_state is not None:
        last_ts, Q, A, cash, prev_price, prev_pv, seen_oids, cum_ret_eth, cum_stock, cum_bot, strat_net_usdt, strat_net_eth = last_state
        n = append_rows(args.out, events, last_ts, Q, A, cash, prev_price, prev_pv, seen_oids, pct,
                        cum_ret_eth, cum_stock, cum_bot,
                        strat_net_usdt=strat_net_usdt, strat_net_eth=strat_net_eth,
                        strategy_start=strategy_start_dt)
        if n:
            print(f"Appended {n} new row(s) → {args.out}")
        return

    # ── FULL BUILD: first run ─────────────────────────────────────────────────
    if args.from_start or not args.opening_date:
        # Start from Q=0, cash=0 — process every event from the very first
        open_Q     = D0
        open_A     = D0
        open_cash  = D0
        open_price = D0
        epoch      = datetime.datetime.fromtimestamp(0, tz=UTC)
        open_ts    = events[0][0] if events else epoch   # first event timestamp
        after      = epoch
        print("Opening state: beginning of trading (Q=0, cash=0)")
    else:
        od     = datetime.date.fromisoformat(args.opening_date)
        cutoff = datetime.datetime(od.year, od.month, od.day, 23, 59, 59, tzinfo=IST).astimezone(UTC)
        open_Q, open_A, open_cash, open_ts, open_price = replay_to(events, cutoff)
        after  = cutoff
        print(f"Opening state (end of {args.opening_date} IST):")
        print(f"  Q     = {float(open_Q):.5f} ETH")
        print(f"  A     = {float(open_A):.4f}  (avg cost)")
        print(f"  cash  = {float(open_cash):.4f} USDC")
        print(f"  price = {float(open_price):.4f}  (last trade price)")
        print(f"  PV    = {float(open_Q * open_price + open_cash):.2f}")

    print()
    rows = build_rows(events, after, open_Q, open_A, open_cash, open_ts, open_price, pct,
                      strategy_start=strategy_start_dt)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        w.writerows(rows)

    trades_rows   = [r for r in rows[1:] if r["event"] in ("BUY", "SELL", "MANUAL_BUY")]
    cycle_rows    = [r for r in rows[1:] if r["is_cycle"] == "1"]
    manual_rows   = [r for r in rows[1:] if r["event"] == "MANUAL_BUY"]
    deposit_rows  = [r for r in rows[1:] if r["event"] in ("DEPOSIT", "WITHDRAW")]
    bad_check     = [r for r in rows[1:] if abs(float(r["check"])) > 0.05
                     and r["event"] not in ("DEPOSIT", "WITHDRAW", "MANUAL_BUY")]

    print(f"Written {len(rows)} rows → {args.out}")
    print(f"  {len(trades_rows)} trades  |  {len(cycle_rows)} cycles  |  "
          f"{len(manual_rows)} manual ETH buys  |  {len(deposit_rows)} cash flows")
    if bad_check:
        print(f"WARNING: {len(bad_check)} rows with |check| > 0.05")
        for r in bad_check[:5]:
            print(f"  {r['ts_ist']}  {r['event']}  check={r['check']}")
    else:
        print("Check column: all rows ≈ 0 ✓")


if __name__ == "__main__":
    main()
