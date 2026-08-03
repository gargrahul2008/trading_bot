"""
Estimate Indian equity trading charges from executed trades (Fyers).

Intraday vs delivery is decided by SAME-DAY qty matching, NOT the order's productType
(we mostly place delivery/CNC orders, so productType is unreliable): per symbol per day,
  intraday_qty      = min(day_buy_qty, day_sell_qty)      # round-tripped same day
  delivery_buy_qty  = max(0, day_buy_qty  - day_sell_qty) # carried into holding
  delivery_sell_qty = max(0, day_sell_qty - day_buy_qty)  # sold from holding

Output is an ESTIMATE — always reconcile against the Fyers contract note. Rates change with
budgets/circulars; edit RATES below. All pct values are fractions (0.0003 == 0.03%).
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List

D0 = Decimal("0")


def _d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return D0


# ---- Editable rate table (Fyers equity; verify vs contract note) ----
RATES: Dict[str, Any] = {
    # ₹20 or pct per executed order, whichever is LOWER.
    "brokerage": {"intraday_pct": Decimal("0.0003"), "delivery_pct": Decimal("0.003"),
                  "cap": Decimal("20")},
    # STT on trade value. intraday: sell side only; delivery: both sides.
    "stt": {"intraday_sell": Decimal("0.00025"),
            "delivery_buy": Decimal("0.001"), "delivery_sell": Decimal("0.001")},
    # Exchange transaction charges on turnover (both sides), by exchange.
    "exchange_txn": {"NSE": Decimal("0.0000297"), "BSE": Decimal("0.0000375")},
    "sebi": Decimal("0.000001"),          # ₹10 per crore, both sides
    # Stamp duty, BUY side only.
    "stamp": {"intraday": Decimal("0.00003"), "delivery": Decimal("0.00015")},
    "gst": Decimal("0.18"),               # on (brokerage + exchange_txn + sebi)
    "dp_per_sell": Decimal("10"),         # per delivery-sell scrip/day (+ depository; approx)
    "mtf_interest_pa": Decimal("0.1249"), # MTF funding interest p.a. on the borrowed amount
}


def mtf_interest(eod_value_by_day: Dict[str, float], leverage: float, today: str) -> float:
    """MTF funding interest accrued per CALENDAR day the position is carried overnight.
    `eod_value_by_day`: {YYYY-MM-DD: position value at that day's last fill}. Days with no fill
    carry the previous day's value. Funded (borrowed) amount = value × (leverage−1)/leverage
    (assumes max leverage — an estimate). Rate from RATES['mtf_interest_pa']."""
    import datetime as _dt
    if leverage <= 1 or not eod_value_by_day:
        return 0.0
    rate = float(RATES["mtf_interest_pa"])
    borrow_frac = (leverage - 1.0) / leverage
    days = sorted(eod_value_by_day)
    d = _dt.date.fromisoformat(days[0])
    end = _dt.date.fromisoformat(today)
    last_val = 0.0
    total = 0.0
    while d <= end:
        ds = d.isoformat()
        if ds in eod_value_by_day:
            last_val = eod_value_by_day[ds]
        if last_val > 0:
            total += (last_val * borrow_frac) * rate / 365.0
        d += _dt.timedelta(days=1)
    return total


def _exchange_of(sym: str, fallback: str) -> str:
    s = str(sym or "")
    if s.upper().startswith("NSE:"):
        return "NSE"
    if s.upper().startswith("BSE:"):
        return "BSE"
    return (fallback or "NSE").upper()


def _brokerage(order_values: List[Decimal], delivery: bool) -> Decimal:
    """₹20-or-pct-whichever-lower, per executed order. Cap dominates for normal sizes, so the
    intraday/delivery distinction rarely moves it; we use the classified rate anyway."""
    pct = RATES["brokerage"]["delivery_pct"] if delivery else RATES["brokerage"]["intraday_pct"]
    cap = RATES["brokerage"]["cap"]
    return sum((min(cap, pct * v) for v in order_values), D0)


def compute_charges(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """`trades`: list of {symbol, side ('BUY'/'SELL'), qty, price, value, date (YYYY-MM-DD),
    exchange, order_id}. Returns per-symbol and total charge estimates."""
    # Group by (symbol, date); also collect distinct order values for brokerage.
    grp: Dict[tuple, Dict[str, Any]] = defaultdict(
        lambda: {"buy_qty": D0, "sell_qty": D0, "buy_val": D0, "sell_val": D0,
                 "orders": {}, "exch": "NSE"})
    for t in trades:
        sym = str(t.get("symbol") or "")
        date = str(t.get("date") or "")
        g = grp[(sym, date)]
        g["exch"] = _exchange_of(sym, t.get("exchange"))
        qty = _d(t.get("qty")); val = _d(t.get("value") or (_d(t.get("price")) * qty))
        if str(t.get("side")).upper() == "BUY":
            g["buy_qty"] += qty; g["buy_val"] += val
        else:
            g["sell_qty"] += qty; g["sell_val"] += val
        oid = str(t.get("order_id") or id(t))
        g["orders"][oid] = g["orders"].get(oid, D0) + val

    by_symbol: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for (sym, _date), g in grp.items():
        bq, sq, bv, sv = g["buy_qty"], g["sell_qty"], g["buy_val"], g["sell_val"]
        avg_b = (bv / bq) if bq > 0 else D0
        avg_s = (sv / sq) if sq > 0 else D0
        intr = min(bq, sq)
        d_buy, d_sell = max(D0, bq - sq), max(D0, sq - bq)
        txn_rate = RATES["exchange_txn"].get(g["exch"], RATES["exchange_txn"]["NSE"])

        # value of each leg
        intr_buy_v, intr_sell_v = intr * avg_b, intr * avg_s
        del_buy_v, del_sell_v = d_buy * avg_b, d_sell * avg_s

        stt = (RATES["stt"]["intraday_sell"] * intr_sell_v
               + RATES["stt"]["delivery_buy"] * del_buy_v
               + RATES["stt"]["delivery_sell"] * del_sell_v)
        stamp = (RATES["stamp"]["intraday"] * intr_buy_v
                 + RATES["stamp"]["delivery"] * del_buy_v)
        turnover = intr_buy_v + intr_sell_v + del_buy_v + del_sell_v
        txn = txn_rate * turnover
        sebi = RATES["sebi"] * turnover
        # Brokerage: per order; classify the whole symbol-day as delivery if any delivery qty.
        brok = _brokerage(list(g["orders"].values()), delivery=(d_buy > 0 or d_sell > 0))
        gst = RATES["gst"] * (brok + txn + sebi)
        dp = RATES["dp_per_sell"] if d_sell > 0 else D0

        total = stt + stamp + txn + sebi + brok + gst + dp
        r = by_symbol[sym]
        r["brokerage"] += float(brok); r["stt"] += float(stt); r["stamp"] += float(stamp)
        r["exchange_txn"] += float(txn); r["sebi"] += float(sebi); r["gst"] += float(gst)
        r["dp"] += float(dp); r["turnover"] += float(turnover); r["total_charges"] += float(total)

    totals = defaultdict(float)
    for r in by_symbol.values():
        for k, v in r.items():
            totals[k] += v
    return {"by_symbol": {k: dict(v) for k, v in by_symbol.items()},
            "totals": dict(totals), "note": "ESTIMATE — reconcile vs Fyers contract note"}
