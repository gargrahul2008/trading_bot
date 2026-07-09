"""Daily scanner: evaluate all universe stocks with clean pipeline and detailed status."""

import logging
import math
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from bottom_zone_grid.scanner.range_calculator import validate_range
from bottom_zone_grid.scanner.grid_calculator import evaluate_grid

LOG = logging.getLogger("bzg.scanner")


def is_breakdown_confirmed(df: pd.DataFrame, range_low: float, config: Dict) -> bool:
    buffer_pct = config.get("exit", {}).get("breakdown_buffer_pct", 2.0)
    confirm_days = config.get("exit", {}).get("breakdown_confirm_days", 2)
    breakdown_level = range_low * (1 - buffer_pct / 100)
    recent_closes = df["close"].tail(confirm_days).values
    return len(recent_closes) >= confirm_days and all(c < breakdown_level for c in recent_closes)


def is_breakout_confirmed(df: pd.DataFrame, range_high: float, config: Dict) -> bool:
    confirm_days = config.get("entry", {}).get("breakout_confirm_days", 2)
    recent_closes = df["close"].tail(confirm_days).values
    return len(recent_closes) >= confirm_days and all(c > range_high for c in recent_closes)


def is_strong_downtrend(df: pd.DataFrame, lookback: int = 20) -> bool:
    recent = df.tail(lookback)
    if len(recent) < lookback:
        return False
    closes = recent["close"].values
    sma = np.mean(closes)
    slope = (closes[-1] - closes[0]) / closes[0] * 100
    below_sma_pct = np.sum(closes < sma) / len(closes) * 100
    return slope < -8 and below_sma_pct > 80


def score_candidate(row: Dict) -> float:
    score = 0.0

    rp = row.get("range_position")
    if rp is not None and rp >= 0:
        score += max(0, (0.10 - rp) / 0.10) * 25

    score += min(row.get("support_touches", 0), 5) * 3
    score += min(row.get("resistance_touches", 0), 5) * 2
    score += min(row.get("containment_pct", 0), 95) / 95 * 15

    net = row.get("net_cycle_profit", 0)
    if net > 0:
        score += min(net / 200, 1.0) * 15

    g2b = row.get("grids_to_bottom", 99)
    if g2b <= 2:
        score += 10
    elif g2b <= 3:
        score += 5

    total_levels = row.get("total_grid_levels", 0)
    if total_levels > 0:
        upside = total_levels - g2b
        score += (upside / total_levels) * 10

    vol = row.get("avg_volume", 0)
    if vol > 5_000_000:
        score += 5
    elif vol > 1_000_000:
        score += 3

    width = row.get("range_width_pct", 0)
    if width > 20:
        score -= (width - 20) * 0.5

    return round(score, 2)


def evaluate_stock(
    sym_info: Dict,
    df: pd.DataFrame,
    cmp: float,
    config: Dict,
    bar_range_data: Optional[Dict[str, Optional[float]]] = None,
) -> Dict:
    """Evaluate one stock through the full pipeline. Returns a row dict."""

    rcfg = config.get("range", {})
    lower_zone_pct = rcfg.get("lower_zone_pct", 0.10)
    capital_per_slot = config.get("portfolio", {}).get("capital_per_slot", 1000000)

    row = {
        "symbol": sym_info.get("nse_symbol", ""),
        "fyers_symbol": sym_info.get("fyers_symbol", ""),
        "industry": sym_info.get("industry", ""),
        "cmp": round(cmp, 2),
        "avg_volume": sym_info.get("avg_volume", 0),
        "avg_turnover": sym_info.get("avg_turnover", 0),
        "data_days": sym_info.get("data_days", 0),
    }

    # Blank defaults for all output columns
    blank_range = {
        "range_low": 0, "range_high": 0, "range_width_pts": 0, "range_width_pct": 0,
        "support_touches": 0, "resistance_touches": 0, "containment_pct": 0,
        "range_valid": False, "range_watch": False,
    }
    blank_position = {
        "range_position": None, "in_bottom_zone": False,
        "below_range": False, "above_range": False,
        "breakdown": False, "breakout": False, "strong_downtrend": False,
    }
    blank_bar_range = {
        "avg_bar_range_15m": None, "avg_bar_range_5m": None,
    }
    blank_grid = {
        "grid_gap_pts": 0, "total_grid_levels": 0, "grids_to_bottom": 0,
        "order_qty": 0, "order_value": 0,
        "gross_cycle_profit": 0, "round_trip_cost": 0, "net_cycle_profit": 0,
    }
    blank_capital = {
        "allocated_capital": 0, "cash_reserve": 0,
        "max_downside_buy_value": 0,
        "initial_buy_value": 0,
        "initial_buy_qty": 0, "initial_buy_used_value": 0, "unused_cash": 0,
    }
    blank_status = {
        "entry_status": "", "grid_status": "NOT_EVALUATED",
        "final_status": "", "final_reason": "", "candidate_score": 0,
    }
    row.update(blank_range)
    row.update(blank_position)
    row.update(blank_bar_range)
    row.update(blank_grid)
    row.update(blank_capital)
    row.update(blank_status)

    # Fill bar range data early so it appears in all rows
    if bar_range_data:
        row["avg_bar_range_15m"] = bar_range_data.get("15")
        row["avg_bar_range_5m"] = bar_range_data.get("5")

    # STEP 3-5: Range validation
    range_info = validate_range(df, config)
    row["range_low"] = range_info.low
    row["range_high"] = range_info.high
    row["range_width_pts"] = range_info.width_points
    row["range_width_pct"] = range_info.width_pct
    row["support_touches"] = range_info.support_touches
    row["resistance_touches"] = range_info.resistance_touches
    row["containment_pct"] = range_info.containment_pct
    row["range_valid"] = range_info.valid
    row["range_watch"] = range_info.in_watch_zone

    if not range_info.valid and not range_info.in_watch_zone:
        row["entry_status"] = range_info.reject_status
        row["final_status"] = range_info.reject_status
        row["final_reason"] = range_info.reject_reason
        return row

    # Range is valid or in watch zone — compute position
    if range_info.width_points > 0:
        rp = (cmp - range_info.low) / range_info.width_points
        row["range_position"] = round(rp, 4)
    else:
        row["entry_status"] = "DATA_FAIL"
        row["final_status"] = "DATA_FAIL"
        row["final_reason"] = "zero range width"
        return row

    # STEP 6: Below-range / Above-range / Breakdown / Breakout
    row["below_range"] = cmp < range_info.low
    row["above_range"] = cmp > range_info.high

    if row["below_range"]:
        bd = is_breakdown_confirmed(df, range_info.low, config)
        row["breakdown"] = bd
        row["entry_status"] = "BREAKDOWN_FAIL" if bd else "BELOW_RANGE"
        row["final_status"] = row["entry_status"]
        row["final_reason"] = "confirmed breakdown" if bd else "CMP below range low"
        return row

    if row["above_range"]:
        bo = is_breakout_confirmed(df, range_info.high, config)
        row["breakout"] = bo
        row["entry_status"] = "BREAKOUT_FAIL" if bo else "ABOVE_RANGE"
        row["final_status"] = row["entry_status"]
        row["final_reason"] = "confirmed breakout" if bo else "CMP above range high"
        return row

    row["strong_downtrend"] = is_strong_downtrend(df)
    if row["strong_downtrend"]:
        row["entry_status"] = "STRONG_DOWNTREND_FAIL"
        row["final_status"] = "STRONG_DOWNTREND_FAIL"
        row["final_reason"] = "strong downtrend detected"
        return row

    # STEP 6b: RANGE_WATCH — track but don't trade
    if range_info.in_watch_zone and not range_info.valid:
        rp = row["range_position"]
        row["in_bottom_zone"] = (rp is not None and 0 <= rp <= lower_zone_pct)
        row["entry_status"] = "RANGE_WATCH"
        row["final_status"] = "RANGE_WATCH"
        row["final_reason"] = range_info.reject_reason
        row["candidate_score"] = score_candidate(row)
        return row

    # STEP 7: Bottom-zone check
    rp = row["range_position"]
    row["in_bottom_zone"] = (rp is not None and 0 <= rp <= lower_zone_pct)

    if not row["in_bottom_zone"]:
        row["entry_status"] = "NOT_IN_BOTTOM_ZONE"
        row["final_status"] = "NOT_IN_BOTTOM_ZONE"
        row["final_reason"] = f"range position {rp:.1%} > {lower_zone_pct:.0%}"
        row["candidate_score"] = score_candidate(row)
        return row

    # STEP 8-10: Grid evaluation
    row["entry_status"] = "ENTRY_VALID"

    grid_info = evaluate_grid(
        range_low=range_info.low,
        range_high=range_info.high,
        cmp=cmp,
        capital_per_slot=capital_per_slot,
        config=config,
        bar_range_data=bar_range_data,
    )

    row["grid_gap_pts"] = grid_info.grid_gap_points
    row["total_grid_levels"] = grid_info.total_levels
    row["grids_to_bottom"] = grid_info.grids_to_bottom
    row["order_qty"] = grid_info.order_qty
    row["order_value"] = grid_info.order_value
    row["gross_cycle_profit"] = grid_info.gross_cycle_profit
    row["round_trip_cost"] = grid_info.round_trip_cost
    row["net_cycle_profit"] = grid_info.net_cycle_profit

    row["avg_bar_range_15m"] = grid_info.avg_bar_range_15m
    row["avg_bar_range_5m"] = grid_info.avg_bar_range_5m

    row["allocated_capital"] = grid_info.allocated_capital
    row["cash_reserve"] = grid_info.cash_reserve
    row["max_downside_buy_value"] = grid_info.max_downside_buy_value
    row["initial_buy_value"] = grid_info.initial_buy_value
    row["initial_buy_qty"] = grid_info.initial_buy_qty
    row["initial_buy_used_value"] = grid_info.initial_buy_used_value
    row["unused_cash"] = grid_info.unused_cash

    row["grid_status"] = grid_info.grid_status

    # STEP 11: Final decision
    if grid_info.viable:
        row["final_status"] = "TRADE_READY"
        row["final_reason"] = (
            f"qty={grid_info.order_qty} gap={grid_info.grid_gap_points} "
            f"net=₹{grid_info.net_cycle_profit:.0f}"
        )
    else:
        row["final_status"] = grid_info.grid_status
        row["final_reason"] = grid_info.reject_reason or ""

    row["candidate_score"] = score_candidate(row)
    return row


def scan_all(
    universe: List[Dict],
    ohlcv_data: Dict[str, pd.DataFrame],
    ltp_prices: Dict[str, float],
    config: Dict,
    bar_range_all: Optional[Dict[str, Dict[str, Optional[float]]]] = None,
) -> List[Dict]:
    rows = []
    for sym_info in universe:
        fsym = sym_info["fyers_symbol"]
        df = ohlcv_data.get(fsym)
        if df is None or df.empty:
            continue

        cmp = ltp_prices.get(fsym) or sym_info.get("last_close")
        if not cmp or cmp <= 0:
            continue

        brd = bar_range_all.get(fsym) if bar_range_all else None
        row = evaluate_stock(sym_info, df, cmp, config, bar_range_data=brd)
        rows.append(row)

    rows.sort(key=lambda r: (
        0 if r["final_status"] == "TRADE_READY" else
        1 if r["final_status"] == "GRID_ALMOST_VIABLE" else
        2 if r["final_status"] == "RANGE_WATCH" else
        3,
        -(r.get("candidate_score") or 0),
        r.get("range_position") or 999,
    ))

    status_counts = {}
    for r in rows:
        st = r["final_status"]
        status_counts[st] = status_counts.get(st, 0) + 1
    LOG.info("Scanner: %d analyzed. %s", len(rows),
             ", ".join(f"{k}={v}" for k, v in sorted(status_counts.items(), key=lambda x: -x[1])))

    return rows


def split_results(rows: List[Dict]) -> Dict[str, List[Dict]]:
    trade_ready = []
    near_ready = []
    range_watch = []
    rejected = []

    near_statuses = {"GRID_ALMOST_VIABLE", "INSUFFICIENT_CAPITAL"}

    for r in rows:
        st = r["final_status"]
        if st == "TRADE_READY":
            trade_ready.append(r)
        elif st in near_statuses or st == "RANGE_WATCH":
            near_ready.append(r)
        else:
            rejected.append(r)

    return {
        "trade_ready": trade_ready,
        "near_ready": near_ready,
        "range_watch": [r for r in rows if r["final_status"] == "RANGE_WATCH"],
        "rejected": rejected,
    }


def build_scan_report(rows: List[Dict], config: Dict) -> Dict:
    status_counts = {}
    for r in rows:
        st = r["final_status"]
        status_counts[st] = status_counts.get(st, 0) + 1

    return {
        "scan_date": datetime.now().strftime("%Y-%m-%d"),
        "scan_time": datetime.now().strftime("%H:%M:%S"),
        "lookback_days": config.get("range", {}).get("lookback_days", 60),
        "total_analyzed": len(rows),
        "status_breakdown": status_counts,
        "capital_per_slot": config.get("portfolio", {}).get("capital_per_slot", 1000000),
    }
