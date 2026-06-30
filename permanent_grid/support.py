#!/usr/bin/env python3
"""
Support level detection for permanent grid strategy.

Finds strong support zones by identifying swing lows and clustering them.
Used to determine grid range bottom and pro-rata entry sizing.
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class SupportZone:
    price: float           # center price of the zone
    touches: int           # number of times price bounced from this zone
    min_price: float       # zone lower bound
    max_price: float       # zone upper bound
    strength: float        # composite score (touches × bounce quality)
    dates: List[str]       # dates of each touch


def find_swing_lows(df: pd.DataFrame, window: int = 10, min_bounce_pct: float = 3.0) -> List[dict]:
    """
    Find swing lows — local minima where price bounced up at least min_bounce_pct% afterward.

    Args:
        df: DataFrame with columns [date, open, high, low, close]
        window: look-back and look-forward window for local minimum detection
        min_bounce_pct: minimum % bounce from the low to qualify as a swing low

    Returns:
        List of {date, low_price, bounce_pct, bounce_high}
    """
    df = df.sort_values("date").reset_index(drop=True)
    lows = df["low"].values
    highs = df["high"].values
    dates = df["date"].values
    n = len(df)

    swing_lows = []

    for i in range(window, n - window):
        local_low = lows[i]

        # Check if this is a local minimum within the window
        left_min = lows[max(0, i - window):i].min()
        right_min = lows[i + 1:min(n, i + window + 1)].min()

        if local_low > left_min or local_low > right_min:
            continue

        # Check bounce: what's the highest price in the next `window` days?
        future_high = highs[i + 1:min(n, i + window * 2 + 1)].max()
        bounce_pct = (future_high / local_low - 1) * 100

        if bounce_pct >= min_bounce_pct:
            swing_lows.append({
                "date": str(dates[i]).split("T")[0].split(" ")[0],
                "low_price": float(local_low),
                "bounce_pct": round(bounce_pct, 2),
                "bounce_high": float(future_high),
                "idx": i,
            })

    return swing_lows


def cluster_supports(swing_lows: List[dict], cluster_pct: float = 3.0) -> List[SupportZone]:
    """
    Cluster swing lows within cluster_pct% of each other into support zones.

    Multiple touches at similar price levels = stronger support.
    """
    if not swing_lows:
        return []

    # Sort by price
    sorted_lows = sorted(swing_lows, key=lambda x: x["low_price"])

    zones = []
    used = set()

    for i, anchor in enumerate(sorted_lows):
        if i in used:
            continue

        cluster = [anchor]
        used.add(i)
        anchor_price = anchor["low_price"]

        for j, candidate in enumerate(sorted_lows):
            if j in used:
                continue
            pct_diff = abs(candidate["low_price"] / anchor_price - 1) * 100
            if pct_diff <= cluster_pct:
                cluster.append(candidate)
                used.add(j)

        prices = [s["low_price"] for s in cluster]
        bounces = [s["bounce_pct"] for s in cluster]
        dates = [s["date"] for s in cluster]

        zone = SupportZone(
            price=round(np.mean(prices), 2),
            touches=len(cluster),
            min_price=round(min(prices), 2),
            max_price=round(max(prices), 2),
            strength=round(len(cluster) * np.mean(bounces), 2),
            dates=dates,
        )
        zones.append(zone)

    # Sort by strength (strongest first)
    zones.sort(key=lambda z: z.strength, reverse=True)
    return zones


def find_nearest_support(zones: List[SupportZone], current_price: float,
                         max_distance_pct: float = 25.0) -> Optional[SupportZone]:
    """
    Find the strongest support zone below current price within max_distance_pct.
    """
    candidates = []
    for z in zones:
        if z.price < current_price:
            dist_pct = (current_price / z.price - 1) * 100
            if dist_pct <= max_distance_pct:
                candidates.append((z, dist_pct))

    if not candidates:
        return None

    # Return the strongest (already sorted by strength)
    return candidates[0][0]


def compute_grid_params(entry_price: float, support: SupportZone,
                        grid_pct: float = 0.01, total_levels: int = 25,
                        capital: float = 800_000) -> dict:
    """
    Compute pro-rata grid parameters based on entry price relative to support.

    Args:
        entry_price: current/entry price
        support: the support zone (range bottom)
        grid_pct: grid step percentage
        total_levels: total grid levels
        capital: capital allocated to this stock

    Returns:
        dict with grid setup parameters
    """
    support_price = support.price

    # Distance from support in terms of grid levels
    if entry_price <= support_price:
        # At or below support — fully loaded, all levels for upside
        levels_below = 0
    else:
        pct_above_support = (entry_price / support_price - 1)
        levels_below = min(int(pct_above_support / grid_pct), total_levels - 1)

    levels_above = total_levels - levels_below
    initial_inventory = levels_above  # lots to buy at entry for selling upward

    shares_per_level = capital / (entry_price * total_levels)

    # Grid range
    grid_bottom = entry_price * (1 - grid_pct * levels_below)
    grid_top = entry_price * (1 + grid_pct * levels_above)

    return {
        "entry_price": round(entry_price, 2),
        "support_price": support_price,
        "support_touches": support.touches,
        "support_strength": support.strength,
        "levels_below": levels_below,
        "levels_above": levels_above,
        "initial_inventory": initial_inventory,
        "shares_per_level": round(shares_per_level, 2),
        "grid_bottom": round(grid_bottom, 2),
        "grid_top": round(grid_top, 2),
        "downside_pct": round(levels_below * grid_pct * 100, 1),
        "upside_pct": round(levels_above * grid_pct * 100, 1),
        "capital_deployed_pct": round(initial_inventory / total_levels * 100, 1),
    }


def analyze_stock(df: pd.DataFrame, current_price: float = None,
                  swing_window: int = 10, min_bounce_pct: float = 3.0,
                  cluster_pct: float = 3.0) -> dict:
    """
    Full support analysis for a single stock.

    Returns:
        dict with swing_lows, support_zones, nearest_support, grid_params
    """
    if current_price is None:
        current_price = float(df.sort_values("date").iloc[-1]["close"])

    swing_lows = find_swing_lows(df, window=swing_window, min_bounce_pct=min_bounce_pct)
    zones = cluster_supports(swing_lows, cluster_pct=cluster_pct)
    nearest = find_nearest_support(zones, current_price)

    grid_params = None
    if nearest:
        grid_params = compute_grid_params(current_price, nearest)

    return {
        "current_price": current_price,
        "swing_lows": swing_lows,
        "support_zones": zones,
        "nearest_support": nearest,
        "grid_params": grid_params,
    }


def print_analysis(symbol: str, result: dict):
    """Pretty-print the support analysis for a stock."""
    print(f"\n{'='*60}")
    print(f"  {symbol}  CMP: {result['current_price']:,.2f}")
    print(f"{'='*60}")

    zones = result["support_zones"]
    if not zones:
        print("  No support zones found!")
        return

    print(f"\n  Support Zones (strongest first):")
    for i, z in enumerate(zones[:5]):
        print(f"    {i+1}. ₹{z.price:,.2f}  ({z.touches} touches, "
              f"strength={z.strength:.0f}, range={z.min_price:,.2f}-{z.max_price:,.2f})")
        print(f"       Dates: {', '.join(z.dates[:4])}"
              f"{'...' if len(z.dates) > 4 else ''}")

    nearest = result["nearest_support"]
    gp = result["grid_params"]
    if nearest and gp:
        dist = (result["current_price"] / nearest.price - 1) * 100
        print(f"\n  Nearest Support: ₹{nearest.price:,.2f} "
              f"({dist:.1f}% below CMP, {nearest.touches} touches)")
        print(f"\n  Pro-Rata Grid Setup:")
        print(f"    Entry:     ₹{gp['entry_price']:,.2f}")
        print(f"    Support:   ₹{gp['support_price']:,.2f} ({gp['support_touches']} touches)")
        print(f"    Levels:    {gp['levels_below']} below / {gp['levels_above']} above")
        print(f"    Range:     ₹{gp['grid_bottom']:,.2f} — ₹{gp['grid_top']:,.2f}")
        print(f"    Downside:  {gp['downside_pct']}%  |  Upside: {gp['upside_pct']}%")
        print(f"    Seed:      {gp['initial_inventory']} lots "
              f"({gp['capital_deployed_pct']}% of capital)")


# ── CLI: run support analysis on all basket stocks ──

if __name__ == "__main__":
    import pickle

    DATA_PATH = "/root/trading_bot/permanent_grid/data/basket_3y.pkl"
    with open(DATA_PATH, "rb") as f:
        data = pickle.load(f)

    basket = data["basket"]
    daily_data = data["daily_data"]

    print("SUPPORT ANALYSIS — Permanent Grid Basket")
    print(f"Data: {DATA_PATH}")

    all_results = {}
    for item in basket:
        sym = item["symbol"]
        fsym = item["fyers_symbol"]
        df = daily_data.get(fsym)
        if df is None:
            continue

        result = analyze_stock(df)
        all_results[sym] = result
        print_analysis(sym, result)

    # Summary table
    print(f"\n{'='*80}")
    print(f"  SUMMARY — Pro-Rata Grid Setup")
    print(f"{'='*80}")
    print(f"  {'Stock':12s} {'CMP':>8s} {'Support':>8s} {'Dist%':>6s} {'Touch':>5s} "
          f"{'Below':>5s} {'Above':>5s} {'Seed':>5s} {'Down%':>6s} {'Up%':>6s}")
    print(f"  {'-'*74}")

    for item in basket:
        sym = item["symbol"]
        r = all_results.get(sym)
        if not r or not r["grid_params"]:
            print(f"  {sym:12s} — no support found")
            continue
        gp = r["grid_params"]
        ns = r["nearest_support"]
        dist = (r["current_price"] / ns.price - 1) * 100
        print(f"  {sym:12s} {r['current_price']:>8,.0f} {ns.price:>8,.0f} {dist:>5.1f}% "
              f"{ns.touches:>5d} {gp['levels_below']:>5d} {gp['levels_above']:>5d} "
              f"{gp['initial_inventory']:>5d} {gp['downside_pct']:>5.1f}% {gp['upside_pct']:>5.1f}%")
