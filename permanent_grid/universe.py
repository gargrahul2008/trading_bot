#!/usr/bin/env python3
"""
Nifty50 universe: master stock list, index change history, and composition lookup.

Provides the full timeline of which stocks were in Nifty50 at any date from 2019 onward.
Used by both the basket selector and the backtest engine.
"""

from datetime import datetime


# Every stock that was in Nifty50 at any point from Jan 2019 to Dec 2025.
# Keyed by NSE trading symbol.

MASTER_STOCKS = {
    # ── Current Nifty50 members (Dec 2025) ──
    "ADANIENT":    {"fyers": "NSE:ADANIENT-EQ",    "sector": "Metals & Mining"},
    "ADANIPORTS":  {"fyers": "NSE:ADANIPORTS-EQ",  "sector": "Services"},
    "APOLLOHOSP":  {"fyers": "NSE:APOLLOHOSP-EQ",  "sector": "Healthcare"},
    "ASIANPAINT":  {"fyers": "NSE:ASIANPAINT-EQ",  "sector": "Consumer Durables"},
    "AXISBANK":    {"fyers": "NSE:AXISBANK-EQ",    "sector": "Financial Services"},
    "BAJAJ-AUTO":  {"fyers": "NSE:BAJAJ-AUTO-EQ",  "sector": "Automobile"},
    "BAJAJFINSV":  {"fyers": "NSE:BAJAJFINSV-EQ",  "sector": "Financial Services"},
    "BAJFINANCE":  {"fyers": "NSE:BAJFINANCE-EQ",  "sector": "Financial Services"},
    "BEL":         {"fyers": "NSE:BEL-EQ",         "sector": "Capital Goods"},
    "BHARTIARTL":  {"fyers": "NSE:BHARTIARTL-EQ",  "sector": "Telecommunication"},
    "CIPLA":       {"fyers": "NSE:CIPLA-EQ",       "sector": "Healthcare"},
    "COALINDIA":   {"fyers": "NSE:COALINDIA-EQ",   "sector": "Oil Gas & Fuels"},
    "DIVISLAB":    {"fyers": "NSE:DIVISLAB-EQ",     "sector": "Healthcare"},
    "DRREDDY":     {"fyers": "NSE:DRREDDY-EQ",     "sector": "Healthcare"},
    "EICHERMOT":   {"fyers": "NSE:EICHERMOT-EQ",   "sector": "Automobile"},
    "ETERNAL":     {"fyers": "NSE:ETERNAL-EQ",     "sector": "Consumer Services"},
    "GRASIM":      {"fyers": "NSE:GRASIM-EQ",      "sector": "Construction Materials"},
    "HCLTECH":     {"fyers": "NSE:HCLTECH-EQ",     "sector": "Information Technology"},
    "HDFCBANK":    {"fyers": "NSE:HDFCBANK-EQ",    "sector": "Financial Services"},
    "HDFCLIFE":    {"fyers": "NSE:HDFCLIFE-EQ",    "sector": "Financial Services"},
    "HEROMOTOCO":  {"fyers": "NSE:HEROMOTOCO-EQ",  "sector": "Automobile"},
    "HINDALCO":    {"fyers": "NSE:HINDALCO-EQ",    "sector": "Metals & Mining"},
    "HINDUNILVR":  {"fyers": "NSE:HINDUNILVR-EQ",  "sector": "FMCG"},
    "ICICIBANK":   {"fyers": "NSE:ICICIBANK-EQ",   "sector": "Financial Services"},
    "INDIGO":      {"fyers": "NSE:INDIGO-EQ",      "sector": "Services"},
    "INDUSINDBK":  {"fyers": "NSE:INDUSINDBK-EQ",  "sector": "Financial Services"},
    "INFY":        {"fyers": "NSE:INFY-EQ",        "sector": "Information Technology"},
    "ITC":         {"fyers": "NSE:ITC-EQ",         "sector": "FMCG"},
    "JIOFIN":      {"fyers": "NSE:JIOFIN-EQ",      "sector": "Financial Services"},
    "JSWSTEEL":    {"fyers": "NSE:JSWSTEEL-EQ",    "sector": "Metals & Mining"},
    "KOTAKBANK":   {"fyers": "NSE:KOTAKBANK-EQ",   "sector": "Financial Services"},
    "LT":          {"fyers": "NSE:LT-EQ",          "sector": "Construction"},
    "LTIM":        {"fyers": "NSE:LTIM-EQ",        "sector": "Information Technology"},
    "M&M":         {"fyers": "NSE:M&M-EQ",         "sector": "Automobile"},
    "MARUTI":      {"fyers": "NSE:MARUTI-EQ",      "sector": "Automobile"},
    "MAXHEALTH":   {"fyers": "NSE:MAXHEALTH-EQ",   "sector": "Healthcare"},
    "NESTLEIND":   {"fyers": "NSE:NESTLEIND-EQ",   "sector": "FMCG"},
    "NTPC":        {"fyers": "NSE:NTPC-EQ",        "sector": "Power"},
    "ONGC":        {"fyers": "NSE:ONGC-EQ",        "sector": "Oil Gas & Fuels"},
    "POWERGRID":   {"fyers": "NSE:POWERGRID-EQ",   "sector": "Power"},
    "RELIANCE":    {"fyers": "NSE:RELIANCE-EQ",    "sector": "Oil Gas & Fuels"},
    "SBILIFE":     {"fyers": "NSE:SBILIFE-EQ",     "sector": "Financial Services"},
    "SBIN":        {"fyers": "NSE:SBIN-EQ",        "sector": "Financial Services"},
    "SHRIRAMFIN":  {"fyers": "NSE:SHRIRAMFIN-EQ",  "sector": "Financial Services"},
    "SUNPHARMA":   {"fyers": "NSE:SUNPHARMA-EQ",   "sector": "Healthcare"},
    "TATACONSUM":  {"fyers": "NSE:TATACONSUM-EQ",  "sector": "FMCG"},
    "TATAMOTORS":  {"fyers": "NSE:TMPV-EQ",         "sector": "Automobile"},
    "TATASTEEL":   {"fyers": "NSE:TATASTEEL-EQ",   "sector": "Metals & Mining"},
    "TCS":         {"fyers": "NSE:TCS-EQ",         "sector": "Information Technology"},
    "TECHM":       {"fyers": "NSE:TECHM-EQ",       "sector": "Information Technology"},
    "TITAN":       {"fyers": "NSE:TITAN-EQ",       "sector": "Consumer Durables"},
    "TRENT":       {"fyers": "NSE:TRENT-EQ",       "sector": "Consumer Services"},
    "ULTRACEMCO":  {"fyers": "NSE:ULTRACEMCO-EQ",  "sector": "Construction Materials"},
    "UPL":         {"fyers": "NSE:UPL-EQ",         "sector": "Chemicals"},
    "WIPRO":       {"fyers": "NSE:WIPRO-EQ",       "sector": "Information Technology"},

    # ── Stocks removed from Nifty50 during 2019-2025 ──
    "BPCL":        {"fyers": "NSE:BPCL-EQ",        "sector": "Oil Gas & Fuels"},
    "BRITANNIA":   {"fyers": "NSE:BRITANNIA-EQ",   "sector": "FMCG"},
    "HINDPETRO":   {"fyers": "NSE:HINDPETRO-EQ",   "sector": "Oil Gas & Fuels"},
    "IBULHSGFIN":  {"fyers": "NSE:IBULHSGFIN-EQ",  "sector": "Financial Services"},
    "YESBANK":     {"fyers": "NSE:YESBANK-EQ",     "sector": "Financial Services"},
    "VEDL":        {"fyers": "NSE:VEDL-EQ",        "sector": "Metals & Mining"},
    "ZEEL":        {"fyers": "NSE:ZEEL-EQ",        "sector": "Media & Entertainment"},
    "GAIL":        {"fyers": "NSE:GAIL-EQ",        "sector": "Oil Gas & Fuels"},
    "IOC":         {"fyers": "NSE:IOC-EQ",         "sector": "Oil Gas & Fuels"},
    "SHREECEM":    {"fyers": "NSE:SHREECEM-EQ",    "sector": "Construction Materials"},
    "HDFC":        {"fyers": "NSE:HDFC-EQ",        "sector": "Financial Services"},
}

# Current Nifty50 composition (post Sep 2025 rebalance)
CURRENT_NIFTY50 = {
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "INDIGO", "INFY", "ITC",
    "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT", "M&M",
    "MARUTI", "MAXHEALTH", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
    "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
}

# Full index change history (effective_date, [added], [removed]).
# Source: Wikipedia NIFTY_50, NSE announcements.
# Sorted chronologically. Used to compute composition at any past date.
INDEX_CHANGES = [
    # ── 2019 ──
    ("2019-03-29", ["BRITANNIA"],             ["HINDPETRO"]),
    ("2019-09-27", ["NESTLEIND"],             ["IBULHSGFIN"]),
    # ── 2020 ──
    ("2020-03-27", ["SHREECEM"],              ["YESBANK"]),
    ("2020-07-31", ["HDFCLIFE"],              ["VEDL"]),
    ("2020-09-25", ["SBILIFE"],               ["ZEEL"]),
    # ── 2021 ──
    ("2021-03-31", ["TATACONSUM"],            ["GAIL"]),
    # ── 2022 ──
    ("2022-03-31", ["APOLLOHOSP"],            ["IOC"]),
    ("2022-09-30", ["ADANIENT"],              ["SHREECEM"]),
    # ── 2023 ──
    ("2023-07-13", ["LTIM"],                  ["HDFC"]),
    # ── 2024 ──
    ("2024-03-28", ["SHRIRAMFIN"],            ["UPL"]),
    ("2024-09-30", ["BEL", "TRENT"],          ["LTIM", "DIVISLAB"]),
    # ── 2025 ──
    ("2025-03-28", ["JIOFIN", "ETERNAL"],     ["BPCL", "BRITANNIA"]),
    ("2025-09-30", ["INDIGO", "MAXHEALTH"],   ["HEROMOTOCO", "INDUSINDBK"]),
]

# Stocks that merged/delisted — data stops at a known date.
# Backtest should auto-replace these when candle data ends.
STOCK_END_DATES = {
    "HDFC": "2023-07-13",       # merged with HDFC Bank
    "YESBANK": None,            # still listed but penny stock, data available
    "IBULHSGFIN": None,         # renamed, may have patchy data
}

NIFTY_SYMBOL = "NSE:NIFTY50-INDEX"


def get_nifty50_at_date(date_str: str) -> set:
    """Compute Nifty50 composition at a given date by reversing future index changes."""
    target = datetime.strptime(date_str, "%Y-%m-%d") if isinstance(date_str, str) else date_str
    members = set(CURRENT_NIFTY50)

    for change_date_str, added, removed in reversed(INDEX_CHANGES):
        change_date = datetime.strptime(change_date_str, "%Y-%m-%d")
        if change_date > target:
            for sym in added:
                members.discard(sym)
            for sym in removed:
                members.add(sym)

    return members


def get_index_changes_in_range(start_str: str, end_str: str) -> list:
    """Return index changes that fall within [start, end]."""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    result = []
    for date_str, added, removed in INDEX_CHANGES:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if start < dt <= end:
            result.append((date_str, added, removed))
    return result


def get_all_stocks_in_range(start_str: str, end_str: str) -> set:
    """Return ALL stocks that were in Nifty50 at any point during [start, end].
    This is the superset needed for data fetching."""
    members = get_nifty50_at_date(start_str)
    for date_str, added, removed in INDEX_CHANGES:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")
        if start <= dt <= end:
            members.update(added)
            members.update(removed)
    return members


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2023-01-01"
    members = get_nifty50_at_date(date)
    print(f"Nifty50 at {date}: {len(members)} stocks")
    for s in sorted(members):
        info = MASTER_STOCKS.get(s, {})
        print(f"  {s:15s}  {info.get('sector', '?'):30s}  {info.get('fyers', '?')}")
