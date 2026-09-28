#!/usr/bin/env python3
"""
Build the current NSE F&O-eligible stock universe from NSE's own official
market-lot file (the same file that lists every derivative contract's lot
size), for the "SHORT only where futures actually exist" scanner rule —
Indian equities generally can't be shorted without an F&O market to borrow
via, so a spot SHORT signal on a non-F&O name isn't a real tradeable setup.

Writes research_futures_universe.json in this repo's futures-universe format
(see research_futures_universe.example.json): one entry per F&O-eligible
stock, with its current near-month lot size.

Refresh whenever NSE adds/removes F&O eligibility (reviewed periodically,
not on a fixed schedule like the index-membership universes). Read-only for
the market; just downloads a public NSE CSV.
Usage:  python scripts/build_fno_universe.py
"""
from __future__ import annotations

import csv
import io
import json
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
URL = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120 Safari/537.36")

# Rows above this marker are INDEX derivatives (NIFTY, BANKNIFTY, ...), not
# individual stocks — everything from the row after it down is one stock per row.
STOCK_SECTION_MARKER = "Derivatives on Individual Securities"


def fetch_rows() -> list[list[str]]:
    req = urllib.request.Request(URL, headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8-sig")
    return list(csv.reader(io.StringIO(text)))


def parse_stock_rows(rows: list[list[str]]) -> list[dict]:
    stocks: list[dict] = []
    in_stock_section = False
    for row in rows:
        if not row:
            continue
        cells = [c.strip() for c in row]
        if cells[0] == STOCK_SECTION_MARKER:
            in_stock_section = True
            continue
        if not in_stock_section:
            continue
        underlying, symbol, *lot_sizes = cells
        if not symbol:
            continue
        near_month_lot = next((int(v) for v in lot_sizes if v), None)
        if near_month_lot is None:
            continue  # a symbol with no currently-listed contract at all — skip
        stocks.append({
            "underlying": underlying,
            "symbol": f"NSE:{symbol}-EQ",
            "data_symbol": f"NSE:{symbol}-EQ",
            "lot_size": near_month_lot,
        })
    return stocks


def main() -> int:
    rows = fetch_rows()
    stocks = parse_stock_rows(rows)
    stocks.sort(key=lambda s: s["symbol"])

    print(f"parsed {len(stocks)} F&O-eligible stocks from {URL}")
    if len(stocks) < 100:
        print("WARNING: far fewer than the usual ~180-220 F&O stocks — check the source format hasn't changed.")

    out = REPO / "research_futures_universe.json"
    out.write_text(json.dumps({
        "name": "nifty_fo_stocks_current",
        "description": (
            "Every stock with a currently-listed F&O contract on NSE (from the official "
            "market-lot file). Used to restrict SHORT signals to names that can actually be "
            "shorted — spot equities generally can't be shorted without an F&O market to "
            "borrow via. Refresh via scripts/build_fno_universe.py."
        ),
        "benchmark_symbol": "NSE:NIFTY50-INDEX",
        "data_mode": "spot_proxy",
        "symbols": stocks,
    }, indent=2) + "\n")
    print(f"wrote {out.name}  ({len(stocks)} symbols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
