#!/usr/bin/env python3
"""
Build the COMPLETE NSE cash-segment equity universe — every currently-listed
EQ-series stock, not just an index-membership subset like NIFTY Total Market
750 (see build_nifty_universes.py) — from FYERS' own public NSE Capital
Market symbol master.

FYERS publishes this as a plain, unauthenticated CSV covering every
instrument tradable on NSE's cash segment (equities, SGBs, ETFs, bonds,
T-bills, SME board, ...); each row's 10th field is already the exact
Fyers-formatted symbol this codebase uses everywhere else
(e.g. "NSE:RELIANCE-EQ"). Filtering to symbols ending "-EQ" is what isolates
the ordinary equity cash segment from everything else in the file (SGBs
"-SG", SME board "-SM"/"-ST", bonds/T-bills "-N0".."-N6"/"-GS"/"-GB"/"-TB",
mutual funds "-MF", trade-to-trade "-BE", etc.) — ~2,700 symbols as of
2026-09, versus ~10,000 rows in the raw file and ~750 in the index-based
universe.

Writes universe_full_cash_segment.json in this repo's load_universe()
format (see src/intraday_research/universe.py) — a flat symbol list, same
shape as universe_nifty_total_market750.json, so it's a drop-in UNIVERSE_FILE
swap in the scanner notebook. Does NOT replace the smaller universes; use
whichever fits the scan you're running (~2,700 symbols scans/backtests
meaningfully slower than ~750, and this list includes plenty of thin,
illiquid names the smaller curated lists screen out).

Refresh whenever you want an up-to-date listing (new IPOs, delistings) — no
fixed schedule, unlike the index-membership universes' semi-annual
reconstitution. Read-only for the market; just downloads a public CSV.
Usage:  python scripts/build_full_cash_universe.py
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120 Safari/537.36")

# 0-indexed columns in FYERS' (headerless) NSE_CM.csv — confirmed by
# inspection, not documented anywhere FYERS publishes.
SYMBOL_COLUMN = 9
# Field 2: a numeric instrument-type code. 9 = ETF, confirmed by checking
# every type-9 row's own name field (field 1) — all 351 are genuinely ETFs
# (some spelled out as "EXCHANGE TRADED FUND" rather than the "ETF"
# abbreviation, which is why this code is used instead of a name-substring
# match). Other codes (0, 2, 4-8, 10) are ordinary equity/other instrument
# types, not distinguished further here.
INSTRUMENT_TYPE_COLUMN = 2
ETF_INSTRUMENT_TYPE = "9"


def fetch_rows() -> list[list[str]]:
    req = urllib.request.Request(URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        text = resp.read().decode("utf-8-sig")
    return list(csv.reader(io.StringIO(text)))


def parse_eq_symbols(rows: list[list[str]]) -> tuple[list[str], list[str]]:
    """Returns (all EQ-series symbols, the subset of those that are ETFs)."""
    symbols: list[str] = []
    etf_symbols: list[str] = []
    for row in rows:
        if len(row) <= SYMBOL_COLUMN:
            continue
        symbol = row[SYMBOL_COLUMN].strip()
        if not (symbol.startswith("NSE:") and symbol.endswith("-EQ")):
            continue
        symbols.append(symbol)
        if row[INSTRUMENT_TYPE_COLUMN].strip() == ETF_INSTRUMENT_TYPE:
            etf_symbols.append(symbol)
    return symbols, etf_symbols


def main() -> int:
    rows = fetch_rows()
    symbols, etf_symbols = parse_eq_symbols(rows)
    symbols = sorted(set(symbols))
    etf_symbols = sorted(set(etf_symbols))

    print(f"parsed {len(rows)} total rows, {len(symbols)} EQ-series symbols "
          f"({len(etf_symbols)} of them ETFs) from {URL}")
    if len(symbols) < 1500:
        print("WARNING: far fewer than the usual ~2,500-2,900 EQ symbols — check the source format hasn't changed.")
    if len(etf_symbols) < 200:
        print("WARNING: far fewer than the usual ~350 ETFs — check the instrument-type code hasn't changed.")

    out = REPO / "universe_full_cash_segment.json"
    out.write_text(json.dumps({
        "name": "nse_full_cash_segment",
        "as_of": dt.date.today().isoformat(),
        "source": f"FYERS NSE Capital Market symbol master ({URL}), filtered to -EQ series",
        "symbols": symbols,
    }, indent=2) + "\n")
    print(f"wrote {out.name}  ({len(symbols)} symbols)")

    etf_out = REPO / "universe_full_cash_segment_etf.json"
    etf_out.write_text(json.dumps({
        "name": "nse_full_cash_segment_etf",
        "as_of": dt.date.today().isoformat(),
        "source": f"FYERS NSE Capital Market symbol master ({URL}), instrument-type code "
                   f"{ETF_INSTRUMENT_TYPE} (ETF) within the -EQ series",
        "symbols": etf_symbols,
    }, indent=2) + "\n")
    print(f"wrote {etf_out.name}  ({len(etf_symbols)} symbols)")

    etf_set = set(etf_symbols)
    ex_etf_symbols = [s for s in symbols if s not in etf_set]
    ex_etf_out = REPO / "universe_full_cash_segment_ex_etf.json"
    ex_etf_out.write_text(json.dumps({
        "name": "nse_full_cash_segment_ex_etf",
        "as_of": dt.date.today().isoformat(),
        "source": f"FYERS NSE Capital Market symbol master ({URL}), -EQ series with "
                   f"instrument-type {ETF_INSTRUMENT_TYPE} (ETF) symbols removed",
        "symbols": ex_etf_symbols,
    }, indent=2) + "\n")
    print(f"wrote {ex_etf_out.name}  ({len(ex_etf_symbols)} symbols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
