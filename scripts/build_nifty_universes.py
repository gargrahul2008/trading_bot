#!/usr/bin/env python3
"""
Build the two BTST paper/live universes from NSE's OFFICIAL market-cap classification
(SEBI large/mid/small rules, reviewed every 6 months by NSE):

    top-250  = NIFTY LargeMidcap 250  (NIFTY 100 + NIFTY Midcap 150)
    next-250 = NIFTY Smallcap 250

Together they are the current NIFTY 500 (top 500 by full market cap). Downloading NSE's
own constituent CSVs avoids needing raw market-cap numbers (Fyers doesn't provide them).

Writes universe files in this repo's load_universe() format:
    universe_nifty_top250.json   (ranks 1-250)
    universe_nifty_next250.json  (ranks 251-500)

Refresh after each NSE semi-annual reconstitution (typically Mar/Sep). Read-only for the
market; just downloads public CSVs. Usage:  python scripts/build_nifty_universes.py
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BASE = "https://niftyindices.com/IndexConstituent"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120 Safari/537.36")

LISTS = {
    "top250":  "ind_niftylargemidcap250list.csv",
    "next250": "ind_niftysmallcap250list.csv",
    "n500":    "ind_nifty500list.csv",   # cross-check only
}


def fetch_csv(fname: str) -> list[dict]:
    req = urllib.request.Request(f"{BASE}/{fname}",
                                 headers={"User-Agent": UA, "Referer": "https://niftyindices.com/"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        text = resp.read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def symbols_of(rows: list[dict]) -> list[str]:
    """NSE:SYMBOL-EQ for EQ-series rows, in the CSV's order."""
    out = []
    for r in rows:
        if str(r.get("Series", "")).strip().upper() != "EQ":
            continue
        sym = str(r.get("Symbol", "")).strip().upper()
        if sym:
            out.append(f"NSE:{sym}-EQ")
    return out


def main() -> int:
    today = dt.date.today().isoformat()
    data = {k: fetch_csv(v) for k, v in LISTS.items()}
    top = symbols_of(data["top250"])
    nxt = symbols_of(data["next250"])
    n500 = set(symbols_of(data["n500"]))

    top_s, nxt_s = set(top), set(nxt)
    overlap = top_s & nxt_s
    union = top_s | nxt_s
    missing_from_500 = union - n500          # in our two lists but not Nifty 500
    n500_uncovered = n500 - union            # in Nifty 500 but in neither list

    print(f"top250 : {len(top)} symbols")
    print(f"next250: {len(nxt)} symbols")
    print(f"nifty500 cross-check: {len(n500)} symbols")
    print(f"overlap top∩next: {len(overlap)}  {sorted(overlap) if overlap else ''}")
    print(f"union {len(union)} vs nifty500 {len(n500)} | not-in-500: {len(missing_from_500)} "
          f"| 500-uncovered: {len(n500_uncovered)} {sorted(n500_uncovered) if n500_uncovered else ''}")

    if overlap:
        print("WARNING: top and next lists overlap — investigate before use.")

    for key, name, syms in [
        ("top250",  "nifty_top250_largemidcap",  top),
        ("next250", "nifty_next250_smallcap",    nxt),
    ]:
        out = REPO / f"universe_{key}.json"
        out.write_text(json.dumps({
            "name": name,
            "as_of": today,
            "source": f"NSE {LISTS[key]}",
            "symbols": syms,
        }, indent=2) + "\n")
        print(f"wrote {out.name}  ({len(syms)} symbols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
