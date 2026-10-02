#!/usr/bin/env python3
"""Import downloaded Fyers statement CSVs into the dashboard store.

The API only serves the current financial year — asking `/ledger-history` for an
earlier window returns zero transactions, verified against jhalak. So anything
before this April can only come from the console's own exports, which is what
this reads.

    scripts/import_statements.py /root/statements            # dry run: what it would do
    scripts/import_statements.py /root/statements --apply

Three report types, recognised by their header rather than their filename:

  ledger    Date / Transaction type / Description / Debit / Credit / Running balance
            -> capital rows, via the same import_capital() the API path uses, so
               the dedup and reference semantics are identical.
  holdings  Total Invested + per-scrip Name/Qty/Buy price/ISIN
            -> the opening-securities capital entry, and opening positions.
  tradebook not handled here yet: the export carries no execution id, only order
            ids, so fills need a synthetic key. Separate pass.

The account is identified by the `Client ID` in the file header (the fy_id), not
by the filename — an export named by the broker carries the id inside it, and a
file renamed by hand should not be able to write to the wrong account.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from webapp.store.schema import connect  # noqa: E402


def _num(s: Any) -> Decimal:
    """'1,71,570.00' -> Decimal. Indian grouping, and '-' for nil."""
    t = str(s or "").strip().replace(",", "").replace("₹", "")
    if t in ("", "-", "--"):
        return Decimal("0")
    try:
        return Decimal(t)
    except InvalidOperation:
        return Decimal("0")


def _date(s: str) -> Optional[str]:
    """'29 Sep 2026' or '30/03/2026' -> '2026-09-29'."""
    t = str(s or "").strip()
    for fmt in ("%d %b %Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            return dt.datetime.strptime(t, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _meta(rows: List[List[str]]) -> Dict[str, str]:
    """The key,value block above the blank line that precedes the real header."""
    out: Dict[str, str] = {}
    for r in rows[:12]:
        if len(r) >= 2 and r[0] and not r[0].startswith(("Date,", "Name,")):
            out[r[0].strip()] = r[1].strip()
    return out


def _header_at(rows: List[List[str]], first_col: str) -> Optional[int]:
    for i, r in enumerate(rows):
        if r and r[0].strip() == first_col:
            return i
    return None


def classify(path: Path) -> Optional[str]:
    rows = list(csv.reader(open(path, newline="", encoding="utf-8-sig")))
    title = ""
    for r in rows[:3]:
        if len(r) >= 2 and r[0].strip() == "Report Title":
            title = r[1].strip().lower()
    if "ledger" in title:
        return "ledger"
    if "holding" in title:
        return "holdings"
    if "tradebook" in title:
        return "tradebook"
    # The tax P&L carries no "Report Title" row — identify it by its summary key.
    for r in rows[:30]:
        if r and r[0].strip() == "Realised P&L Summary":
            return "tax_pnl"
    return None


def parse_ledger(path: Path) -> Dict[str, Any]:
    """Into the shape `/ledger-history` returns, so import_capital() can consume
    it unchanged — same capital-type filter, same reference format, same dedup."""
    rows = list(csv.reader(open(path, newline="", encoding="utf-8-sig")))
    meta = _meta(rows)
    h = _header_at(rows, "Date")
    if h is None:
        raise ValueError("no 'Date' header row in %s" % path.name)

    txns = []
    for r in rows[h + 1:]:
        if not r or not any(r) or len(r) < 5:
            continue
        d = _date(r[0])
        if not d:
            continue
        txns.append({
            "date": d,
            "transaction_type": r[1].strip(),
            "description": r[2].strip(),
            "debit": float(_num(r[3])),
            "credit": float(_num(r[4])),
        })
    return {
        "client_id": meta.get("Client ID", ""),
        "range": meta.get("Date Range", ""),
        "transactions": txns,
    }


def parse_holdings(path: Path) -> Dict[str, Any]:
    rows = list(csv.reader(open(path, newline="", encoding="utf-8-sig")))
    meta = _meta(rows)
    h = _header_at(rows, "Name")
    lots = []
    if h is not None:
        for r in rows[h + 1:]:
            if not r or not any(r) or len(r) < 4:
                continue
            lots.append({
                "symbol": r[0].strip(),
                "qty": float(_num(r[1])),
                "buy_price": float(_num(r[2])),
                "invested": float(_num(r[3])),
                "isin": r[8].strip() if len(r) > 8 else "",
            })
    return {
        "client_id": meta.get("Client ID", ""),
        "as_of": _date(meta.get("Date", "")),
        "total_invested": float(_num(meta.get("Total Invested", 0))),
        "lots": lots,
    }


# The summary block is the whole point: the broker's own realised figure for the
# year, already split by the categories tax treats differently, and already netted
# of charges it itemises below. Validated against the API on rahul FY2026-27 —
# Total Charges matched to the paisa (41,768.24).
TAX_KEYS = (
    "Realised P&L Summary", "Net LTCG P&L", "Net STCG P&L", "Taxable Intraday P&L",
    "Taxable Future P&L", "Taxable Options P&L", "Total Charges",
    "Turnover Equity (ICAI)", "Turnover FnO (ICAI)",
)
EXPENSE_KEYS = (
    "Transaction Charges", "SEBI Charges", "STT", "CTT", "Stamp Duty",
    "Brokerage", "GST", "IPFT",
)


def parse_tax_pnl(path: Path) -> Dict[str, Any]:
    """The realised-P&L statement: what the broker files, per financial year.

    This is the only source that gets prior years right. The holdings-snapshot
    method excludes MTF and F&O positions (rahul's RELIANCE MTF position was
    larger than his entire holdings statement), and ledger `Trading` rows are
    cash settlement rather than profit. Both were tested against the API's
    FY2026-27 figure and both failed; this one reconciles.
    """
    rows = list(csv.reader(open(path, newline="", encoding="utf-8-sig")))
    meta: Dict[str, str] = {}
    summary: Dict[str, float] = {}
    expenses: Dict[str, float] = {}

    for r in rows:
        if len(r) < 2 or not r[0].strip():
            continue
        k, v = r[0].strip(), r[1].strip()
        if k in ("Client ID", "Client Name", "PAN", "Financial Year", "Date Range"):
            meta[k] = v
        elif k in TAX_KEYS:
            summary[k] = float(_num(v))
        elif k.rstrip() in EXPENSE_KEYS:
            expenses[k.rstrip()] = float(_num(v))

    return {
        "client_id": meta.get("Client ID", ""),
        "financial_year": meta.get("Financial Year", ""),
        "range": meta.get("Date Range", ""),
        "summary": summary,
        "expenses": expenses,
    }


def fy_id_map() -> Dict[str, str]:
    """fy_id -> account name, from the same register everything else uses."""
    sys.path.insert(0, str(REPO / "deploy"))
    from accounts import load  # noqa: E402
    out = {}
    for a in load():
        fy = str(getattr(a, "fy_id", "") or "").strip().upper()
        if fy and a.name:
            out[fy] = a.name
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", help="directory of downloaded CSVs (searched recursively)")
    ap.add_argument("--apply", action="store_true", help="write to the store")
    ap.add_argument("--account", help="only this account")
    ap.add_argument("--db", default=None)
    args = ap.parse_args(argv)

    root = Path(args.root)
    files = sorted(p for p in root.rglob("*.csv"))
    if not files:
        print("no CSVs under %s" % root)
        return 1

    fymap = fy_id_map()
    print("known accounts: " + ", ".join("%s=%s" % (k, v) for k, v in sorted(fymap.items())))
    print()

    seen: Dict[str, Dict[str, List[Path]]] = {}
    for p in files:
        kind = classify(p)
        if not kind:
            print("  SKIP  %s — unrecognised report" % p.name)
            continue
        rows = list(csv.reader(open(p, newline="", encoding="utf-8-sig")))
        cid = _meta(rows).get("Client ID", "").upper()
        acct = fymap.get(cid)
        if not acct:
            print("  SKIP  %s — Client ID %r maps to no account" % (p.name, cid))
            continue
        if args.account and acct != args.account:
            continue
        seen.setdefault(acct, {}).setdefault(kind, []).append(p)

    for acct, kinds in sorted(seen.items()):
        print("── %s" % acct)
        for kind, paths in sorted(kinds.items()):
            for p in paths:
                if kind == "ledger":
                    d = parse_ledger(p)
                    caps = [t for t in d["transactions"]
                            if t["transaction_type"].lower() in ("funds added", "funds withdrawn")]
                    opens = [t for t in d["transactions"]
                             if t["transaction_type"].lower() == "opening balance"]
                    net = sum(Decimal(str(t["credit"])) - Decimal(str(t["debit"])) for t in caps)
                    print("   ledger   %-52s %4d txns, %2d capital (net %s), %d opening"
                          % (p.name[:52], len(d["transactions"]), len(caps),
                             format(net, "+,.2f"), len(opens)))
                elif kind == "holdings":
                    d = parse_holdings(p)
                    print("   holdings %-52s as-of %s  total invested %s  (%d scrips)"
                          % (p.name[:52], d["as_of"],
                             format(d["total_invested"], ",.2f"), len(d["lots"])))
                elif kind == "tax_pnl":
                    d = parse_tax_pnl(p)
                    sm = d["summary"]
                    print("   tax_pnl  %-52s FY %s" % (p.name[:52], d["financial_year"]))
                    print("       realised %s   charges %s   (%s)"
                          % (format(sm.get("Realised P&L Summary", 0), ",.2f"),
                             format(sm.get("Total Charges", 0), ",.2f"), d["range"]))
                    for k in ("Net LTCG P&L", "Net STCG P&L", "Taxable Intraday P&L",
                              "Taxable Future P&L", "Taxable Options P&L"):
                        if k in sm:
                            print("         %-22s %s" % (k, format(sm[k], ">14,.2f")))
                else:
                    print("   tradebook %-51s (not needed \u2014 tax_pnl supersedes it)" % p.name[:51])
        print()

    if not args.apply:
        print("Dry run — nothing written. Re-run with --apply once the figures above look right.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
