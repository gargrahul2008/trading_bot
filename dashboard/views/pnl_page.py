"""
P&L dashboard page — strategy-level and user-level, gross (local) vs net (broker).

Pure reader: aggregates each run's local trades.jsonl and the cached broker report
(accounts/<user>/reports/broker_pnl.json written by the IP-bound scripts/fetch_broker_pnl.py).
Makes no broker calls itself.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(os.getenv("TRADING_BOT_ROOT", Path(__file__).resolve().parents[2]))

import sys
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.reporting import pnl as pnl_mod


def _f(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _inr(x) -> str:
    return f"₹{_f(x):,.2f}"


def _broker_freshness(accounts_dir: Path) -> dict:
    out = {}
    if not accounts_dir.exists():
        return out
    for user_dir in accounts_dir.iterdir():
        rep = user_dir / "reports" / "broker_pnl.json"
        if rep.exists():
            try:
                out[user_dir.name] = json.loads(rep.read_text()).get("fetched_at")
            except Exception:
                out[user_dir.name] = "unreadable"
    return out


def render_page() -> None:
    st.title("P&L — by strategy & user")
    accounts_dir = REPO_ROOT / "accounts"

    rows = pnl_mod.build_report(accounts_dir)
    if not rows:
        st.info(
            "No account runs found under `accounts/`. Once bots have traded (and written "
            "`state/trades.jsonl`), and `fetch_broker_pnl.py` has cached broker reports, "
            "P&L will appear here."
        )
        return

    totals = pnl_mod.user_totals(rows)
    grand = totals.get("", {})

    # ── grand totals ────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    net = grand.get("net_realized", Decimal(0))
    c1.metric("Net realized (all users)", _inr(net))
    c2.metric("Gross realized (local)", _inr(grand.get("local_realized", 0)))
    c3.metric("Charges", _inr(grand.get("charges", 0)))
    c4.metric("Fills", int(_f(grand.get("n_fills", 0))))

    # broker data freshness / presence
    fresh = _broker_freshness(accounts_dir)
    has_broker = any(sp.broker_realized is not None for sp in rows)
    if not has_broker:
        st.warning(
            "Showing **local gross** P&L only — no broker report found. Run "
            "`fetch_broker_pnl.py` per account (through its IP) to add broker-truth net P&L "
            "and charges."
        )
    if fresh:
        st.caption("Broker report last fetched: " + " · ".join(f"{u}: {t}" for u, t in fresh.items()))

    # ── per-strategy table ──────────────────────────────────────────────────────
    df = pd.DataFrame([sp.as_row() for sp in rows])
    num_cols = ["local_realized", "broker_realized", "charges", "net_realized",
                "reconcile_delta", "avg_slippage_bps"]
    for col in num_cols:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    display = df.rename(columns={
        "user": "User", "strategy": "Strategy", "symbol": "Symbol",
        "local_realized": "Gross (local)", "broker_realized": "Realized (broker)",
        "charges": "Charges", "net_realized": "Net", "reconcile_delta": "Recon Δ",
        "n_fills": "Fills", "avg_slippage_bps": "Slip bps", "last_trade_ts": "Last trade",
    })
    cols = ["User", "Strategy", "Symbol", "Gross (local)", "Realized (broker)", "Charges",
            "Net", "Recon Δ", "Fills", "Slip bps", "Last trade"]
    cols = [c for c in cols if c in display.columns]

    st.subheader("Per strategy")
    st.dataframe(display[cols], use_container_width=True, hide_index=True)

    # Reconciliation flag — local vs broker realized disagree beyond a small tolerance.
    if "reconcile_delta" in df:
        flagged = df[df["reconcile_delta"].abs() > 1]
        if not flagged.empty:
            st.subheader("⚠️ Reconciliation mismatches (local vs broker)")
            st.caption("Non-trivial gap between the bot's recorded realized P&L and the "
                       "broker's — investigate missed/partial/phantom fills.")
            st.dataframe(
                display.loc[flagged.index, ["User", "Strategy", "Symbol",
                                            "Gross (local)", "Realized (broker)", "Recon Δ"]],
                use_container_width=True, hide_index=True,
            )

    # ── per-user subtotals ──────────────────────────────────────────────────────
    st.subheader("Per user")
    user_rows = []
    for user, t in totals.items():
        if user == "":
            continue
        user_rows.append({
            "User": user,
            "Gross (local)": _f(t["local_realized"]),
            "Realized (broker)": _f(t["broker_realized"]),
            "Charges": _f(t["charges"]),
            "Net": _f(t["net_realized"]),
            "Fills": int(_f(t["n_fills"])),
        })
    st.dataframe(pd.DataFrame(user_rows), use_container_width=True, hide_index=True)
