"""
BTST paper-bot dashboard page. Read-only view of the '1lg0' overnight paper run(s): per
universe cumulative P&L, the current overnight book, today's signal, and the daily overnight
P&L curve. Reads state/btst_paper/<universe>/{state.json, paper_trades.jsonl}.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path(os.getenv("TRADING_BOT_ROOT", Path(__file__).resolve().parents[2]))
BTST_DIR = REPO / "state" / "btst_paper"


def _load(p: Path) -> dict:
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _trades(uni_dir: Path) -> pd.DataFrame:
    p = uni_dir / "paper_trades.jsonl"
    if not p.exists():
        return pd.DataFrame()
    rows = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return pd.DataFrame(rows)


def _inr(x) -> str:
    try:
        return f"₹{float(x):,.0f}"
    except Exception:
        return "₹0"


def render_page() -> None:
    st.title("BTST Paper — 1lg0 overnight")
    st.caption("Buy at 15:05 close, sell at 09:20 open, held for lf nights (flat intraday → "
               "only the close→open move is captured). Simulated fills at real prices.")

    unis = sorted(d.name for d in BTST_DIR.iterdir() if d.is_dir()) if BTST_DIR.exists() else []
    if not unis:
        st.info("No paper runs yet — the bot writes state under `state/btst_paper/<universe>/`.")
        return

    for uni in unis:
        st.subheader(uni)
        state = _load(BTST_DIR / uni / "state.json")
        if not state:
            st.info("no state yet")
            continue

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Cumulative realized P&L", _inr(state.get("realized_pnl", 0)))
        c2.metric("Active tranches", len(state.get("tranches", [])))
        c3.metric("Phase", state.get("phase", "—"))
        c4.metric("Last signal", state.get("signal_date") or "—")

        pending = state.get("pending_entry") or []
        if pending:
            st.caption("Signal pending for the next 15:05 buy: "
                       + ", ".join(f"{p['ticker']} ({p['perc']:.2f})" for p in pending))

        # current overnight book
        rows = []
        for tr in state.get("tranches", []):
            for tkr, pos in tr["positions"].items():
                rows.append({"ticker": tkr, "qty": pos["qty"],
                             "buy_price": round(pos["last_buy_price"], 2),
                             "nights_left": tr["nights_remaining"], "entered": tr["entry_date"]})
        if rows:
            st.caption("Current overnight book")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # daily overnight P&L from the sell legs
        tdf = _trades(BTST_DIR / uni)
        if not tdf.empty and "action" in tdf:
            sells = tdf[tdf["action"] == "SELL"].copy()
            if not sells.empty and "pnl" in sells:
                sells["pnl"] = pd.to_numeric(sells["pnl"], errors="coerce").fillna(0.0)
                daily = sells.groupby("ts")["pnl"].sum().reset_index().sort_values("ts")
                daily["cumulative"] = daily["pnl"].cumsum()
                st.line_chart(daily.set_index("ts")[["cumulative"]], height=200)
                with st.expander(f"{uni} — daily overnight P&L"):
                    st.dataframe(daily.rename(columns={"ts": "date", "pnl": "overnight_pnl"}),
                                 use_container_width=True, hide_index=True)
        st.divider()
