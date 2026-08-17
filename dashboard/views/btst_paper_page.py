"""
BTST paper-bot dashboard — detailed view of the '1lg0' overnight paper run(s).

Per universe: headline P&L/notional, the current overnight book (every held share with qty,
weight, ORIGINAL entry price, tonight's re-buy cost, nights left, notional, entry timestamp),
today's activity (sold at 09:20 / opened at 15:05), and the full trade ledger.

Note on prices: flat_legs re-buys the whole book at each 15:05 close, so every overnight
holding's "cost tonight" (last_buy_price) is the SAME today's-close price — the number that
distinguishes tranches is the ORIGINAL entry price. Both are shown.
Reads state/btst_paper/<universe>/{state.json, paper_trades.jsonl}.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = Path(os.getenv("TRADING_BOT_ROOT", Path(__file__).resolve().parents[2]))
BTST_DIR = REPO / "state" / "btst_paper"
ENTRY_TIME, EXIT_TIME = "15:05", "09:20"   # buy-at-close / sell-at-open marks


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


def _book_rows(state: dict) -> list[dict]:
    """Every held share, one row, with original entry vs tonight's cost + weight + notional."""
    rows = []
    for tr in state.get("tranches", []):
        pos = tr["positions"]
        # allocation %: prefer stored perc; else fall back to notional share within the tranche
        tot_notional = sum(p["qty"] * p["last_buy_price"] for p in pos.values()) or 1.0
        for tkr, p in pos.items():
            notional = p["qty"] * p["last_buy_price"]
            perc = p.get("perc")
            alloc = (perc * 100) if perc is not None else (notional / tot_notional * 100)
            rows.append({
                "Opened": f"{tr['entry_date']} {ENTRY_TIME}",
                "Ticker": tkr,
                "Qty": p["qty"],
                "Alloc %": round(alloc, 1),
                "Entry ₹ (original)": round(p.get("entry_price", p["last_buy_price"]), 2),
                "Cost tonight ₹": round(p["last_buy_price"], 2),
                "Nights left": tr["nights_remaining"],
                "Notional ₹": round(notional, 2),
            })
    return rows


def render_page() -> None:
    st.title("BTST Paper — 1lg0 overnight")
    st.caption("Buy at 15:05 close, sell at 09:20 open, held lf=5 nights (flat intraday → only "
               "the close→open move is captured). Simulated fills at real prices; no broker orders.")

    unis = sorted(d.name for d in BTST_DIR.iterdir() if d.is_dir()) if BTST_DIR.exists() else []
    if not unis:
        st.info("No paper runs yet — the bot writes state under `state/btst_paper/<universe>/`.")
        return

    for uni in unis:
        state = _load(BTST_DIR / uni / "state.json")
        if not state:
            continue
        st.header(uni)
        book = _book_rows(state)
        total_notional = sum(r["Notional ₹"] for r in book)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Cumulative P&L", _inr(state.get("realized_pnl", 0)))
        c2.metric("Deployed notional", _inr(total_notional))
        c3.metric("Active tranches", len(state.get("tranches", [])))
        c4.metric("Phase", state.get("phase", "—"))
        c5.metric("Last signal", state.get("signal_date") or "—")

        pending = state.get("pending_entry") or []
        if pending:
            st.info("Signal pending for the next 15:05 buy: "
                    + ", ".join(f"{p['ticker']} ({p['perc']*100:.0f}%)" for p in pending))

        # ── current overnight book ────────────────────────────────────────────
        st.subheader("Current overnight book")
        st.caption("Each held position. **Entry ₹** = the price it was first opened at (differs per "
                   "tranche). **Cost tonight ₹** = today's 15:05 re-buy price (same for all holdings — "
                   "flat_legs re-buys the whole book each close). Tomorrow's 09:20 sell is marked vs "
                   "*Cost tonight*.")
        if book:
            st.dataframe(pd.DataFrame(book), use_container_width=True, hide_index=True)
        else:
            st.write("— flat (no overnight positions right now) —")

        # ── today's activity ─────────────────────────────────────────────────
        tdf = _trades(BTST_DIR / uni)
        if not tdf.empty:
            last_day = str(tdf["ts"].max())
            day = tdf[tdf["ts"].astype(str) == last_day].copy()
            day["notional"] = (day["qty"] * day["price"]).round(2)

            st.subheader(f"Today's activity — {last_day}")
            sells = day[day["action"] == "SELL"]
            if not sells.empty:
                sv = sells.assign(**{"Sold at": f"{last_day} {EXIT_TIME}"})[
                    ["Sold at", "ticker", "qty", "price", "notional", "pnl"]].rename(
                    columns={"ticker": "Ticker", "qty": "Qty", "price": "Sell ₹",
                             "notional": "Notional ₹", "pnl": "Overnight P&L ₹"})
                st.caption("Sold at the 09:20 open (overnight legs booked)")
                st.dataframe(sv, use_container_width=True, hide_index=True)

            opens = day[(day["action"] == "BUY") & (day["kind"] == "open")]
            if not opens.empty:
                ov = opens.assign(**{"Opened at": f"{last_day} {ENTRY_TIME}"})
                ov["Alloc %"] = (ov["perc"] * 100).round(1) if "perc" in ov else None
                ov = ov[["Opened at", "ticker", "qty", "price", "Alloc %", "notional"]].rename(
                    columns={"ticker": "Ticker", "qty": "Qty", "price": "Buy ₹", "notional": "Notional ₹"})
                st.caption("New tranche opened at the 15:05 close")
                st.dataframe(ov, use_container_width=True, hide_index=True)

            rolls = day[(day["action"] == "BUY") & (day["kind"] == "roll")]
            if not rolls.empty:
                st.caption(f"Re-bought {len(rolls)} rolled legs at the 15:05 close "
                           f"(pending tranches re-established for the next night)")

            # ── full ledger ───────────────────────────────────────────────────
            with st.expander(f"{uni} — full trade ledger"):
                led = tdf.copy()
                led["time"] = led["action"].map({"SELL": EXIT_TIME, "BUY": ENTRY_TIME})
                led["notional"] = (led["qty"] * led["price"]).round(2)
                cols = [c for c in ["ts", "time", "action", "kind", "ticker", "qty", "price",
                                    "notional", "perc", "pnl"] if c in led.columns]
                st.dataframe(led[cols].rename(columns={"ts": "date"}),
                             use_container_width=True, hide_index=True)

            # ── daily overnight P&L curve ─────────────────────────────────────
            sells_all = tdf[tdf["action"] == "SELL"].copy()
            if not sells_all.empty and "pnl" in sells_all:
                sells_all["pnl"] = pd.to_numeric(sells_all["pnl"], errors="coerce").fillna(0.0)
                daily = sells_all.groupby("ts")["pnl"].sum().reset_index().sort_values("ts")
                daily["cumulative"] = daily["pnl"].cumsum()
                st.line_chart(daily.set_index("ts")[["cumulative"]], height=200)
        st.divider()
