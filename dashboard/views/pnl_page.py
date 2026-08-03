"""
P&L dashboard page — per user, two layers:

  1. Portfolio (broker, live)  — holdings + open positions + funds, with mark-to-market
     UNREALIZED P&L. "What I hold and what it's worth." From accounts/<user>/reports/portfolio.json
     (scripts/fetch_broker_portfolio.py).
  2. Bot performance           — REALIZED P&L per strategy/symbol/day from each bot's own trade
     logs. "How the strategies are doing." From accounts/<user>/reports/bot_pnl_history.json
     (scripts/build_bot_pnl_history.py), validated to match state.

These two are additive, not a cross-check: portfolio P&L includes base holdings the bot never
traded. A broker-vs-bot reconciliation (charges + audit) is available in the expander at the
bottom. Pure reader — makes no broker calls itself.
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


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _users(accounts_dir: Path) -> list:
    if not accounts_dir.exists():
        return []
    return sorted(p.name for p in accounts_dir.iterdir()
                  if p.is_dir() and not p.name.startswith("_"))


def _user_aggregates(accounts_dir: Path, user_filter) -> tuple:
    """(bot_realized, unrealized, charges, mtf_interest, grid_realized) over the selected user(s)."""
    bot_r = unreal = chg = mtf = grid = 0.0
    for name in _users(accounts_dir):
        if user_filter and name != user_filter:
            continue
        rep = accounts_dir / name / "reports"
        bh = _load_json(rep / "bot_pnl_history.json")
        bot_r += _f(bh.get("total_realized", 0))
        grid += _f(bh.get("total_grid_realized", 0))   # grid's own round-trips (excl. base)
        chg += _f(bh.get("total_charges", 0))          # trade charges from bot's full history
        mtf += _f(bh.get("total_mtf_interest", 0))     # MTF funding interest (leveraged runs)
        unreal += _f(_load_json(rep / "portfolio.json").get("totals", {}).get("unrealized_total", 0))
    return bot_r, unreal, chg, mtf, grid


def _render_portfolio(accounts_dir: Path, user_filter) -> None:
    st.subheader("Portfolio — holdings & positions (broker, live)")
    any_shown = False
    for name in _users(accounts_dir):
        if user_filter and name != user_filter:
            continue
        pf = _load_json(accounts_dir / name / "reports" / "portfolio.json")
        if not pf:
            continue
        any_shown = True
        st.markdown(f"**{name}**")
        funds = pf.get("funds", {})
        tot = pf.get("totals", {})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Available", _inr(funds.get("available", 0)))
        c2.metric("Utilized", _inr(funds.get("utilized", 0)))
        c3.metric("Holdings value", _inr(tot.get("holdings_value", 0)))
        c4.metric("Unrealized (total)", _inr(tot.get("unrealized_total", 0)))
        if pf.get("holdings"):
            st.caption("Holdings")
            st.dataframe(pd.DataFrame(pf["holdings"]), use_container_width=True, hide_index=True)
        if pf.get("positions"):
            st.caption("Open positions")
            st.dataframe(pd.DataFrame(pf["positions"]), use_container_width=True, hide_index=True)
        st.caption(f"fetched: {pf.get('fetched_at', '—')}")
    if not any_shown:
        st.info("No portfolio snapshot yet — run `fetch_broker_portfolio.py` per account "
                "(through its IP) to populate holdings/positions/funds.")


def _render_bot_history(accounts_dir: Path, user_filter=None) -> None:
    """Bot realized-P&L history from each bot's own trade logs (validated vs state)."""
    st.subheader("Bot performance — realized P&L (from trade logs)")
    st.caption("Realized P&L on the quantity each strategy manages — gross of charges, "
               "independent of base holdings.")
    any_shown = False
    for name in _users(accounts_dir):
        if user_filter and name != user_filter:
            continue
        h = _load_json(accounts_dir / name / "reports" / "bot_pnl_history.json")
        if not h:
            continue
        any_shown = True
        st.markdown(f"**{name}** — grid P&L {_inr(h.get('total_grid_realized', 0))} · "
                    f"realized {_inr(h.get('total_realized', 0))} · "
                    f"charges {_inr(h.get('total_charges', 0))} · "
                    f"MTF interest {_inr(h.get('total_mtf_interest', 0))} · "
                    f"net {_inr(h.get('net_realized', 0))}")
        # per strategy (run) → per symbol
        strat_rows = []
        for run, r in (h.get("runs") or {}).items():
            cbs = r.get("charges_by_symbol") or {}
            mbs = r.get("mtf_interest_by_symbol") or {}
            gbs = r.get("grid_realized_by_symbol") or {}
            for sym, sv in (r.get("by_symbol") or {}).items():
                real = round(_f(sv.get("realized")), 2)
                gr = round(_f(gbs.get(sym)), 2)
                chg = round(_f(cbs.get(sym)), 2)
                mtf = round(_f(mbs.get(sym)), 2)
                strat_rows.append({"Strategy": run, "Symbol": sym, "Grid RT (₹)": gr,
                                   "Realized (₹)": real, "Charges (₹)": chg, "MTF int (₹)": mtf,
                                   "Net (₹)": round(real - chg - mtf, 2),
                                   "Fills": int(_f(sv.get("n_fills")))})
        if strat_rows:
            st.dataframe(pd.DataFrame(strat_rows), use_container_width=True, hide_index=True)
        days = h.get("by_day", {})
        if days:
            dd = pd.DataFrame([{"date": d, "daily": _f(v)} for d, v in sorted(days.items())])
            dd["cumulative"] = dd["daily"].cumsum()
            st.line_chart(dd.set_index("date")[["cumulative"]], height=200)
            with st.expander(f"{name} — daily P&L"):
                st.dataframe(dd.set_index("date"), use_container_width=True)
    if not any_shown:
        st.info("No bot P&L history yet — run `build_bot_pnl_history.py`.")


def _render_audit(rows, user_filter) -> None:
    """Broker-vs-bot reconciliation (charges + realized). Additive-not-a-check for base holdings,
    so it's tucked away — useful mainly for full-position symbols and charge totals."""
    with st.expander("Broker reconciliation & charges (audit)"):
        st.caption("Broker-truth realized (replayed from tradebook) vs the bot's realized, plus "
                   "estimated charges. Expect divergence where you hold base quantity or trade "
                   "manually — that's activity outside the bot, not an error.")
        df = pd.DataFrame([sp.as_row() for sp in rows])
        for c in ["local_realized", "broker_realized", "charges", "net_realized", "reconcile_delta"]:
            if c in df:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        disp = df.rename(columns={
            "user": "User", "strategy": "Strategy", "symbol": "Symbol",
            "local_realized": "Bot realized", "broker_realized": "Broker realized",
            "charges": "Charges", "reconcile_delta": "Δ (broker−bot)", "n_fills": "Fills"})
        cols = [c for c in ["User", "Strategy", "Symbol", "Bot realized", "Broker realized",
                            "Charges", "Δ (broker−bot)", "Fills"] if c in disp.columns]
        st.dataframe(disp[cols], use_container_width=True, hide_index=True)


def render_page() -> None:
    st.title("P&L — by user")
    accounts_dir = REPO_ROOT / "accounts"

    users = _users(accounts_dir)
    if not users:
        st.info("No accounts found under `accounts/`.")
        return

    choice = st.selectbox("Select user", ["All users"] + users, index=0)
    user_filter = None if choice == "All users" else choice

    # ── headline ─────────────────────────────────────────────────────────────────
    bot_r, unreal, chg, mtf, grid = _user_aggregates(accounts_dir, user_filter)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Grid round-trip P&L", _inr(grid))
    c2.metric(f"Bot realized — {choice}", _inr(bot_r))
    c3.metric("Charges (est.)", _inr(chg))
    c4.metric("MTF interest (est.)", _inr(mtf))
    c5.metric("Unrealized (holdings)", _inr(unreal))
    c6.metric("Total P&L", _inr(bot_r - chg - mtf + unreal))
    st.caption("**Grid round-trip P&L** = the grid's own buy↔sell cycles only (excludes selling "
               "down base holdings). **Bot realized** = actual, incl. base-holding liquidation. "
               "**Total P&L** = bot realized − charges − MTF interest + holdings unrealized.")

    _render_portfolio(accounts_dir, user_filter)
    _render_bot_history(accounts_dir, user_filter)

    # audit / reconciliation (de-emphasized)
    rows = pnl_mod.build_report(accounts_dir)
    sel_rows = rows if user_filter is None else [sp for sp in rows if sp.user == user_filter]
    if sel_rows:
        _render_audit(sel_rows, user_filter)
