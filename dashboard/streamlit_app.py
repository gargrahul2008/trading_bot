# streamlit_app.py
# Drop-in Streamlit dashboard for this trading_bot repo.
#
# Features:
# - Select strategy + run folder (state/...) and load snapshot/summary/trades
# - UTC date-range filter across trades
# - Daily aggregates (realized, buy/sell quote, cycles_est)
# - Slippage view (expected vs fill, slippage_bps) if present in trades.jsonl
# - Current snapshot view (created/deployed/cycles/holdings + per-symbol table)
# - Optional pnl_points.csv plot if present
#
# Usage:
#   streamlit run streamlit_app.py
#
# Assumptions:
# - Per run folder contains:
#     - positions_snapshot.json (optional)
#     - pnl_summary.json (optional)
#     - trades.jsonl (or any *trades*.jsonl) (optional)
#     - pnl_points.csv (optional)
#     - state.json (optional; used for cycle_unit_quote_by_symbol)
#
# If your filenames differ, the file picker will attempt "best effort" discovery.

from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


# -------------------------
# Helpers
# -------------------------

def _safe_json_load(path: str) -> Optional[dict]:
    try:
        if not path or not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_jsonl(path: str) -> List[dict]:
    out: List[dict] = []
    if not path or not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    return out


def _latest_by_mtime(paths: List[str]) -> Optional[str]:
    if not paths:
        return None
    paths2 = [p for p in paths if os.path.exists(p)]
    if not paths2:
        return None
    paths2.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths2[0]


def _find_file(run_dir: str, preferred_names: List[str], patterns: List[str]) -> Optional[str]:
    # 1) exact preferred names
    for nm in preferred_names:
        p = os.path.join(run_dir, nm)
        if os.path.exists(p):
            return p
    # 2) glob patterns
    hits: List[str] = []
    for pat in patterns:
        hits.extend(glob.glob(os.path.join(run_dir, pat)))
    return _latest_by_mtime(hits)


def _discover_state_dirs(repo_root: str) -> List[str]:
    # Prefer ./strategies/*/state, but also allow any */state under repo_root
    candidates = []
    p1 = os.path.join(repo_root, "strategies", "*", "state")
    candidates += [p for p in glob.glob(p1) if os.path.isdir(p)]
    # fallback: any state dir
    p2 = os.path.join(repo_root, "**", "state")
    candidates += [p for p in glob.glob(p2, recursive=True) if os.path.isdir(p)]
    # de-dup, stable sort
    uniq = sorted(set(candidates))
    return uniq


def _discover_run_dirs(state_dir: str) -> List[str]:
    # runs are subfolders; also include the state_dir itself as "current"
    runs = [state_dir]
    try:
        for p in sorted(glob.glob(os.path.join(state_dir, "*"))):
            if os.path.isdir(p):
                runs.append(p)
    except Exception:
        pass
    return runs


def _read_cycle_units_from_state_json(run_dir: str) -> Dict[str, str]:
    # Looks for state.json (or any *state*.json) and reads extras.cycle_unit_quote_by_symbol if present.
    state_path = _find_file(run_dir, preferred_names=["state.json"], patterns=["*state*.json"])
    raw = _safe_json_load(state_path or "")
    if not isinstance(raw, dict):
        return {}
    extras = raw.get("extras")
    if not isinstance(extras, dict):
        return {}
    m = extras.get("cycle_unit_quote_by_symbol")
    if isinstance(m, dict):
        return {str(k): str(v) for k, v in m.items()}
    return {}


def _coerce_ts(df: pd.DataFrame) -> pd.DataFrame:
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["date_utc"] = df["ts"].dt.date.astype(str)
    return df


def _to_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _pretty_pct(x: Any) -> str:
    try:
        v = float(x)
        return f"{v*100:.2f}%"
    except Exception:
        return str(x)


@dataclass
class RunFiles:
    run_dir: str
    snapshot_path: Optional[str]
    summary_path: Optional[str]
    trades_path: Optional[str]
    points_path: Optional[str]


def _resolve_run_files(run_dir: str) -> RunFiles:
    snapshot = _find_file(run_dir, preferred_names=["positions_snapshot.json"], patterns=["*snapshot*.json", "positions*.json"])
    summary = _find_file(run_dir, preferred_names=["pnl_summary.json"], patterns=["*summary*.json", "pnl*.json"])
    trades = _find_file(run_dir, preferred_names=["trades.jsonl"], patterns=["*trades*.jsonl", "*.jsonl"])
    points = _find_file(run_dir, preferred_names=["pnl_points.csv"], patterns=["*pnl*points*.csv", "*.csv"])
    # try to avoid rejects.jsonl if possible
    if trades and "reject" in os.path.basename(trades).lower():
        alt = _find_file(run_dir, preferred_names=[], patterns=["*trades*.jsonl"])
        if alt:
            trades = alt
    return RunFiles(run_dir=run_dir, snapshot_path=snapshot, summary_path=summary, trades_path=trades, points_path=points)


# -------------------------
# UI
# -------------------------

st.set_page_config(page_title="Trading Bot Dashboard", layout="wide")

st.title("Trading Bot Dashboard")

repo_root_default = os.getenv("TRADING_BOT_ROOT", str(Path.cwd()))
repo_root = st.sidebar.text_input("Repo root", value=repo_root_default)

state_dirs = _discover_state_dirs(repo_root)
if not state_dirs:
    st.warning("No state directories found. Set 'Repo root' to your trading_bot folder.")
    st.stop()

state_dir = st.sidebar.selectbox("Strategy state directory", options=state_dirs, index=0)
run_dirs = _discover_run_dirs(state_dir)

def _label_run(p: str) -> str:
    if os.path.abspath(p) == os.path.abspath(state_dir):
        return f"(current) {p}"
    return p

selected_runs = st.sidebar.multiselect(
    "Run folder(s)",
    options=run_dirs,
    default=[run_dirs[0]],
    format_func=_label_run,
)

# Load trades across selected runs
all_trades: List[dict] = []
run_file_map: Dict[str, RunFiles] = {}
cycle_units: Dict[str, str] = {}

for rd in selected_runs:
    rf = _resolve_run_files(rd)
    run_file_map[rd] = rf
    # merge cycle units (state.json)
    cycle_units.update(_read_cycle_units_from_state_json(rd))
    # load trades
    if rf.trades_path:
        recs = _load_jsonl(rf.trades_path)
        for r in recs:
            r["_run_dir"] = rd
            r["_trades_file"] = rf.trades_path
        all_trades.extend(recs)

df = pd.DataFrame(all_trades)
if not df.empty:
    df = _coerce_ts(df)
    df = _to_num(df, ["qty", "price", "cum_quote_qty", "realized_delta", "expected_price", "slippage_bps"])
else:
    st.info("No trades found for selected run(s). You can still view snapshot/summary if present.")

# Filters
symbol_list = sorted([s for s in df["symbol"].dropna().unique().tolist()]) if not df.empty and "symbol" in df.columns else []
sel_symbols = st.sidebar.multiselect("Symbols", options=symbol_list, default=symbol_list)

only_fills = st.sidebar.checkbox("Only FILL events", value=True)

if not df.empty and "ts" in df.columns:
    ts_min = df["ts"].min()
    ts_max = df["ts"].max()
    if pd.notnull(ts_min) and pd.notnull(ts_max):
        d0 = ts_min.date()
        d1 = ts_max.date()
        d_from, d_to = st.sidebar.date_input("Date range (UTC)", value=(d0, d1))
    else:
        d_from = d_to = None
else:
    d_from = d_to = None

dff = df.copy()
if not dff.empty:
    if sel_symbols and "symbol" in dff.columns:
        dff = dff[dff["symbol"].isin(sel_symbols)]
    if only_fills and "event" in dff.columns:
        dff = dff[dff["event"] == "FILL"]
    if d_from and d_to and "ts" in dff.columns:
        start = pd.Timestamp(d_from, tz="UTC")
        end = pd.Timestamp(d_to, tz="UTC") + pd.Timedelta(days=1)
        dff = dff[(dff["ts"] >= start) & (dff["ts"] < end)]

default_cycle_unit = st.sidebar.number_input("Default cycle unit quote (if missing)", min_value=1.0, value=1500.0, step=100.0)

# Pick “latest snapshot/summary” among selected runs
latest_snapshot_path = _latest_by_mtime([run_file_map[r].snapshot_path for r in selected_runs if run_file_map[r].snapshot_path] or [])
latest_summary_path = _latest_by_mtime([run_file_map[r].summary_path for r in selected_runs if run_file_map[r].summary_path] or [])
snapshot = _safe_json_load(latest_snapshot_path or "")
summary = _safe_json_load(latest_summary_path or "")

# -------------------------
# Top metrics
# -------------------------

colA, colB, colC, colD = st.columns(4)

if isinstance(summary, dict):
    pv = summary.get("portfolio_value")
    ppnl = summary.get("portfolio_pnl")
    ppct = summary.get("portfolio_pnl_pct")
    created = summary.get("created") if isinstance(summary.get("created"), dict) else {}
    st_total = created.get("strategy_total_now")
    st_real_td = created.get("strategy_realized_today")

    with colA:
        st.metric("Portfolio Value", str(pv) if pv is not None else "—")
    with colB:
        st.metric("Portfolio PnL", str(ppnl) if ppnl is not None else "—", delta=_pretty_pct(ppct) if ppct is not None else None)
    with colC:
        st.metric("Strategy Total (now)", str(st_total) if st_total is not None else "—")
    with colD:
        st.metric("Strategy Realized Today (UTC)", str(st_real_td) if st_real_td is not None else "—")
else:
    with colA:
        st.metric("Portfolio Value", "—")
    with colB:
        st.metric("Portfolio PnL", "—")
    with colC:
        st.metric("Strategy Total (now)", "—")
    with colD:
        st.metric("Strategy Realized Today (UTC)", "—")

# -------------------------
# Snapshot view
# -------------------------

st.subheader("Current Snapshot (latest)")

if isinstance(snapshot, dict):
    c1, c2 = st.columns([1.1, 1.0])

    with c1:
        # Per-symbol table
        sym_map = snapshot.get("symbols") if isinstance(snapshot.get("symbols"), dict) else {}
        rows = []
        for sym, d in (sym_map or {}).items():
            if not isinstance(d, dict):
                continue
            r = {"symbol": sym}
            r.update(d)
            rows.append(r)
        if rows:
            st.dataframe(pd.DataFrame(rows))
        else:
            st.info("No symbols in snapshot.")

    with c2:
        created = snapshot.get("created") if isinstance(snapshot.get("created"), dict) else {}
        deployed = snapshot.get("deployed") if isinstance(snapshot.get("deployed"), dict) else {}
        cycles_today = snapshot.get("cycles_today") if isinstance(snapshot.get("cycles_today"), dict) else {}
        cycles_all = snapshot.get("cycles_all_time") if isinstance(snapshot.get("cycles_all_time"), dict) else {}
        holdings = snapshot.get("holdings") if isinstance(snapshot.get("holdings"), dict) else {}

        st.markdown("**Created**")
        st.json(created)

        st.markdown("**Deployed**")
        st.json(deployed)

        st.markdown("**Cycles (Today UTC)**")
        st.json(cycles_today)

        st.markdown("**Cycles (All-time)**")
        st.json(cycles_all)

        st.markdown("**Holdings**")
        st.json(holdings)

    with st.expander("Raw positions_snapshot.json"):
        st.code(json.dumps(snapshot, indent=2), language="json")
else:
    st.info("No snapshot found in selected run(s).")

# -------------------------
# PnL points (optional)
# -------------------------

st.subheader("PnL Points (optional)")

points_paths = [run_file_map[r].points_path for r in selected_runs if run_file_map[r].points_path]
points_path = _latest_by_mtime([p for p in points_paths if p])
if points_path and os.path.exists(points_path):
    try:
        pdf = pd.read_csv(points_path)
        if "ts" in pdf.columns:
            pdf["ts"] = pd.to_datetime(pdf["ts"], utc=True, errors="coerce")
        st.caption(f"Using: {points_path}")
        if not pdf.empty and "portfolio_value" in pdf.columns and "ts" in pdf.columns:
            pdf = pdf.sort_values("ts")
            st.line_chart(pdf.set_index("ts")["portfolio_value"])
        if not pdf.empty and "strategy_total" in pdf.columns and "ts" in pdf.columns:
            st.line_chart(pdf.set_index("ts")["strategy_total"])
        with st.expander("pnl_points.csv"):
            st.dataframe(pdf)
    except Exception as e:
        st.warning(f"Failed reading pnl_points.csv: {e}")
else:
    st.info("No pnl_points.csv found (this is optional).")

# -------------------------
# Trades view + daily summary
# -------------------------

st.subheader("Trades (filtered)")

if dff.empty:
    st.info("No trades match current filters.")
else:
    st.caption(f"Trades loaded: {len(df)} | After filters: {len(dff)}")
    # Slippage metrics
    slp = dff.dropna(subset=["slippage_bps"]) if "slippage_bps" in dff.columns else pd.DataFrame()
    if not slp.empty:
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Avg slippage (bps)", f"{slp['slippage_bps'].mean():.2f}")
        with m2:
            st.metric("Median slippage (bps)", f"{slp['slippage_bps'].median():.2f}")
        with m3:
            st.metric("Worst slippage (bps)", f"{slp['slippage_bps'].max():.2f}")

    cols = ["ts", "symbol", "side", "qty", "expected_price", "price", "slippage_bps", "cum_quote_qty", "realized_delta", "reason", "order_id", "_run_dir"]
    cols = [c for c in cols if c in dff.columns]
    st.dataframe(dff.sort_values("ts")[cols])

    # Daily aggregation
    st.subheader("Daily Summary (UTC, filtered)")

    # per-symbol unit lookup
    def _unit_for(sym: str) -> float:
        # 1) from state.json extras if present
        if sym in cycle_units:
            try:
                return float(cycle_units[sym])
            except Exception:
                pass
        # 2) from latest snapshot cycles_today if present
        try:
            if isinstance(snapshot, dict):
                ct = snapshot.get("cycles_today", {})
                ps = ct.get("per_symbol", {}) if isinstance(ct, dict) else {}
                if sym in ps:
                    u = ps[sym].get("cycle_unit_quote")
                    if u:
                        return float(u)
        except Exception:
            pass
        return float(default_cycle_unit)

    fills = dff.copy()
    if "event" in fills.columns:
        # in case only_fills is off
        fills = fills[fills["event"] == "FILL"] if "FILL" in fills["event"].unique().tolist() else fills

    if not fills.empty and "date_utc" in fills.columns and "symbol" in fills.columns:
        # buy/sell quote sums
        buy_mask = (fills["side"].astype(str).str.upper() == "BUY") if "side" in fills.columns else False
        sell_mask = (fills["side"].astype(str).str.upper() == "SELL") if "side" in fills.columns else False

        # group
        g = fills.groupby(["date_utc", "symbol"], dropna=True)

        daily = g.agg(
            fills=("order_id", "count") if "order_id" in fills.columns else ("symbol", "count"),
            realized=("realized_delta", "sum") if "realized_delta" in fills.columns else ("symbol", "count"),
            avg_slip_bps=("slippage_bps", "mean") if "slippage_bps" in fills.columns else ("symbol", "count"),
        ).reset_index()

        # add buy_quote/sell_quote
        if "cum_quote_qty" in fills.columns:
            bq = fills[buy_mask].groupby(["date_utc", "symbol"])["cum_quote_qty"].sum().rename("buy_quote")
            sq = fills[sell_mask].groupby(["date_utc", "symbol"])["cum_quote_qty"].sum().rename("sell_quote")
            daily = daily.merge(bq.reset_index(), on=["date_utc", "symbol"], how="left")
            daily = daily.merge(sq.reset_index(), on=["date_utc", "symbol"], how="left")
            daily["buy_quote"] = daily["buy_quote"].fillna(0.0)
            daily["sell_quote"] = daily["sell_quote"].fillna(0.0)
            daily["cycle_quote"] = daily[["buy_quote", "sell_quote"]].min(axis=1)
            daily["cycle_unit_quote"] = daily["symbol"].apply(_unit_for)
            daily["cycles_est"] = daily["cycle_quote"] / daily["cycle_unit_quote"]

        st.dataframe(daily.sort_values(["date_utc", "symbol"]))
    else:
        st.info("Not enough fields to compute daily summary (need ts/date_utc, symbol).")

# -------------------------
# Raw summary
# -------------------------

with st.expander("Raw pnl_summary.json (latest)"):
    if isinstance(summary, dict):
        st.code(json.dumps(summary, indent=2), language="json")
    else:
        st.info("No pnl_summary.json found.")