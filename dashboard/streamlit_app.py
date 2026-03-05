# streamlit_app.py  (MANUAL REFRESH ONLY)

from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def _tail_jsonl(path: str, max_lines: int = 50000) -> List[dict]:
    """Tail last max_lines of a jsonl file without reading whole file."""
    if not path or not os.path.exists(path):
        return []
    out: List[dict] = []
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            chunk = 1024 * 1024  # 1MB
            data = b""
            pos = size
            while pos > 0 and data.count(b"\n") < max_lines:
                step = chunk if pos >= chunk else pos
                pos -= step
                f.seek(pos)
                data = f.read(step) + data
            text = data.decode("utf-8", errors="ignore")
            lines = text.splitlines()[-max_lines:]
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        return []
    return out


def _latest_by_mtime(paths: List[str]) -> Optional[str]:
    paths2 = [p for p in paths if p and os.path.exists(p)]
    if not paths2:
        return None
    paths2.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths2[0]


def _find_file(run_dir: str, preferred_names: List[str], patterns: List[str]) -> Optional[str]:
    for nm in preferred_names:
        p = os.path.join(run_dir, nm)
        if os.path.exists(p):
            return p
    hits: List[str] = []
    for pat in patterns:
        hits.extend(glob.glob(os.path.join(run_dir, pat)))
    return _latest_by_mtime(hits)


def _discover_state_dirs(repo_root: str) -> List[str]:
    candidates: List[str] = []
    candidates += [p for p in glob.glob(os.path.join(repo_root, "strategies", "*", "state")) if os.path.isdir(p)]
    candidates += [p for p in glob.glob(os.path.join(repo_root, "**", "state"), recursive=True) if os.path.isdir(p)]
    return sorted(set(candidates))


def _discover_run_dirs(state_dir: str) -> List[str]:
    runs = [state_dir]
    for p in sorted(glob.glob(os.path.join(state_dir, "*"))):
        if os.path.isdir(p):
            runs.append(p)
    return runs


def _read_cycle_units_from_state_json(run_dir: str) -> Dict[str, str]:
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


@dataclass
class RunFiles:
    run_dir: str
    snapshot_path: Optional[str]
    summary_path: Optional[str]
    trades_path: Optional[str]
    manual_path: Optional[str]


def _resolve_run_files(run_dir: str) -> RunFiles:
    snapshot = _find_file(run_dir, ["positions_snapshot.json"], ["*snapshot*.json", "positions*.json"])
    summary = _find_file(run_dir, ["pnl_summary.json"], ["*summary*.json", "pnl*.json"])
    trades = _find_file(run_dir, ["trades.jsonl"], ["*trades*.jsonl", "*.jsonl"])
    manual = _find_file(run_dir, ["manual_adjustments.jsonl"], ["*manual*adjust*.jsonl", "*manual*.jsonl"])
    if trades and "reject" in os.path.basename(trades).lower():
        alt = _find_file(run_dir, [], ["*trades*.jsonl"])
        if alt:
            trades = alt
    return RunFiles(run_dir, snapshot, summary, trades, manual)


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

def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _sum_cycles(store: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    per = store.get("per_symbol", {}) if isinstance(store, dict) else {}
    total_cycles = 0.0
    total_cycle_quote = 0.0
    has_cycles = False
    has_quote = False
    for rec in (per or {}).values():
        if not isinstance(rec, dict):
            continue
        ce = _safe_float(rec.get("cycles_est"))
        if ce is not None:
            total_cycles += ce
            has_cycles = True
        cq = _safe_float(rec.get("cycle_quote"))
        if cq is not None:
            total_cycle_quote += cq
            has_quote = True
    return (total_cycles if has_cycles else None, total_cycle_quote if has_quote else None)


# -------------------------
# Manual-refresh state
# -------------------------

def _ensure_session_defaults() -> None:
    ss = st.session_state
    ss.setdefault("loaded", False)
    ss.setdefault("data_snapshot", None)
    ss.setdefault("data_summary", None)
    ss.setdefault("data_trades_df", pd.DataFrame())
    ss.setdefault("data_manual_df", pd.DataFrame())
    ss.setdefault("data_cycle_units", {})
    ss.setdefault("data_run_files", {})
    ss.setdefault("last_loaded_at", None)


def _load_all(repo_root: str, selected_runs: List[str], max_lines: int) -> None:
    run_file_map: Dict[str, RunFiles] = {}
    cycle_units: Dict[str, str] = {}
    trades: List[dict] = []
    manuals: List[dict] = []

    for rd in selected_runs:
        rf = _resolve_run_files(rd)
        run_file_map[rd] = rf
        cycle_units.update(_read_cycle_units_from_state_json(rd))
        if rf.trades_path:
            recs = _tail_jsonl(rf.trades_path, max_lines=max_lines)
            for r in recs:
                r["_run_dir"] = rd
                r["_trades_file"] = rf.trades_path
            trades.extend(recs)
        if rf.manual_path:
            recs = _tail_jsonl(rf.manual_path, max_lines=max_lines)
            for r in recs:
                r["_run_dir"] = rd
                r["_manual_file"] = rf.manual_path
            manuals.extend(recs)

    df = pd.DataFrame(trades)
    if not df.empty:
        df = _coerce_ts(df)
        df = _to_num(df, ["qty", "price", "cum_quote_qty", "realized_delta", "expected_price", "slippage_bps"])

    # pick latest snapshot/summary among selected runs
    latest_snapshot_path = _latest_by_mtime([run_file_map[r].snapshot_path for r in selected_runs if run_file_map[r].snapshot_path] or [])
    latest_summary_path = _latest_by_mtime([run_file_map[r].summary_path for r in selected_runs if run_file_map[r].summary_path] or [])

    snapshot = _safe_json_load(latest_snapshot_path or "")
    summary = _safe_json_load(latest_summary_path or "")
    manual_df = pd.DataFrame(manuals)
    if not manual_df.empty:
        manual_df = _coerce_ts(manual_df)

    st.session_state["loaded"] = True
    st.session_state["data_snapshot"] = snapshot
    st.session_state["data_summary"] = summary
    st.session_state["data_trades_df"] = df
    st.session_state["data_manual_df"] = manual_df
    st.session_state["data_cycle_units"] = cycle_units
    st.session_state["data_run_files"] = {k: run_file_map[k].__dict__ for k in run_file_map}
    st.session_state["last_loaded_at"] = pd.Timestamp.utcnow().isoformat() + "Z"


# -------------------------
# UI
# -------------------------

st.set_page_config(page_title="Trading Bot Dashboard", layout="wide")
_ensure_session_defaults()

st.title("Trading Bot Dashboard (Manual Refresh Only)")

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

max_lines = int(st.sidebar.number_input("Max trades lines to load (tail)", min_value=1000, value=50000, step=5000))

col_btn1, col_btn2 = st.sidebar.columns(2)
with col_btn1:
    if st.button("Refresh", type="primary"):
        with st.spinner("Loading data..."):
            _load_all(repo_root, selected_runs, max_lines=max_lines)
with col_btn2:
    if st.button("Clear"):
        for k in ["loaded", "data_snapshot", "data_summary", "data_trades_df", "data_manual_df", "data_cycle_units", "data_run_files", "last_loaded_at"]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

if not st.session_state.get("loaded"):
    st.info("Click **Refresh** to load data. No background refresh is performed.")
    st.stop()

snapshot = st.session_state.get("data_snapshot")
summary = st.session_state.get("data_summary")
df = st.session_state.get("data_trades_df")
manual_df = st.session_state.get("data_manual_df")
cycle_units = st.session_state.get("data_cycle_units") or {}
last_loaded_at = st.session_state.get("last_loaded_at")

st.caption(f"Loaded at: {last_loaded_at}")

# -------------------------
# Filters (do NOT reload data)
# -------------------------

if isinstance(df, pd.DataFrame) and not df.empty:
    symbol_list = sorted(df["symbol"].dropna().unique().tolist()) if "symbol" in df.columns else []
else:
    symbol_list = []

sel_symbols = st.sidebar.multiselect("Symbols (filter)", options=symbol_list, default=symbol_list)
only_fills = st.sidebar.checkbox("Only FILL events", value=True)

dff = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
if not dff.empty:
    if sel_symbols and "symbol" in dff.columns:
        dff = dff[dff["symbol"].isin(sel_symbols)]
    if only_fills and "event" in dff.columns:
        dff = dff[dff["event"] == "FILL"]

    if "ts" in dff.columns and dff["ts"].notna().any():
        ts_min = dff["ts"].min()
        ts_max = dff["ts"].max()
        d0 = ts_min.date()
        d1 = ts_max.date()
        d_from, d_to = st.sidebar.date_input("Date range (UTC)", value=(d0, d1))
        start = pd.Timestamp(d_from, tz="UTC")
        end = pd.Timestamp(d_to, tz="UTC") + pd.Timedelta(days=1)
        dff = dff[(dff["ts"] >= start) & (dff["ts"] < end)]

default_cycle_unit = float(st.sidebar.number_input("Default cycle unit quote", min_value=1.0, value=1500.0, step=100.0))

def _unit_for(sym: str) -> float:
    if sym in cycle_units:
        try:
            return float(cycle_units[sym])
        except Exception:
            pass
    # try latest snapshot cycles_today
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
    return default_cycle_unit


# -------------------------
# Top metrics
# -------------------------

colA, colB, colC, colD, colE = st.columns(5)

if isinstance(summary, dict):
    pv = summary.get("portfolio_value")
    ppnl = summary.get("portfolio_pnl")
    ppct = summary.get("portfolio_pnl_pct")
    created = summary.get("created") if isinstance(summary.get("created"), dict) else {}
    bot = summary.get("bot") if isinstance(summary.get("bot"), dict) else {}
    non_strategy = summary.get("non_strategy") if isinstance(summary.get("non_strategy"), dict) else {}

    st_total = bot.get("total_now") if bot.get("total_now") is not None else created.get("strategy_total_now")
    st_real_td = bot.get("realized_today") if bot.get("realized_today") is not None else created.get("strategy_realized_today")
    nsv = non_strategy.get("value_est")
    nsp = non_strategy.get("value_pct_est")

    colA.metric("Portfolio Value", str(pv) if pv is not None else "—")
    colB.metric("Portfolio PnL", str(ppnl) if ppnl is not None else "—", delta=_pretty_pct(ppct) if ppct is not None else None)
    colC.metric("Bot Total (now)", str(st_total) if st_total is not None else "—")
    colD.metric("Bot Realized Today (UTC)", str(st_real_td) if st_real_td is not None else "—")
    colE.metric("Non-Strategy Value (est)", str(nsv) if nsv is not None else "—", delta=_pretty_pct(nsp) if nsp is not None else None)
else:
    colA.metric("Portfolio Value", "—")
    colB.metric("Portfolio PnL", "—")
    colC.metric("Bot Total (now)", "—")
    colD.metric("Bot Realized Today (UTC)", "—")
    colE.metric("Non-Strategy Value (est)", "—")

# -------------------------
# Bot summary panel
# -------------------------

st.subheader("Bot Summary")

if isinstance(summary, dict):
    bot = summary.get("bot") if isinstance(summary.get("bot"), dict) else {}
    created = summary.get("created") if isinstance(summary.get("created"), dict) else {}
    bot_total = bot.get("total_now") if bot.get("total_now") is not None else created.get("strategy_total_now")
    bot_real_today = bot.get("realized_today") if bot.get("realized_today") is not None else created.get("strategy_realized_today")
    bot_real_all = bot.get("realized_all_time") if bot.get("realized_all_time") is not None else created.get("strategy_realized_all_time")
else:
    bot_total = bot_real_today = bot_real_all = None

cycles_today = snapshot.get("cycles_today", {}) if isinstance(snapshot, dict) else {}
cycles_all = snapshot.get("cycles_all_time", {}) if isinstance(snapshot, dict) else {}
ct_est, ct_quote = _sum_cycles(cycles_today)
ca_est, ca_quote = _sum_cycles(cycles_all)

s1, s2, s3, s4, s5 = st.columns(5)
s1.metric("Bot PnL Today (realized)", str(bot_real_today) if bot_real_today is not None else "—")
s2.metric("Bot PnL All-time (realized)", str(bot_real_all) if bot_real_all is not None else "—")
s3.metric("Bot Total PnL (now)", str(bot_total) if bot_total is not None else "—")
s4.metric("Cycles Today (est)", f"{ct_est:.4f}" if ct_est is not None else "—")
s5.metric("Cycles All-time (est)", f"{ca_est:.4f}" if ca_est is not None else "—")


# -------------------------
# Snapshot view
# -------------------------

st.subheader("Current Snapshot (latest loaded)")

if isinstance(snapshot, dict):
    c1, c2 = st.columns([1.2, 1.0])

    with c1:
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
        st.markdown("**Bot**")
        st.json(snapshot.get("bot", {}))
        st.markdown("**Non-Strategy (est)**")
        st.json(snapshot.get("non_strategy", {}))
        st.markdown("**Manual Inventory**")
        st.json(snapshot.get("manual_inventory_by_symbol", {}))
        st.markdown("**Created**")
        st.json(snapshot.get("created", {}))
        st.markdown("**Deployed**")
        st.json(snapshot.get("deployed", {}))
        st.markdown("**Cycles (Today UTC)**")
        st.json(snapshot.get("cycles_today", {}))
        st.markdown("**Cycles (All-time)**")
        st.json(snapshot.get("cycles_all_time", {}))
        st.markdown("**Holdings**")
        st.json(snapshot.get("holdings", {}))

    with st.expander("Raw positions_snapshot.json (loaded)"):
        st.code(json.dumps(snapshot, indent=2), language="json")
else:
    st.info("No snapshot loaded.")


# -------------------------
# Trades view + daily summary
# -------------------------

st.subheader("Trades (filtered)")

if dff.empty:
    st.info("No trades match current filters.")
else:
    st.caption(f"Trades loaded: {len(df)} | After filters: {len(dff)}")

    # Slippage stats
    if "slippage_bps" in dff.columns:
        slp = dff.dropna(subset=["slippage_bps"])
        if not slp.empty:
            m1, m2, m3 = st.columns(3)
            m1.metric("Avg slippage (bps)", f"{slp['slippage_bps'].mean():.2f}")
            m2.metric("Median slippage (bps)", f"{slp['slippage_bps'].median():.2f}")
            m3.metric("Worst slippage (bps)", f"{slp['slippage_bps'].max():.2f}")

    cols = ["ts", "symbol", "side", "qty", "expected_price", "price", "slippage_bps",
            "cum_quote_qty", "realized_delta", "reason", "order_id", "_run_dir"]
    cols = [c for c in cols if c in dff.columns]
    st.dataframe(dff.sort_values("ts")[cols], use_container_width=True)

    st.subheader("Daily Summary (UTC, filtered)")

    fills = dff.copy()
    if "event" in fills.columns:
        fills = fills[fills["event"] == "FILL"] if "FILL" in fills["event"].unique().tolist() else fills

    if not fills.empty and "date_utc" in fills.columns and "symbol" in fills.columns:
        buy_mask = fills["side"].astype(str).str.upper().eq("BUY") if "side" in fills.columns else False
        sell_mask = fills["side"].astype(str).str.upper().eq("SELL") if "side" in fills.columns else False

        g = fills.groupby(["date_utc", "symbol"], dropna=True)

        daily = g.agg(
            fills=("order_id", "count") if "order_id" in fills.columns else ("symbol", "count"),
            realized=("realized_delta", "sum") if "realized_delta" in fills.columns else ("symbol", "count"),
            avg_slip_bps=("slippage_bps", "mean") if "slippage_bps" in fills.columns else ("symbol", "count"),
        ).reset_index()

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

        st.dataframe(daily.sort_values(["date_utc", "symbol"]), use_container_width=True)

# -------------------------
# Manual adjustments (balance reconcile)
# -------------------------

st.subheader("Manual Adjustments (balance reconcile)")

if not isinstance(manual_df, pd.DataFrame) or manual_df.empty:
    st.info("No manual adjustments loaded.")
else:
    cols = ["ts", "symbol", "manual_delta", "manual_qty", "base_total", "bot_net_qty", "px", "reason", "_run_dir"]
    cols = [c for c in cols if c in manual_df.columns]
    st.dataframe(manual_df.sort_values("ts")[cols], use_container_width=True)
    else:
        st.info("Not enough fields to compute daily summary.")

with st.expander("Raw pnl_summary.json (loaded)"):
    if isinstance(summary, dict):
        st.code(json.dumps(summary, indent=2), language="json")
    else:
        st.info("No summary loaded.")
