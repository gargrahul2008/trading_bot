# streamlit_app.py  (MANUAL REFRESH ONLY)
#
# Uses ONLY these two manual sources (as specified in config JSON):
#   1) paths.manual_positions_file   (or top-level manual_positions_file)
#   2) paths.capital_flows_file      (or top-level capital_flows_file)
#
# It IGNOREs:
#   - manual_adjustments.jsonl
#   - any snapshot-derived manual maps
#   - any in-dashboard manual editor inputs
#   - pnl_points.csv (not used)
#
# NOTE:
# - The app still re-runs on UI interaction (Streamlit behavior),
#   but it will NOT reload files from disk unless you click Refresh.

from __future__ import annotations

import glob
import io
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


def _coerce_ts(df: pd.DataFrame) -> pd.DataFrame:
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["date_utc"] = df["ts"].dt.date.astype(str)
    return df


def _to_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = df[c]
            if s.dtype == "O":
                s = (
                    s.astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("_", "", regex=False)
                    .str.strip()
                )
                s = s.replace({"": None, "None": None, "nan": None, "NaN": None})
            df[c] = pd.to_numeric(s, errors="coerce")
    return df


def _pretty_pct(x: Any) -> str:
    try:
        v = float(x)
        return f"{v*100:.2f}%"
    except Exception:
        return str(x)


def _fmt_num(x: Any, digits: int = 2) -> str:
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return "—"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if isinstance(x, str):
            x = x.replace(",", "").replace("_", "").strip()
        return float(x)
    except Exception:
        return None


def _normalize_capital_delta(amount: Any, flow_type: Any = None) -> Optional[float]:
    """
    Normalize amount sign from optional flow type.
    - add/deposit/in/credit => +abs(amount)
    - remove/withdraw/out/debit => -abs(amount)
    - unknown type => amount as-is
    """
    amt = _safe_float(amount)
    if amt is None:
        return None
    t = str(flow_type or "").strip().upper().replace("-", "_").replace(" ", "_")
    add_types = {"ADD", "ADDED", "DEPOSIT", "DEPOSITED", "IN", "CREDIT", "CR"}
    remove_types = {"REMOVE", "REMOVED", "WITHDRAW", "WITHDRAWAL", "WITHDRAWN", "OUT", "DEBIT", "DR"}
    if t in add_types or t.startswith("DEPOSIT"):
        return abs(amt)
    if t in remove_types or t.startswith("WITHDRAW"):
        return -abs(amt)
    return amt


def _parse_ts_user_ist_to_utc(x: Any) -> pd.Timestamp:
    """
    Parse user-entered timestamp.
    If timezone is missing, treat it as IST (Asia/Kolkata), then convert to UTC.
    Accepted examples:
      - 2026-03-09 14:30
      - 2026-03-09 14:30:45
      - 2026-03-09T14:30
      - 2026-03-09 14:30 IST
    """
    if x is None:
        return pd.NaT
    s = str(x).strip()
    if not s or s.lower() in {"none", "nan", "nat"}:
        return pd.NaT
    s = s.replace(" IST", "").replace(" ist", "").replace("IST", "").strip()
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        try:
            ts = ts.tz_localize("Asia/Kolkata")
        except Exception:
            return pd.NaT
    return ts.tz_convert("UTC")


def _norm_symbol(sym: Any) -> str:
    s = str(sym or "").upper()
    return "".join(ch for ch in s if ch.isalnum())


def _resolve_manual_cmp(symbol: Any, latest_px_norm: Dict[str, float]) -> tuple[Optional[float], Optional[str]]:
    """
    Resolve manual symbol to CMP robustly.
    Handles exact matches and base-symbol inputs like ETH -> ETHUSDC (if unambiguous).
    """
    n = _norm_symbol(symbol)
    if not n:
        return None, None
    if n in latest_px_norm:
        return latest_px_norm[n], n

    # Base symbol fallback: ETH -> ETHUSDT/ETHUSDC/... choose only if unambiguous
    cands = [k for k in latest_px_norm.keys() if k.startswith(n) or n.startswith(k)]
    if not cands:
        return None, None
    if len(cands) == 1:
        k = cands[0]
        return latest_px_norm.get(k), k

    # Prefer common quote suffixes if still ambiguous
    for q in ("USDT", "USDC", "USD", "INR", "BTC", "ETH"):
        filt = [k for k in cands if k.endswith(q)]
        if len(filt) == 1:
            k = filt[0]
            return latest_px_norm.get(k), k

    return None, None


def _load_manual_positions_file(path: str) -> pd.DataFrame:
    """
    Manual positions file format (CSV or JSON):
      Columns/keys:
        - ts (optional; if missing, it's active immediately)
        - symbol
        - qty
        - buy_price
    """
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=["ts", "symbol", "qty", "buy_price"])

    ext = Path(path).suffix.lower()
    try:
        if ext == ".csv":
            raw_df = pd.read_csv(path)
            if raw_df.empty:
                return pd.DataFrame(columns=["ts", "symbol", "qty", "buy_price"])
            out = pd.DataFrame()
            out["ts"] = raw_df.get("ts") if "ts" in raw_df.columns else raw_df.get("date")
            out["symbol"] = raw_df.get("symbol")
            out["qty"] = raw_df.get("qty") if "qty" in raw_df.columns else raw_df.get("quantity")
            out["buy_price"] = raw_df.get("buy_price") if "buy_price" in raw_df.columns else raw_df.get("price")
            out = out.dropna(subset=["symbol", "qty", "buy_price"])
            out["symbol"] = out["symbol"].astype(str).str.strip()
            out = _to_num(out, ["qty", "buy_price"])
            out = out.dropna(subset=["qty", "buy_price"])
            return out[["ts", "symbol", "qty", "buy_price"]]

        raw = _safe_json_load(path)
        rows: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            for rec in raw:
                if isinstance(rec, dict):
                    rows.append({
                        "ts": rec.get("ts") or rec.get("date"),
                        "symbol": rec.get("symbol"),
                        "qty": rec.get("qty") or rec.get("quantity"),
                        "buy_price": rec.get("buy_price") or rec.get("price") or rec.get("avg_price"),
                    })
        elif isinstance(raw, dict):
            arr = raw.get("positions") or raw.get("manual_positions") or raw.get("data") or raw.get("lots")
            if isinstance(arr, list):
                for rec in arr:
                    if isinstance(rec, dict):
                        rows.append({
                            "ts": rec.get("ts") or rec.get("date"),
                            "symbol": rec.get("symbol"),
                            "qty": rec.get("qty") or rec.get("quantity"),
                            "buy_price": rec.get("buy_price") or rec.get("price") or rec.get("avg_price"),
                        })
        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(columns=["ts", "symbol", "qty", "buy_price"])
        out = out.dropna(subset=["symbol", "qty", "buy_price"])
        out["symbol"] = out["symbol"].astype(str).str.strip()
        out = _to_num(out, ["qty", "buy_price"])
        out = out.dropna(subset=["qty", "buy_price"])
        return out[["ts", "symbol", "qty", "buy_price"]]
    except Exception:
        return pd.DataFrame(columns=["ts", "symbol", "qty", "buy_price"])


def _load_capital_flows_file(path: str) -> pd.DataFrame:
    """
    Capital flows file format (CSV or JSON):
      Columns/keys:
        - ts (recommended, but can be blank; blank rows will be ignored)
        - delta OR amount
        - type (optional): deposit/withdraw etc
        - note (optional)
    Returns normalized columns:
      - ts (string; will be normalized to UTC ISO when possible)
      - delta (float)
      - note (optional)
    """
    if not path or not os.path.exists(path):
        return pd.DataFrame(columns=["ts", "delta", "note"])

    ext = Path(path).suffix.lower()
    try:
        if ext == ".csv":
            raw_df = pd.read_csv(path)
            if raw_df.empty:
                return pd.DataFrame(columns=["ts", "delta", "note"])
            out = pd.DataFrame()
            out["ts"] = raw_df.get("ts") if "ts" in raw_df.columns else raw_df.get("date")
            if "delta" in raw_df.columns:
                if "type" in raw_df.columns:
                    out["delta"] = [
                        _normalize_capital_delta(d, typ)
                        for d, typ in zip(raw_df["delta"], raw_df["type"])
                    ]
                else:
                    out["delta"] = raw_df.get("delta")
            elif "amount" in raw_df.columns:
                if "type" in raw_df.columns:
                    out["delta"] = [
                        _normalize_capital_delta(amt, typ)
                        for amt, typ in zip(raw_df["amount"], raw_df["type"])
                    ]
                else:
                    out["delta"] = raw_df.get("amount")
            else:
                out["delta"] = None
            out["note"] = raw_df.get("note")
            out = _to_num(out, ["delta"])
            out = out.dropna(subset=["delta"])
            out["ts_utc"] = out["ts"].apply(_parse_ts_user_ist_to_utc)
            out = out.dropna(subset=["ts_utc"])
            out["ts"] = out["ts_utc"].astype(str)
            return out[["ts", "delta", "note"]]

        raw = _safe_json_load(path)
        rows: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            for rec in raw:
                if not isinstance(rec, dict):
                    continue
                delta = rec.get("delta")
                if delta is not None:
                    delta = _normalize_capital_delta(delta, rec.get("type"))
                elif rec.get("amount") is not None:
                    delta = _normalize_capital_delta(rec.get("amount"), rec.get("type"))
                rows.append({"ts": rec.get("ts") or rec.get("date"), "delta": delta, "note": rec.get("note")})
        elif isinstance(raw, dict):
            arr = raw.get("flows") or raw.get("capital_flows") or raw.get("data")
            if isinstance(arr, list):
                for rec in arr:
                    if not isinstance(rec, dict):
                        continue
                    delta = rec.get("delta")
                    if delta is not None:
                        delta = _normalize_capital_delta(delta, rec.get("type"))
                    elif rec.get("amount") is not None:
                        delta = _normalize_capital_delta(rec.get("amount"), rec.get("type"))
                    rows.append({"ts": rec.get("ts") or rec.get("date"), "delta": delta, "note": rec.get("note")})

        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(columns=["ts", "delta", "note"])
        out = _to_num(out, ["delta"])
        out = out.dropna(subset=["delta"])
        out["ts_utc"] = out["ts"].apply(_parse_ts_user_ist_to_utc)
        out = out.dropna(subset=["ts_utc"])
        out["ts"] = out["ts_utc"].astype(str)
        return out[["ts", "delta", "note"]]
    except Exception:
        return pd.DataFrame(columns=["ts", "delta", "note"])


def _infer_strategy_dir_from_state_dir(state_dir: str) -> Optional[str]:
    """
    Typical:
      .../strategies/<strategy>/state[/run_subdir]
    We want .../strategies/<strategy>
    """
    p = Path(state_dir).resolve()
    # if we are inside .../state/<run>, go up to .../state
    if p.name != "state":
        # try find a parent named "state"
        for parent in p.parents:
            if parent.name == "state":
                p = parent
                break
    # strategy dir is parent of "state"
    if p.name == "state":
        return str(p.parent)
    return None


def _find_config_in_strategy_dir(strategy_dir: str) -> Optional[str]:
    """
    Find config JSON in the strategy folder.
    Preference:
      - config.json
      - config*.json (latest mtime)
    """
    if not strategy_dir or not os.path.isdir(strategy_dir):
        return None
    direct = os.path.join(strategy_dir, "config.json")
    if os.path.exists(direct):
        return direct
    hits = glob.glob(os.path.join(strategy_dir, "config*.json"))
    return _latest_by_mtime(hits)


def _get_manual_paths_from_config(config_path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Reads:
      paths.manual_positions_file
      paths.capital_flows_file
    Also supports top-level keys for robustness.
    """
    cfg = _safe_json_load(config_path) or {}
    base_dir = os.path.dirname(os.path.abspath(config_path))

    def _get_key(key: str) -> Optional[str]:
        # prefer paths.*
        paths = cfg.get("paths") if isinstance(cfg.get("paths"), dict) else {}
        v = paths.get(key) if isinstance(paths, dict) else None
        if not v:
            v = cfg.get(key)
        if not isinstance(v, str) or not v.strip():
            return None
        p = v.strip()
        if not os.path.isabs(p):
            p = os.path.normpath(os.path.join(base_dir, p))
        return p

    mp = _get_key("manual_positions_file")
    cf = _get_key("capital_flows_file")
    return (mp if mp and os.path.exists(mp) else mp, cf if cf and os.path.exists(cf) else cf)


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
    ss.setdefault("data_price_df", pd.DataFrame())
    ss.setdefault("data_pnl_daily_df", pd.DataFrame())
    ss.setdefault("data_price_daily_df", pd.DataFrame())
    ss.setdefault("data_manual_positions_df", pd.DataFrame(columns=["ts", "symbol", "qty", "buy_price"]))
    ss.setdefault("data_manual_positions_path", None)
    ss.setdefault("data_capital_flows_df", pd.DataFrame(columns=["ts", "delta", "note"]))
    ss.setdefault("data_capital_flows_path", None)
    ss.setdefault("data_config_path", None)
    ss.setdefault("last_loaded_at", None)


@dataclass
class RunFiles:
    run_dir: str
    snapshot_path: Optional[str]
    summary_path: Optional[str]
    trades_path: Optional[str]
    price_points_path: Optional[str]
    pnl_daily_path: Optional[str]
    price_daily_path: Optional[str]


def _resolve_run_files(run_dir: str) -> RunFiles:
    snapshot = _find_file(run_dir, ["positions_snapshot.json"], ["*snapshot*.json", "positions*.json"])
    summary = _find_file(run_dir, ["pnl_summary.json"], ["*summary*.json", "pnl*.json"])
    trades = _find_file(run_dir, ["trades.jsonl"], ["*trades*.jsonl", "*.jsonl"])
    price_points = _find_file(run_dir, ["price_points.jsonl"], ["*price*points*.jsonl"])
    pnl_daily = _find_file(run_dir, ["pnl_daily.csv"], ["*pnl*daily*.csv"])
    price_daily = _find_file(run_dir, ["price_daily.csv"], ["*price*daily*.csv"])
    if trades and "reject" in os.path.basename(trades).lower():
        alt = _find_file(run_dir, [], ["*trades*.jsonl"])
        if alt:
            trades = alt
    return RunFiles(
        run_dir=run_dir,
        snapshot_path=snapshot,
        summary_path=summary,
        trades_path=trades,
        price_points_path=price_points,
        pnl_daily_path=pnl_daily,
        price_daily_path=price_daily,
    )


def _load_all(repo_root: str, selected_runs: List[str], max_lines: int, max_curve_lines: int) -> None:
    run_file_map: Dict[str, RunFiles] = {}
    trades: List[dict] = []
    prices: List[dict] = []
    pnl_daily_frames: List[pd.DataFrame] = []
    price_daily_frames: List[pd.DataFrame] = []

    for rd in selected_runs:
        rf = _resolve_run_files(rd)
        run_file_map[rd] = rf

        if rf.trades_path:
            recs = _tail_jsonl(rf.trades_path, max_lines=max_lines)
            for r in recs:
                r["_run_dir"] = rd
                r["_trades_file"] = rf.trades_path
            trades.extend(recs)

        if rf.price_points_path:
            recs = _tail_jsonl(rf.price_points_path, max_lines=max_curve_lines)
            for r in recs:
                r["_run_dir"] = rd
                r["_price_file"] = rf.price_points_path
            prices.extend(recs)

        if rf.pnl_daily_path:
            try:
                pday = pd.read_csv(rf.pnl_daily_path)
                if not pday.empty:
                    pday["_run_dir"] = rd
                    pnl_daily_frames.append(pday)
            except Exception:
                pass

        if rf.price_daily_path:
            try:
                prday = pd.read_csv(rf.price_daily_path)
                if not prday.empty:
                    prday["_run_dir"] = rd
                    price_daily_frames.append(prday)
            except Exception:
                pass

    df = pd.DataFrame(trades)
    if not df.empty:
        df = _coerce_ts(df)
        df = _to_num(df, ["qty", "price", "cum_quote_qty", "realized_delta", "expected_price", "slippage_bps"])

    price_df = pd.DataFrame(prices)
    if not price_df.empty:
        price_df = _coerce_ts(price_df)

    pnl_daily_df = pd.concat(pnl_daily_frames, ignore_index=True) if pnl_daily_frames else pd.DataFrame()
    if not pnl_daily_df.empty:
        pnl_daily_df = _coerce_ts(pnl_daily_df)
        pnl_daily_df = _to_num(pnl_daily_df, ["portfolio_value", "portfolio_pnl", "portfolio_pnl_pct"])

    price_daily_df = pd.concat(price_daily_frames, ignore_index=True) if price_daily_frames else pd.DataFrame()
    if not price_daily_df.empty:
        price_daily_df = _coerce_ts(price_daily_df)

    latest_snapshot_path = _latest_by_mtime([run_file_map[r].snapshot_path for r in selected_runs if run_file_map[r].snapshot_path] or [])
    latest_summary_path = _latest_by_mtime([run_file_map[r].summary_path for r in selected_runs if run_file_map[r].summary_path] or [])

    snapshot = _safe_json_load(latest_snapshot_path or "")
    summary = _safe_json_load(latest_summary_path or "")

    # ---- ONLY manual files from CONFIG ----
    config_path = None
    manual_positions_path = None
    capital_flows_path = None
    if selected_runs:
        strategy_dir = _infer_strategy_dir_from_state_dir(selected_runs[0])
        if strategy_dir:
            config_path = _find_config_in_strategy_dir(strategy_dir)
    if config_path:
        mp, cf = _get_manual_paths_from_config(config_path)
        manual_positions_path = mp
        capital_flows_path = cf

    manual_positions_df = _load_manual_positions_file(manual_positions_path or "")
    capital_flows_df = _load_capital_flows_file(capital_flows_path or "")

    st.session_state["loaded"] = True
    st.session_state["data_snapshot"] = snapshot
    st.session_state["data_summary"] = summary
    st.session_state["data_trades_df"] = df
    st.session_state["data_price_df"] = price_df
    st.session_state["data_pnl_daily_df"] = pnl_daily_df
    st.session_state["data_price_daily_df"] = price_daily_df
    st.session_state["data_manual_positions_df"] = manual_positions_df
    st.session_state["data_manual_positions_path"] = manual_positions_path
    st.session_state["data_capital_flows_df"] = capital_flows_df
    st.session_state["data_capital_flows_path"] = capital_flows_path
    st.session_state["data_config_path"] = config_path
    st.session_state["last_loaded_at"] = pd.Timestamp.utcnow().isoformat() + "Z"


# -------------------------
# UI
# -------------------------

st.set_page_config(page_title="Trading Bot Dashboard", layout="wide")
_ensure_session_defaults()

st.title("Trading Bot Dashboard (Manual Refresh Only)")
st.caption("Loads from disk ONLY when you click Refresh. No background refresh. No manual editor inputs.")

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
max_curve_lines = int(st.sidebar.number_input("Max curve points to load (tail)", min_value=500, value=5000, step=500))

col_btn1, col_btn2 = st.sidebar.columns(2)
with col_btn1:
    if st.button("Refresh", type="primary"):
        with st.spinner("Loading data..."):
            _load_all(repo_root, selected_runs, max_lines=max_lines, max_curve_lines=max_curve_lines)
with col_btn2:
    if st.button("Clear"):
        for k in list(st.session_state.keys()):
            if k.startswith("data_") or k in {"loaded", "last_loaded_at"}:
                del st.session_state[k]
        st.rerun()

if not st.session_state.get("loaded"):
    st.info("Click **Refresh** to load data. No background refresh is performed.")
    st.stop()

snapshot = st.session_state.get("data_snapshot")
summary = st.session_state.get("data_summary")
df = st.session_state.get("data_trades_df")
price_df = st.session_state.get("data_price_df")
pnl_daily_df = st.session_state.get("data_pnl_daily_df")
price_daily_df = st.session_state.get("data_price_daily_df")
manual_positions_df = st.session_state.get("data_manual_positions_df")
manual_positions_path = st.session_state.get("data_manual_positions_path")
capital_flows_df = st.session_state.get("data_capital_flows_df")
capital_flows_path = st.session_state.get("data_capital_flows_path")
config_path = st.session_state.get("data_config_path")
last_loaded_at = st.session_state.get("last_loaded_at")

st.caption(f"Loaded at: {last_loaded_at}")
if config_path:
    st.caption(f"Config used for manual files: {config_path}")
else:
    st.warning("Could not find config*.json in the strategy folder. Manual files will be empty.")

if manual_positions_path:
    st.caption(f"Manual positions file (from config): {manual_positions_path}")
else:
    st.warning("manual_positions_file not found or not set in config.paths.")
if capital_flows_path:
    st.caption(f"Capital flows file (from config): {capital_flows_path}")
else:
    st.warning("capital_flows_file not found or not set in config.paths.")

# -------------------------
# Filters (do NOT reload data)
# -------------------------

symbol_list = sorted(df["symbol"].dropna().unique().tolist()) if isinstance(df, pd.DataFrame) and not df.empty and "symbol" in df.columns else []
sel_symbols = st.sidebar.multiselect("Symbols (filter)", options=symbol_list, default=symbol_list)
only_fills = st.sidebar.checkbox("Only FILL events", value=True)

dff = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
if not dff.empty:
    if sel_symbols and "symbol" in dff.columns:
        dff = dff[dff["symbol"].isin(sel_symbols)]
    if only_fills and "event" in dff.columns:
        dff = dff[dff["event"] == "FILL"]

# Date range filter (UTC)
if not dff.empty and "ts" in dff.columns and dff["ts"].notna().any():
    ts_min = dff["ts"].min()
    ts_max = dff["ts"].max()
    d0 = ts_min.date()
    d1 = ts_max.date()
    d_from, d_to = st.sidebar.date_input("Date range (UTC)", value=(d0, d1))
    start = pd.Timestamp(d_from, tz="UTC")
    end = pd.Timestamp(d_to, tz="UTC") + pd.Timedelta(days=1)
    dff = dff[(dff["ts"] >= start) & (dff["ts"] < end)]
    pnl_date = st.sidebar.date_input("PnL date (UTC)", value=d1)
else:
    pnl_date = pd.Timestamp.utcnow().date()

default_cycle_unit = float(st.sidebar.number_input("Default cycle unit quote", min_value=1.0, value=1500.0, step=100.0))

# -------------------------
# Capital flow adjustment (ONLY from file)
# -------------------------

strategy_start_ts_utc = pd.NaT
if isinstance(pnl_daily_df, pd.DataFrame) and not pnl_daily_df.empty and "ts" in pnl_daily_df.columns:
    ts_hist = pd.to_datetime(pnl_daily_df["ts"], utc=True, errors="coerce").dropna()
    if not ts_hist.empty:
        strategy_start_ts_utc = ts_hist.min()
if pd.isna(strategy_start_ts_utc) and isinstance(df, pd.DataFrame) and not df.empty and "ts" in df.columns:
    ts_hist2 = pd.to_datetime(df["ts"], utc=True, errors="coerce").dropna()
    if not ts_hist2.empty:
        strategy_start_ts_utc = ts_hist2.min()

auto_capital_flow = 0.0
capital_flow_rows_used = 0
if isinstance(capital_flows_df, pd.DataFrame) and not capital_flows_df.empty and "delta" in capital_flows_df.columns:
    cf = capital_flows_df.copy()
    cf["delta_num"] = pd.to_numeric(cf["delta"], errors="coerce")
    cf = cf[cf["delta_num"].notna()]
    cf["ts_utc"] = pd.to_datetime(cf["ts"], utc=True, errors="coerce")
    cf = cf[cf["ts_utc"].notna()]
    if not pd.isna(strategy_start_ts_utc):
        cf = cf[cf["ts_utc"] > strategy_start_ts_utc]
    capital_flow_rows_used = int(len(cf))
    auto_capital_flow = float(cf["delta_num"].sum())

capital_added_since_start = auto_capital_flow

if capital_flows_path:
    st.caption(f"Capital flows used (rows={capital_flow_rows_used}) net={_fmt_num(auto_capital_flow)}")
    if not pd.isna(strategy_start_ts_utc):
        st.caption(f"Included only rows strictly after bot start: {str(strategy_start_ts_utc)}")

# -------------------------
# Latest prices for manual CMP resolution
# -------------------------

latest_px_by_symbol: Dict[str, float] = {}
if isinstance(snapshot, dict):
    snap_symbols = snapshot.get("symbols") if isinstance(snapshot.get("symbols"), dict) else {}
    for sym, rec in snap_symbols.items():
        if isinstance(rec, dict):
            px = _safe_float(rec.get("px"))
            if px is not None:
                latest_px_by_symbol[str(sym)] = px

if isinstance(df, pd.DataFrame) and not df.empty and "symbol" in df.columns and "price" in df.columns:
    latest_trade_rows = df.dropna(subset=["symbol", "price"]).copy()
    if not latest_trade_rows.empty and "ts" in latest_trade_rows.columns:
        latest_trade_rows = latest_trade_rows.sort_values("ts")
    latest_trade_rows = latest_trade_rows.groupby("symbol", dropna=True).tail(1)
    for _, r in latest_trade_rows.iterrows():
        sym = str(r.get("symbol"))
        if sym and sym not in latest_px_by_symbol:
            px = _safe_float(r.get("price"))
            if px is not None:
                latest_px_by_symbol[sym] = px

if isinstance(price_df, pd.DataFrame) and not price_df.empty and "prices" in price_df.columns and "ts" in price_df.columns:
    psrc = price_df.dropna(subset=["ts"]).copy().sort_values("ts")
    for _, r in psrc.iterrows():
        pmap = r.get("prices")
        if isinstance(pmap, str):
            try:
                pmap = json.loads(pmap)
            except Exception:
                pmap = None
        if not isinstance(pmap, dict):
            continue
        for sym, px in pmap.items():
            pxf = _safe_float(px)
            if pxf is not None:
                latest_px_by_symbol[str(sym)] = pxf

latest_px_norm: Dict[str, float] = {}
for k, v in latest_px_by_symbol.items():
    nk = _norm_symbol(k)
    if nk:
        latest_px_norm[nk] = v

# -------------------------
# Manual positions adjustment (ONLY from file)
# -------------------------

manual_calc = pd.DataFrame()
manual_pnl_total = 0.0
if isinstance(manual_positions_df, pd.DataFrame) and not manual_positions_df.empty:
    m = manual_positions_df.copy()
    for c in ["ts", "symbol", "qty", "buy_price"]:
        if c not in m.columns:
            m[c] = None
    m["symbol"] = m["symbol"].astype(str).str.strip()
    m = _to_num(m, ["qty", "buy_price"])
    m = m.dropna(subset=["symbol", "qty", "buy_price"])

    m["ts_utc"] = m["ts"].apply(_parse_ts_user_ist_to_utc)
    m["ts_provided"] = m["ts"].astype(str).str.strip().replace({"None": "", "nan": "", "NaN": ""}) != ""
    m["symbol_norm"] = m["symbol"].apply(_norm_symbol)

    resolved = m["symbol"].apply(lambda s: _resolve_manual_cmp(s, latest_px_norm))
    m["cmp"] = resolved.apply(lambda t: t[0] if isinstance(t, tuple) else None)
    m["cmp_symbol"] = resolved.apply(lambda t: t[1] if isinstance(t, tuple) else None)

    m["status"] = "ok"
    m.loc[m["symbol_norm"] == "", "status"] = "missing_symbol"
    m.loc[m["qty"].isna(), "status"] = "missing_qty"
    m.loc[m["buy_price"].isna(), "status"] = "missing_buy_price"
    m.loc[(m["status"] == "ok") & m["ts_provided"] & m["ts_utc"].isna(), "status"] = "bad_ts"
    m.loc[(m["status"] == "ok") & (m["cmp"].isna()), "status"] = "cmp_not_found"

    now_utc = pd.Timestamp.now(tz="UTC")
    m["active_now"] = m["ts_utc"].isna() | (m["ts_utc"] <= now_utc)
    m.loc[(m["status"] == "ok") & (~m["active_now"]), "status"] = "not_active_yet"

    manual_calc = m[m["status"] == "ok"].copy()
    if not manual_calc.empty:
        manual_calc["manual_cost"] = manual_calc["qty"] * manual_calc["buy_price"]
        manual_calc["manual_market"] = manual_calc["qty"] * manual_calc["cmp"]
        manual_calc["manual_pnl"] = manual_calc["manual_market"] - manual_calc["manual_cost"]
        manual_pnl_total = float(manual_calc["manual_pnl"].sum())

# -------------------------
# Top metrics
# -------------------------

st.subheader("Top Metrics")

colA, colB, colC, colD, colE, colF = st.columns(6)

pv = ppnl = ppct = None
st_real_all = st_real_td = None
non_strategy_value = non_strategy_pct = None

if isinstance(summary, dict):
    pv = summary.get("portfolio_value")
    ppnl = summary.get("portfolio_pnl")
    ppct = summary.get("portfolio_pnl_pct")
    created = summary.get("created") if isinstance(summary.get("created"), dict) else {}
    bot = summary.get("bot") if isinstance(summary.get("bot"), dict) else {}
    non_strategy = summary.get("non_strategy") if isinstance(summary.get("non_strategy"), dict) else {}

    st_real_all = bot.get("realized_all_time") if bot.get("realized_all_time") is not None else created.get("strategy_realized_all_time")
    st_real_td = bot.get("realized_today") if bot.get("realized_today") is not None else created.get("strategy_realized_today")
    non_strategy_value = non_strategy.get("value_est")
    non_strategy_pct = non_strategy.get("value_pct_est")

pvf = _safe_float(pv)
ppnlf = _safe_float(ppnl)
eff_ppnl = ppnlf
eff_ppct = _safe_float(ppct)

if pvf is not None and ppnlf is not None:
    raw_start = pvf - ppnlf
    eff_start = raw_start + float(capital_added_since_start)
    eff_ppnl = pvf - eff_start
    eff_ppct = (eff_ppnl / eff_start) if eff_start > 0 else None

adjusted_current = (pvf - manual_pnl_total) if pvf is not None else None
raw_initial = None
if pvf is not None and ppnlf is not None:
    raw_initial = (pvf - ppnlf) + float(capital_added_since_start)
adjusted_pnl = (adjusted_current - raw_initial) if (adjusted_current is not None and raw_initial is not None) else None

colA.metric("Portfolio Value", str(pv) if pv is not None else "—")
colB.metric("Portfolio PnL (effective)", _fmt_num(eff_ppnl), delta=_pretty_pct(eff_ppct) if eff_ppct is not None else None)
colC.metric("Bot Realized All-time", str(st_real_all) if st_real_all is not None else "—")
colD.metric("Bot Realized Today (UTC)", str(st_real_td) if st_real_td is not None else "—")
colE.metric("Legacy PnL Removed (manual_positions_file)", _fmt_num(manual_pnl_total))
colF.metric("Adjusted PnL (effective - legacy)", _fmt_num(adjusted_pnl))

if non_strategy_value is not None:
    st.caption(f"Non-strategy value (estimate): {non_strategy_value} ({_pretty_pct(non_strategy_pct) if non_strategy_pct is not None else ''})")

# -------------------------
# Manual positions view (file only)
# -------------------------

st.subheader("Manual Positions (from config file)")

if manual_positions_path:
    st.caption("This table is loaded ONLY from manual_positions_file. No in-dashboard editing is supported.")
    if not manual_calc.empty:
        show_cols = [c for c in ["ts", "symbol", "qty", "buy_price", "cmp_symbol", "cmp", "manual_cost", "manual_market", "manual_pnl"] if c in manual_calc.columns]
        st.dataframe(manual_calc[show_cols], use_container_width=True)
    elif isinstance(manual_positions_df, pd.DataFrame) and not manual_positions_df.empty:
        st.warning("Manual positions file loaded, but rows could not be evaluated (CMP missing or bad rows).")
        st.dataframe(manual_positions_df, use_container_width=True)
    else:
        st.info("manual_positions_file exists but seems empty.")
else:
    st.info("No manual_positions_file configured.")

# -------------------------
# Portfolio Value Curve (daily)
# -------------------------

st.subheader("Portfolio Value Curve (daily points)")
st.caption("Uses pnl_daily.csv when available; otherwise shows only the current point from pnl_summary.json.")

curve_daily_base = pd.DataFrame()
if isinstance(pnl_daily_df, pd.DataFrame) and not pnl_daily_df.empty and "ts" in pnl_daily_df.columns and "portfolio_value" in pnl_daily_df.columns:
    curve_daily_base = pnl_daily_df.dropna(subset=["ts", "portfolio_value"]).sort_values("ts")

curve = curve_daily_base[["ts", "portfolio_value"]].copy() if not curve_daily_base.empty else pd.DataFrame()
if curve.empty and isinstance(summary, dict):
    s_ts = pd.to_datetime(summary.get("ts"), utc=True, errors="coerce")
    if pd.isna(s_ts):
        s_ts = pd.Timestamp.utcnow()
    s_val = _safe_float(summary.get("portfolio_value"))
    if s_val is not None:
        curve = pd.DataFrame([{"ts": s_ts, "portfolio_value": s_val}])

if not curve.empty:
    curve = curve.dropna(subset=["ts", "portfolio_value"]).sort_values("ts")
    st.line_chart(curve.set_index("ts")[["portfolio_value"]], use_container_width=True)
else:
    st.info("No curve data available yet.")

# -------------------------
# Current Snapshot
# -------------------------

st.subheader("Current Snapshot (latest loaded)")

if isinstance(snapshot, dict):
    sym_map = snapshot.get("symbols") if isinstance(snapshot.get("symbols"), dict) else {}
    rows = []
    for sym, d in (sym_map or {}).items():
        if not isinstance(d, dict):
            continue
        r = {"symbol": sym}
        r.update(d)
        rows.append(r)
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.info("No symbols in snapshot.")
    with st.expander("Raw positions_snapshot.json (loaded)"):
        st.code(json.dumps(snapshot, indent=2), language="json")
else:
    st.info("No snapshot loaded.")

# -------------------------
# Trades (filtered) + Slippage
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

# -------------------------
# PnL For Selected Date (UTC) - from trades
# -------------------------

st.subheader("PnL For Selected Date (UTC)")

if not isinstance(df, pd.DataFrame) or df.empty or "ts" not in df.columns:
    st.info("No trades available for date-based PnL.")
else:
    day_start = pd.Timestamp(pnl_date, tz="UTC")
    day_end = day_start + pd.Timedelta(days=1)

    base = df.copy()
    if sel_symbols and "symbol" in base.columns:
        base = base[base["symbol"].isin(sel_symbols)]
    if only_fills and "event" in base.columns:
        base = base[base["event"] == "FILL"]

    day_df = base[(base["ts"] >= day_start) & (base["ts"] < day_end)]
    if day_df.empty:
        st.info("No trades on selected date.")
    else:
        realized_day = day_df["realized_delta"].sum() if "realized_delta" in day_df.columns else None
        c1, c2 = st.columns(2)
        c1.metric("Realized PnL (day)", str(realized_day) if realized_day is not None else "—")
        c2.metric("Fills (day)", str(len(day_df)))

# -------------------------
# Raw summary file
# -------------------------

with st.expander("Raw pnl_summary.json (loaded)"):
    if isinstance(summary, dict):
        st.code(json.dumps(summary, indent=2), language="json")
    else:
        st.info("No summary loaded.")