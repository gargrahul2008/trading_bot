import json
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Trading PnL Dashboard", layout="wide")
st.title("Trading PnL Dashboard")

root = st.sidebar.text_input("Root folder to scan for pnl_points.csv", value=str(Path.cwd()))
rootp = Path(root).expanduser().resolve()

pnl_files = list(rootp.rglob("pnl_points.csv"))
if not pnl_files:
    st.warning("No pnl_points.csv found under this root. Run a strategy first.")
    st.stop()

choices = {str(p): p for p in pnl_files}
selected = st.sidebar.selectbox("Select pnl_points.csv", options=list(choices.keys()))
pnl_path = choices[selected]
state_dir = pnl_path.parent

st.sidebar.caption(f"State dir: {state_dir}")

snap_path = state_dir / "positions_snapshot.json"
summary_path = state_dir / "pnl_summary.json"

col1, col2 = st.columns(2)
with col1:
    st.subheader("Summary")
    if summary_path.exists():
        st.json(json.loads(summary_path.read_text()))
    else:
        st.info("pnl_summary.json not found yet.")
with col2:
    st.subheader("Latest Snapshot")
    if snap_path.exists():
        snap = json.loads(snap_path.read_text())
        st.json({k: snap.get(k) for k in ["ts","broker","quote_asset","portfolio_value","portfolio_pnl","portfolio_pnl_pct","drawdown_pct"]})
    else:
        st.info("positions_snapshot.json not found yet.")

df = pd.read_csv(pnl_path)
df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

last = df.tail(1)
if not last.empty:
    pv = float(last["portfolio_value"].iloc[0])
    ppnl = float(last["portfolio_pnl"].iloc[0])
    dd = float(last["drawdown_pct"].iloc[0]) * 100.0
    stpnl = float(last["strategy_total"].iloc[0])
    a,b,c,d = st.columns(4)
    a.metric("Portfolio Value", f"{pv:,.2f}")
    b.metric("Portfolio PnL", f"{ppnl:,.2f}")
    c.metric("Drawdown %", f"{dd:,.2f}%")
    d.metric("Strategy Total PnL", f"{stpnl:,.2f}")

st.subheader("Portfolio Value")
st.line_chart(df.set_index("ts")[["portfolio_value"]])

st.subheader("Portfolio PnL")
st.line_chart(df.set_index("ts")[["portfolio_pnl"]])

st.subheader("Strategy PnL")
st.line_chart(df.set_index("ts")[["strategy_realized","strategy_unrealized","strategy_total"]])

st.subheader("Latest 500 rows")
st.dataframe(df.tail(500))
