"""
MEXC Trading Bot Dashboard — simple, correct, fresh-start aware.
"""
from __future__ import annotations

import glob
import io
import json
import os
from typing import Any, List, Optional

import pandas as pd
import streamlit as st

REPO_ROOT = os.getenv("TRADING_BOT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
STATE_DIR = os.path.join(REPO_ROOT, "strategies", "pct_ladder", "state")
SUMMARY_FILE = os.path.join(STATE_DIR, "pnl_summary.json")

# Bucket-aware paths (2-bucket design deployed 2026-05-28)
BUCKETS = {
    "Bucket 1 (tight 0.4%)": {
        "state_dir": os.path.join(STATE_DIR, "bucket1"),
        "config": os.path.join(REPO_ROOT, "strategies", "pct_ladder", "config.mexc.bucket1.json"),
        "label": "Bucket 1 — Tight 0.4% grid, $50k pool",
    },
    "Bucket 2 (wide 10%)": {
        "state_dir": os.path.join(STATE_DIR, "bucket2"),
        "config": os.path.join(REPO_ROOT, "strategies", "pct_ladder", "config.mexc.bucket2.json"),
        "label": "Bucket 2 — Wide 10% upside grid, $47k pool",
    },
}
# Off-bot ETH allocated to Bucket 2's HODL stack (untouched by either bot)
# Computed as: total broker ETH - bucket1 traded_qty - bucket2 traded_qty
# Stays constant because each bot's fills move broker ETH 1:1 with state.traded_qty
OFF_BOT_HODL_ETH = 17.709


def _jload(path: str) -> Optional[dict]:
    try:
        if not path or not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _newest(pattern: str) -> Optional[str]:
    files = [p for p in glob.glob(pattern) if os.path.isfile(p)]
    return max(files, key=os.path.getmtime) if files else None


def _sf(x: Any) -> Optional[float]:
    try:
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return float(x)
    except Exception:
        return None


def _fmt(x: Any, digits: int = 2, prefix: str = "$") -> str:
    v = _sf(x)
    if v is None:
        return "—"
    return f"{prefix}{v:,.{digits}f}" if prefix else f"{v:,.{digits}f}"


def _load_trades(path: str, max_lines: int = 50_000) -> List[dict]:
    if not path or not os.path.exists(path):
        return []
    out: List[dict] = []
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            chunk = 1024 * 1024
            data = b""
            pos = size
            while pos > 0 and data.count(b"\n") < max_lines:
                step = min(chunk, pos)
                pos -= step
                f.seek(pos)
                data = f.read(step) + data
        for line in data.decode("utf-8", errors="ignore").splitlines()[-max_lines:]:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if row.get("event") == "FILL":
                    out.append(row)
            except Exception:
                pass
    except Exception:
        pass
    return out


def _coerce_ts(df: pd.DataFrame) -> pd.DataFrame:
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["date_utc"] = df["ts"].dt.date.astype(str)
    return df


def _to_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            series = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            series = series.replace({"": None, "None": None, "nan": None, "NaN": None, "NaT": None})
            df[col] = pd.to_numeric(series, errors="coerce")
    return df


def _count_cycles(store: dict, step: float = 0.0) -> int:
    ps = store.get("per_symbol", {}) if isinstance(store, dict) else {}
    total = 0
    for value in ps.values():
        if not isinstance(value, dict):
            continue
        est = _sf(value.get("cycles_est"))
        if est is not None:
            total += int(est)
            continue
        cycle_quote = _sf(value.get("cycle_quote"))
        unit = _sf(value.get("cycle_unit_quote")) or step
        if cycle_quote and unit and unit > 0:
            total += int(cycle_quote / unit)
    return total


def _lifo_realized(trades_seq) -> float:
    """Separate-stack LIFO matching. Returns total realized PnL from matched pairs."""
    buy_stack: list = []
    sell_stack: list = []
    realized = 0.0
    for side, qty, px in trades_seq:
        if qty < 1e-9:
            continue
        if side == "BUY":
            remaining = qty
            while remaining > 1e-9 and sell_stack:
                take = min(remaining, sell_stack[-1][0])
                realized += take * (sell_stack[-1][1] - px)
                remaining -= take
                sell_stack[-1][0] -= take
                if sell_stack[-1][0] < 1e-9:
                    sell_stack.pop()
            if remaining > 1e-9:
                buy_stack.append([remaining, px])
        elif side == "SELL":
            remaining = qty
            while remaining > 1e-9 and buy_stack:
                take = min(remaining, buy_stack[-1][0])
                realized += take * (px - buy_stack[-1][1])
                remaining -= take
                buy_stack[-1][0] -= take
                if buy_stack[-1][0] < 1e-9:
                    buy_stack.pop()
            if remaining > 1e-9:
                sell_stack.append([remaining, px])
    return realized


def _render_aggregate_view() -> None:
    """High-level summary across both buckets + off-bot HODL."""
    st.subheader("Aggregate — Both Buckets")
    st.caption("Combined view across Bucket 1 + Bucket 2 + off-bot HODL stack. Switch to a specific bucket in the sidebar for trade-level detail.")

    rows = []
    total_pv = 0.0
    total_realized = 0.0
    total_cash = 0.0
    total_eth = 0.0
    eth_price = None
    for label, paths in BUCKETS.items():
        sumf = os.path.join(paths["state_dir"], "pnl_summary.json")
        s = _jload(sumf) or {}
        statef = _newest(os.path.join(paths["state_dir"], "state_*_v1.json"))
        st_data = _jload(statef) or {}
        ss = (st_data.get("symbol_states") or {}).get("ETHUSDC") or {}
        cash = _sf(st_data.get("cash")) or 0.0
        eth = _sf(ss.get("traded_qty")) or 0.0
        last_px = _sf(ss.get("last_mark_price")) or _sf((st_data.get("last_prices") or {}).get("ETHUSDC")) or 0.0
        if last_px > 0 and eth_price is None:
            eth_price = last_px
        bot = s.get("bot") or {}
        realized = _sf(bot.get("realized_all_time")) or 0.0
        pv = cash + eth * (last_px or 0.0)
        rows.append({
            "Bucket": label,
            "Cash (USDC)": cash,
            "ETH": eth,
            "@ Px": last_px,
            "Value": pv,
            "Realized (all-time)": realized,
        })
        total_pv += pv
        total_realized += realized
        total_cash += cash
        total_eth += eth

    hodl_value = OFF_BOT_HODL_ETH * (eth_price or 0.0)
    rows.append({
        "Bucket": "Off-bot HODL",
        "Cash (USDC)": 0.0,
        "ETH": OFF_BOT_HODL_ETH,
        "@ Px": eth_price or 0.0,
        "Value": hodl_value,
        "Realized (all-time)": 0.0,
    })
    total_pv += hodl_value
    total_eth += OFF_BOT_HODL_ETH

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Value", _fmt(total_pv))
    c2.metric("Total Cash", _fmt(total_cash))
    c3.metric(f"Total ETH ({total_eth:.4f})", _fmt(total_eth * (eth_price or 0.0)),
              delta=f"@ ${eth_price:,.2f}" if eth_price else None, delta_color="off")
    c4.metric("Total Realized", _fmt(total_realized))

    st.markdown("**Per-bucket breakdown**")
    df = pd.DataFrame(rows)
    df_disp = df.copy()
    for col in ["Cash (USDC)", "Value", "Realized (all-time)"]:
        df_disp[col] = df_disp[col].apply(lambda v: _fmt(v))
    df_disp["@ Px"] = df_disp["@ Px"].apply(lambda v: f"${v:,.2f}" if v else "—")
    df_disp["ETH"] = df_disp["ETH"].apply(lambda v: f"{v:.6f}")
    st.dataframe(df_disp, use_container_width=True, hide_index=True)

    st.info(
        "Off-bot HODL = 17.7 ETH allocated to Bucket 2 but kept outside bot tracking. "
        "It rides full upside trends without being sold by the wide grid. "
        "Bucket totals show only what each bot tracks in its state file."
    )


def render_page() -> None:
    st.title("MEXC ETH/USDC Bot Dashboard")

    with st.sidebar:
        if st.button("Refresh", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.caption("Data is not refreshed automatically. Click Refresh to reload.")
        st.divider()
        view_options = ["Aggregate"] + list(BUCKETS.keys())
        view = st.radio("View", options=view_options, index=0)
        st.divider()
        max_trades = int(st.number_input("Max trades to load", min_value=100, value=10000, step=1000))
        show_raw = st.checkbox("Show raw JSON (debug)", value=False)

    if view == "Aggregate":
        _render_aggregate_view()
        return

    bucket_paths = BUCKETS[view]
    bucket_state_dir = bucket_paths["state_dir"]
    st.caption(bucket_paths["label"])

    summary = _jload(os.path.join(bucket_state_dir, "pnl_summary.json")) or {}
    state_path = _newest(os.path.join(bucket_state_dir, "state_*_v1.json"))
    state = _jload(state_path) or {}
    extras = state.get("extras") or {}
    config = _jload(bucket_paths["config"]) or {}
    trades_path = _newest(os.path.join(bucket_state_dir, "trades_*_v1.jsonl"))
    raw_trades = _load_trades(trades_path, max_lines=max_trades)
    snapshot_path = os.path.join(bucket_state_dir, "positions_snapshot.json")
    snapshot = _jload(snapshot_path) or {}

    df_all = pd.DataFrame(raw_trades) if raw_trades else pd.DataFrame()
    if not df_all.empty:
        df_all = _coerce_ts(df_all)
        df_all = _to_num(df_all, ["qty", "price", "cum_quote_qty", "realized_delta"])
        df_all = df_all.sort_values("ts") if "ts" in df_all.columns else df_all

    portfolio_value = _sf(summary.get("portfolio_value")) or 0.0
    bot = summary.get("bot") or {}
    realized_today = _sf(bot.get("realized_today")) or 0.0
    realized_alltime = _sf(bot.get("realized_all_time")) or 0.0
    holdings = summary.get("holdings") or {}
    usdc_balance = _sf(holdings.get("quote_total")) or 0.0
    eth_data = (holdings.get("per_symbol") or {}).get("ETHUSDC") or {}
    eth_qty = _sf(eth_data.get("base_total")) or 0.0
    eth_price = _sf(eth_data.get("px")) or 0.0
    eth_value = eth_qty * eth_price
    cycles_today_d = summary.get("cycles_today") or {}
    cycles_alltime_d = summary.get("cycles_all_time") or {}
    initial_equity = _sf(extras.get("compound_initial_equity")) or 0.0
    initial_step = _sf(extras.get("compound_initial_buy_quote")) or 0.0
    current_step = _sf(extras.get("compound_buy_quote")) or initial_step
    lifo_pnl = _sf(extras.get("compound_last_actual_pnl")) or 0.0
    compound_last_ts = extras.get("compound_last_ts", "—")
    pnl_since_start = (portfolio_value - initial_equity) if initial_equity > 0 else None
    pct_since_start = (pnl_since_start / initial_equity * 100) if initial_equity > 0 and pnl_since_start is not None else None
    step_pct = (current_step / initial_equity * 100) if initial_equity > 0 and current_step > 0 else 0.0
    step_for_cycles = current_step if current_step > 0 else initial_step
    ct_count = _count_cycles(cycles_today_d, step=step_for_cycles)
    ca_count = _count_cycles(cycles_alltime_d, step=step_for_cycles)

    # Runway calculations
    strategy_cfg = config.get("strategy") or {}
    exec_cfg = config.get("execution") or {}
    grid_pct = _sf(strategy_cfg.get("lower_pct")) or 0.0   # downside grid step %
    upper_grid_pct = _sf(strategy_cfg.get("upper_pct")) or grid_pct
    quote_reserve = _sf(exec_cfg.get("quote_reserve_usdt")) or 0.0
    buy_step = current_step if current_step > 0 else (_sf(strategy_cfg.get("buy_quote")) or 1.0)
    sell_step = (_sf(extras.get("compound_sell_quote")) or buy_step)
    effective_usdc = max(usdc_balance - quote_reserve, 0.0)
    # grid_pct is already in % units (e.g. 0.2 means 0.2%), so result is directly in %
    usdc_runway_pct = (effective_usdc / buy_step * grid_pct) if buy_step > 0 and grid_pct > 0 else None
    eth_runway_pct = (eth_value / sell_step * upper_grid_pct) if sell_step > 0 and upper_grid_pct > 0 and eth_value > 0 else None

    st.subheader("Portfolio")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Value", _fmt(portfolio_value))
    usdc_runway_str = f"↓ {usdc_runway_pct:.1f}% runway" if usdc_runway_pct is not None else ""
    c2.metric("USDC Balance", _fmt(usdc_balance), delta=usdc_runway_str or None, delta_color="off")
    eth_runway_str = f"↑ {eth_runway_pct:.1f}% runway" if eth_runway_pct is not None else ""
    c3.metric(f"ETH ({eth_qty:.4f})", _fmt(eth_value), delta=f"@ ${eth_price:,.2f}  ·  {eth_runway_str}", delta_color="off")
    if pnl_since_start is not None:
        c4.metric("PnL Since Start", _fmt(pnl_since_start), delta=f"{pct_since_start:+.2f}%", delta_color="normal")
    else:
        c4.metric("PnL Since Start", "—")

    # ── PnL Breakdown: where's my money? ────────────────────────────────
    st.subheader("Where's My Money?")

    # Data sources
    start_pv = _sf(extras.get("portfolio_start_value")) or initial_equity
    strategy_unrealized = _sf(bot.get("unrealized_now")) or 0.0
    strategy_realized = _sf(bot.get("realized_all_time")) or 0.0
    sym_snap = (snapshot.get("symbols") or {}).get("ETHUSDC") or {}
    strategy_eth = _sf(sym_snap.get("traded_qty")) or 0.0
    strategy_avg_px = _sf(sym_snap.get("avg_price")) or 0.0

    # --- Compute exact start state from trade replay ---
    # Net ETH/USDC changes from all fills → back-compute start holdings
    net_eth_trades = 0.0
    net_usdc_trades = 0.0
    # Track grid vs rebalance flows separately
    grid_net_usdc = 0.0
    grid_net_eth = 0.0
    rebal_net_usdc = 0.0
    rebal_net_eth = 0.0
    if not df_all.empty and "side" in df_all.columns:
        for _, row in df_all.iterrows():
            side = str(row.get("side", "")).upper()
            qty = float(row.get("qty", 0) or 0)
            quote = float(row.get("cum_quote_qty", 0) or 0)
            reason = str(row.get("reason", ""))
            is_rebal = reason.startswith("rebalance_")
            if side == "BUY":
                net_eth_trades += qty
                net_usdc_trades -= quote
                if is_rebal:
                    rebal_net_usdc -= quote
                    rebal_net_eth += qty
                else:
                    grid_net_usdc -= quote
                    grid_net_eth += qty
            elif side == "SELL":
                net_eth_trades -= qty
                net_usdc_trades += quote
                if is_rebal:
                    rebal_net_usdc += quote
                    rebal_net_eth -= qty
                else:
                    grid_net_usdc += quote
                    grid_net_eth -= qty

    start_usdc = usdc_balance - net_usdc_trades if net_usdc_trades != 0 else None
    start_eth = eth_qty - net_eth_trades if net_eth_trades != 0 else None
    start_price = ((start_pv - start_usdc) / start_eth) if start_usdc is not None and start_eth and start_eth > 0.1 else None

    # 4-bucket PnL: separate-stack LIFO for realized, flow-derived unrealized
    grid_realized = 0.0
    rebal_realized = 0.0
    if not df_all.empty and "side" in df_all.columns:
        sorted_tr = df_all.sort_values("ts") if "ts" in df_all.columns else df_all
        grid_seq, rebal_seq = [], []
        for _, row in sorted_tr.iterrows():
            s = str(row.get("side", "")).upper()
            q = float(row.get("qty", 0) or 0)
            p = float(row.get("price", 0) or 0)
            r = str(row.get("reason", ""))
            (rebal_seq if r.startswith("rebalance_") else grid_seq).append((s, q, p))
        grid_realized = _lifo_realized(grid_seq)
        rebal_realized = _lifo_realized(rebal_seq)

    grid_total = grid_net_usdc + grid_net_eth * eth_price if eth_price else 0.0
    rebal_total = rebal_net_usdc + rebal_net_eth * eth_price if eth_price else 0.0
    grid_unrealized = grid_total - grid_realized
    rebal_unrealized = rebal_total - rebal_realized

    # --- "Then vs Now" snapshot ---
    st.markdown("**Then vs Now**")
    t1, t2, t3 = st.columns(3)
    t1.metric("Started With", _fmt(start_pv))
    t2.metric("Current Value", _fmt(portfolio_value))
    if pnl_since_start is not None:
        t3.metric("Change", _fmt(pnl_since_start),
                  delta=f"{pct_since_start:+.2f}%", delta_color="normal")

    if start_usdc is not None and start_price is not None and start_eth is not None:
        col_then, col_now = st.columns(2)
        with col_then:
            st.caption(f"**Start** (ETH ${start_price:,.0f})")
            st.markdown(
                f"- USDC: **${start_usdc:,.0f}**\n"
                f"- ETH: **{start_eth:.1f}** × ${start_price:,.0f} = **${start_eth * start_price:,.0f}**"
            )
        with col_now:
            st.caption(f"**Now** (ETH ${eth_price:,.0f})")
            st.markdown(
                f"- USDC: **${usdc_balance:,.0f}**\n"
                f"- ETH: **{eth_qty:.1f}** × ${eth_price:,.0f} = **${eth_value:,.0f}**\n"
                f"  - {start_eth:.1f} original\n"
                f"  - {net_eth_trades:+.1f} from trading ({strategy_eth:.1f} open @ avg ${strategy_avg_px:,.0f})"
            )

    # --- PnL Breakdown ---
    st.markdown("**PnL Breakdown**")

    eth_holding_pnl = None
    if start_price is not None and start_eth is not None:
        eth_holding_pnl = start_eth * (eth_price - start_price)

    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Bot Realized", _fmt(grid_realized),
              delta=f"net {grid_net_eth:+.1f} ETH, {_fmt(grid_net_usdc)} USDC",
              delta_color="off")
    p2.metric("Bot Unrealized", _fmt(grid_unrealized),
              delta=f"{grid_net_eth:+.4f} ETH open" if abs(grid_net_eth) > 0.001 else "flat",
              delta_color="off")
    p3.metric("Rebal Realized", _fmt(rebal_realized),
              delta=f"net {rebal_net_eth:+.1f} ETH, {_fmt(rebal_net_usdc)} USDC",
              delta_color="off")
    p4.metric("Rebal Unrealized", _fmt(rebal_unrealized),
              delta=f"{rebal_net_eth:+.4f} ETH open" if abs(rebal_net_eth) > 0.001 else "flat",
              delta_color="off")

    trading_total = grid_total + rebal_total
    net_computed = (eth_holding_pnl or 0.0) + trading_total
    t1, t2, t3 = st.columns(3)
    if eth_holding_pnl is not None:
        price_chg_pct = (eth_price / start_price - 1) * 100 if start_price else 0
        t1.metric("ETH Holding PnL", _fmt(eth_holding_pnl),
                  delta=f"{start_eth:.1f} ETH × {price_chg_pct:+.1f}%", delta_color="off")
    else:
        t1.metric("ETH Holding PnL", "—")
    t2.metric("All Trading PnL", _fmt(trading_total),
              delta="bot + rebalance", delta_color="off")
    t3.metric("Net PnL", _fmt(net_computed),
              delta=f"vs actual: {_fmt(pnl_since_start)}" if pnl_since_start is not None else None,
              delta_color="off")

    if pnl_since_start is not None and eth_holding_pnl is not None:
        st.info(
            f"**Holding + Bot + Rebalance = Net PnL:**  \n"
            f"1. ETH price {'fell' if eth_holding_pnl < 0 else 'rose'} "
            f"${abs(eth_price - start_price):,.0f} on {start_eth:.1f} original ETH → "
            f"**{_fmt(eth_holding_pnl)}**  \n"
            f"2. Bot realized (grid spreads) → **{_fmt(grid_realized)}**  \n"
            f"3. Bot unrealized ({grid_net_eth:+.1f} ETH × price) → **{_fmt(grid_unrealized)}**  \n"
            f"4. Rebal realized → **{_fmt(rebal_realized)}**  \n"
            f"5. Rebal unrealized ({rebal_net_eth:+.1f} ETH × price) → **{_fmt(rebal_unrealized)}**  \n"
            f"{'---'}  \n"
            f"Total: {_fmt(eth_holding_pnl)} + {_fmt(grid_total)} + {_fmt(rebal_total)} = "
            f"**{_fmt(net_computed)}** (actual: {_fmt(pnl_since_start)})"
        )

    st.divider()

    st.subheader("Bot Performance")
    st.caption("Realized PnL uses avg-cost basis (tracked by engine per fill). Cycles from snapshot.")
    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Realized Today", _fmt(realized_today))
    b2.metric("Realized All-time", _fmt(realized_alltime))
    b3.metric("Cycles Today", ct_count)
    b4.metric("Cycles All-time", ca_count)

    st.subheader("Compounding (LIFO Actual PnL)")
    st.caption("LIFO PnL = sum of (sell_price - buy_price) x qty for profitable buy-sell pairs only. Updated daily by compound cron.")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Initial Equity", _fmt(initial_equity))
    d2.metric("Initial Step", _fmt(initial_step))
    d3.metric("Current Step", f"${current_step:,.2f} ({step_pct:.2f}%)" if current_step > 0 else "—")
    d4.metric("LIFO Cycle PnL", _fmt(lifo_pnl))
    st.caption(f"Last compound run: {compound_last_ts}")

    st.subheader("Open Buy Lots (LIFO)")
    sym_states = state.get("symbol_states") or {}
    lots_rows = []
    for sym, sd in sym_states.items():
        if not isinstance(sd, dict):
            continue
        for lot in (sd.get("lots") or []):
            if isinstance(lot, (list, tuple)) and len(lot) >= 2:
                lots_rows.append({"symbol": sym, "qty": lot[0], "buy_price": lot[1], "value": float(lot[0] or 0) * float(lot[1] or 0)})
            elif isinstance(lot, dict):
                qty = _sf(lot.get("qty") or lot.get("quantity")) or 0
                buy_price = _sf(lot.get("buy_price") or lot.get("price")) or 0
                lots_rows.append({"symbol": sym, "qty": qty, "buy_price": buy_price, "value": qty * buy_price})

    if lots_rows:
        lots_df = pd.DataFrame(lots_rows)
        for col in ["qty", "buy_price", "value"]:
            if col in lots_df.columns:
                lots_df[col] = lots_df[col].round(4 if col == "qty" else 2)
        l1, l2 = st.columns([2, 1])
        with l1:
            st.dataframe(lots_df, use_container_width=True, hide_index=True)
        with l2:
            total_lots_qty = lots_df["qty"].sum()
            total_lots_val = lots_df["value"].sum()
            mark_val = total_lots_qty * eth_price
            unrealized = mark_val - total_lots_val if eth_price > 0 else 0.0
            st.metric("Open Lots", len(lots_df))
            st.metric("Total Lot Qty", f"{total_lots_qty:.4f} ETH")
            st.metric("Lot Cost Basis", f"${total_lots_val:,.2f}")
            st.metric("Mark Value", f"${mark_val:,.2f}")
            st.metric("Unrealized (lots)", f"${unrealized:+,.2f}")
    else:
        st.info("No open lots in state (or no trades yet).")

    st.subheader("Recent Trades")
    if df_all.empty:
        st.info(f"No trades yet in {trades_path or 'no file found'}.")
    else:
        if "symbol" in df_all.columns:
            sym_options = sorted(df_all["symbol"].dropna().unique().tolist())
            default_sym = sym_options[:1]
            sel_syms = st.sidebar.multiselect("Symbol filter", options=sym_options, default=default_sym)
            dff = df_all[df_all["symbol"].isin(sel_syms)] if sel_syms else df_all
        else:
            dff = df_all

        if "ts" in dff.columns and dff["ts"].notna().any():
            d0 = dff["ts"].min().date()
            d1 = dff["ts"].max().date()
            d_from, d_to = st.sidebar.date_input("Date range", value=(d0, d1))
            dff = dff[(dff["ts"] >= pd.Timestamp(d_from, tz="UTC")) & (dff["ts"] < pd.Timestamp(d_to, tz="UTC") + pd.Timedelta(days=1))]

        st.caption(f"File: {trades_path}  |  Total fills: {len(df_all)}  |  Showing: {len(dff)}")
        tab1, tab2, tab3 = st.tabs(["Ladder Trades", "Rebalance Trades", "Running Book"])
        base_cols = ["ts", "symbol", "side", "qty", "price", "avg_cost", "realized_delta", "reason"]

        def _prepare_display(src: pd.DataFrame) -> pd.DataFrame:
            disp = src.copy()
            if "realized_delta" in disp.columns and "price" in disp.columns and "qty" in disp.columns:
                rd = pd.to_numeric(disp["realized_delta"], errors="coerce").fillna(0)
                qty = pd.to_numeric(disp["qty"], errors="coerce").replace(0, float("nan"))
                px = pd.to_numeric(disp["price"], errors="coerce")
                sign = disp["side"].astype(str).str.upper().map({"BUY": 1, "SELL": -1}).fillna(-1)
                implied = (px + sign * rd / qty).round(2)
                disp["avg_cost"] = implied.where(rd != 0, other=float("nan"))
            if "ts" in disp.columns:
                disp = disp.sort_values("ts", ascending=False)
                disp["ts"] = disp["ts"].dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M:%S IST")
            show = [col for col in base_cols if col in disp.columns]
            disp = disp[show]
            if "qty" in disp.columns:
                disp["qty"] = disp["qty"].round(6)
            for col in ["price", "avg_cost", "realized_delta"]:
                if col in disp.columns:
                    disp[col] = pd.to_numeric(disp[col], errors="coerce").round(2)
            return disp.reset_index(drop=True)

        with tab1:
            ladder = dff[~dff["reason"].astype(str).str.startswith("rebalance_")] if "reason" in dff.columns else dff
            st.dataframe(_prepare_display(ladder), use_container_width=True, hide_index=True)

        with tab2:
            rebal = dff[dff["reason"].astype(str).str.startswith("rebalance_")] if "reason" in dff.columns else pd.DataFrame()
            if rebal.empty:
                st.info("No rebalance trades.")
            else:
                rb = rebal.copy().sort_values("ts", ascending=True).reset_index(drop=True)
                rb = _to_num(rb, ["qty", "price"])

                # --- Pass 1: FIFO matching (two-pass so buys know their future sell match) ---
                # Each entry: [row_idx, qty_remaining, price, ts_str]
                open_buys: list[list]  = []
                open_sells: list[list] = []
                realized_pnl = 0.0
                # match_info[row_idx] = {"matched_qty": float, "matched_avg_px": float, "pnl": float}
                match_info: dict[int, dict] = {i: {"matched_qty": 0.0, "matched_cost": 0.0, "pnl": 0.0} for i in range(len(rb))}

                for i, row in rb.iterrows():
                    side = str(row.get("side", "")).upper()
                    qty  = float(row.get("qty", 0) or 0)
                    px   = float(row.get("price", 0) or 0)

                    if side == "BUY":
                        remaining = qty
                        while remaining > 1e-9 and open_sells:
                            si, sq, spx, _ = open_sells[0]
                            take = min(remaining, sq)
                            pnl_piece = take * (spx - px)
                            realized_pnl += pnl_piece
                            # record on both sides
                            match_info[i]["matched_qty"]  += take
                            match_info[i]["matched_cost"] += spx * take
                            match_info[i]["pnl"]          += pnl_piece
                            match_info[si]["matched_qty"]  += take
                            match_info[si]["matched_cost"] += px * take
                            match_info[si]["pnl"]          += pnl_piece
                            remaining -= take
                            open_sells[0][1] -= take
                            if open_sells[0][1] < 1e-9:
                                open_sells.pop(0)
                        if remaining > 1e-9:
                            open_buys.append([i, remaining, px, str(row.get("ts", ""))])

                    elif side == "SELL":
                        remaining = qty
                        while remaining > 1e-9 and open_buys:
                            bi, bq, bpx, _ = open_buys[0]
                            take = min(remaining, bq)
                            pnl_piece = take * (px - bpx)
                            realized_pnl += pnl_piece
                            match_info[i]["matched_qty"]  += take
                            match_info[i]["matched_cost"] += bpx * take
                            match_info[i]["pnl"]          += pnl_piece
                            match_info[bi]["matched_qty"]  += take
                            match_info[bi]["matched_cost"] += px * take
                            match_info[bi]["pnl"]          += pnl_piece
                            remaining -= take
                            open_buys[0][1] -= take
                            if open_buys[0][1] < 1e-9:
                                open_buys.pop(0)
                        if remaining > 1e-9:
                            open_sells.append([i, remaining, px, str(row.get("ts", ""))])

                # --- Pass 2: build display columns ---
                open_idx_set = {entry[0] for entry in open_buys} | {entry[0] for entry in open_sells}
                open_qty_map = {entry[0]: entry[1] for entry in open_buys + open_sells}

                row_matched: list[str] = []
                row_pnl: list = []
                row_status: list[str] = []

                for i, row in rb.iterrows():
                    qty = float(row.get("qty", 0) or 0)
                    side = str(row.get("side", "")).upper()
                    info = match_info[i]
                    mq = info["matched_qty"]
                    mc = info["matched_cost"]
                    mp = info["pnl"]

                    if mq > 1e-9:
                        avg_px = mc / mq
                        label = "sell" if side == "BUY" else "buy"
                        row_matched.append(f"{label} @ {avg_px:.2f}")
                        row_pnl.append(round(mp, 2))
                        open_q = open_qty_map.get(i, 0.0)
                        if open_q > 1e-9:
                            row_status.append(f"partial ({open_q:.4f} open)")
                        else:
                            row_status.append("matched")
                    else:
                        row_matched.append("")
                        row_pnl.append(None)
                        row_status.append("open")

                # Unrealized PnL on open positions
                open_buy_qty  = sum(e[1] for e in open_buys)
                open_buy_cost = sum(e[1] * e[2] for e in open_buys)
                open_sell_qty  = sum(e[1] for e in open_sells)
                open_sell_cost = sum(e[1] * e[2] for e in open_sells)
                unrealized_pnl = 0.0
                if open_buy_qty > 1e-9 and eth_price > 0:
                    unrealized_pnl += open_buy_qty * eth_price - open_buy_cost
                if open_sell_qty > 1e-9 and eth_price > 0:
                    unrealized_pnl += open_sell_cost - open_sell_qty * eth_price

                # Summary metrics
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Realized PnL", f"${realized_pnl:+.2f}")
                m2.metric("Unrealized PnL", f"${unrealized_pnl:+.2f}",
                          delta=f"{open_buy_qty:.4f} ETH open" if open_buy_qty > 1e-9 else None,
                          delta_color="off")
                m3.metric("Total PnL", f"${realized_pnl + unrealized_pnl:+.2f}")
                m4.metric("Open positions", f"{open_buy_qty + open_sell_qty:.4f} ETH")

                st.caption("FIFO matching across rebalance trades. **matched** = counterpart avg price. "
                           "Unrealized on open positions at current mark price.")

                # Build display table
                rb["matched"] = row_matched
                rb["pnl"]     = row_pnl
                rb["status"]  = row_status
                rb_disp = rb.copy().sort_values("ts", ascending=False)
                rb_disp["ts"] = rb_disp["ts"].dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M:%S IST")
                show_cols = ["ts", "side", "qty", "price", "matched", "pnl", "status", "reason"]
                show_cols = [c for c in show_cols if c in rb_disp.columns]
                rb_disp["qty"] = pd.to_numeric(rb_disp["qty"], errors="coerce").round(5)
                rb_disp["price"] = pd.to_numeric(rb_disp["price"], errors="coerce").round(2)
                rb_disp["pnl"] = pd.to_numeric(rb_disp["pnl"], errors="coerce").round(2)
                st.dataframe(rb_disp[show_cols].reset_index(drop=True), use_container_width=True, hide_index=True)

        with tab3:
            st.caption(
                "Inventory book: tracks total ETH, buy avg cost, sell avg price. **delta** = (sell_price − buy_avg) × qty "
                "(includes ETH appreciation). **spread** = LIFO-matched (sell_price − matched_buy_price) × qty. "
                "Sells with no matching buy (initial inventory) have spread = 0. **status** on BUY rows: matched / partial / pending."
            )
            if dff.empty:
                st.info("No trades.")
            else:
                book = dff.copy().sort_values("ts", ascending=True)
                book = _to_num(book, ["qty", "price", "realized_delta"])
                manual_inv = summary.get("manual_inventory_by_symbol") or {}
                init_qty = _sf(manual_inv.get("ETHUSDC")) or 0.0
                first_price = float(book.iloc[0]["price"]) if len(book) > 0 else 0.0
                mark_px = eth_price if eth_price > 0 else first_price

                p1_stack: list[tuple[int, float, float, str, str]] = []
                p1_buy_idx = 0
                for _, row in book.iterrows():
                    side = str(row.get("side", "")).upper()
                    qty = float(row.get("qty", 0) or 0)
                    if side == "BUY" and qty > 0:
                        reason_raw = str(row.get("reason", ""))
                        row_type = "rebalance" if "rebalance" in reason_raw.lower() else "ladder"
                        p1_stack.append((p1_buy_idx, float(row.get("price", 0)), qty, str(row.get("ts", "")), row_type))
                        p1_buy_idx += 1
                    elif side == "SELL" and qty > 0:
                        remaining = qty
                        while remaining > 1e-9 and p1_stack:
                            bi, bpx, bqty, bts, btype = p1_stack[-1]
                            matched = min(remaining, bqty)
                            remaining -= matched
                            bqty -= matched
                            if bqty < 1e-9:
                                p1_stack.pop()
                            else:
                                p1_stack[-1] = (bi, bpx, bqty, bts, btype)

                pending_map: dict[int, float] = {bi: bqty for bi, _, bqty, _, _ in p1_stack}
                total_qty = init_qty
                buy_avg = first_price
                cum_sell_qty = 0.0
                cum_sell_quote = 0.0
                buy_stack: list[tuple[float, float]] = []
                buy_idx = 0
                rows = []

                for _, row in book.iterrows():
                    side = str(row.get("side", "")).upper()
                    qty = float(row.get("qty", 0) or 0)
                    px = float(row.get("price", 0) or 0)
                    reason_raw = str(row.get("reason", ""))
                    reason_label = "rebalance" if "rebalance" in reason_raw.lower() else "ladder"
                    delta = 0.0
                    spread = 0.0
                    status = None

                    if side == "SELL" and qty > 0:
                        delta = (px - buy_avg) * qty
                        remaining_sell = qty
                        matched_cost = 0.0
                        matched_total = 0.0
                        while remaining_sell > 1e-9 and buy_stack:
                            buy_px, buy_qty = buy_stack[-1]
                            matched = min(remaining_sell, buy_qty)
                            spread += (px - buy_px) * matched
                            matched_cost += buy_px * matched
                            matched_total += matched
                            remaining_sell -= matched
                            buy_qty -= matched
                            if buy_qty < 1e-9:
                                buy_stack.pop()
                            else:
                                buy_stack[-1] = (buy_px, buy_qty)
                        if matched_total > 1e-9:
                            avg_matched_buy = matched_cost / matched_total
                            from_inv = remaining_sell
                            status = f"partial @ {avg_matched_buy:.2f} ({from_inv:.5f} from inventory)" if from_inv > 1e-9 else f"matched @ {avg_matched_buy:.2f}"
                        else:
                            status = "from inventory"
                        total_qty -= qty
                        cum_sell_qty += qty
                        cum_sell_quote += qty * px
                    elif side == "BUY" and qty > 0:
                        if total_qty + qty > 0:
                            buy_avg = (buy_avg * total_qty + px * qty) / (total_qty + qty)
                        total_qty += qty
                        buy_stack.append((px, qty))
                        qty_remaining = pending_map.get(buy_idx, 0.0)
                        if qty_remaining < 1e-9:
                            status = "matched"
                        elif abs(qty_remaining - qty) < 1e-9:
                            status = "pending"
                        else:
                            status = f"partial ({round(qty_remaining, 5)} left)"
                        buy_idx += 1

                    sell_avg = (cum_sell_quote / cum_sell_qty) if cum_sell_qty > 0 else None
                    rows.append(
                        {
                            "ts": row.get("ts"),
                            "side": side,
                            "type": reason_label,
                            "qty": round(qty, 6),
                            "price": round(px, 2),
                            "buy_avg": round(buy_avg, 2),
                            "sell_avg": round(sell_avg, 2) if sell_avg is not None else None,
                            "total_qty": round(total_qty, 4),
                            "delta": round(delta, 2),
                            "spread": round(spread, 2),
                            "status": status,
                        }
                    )

                book_df = pd.DataFrame(rows)
                book_df["cum_delta"] = book_df["delta"].cumsum().round(2)
                book_df["cum_spread"] = book_df["spread"].cumsum().round(2)
                if "ts" in book_df.columns:
                    book_df = book_df.sort_values("ts", ascending=False)
                    book_df["ts"] = pd.to_datetime(book_df["ts"], utc=True, errors="coerce")
                    book_df["ts"] = book_df["ts"].dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d %H:%M:%S IST")

                if p1_stack:
                    stranded_qty = sum(bqty for _, _, bqty, _, _ in p1_stack)
                    stranded_cost = sum(bpx * bqty for _, bpx, bqty, _, _ in p1_stack)
                    stranded_avg = stranded_cost / stranded_qty if stranded_qty > 0 else 0.0
                    unrealized_spread_loss = (mark_px - stranded_avg) * stranded_qty
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Stranded Buys (lots)", len(p1_stack))
                    c2.metric("Stranded ETH qty", f"{stranded_qty:.4f}")
                    c3.metric("Avg stranded buy px", f"{stranded_avg:.2f}")
                    c4.metric("Unrealized spread loss", f"{unrealized_spread_loss:.2f} USDC")

                st.subheader("Trade Book")
                st.dataframe(book_df, use_container_width=True, hide_index=True)

                if p1_stack:
                    st.subheader("Stranded (Pending) Buys")
                    stranded_rows = []
                    for _, bpx, bqty, bts, btype in sorted(p1_stack, key=lambda x: -x[1]):
                        loss = (mark_px - bpx) * bqty
                        try:
                            bts_fmt = pd.to_datetime(bts, utc=True).tz_convert("Asia/Kolkata").strftime("%Y-%m-%d %H:%M:%S IST")
                        except Exception:
                            bts_fmt = bts
                        stranded_rows.append(
                            {
                                "ts": bts_fmt,
                                "type": btype,
                                "buy_price": round(bpx, 2),
                                "qty": round(bqty, 5),
                                "mark_price": round(mark_px, 2),
                                "loss_per_eth": round(mark_px - bpx, 2),
                                "total_loss": round(loss, 2),
                            }
                        )
                    st.dataframe(pd.DataFrame(stranded_rows), use_container_width=True, hide_index=True)

    st.subheader("Daily Summary")
    if df_all.empty or "ts" not in df_all.columns:
        st.info("No trades for daily summary.")
    else:
        fills = df_all.copy()
        if "reason" in fills.columns:
            ladder_mask = ~fills["reason"].astype(str).str.startswith("rebalance_")
        else:
            ladder_mask = pd.Series([True] * len(fills))

        lf = fills[ladder_mask]
        if "date_utc" in lf.columns and "cum_quote_qty" in lf.columns:
            group = lf.groupby("date_utc")

            def _day_summary(group_df: pd.DataFrame) -> pd.Series:
                buy_sum = group_df[group_df["side"].astype(str).str.upper() == "BUY"]["cum_quote_qty"].sum()
                sell_sum = group_df[group_df["side"].astype(str).str.upper() == "SELL"]["cum_quote_qty"].sum()
                buys = int((group_df["side"].astype(str).str.upper() == "BUY").sum())
                sells = int((group_df["side"].astype(str).str.upper() == "SELL").sum())
                return pd.Series(
                    {
                        "buys": buys,
                        "sells": sells,
                        "cycles": min(buys, sells),
                        "buy_usdc": round(float(buy_sum), 2),
                        "sell_usdc": round(float(sell_sum), 2),
                        "net_usdc": round(float(sell_sum - buy_sum), 2),
                    }
                )

            daily_df = group.apply(_day_summary).reset_index().sort_values("date_utc", ascending=False)
            st.caption("Ladder trades only. Cycles = min(buys, sells). Net USDC = sell USDC − buy USDC.")
            st.dataframe(daily_df, use_container_width=True, hide_index=True)
        else:
            st.info("Not enough data for daily summary.")

    if show_raw:
        with st.expander("pnl_summary.json"):
            st.code(json.dumps(summary, indent=2, default=str), language="json")
        with st.expander("State extras"):
            st.code(json.dumps(extras, indent=2, default=str), language="json")
        with st.expander("positions_snapshot.json"):
            st.code(json.dumps(snapshot, indent=2, default=str), language="json")
        with st.expander("Files loaded"):
            st.write(
                {
                    "state_path": state_path,
                    "trades_path": trades_path,
                    "summary_path": os.path.join(bucket_state_dir, "pnl_summary.json"),
                    "snapshot_path": snapshot_path,
                    "config_path": bucket_paths["config"],
                }
            )

