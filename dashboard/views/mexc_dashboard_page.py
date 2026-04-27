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


def render_page() -> None:
    st.title("MEXC ETH/USDC Bot Dashboard")

    with st.sidebar:
        if st.button("Refresh", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.caption("Data is not refreshed automatically. Click Refresh to reload.")
        st.divider()
        max_trades = int(st.number_input("Max trades to load", min_value=100, value=10000, step=1000))
        show_raw = st.checkbox("Show raw JSON (debug)", value=False)

    summary = _jload(SUMMARY_FILE) or {}
    state_path = _newest(os.path.join(STATE_DIR, "mexc_state_*_v1.json"))
    state = _jload(state_path) or {}
    extras = state.get("extras") or {}
    trades_path = _newest(os.path.join(STATE_DIR, "mexc_trades_*_v1.jsonl"))
    raw_trades = _load_trades(trades_path, max_lines=max_trades)
    snapshot_path = os.path.join(STATE_DIR, "positions_snapshot.json")
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

    st.subheader("Portfolio")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Value", _fmt(portfolio_value))
    c2.metric("USDC Balance", _fmt(usdc_balance))
    c3.metric(f"ETH ({eth_qty:.4f})", _fmt(eth_value), delta=f"@ ${eth_price:,.2f}")
    if pnl_since_start is not None:
        c4.metric("PnL Since Start", _fmt(pnl_since_start), delta=f"{pct_since_start:+.2f}%", delta_color="normal")
    else:
        c4.metric("PnL Since Start", "—")

    st.subheader("Bot Performance")
    st.caption("Realized PnL uses avg-cost basis (tracked by engine per fill). Cycles from snapshot.")
    b1, b2, b3, b4 = st.columns(4)
    b1.metric("Realized Today", _fmt(realized_today))
    b2.metric("Realized All-time", _fmt(realized_alltime))
    b3.metric("Cycles Today", ct_count)
    b4.metric("Cycles All-time", ca_count)

    st.subheader("Compounding (LIFO Actual PnL)")
    st.caption("LIFO PnL = sum of (sell_price − buy_price) × qty for profitable buy→sell pairs only. Updated daily by compound cron.")
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
                st.dataframe(_prepare_display(rebal), use_container_width=True, hide_index=True)

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
                    "summary_path": SUMMARY_FILE,
                    "snapshot_path": snapshot_path,
                }
            )

