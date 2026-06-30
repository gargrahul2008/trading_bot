#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from common.engine.state import GlobalState
from common.broker.interfaces import to_decimal

D0 = Decimal("0")


def _dec(x: Any) -> Decimal:
    return to_decimal(x)


def _load_config(path: str) -> tuple[Dict[str, Any], Path]:
    p = Path(path).expanduser().resolve()
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg, p.parent


def _load_strategy(cfg: Dict[str, Any]):
    name = str(cfg.get("strategy_name") or "pct_ladder")
    module = importlib.import_module(f"strategies.{name}.strategy")
    if not hasattr(module, "create_strategy"):
        raise SystemExit(f"Strategy module strategies.{name}.strategy must export create_strategy(strategy_cfg)")
    return module.create_strategy(cfg.get("strategy") or {})


def _normalize_candles(csv_path: str, symbols: List[str], start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        raise SystemExit("Candles CSV is empty.")

    lower = {c: c.strip().lower() for c in df.columns}
    df = df.rename(columns=lower)

    ts_col = None
    for c in ("ts", "timestamp", "time", "datetime", "date"):
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise SystemExit("Candles CSV must contain one of: ts, timestamp, time, datetime, date.")
    if ts_col != "ts":
        df = df.rename(columns={ts_col: "ts"})

    if "close" not in df.columns:
        for alt in ("c", "ltp", "price"):
            if alt in df.columns:
                df = df.rename(columns={alt: "close"})
                break
    if "close" not in df.columns:
        raise SystemExit("Candles CSV must contain close (or c/ltp/price).")

    if "symbol" not in df.columns:
        if len(symbols) == 1:
            df["symbol"] = symbols[0]
        else:
            raise SystemExit("Candles CSV must contain symbol column for multi-symbol backtest.")

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df = df[df["symbol"].isin(symbols)]
    if df.empty:
        raise SystemExit("No candle rows match strategy symbols.")

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["ts", "close"])

    if start:
        s = pd.to_datetime(start, utc=True, errors="coerce")
        if pd.isna(s):
            raise SystemExit(f"Invalid --start timestamp: {start}")
        df = df[df["ts"] >= s]
    if end:
        e = pd.to_datetime(end, utc=True, errors="coerce")
        if pd.isna(e):
            raise SystemExit(f"Invalid --end timestamp: {end}")
        df = df[df["ts"] <= e]

    if df.empty:
        raise SystemExit("No candles left after time filtering.")

    df = df.sort_values(["ts", "symbol"]).groupby(["ts", "symbol"], as_index=False).tail(1)
    return df[["ts", "symbol", "close"]].sort_values(["ts", "symbol"]).reset_index(drop=True)


@dataclass
class FillResult:
    executed: bool
    reason: str
    realized_delta: Decimal = D0
    fee: Decimal = D0
    fill_price: Decimal = D0
    notional: Decimal = D0


class BacktestExecutor:
    def __init__(self, state: GlobalState, *, fee_bps: Decimal, slippage_bps: Decimal):
        self.state = state
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self.trades: List[Dict[str, Any]] = []
        self.rejected: int = 0

    def _ensure_lots(self, ss) -> None:
        lots = getattr(ss, "lots", None)
        if lots is None:
            ss.lots = []
            lots = ss.lots
        cleaned = []
        for lot in (lots or []):
            if not isinstance(lot, dict):
                continue
            qty = _dec(lot.get("qty") or lot.get("quantity") or 0)
            price = _dec(lot.get("price") or lot.get("avg_price") or 0)
            if qty > 0:
                cleaned.append({"qty": qty, "price": price})
        if not cleaned and ss.traded_qty > 0:
            cleaned = [{"qty": _dec(ss.traded_qty), "price": _dec(ss.traded_avg_price)}]
        ss.lots = cleaned
        self._recalc_from_lots(ss)

    def _recalc_from_lots(self, ss) -> None:
        lots = getattr(ss, "lots", None) or []
        total_qty = D0
        total_cost = D0
        for lot in lots:
            if not isinstance(lot, dict):
                continue
            qty = _dec(lot.get("qty") or 0)
            price = _dec(lot.get("price") or 0)
            if qty <= 0:
                continue
            total_qty += qty
            total_cost += qty * price
        if total_qty <= 0:
            ss.traded_qty = D0
            ss.traded_avg_price = D0
            ss.lots = []
        else:
            ss.traded_qty = total_qty
            ss.traded_avg_price = total_cost / total_qty

    def _add_lot(self, ss, qty: Decimal, price: Decimal) -> None:
        if qty <= 0:
            return
        self._ensure_lots(ss)
        ss.lots.append({"qty": _dec(qty), "price": _dec(price)})
        self._recalc_from_lots(ss)

    def _consume_lots_lifo(self, ss, qty: Decimal, sell_price: Decimal) -> Decimal:
        if qty <= 0:
            return D0
        self._ensure_lots(ss)
        remaining = _dec(qty)
        realized = D0
        while remaining > 0 and ss.lots:
            lot = ss.lots[-1]
            lot_qty = _dec(lot.get("qty") or 0)
            lot_price = _dec(lot.get("price") or 0)
            if lot_qty <= 0:
                ss.lots.pop()
                continue
            take = remaining if remaining < lot_qty else lot_qty
            realized += take * (_dec(sell_price) - lot_price)
            lot_qty -= take
            remaining -= take
            if lot_qty <= 0:
                ss.lots.pop()
            else:
                lot["qty"] = lot_qty
        self._recalc_from_lots(ss)
        return realized

    def execute_intent(self, *, ts: pd.Timestamp, symbol: str, side: str, qty: Decimal, ltp: Decimal, reason: str) -> FillResult:
        if qty <= 0 or ltp <= 0:
            self.rejected += 1
            return FillResult(False, "invalid_qty_or_price")

        ss = self.state.symbol_states[symbol]
        slip = (self.slippage_bps / Decimal("10000")) if self.slippage_bps > 0 else D0
        fill_price = ltp * (Decimal("1") + slip) if side == "BUY" else ltp * (Decimal("1") - slip)
        notional = fill_price * qty
        fee = notional * (self.fee_bps / Decimal("10000")) if self.fee_bps > 0 else D0
        realized_delta = D0

        if side == "BUY":
            cost = notional + fee
            if self.state.cash < cost:
                self.rejected += 1
                return FillResult(False, "insufficient_cash")

            self.state.cash -= cost
            remaining = qty
            if ss.borrowed_qty > 0:
                cover = min(remaining, ss.borrowed_qty)
                if cover > 0:
                    realized_delta = cover * (ss.borrowed_avg_sell - fill_price)
                    ss.realized_pnl += realized_delta
                    ss.borrowed_qty -= cover
                    remaining -= cover
                    if ss.borrowed_qty <= 0:
                        ss.borrowed_qty = D0
                        ss.borrowed_avg_sell = D0
            if remaining > 0:
                self._add_lot(ss, remaining, fill_price)
        else:  # SELL
            proceeds = notional - fee
            self.state.cash += proceeds
            self._ensure_lots(ss)

            sell_from_traded = min(qty, ss.traded_qty) if ss.traded_qty > 0 else D0
            if sell_from_traded > 0:
                realized_delta = self._consume_lots_lifo(ss, sell_from_traded, fill_price)
                ss.realized_pnl += realized_delta

            sell_from_borrow = qty - sell_from_traded
            if sell_from_borrow > 0:
                old_b = ss.borrowed_qty
                new_b = old_b + sell_from_borrow
                if new_b > 0:
                    ss.borrowed_avg_sell = ((ss.borrowed_avg_sell * old_b) + (fill_price * sell_from_borrow)) / new_b
                ss.borrowed_qty = new_b

        ss.reference_price = fill_price
        self.trades.append({
            "ts": ts.isoformat(),
            "symbol": symbol,
            "side": side,
            "qty": float(qty),
            "price": float(fill_price),
            "notional": float(notional),
            "fee": float(fee),
            "reason": reason,
            "realized_delta": float(realized_delta),
            "cash_after": float(self.state.cash),
            "traded_qty_after": float(ss.traded_qty),
            "traded_avg_after": float(ss.traded_avg_price),
            "borrowed_qty_after": float(ss.borrowed_qty),
            "reference_after": float(ss.reference_price) if ss.reference_price is not None else None,
        })
        return FillResult(True, "filled", realized_delta=realized_delta, fee=fee, fill_price=fill_price, notional=notional)


def _compute_equity_curve(state: GlobalState, ts: pd.Timestamp) -> Dict[str, Any]:
    realized = _dec(state.total_realized())
    unreal = _dec(state.total_unrealized())
    equity = _dec(state.strategy_equity())
    return {
        "ts": ts.isoformat(),
        "cash": float(state.cash),
        "strategy_equity": float(equity),
        "realized": float(realized),
        "unrealized": float(unreal),
    }


def run_backtest(
    *,
    cfg: Dict[str, Any],
    candles: pd.DataFrame,
    initial_cash: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
) -> tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
    strategy = _load_strategy(cfg)
    symbols = list((cfg.get("strategy") or {}).get("symbols") or [])
    if not symbols:
        raise SystemExit("Config must include strategy.symbols.")

    state = GlobalState(cash=initial_cash)
    state.ensure_symbols(symbols)
    ex = cfg.get("execution") or {}
    state.extras["use_inventory_buffer"] = bool(ex.get("use_inventory_buffer", False))

    execu = BacktestExecutor(state, fee_bps=fee_bps, slippage_bps=slippage_bps)
    curve_rows: List[Dict[str, Any]] = []
    last_prices: Dict[str, Decimal] = {}

    for ts, grp in candles.groupby("ts", sort=True):
        for _, r in grp.iterrows():
            sym = str(r["symbol"])
            px = _dec(r["close"])
            last_prices[sym] = px
            state.last_prices[sym] = px
            state.symbol_states[sym].last_mark_price = px

        # wait until all symbols have at least one seen price
        if any(sym not in last_prices for sym in symbols):
            continue

        # initialize ladder anchors like live runner
        for sym in symbols:
            ss = state.symbol_states[sym]
            if ss.reference_price is None:
                ss.reference_price = _dec(last_prices[sym])

        # keep portfolio value available for fixed_percent_of_portfolio sizing mode
        state.extras["portfolio_value"] = str(state.strategy_equity())
        prices_for_strategy = {s: _dec(last_prices[s]) for s in symbols}
        intents = strategy.on_prices(prices_for_strategy, state, ts.isoformat())

        for it in intents:
            execu.execute_intent(
                ts=ts,
                symbol=str(it.symbol),
                side=str(it.side),
                qty=_dec(it.qty),
                ltp=_dec(last_prices[str(it.symbol)]),
                reason=str(it.reason),
            )

        curve_rows.append(_compute_equity_curve(state, ts))

    curve = pd.DataFrame(curve_rows)
    trades = pd.DataFrame(execu.trades)

    end_equity = _dec(curve["strategy_equity"].iloc[-1]) if not curve.empty else _dec(initial_cash)
    start_equity = _dec(initial_cash)
    pnl = end_equity - start_equity
    pnl_pct = (pnl / start_equity) if start_equity > 0 else D0
    realized = _dec(state.total_realized())
    unrealized = _dec(state.total_unrealized())

    max_dd_pct = D0
    if not curve.empty:
        s = pd.to_numeric(curve["strategy_equity"], errors="coerce")
        peak = s.cummax()
        dd = (peak - s) / peak.replace(0, pd.NA)
        dd = dd.fillna(0.0)
        max_dd_pct = _dec(dd.max())

    summary = {
        "symbols": symbols,
        "rows_processed": int(len(candles)),
        "ticks_processed": int(len(curve)),
        "trades_filled": int(len(trades)),
        "orders_rejected": int(execu.rejected),
        "initial_cash": float(start_equity),
        "final_equity": float(end_equity),
        "pnl": float(pnl),
        "pnl_pct": float(pnl_pct),
        "realized": float(realized),
        "unrealized": float(unrealized),
        "max_drawdown_pct": float(max_dd_pct),
        "start_ts": str(candles["ts"].min()) if not candles.empty else None,
        "end_ts": str(candles["ts"].max()) if not candles.empty else None,
    }
    return summary, trades, curve


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest pct_ladder strategy on historical candle CSV.")
    ap.add_argument("--config", required=True, help="Strategy config JSON path (pct_ladder config).")
    ap.add_argument("--candles-csv", required=True, help="CSV with columns: ts,symbol,close (symbol optional for single symbol).")
    ap.add_argument("--start", default=None, help="Optional UTC start timestamp filter.")
    ap.add_argument("--end", default=None, help="Optional UTC end timestamp filter.")
    ap.add_argument("--initial-cash", type=str, default=None, help="Initial quote cash. Defaults to backtest.initial_cash or strategy.fixed_capital or 10000.")
    ap.add_argument("--fee-bps", type=str, default="0", help="Fee in basis points per fill (applied on notional).")
    ap.add_argument("--slippage-bps", type=str, default="0", help="Slippage in basis points per fill.")
    ap.add_argument("--out-dir", default="", help="Output directory. Default: backtests/pct_ladder_<UTC timestamp>.")
    args = ap.parse_args()

    cfg, _ = _load_config(args.config)
    symbols = list((cfg.get("strategy") or {}).get("symbols") or [])
    if not symbols:
        raise SystemExit("Config must include strategy.symbols list.")

    bt_cfg = cfg.get("backtest") if isinstance(cfg.get("backtest"), dict) else {}
    if args.initial_cash is not None:
        initial_cash = _dec(args.initial_cash)
    elif bt_cfg.get("initial_cash") is not None:
        initial_cash = _dec(bt_cfg.get("initial_cash"))
    elif (cfg.get("strategy") or {}).get("fixed_capital") is not None:
        initial_cash = _dec((cfg.get("strategy") or {}).get("fixed_capital"))
    else:
        initial_cash = Decimal("10000")
    if initial_cash < 0:
        raise SystemExit("--initial-cash must be >= 0.")

    candles = _normalize_candles(args.candles_csv, symbols=symbols, start=args.start, end=args.end)
    summary, trades, curve = run_backtest(
        cfg=cfg,
        candles=candles,
        initial_cash=initial_cash,
        fee_bps=_dec(args.fee_bps),
        slippage_bps=_dec(args.slippage_bps),
    )

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else Path("backtests") / f"pct_ladder_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}"
    out_dir.mkdir(parents=True, exist_ok=True)
    trades_path = out_dir / "trades.csv"
    curve_path = out_dir / "equity_curve.csv"
    summary_path = out_dir / "summary.json"

    trades.to_csv(trades_path, index=False)
    curve.to_csv(curve_path, index=False)
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"\nSaved:\n- {summary_path}\n- {trades_path}\n- {curve_path}")


if __name__ == "__main__":
    main()

