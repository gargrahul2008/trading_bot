#!/usr/bin/env python3
"""
run_fib_backtest.py — Run the BOS-Fib scalping strategy on any ticker(s).

Supports:
  • NSE equities  (data/fyers/NSE_<SYM>_EQ.parquet)
  • NIFTY index   (data/fyers/NSE_NIFTY50_INDEX.parquet)
  • Crypto        (data/binance/<SYM>_1m.parquet)

Missing data is auto-fetched: equity/index from Fyers, crypto from Binance.

Strategy parameters (min_impulse_points, stop_buffer_points) are scaled automatically
relative to each symbol's median price so the strategy behaves comparably across all
price levels.  Override with --min-impulse-pct / --stop-buffer-pct if needed.

Usage:
    # Two equity tickers
    python scripts/run_fib_backtest.py --symbols RELIANCE HDFCBANK

    # All ~50 NIFTY50 constituents
    python scripts/run_fib_backtest.py --all-equity

    # NIFTY index
    python scripts/run_fib_backtest.py --symbols NIFTY50 --index

    # Crypto (after fetching with fetch_binance_1m.py)
    python scripts/run_fib_backtest.py --symbols BTCUSDT --crypto

    # Mix: equity + crypto in one run, custom date range
    python scripts/run_fib_backtest.py --symbols RELIANCE BTCUSDT \\
        --start 2026-04-01 --end 2026-06-08

    # Tune: zone_touch mode, strong setups only (0.2% impulse min)
    python scripts/run_fib_backtest.py --symbols RELIANCE --mode zone_touch_confirmation \\
        --min-impulse-pct 0.20
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT  = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

from intraday_research.analytics import build_backtest_summary
from intraday_research.backtester import BacktestConfig, IntradayBacktester
from intraday_research.costs import TransactionCostModel
from intraday_research.features import FeatureEngine
from intraday_research.provider import MarketDataProvider
from intraday_research.risk import RiskManager
from intraday_research.strategies import NiftyBOSFibScalpStrategy
from intraday_research.types import InstrumentSpec

# ── All NIFTY50 equities available in data/fyers/ ────────────────────────────
_ALL_EQUITY_SYMS = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ_AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
    "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JIOFIN", "JSWSTEEL",
    "KOTAKBANK", "LT", "M&M", "MARUTI", "NESTLEIND",
    "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATASTEEL",
    "TCS", "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
]

# NSE equity intraday costs (Zerodha-style flat brokerage)
_EQUITY_COSTS = dict(
    brokerage_rate=0.0003,
    brokerage_cap_per_order=20.0,
    stt_rate=0.00025,
    exchange_charge_rate=0.0000345,
    gst_rate=0.18,
    sebi_charge_rate=0.000001,
    stamp_duty_rate=0.00003,
    slippage_bps_per_side=1.0,   # equities: slightly wider spread than index
)

# Crypto costs (Binance, 0.1% maker/taker)
_CRYPTO_COSTS = dict(
    brokerage_rate=0.001,
    brokerage_cap_per_order=1e9,  # no cap
    stt_rate=0.0,
    exchange_charge_rate=0.0,
    gst_rate=0.0,
    sebi_charge_rate=0.0,
    stamp_duty_rate=0.0,
    slippage_bps_per_side=2.0,   # crypto: wider spread
)


def _asset_type(is_crypto: bool, is_index: bool, symbol: str) -> str:
    if is_crypto or symbol.endswith("USDT"):
        return "crypto"
    if is_index or symbol == "NIFTY50":
        return "index"
    return "equity"


def _scale_params(df: pd.DataFrame, min_impulse_pct: float,
                  stop_buffer_pct: float) -> tuple[float, float]:
    """Compute absolute point parameters from percentage-of-price."""
    med_price = float(df["close"].median())
    min_imp   = round(med_price * min_impulse_pct / 100, 4)
    stop_buf  = round(max(med_price * stop_buffer_pct / 100, 0.05), 4)
    return min_imp, stop_buf


def run_one(symbol: str, market_data: pd.DataFrame, asset_type: str,
            mode: str, quantity: int,
            min_impulse_pct: float, stop_buffer_pct: float) -> dict:
    """Run backtest for one symbol. Returns summary dict."""

    min_imp, stop_buf = _scale_params(market_data, min_impulse_pct, stop_buffer_pct)
    sym_col = market_data["symbol"].iloc[0]

    if asset_type == "index":
        spec = InstrumentSpec(quantity_mode="lots", lot_size=65, instrument_type="index")
    else:
        spec = InstrumentSpec(quantity_mode="units", lot_size=1, instrument_type="equity")

    cost_params = _CRYPTO_COSTS if asset_type == "crypto" else _EQUITY_COSTS
    cost_model  = TransactionCostModel(**cost_params)

    strategy = NiftyBOSFibScalpStrategy(
        name=f"BOS_FIB_{symbol}",
        pivot_lookback=2,
        min_swing_points=2,
        fib_zone_low=0.50,
        fib_zone_high=0.618,
        stop_buffer_points=stop_buf,
        min_impulse_points=min_imp,
        max_hold_bars=15,
        max_trades_per_day=500,
        max_consecutive_losses_per_day=200,
        entry_mode=mode,
        min_confirmation_rr=0.0,
    )

    backtester = IntradayBacktester(
        strategies=[strategy],
        feature_engine=FeatureEngine(),
        risk_manager=RiskManager(
            max_positions=1,
            max_daily_loss=50_000.0,
            quantity_per_trade=quantity,
        ),
        cost_model=cost_model,
    )

    result = backtester.run(
        market_data,
        BacktestConfig(
            output_dir=f"artifacts/multi_ticker/{symbol}",
            instrument_specs={sym_col: spec},
            initial_capital=1_000_000.0,
            max_positions=1,
            max_daily_loss=50_000.0,
            quantity_per_trade=quantity,
            sizing_mode="fixed",
            capital_per_trade_pct=0.10,
            risk_per_trade_pct=0.005,
            max_capital_per_trade_pct=0.10,
            # Crypto timestamps are converted to IST at load time, so the
            # same 09:20–15:25 IST window applies for both equity and crypto.
            first_trade_time=pd.Timestamp("09:20").time(),
            last_entry_time=pd.Timestamp("15:25").time(),
            time_exit_minutes=15,
            force_square_off_minutes_before_close=1,
        ),
    )

    trades = result["trades"]
    summary = build_backtest_summary(trades, result["daily_pnl"])

    n_signals = len(result.get("selected_strategy_signals", []))
    stops  = int((trades["exit_reason"] == "stop_loss").sum()) if not trades.empty else 0
    tgts   = int((trades["exit_reason"] == "target").sum()) if not trades.empty else 0
    timeex = int((trades["exit_reason"] == "time_exit").sum()) if not trades.empty else 0

    return {
        "symbol":      symbol,
        "min_imp":     min_imp,
        "stop_buf":    stop_buf,
        "signals":     n_signals,
        "trades":      summary["total_trades"],
        "win_pct":     summary["win_rate"],
        "gross":       summary["gross_pnl"],
        "net":         summary["net_pnl"],
        "pf":          summary["profit_factor"],
        "stops":       stops,
        "tgts":        tgts,
        "time_exits":  timeex,
    }


def main():
    ap = argparse.ArgumentParser(description="Multi-ticker BOS-Fib backtest")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--symbols", nargs="+",
                     help="Ticker name(s): RELIANCE, HDFCBANK, BTCUSDT, etc.")
    grp.add_argument("--all-equity", action="store_true",
                     help="Run on all ~50 available NIFTY50 equities")

    ap.add_argument("--crypto", action="store_true",
                    help="Symbols are crypto (loads from data/binance/)")
    ap.add_argument("--index", action="store_true",
                    help="Symbol is a NIFTY index (loads NSE_NIFTY50_INDEX.parquet)")
    ap.add_argument("--start", default="2026-04-01",
                    help="Start date YYYY-MM-DD (default: 2026-04-01)")
    ap.add_argument("--end", default="2026-06-08",
                    help="End date YYYY-MM-DD (default: 2026-06-08)")
    ap.add_argument("--mode", default="limit_618",
                    choices=["limit_618", "zone_touch_confirmation"],
                    help="Entry mode (default: limit_618)")
    ap.add_argument("--quantity", type=int, default=1,
                    help="Quantity per trade — shares/units/lots (default: 1)")
    ap.add_argument("--min-impulse-pct", type=float, default=0.09,
                    help="Min BOS impulse as %% of price (default: 0.09 → ~20 pts for NIFTY)")
    ap.add_argument("--stop-buffer-pct", type=float, default=0.009,
                    help="Stop buffer as %% of price (default: 0.009 → ~2 pts for NIFTY)")
    args = ap.parse_args()

    symbols = args.symbols or (_ALL_EQUITY_SYMS if args.all_equity else None)
    if not symbols:
        ap.error("Provide --symbols or --all-equity")

    print(f"\nBOS-Fib Backtest  mode={args.mode}  {args.start} → {args.end}")
    print(f"min_impulse={args.min_impulse_pct}% of price  stop_buffer={args.stop_buffer_pct}% of price")
    print(f"{'symbol':<16} {'min_imp':>8} {'signals':>8} {'trades':>7} "
          f"{'gross':>12} {'net':>12} {'win%':>6} {'PF':>5} {'S/T/Te':>10}")
    print("─" * 100)

    provider = MarketDataProvider(repo_root=REPO_ROOT, verbose=True)
    results = []
    for sym in symbols:
        try:
            asset_type = _asset_type(args.crypto, args.index, sym)
            market_data = provider.load(sym, asset_type, args.start, args.end)
            if market_data.empty:
                print(f"  {sym:<14}  no data in date range — skipping")
                continue

            r = run_one(sym, market_data, asset_type=asset_type,
                        mode=args.mode,
                        quantity=args.quantity,
                        min_impulse_pct=args.min_impulse_pct,
                        stop_buffer_pct=args.stop_buffer_pct)
            results.append(r)
            print(f"  {r['symbol']:<14} {r['min_imp']:>8.2f} {r['signals']:>8} {r['trades']:>7} "
                  f"{r['gross']:>12,.0f} {r['net']:>12,.0f} "
                  f"{r['win_pct']:>6.1f} {r['pf']:>5.2f} "
                  f"{r['stops']:>3}/{r['tgts']:>3}/{r['time_exits']:>3}")
        except FileNotFoundError as e:
            print(f"  {sym:<14}  SKIP — {e}")
        except Exception as e:
            print(f"  {sym:<14}  ERROR — {e}")
            import traceback; traceback.print_exc()

    if not results:
        print("\nNo results.")
        return

    # ── Summary ───────────────────────────────────────────────────────────────
    df_res = pd.DataFrame(results).sort_values("pf", ascending=False)
    print("\n── Top 10 by Profit Factor ──")
    print(df_res.head(10)[["symbol", "trades", "win_pct", "gross", "net", "pf"]].to_string(index=False))

    winners = df_res[df_res["gross"] > 0]
    print(f"\nGross positive: {len(winners)}/{len(df_res)} symbols")
    if not winners.empty:
        best = df_res.sort_values("gross", ascending=False).iloc[0]
        print(f"Best gross PnL: {best['symbol']}  gross={best['gross']:,.0f}")


if __name__ == "__main__":
    main()
