#!/usr/bin/env python3
"""
run_custom_basket.py — run the permanent-grid backtest on an ARBITRARY list of
stocks fetched by fetch_custom_data.py. Bypasses the Nifty50 auto-selector and
uses your exact basket (no dynamic replacement).

Usage:
    env/bin/python permanent_grid/run_custom_basket.py \
        --data permanent_grid/data/custom_data.pkl \
        --start 2023-01-01 --end 2024-06-30 \
        --capital 10000000 --grid-pct 0.02 --runway 0.25
    # optional: restrict to a subset already in the pickle
        --symbols NSE:KPITTECH-EQ,NSE:CDSL-EQ

Importable too:  from permanent_grid.run_custom_basket import run_custom_basket
"""
import sys, os, pickle, argparse
sys.path.insert(0, "/root/trading_bot")
from permanent_grid.backtest_5min import PermanentGridBacktest5Min, CostModel


def run_custom_basket(data_pkl, sim_start, sim_end, *, symbols=None,
                      total_capital=10_000_000, grid_pct=0.02, runway_pct=0.25,
                      up_runway=None, down_runway=None, entry_filter=True,
                      capital_map=None, auto_reenter=False,
                      leverage=1.0, mtf_interest_annual_pct=0.0,
                      verbose=True):
    """Returns (bt, equity_df). `symbols` (fyers) optionally restricts the basket.

    entry_filter=False  -> every stock enters immediately on day 1 (skips the
                           DMA/52-week/support entry gate). Use for ETFs / new listings.
    up_runway/down_runway -> asymmetric runway: seed `up_runway` of capital into stock
                           at entry, reserve `down_runway` as cash for dip-buys.
                           (overrides runway_pct when both are set)
    capital_map        -> {fyers_symbol: capital} per-ticker capital (overrides the
                           equal split of total_capital). Missing tickers fall back to equal.
    auto_reenter=True   -> an UPSIDE_OUT / BLOCKED grid auto re-seeds the moment price is
                           back inside its runway band (no manual action / replacement).
    leverage           -> 1.0 = cash only; 2.0 = 2x; 3.0 = 3x. Positions sized at
                           capital*leverage; broker funds (1-1/leverage) of each position.
    mtf_interest_annual_pct -> annual % charged on borrowed funds (e.g. 18 or 16), daily.
    """
    d = pickle.load(open(data_pkl, "rb"))
    all_daily, all_5min = d["daily"], d["min5"]
    nifty_5min = d.get("nifty_5min")
    sectors = d.get("sectors", {})

    use = symbols or d.get("symbols") or list(all_daily.keys())
    missing = [s for s in use if s not in all_daily]
    if missing:
        raise ValueError(f"No data for {missing} — fetch them first with fetch_custom_data.py")

    basket, daily_data, data_5min = [], {}, {}
    cap_by_name = {}
    for fsym in use:
        name = fsym.split(":", 1)[-1].replace("-EQ", "")
        basket.append({"symbol": name, "fyers_symbol": fsym,
                       "sector": sectors.get(fsym, "Custom")})
        daily_data[fsym] = all_daily[fsym]
        if fsym in all_5min:
            data_5min[fsym] = all_5min[fsym]
        if capital_map and fsym in capital_map:
            cap_by_name[name] = capital_map[fsym]

    if verbose:
        print(f"Custom basket ({len(basket)}): {[b['symbol'] for b in basket]}")

    bt = PermanentGridBacktest5Min(
        basket=basket, daily_data=daily_data, data_5min=data_5min, nifty_5min=nifty_5min,
        total_capital=total_capital, grid_pct=grid_pct, runway_pct=runway_pct,
        up_runway_pct=up_runway, down_runway_pct=down_runway, entry_filter=entry_filter,
        capital_map=(cap_by_name or None), auto_reenter=auto_reenter,
        leverage=leverage, mtf_interest_annual_pct=mtf_interest_annual_pct,
        sim_start=sim_start, sim_end=sim_end,
        dynamic_replacement=False, entry_mode="basket", cost_model=CostModel())
    bt.run()
    return bt, bt.get_equity_curve()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="/root/trading_bot/permanent_grid/data/custom_data.pkl")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--symbols", help="comma-separated subset (fyers symbols)")
    ap.add_argument("--capital", type=float, default=10_000_000)
    ap.add_argument("--grid-pct", type=float, default=0.02)
    ap.add_argument("--runway", type=float, default=0.25)
    ap.add_argument("--up-runway", type=float, default=None, help="asymmetric: upside runway (e.g. 0.25)")
    ap.add_argument("--down-runway", type=float, default=None, help="asymmetric: downside cash runway (e.g. 0.05)")
    ap.add_argument("--no-entry-filter", action="store_true", help="enter all tickers immediately (skip DMA/52w/support gate)")
    ap.add_argument("--auto-reenter", action="store_true", help="auto re-seed UPSIDE_OUT/BLOCKED grids when price is back in band")
    ap.add_argument("--leverage", type=float, default=1.0, help="1=cash, 2=2x, 3=3x")
    ap.add_argument("--mtf-interest", type=float, default=0.0, help="annual %% on borrowed funds (e.g. 18)")
    args = ap.parse_args()

    subset = [s.strip() for s in args.symbols.split(",")] if args.symbols else None
    bt, eq = run_custom_basket(args.data, args.start, args.end, symbols=subset,
                               total_capital=args.capital, grid_pct=args.grid_pct,
                               runway_pct=args.runway, up_runway=args.up_runway,
                               down_runway=args.down_runway,
                               entry_filter=not args.no_entry_filter,
                               auto_reenter=args.auto_reenter,
                               leverage=args.leverage,
                               mtf_interest_annual_pct=args.mtf_interest)
    net = float(eq["total_pnl_net"].iloc[-1])
    print(f"\nFinal net PnL: ₹{net:,.0f}  ({net/args.capital*100:+.2f}% on capital)")
    print(eq[["date", "total_pnl_gross", "total_pnl_net"]].tail())


if __name__ == "__main__":
    main()
