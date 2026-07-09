from __future__ import annotations

import pandas as pd

from intraday_research.positional_futures import (
    DailyFuturesFeatureEngine,
    DailyPullbackFuturesStrategy,
    FuturesInstrument,
    FuturesSide,
    PositionalFuturesBacktester,
    PositionalPullbackConfig,
    PositionalSignal,
    load_futures_universe,
    positional_summary,
)


def make_daily(rows: list[list[object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return frame


def signal(
    *,
    side: FuturesSide = FuturesSide.LONG,
    trigger_price: float = 105.0,
    stop_price: float = 99.0,
    valid_until: str = "2026-01-04 15:30:00+05:30",
) -> PositionalSignal:
    return PositionalSignal(
        symbol="RELIANCE_FUT_CONT",
        signal_time=pd.Timestamp("2026-01-01 15:30:00+05:30"),
        side=side,
        score=80.0,
        trigger_price=trigger_price,
        stop_price=stop_price,
        valid_until=pd.Timestamp(valid_until),
        reason="test",
        trend_gap_atr=0.8 if side is FuturesSide.LONG else -0.8,
        pullback_depth_atr=0.4,
        benchmark_regime=side.value,
        benchmark_trend_strength_atr=0.7,
    )


def test_daily_pullback_strategy_generates_long_and_blocks_against_benchmark() -> None:
    config = PositionalPullbackConfig(
        ema_fast_span=2,
        ema_slow_span=3,
        atr_window=2,
        benchmark_symbol="NIFTY_FUT_CONT",
        benchmark_trend_min_atr=0.01,
        benchmark_trend_max_atr=10.0,
        symbol_trend_min_atr=0.01,
        pullback_min_depth_atr=0.0,
        pullback_max_depth_atr=10.0,
        minimum_pullback_bars=1,
        min_reclaim_body_pct=0.01,
        min_close_location_pct=0.50,
        min_rejection_wick_to_body_ratio=0.0,
        min_volume_ratio=0.1,
        maximum_consumed_target_pct=10.0,
        min_signal_score=0.0,
    )
    raw = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "NIFTY_FUT_CONT", 100.0, 102.0, 99.0, 101.0, 1000],
            ["2026-01-02 15:30:00+05:30", "NIFTY_FUT_CONT", 101.0, 105.0, 100.0, 104.0, 1000],
            ["2026-01-03 15:30:00+05:30", "NIFTY_FUT_CONT", 104.0, 108.0, 103.0, 107.0, 1000],
            ["2026-01-04 15:30:00+05:30", "NIFTY_FUT_CONT", 107.0, 111.0, 106.0, 110.0, 1000],
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 102.0, 99.0, 101.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 105.0, 99.5, 102.0, 900],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 106.0, 100.5, 105.5, 1500],
            ["2026-01-04 15:30:00+05:30", "RELIANCE_FUT_CONT", 105.0, 108.0, 104.0, 107.0, 1500],
        ]
    )
    featured = DailyFuturesFeatureEngine(config).transform(raw)
    strategy = DailyPullbackFuturesStrategy(config)
    signals = strategy.generate_signals(featured)
    diagnostics = strategy.diagnose_filters(featured)

    assert any(item.symbol == "RELIANCE_FUT_CONT" and item.side is FuturesSide.LONG for item in signals)
    assert not any(item.symbol == "RELIANCE_FUT_CONT" and item.side is FuturesSide.SHORT for item in signals)
    assert int(diagnostics["would_signal"].sum()) >= 1


def test_positional_backtester_enters_long_on_signal_high_break_and_stops() -> None:
    featured = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 105.0, 99.0, 104.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 106.0, 98.0, 100.0, 1000],
        ]
    )
    config = PositionalPullbackConfig(sizing_mode="fixed_lots", fixed_lots=2, fee_bps_per_side=0.0, slippage_bps_per_side=0.0)
    backtester = PositionalFuturesBacktester(
        instruments={"RELIANCE_FUT_CONT": FuturesInstrument("RELIANCE_FUT_CONT", lot_size=250)},
        config=config,
    )
    trades = backtester.run(featured, [signal()])

    assert len(trades) == 1
    trade = trades.iloc[0]
    assert trade["side"] == "LONG"
    assert trade["lots"] == 2
    assert trade["effective_quantity"] == 500
    assert trade["entry_price"] == 105.0
    assert trade["exit_price"] == 99.0
    assert trade["exit_reason"] == "stop_loss"
    assert trade["gross_pnl"] == -3000.0


def test_daily_pullback_strategy_requires_minimum_pullback_bars() -> None:
    config = PositionalPullbackConfig(
        ema_fast_span=2,
        ema_slow_span=3,
        atr_window=2,
        symbol_trend_min_atr=0.01,
        pullback_min_depth_atr=0.0,
        pullback_max_depth_atr=10.0,
        minimum_pullback_bars=2,
        min_reclaim_body_pct=0.01,
        min_close_location_pct=0.50,
        min_rejection_wick_to_body_ratio=0.0,
        min_volume_ratio=0.1,
        maximum_consumed_target_pct=10.0,
        min_signal_score=0.0,
    )
    raw = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 102.0, 99.0, 101.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 105.0, 99.5, 102.0, 900],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 106.0, 100.5, 105.5, 1500],
        ]
    )
    featured = DailyFuturesFeatureEngine(config).transform(raw)
    signals = DailyPullbackFuturesStrategy(config).generate_signals(featured)

    assert signals == []


def test_daily_pullback_strategy_makes_rejection_wick_optional_for_daily() -> None:
    config = PositionalPullbackConfig(
        ema_fast_span=2,
        ema_slow_span=3,
        atr_window=2,
        symbol_trend_min_atr=0.01,
        pullback_min_depth_atr=0.0,
        pullback_max_depth_atr=10.0,
        minimum_pullback_bars=1,
        min_reclaim_body_pct=0.01,
        min_close_location_pct=0.50,
        require_rejection_wick=False,
        min_rejection_wick_to_body_ratio=2.0,
        min_volume_ratio=0.1,
        maximum_consumed_target_pct=10.0,
        min_signal_score=0.0,
    )
    raw = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 102.0, 99.0, 101.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 105.0, 99.5, 102.0, 900],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 106.0, 101.0, 105.5, 1500],
        ]
    )
    featured = DailyFuturesFeatureEngine(config).transform(raw)
    signals = DailyPullbackFuturesStrategy(config).generate_signals(featured)

    strict_config = PositionalPullbackConfig(**{**config.__dict__, "require_rejection_wick": True})
    strict_signals = DailyPullbackFuturesStrategy(strict_config).generate_signals(
        DailyFuturesFeatureEngine(strict_config).transform(raw)
    )

    assert len(signals) >= 1
    assert strict_signals == []


def test_daily_pullback_strategy_makes_volume_expansion_optional_for_daily() -> None:
    config = PositionalPullbackConfig(
        ema_fast_span=2,
        ema_slow_span=3,
        atr_window=2,
        symbol_trend_min_atr=0.01,
        pullback_min_depth_atr=0.0,
        pullback_max_depth_atr=10.0,
        minimum_pullback_bars=1,
        min_reclaim_body_pct=0.01,
        min_close_location_pct=0.50,
        require_volume_expansion=False,
        min_volume_ratio=10.0,
        maximum_consumed_target_pct=10.0,
        min_signal_score=0.0,
    )
    raw = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 102.0, 99.0, 101.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 105.0, 99.5, 102.0, 900],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 101.0, 106.0, 101.0, 105.5, 800],
        ]
    )
    featured = DailyFuturesFeatureEngine(config).transform(raw)
    signals = DailyPullbackFuturesStrategy(config).generate_signals(featured)

    strict_config = PositionalPullbackConfig(**{**config.__dict__, "require_volume_expansion": True})
    strict_signals = DailyPullbackFuturesStrategy(strict_config).generate_signals(
        DailyFuturesFeatureEngine(strict_config).transform(raw)
    )

    assert len(signals) >= 1
    assert strict_signals == []


def test_daily_pullback_strategy_rejects_broken_swing_structure() -> None:
    config = PositionalPullbackConfig(
        ema_fast_span=2,
        ema_slow_span=3,
        atr_window=2,
        symbol_trend_min_atr=0.01,
        pullback_min_depth_atr=0.0,
        pullback_max_depth_atr=10.0,
        minimum_pullback_bars=1,
        swing_lookback=2,
        min_reclaim_body_pct=0.01,
        min_close_location_pct=0.50,
        min_rejection_wick_to_body_ratio=0.0,
        min_volume_ratio=0.1,
        maximum_consumed_target_pct=10.0,
        min_signal_score=0.0,
    )
    raw = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 103.0, 100.0, 102.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 102.0, 106.0, 101.0, 105.0, 1000],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 106.0, 99.0, 102.0, 900],
            ["2026-01-04 15:30:00+05:30", "RELIANCE_FUT_CONT", 102.0, 107.0, 101.5, 106.0, 1500],
        ]
    )
    featured = DailyFuturesFeatureEngine(config).transform(raw)
    signals = DailyPullbackFuturesStrategy(config).generate_signals(featured)

    assert signals == []


def test_positional_backtester_enters_short_on_signal_low_break() -> None:
    featured = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 101.0, 95.0, 96.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 96.0, 97.0, 94.0, 95.0, 1000],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 95.0, 93.0, 90.0, 91.0, 1000],
        ]
    )
    config = PositionalPullbackConfig(
        sizing_mode="fixed_lots",
        fixed_lots=1,
        max_holding_days=1,
        fee_bps_per_side=0.0,
        slippage_bps_per_side=0.0,
    )
    backtester = PositionalFuturesBacktester(config=config)
    trades = backtester.run(featured, [signal(side=FuturesSide.SHORT, trigger_price=95.0, stop_price=101.0)])

    assert len(trades) == 1
    trade = trades.iloc[0]
    assert trade["side"] == "SHORT"
    assert trade["entry_price"] == 95.0
    assert trade["exit_price"] == 91.0
    assert trade["gross_pnl"] == 4.0


def test_positional_backtester_allows_reentry_after_early_stop() -> None:
    featured = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 105.0, 99.0, 104.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 106.0, 98.0, 100.0, 1000],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 106.0, 102.0, 104.0, 1000],
            ["2026-01-04 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 108.0, 103.0, 107.0, 1000],
        ]
    )
    config = PositionalPullbackConfig(
        sizing_mode="fixed_lots",
        fixed_lots=1,
        max_reentries=1,
        reentry_days_after_stop=3,
        fee_bps_per_side=0.0,
        slippage_bps_per_side=0.0,
    )
    backtester = PositionalFuturesBacktester(config=config)
    trades = backtester.run(featured, [signal(valid_until="2026-01-04 15:30:00+05:30")])

    assert len(trades) == 2
    assert trades.iloc[0]["exit_reason"] == "stop_loss"
    assert trades.iloc[1]["reentry_number"] == 1


def test_positional_backtester_does_not_trail_before_activation_r() -> None:
    featured = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 105.0, 99.0, 104.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 106.0, 103.0, 105.0, 1000],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 105.0, 106.0, 103.5, 105.5, 1000],
        ]
    )
    config = PositionalPullbackConfig(
        sizing_mode="fixed_lots",
        fixed_lots=1,
        trailing_activation_r=1.0,
        fee_bps_per_side=0.0,
        slippage_bps_per_side=0.0,
    )
    backtester = PositionalFuturesBacktester(config=config)
    trades = backtester.run(featured, [signal(trigger_price=105.0, stop_price=99.0)])

    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "end_of_data"
    assert trades.iloc[0]["exit_price"] == 105.5


def test_positional_backtester_trails_after_activation_r() -> None:
    featured = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 105.0, 99.0, 104.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 112.0, 104.0, 111.0, 1000],
            ["2026-01-03 15:30:00+05:30", "RELIANCE_FUT_CONT", 111.0, 113.0, 103.5, 105.5, 1000],
        ]
    )
    config = PositionalPullbackConfig(
        sizing_mode="fixed_lots",
        fixed_lots=1,
        trailing_activation_r=1.0,
        fee_bps_per_side=0.0,
        slippage_bps_per_side=0.0,
    )
    backtester = PositionalFuturesBacktester(config=config)
    trades = backtester.run(featured, [signal(trigger_price=105.0, stop_price=99.0)])

    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "stop_loss"
    assert trades.iloc[0]["exit_price"] == 104.0


def test_positional_backtester_risk_sizing_uses_lot_size() -> None:
    featured = make_daily(
        [
            ["2026-01-01 15:30:00+05:30", "RELIANCE_FUT_CONT", 100.0, 105.0, 100.0, 104.0, 1000],
            ["2026-01-02 15:30:00+05:30", "RELIANCE_FUT_CONT", 104.0, 106.0, 103.0, 105.0, 1000],
        ]
    )
    config = PositionalPullbackConfig(
        sizing_mode="risk",
        initial_capital=1_000_000.0,
        risk_per_trade_pct=0.01,
        max_capital_per_trade_pct=1.0,
        fee_bps_per_side=0.0,
        slippage_bps_per_side=0.0,
    )
    backtester = PositionalFuturesBacktester(
        instruments={"RELIANCE_FUT_CONT": FuturesInstrument("RELIANCE_FUT_CONT", lot_size=100)},
        config=config,
    )
    trades = backtester.run(featured, [signal(trigger_price=105.0, stop_price=100.0)])

    assert len(trades) == 1
    assert trades.iloc[0]["lots"] == 20
    assert trades.iloc[0]["effective_quantity"] == 2000
    assert positional_summary(trades)["trades"] == 1


def test_load_futures_universe_reads_lot_sizes(tmp_path) -> None:
    path = tmp_path / "futures_universe.json"
    path.write_text(
        """
        {
          "name": "test_futures",
          "benchmark_symbol": "NSE:NIFTY26JUNFUT",
          "symbols": [
            {
              "symbol": "NSE:RELIANCE-EQ",
              "data_symbol": "NSE:RELIANCE-EQ",
              "underlying": "RELIANCE",
              "lot_size": 250
            }
          ]
        }
        """
    )

    universe = load_futures_universe(path)

    assert universe.name == "test_futures"
    assert universe.benchmark_symbol == "NSE:NIFTY26JUNFUT"
    assert universe.symbols == ("NSE:RELIANCE-EQ",)
    assert universe.data_symbols == ("NSE:RELIANCE-EQ",)
    assert universe.data_symbol_map == {"NSE:RELIANCE-EQ": "NSE:RELIANCE-EQ"}
    assert universe.instrument_map["NSE:RELIANCE-EQ"].lot_size == 250
