from __future__ import annotations

import json

import pandas as pd

from intraday_research.backtester import BacktestConfig, IntradayBacktester
from intraday_research.costs import TransactionCostModel
from intraday_research.data import MarketDataLoader
from intraday_research.reporting import daily_pnl_report
from intraday_research.risk import RiskManager
from intraday_research.strategy import Strategy
from intraday_research.sweeps import run_parameter_sweep
from intraday_research.types import InstrumentSpec
from tests.helpers import make_full_session_frame


class StaticSignalStrategy(Strategy):
    name = "static_signal"

    def __init__(self, signals: list[dict[str, object]]) -> None:
        self.signals = signals

    def generate_signals(self, frame: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for signal in self.signals:
            signal_time = pd.Timestamp(signal["timestamp"], tz="Asia/Kolkata")
            match = frame[frame["timestamp"] == signal_time].iloc[0]
            rows.append(
                {
                    "timestamp": match["timestamp"],
                    "symbol": match["symbol"],
                    "direction": signal["direction"],
                    "strength": signal.get("strength", 1.0),
                    "reason": signal.get("reason", "test_signal"),
                    "stop_loss": signal["stop_loss"],
                    "target": signal["target"],
                    "strategy_name": signal.get("strategy_name", self.name),
                }
            )
        return pd.DataFrame(rows)


def build_backtester(strategy: Strategy, *, cost_model: TransactionCostModel | None = None) -> IntradayBacktester:
    return IntradayBacktester(
        strategies=[strategy],
        risk_manager=RiskManager(quantity_per_trade=1, max_daily_loss=10_000),
        cost_model=cost_model or TransactionCostModel(
            brokerage_rate=0.0,
            brokerage_cap_per_order=0.0,
            stt_rate=0.0,
            exchange_charge_rate=0.0,
            gst_rate=0.0,
            sebi_charge_rate=0.0,
            stamp_duty_rate=0.0,
            slippage_bps_per_side=0.0,
        ),
    )


def test_force_square_off_before_close(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 20 else 101.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 95.0, "target": 120.0}]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=10_000, force_square_off_minutes_before_close=1),
    )

    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "force_square_off"
    assert pd.Timestamp(trades.iloc[0]["exit_time"]).tz_convert("Asia/Kolkata").time() == pd.Timestamp(
        "2026-06-01 15:29:00", tz="Asia/Kolkata"
    ).time()


def test_stop_loss_exit(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 100.5 if idx != 7 else 100.2, 99.5 if idx != 7 else 97.5, 100.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 105.0}]
    )
    result = build_backtester(strategy).run(prepared, BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=60))

    assert result["trades"].iloc[0]["exit_reason"] == "stop_loss"
    assert result["trades"].iloc[0]["exit_price"] == 98.0


def test_target_exit(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 100.5 if idx < 8 else 103.5, 99.5, 100.0 if idx < 8 else 102.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 103.0}]
    )
    result = build_backtester(strategy).run(prepared, BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=60))

    assert result["trades"].iloc[0]["exit_reason"] == "target"
    assert result["trades"].iloc[0]["exit_price"] == 103.0


def test_same_candle_stop_target_uses_conservative_assumption(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (
            100.0,
            104.0 if idx == 7 else 100.5,
            97.0 if idx == 7 else 99.5,
            100.0,
            1000.0,
        ),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 103.0}]
    )
    result = build_backtester(strategy).run(prepared, BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=60))

    assert result["trades"].iloc[0]["exit_reason"] == "stop_loss"
    assert result["trades"].iloc[0]["exit_price"] == 98.0


def test_no_overlapping_position_for_same_symbol(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.5, 100.0 if idx < 12 else 100.8, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
            {"timestamp": "2026-06-01 09:22:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
        ]
    )
    result = build_backtester(strategy).run(prepared, BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5))

    trades = result["trades"]
    assert len(trades) == 1
    assert trades.iloc[0]["entry_time"] != trades.iloc[0]["exit_time"]


def test_different_strategies_can_hold_same_symbol_concurrently(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.5, 100.0 if idx < 12 else 100.8, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    alpha = StaticSignalStrategy(
        [
            {
                "timestamp": "2026-06-01 09:20:00",
                "direction": "LONG",
                "stop_loss": 98.0,
                "target": 120.0,
                "strategy_name": "alpha",
            }
        ]
    )
    beta = StaticSignalStrategy(
        [
            {
                "timestamp": "2026-06-01 09:20:00",
                "direction": "LONG",
                "stop_loss": 98.0,
                "target": 120.0,
                "strategy_name": "beta",
            }
        ]
    )
    backtester = IntradayBacktester(
        strategies=[alpha, beta],
        risk_manager=RiskManager(quantity_per_trade=1, max_daily_loss=10_000, max_positions=2),
        cost_model=TransactionCostModel(
            brokerage_rate=0.0,
            brokerage_cap_per_order=0.0,
            stt_rate=0.0,
            exchange_charge_rate=0.0,
            gst_rate=0.0,
            sebi_charge_rate=0.0,
            stamp_duty_rate=0.0,
            slippage_bps_per_side=0.0,
        ),
    )

    result = backtester.run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5, max_positions=2),
    )

    trades = result["trades"].sort_values(["strategy_name", "entry_time"]).reset_index(drop=True)
    assert len(trades) == 2
    assert set(trades["strategy_name"]) == {"alpha", "beta"}
    assert len(set(trades["entry_time"])) == 1
    assert len(set(trades["exit_time"])) == 1


def test_no_same_candle_reentry_after_exit(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (
            100.0,
            103.5 if idx >= 7 else 100.5,
            99.5,
            102.0 if idx >= 7 else 100.0,
            1000.0,
        ),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 103.0},
            {"timestamp": "2026-06-01 09:22:00", "direction": "LONG", "stop_loss": 98.0, "target": 103.0},
        ]
    )
    result = build_backtester(strategy).run(prepared, BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=60))

    trades = result["trades"].sort_values(["entry_time", "exit_time"]).reset_index(drop=True)
    assert len(trades) == 1


def test_strategy_consecutive_loss_limit_blocks_entries_for_day(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (
            100.0,
            101.0,
            98.0 if idx in {6, 8, 10} else 99.5,
            100.0,
            1000.0,
        ),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 99.0, "target": 120.0},
            {"timestamp": "2026-06-01 09:22:00", "direction": "LONG", "stop_loss": 99.0, "target": 120.0},
            {"timestamp": "2026-06-01 09:24:00", "direction": "LONG", "stop_loss": 99.0, "target": 120.0},
        ]
    )
    strategy.max_consecutive_losses_per_day = 2

    result = build_backtester(strategy).run(prepared, BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=60))

    assert len(result["trades"]) == 2
    assert len(result["rejected_trades"]) == 1
    assert result["rejected_trades"].iloc[0]["reason"] == "max_consecutive_losses_per_day_reached"


def test_cost_calculation_breakdown(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 10 else 101.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 95.0, "target": 120.0}]
    )
    cost_model = TransactionCostModel(
        brokerage_rate=0.001,
        brokerage_cap_per_order=5.0,
        stt_rate=0.001,
        exchange_charge_rate=0.001,
        gst_rate=0.10,
        sebi_charge_rate=0.001,
        stamp_duty_rate=0.001,
        slippage_bps_per_side=10.0,
    )
    result = build_backtester(strategy, cost_model=cost_model).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5),
    )

    trade = result["trades"].iloc[0]
    assert round(trade["brokerage"], 6) == 0.201
    assert trade["stt"] > 0.0
    assert trade["exchange_charges"] > 0.0
    assert trade["gst"] > 0.0
    assert trade["sebi_charges"] > 0.0
    assert trade["stamp_duty"] > 0.0
    assert trade["entry_slippage"] == 0.0   # limit entry — no slippage
    assert trade["exit_slippage"] > 0.0    # time_exit is a market order
    assert trade["buy_turnover"] > 0.0
    assert trade["sell_turnover"] > 0.0
    assert trade["total_cost"] > 0.0
    assert trade["gross_pnl_per_share"] == trade["gross_pnl"]
    assert trade["cost_per_share"] == trade["total_cost"]
    assert trade["breakeven_move_required"] == trade["total_cost"]
    assert trade["cost_bps"] > 0.0
    assert trade["gross_bps"] != 0.0
    assert trade["net_bps"] < trade["gross_bps"]
    assert trade["net_pnl"] < trade["gross_pnl"]


def test_backtester_filters_weak_signal_before_execution(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 10 else 101.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {
                "timestamp": "2026-06-01 09:20:00",
                "direction": "LONG",
                "strength": 0.5,
                "stop_loss": 95.0,
                "target": 120.0,
                "strategy_name": "trend_pullback",
            }
        ]
    )

    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5, minimum_signal_score=1.0),
    )

    assert result["trades"].empty
    assert len(result["rejected_signals"]) == 1
    assert result["rejected_signals"].iloc[0]["stage"] == "signal_selector"


def test_backtester_applies_external_signal_filter_before_execution(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 10 else 101.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {
                "timestamp": "2026-06-01 09:20:00",
                "direction": "LONG",
                "strength": 80.0,
                "stop_loss": 95.0,
                "target": 120.0,
                "strategy_name": "trend_pullback",
            }
        ]
    )

    def reject_all(signals, featured):
        assert not featured.empty
        return [], [
            {
                "timestamp": signals[0].timestamp,
                "symbol": signals[0].symbol,
                "strategy_name": signals[0].strategy_name,
                "side": signals[0].side.value,
                "signal_score": signals[0].final_score,
                "reason": "external_filter_blocked",
            }
        ]

    result = IntradayBacktester(
        strategies=[strategy],
        risk_manager=RiskManager(quantity_per_trade=1, max_daily_loss=10_000),
        cost_model=TransactionCostModel(
            brokerage_rate=0.0,
            brokerage_cap_per_order=0.0,
            stt_rate=0.0,
            exchange_charge_rate=0.0,
            gst_rate=0.0,
            sebi_charge_rate=0.0,
            stamp_duty_rate=0.0,
            slippage_bps_per_side=0.0,
        ),
        signal_filter=reject_all,
    ).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5),
    )

    assert result["trades"].empty
    assert len(result["rejected_signals"]) == 1
    assert result["rejected_signals"].iloc[0]["stage"] == "signal_filter"
    assert result["rejected_signals"].iloc[0]["reason"] == "external_filter_blocked"


def test_risk_based_position_sizing_respects_risk_and_capital_limits(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 10 else 101.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 95.0, "target": 120.0}]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(
            output_dir=str(tmp_path),
            sizing_mode="risk_based",
            risk_per_trade_pct=0.01,
            max_capital_per_trade_pct=0.10,
            initial_capital=100_000,
        ),
    )

    trade = result["trades"].iloc[0]
    assert trade["quantity"] == 100


def test_capital_based_position_sizing_equalizes_deployed_capital(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (250.0, 251.0, 249.0, 250.0 if idx < 10 else 251.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 245.0, "target": 270.0}]
    )

    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(
            output_dir=str(tmp_path),
            sizing_mode="capital_based",
            capital_per_trade_amount=25_000.0,
            time_exit_minutes=5,
        ),
    )

    trade = result["trades"].iloc[0]
    assert trade["quantity"] == 100
    assert trade["buy_turnover"] == 25_000.0


def test_lot_based_index_position_scales_pnl_and_costs(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        symbol="INDEX",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 10 else 101.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [{"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 95.0, "target": 120.0}]
    )
    cost_model = TransactionCostModel(
        brokerage_rate=0.0,
        brokerage_cap_per_order=0.0,
        stt_rate=0.0,
        exchange_charge_rate=0.001,
        gst_rate=0.0,
        sebi_charge_rate=0.0,
        stamp_duty_rate=0.0,
        slippage_bps_per_side=0.0,
    )

    result = build_backtester(strategy, cost_model=cost_model).run(
        prepared,
        BacktestConfig(
            output_dir=str(tmp_path),
            time_exit_minutes=5,
            quantity_per_trade=2,
            instrument_specs={"INDEX": InstrumentSpec(quantity_mode="lots", lot_size=25, instrument_type="index")},
        ),
    )

    trade = result["trades"].iloc[0]
    assert trade["quantity"] == 2
    assert trade["effective_quantity"] == 50
    assert trade["lot_size"] == 25
    assert trade["gross_pnl"] == 50.0
    assert trade["buy_turnover"] == 5000.0
    assert trade["sell_turnover"] == 5050.0
    assert trade["exchange_charges"] == 10.05


def test_parameter_sweep_returns_one_row_per_parameter_set_and_strategy_mode(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0 if idx < 15 else 100.0 + ((idx - 14) * 0.2), 101.0 if idx < 15 else 101.5 + ((idx - 14) * 0.2), 99.0 if idx < 15 else 99.8 + ((idx - 14) * 0.2), 100.0 if idx < 15 else 100.5 + ((idx - 14) * 0.2), 1000.0 + idx),
    )
    prepared = MarketDataLoader().prepare(frame)
    sweep = run_parameter_sweep(
        prepared,
        base_config=BacktestConfig(output_dir=str(tmp_path)),
        grid={
            "first_trade_time": ["09:20", "09:25"],
            "last_entry_time": ["14:30"],
            "max_trades_per_symbol_per_day": [2],
            "cooldown_minutes_after_exit": [10],
            "opening_range_minutes": [15],
            "orb_breakout_buffer_atr": [0.1],
            "orb_stop_atr_multiple": [1.0],
            "orb_target_atr_multiple": [1.5],
            "vwap_reversion_band_atr": [1.0],
            "vwap_stop_atr_multiple": [1.0],
            "vwap_target_atr_multiple": [1.0],
            "pullback_touch_buffer_atr": [0.1],
            "pullback_min_depth_atr": [0.2],
            "pullback_stop_atr_multiple": [1.0],
            "pullback_target_atr_multiple": [1.5],
            "pullback_min_trend_gap_atr": [0.2],
            "pullback_min_reclaim_body_pct": [0.3],
        },
        strategy_modes=("orb_only", "combined", "trend_pullback_only", "orb_vwap_pullback"),
        cost_model=TransactionCostModel(
            brokerage_rate=0.0,
            brokerage_cap_per_order=0.0,
            stt_rate=0.0,
            exchange_charge_rate=0.0,
            gst_rate=0.0,
            sebi_charge_rate=0.0,
            stamp_duty_rate=0.0,
            slippage_bps_per_side=0.0,
        ),
    )

    assert len(sweep) == 8
    assert {
        "strategy_mode",
        "net_pnl",
        "gross_pnl",
        "trades",
        "total_cost",
        "net_expectancy",
        "avg_cost_per_trade",
        "avg_gross_per_trade",
        "avg_cost_bps",
        "avg_gross_bps",
        "max_drawdown",
        "profit_factor",
        "gross_win_rate",
        "net_win_rate",
    }.issubset(sweep.columns)


def test_daily_pnl_aggregation_and_outputs(tmp_path) -> None:
    day_one = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.0, 100.0 if idx < 10 else 101.0, 1000.0),
    )
    day_two = make_full_session_frame(
        "2026-06-02",
        price_fn=lambda idx, _ts: (100.0, 100.5, 98.0 if idx == 10 else 99.0, 100.0, 1000.0),
    )
    prepared = MarketDataLoader().prepare(pd.concat([day_one, day_two], ignore_index=True))
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 95.0, "target": 120.0, "strategy_name": "alpha"},
            {"timestamp": "2026-06-02 09:20:00", "direction": "LONG", "stop_loss": 99.0, "target": 120.0, "strategy_name": "alpha"},
        ]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5),
    )

    trades = result["trades"]
    daily = result["daily_pnl"]
    report_paths = result["report_paths"]
    summary = json.loads(report_paths["backtest_summary"].read_text())

    assert len(trades) == 2
    assert len(daily) == 2
    assert daily_pnl_report([]).empty
    assert daily["net_pnl"].sum() == trades["net_pnl"].sum()
    assert report_paths["trade_journal"].exists()
    assert report_paths["daily_pnl"].exists()
    assert report_paths["strategy_summary"].exists()
    assert report_paths["symbol_summary"].exists()
    assert report_paths["hourly_summary"].exists()
    assert report_paths["rejected_trades"].exists()
    assert report_paths["backtest_summary"].exists()
    assert summary["total_trades"] == 2
    assert "average_gross_pnl_per_trade" in summary
    assert "average_cost_per_trade" in summary
    assert "average_cost_bps" in summary
    assert "average_gross_edge_bps" in summary
    assert "total_turnover" in summary
    assert "brokerage_as_pct_of_total_cost" in summary
    assert "slippage_as_pct_of_total_cost" in summary
    assert "strategy_wise_performance" in summary
    assert "symbol_wise_performance" in summary
    assert "time_of_day_performance" in summary


def test_max_trades_per_symbol_per_day_blocks_extra_entries_and_logs_rejections(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.5, 100.0 if idx < 14 else 100.8, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
            {"timestamp": "2026-06-01 09:28:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
        ]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5, max_trades_per_symbol_per_day=1),
    )

    assert len(result["trades"]) == 1
    assert len(result["rejected_trades"]) == 1
    assert result["rejected_trades"].iloc[0]["reason"] == "max_trades_per_symbol_per_day_reached"


def test_cooldown_minutes_after_exit_blocks_reentry_and_logs_rejection(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.5, 100.0 if idx < 14 else 100.8, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
            {"timestamp": "2026-06-01 09:26:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
        ]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5, cooldown_minutes_after_exit=10),
    )

    assert len(result["trades"]) == 1
    assert result["rejected_trades"].iloc[0]["reason"] == "cooldown_minutes_after_exit"


def test_no_reentry_same_side_until_new_signal_blocks_same_side_reentry(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.5, 100.0 if idx < 14 else 100.8, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
            {"timestamp": "2026-06-01 09:28:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
        ]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(output_dir=str(tmp_path), time_exit_minutes=5, no_reentry_same_side_until_new_signal=True),
    )

    assert len(result["trades"]) == 1
    assert result["rejected_trades"].iloc[0]["reason"] == "no_reentry_same_side_until_new_signal"


def test_first_trade_time_and_last_entry_time_are_enforced(tmp_path) -> None:
    frame = make_full_session_frame(
        "2026-06-01",
        price_fn=lambda idx, _ts: (100.0, 101.0, 99.5, 100.0 if idx < 14 else 100.8, 1000.0),
    )
    prepared = MarketDataLoader().prepare(frame)
    strategy = StaticSignalStrategy(
        [
            {"timestamp": "2026-06-01 09:20:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
            {"timestamp": "2026-06-01 15:05:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
            {"timestamp": "2026-06-01 10:05:00", "direction": "LONG", "stop_loss": 98.0, "target": 120.0},
        ]
    )
    result = build_backtester(strategy).run(
        prepared,
        BacktestConfig(
            output_dir=str(tmp_path),
            time_exit_minutes=5,
            first_trade_time=pd.Timestamp("09:30").time(),
            last_entry_time=pd.Timestamp("15:00").time(),
        ),
    )

    assert len(result["trades"]) == 1
    reasons = set(result["rejected_trades"]["reason"].tolist())
    assert "before_first_trade_time" in reasons
    assert "after_last_entry_time" in reasons
