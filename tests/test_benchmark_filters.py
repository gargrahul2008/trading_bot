from __future__ import annotations

import pandas as pd

from intraday_research.benchmark_filters import (
    BenchmarkAwareSignalFilter,
    BenchmarkFilterConfig,
)
from intraday_research.types import CombinedSignal, SignalSide


def build_featured_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2026-06-01 09:30:00+05:30"),
                "symbol": "RELIANCE",
                "trade_date": pd.Timestamp("2026-06-01").date(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0,
                "vwap": 100.0,
                "atr": 2.0,
                "ema_fast": 100.2,
                "ema_slow": 99.8,
                "trend_gap_atr": 0.2,
            }
        ]
    )
    return frame


def build_benchmark_frame(regime: str = "SHORT", benchmark_close: float = 99.5) -> pd.DataFrame:
    ema_fast, ema_slow = (99.5, 100.5) if regime == "SHORT" else (100.5, 99.5)
    if regime == "FLAT":
        ema_fast, ema_slow = 100.0, 100.0
    return pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2026-06-01 09:30:00+05:30"),
                "benchmark_regime": regime,
                "benchmark_close_change_pct": 0.1,
                "close": benchmark_close,
                "vwap": 100.0,
                "atr": 1.0,
                "ema_fast": ema_fast,
                "ema_slow": ema_slow,
            }
        ]
    )


def build_signal(score: float = 72.0) -> CombinedSignal:
    return CombinedSignal(
        timestamp=pd.Timestamp("2026-06-01 09:30:00+05:30"),
        symbol="RELIANCE",
        final_decision=SignalSide.LONG,
        final_score=score,
        price=100.5,
        stop_price=99.5,
        target_price=102.0,
        explanation="test",
        strategy_name="trend_pullback",
    )


def test_benchmark_filter_blocks_against_benchmark_signal() -> None:
    signal_filter = BenchmarkAwareSignalFilter(
        build_benchmark_frame(regime="SHORT"),
        BenchmarkFilterConfig(
            use_benchmark_filter=True,
            require_benchmark_alignment=True,
            allow_flat_benchmark=False,
        ),
    )

    accepted, rejected = signal_filter([build_signal()], build_featured_frame())

    assert accepted == []
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "against_benchmark_blocked"
    assert rejected[0]["benchmark_alignment_bucket"] == "against_benchmark"


def test_benchmark_filter_allows_high_score_override_against_benchmark() -> None:
    signal_filter = BenchmarkAwareSignalFilter(
        build_benchmark_frame(regime="SHORT"),
        BenchmarkFilterConfig(
            use_benchmark_filter=True,
            require_benchmark_alignment=True,
            allow_against_benchmark_above_score=70.0,
        ),
    )

    accepted, rejected = signal_filter([build_signal(score=75.0)], build_featured_frame())

    assert len(accepted) == 1
    assert rejected == []


def test_benchmark_filter_blocks_symbol_overextension() -> None:
    featured = build_featured_frame()
    featured.loc[:, "close"] = 104.0
    featured.loc[:, "vwap"] = 100.0
    featured.loc[:, "atr"] = 1.0
    featured.loc[:, "trend_gap_atr"] = 1.8

    signal_filter = BenchmarkAwareSignalFilter(
        build_benchmark_frame(regime="LONG", benchmark_close=100.6),
        BenchmarkFilterConfig(
            symbol_vwap_gap_max_atr=2.5,
            symbol_trend_gap_max_atr=1.0,
        ),
    )

    accepted, rejected = signal_filter([build_signal(score=80.0)], featured)

    assert accepted == []
    assert len(rejected) == 1
    assert rejected[0]["reason"] == "symbol_vwap_gap_above_max_atr:2.500"
