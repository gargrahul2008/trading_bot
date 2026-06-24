from __future__ import annotations

import pandas as pd

from intraday_research.combiner import SignalCombiner
from intraday_research.types import SignalSide


def test_signal_combiner_flattens_equal_conflict() -> None:
    timestamp = pd.Timestamp("2026-06-02 10:00:00", tz="Asia/Kolkata")
    signals = pd.DataFrame(
        [
            [timestamp, "NIFTY", "LONG", 1.0, "long_setup", 99.0, 102.0, "orb"],
            [timestamp, "NIFTY", "SHORT", 1.0, "short_setup", 101.0, 98.0, "vwap"],
        ],
        columns=["timestamp", "symbol", "direction", "strength", "reason", "stop_loss", "target", "strategy_name"],
    )

    combined = SignalCombiner().combine(signals)

    assert len(combined) == 1
    assert combined[0].final_decision is SignalSide.FLAT
    assert combined[0].final_score == 0.0
    assert combined[0].stop_price is None
    assert combined[0].target_price is None
    assert combined[0].strategy_name == "orb+vwap"
    assert "netted 2 signals" in combined[0].explanation
    assert "to FLAT" in combined[0].explanation


def test_signal_combiner_nets_to_long_when_long_score_is_stronger() -> None:
    timestamp = pd.Timestamp("2026-06-02 10:01:00", tz="Asia/Kolkata")
    signals = pd.DataFrame(
        [
            [timestamp, "BANKNIFTY", "LONG", 1.5, "breakout", 52000.0, 52400.0, "orb"],
            [timestamp, "BANKNIFTY", "SHORT", 0.5, "fade", 52150.0, 51800.0, "vwap"],
        ],
        columns=["timestamp", "symbol", "direction", "strength", "reason", "stop_loss", "target", "strategy_name"],
    )

    combined = SignalCombiner().combine(signals)

    assert len(combined) == 1
    assert combined[0].final_decision is SignalSide.LONG
    assert combined[0].final_score == 1.0
    assert combined[0].stop_price == 52000.0
    assert combined[0].target_price == 52400.0
    assert "long=1, short=1" in combined[0].explanation
    assert "to LONG" in combined[0].explanation


def test_signal_combiner_nets_to_short_when_short_score_is_stronger() -> None:
    timestamp = pd.Timestamp("2026-06-02 10:02:00", tz="Asia/Kolkata")
    signals = pd.DataFrame(
        [
            [timestamp, "FINNIFTY", "LONG", 0.75, "bounce", 24800.0, 25000.0, "vwap"],
            [timestamp, "FINNIFTY", "SHORT", 1.25, "breakdown", 24920.0, 24600.0, "orb"],
        ],
        columns=["timestamp", "symbol", "direction", "strength", "reason", "stop_loss", "target", "strategy_name"],
    )

    combined = SignalCombiner().combine(signals)

    assert len(combined) == 1
    assert combined[0].final_decision is SignalSide.SHORT
    assert combined[0].final_score == -0.5
    assert combined[0].stop_price == 24920.0
    assert combined[0].target_price == 24600.0
    assert "to SHORT" in combined[0].explanation


def test_signal_combiner_uses_direction_not_strength_sign() -> None:
    timestamp = pd.Timestamp("2026-06-02 10:03:00", tz="Asia/Kolkata")
    signals = pd.DataFrame(
        [
            [timestamp, "NIFTY", "LONG", -0.75, "bounce", 24800.0, 25000.0, "vwap"],
        ],
        columns=["timestamp", "symbol", "direction", "strength", "reason", "stop_loss", "target", "strategy_name"],
    )

    combined = SignalCombiner().combine(signals)

    assert len(combined) == 1
    assert combined[0].final_decision is SignalSide.LONG
    assert combined[0].final_score == 0.75
    assert combined[0].stop_price == 24800.0
    assert combined[0].target_price == 25000.0
