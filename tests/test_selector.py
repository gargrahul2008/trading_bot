from __future__ import annotations

import pandas as pd

from intraday_research.selector import SignalSelector
from intraday_research.types import CombinedSignal, SignalSide


def make_signal(*, score: float = 2.0, stop_price: float = 98.0, target_price: float = 104.0) -> CombinedSignal:
    return CombinedSignal(
        timestamp=pd.Timestamp("2026-06-01 10:00:00", tz="Asia/Kolkata"),
        symbol="NSE:RELIANCE-EQ",
        final_decision=SignalSide.LONG,
        final_score=score,
        price=100.0,
        stop_price=stop_price,
        target_price=target_price,
        explanation="test",
        strategy_name="trend_pullback",
    )


def test_signal_selector_filters_below_minimum_score() -> None:
    decision = SignalSelector().select_signal(
        make_signal(score=1.5),
        minimum_signal_score=2.0,
        minimum_reward_to_risk=0.0,
    )

    assert not decision.approved
    assert decision.reason == "below_minimum_signal_score:2.000"


def test_signal_selector_filters_below_minimum_reward_to_risk() -> None:
    decision = SignalSelector().select_signal(
        make_signal(score=3.0, stop_price=99.0, target_price=100.5),
        minimum_signal_score=0.0,
        minimum_reward_to_risk=1.0,
    )

    assert not decision.approved
    assert decision.reason == "below_minimum_reward_to_risk:1.000"
    assert decision.reward_to_risk == 0.5


def test_signal_selector_supports_strategy_specific_score_threshold() -> None:
    decision = SignalSelector().select_signal(
        make_signal(score=2.5),
        minimum_signal_score=0.0,
        minimum_reward_to_risk=0.0,
        minimum_signal_score_by_strategy={"trend_pullback": 3.0},
    )

    assert not decision.approved
    assert decision.reason == "below_minimum_signal_score:3.000"
