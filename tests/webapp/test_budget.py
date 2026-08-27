"""The budget is what keeps the dashboard from starving the bots, so its
refusals matter as much as its permissions."""
from webapp.agent.budget import COOLDOWN_SECONDS, Budget, is_rate_limit
from common.broker.interfaces import BrokerError

from tests.webapp.fakes import FakeClock


def test_burst_then_refuses_until_refilled():
    clock = FakeClock()
    budget = Budget(per_min=60.0, burst=3, clock=clock)

    assert [budget.take() for _ in range(3)] == [True, True, True]
    assert budget.take() is False, "burst exhausted"

    clock.advance(1.0)  # 60/min == 1 per second
    assert budget.take() is True


def test_tokens_never_exceed_burst():
    clock = FakeClock()
    budget = Budget(per_min=600.0, burst=5, clock=clock)
    clock.advance(3600.0)
    assert sum(budget.take() for _ in range(50)) == 5


def test_rate_limit_stops_calls_then_halves_the_rate():
    clock = FakeClock()
    budget = Budget(per_min=60.0, burst=5, clock=clock)
    budget.penalise()

    assert budget.take() is False
    assert budget.cooling_down() is True

    clock.advance(COOLDOWN_SECONDS + 0.1)
    assert budget.cooling_down() is False
    # Still throttled: at half of 60/min a single token takes two seconds.
    assert budget.take() is False
    clock.advance(2.0)
    assert budget.take() is True


def test_snapshot_reports_the_throttle():
    clock = FakeClock()
    budget = Budget(per_min=60.0, clock=clock)
    budget.penalise()
    snap = budget.snapshot()
    assert snap["rate_limited"] == 1
    assert snap["cooling_down"] is True
    assert snap["effective_per_min"] == 30.0


def test_recognises_the_fyers_rate_limit_shapes():
    assert is_rate_limit(BrokerError("boom", resp={"code": -429, "s": "error"})) is True
    assert is_rate_limit(BrokerError("Orderbook error: {'code': -429}")) is True
    assert is_rate_limit(BrokerError("request limit reached, too many requests")) is True
    assert is_rate_limit(BrokerError("boom", resp={"code": -50, "s": "error"})) is False
    assert is_rate_limit(ValueError("unrelated")) is False
