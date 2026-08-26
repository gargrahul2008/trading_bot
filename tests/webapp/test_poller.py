"""The polling schedule: cadence, what gets dropped under pressure, and the
fill-triggered tradebook pull."""
from common.broker.interfaces import BrokerError

from webapp.agent.book import Book
from webapp.agent.budget import Budget
from webapp.agent.poller import Poller

from tests.webapp.fakes import FakeClock, FakeGateway, FixedSession

LIVE = {"positions": 3.0, "orders": 3.0, "funds": 30.0, "holdings": 60.0}


def build(intervals=None, per_min=600.0, burst=50):
    clock = FakeClock()
    gateway = FakeGateway()
    book = Book("rahul")
    poller = Poller(
        gateway,
        book,
        budget=Budget(per_min=per_min, burst=burst, clock=clock),
        session=FixedSession(intervals or LIVE),
        clock=clock,
    )
    return clock, gateway, book, poller


def test_each_section_polls_on_its_own_interval():
    clock, gateway, _, poller = build()

    poller.tick()  # everything is due at start
    assert gateway.calls == {"positions": 1, "orders": 1, "trades": 1, "funds": 1, "holdings": 1}

    clock.advance(3.0)
    poller.tick()
    assert gateway.calls["positions"] == 2
    assert gateway.calls["funds"] == 1, "funds is on a 30s interval, not 3s"
    assert gateway.calls["holdings"] == 1

    clock.advance(30.0)
    poller.tick()
    assert gateway.calls["funds"] == 2


def test_live_cadence_stays_inside_the_budget():
    """~45 calls/min is what has to fit alongside four bot runs on one app."""
    clock, gateway, _, poller = build()
    for _ in range(120):  # one minute at the 0.5s tick
        poller.tick()
        clock.advance(0.5)

    total = sum(gateway.calls.values())
    assert total <= 60, "agent must stay well under the app's per-minute limit, got %d" % total
    assert gateway.calls["positions"] >= 19, "positions must still be near-live"


def test_a_squeezed_budget_sheds_holdings_before_orders():
    # Only enough allowance for the two sections that matter.
    clock, gateway, _, poller = build(per_min=6.0, burst=2)
    for _ in range(20):
        poller.tick()
        clock.advance(0.5)

    assert gateway.calls.get("positions", 0) >= 1
    assert gateway.calls.get("orders", 0) >= 1
    assert gateway.calls.get("holdings", 0) == 0, "lowest priority must be the first dropped"


def test_a_refused_section_is_retried_next_tick_not_after_a_full_interval():
    clock, gateway, _, poller = build(per_min=60.0, burst=1)
    poller.tick()                       # spends the single token on positions
    assert gateway.calls == {"positions": 1}

    clock.advance(1.0)                  # one token back
    poller.tick()
    assert gateway.calls["orders"] == 1, "orders was still due, so it goes next"
    assert gateway.calls["positions"] == 1, "positions is not due again yet"


def test_a_fill_pulls_the_tradebook_in_the_same_tick():
    clock, gateway, _, poller = build()
    gateway.order_rows = [{"order_id": "1", "filled_qty": 0.0}]
    poller.tick()
    assert gateway.calls["trades"] == 1

    clock.advance(3.0)
    gateway.order_rows = [{"order_id": "1", "filled_qty": 70.0}]
    poller.tick()
    assert gateway.calls["trades"] == 2, "a new fill must pull the exact traded price at once"

    clock.advance(3.0)
    poller.tick()
    assert gateway.calls["trades"] == 2, "an unchanged fill must not pull it again"


def test_a_partial_fill_that_grows_counts_as_a_new_fill():
    clock, gateway, _, poller = build()
    gateway.order_rows = [{"order_id": "1", "filled_qty": 10.0}]
    poller.tick()
    before = gateway.calls["trades"]

    clock.advance(3.0)
    gateway.order_rows = [{"order_id": "1", "filled_qty": 25.0}]
    poller.tick()
    assert gateway.calls["trades"] == before + 1


def test_a_broker_error_ages_the_section_but_keeps_the_last_good_data():
    clock, gateway, book, poller = build()
    poller.tick()
    assert book.get("positions")["data"]

    gateway.fail_with["positions"] = BrokerError("Positions error: timeout")
    clock.advance(3.0)
    poller.tick()

    section = book.get("positions")
    assert section["data"], "a failed refresh must not blank the screen"
    assert "timeout" in section["error"]
    # The section keeps its original fetch time, so it goes on ageing towards
    # stale rather than looking as if the failed poll had refreshed it. Book
    # stamps wall-clock time (the UI shows it), which the fake clock does not
    # move, so age is asserted by pushing the timestamp back directly.
    section_obj = book.section("positions")
    section_obj.fetched_at -= 12.0
    assert book.get("positions")["stale"] is True


def test_a_rate_limit_backs_the_agent_off():
    clock, gateway, _, poller = build()
    gateway.fail_with["positions"] = BrokerError("boom", resp={"code": -429})
    poller.tick()

    assert poller.budget.snapshot()["rate_limited"] == 1
    assert poller.budget.cooling_down() is True

    calls_before = sum(gateway.calls.values())
    clock.advance(1.0)
    poller.tick()
    assert sum(gateway.calls.values()) == calls_before, "no calls at all while cooling down"


def test_a_stale_section_is_reported_as_stale():
    clock, _, book, poller = build()
    poller.tick()
    assert book.health()["live"] is True

    # Nothing polls for a while (say the process was suspended).
    import time as _time

    book.section("positions").fetched_at = _time.time() - 60
    assert book.health()["live"] is False


def test_session_uses_market_time_even_without_tzdata(monkeypatch):
    """The control host runs UTC. If the session ever fell back to the host
    clock, 11:00 IST would read as 05:30 and the agent would sit at closed
    cadence through the whole trading day."""
    import datetime as dt

    from webapp.agent import session as session_mod

    monkeypatch.setattr(session_mod, "ZoneInfo", None)
    fallback = session_mod.Session(holidays=set())

    assert fallback.now().utcoffset() == dt.timedelta(hours=5, minutes=30)

    # A Wednesday, 11:00 IST == 05:30 UTC.
    ist_1100 = dt.datetime(2026, 8, 26, 11, 0, tzinfo=session_mod.IST)
    assert fallback.phase(ist_1100) == "live"
    assert fallback.intervals(ist_1100)["positions"] == 3.0

    # And the same instant expressed in UTC must not be read as 05:30 local.
    assert ist_1100.astimezone(dt.timezone.utc).hour == 5


def test_staleness_scales_with_the_session_cadence():
    """A fixed tolerance marked every section stale the moment the market shut:
    the closed-market interval is 60s but the tolerance was 10s, so all three
    live agents reported live=False overnight."""
    for interval in (3.0, 60.0, 900.0):
        clock = FakeClock()
        gateway = FakeGateway()
        book = Book("rahul")
        poller = Poller(
            gateway, book,
            budget=Budget(per_min=600.0, burst=50, clock=clock),
            session=FixedSession({"positions": interval, "orders": interval,
                                  "funds": interval, "holdings": interval}),
            clock=clock,
        )
        poller.tick()
        section = book.get("positions")
        assert section["stale_after_s"] >= interval, (
            "tolerance must cover at least one poll interval, got %s for %s"
            % (section["stale_after_s"], interval)
        )
        assert book.health()["live"] is True, "idle is not the same as broken"


def test_a_genuinely_missed_poll_is_still_reported_stale():
    """Scaling the tolerance must not make staleness unreachable."""
    import time as _time

    clock = FakeClock()
    book = Book("rahul")
    poller = Poller(
        FakeGateway(), book,
        budget=Budget(per_min=600.0, burst=50, clock=clock),
        session=FixedSession(LIVE), clock=clock,
    )
    poller.tick()
    assert book.health()["live"] is True

    # Three intervals pass with nothing refreshed.
    book.section("positions").fetched_at = _time.time() - 30
    assert book.health()["live"] is False
