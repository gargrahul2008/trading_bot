"""One row per position, not one row per fill.

A hundred-share order fills in whatever pieces the exchange gives it — 1, then
16, then 18 — and FIFO matches each piece against whatever it closes. Both are
correct, and together they turn one trade into a dozen rows that have to be
added up by eye before they mean anything.
"""
from decimal import Decimal

from webapp.pnl.consolidate import closed, open_positions, recent


def match(qty, entry, exit_price, gross, charges="1", day="2026-09-02",
          symbol="NSE:RELIANCE-EQ", kind="intraday", direction="LONG",
          account="rahul", at="2026-09-02 10:00:00", product="MTF"):
    return {"account": account, "symbol": symbol, "closed_day": day, "kind": kind,
            "direction": direction, "qty": qty, "entry_price": entry,
            "exit_price": exit_price, "gross": gross, "charges": charges,
            "product_type": product, "closed_at": at}


def position(qty, avg, ltp, unrealised, product="CNC", opened=None,
             symbol="NSE:RELIANCE-EQ", account="rahul", **kw):
    row = {"account": account, "symbol": symbol, "net_qty": qty, "avg_price": avg,
           "ltp": ltp, "unrealised": unrealised, "product_type": product,
           "opened_day": opened}
    row.update(kw)
    return row


# ── closed ──────────────────────────────────────────────────────────────────

def test_a_split_fill_is_one_line():
    """The complaint exactly: one 100-share trade appearing as 1, 16 and 83."""
    rows = closed([match(1, 1300, 1310, 10), match(16, 1300, 1312, 192),
                   match(83, 1305, 1311, 498)])

    assert len(rows) == 1
    assert rows[0]["qty"] == "100"
    assert rows[0]["fills"] == 3


def test_the_in_price_is_what_was_actually_paid():
    """Not the mean of the fills. A 1-share fill and a 99-share fill do not
    average to the middle, and on a laddered entry it is not close."""
    rows = closed([match(1, 1000, 1100, 100, charges="0"),
                   match(99, 1300, 1400, 9900, charges="0")])

    assert rows[0]["entry_price"] == "1297.0000"
    assert rows[0]["exit_price"] == "1397.0000"


def test_the_pnl_is_the_sum_and_the_percentage_is_on_the_whole():
    rows = closed([match(1, 1300, 1310, 10, charges="1"),
                   match(16, 1300, 1312, 192, charges="4"),
                   match(83, 1305, 1311, 498, charges="20")])

    assert rows[0]["gross"] == "700"
    assert rows[0]["charges"] == "25"
    assert rows[0]["net"] == "675"
    assert rows[0]["pct"] == "0.52", "675 on 130,415 committed"


def test_intraday_and_positional_stay_apart():
    """They are taxed differently and cost differently. A day's Reliance
    trading that was partly each is two facts, not one."""
    rows = closed([match(50, 1300, 1310, 500, kind="intraday"),
                   match(50, 1200, 1310, 5500, kind="positional")])

    assert len(rows) == 2
    assert {r["trade_kind"] for r in rows} == {"intraday", "positional"}


def test_two_different_days_stay_apart():
    rows = closed([match(50, 1300, 1310, 500, day="2026-09-01"),
                   match(50, 1300, 1315, 750, day="2026-09-02")])

    assert len(rows) == 2


def test_a_long_and_a_short_close_are_not_added_together():
    rows = closed([match(50, 1300, 1310, 500, direction="LONG"),
                   match(50, 1310, 1300, 500, direction="SHORT")])

    assert len(rows) == 2


def test_an_unknown_charge_leaves_the_net_unknown():
    """An unknown cost is not a zero one. Summing the known part and calling it
    net would understate what the trade cost, confidently."""
    rows = closed([match(50, 1300, 1310, 500, charges="12"),
                   match(50, 1300, 1312, 600, charges=None)])

    assert rows[0]["gross"] == "1100"
    assert rows[0]["net"] is None
    assert rows[0]["charges"] is None


# ── open ────────────────────────────────────────────────────────────────────

def test_holdings_and_a_fresh_buy_are_one_position():
    """The broker keeps settled stock in holdings and today's buy in positions.
    They are the same money and the same scrip."""
    rows = open_positions([
        position(900, 1250, 1310, 54000, product="CNC", opened="2026-04-10"),
        position(100, 1300, 1310, 1000, product="MTF", opened="2026-08-20"),
    ])

    assert len(rows) == 1
    assert rows[0]["qty"] == "1000"
    assert rows[0]["entry_price"] == "1255.0000", "the blended cost, not either one"
    assert rows[0]["gross"] == "55000"
    assert rows[0]["product_type"] == "CNC+MTF"


def test_a_merged_position_is_dated_from_the_older_half():
    """Buying more of something held since April does not make it a new trade."""
    rows = open_positions([
        position(900, 1250, 1310, 54000, product="CNC", opened="2026-04-10"),
        position(100, 1300, 1310, 1000, product="MTF", opened="2026-08-20"),
    ])

    assert rows[0]["day"] == "2026-04-10"


def test_a_hedge_is_not_netted_away():
    """Long 900 in delivery and short 100 intraday is a hedge, not 800 long.
    Netting it would report less risk than is actually held."""
    rows = open_positions([
        position(900, 1250, 1310, 54000, product="CNC"),
        position(-100, 1305, 1310, -500, product="INTRADAY"),
    ])

    assert len(rows) == 2
    assert {r["direction"] for r in rows} == {"LONG", "SHORT"}


def test_a_delivery_sale_is_not_an_open_position():
    """Stock sold out of holdings carries no open risk, and the broker's
    unrealised on it is the mark of a short that does not exist."""
    rows = open_positions([position(-1000, 223.91, 220.78, 3134.5,
                                    product="CNC", delivery_sale=True)])

    assert rows == []


def test_a_flat_row_is_not_an_open_position():
    assert open_positions([position(0, 1300, 1310, 0)]) == []


def test_accounts_are_never_merged():
    rows = open_positions([
        position(100, 1300, 1310, 1000, account="rahul"),
        position(100, 1290, 1310, 2000, account="pratibha"),
    ])

    assert len(rows) == 2


# ── the list ────────────────────────────────────────────────────────────────

def test_every_open_position_survives_the_cap():
    """They are what is held. Dropping one to make room for history would hide
    live money."""
    opens = [position(100, 1300, 1310, 1000, symbol="NSE:S%d-EQ" % i)
             for i in range(30)]
    closes = [match(10, 1300, 1310, 100, symbol="NSE:C%d-EQ" % i) for i in range(30)]

    lines = recent(opens, closes, closed_limit=5)

    assert sum(1 for line in lines if line["state"] == "open") == 30
    assert sum(1 for line in lines if line["state"] == "closed") == 5


def test_the_order_is_by_time_and_is_stable():
    """Sorting by P&L moved rows on every tick, which is how a list becomes
    unreadable while it is being read."""
    closes = [match(10, 1300, 1310, 100, day="2026-09-01", at="2026-09-01 10:00:00",
                    symbol="NSE:A-EQ"),
              match(10, 1300, 1320, 200, day="2026-09-02", at="2026-09-02 09:30:00",
                    symbol="NSE:B-EQ")]

    lines = recent([], closes)
    assert [line["symbol"] for line in lines] == ["NSE:B-EQ", "NSE:A-EQ"]
    assert recent([], list(reversed(closes))) == lines, "input order must not matter"
