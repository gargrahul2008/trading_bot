"""Reshaping Fyers payloads. Every fixture here is a real response from a live
account, trimmed but not otherwise altered."""
from webapp.agent.gateway import (
    normalise_holding,
    normalise_order,
    normalise_position,
    summarise_funds,
)

# A short future opened today.
CROMPTON = {
    "symbol": "NSE:CROMPTON26SEPFUT", "id": "NSE:CROMPTON26SEPFUT-MARGIN",
    "buyAvg": 0, "buyQty": 0, "buyVal": 0,
    "sellAvg": 242.65, "sellQty": 2150, "sellVal": 521697.5,
    "netAvg": 242.65, "netQty": -2150, "side": -1, "qty": 2150,
    "productType": "MARGIN", "realized_profit": 0, "exchange": 10, "segment": 11,
    "dayBuyQty": 0, "daySellQty": 2150, "cfBuyQty": 0, "cfSellQty": 0,
    "pl": 5052.499999999988, "unrealized_profit": 5052.499999999988, "ltp": 240.3,
}

# A short future carried in overnight and bought back today.
TATAELXSI = {
    "symbol": "NSE:TATAELXSI26SEPFUT", "id": "NSE:TATAELXSI26SEPFUT-MARGIN",
    "buyAvg": 3706, "buyQty": 125, "buyVal": 463250,
    "sellAvg": 3631.8, "sellQty": 125, "sellVal": 453975,
    "netAvg": 0, "netQty": 0, "side": 0, "qty": 0,
    "productType": "MARGIN", "realized_profit": -9274.999999999978,
    "exchange": 10, "segment": 11,
    "dayBuyQty": 125, "daySellQty": 0, "cfBuyQty": 0, "cfSellQty": 125,
    "pl": -9274.999999999978, "unrealized_profit": 0, "ltp": 3633,
}

# Holdings sold today: a negative CNC equity position, not a short.
SHRINGARMS = {
    "symbol": "NSE:SHRINGARMS-EQ", "id": "NSE:SHRINGARMS-EQ-CNC",
    "buyAvg": 0, "buyQty": 0, "sellAvg": 223.9145, "sellQty": 1000, "sellVal": 223914.5,
    "netAvg": 223.9145, "netQty": -1000, "side": -1, "qty": 1000,
    "productType": "CNC", "realized_profit": 0, "exchange": 10, "segment": 10,
    "dayBuyQty": 0, "daySellQty": 1000, "cfBuyQty": 0, "cfSellQty": 0,
    "pl": 3134.5000000000027, "unrealized_profit": 3134.5000000000027, "ltp": 220.78,
}


def test_a_short_position_reads_as_short():
    position = normalise_position(CROMPTON)
    assert position["direction"] == "SHORT"
    assert position["net_qty"] == -2150
    assert position["avg_price"] == 242.65
    assert position["ltp"] == 240.3
    assert position["unrealised"] == 5052.499999999988
    assert position["position_id"] == "NSE:CROMPTON26SEPFUT-MARGIN", "needed to exit it"


def test_a_carried_position_is_distinguished_from_one_opened_today():
    """product_type says what a position is allowed to be; cf/day quantities say
    what it actually is."""
    assert normalise_position(CROMPTON)["opened_today"] is True
    assert normalise_position(CROMPTON)["carried"] is False

    assert normalise_position(TATAELXSI)["carried"] is True
    assert normalise_position(TATAELXSI)["opened_today"] is False


def test_position_realised_is_the_life_of_the_trade_not_the_day():
    """The account-level figure in funds is today's mark-to-market from the
    previous close. For anything carried in they differ — this one showed
    -9,275 against -3,750 — so they must never share a column."""
    position = normalise_position(TATAELXSI)
    assert position["realised"] == -9274.999999999978
    assert position["unrealised"] == 0, "flat, so nothing is unrealised"
    assert position["direction"] == "FLAT"


def test_a_cnc_equity_sale_is_not_reported_as_a_short():
    """You cannot short on delivery. A negative CNC equity position is stock
    sold out of holdings and awaiting settlement; showing it as SHORT would read
    as an open risk that has to be bought back."""
    position = normalise_position(SHRINGARMS)
    assert position["delivery_sale"] is True
    assert position["is_derivative"] is False


def test_a_short_future_is_not_mistaken_for_a_delivery_sale():
    assert normalise_position(CROMPTON)["delivery_sale"] is False
    assert normalise_position(CROMPTON)["is_derivative"] is True


def test_a_sold_out_holding_is_flagged_closed():
    """Fyers keeps returning the row with qty 0 and the old cost price."""
    sold = normalise_holding({"symbol": "NSE:SHRINGARMS-EQ", "quantity": 0,
                              "costPrice": 230.57, "ltp": 220.78})
    assert sold["is_open"] is False
    held = normalise_holding({"symbol": "NSE:WABAG-EQ", "quantity": 100,
                              "costPrice": 1994.49, "ltp": 2155.30})
    assert held["is_open"] is True
    assert round(held["unrealised"], 2) == 16081.0


def test_funds_are_read_by_title():
    """Matching is on the title text, so the whole real list is exercised."""
    funds = summarise_funds({"s": "ok", "fund_limit": [
        {"id": 1, "title": "Total Balance", "equityAmount": 408588.04},
        {"id": 2, "title": "Utilized Amount", "equityAmount": -116645.8},
        {"id": 3, "title": "Clear Balance", "equityAmount": 183375.54},
        {"id": 4, "title": "Realized Profit and Loss", "equityAmount": -3750},
        {"id": 5, "title": "Collaterals", "equityAmount": 0},
        {"id": 7, "title": "Receivables", "equityAmount": -223910},
        {"id": 10, "title": "Available Balance", "equityAmount": 300021.34},
    ]})
    assert funds["available"] == 300021.34
    assert funds["total"] == 408588.04
    assert funds["realised_pnl"] == -3750
    assert funds["utilised"] == -116645.8, "negative is real: credit from the shorts"


def test_a_cancelled_bot_order_keeps_its_quantities():
    """The EOD-cancelled ladder orders that drove the attribution fix."""
    order = normalise_order({
        "id": "26082600000580", "symbol": "NSE:RELIANCE-EQ", "side": 1,
        "qty": 140, "filledQty": 0, "status": 1, "productType": "MTF",
        "limitPrice": 1290.5,
    })
    assert order["status"] == "CANCELLED"
    assert order["is_open"] is False
    assert order["side"] == "BUY"
    assert order["remaining_qty"] == 140
    assert order["kind"] == "positional"


def test_an_unknown_field_never_loses_the_payload():
    position = normalise_position({"symbol": "NSE:X-EQ", "netQty": 1, "surprise": 42})
    assert position["raw"]["surprise"] == 42
