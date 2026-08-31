"""Telling a bot's orders from a human's.

The case that drove this: a bot order cancelled without filling vanishes from
both of the bot's own records — `_clear_pro_oids` empties the live list and
trades.jsonl is only written on a fill. After the EOD cancel every unfilled bot
order of the day read as manual.
"""
import json

import pytest

from webapp.agent.attribution import BY_CHANNEL, BY_ORDER_ID, BY_SYMBOL, Attribution

RELIANCE_CONFIG = {
    "strategy": {"symbols": ["NSE:RELIANCE-EQ"]},
    "execution": {"product_type": "MTF"},
}


@pytest.fixture
def account(tmp_path):
    """accounts/rahul/reliance/ with a config and an empty state dir."""
    run = tmp_path / "rahul" / "reliance"
    (run / "state").mkdir(parents=True)
    (run / "config.json").write_text(json.dumps(RELIANCE_CONFIG))
    return tmp_path / "rahul", run


def build(user_dir, tmp_path):
    return Attribution(str(user_dir), claims_path=str(tmp_path / "claims.json"))


def test_a_working_order_is_claimed_by_order_id(account, tmp_path):
    user_dir, run = account
    (run / "state" / "state.json").write_text(
        json.dumps({"extras": {"pro_buy_oids_NSE:RELIANCE-EQ": ["ORDER-1"]}})
    )
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    label = attribution.label({"order_id": "ORDER-1", "symbol": "NSE:RELIANCE-EQ"})
    assert label == {"source": "bot", "run": "rahul/reliance", "matched_by": BY_ORDER_ID}


def test_a_claim_survives_the_bot_cancelling_the_order(account, tmp_path):
    """The regression this file exists for."""
    user_dir, run = account
    state = run / "state" / "state.json"
    state.write_text(json.dumps({"extras": {"pro_buy_oids_NSE:RELIANCE-EQ": ["ORDER-1"]}}))

    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    # EOD cancel: the bot clears its own list, and the order never filled, so it
    # is written to neither state.json nor trades.jsonl.
    state.write_text(json.dumps({"extras": {"pro_buy_oids_NSE:RELIANCE-EQ": []}}))
    attribution.refresh(force=True)

    label = attribution.label({"order_id": "ORDER-1", "symbol": "NSE:RELIANCE-EQ"})
    assert label["source"] == "bot", "a cancelled bot order must not read as manual"
    assert label["matched_by"] == BY_ORDER_ID


def test_claims_survive_a_restart(account, tmp_path):
    user_dir, run = account
    (run / "state" / "state.json").write_text(
        json.dumps({"extras": {"pro_buy_oids_NSE:RELIANCE-EQ": ["ORDER-1"]}})
    )
    build(user_dir, tmp_path).refresh(force=True)

    # A fresh process, and by now the bot has cleared the order.
    (run / "state" / "state.json").write_text(json.dumps({"extras": {}}))
    restarted = build(user_dir, tmp_path)
    restarted.refresh(force=True)
    assert restarted.owner("ORDER-1") == "rahul/reliance"


def test_an_unclaimed_order_falls_back_to_the_runs_configured_symbol(account, tmp_path):
    """No claim exists — an agent that was not running when the order was placed
    still recognises the ladder by what the run is configured to trade."""
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    label = attribution.label(
        {"order_id": "26082600000580", "symbol": "NSE:RELIANCE-EQ", "product_type": "MTF"}
    )
    assert label == {"source": "bot", "run": "rahul/reliance", "matched_by": BY_SYMBOL}


def test_the_same_symbol_on_a_different_product_is_not_the_bots(account, tmp_path):
    """The reliance ladder trades MTF. Buying RELIANCE as CNC by hand is a
    manual trade, and calling it the bot's would put a warning on the wrong
    orders and hide it from the right ones."""
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    label = attribution.label(
        {"order_id": "X", "symbol": "NSE:RELIANCE-EQ", "product_type": "CNC"}
    )
    assert label["source"] == "manual"


def test_a_symbol_no_run_trades_is_manual(account, tmp_path):
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)
    assert attribution.classify({"order_id": "X", "symbol": "NSE:SBIN-EQ"}) == "manual"


def test_an_ambiguous_symbol_is_not_guessed(tmp_path):
    """Two runs on the same symbol and product cannot be told apart, so neither
    is claimed — a wrong run name is worse than none."""
    for name in ("one", "two"):
        run = tmp_path / "rahul" / name
        (run / "state").mkdir(parents=True)
        (run / "config.json").write_text(json.dumps(RELIANCE_CONFIG))
    attribution = build(tmp_path / "rahul", tmp_path)
    attribution.refresh(force=True)
    assert attribution.run_for_symbol("NSE:RELIANCE-EQ", "MTF") is None


def test_a_brand_new_unclaimed_order_is_pending_not_manual(account, tmp_path):
    """A bot writes its state just after placing, so an order seen in between
    must not flicker as manual."""
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    fresh = {"order_id": "NEW", "symbol": "NSE:SBIN-EQ"}
    assert attribution.classify(fresh, order_age_s=3.0) == "pending"
    assert attribution.classify(fresh, order_age_s=600.0) == "manual"
    assert attribution.classify(fresh, order_age_s=None) == "manual"


def test_runs_lists_configured_runs_even_before_they_trade(account, tmp_path):
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)
    assert attribution.runs() == ["rahul/reliance"]


def test_a_filled_order_is_claimed_from_the_trade_log(account, tmp_path):
    user_dir, run = account
    (run / "state" / "trades.jsonl").write_text(
        json.dumps({"order_id": "FILLED-1", "symbol": "NSE:RELIANCE-EQ"}) + "\n"
    )
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)
    assert attribution.owner("FILLED-1") == "rahul/reliance"


def test_an_unreadable_claims_file_does_not_break_attribution(account, tmp_path):
    user_dir, _ = account
    claims = tmp_path / "claims.json"
    claims.write_text("{not json")
    attribution = Attribution(str(user_dir), claims_path=str(claims))
    attribution.refresh(force=True)
    assert attribution.classify({"order_id": "X", "symbol": "NSE:RELIANCE-EQ",
                                 "product_type": "MTF"}) == "bot"


def test_a_web_terminal_order_is_manual_whatever_symbol_it_is_on(account, tmp_path):
    """Fyers stamps the channel: our bots come back API, orders placed by hand
    in the web terminal come back W/W1. That is broker truth about who placed an
    order, and it outranks any inference from our own files.

    This is the case the symbol inference used to get wrong: a human buying
    RELIANCE MTF by hand was indistinguishable from the ladder.
    """
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    by_hand = {"order_id": "W-1", "symbol": "NSE:RELIANCE-EQ",
               "product_type": "MTF", "channel": "web"}
    label = attribution.label(by_hand)
    assert label["source"] == "manual"
    assert label["matched_by"] == BY_CHANNEL
    assert label["run"] is None


def test_an_api_order_on_a_runs_symbol_is_still_the_bots(account, tmp_path):
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    label = attribution.label({"order_id": "A-1", "symbol": "NSE:RELIANCE-EQ",
                               "product_type": "MTF", "channel": "api"})
    assert label["source"] == "bot"
    assert label["run"] == "rahul/reliance"
    assert label["matched_by"] == BY_SYMBOL


def test_a_claimed_order_stays_the_bots_even_if_the_channel_is_unknown(account, tmp_path):
    """An order id the bot recorded is certain; a missing channel must not
    downgrade it."""
    user_dir, run = account
    (run / "state" / "state.json").write_text(
        json.dumps({"extras": {"pro_buy_oids_NSE:RELIANCE-EQ": ["ORDER-1"]}})
    )
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    label = attribution.label({"order_id": "ORDER-1", "symbol": "NSE:RELIANCE-EQ",
                               "channel": ""})
    assert label["source"] == "bot"
    assert label["matched_by"] == BY_ORDER_ID


def test_an_api_order_on_no_run_symbol_is_manual(account, tmp_path):
    """Once the dashboard can place orders it will also be stamped API, so the
    channel alone never means 'bot'."""
    user_dir, _ = account
    attribution = build(user_dir, tmp_path)
    attribution.refresh(force=True)

    label = attribution.label({"order_id": "A-2", "symbol": "NSE:SBIN-EQ",
                               "product_type": "CNC", "channel": "api"},
                              order_age_s=600)
    assert label["source"] == "manual"
