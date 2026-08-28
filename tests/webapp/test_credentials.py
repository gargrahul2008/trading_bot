"""Keeping the access token current.

The regression: all three agents ran for 34 hours against a token that had
expired overnight. fyers_auto_auth had rewritten fyers_auth.json that morning,
but the agent read it once at startup. Every section reported stale with
'Could not authenticate the user' and the agent went right on polling — because
it catches broker errors to survive a bad poll, it never crashed into the
restart that saves the bots from the same thing.
"""
import json
import os
import time

import pytest

from common.broker.interfaces import BrokerError
from webapp.agent.credentials import CredentialSource, is_auth_error
from webapp.agent.gateway import FyersGateway


class FakeClient:
    def __init__(self, token):
        self.token = token
        self.calls = 0

    def positions(self):
        self.calls += 1
        if self.token == "expired":
            raise BrokerError(
                "Positions error: {'code': -16, 'message': 'Could not authenticate the user'}",
                resp={"code": -16, "message": "Could not authenticate the user", "s": "error"},
            )
        return []

    def orderbook(self):
        raise BrokerError("Orderbook error: rate limited", resp={"code": -429})


@pytest.fixture
def auth_file(tmp_path):
    path = tmp_path / "fyers_auth.json"
    path.write_text(json.dumps({"users": {"user1": {"access_token": "expired"}}}))
    return path


def source(auth_file, tokens):
    """`tokens` is a list the fixture pops from, standing in for the file being
    rewritten by the daily refresh."""
    built = []

    def build(client_id, token):
        client = FakeClient(token)
        built.append(client)
        return client

    def read(_path, _user_key):
        return "APP-ID", tokens[-1]

    return CredentialSource(str(auth_file), "user1", build=build, read_creds=read), built


def test_recognises_the_fyers_auth_failure():
    assert is_auth_error(BrokerError("x", resp={"code": -16})) is True
    assert is_auth_error(
        BrokerError("Positions error: {'code': -16, 'message': 'Could not authenticate the user'}")
    ) is True
    assert is_auth_error(BrokerError("x", resp={"code": -429})) is False, "rate limit is not auth"


def test_a_rejected_token_is_reloaded_and_the_call_retried(auth_file):
    """The whole point: recovery without a restart and without a lost poll."""
    tokens = ["expired"]
    credentials, built = source(auth_file, tokens)
    gateway = FyersGateway(credentials)
    credentials.client()          # the agent started yesterday, on yesterday's token
    assert built[0].token == "expired"

    # Overnight, fyers_auto_auth rewrites the file with a working token.
    tokens.append("fresh")

    assert gateway.positions() == []
    assert len(built) == 2, "the client was rebuilt from the refreshed file"
    assert built[-1].token == "fresh"


def test_a_still_dead_token_surfaces_rather_than_looping(auth_file):
    """If the file has not been refreshed either, the error must reach the
    caller so /health can say the token needs attention."""
    credentials, built = source(auth_file, ["expired"])
    gateway = FyersGateway(credentials)

    with pytest.raises(BrokerError):
        gateway.positions()
    assert len(built) == 2, "reloaded once, then gave up rather than retrying forever"


def test_a_rate_limit_is_not_treated_as_an_auth_failure(auth_file):
    """Reloading the token on a -429 would spend a call for nothing and hide
    the backoff the budget depends on."""
    credentials, built = source(auth_file, ["fresh"])
    gateway = FyersGateway(credentials)

    with pytest.raises(BrokerError):
        gateway.orders()
    assert len(built) == 1, "no reload for a rate limit"


def test_the_client_is_rebuilt_when_the_file_is_rewritten(auth_file):
    """The ordinary path: the daily refresh rewrites the file and the agent
    picks it up on its next poll, without ever seeing an error."""
    tokens = ["day-one"]
    credentials, built = source(auth_file, tokens)

    assert credentials.client().token == "day-one"
    assert credentials.client().token == "day-one", "unchanged file means no rebuild"
    assert len(built) == 1

    tokens.append("day-two")
    # Touch the file the way fyers_auto_auth's atomic write would.
    later = time.time() + 10
    os.utime(auth_file, (later, later))

    assert credentials.client().token == "day-two"
    assert len(built) == 2


def test_status_reports_reloads_without_leaking_the_token(auth_file):
    credentials, _ = source(auth_file, ["supersecrettoken"])
    credentials.client()
    status = credentials.status()

    assert status["reloads"] == 1
    assert status["loaded"] is True
    assert "supersecrettoken" not in json.dumps(status), "the token must never be reported"
    assert status["token_tail"] == "ttoken", "a tail is enough to see that it changed"
