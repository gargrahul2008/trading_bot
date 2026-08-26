"""Per-account Fyers agent: the only process that talks to a broker on behalf
of the dashboard.

One process per Fyers user, started under that user's account.env so its REST
calls egress through the account's whitelisted static IP. See
docs/multi_account_architecture.md for why the IP binding forces this shape.
"""
