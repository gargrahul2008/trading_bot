"""Fetching a Fyers account's history: capital, realised P&L and charges.

Direct REST rather than the SDK. The control host runs fyers-apiv3 3.1.10, which
has none of these three methods — and that is the library the live bots trade
through, so upgrading it for a reporting feature is the wrong risk. These calls
use the SDK's own base URL and header format and touch nothing the bots use.
"""
