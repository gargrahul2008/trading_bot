"""Turning fills into P&L.

`matcher.py` is pure: fills in, closed round trips out. No database, no broker,
no clock — so the arithmetic that decides what a trade earned can be read and
tested on its own.
"""
