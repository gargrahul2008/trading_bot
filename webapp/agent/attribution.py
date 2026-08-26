"""Which orders belong to a bot, and which a human placed.

The dashboard shows every order in the account, whatever placed it. To label
them we read what the bots themselves recorded under
accounts/<user>/<run>/state/ — no broker call, just files on the same host.

Live claims come from state.json (the order ids a run currently has working);
historical ones from trades.jsonl (every id a run ever filled). An id claimed by
neither is manual.

The race this has to survive: a bot places an order and writes its state a
moment later, so a poll landing in between sees an unclaimed id that is not
actually manual. Anything younger than GRACE_SECONDS is therefore reported as
"pending" rather than "manual", and settles on a later poll. Without it every
bot order would flicker as manual for one tick.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Set

GRACE_SECONDS = 15.0

BOT = "bot"
MANUAL = "manual"
PENDING = "pending"

# Re-reading every run's state on each 3-second poll is wasteful; the files only
# change when a bot acts.
_REFRESH_SECONDS = 5.0


def _iter_run_dirs(user_dir: str) -> List[str]:
    """One directory per strategy run: accounts/<user>/<run>/."""
    try:
        entries = sorted(os.listdir(user_dir))
    except OSError:
        return []
    out = []
    for name in entries:
        path = os.path.join(user_dir, name)
        if os.path.isdir(path) and os.path.exists(os.path.join(path, "config.json")):
            out.append(path)
    return out


def _live_oids_from_state(state_path: str) -> Set[str]:
    """Order ids a run currently has working.

    Two places hold them: `pending_order_id` on each symbol (reactive runs) and
    the `pro_{buy,sell}_oids_<symbol>` lists in extras (proactive ladders). See
    generic_runner.py:1400 for how the latter are maintained.
    """
    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError):
        return set()
    if not isinstance(raw, dict):
        return set()

    oids: Set[str] = set()
    for sym_state in (raw.get("symbol_states") or {}).values():
        if isinstance(sym_state, dict):
            oid = sym_state.get("pending_order_id")
            if oid:
                oids.add(str(oid))

    extras = raw.get("extras") or {}
    if isinstance(extras, dict):
        for key, value in extras.items():
            if not (key.startswith("pro_buy_oids_") or key.startswith("pro_sell_oids_")):
                continue
            if isinstance(value, list):
                oids.update(str(v) for v in value if v)
        # Older runs stored a single id under the singular key.
        for key, value in extras.items():
            if (key.startswith("pro_buy_oid_") or key.startswith("pro_sell_oid_")) and value:
                oids.add(str(value))
    return oids


def _filled_oids_from_trades(trades_path: str, limit_bytes: int = 2_000_000) -> Set[str]:
    """Order ids this run has filled. Only the tail is read — the file grows all
    session and the ids we need to label are recent ones."""
    oids: Set[str] = set()
    try:
        size = os.path.getsize(trades_path)
        with open(trades_path, "r", encoding="utf-8", errors="replace") as fh:
            if size > limit_bytes:
                fh.seek(size - limit_bytes)
                fh.readline()  # discard the partial line seeking landed in
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                oid = rec.get("order_id") if isinstance(rec, dict) else None
                if oid:
                    oids.add(str(oid))
    except OSError:
        return oids
    return oids


class Attribution:
    """Order id -> owning run, refreshed from the runs' own state files."""

    def __init__(self, user_dir: str, refresh_seconds: float = _REFRESH_SECONDS) -> None:
        self.user_dir = user_dir
        self._refresh_seconds = refresh_seconds
        self._owner: Dict[str, str] = {}
        self._loaded_at = 0.0

    def refresh(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._loaded_at) < self._refresh_seconds:
            return
        owner: Dict[str, str] = {}
        user_name = os.path.basename(self.user_dir.rstrip(os.sep))
        for run_dir in _iter_run_dirs(self.user_dir):
            run_name = "{}/{}".format(user_name, os.path.basename(run_dir))
            state_dir = os.path.join(run_dir, "state")
            for oid in _live_oids_from_state(os.path.join(state_dir, "state.json")):
                owner[oid] = run_name
            for oid in _filled_oids_from_trades(os.path.join(state_dir, "trades.jsonl")):
                owner.setdefault(oid, run_name)
        self._owner = owner
        self._loaded_at = now

    def owner(self, order_id: str) -> Optional[str]:
        return self._owner.get(str(order_id))

    def classify(self, order_id: str, order_age_s: Optional[float]) -> str:
        """bot / manual / pending. `order_age_s` is how long ago the broker says
        the order was placed; None (unknown age) is treated as old, because an
        order we cannot date is almost certainly not one placed this second."""
        if self.owner(order_id):
            return BOT
        if order_age_s is not None and order_age_s < GRACE_SECONDS:
            return PENDING
        return MANUAL

    def label(self, order_id: str, order_age_s: Optional[float]) -> Dict[str, Any]:
        kind = self.classify(order_id, order_age_s)
        return {"source": kind, "run": self.owner(order_id)}

    def runs(self) -> List[str]:
        return sorted(set(self._owner.values()))
