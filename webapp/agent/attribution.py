"""Which orders belong to a bot, and which a human placed.

The dashboard shows every order in the account, whatever placed it. To label
them we read what the bots themselves recorded under
accounts/<user>/<run>/state/ — no broker call, just files on the same host.

Three sources, in descending order of certainty:

1. **Live claims** — the order ids a run currently has working, from its
   state.json. Certain, but short-lived.
2. **Sticky claims** — every id we have ever seen claimed, kept in
   accounts/<user>/reports/agent_claims.json. This exists because a bot order
   that is *cancelled without filling* disappears from both of the bot's own
   records: `_clear_pro_oids` empties the live list, and trades.jsonl is only
   written on a fill (generic_runner.py:667). After the EOD cancel every
   unfilled bot order of the day would otherwise read as manual. Recording the
   claim while the order is still working makes it survive the cancel.
3. **The broker's own channel** — Fyers stamps each order with how it was
   placed. Our bots come back `API`; anything placed by hand in the web terminal
   comes back `W`/`W1` with a tag naming the control. A web order is manual as a
   matter of fact, not inference, whatever symbol it is on.
4. **Configured symbol** — a run trades one symbol with one product type, both
   named in its config.json. An API order matching that pair is almost certainly
   that run's. This remains an *inference*, but the channel now rules out the
   case that used to make it unsafe: a human buying the same symbol by hand no
   longer matches, because their order is stamped `web`.

The race this has to survive: a bot places an order and writes its state a
moment later, so a poll landing in between sees an unclaimed id that is not
actually manual. Anything younger than GRACE_SECONDS is therefore reported as
"pending" rather than "manual", and settles on a later poll.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

GRACE_SECONDS = 15.0

BOT = "bot"
MANUAL = "manual"
PENDING = "pending"

BY_ORDER_ID = "order_id"
BY_SYMBOL = "symbol"
BY_CHANNEL = "channel"

# Re-reading every run's state on each 3-second poll is wasteful; the files only
# change when a bot acts.
_REFRESH_SECONDS = 5.0

# Sticky claims are pruned to this many, newest first. Fyers order ids begin
# with the date (26082600000580 -> 2026-08-26), so a lexical sort is a
# chronological one and the oldest entries drop out first.
_MAX_STICKY_CLAIMS = 20000


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


def _read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _run_signature(config_path: str) -> Optional[Tuple[Set[str], str]]:
    """(symbols, product_type) for a run, from its config.json."""
    config = _read_json(config_path)
    if config is None:
        return None
    raw_symbols = ((config.get("strategy") or {}).get("symbols")) or []
    symbols = set()
    for entry in raw_symbols:
        # sell_first-style configs use [{"symbol": ...}] rather than a bare list.
        symbol = entry.get("symbol") if isinstance(entry, dict) else entry
        if symbol:
            symbols.add(str(symbol).upper())
    if not symbols:
        return None
    product = str((config.get("execution") or {}).get("product_type") or "").upper()
    return symbols, product


def _live_oids_from_state(state_path: str) -> Set[str]:
    """Order ids a run currently has working.

    Two places hold them: `pending_order_id` on each symbol (reactive runs) and
    the `pro_{buy,sell}_oids_<symbol>` lists in extras (proactive ladders). See
    generic_runner.py:1400 for how the latter are maintained.
    """
    raw = _read_json(state_path)
    if raw is None:
        return set()

    oids: Set[str] = set()
    for sym_state in (raw.get("symbol_states") or {}).values():
        if isinstance(sym_state, dict) and sym_state.get("pending_order_id"):
            oids.add(str(sym_state["pending_order_id"]))

    extras = raw.get("extras") or {}
    if isinstance(extras, dict):
        for key, value in extras.items():
            if key.startswith("pro_buy_oids_") or key.startswith("pro_sell_oids_"):
                if isinstance(value, list):
                    oids.update(str(v) for v in value if v)
            # Older runs stored a single id under the singular key.
            elif (key.startswith("pro_buy_oid_") or key.startswith("pro_sell_oid_")) and value:
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
                if isinstance(rec, dict) and rec.get("order_id"):
                    oids.add(str(rec["order_id"]))
    except OSError:
        return oids
    return oids


class Attribution:
    """Order -> owning run, from the runs' own files."""

    def __init__(
        self,
        user_dir: str,
        refresh_seconds: float = _REFRESH_SECONDS,
        claims_path: Optional[str] = None,
    ) -> None:
        self.user_dir = user_dir
        self.user = os.path.basename(user_dir.rstrip(os.sep))
        self._refresh_seconds = refresh_seconds
        self._claims_path = claims_path or os.path.join(
            user_dir, "reports", "agent_claims.json"
        )
        self._owner: Dict[str, str] = {}
        self._signatures: List[Tuple[str, Set[str], str]] = []
        self._loaded_at = 0.0
        self._sticky_dirty = False
        self._load_sticky()

    # ── sticky claims ───────────────────────────────────────────────────────
    def _load_sticky(self) -> None:
        data = _read_json(self._claims_path) or {}
        claims = data.get("claims")
        if isinstance(claims, dict):
            self._owner.update({str(k): str(v) for k, v in claims.items()})

    def _save_sticky(self) -> None:
        """Best effort: losing the file costs attribution of already-cancelled
        orders, never correctness of anything live."""
        if not self._sticky_dirty:
            return
        claims = self._owner
        if len(claims) > _MAX_STICKY_CLAIMS:
            keep = sorted(claims)[-_MAX_STICKY_CLAIMS:]
            claims = {oid: claims[oid] for oid in keep}
            self._owner = claims
        payload = {"user": self.user, "updated_at": time.time(), "claims": claims}
        tmp = self._claims_path + ".tmp"
        try:
            os.makedirs(os.path.dirname(self._claims_path), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, self._claims_path)
            self._sticky_dirty = False
        except OSError:
            pass

    # ── refresh ─────────────────────────────────────────────────────────────
    def refresh(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._loaded_at) < self._refresh_seconds:
            return

        signatures: List[Tuple[str, Set[str], str]] = []
        for run_dir in _iter_run_dirs(self.user_dir):
            run_name = "{}/{}".format(self.user, os.path.basename(run_dir))
            signature = _run_signature(os.path.join(run_dir, "config.json"))
            if signature is not None:
                signatures.append((run_name, signature[0], signature[1]))

            state_dir = os.path.join(run_dir, "state")
            claimed = _live_oids_from_state(os.path.join(state_dir, "state.json"))
            claimed |= _filled_oids_from_trades(os.path.join(state_dir, "trades.jsonl"))
            for oid in claimed:
                if self._owner.get(oid) != run_name:
                    # Recorded now so it survives the bot clearing its own list
                    # when the order is cancelled.
                    self._owner[oid] = run_name
                    self._sticky_dirty = True

        self._signatures = signatures
        self._loaded_at = now
        self._save_sticky()

    # ── lookups ─────────────────────────────────────────────────────────────
    def owner(self, order_id: str) -> Optional[str]:
        return self._owner.get(str(order_id))

    def run_for_symbol(self, symbol: str, product_type: str = "") -> Optional[str]:
        """The run configured to trade this symbol, if exactly one is.

        Product type has to agree when the run declares one — the same symbol
        held as CNC by hand is not the MTF ladder's position.
        """
        if not symbol:
            return None
        symbol = str(symbol).upper()
        product = str(product_type or "").upper()
        matches = {
            run for run, symbols, run_product in self._signatures
            if symbol in symbols and (not run_product or not product or run_product == product)
        }
        return matches.pop() if len(matches) == 1 else None

    def label(self, order: Any, order_age_s: Optional[float] = None) -> Dict[str, Any]:
        """bot / manual / pending, with how we decided.

        `order` may be a normalised order dict or a bare order id. `order_age_s`
        is how long ago the broker says it was placed; None (unknown age) counts
        as old, because an order we cannot date is almost certainly not one
        placed this second.
        """
        if isinstance(order, dict):
            order_id = str(order.get("order_id") or "")
            symbol = str(order.get("symbol") or "")
            product = str(order.get("product_type") or "")
        else:
            order_id, symbol, product = str(order or ""), "", ""

        run = self.owner(order_id)
        if run:
            return {"source": BOT, "run": run, "matched_by": BY_ORDER_ID}

        channel = str(order.get("channel") or "") if isinstance(order, dict) else ""

        # The broker says a person placed this in the web terminal. No amount of
        # symbol matching outranks that.
        if channel == "web":
            return {"source": MANUAL, "run": None, "matched_by": BY_CHANNEL}

        run = self.run_for_symbol(symbol, product)
        if run:
            # Inferred from what the run is configured to trade. Safe now that a
            # hand-placed order on the same symbol is excluded above — but still
            # an inference, so the UI says which orders were claimed outright.
            return {"source": BOT, "run": run, "matched_by": BY_SYMBOL}

        if order_age_s is not None and order_age_s < GRACE_SECONDS:
            return {"source": PENDING, "run": None, "matched_by": None}
        return {"source": MANUAL, "run": None, "matched_by": None}

    def classify(self, order: Any, order_age_s: Optional[float] = None) -> str:
        return self.label(order, order_age_s)["source"]

    def runs(self) -> List[str]:
        """Every run configured for this account, whether or not it has traded."""
        return sorted({run for run, _, _ in self._signatures} | set(self._owner.values()))
