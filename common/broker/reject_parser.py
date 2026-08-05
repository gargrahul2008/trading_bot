from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class RejectAction:
    kind: str  # 'REDUCE_QTY' | 'AUTH_REQUIRED' | 'MARGIN_SHORTFALL' | 'CIRCUIT_LIMIT'
               # | 'SESSION_CLOSED' | 'DQ_NOT_ALLOWED' | 'NOT_RETRYABLE'
    max_qty: Optional[int] = None
    reason: str = ""
    raw_message: str = ""

_MARGIN_PATTERNS = [
    r"margin\s+shortfall", r"insufficient\s+margin", r"insufficient\s+funds",
    r"insufficient\s+balance", r"available\s+margin",
]
_CIRCUIT_PATTERNS = [
    r"circuit\s+limit", r"upper\s*circuit", r"lower\s*circuit", r"price\s+band",
]
# CAS (Closing Auction Session) for F&O stocks: after ~15:15 the continuous market
# closes and normal limit orders are rejected ("not allowed to trade in this market");
# in the auction window disclosed-qty (DQ / iceberg) orders are rejected. Both are
# session-window rejects that only clear next session — never worth re-probing on a
# short timer, so classify them for a long back-off.
_SESSION_PATTERNS = [
    r"not\s+allowed\s+to\s+trade\s+in\s+this\s+market", r"\b16387\b",
    r"market\s+is\s+closed", r"trading\s+is\s+not\s+allowed", r"session\s+is\s+closed",
]
_DQ_PATTERNS = [
    r"\b16439\b", r"dq\s+orders?\s+are\s+not\s+allowed", r"disclosed\s+q",
    r"iceberg.*not\s+allowed",
]

_AUTH_PATTERNS = [
    r"tpin", r"e-?dis", r"authori[sz]e", r"cdsl", r"ddpi", r"poa", r"authorization required",
    r"holdings.*authori[sz]", r"verify.*tpin"
]
_QTY_PATTERNS = [
    r"insufficient\s+qty", r"insufficient\s+quantity",
    r"insufficient\s+holdings", r"insufficient\s+shares",
    r"exceed[s]?\s+available", r"available\s+qty", r"available\s+quantity",
    r"only\s+\d+\s+.*available", r"sell\s+only\s+\d+",
    r"you\s+can\s+sell\s+only", r"short\s+selling\s+not\s+allowed",
    r"rms"
]

def _extract_qty_candidates(msg: str) -> list[int]:
    # pick plausible share quantities from message
    nums = [int(x) for x in re.findall(r"(\d{1,9})", msg)]
    # remove tiny irrelevant values like 1 or 2 if many
    return [n for n in nums if n >= 1]

def parse_reject(resp_or_msg: Any) -> RejectAction:
    msg = ""
    if isinstance(resp_or_msg, dict):
        msg = str(resp_or_msg.get("message") or resp_or_msg.get("msg") or resp_or_msg.get("error") or "")
        if not msg:
            # sometimes nested
            data = resp_or_msg.get("data") or {}
            if isinstance(data, dict):
                msg = str(data.get("message") or data.get("msg") or "")
    else:
        msg = str(resp_or_msg or "")
    low = msg.lower()

    if any(re.search(p, low) for p in _AUTH_PATTERNS):
        return RejectAction(kind="AUTH_REQUIRED", reason="Authorization/TPIN/eDIS required", raw_message=msg)

    if any(re.search(p, low) for p in _MARGIN_PATTERNS):
        # Capital-bound: only worth retrying after freed capital (a sell fill), not on a blind timer.
        return RejectAction(kind="MARGIN_SHORTFALL", reason="Insufficient margin/funds", raw_message=msg)

    if any(re.search(p, low) for p in _CIRCUIT_PATTERNS):
        # Price band is fixed for the whole trading day — a short retry timer is futile.
        return RejectAction(kind="CIRCUIT_LIMIT", reason="Order outside daily circuit/price band", raw_message=msg)

    if any(re.search(p, low) for p in _DQ_PATTERNS):
        # Disclosed-qty not accepted in this session (e.g. CAS auction window) — the order
        # would go through without disclosed qty, but it won't clear on a short timer here.
        return RejectAction(kind="DQ_NOT_ALLOWED", reason="Disclosed-qty order not allowed in this session", raw_message=msg)

    if any(re.search(p, low) for p in _SESSION_PATTERNS):
        # Security not tradable in this market/session right now (e.g. CAS closes the
        # continuous market ~15:15 for F&O names). Persists for the rest of the session.
        return RejectAction(kind="SESSION_CLOSED", reason="Security not tradable now (session/CAS)", raw_message=msg)

    if any(re.search(p, low) for p in _QTY_PATTERNS):
        # try to infer a max qty from message numbers
        cands = _extract_qty_candidates(low)
        # heuristic: smallest positive number in msg is often the allowed qty
        max_qty = min(cands) if cands else None
        return RejectAction(kind="REDUCE_QTY", max_qty=max_qty, reason="Quantity/holdings constraint", raw_message=msg)

    return RejectAction(kind="NOT_RETRYABLE", reason="Unknown/non-retryable reject", raw_message=msg)
