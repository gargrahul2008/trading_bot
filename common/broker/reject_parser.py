from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class RejectAction:
    kind: str  # 'REDUCE_QTY' | 'AUTH_REQUIRED' | 'NOT_RETRYABLE'
    max_qty: Optional[int] = None
    reason: str = ""
    raw_message: str = ""

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

    if any(re.search(p, low) for p in _QTY_PATTERNS):
        # try to infer a max qty from message numbers
        cands = _extract_qty_candidates(low)
        # heuristic: smallest positive number in msg is often the allowed qty
        max_qty = min(cands) if cands else None
        return RejectAction(kind="REDUCE_QTY", max_qty=max_qty, reason="Quantity/holdings constraint", raw_message=msg)

    return RejectAction(kind="NOT_RETRYABLE", reason="Unknown/non-retryable reject", raw_message=msg)
