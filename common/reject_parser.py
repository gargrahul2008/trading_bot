from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

@dataclass
class QtySuggestion:
    suggested_qty: Optional[int]
    reason: str

_QTY_PATTERNS = [
    # "You can sell only 123 quantity"
    re.compile(r"sell\s+only\s+(\d+)", re.IGNORECASE),
    re.compile(r"only\s+(\d+)\s+(?:qty|quantity)\s+(?:is\s+)?(?:available|allowed)", re.IGNORECASE),
    re.compile(r"(?:available|allowed)\s+(?:qty|quantity)\s*[:=]?\s*(\d+)", re.IGNORECASE),
    re.compile(r"(?:max|max\.|maximum)\s+(?:qty|quantity)\s*[:=]?\s*(\d+)", re.IGNORECASE),
    # generic: "... (\d+) shares ..."
    re.compile(r"(\d+)\s+(?:shares|share|qty|quantity)\b", re.IGNORECASE),
]

def parse_qty_suggestion(message: str, requested_qty: int) -> QtySuggestion:
    """Try to infer a better qty from a broker reject message."""
    msg = str(message or "")
    nums = []
    for pat in _QTY_PATTERNS:
        for m in pat.finditer(msg):
            try:
                nums.append(int(m.group(1)))
            except Exception:
                continue
    nums = [n for n in nums if n > 0]
    if not nums:
        return QtySuggestion(None, "no_qty_found")

    # choose the smallest positive number not exceeding requested_qty (safest)
    nums.sort()
    for n in nums:
        if n <= int(requested_qty):
            return QtySuggestion(int(n), f"parsed_from_message:{n}")
    # else, fallback to smallest parsed
    return QtySuggestion(int(nums[0]), f"parsed_from_message_smallest:{nums[0]}")

def is_insufficient_funds(message: str) -> bool:
    msg = (message or "").lower()
    keys = ["insufficient", "margin", "fund", "balance", "cash"]
    return any(k in msg for k in keys)
