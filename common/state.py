from __future__ import annotations

import json
import os
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional, Type, TypeVar

T = TypeVar("T")

def load_json(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def atomic_write_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        if is_dataclass(obj):
            json.dump(asdict(obj), f, indent=2, default=str)
        else:
            json.dump(obj, f, indent=2, default=str)
    os.replace(tmp, path)

def load_dataclass(path: str, cls: Type[T]) -> T:
    raw = load_json(path)
    return cls(**raw)  # type: ignore[arg-type]
