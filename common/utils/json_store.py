from __future__ import annotations
import json, os, dataclasses
from typing import Any

def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def atomic_write_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
    os.replace(tmp, path)

def asdict(x: Any) -> Any:
    return dataclasses.asdict(x) if dataclasses.is_dataclass(x) else x
