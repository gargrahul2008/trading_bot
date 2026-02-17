from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .logger import LOG

def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)  # type: ignore[arg-type]
        else:
            out[k] = v
    return out

def load_config(path: str, *, overrides: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    """Load JSON config. Returns (config_dict, config_dir)."""
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if overrides:
        cfg = _deep_merge(cfg, overrides)
    cfg_dir = os.path.dirname(os.path.abspath(path))
    return cfg, cfg_dir

def resolve_path(cfg_dir: str, maybe_rel: str) -> str:
    if not maybe_rel:
        return maybe_rel
    if os.path.isabs(maybe_rel):
        return maybe_rel
    return os.path.normpath(os.path.join(cfg_dir, maybe_rel))
