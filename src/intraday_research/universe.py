from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ResearchUniverse:
    name: str
    symbols: tuple[str, ...]


def load_universe(path: str | Path) -> ResearchUniverse:
    payload = json.loads(Path(path).read_text())
    name = str(payload.get("name") or "").strip()
    symbols = tuple(str(symbol).strip() for symbol in (payload.get("symbols") or []) if str(symbol).strip())
    if not name:
        raise ValueError("Universe file must include a non-empty 'name'.")
    if not symbols:
        raise ValueError("Universe file must include at least one symbol.")
    return ResearchUniverse(name=name, symbols=symbols)


def save_universe(universe: ResearchUniverse, path: str | Path) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps({"name": universe.name, "symbols": list(universe.symbols)}, indent=2))
    return destination
