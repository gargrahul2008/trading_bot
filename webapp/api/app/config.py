"""Where the API finds the account agents, and its own secrets.

Anchored to the repo root rather than the working directory, so running uvicorn
from webapp/api and from the repo root read the same files.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

REPO = Path(__file__).resolve().parents[3]

AGENT_PORTS = REPO / "deploy" / "agent_ports.json"
AGENT_ENV = REPO / "webapp" / "agent.env"
ACCOUNTS_DIR = REPO / "accounts"

# An agent answers a READ from memory, so a slow reply means it is wedged, not
# busy. Short enough that one bad agent cannot hold up the page.
AGENT_TIMEOUT_SECONDS = float(os.getenv("AGENT_TIMEOUT_SECONDS", "2.5"))

# A trade is the opposite case: the agent is waiting on the broker, so a slow
# reply is the normal one. Placing an order through Fyers routinely takes longer
# than a read timeout allows, and giving up early does not stop the order — it
# only stops us finding out what happened to it, which is the worst outcome
# available. Long enough to outlast a slow broker, bounded so a wedged agent
# still eventually returns.
AGENT_TRADE_TIMEOUT_SECONDS = float(os.getenv("AGENT_TRADE_TIMEOUT_SECONDS", "20"))


def _read_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    except OSError:
        pass
    return values


def agent_token() -> str:
    """The shared secret for talking to the agents.

    Read from the environment first so a container can inject it, falling back
    to the same webapp/agent.env the agents' systemd units use — one secret, one
    place to rotate it.
    """
    return os.getenv("AGENT_TOKEN") or _read_env_file(AGENT_ENV).get("AGENT_TOKEN", "")


def agent_ports() -> Dict[str, int]:
    """account -> loopback port, from the registry the unit generator maintains."""
    try:
        raw = json.loads(AGENT_PORTS.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    ports: Dict[str, int] = {}
    for account, port in raw.items():
        try:
            ports[str(account)] = int(port)
        except (TypeError, ValueError):
            continue
    return ports


def known_accounts() -> List[str]:
    """Accounts with an agent port assigned, in a stable display order."""
    return sorted(agent_ports())


class Settings:
    def __init__(self) -> None:
        self.agent_host = os.getenv("AGENT_HOST", "127.0.0.1")
        self.agent_timeout = AGENT_TIMEOUT_SECONDS
        self.agent_trade_timeout = AGENT_TRADE_TIMEOUT_SECONDS
        self.cors_origins = [
            origin.strip()
            for origin in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")
            if origin.strip()
        ]

    def problems(self) -> List[str]:
        """Refuse to start rather than serve a dashboard that silently shows
        nothing, which looks identical to 'all your accounts are empty'."""
        issues: List[str] = []
        if not agent_token():
            issues.append(
                "No AGENT_TOKEN. Set it in the environment or in %s — the API "
                "cannot talk to any agent without it." % AGENT_ENV
            )
        if not agent_ports():
            issues.append(
                "No agent ports in %s. Run deploy/gen_systemd_units.py to create it."
                % AGENT_PORTS
            )
        return issues


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
