#!/usr/bin/env python3
"""
Generate systemd units for the multi-account host by scanning the accounts/ layout.

Emits into deploy/systemd/generated/ (review, then copy to /etc/systemd/system/):
  - bot-<user>-<strategy>.service   : one live bot per strategy run.
      Reads accounts/<user>/account.env (identity + this account's IP/proxy — single source),
      runs run_strategy.py against that run's config.
  - fyers-auth-<user>.service       : one daily TOTP token refresh per user, IP-bound
      (reads the SAME account.env, so auth exits through the account's whitelisted IP).
  - agent-<user>.service            : the dashboard's per-account Fyers agent, IP-bound
      (same account.env again). Polls positions/orders/funds/holdings for the dashboard
      and is the only path by which the dashboard reaches a broker.

All of a user's bot units share that user's ONE account.env, so changing the account's IP
is a one-file edit. Re-run this after adding/removing a strategy folder.

INSTALL_DIR must match where the repo lives on the control host.
"""
from __future__ import annotations
import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ACCOUNTS = REPO / "accounts"
OUT = REPO / "deploy" / "systemd" / "generated"

# Where the repo lives on the CONTROL HOST (edit to match, e.g. /root/trading_bot).
INSTALL_DIR = os.environ.get("INSTALL_DIR", "/opt/trading_bot")
PYTHON = f"{INSTALL_DIR}/.venv/bin/python"

# Loopback port for the first account's agent; each further account takes the
# next one. Assigned in directory order so a user keeps the same port across
# regenerations — the dashboard's config points at these.
AGENT_BASE_PORT = int(os.environ.get("AGENT_BASE_PORT", "9101"))

# Requests per minute each agent may spend. The bots on the same app are already
# spending roughly 24/min per run, and the app limit is shared, so this is set
# well below it — see webapp/agent/budget.py.
AGENT_PER_MIN = os.environ.get("AGENT_PER_MIN", "60")

BOT_UNIT = """[Unit]
Description=Fyers bot: {user}/{strat}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={install}
EnvironmentFile={install}/accounts/{user}/account.env
ExecStart={python} run_strategy.py --config accounts/{user}/{strat}/config.json
Restart=always
RestartSec=5
SyslogIdentifier=bot-{user}-{strat}

[Install]
WantedBy=multi-user.target
"""

AUTH_UNIT = """[Unit]
Description=Fyers daily token refresh (IP-bound) for account {user}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={install}
EnvironmentFile={install}/accounts/{user}/account.env
# Refresh THIS user only (never --enabled-only, which would use one shared IP).
ExecStart={python} scripts/fyers_auto_auth.py --auth-file fyers_auth.json \\
    --user-key ${{FYERS_USER_KEY}} --loop --daily-at 08:30 --timezone Asia/Kolkata
Restart=always
RestartSec=30
SyslogIdentifier=fyers-auth-{user}

[Install]
WantedBy=multi-user.target
"""

AGENT_UNIT = """[Unit]
Description=Dashboard Fyers agent (IP-bound) for account {user}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={install}
# The SAME account.env as this user's bots: identity, and the proxy that puts
# these calls on the account's whitelisted IP.
EnvironmentFile={install}/accounts/{user}/account.env
# AGENT_TOKEN is the shared secret the dashboard authenticates with. Keep it out
# of account.env (which is per-account) and out of the repo.
EnvironmentFile={install}/webapp/agent.env
ExecStart={python} -m webapp.agent.main \\
    --user {user} --port {port} --per-min {per_min}{trading}
Restart=always
RestartSec=10
SyslogIdentifier=agent-{user}

[Install]
WantedBy=multi-user.target
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    # Trading routes are opt-in: without ALLOW_TRADING=1 the generated agents are
    # read-only and cannot place an order however they are called.
    allow_trading = os.environ.get("ALLOW_TRADING", "").strip() in ("1", "true", "yes")
    port = AGENT_BASE_PORT

    for user_dir in sorted(ACCOUNTS.iterdir()):
        if not user_dir.is_dir() or user_dir.name.startswith("_"):
            continue
        user = user_dir.name
        # one dashboard agent per user, on its own loopback port
        (OUT / f"agent-{user}.service").write_text(
            AGENT_UNIT.format(
                user=user, install=INSTALL_DIR, python=PYTHON, port=port,
                per_min=AGENT_PER_MIN,
                trading=" \\\n    --allow-trading" if allow_trading else "",
            )
        )
        print(f"  agent-{user}.service (port {port}, trading "
              f"{'ENABLED' if allow_trading else 'disabled'})")
        port += 1
        # one auth unit per user
        (OUT / f"fyers-auth-{user}.service").write_text(
            AUTH_UNIT.format(user=user, install=INSTALL_DIR, python=PYTHON)
        )
        print(f"  fyers-auth-{user}.service")
        # one bot unit per strategy folder (a dir containing config.json)
        for strat_dir in sorted(user_dir.iterdir()):
            if not strat_dir.is_dir() or not (strat_dir / "config.json").exists():
                continue
            strat = strat_dir.name
            (OUT / f"bot-{user}-{strat}.service").write_text(
                BOT_UNIT.format(user=user, strat=strat, install=INSTALL_DIR, python=PYTHON)
            )
            print(f"  bot-{user}-{strat}.service")


if __name__ == "__main__":
    main()
