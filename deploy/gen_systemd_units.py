#!/usr/bin/env python3
"""
Generate systemd units for the multi-account host by scanning the accounts/ layout.

Emits into deploy/systemd/generated/ (review, then copy to /etc/systemd/system/):
  - bot-<user>-<strategy>.service   : one live bot per strategy run.
      Reads accounts/<user>/account.env (identity + this account's IP/proxy — single source),
      runs run_strategy.py against that run's config.
Token refresh is NOT a systemd unit — it runs as a daily cron one-shot
(deploy/cron/refresh_tokens.sh, per-user and IP-bound). So only bot units are emitted here.

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
# Venv location on the control host. Override PYTHON directly, or VENV to name the venv dir
# (this host uses env/, not the .venv/ default). PYTHON wins if both are set.
VENV = os.environ.get("VENV", ".venv")
PYTHON = os.environ.get("PYTHON") or f"{INSTALL_DIR}/{VENV}/bin/python"

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

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for user_dir in sorted(ACCOUNTS.iterdir()):
        if not user_dir.is_dir() or user_dir.name.startswith("_"):
            continue
        user = user_dir.name
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
