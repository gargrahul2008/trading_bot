#!/usr/bin/env python3
"""
Generate systemd units for the multi-account host by scanning the accounts/ layout.

Emits into deploy/systemd/generated/ (review, then copy to /etc/systemd/system/):
  - bot-<user>-<strategy>.service   : one live bot per strategy run.
      Reads accounts/<user>/account.env (identity + this account's IP/proxy — single source),
      runs run_strategy.py against that run's config.
      DELIBERATELY NOT ENABLE-ABLE: no [Install] section, so `systemctl enable` refuses.
      deploy/cron/start_equity_bots.sh starts these at 08:55 IST and stop_equity_bots.sh
      stops them at 15:31. A Fyers SDK session left idling overnight wedges at HTTP 429 on
      the open-bell burst and never recovers, so a fresh session each morning is the design,
      not an accident — and that script is also where bots are held down. Enabling these
      would start every bot on boot, including any that is deliberately held down.
  - dashboard.service               : the dashboard API + built UI, on loopback only.
      Reaches the brokers only through the agents, holds no credentials of its own, and
      is fronted by Tailscale Serve for TLS (see docs/dashboard_https.md).
  - agent-<user>.service            : the dashboard's per-account Fyers agent, IP-bound
      (same account.env again). Polls positions/orders/funds/holdings for the dashboard
      and is the only path by which the dashboard reaches a broker.
Token refresh is NOT a systemd unit — it runs as a daily cron one-shot
(deploy/cron/refresh_tokens.sh, per-user and IP-bound).

All of a user's bot units share that user's ONE account.env, so changing the account's IP
is a one-file edit. Re-run this after adding/removing a strategy folder.

INSTALL_DIR must match where the repo lives on the control host.
"""
from __future__ import annotations
import json
import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ACCOUNTS = REPO / "accounts"
OUT = REPO / "deploy" / "systemd" / "generated"

#: Which accounts may place orders. One account name per line, or the single
#: word "all". Absent means every agent is read-only, which is the right default
#: for a fresh clone.
TRADING_FILE = REPO / "deploy" / "trading_enabled"

# Where the repo and its virtualenv live on the CONTROL HOST (64.227.135.117).
# These are the real values, not placeholders, so regenerating on the host
# produces no diff — and the units committed here are the ones that actually
# work if copied to /etc/systemd/system. Override for a different host.
INSTALL_DIR = os.environ.get("INSTALL_DIR", "/root/trading_bot")
PYTHON = os.environ.get("PYTHON", f"{INSTALL_DIR}/env/bin/python")

# Loopback port for the first account's agent. Assignments are recorded in
# AGENT_PORTS (tracked in git) and never reused, so an account keeps its port
# forever — including when a new account is added that sorts before it. The
# dashboard config points at these, and a silent reshuffle would aim it at the
# wrong account.
AGENT_BASE_PORT = int(os.environ.get("AGENT_BASE_PORT", "9101"))
AGENT_PORTS = REPO / "deploy" / "agent_ports.json"

# The dashboard's own port. Loopback only — Tailscale Serve fronts it.
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8000"))

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

# No [Install] section, on purpose: `systemctl enable` refuses a unit without one.
# These are started and stopped daily by deploy/cron/start_equity_bots.sh and
# stop_equity_bots.sh — a fresh SDK session each morning, because one left idling
# overnight wedges at HTTP 429 on the open-bell burst and never recovers. That
# script is also where individual bots are held down; enabling this unit would
# start it on boot regardless, held down or not.
"""

def load_agent_ports() -> dict:
    try:
        with open(AGENT_PORTS, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return {str(k): int(v) for k, v in data.items()} if isinstance(data, dict) else {}


def assign_agent_port(ports: dict, user: str) -> int:
    """The user's recorded port, or the lowest free one above the base."""
    if user in ports:
        return ports[user]
    taken = set(ports.values())
    port = AGENT_BASE_PORT
    while port in taken:
        port += 1
    ports[user] = port
    return port


DASHBOARD_UNIT = """[Unit]
Description=Trading dashboard (API + UI)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={install}
# AGENT_TOKEN, to reach the agents; and the dashboard's own password hash and
# session secret. No broker credentials — this process never calls a broker.
EnvironmentFile={install}/webapp/agent.env
EnvironmentFile={install}/webapp/dashboard.env
# Loopback only, always. TLS and access control are Tailscale Serve's job, and
# binding a port that can place orders to 0.0.0.0 would put it one firewall rule
# away from the open internet.
ExecStart={install}/webapp/api/.venv/bin/uvicorn app.main:app \\
    --app-dir {install}/webapp/api --host 127.0.0.1 --port {port}
Restart=always
RestartSec=5
SyslogIdentifier=dashboard

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


def trading_accounts() -> object:
    """Which accounts may place orders: a set of names, or "all".

    Read from a file rather than only from the environment, because arming has
    to survive the next deploy. An environment variable lives for one shell
    invocation: a later deploy run without it would regenerate every unit
    read-only and restart the agents, turning trading off minutes after it was
    turned on and saying nothing about it.

    Per account, not all-or-nothing, so the account being traded by hand can be
    armed while the ones running bots stay read-only.
    """
    raw = os.environ.get("ALLOW_TRADING", "").strip()
    if not raw and TRADING_FILE.exists():
        raw = " ".join(
            line.split("#")[0].strip()
            for line in TRADING_FILE.read_text(encoding="utf-8").splitlines()
        ).strip()
    if not raw:
        return set()
    names = {part.strip() for part in raw.replace(",", " ").split() if part.strip()}
    if names & {"1", "true", "yes", "all"}:
        return "all"
    return names


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    # Trading routes are opt-in: an agent generated without its account named
    # here is read-only and cannot place an order however it is called.
    armed = trading_accounts()
    emit_auth = os.environ.get("EMIT_AUTH_UNITS", "").strip() in ("1", "true", "yes")
    ports = load_agent_ports()
    known = dict(ports)

    (OUT / "dashboard.service").write_text(
        DASHBOARD_UNIT.format(install=INSTALL_DIR, port=DASHBOARD_PORT)
    )
    print(f"  dashboard.service (127.0.0.1:{DASHBOARD_PORT}, TLS via Tailscale Serve)")

    for user_dir in sorted(ACCOUNTS.iterdir()):
        if not user_dir.is_dir() or user_dir.name.startswith("_"):
            continue
        user = user_dir.name
        # one dashboard agent per user, on its own stable loopback port
        port = assign_agent_port(ports, user)
        allow_trading = armed == "all" or user in armed
        (OUT / f"agent-{user}.service").write_text(
            AGENT_UNIT.format(
                user=user, install=INSTALL_DIR, python=PYTHON, port=port,
                per_min=AGENT_PER_MIN,
                trading=" \\\n    --allow-trading" if allow_trading else "",
            )
        )
        print(f"  agent-{user}.service (port {port}, trading "
              f"{'ENABLED' if allow_trading else 'disabled'})")
        # (token refresh is not a systemd unit — deploy/cron/refresh_tokens.sh handles it)
        # one bot unit per strategy folder (a dir containing config.json)
        for strat_dir in sorted(user_dir.iterdir()):
            if not strat_dir.is_dir() or not (strat_dir / "config.json").exists():
                continue
            strat = strat_dir.name
            (OUT / f"bot-{user}-{strat}.service").write_text(
                BOT_UNIT.format(user=user, strat=strat, install=INSTALL_DIR, python=PYTHON)
            )
            print(f"  bot-{user}-{strat}.service (started by cron, not enable-able)")

    if armed:
        who = "every account" if armed == "all" else ", ".join(sorted(armed))
        print(f"\n  *** TRADING ENABLED for {who} — these agents can place real orders.")
        print(f"  Source: {'ALLOW_TRADING in the environment' if os.environ.get('ALLOW_TRADING') else TRADING_FILE}")
        unknown = set() if armed == "all" else armed - set(ports)
        if unknown:
            print(f"  WARNING: {', '.join(sorted(unknown))} named but not an account here"
                  " — check the spelling, nothing was armed for it.")

    if not emit_auth:
        print("\n  fyers-auth-*.service not emitted: deploy/cron/refresh_tokens.sh"
              " already refreshes tokens daily.")
        print("  Set EMIT_AUTH_UNITS=1 only on a host without that cron.")

    if ports != known:
        with open(AGENT_PORTS, "w", encoding="utf-8") as fh:
            json.dump(dict(sorted(ports.items())), fh, indent=2)
            fh.write("\n")
        added = sorted(set(ports) - set(known))
        print(f"\nRecorded new agent port(s) for {', '.join(added)} in {AGENT_PORTS.name}.")
        print("Commit it, so the host and your workstation agree on the ports.")


if __name__ == "__main__":
    main()
