"""Is every account agent healthy?

Prints LEVEL|message lines for deploy/preflight.sh.

Checks the three things that have actually gone wrong in production: the broker
rejecting the access token, the agent being rate limited against the bots, and
sections older than the current cadence allows.
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

TIMEOUT = 3.0


def health_of(port: int, token: str) -> dict:
    request = urllib.request.Request("http://127.0.0.1:%d/health" % port)
    request.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


def main(repo: str, token: str) -> int:
    try:
        ports = json.load(open(repo + "/deploy/agent_ports.json"))
    except Exception as exc:
        print("FAIL|cannot read deploy/agent_ports.json: %s" % exc)
        return 0

    for account, port in sorted(ports.items()):
        try:
            health = health_of(int(port), token)
        except urllib.error.HTTPError as exc:
            print("FAIL|%s (:%s): agent answered %s — wrong AGENT_TOKEN?" % (account, port, exc.code))
            continue
        except Exception as exc:
            print("FAIL|%s (:%s): unreachable (%s)" % (account, port, exc))
            continue

        if not health.get("auth_ok", True):
            print("FAIL|%s: the broker is rejecting its access token — restart agent-%s"
                  % (account, account))
            continue

        budget = (health.get("poller") or {}).get("budget") or {}
        if budget.get("rate_limited"):
            print("FAIL|%s: rate limited %d time(s) — it is competing with the bots, lower --per-min"
                  % (account, budget["rate_limited"]))
            continue

        phase = (health.get("poller") or {}).get("phase")
        reloads = (health.get("credentials") or {}).get("reloads")
        stale = sorted(
            name for name, section in (health.get("sections") or {}).items()
            if section.get("stale")
        )
        trading = ", TRADING ENABLED" if health.get("allow_trading") else ""

        if stale:
            # The tolerance scales with the cadence, so a stale section outside
            # market hours is still a genuine problem, not just a slow poll.
            print("WARN|%s: stale sections %s (phase %s)" % (account, ",".join(stale), phase))
        else:
            print("PASS|%s: live, phase %s, token reloads %s%s"
                  % (account, phase, reloads, trading))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
