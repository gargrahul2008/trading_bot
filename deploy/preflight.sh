#!/usr/bin/env bash
# One command that checks whether this host is in the state it is meant to be in.
#
#   deploy/preflight.sh            full check, including egress IPs
#   deploy/preflight.sh --quick    skip the checks that leave the machine
#
# Written after an afternoon of guessing at causes across four separate
# problems, every one of which this would have named in two seconds:
#   - the bot units had been enabled, which would have started held-down bots on
#     the next boot
#   - the agents were polling with a token that expired 34 hours earlier
#   - a bot "not running" that was simply outside market hours
#   - auth units that would have double-refreshed against the cron
#
# Exits non-zero if anything FAILs, so it can gate a deploy.
set -u

cd "$(dirname "$0")/.." || exit 1
REPO="$(pwd)"
QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1

PY="$REPO/env/bin/python"
[ -x "$PY" ] || PY="$(command -v python3)"

n_pass=0; n_warn=0; n_fail=0
if [ -t 1 ]; then G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; D=$'\033[2m'; Z=$'\033[0m'
else G=""; Y=""; R=""; D=""; Z=""; fi

section() { printf '\n%s── %s%s\n' "$D" "$1" "$Z"; }
ok()   { n_pass=$((n_pass+1)); printf '  %sPASS%s  %s\n' "$G" "$Z" "$1"; }
warn() { n_warn=$((n_warn+1)); printf '  %sWARN%s  %s\n' "$Y" "$Z" "$1"; }
bad()  { n_fail=$((n_fail+1)); printf '  %sFAIL%s  %s\n' "$R" "$Z" "$1"; }

have_systemd() { command -v systemctl >/dev/null 2>&1; }

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

# Runs a command that emits "PASS|msg" / "WARN|msg" / "FAIL|msg" / "SKIP|msg"
# lines and folds them into the counters. A pipeline would put the counters in a
# subshell, where every increment is discarded.
report() {
  "$@" > "$TMP" 2>&1
  while IFS='|' read -r level message; do
    case "$level" in
      PASS) ok   "$message" ;;
      WARN) warn "$message" ;;
      FAIL) bad  "$message" ;;
      SKIP) printf '  %sSKIP%s  %s\n' "$D" "$Z" "$message" ;;
      *)    [ -n "$level$message" ] && printf '  %s%s\n' "$level" "${message:+|$message}" ;;
    esac
  done < "$TMP"
}

accounts() {
  for d in "$REPO"/accounts/*/; do
    [ -f "$d/account.env" ] || continue
    basename "$d"
  done
}

runs_for() {
  for d in "$REPO/accounts/$1"/*/; do
    [ -f "$d/config.json" ] || continue
    basename "$d"
  done
}

# ── environment ─────────────────────────────────────────────────────────────
section "Environment"
printf '  %srepo%s     %s\n' "$D" "$Z" "$REPO"
printf '  %sbranch%s   %s @ %s\n' "$D" "$Z" \
  "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')" \
  "$(git rev-parse --short HEAD 2>/dev/null || echo '?')"

if [ -x "$PY" ]; then ok "interpreter $PY"; else bad "no interpreter at $REPO/env/bin/python"; fi

if git diff --quiet 2>/dev/null; then
  ok "working tree clean"
else
  # Not fatal, but a deploy from a dirty tree is not the deploy you reviewed.
  warn "working tree has uncommitted changes — running code may not match the commit above"
fi

# ── secrets ─────────────────────────────────────────────────────────────────
section "Secrets"
check_secret() {
  local path="$1" key="$2" required="$3"
  if [ ! -f "$path" ]; then
    if [ "$required" = "yes" ]; then bad "$path missing"; else warn "$path missing (optional)"; fi
    return
  fi
  local mode; mode="$(stat -c '%a' "$path" 2>/dev/null || echo '?')"
  if ! grep -q "^$key=" "$path" 2>/dev/null; then
    bad "$path has no $key"
  elif grep -q "^$key=replace-me" "$path" 2>/dev/null; then
    bad "$path still has the placeholder $key"
  elif [ "$mode" != "600" ]; then
    warn "$path has $key but mode $mode — should be 600"
  else
    ok "$path ($key set, mode 600)"
  fi
}
check_secret "$REPO/webapp/agent.env" AGENT_TOKEN yes
check_secret "$REPO/webapp/dashboard.env" DASHBOARD_PASSWORD_HASH no

# ── systemd units ───────────────────────────────────────────────────────────
section "Units"
if ! have_systemd; then
  warn "no systemctl on this machine — skipping unit checks"
else
  for user in $(accounts); do
    for run in $(runs_for "$user"); do
      unit="bot-$user-$run.service"
      # NB: is-enabled exits 1 for a disabled unit, so its exit code says nothing
      # about whether the unit exists — only empty output does. Reading the code
      # instead had this check warning "not installed" about six units that were
      # installed, two of them trading at the time.
      state="$(systemctl is-enabled "$unit" 2>/dev/null)"
      active="$(systemctl is-active "$unit" 2>/dev/null)"
      case "${state:-missing}" in
        disabled|static)
          # Disabled is correct: deploy/cron/start_equity_bots.sh owns the daily
          # lifecycle. Whether it is running right now depends on the hour and on
          # whether the run is held down, so report that separately.
          ok "$unit $state, currently ${active:-unknown}" ;;
        enabled)
          bad "$unit is ENABLED — it must not start on boot (cron owns it, and holds bots down). Run: systemctl disable $unit" ;;
        *)
          warn "$unit not installed" ;;
      esac
    done
  done

  for user in $(accounts); do
    unit="agent-$user.service"
    state="$(systemctl is-enabled "$unit" 2>/dev/null)"
    if [ "$state" = "enabled" ]; then
      active="$(systemctl is-active "$unit" 2>&1)"
      if [ "$active" = "active" ]; then ok "$unit enabled and active"
      else bad "$unit enabled but $active"; fi
    else
      warn "$unit is ${state:-not installed} — the dashboard has no data for $user without it"
    fi
  done

  for f in "$REPO"/deploy/systemd/generated/*.service; do
    [ -f "$f" ] || continue
    installed="/etc/systemd/system/$(basename "$f")"
    [ -f "$installed" ] || continue
    if ! cmp -s "$f" "$installed"; then
      warn "$(basename "$f") in /etc/systemd/system differs from the generated one — cp it in a non-trading window"
    fi
  done

  if ls /etc/systemd/system/fyers-auth-*.service >/dev/null 2>&1; then
    bad "fyers-auth-*.service installed AND deploy/cron/refresh_tokens.sh exists — both would refresh, and could invalidate a token mid-session"
  else
    ok "no fyers-auth units installed (cron owns token refresh)"
  fi
fi

# ── cron ────────────────────────────────────────────────────────────────────
section "Cron"
CRON="$(crontab -l 2>/dev/null)"
if [ -z "$CRON" ]; then
  warn "no crontab for $(whoami) — the bots' whole daily lifecycle lives there"
else
  for script in refresh_tokens.sh start_equity_bots.sh stop_equity_bots.sh; do
    if printf '%s' "$CRON" | grep -q "deploy/cron/$script"; then
      ok "cron runs $script"
    else
      bad "cron has no entry for deploy/cron/$script"
    fi
  done
fi

# ── tokens ──────────────────────────────────────────────────────────────────
section "Tokens"
report "$PY" "$REPO/deploy/checks/tokens.py" "$REPO"

# ── agents ──────────────────────────────────────────────────────────────────
section "Agents"
TOKEN="$(sed -n 's/^AGENT_TOKEN=//p' "$REPO/webapp/agent.env" 2>/dev/null)"
if [ -z "$TOKEN" ]; then
  bad "no AGENT_TOKEN — cannot query any agent"
else
  report "$PY" "$REPO/deploy/checks/agents.py" "$REPO" "$TOKEN"
fi

# ── egress ──────────────────────────────────────────────────────────────────
section "Egress IPs"
if [ "$QUICK" = "1" ]; then
  warn "skipped (--quick)"
else
  for user in $(accounts); do
    env_file="$REPO/accounts/$user/account.env"
    proxy="$(sed -n 's/^HTTPS_PROXY=//p' "$env_file" | head -1)"
    seen="$(env $(grep -v '^#' "$env_file" | xargs) curl -s --max-time 10 https://api.ipify.org)"
    if [ -z "$seen" ]; then
      bad "$user: could not determine egress IP"
    elif [ -z "$proxy" ]; then
      ok "$user: $seen (no proxy — host IP is its whitelisted IP)"
    else
      # Self-validating: the address seen from outside must be the proxy's own.
      expected="$(printf '%s' "$proxy" | sed -E 's#^https?://##; s#:.*$##')"
      if [ "$seen" = "$expected" ]; then
        ok "$user: $seen (via its proxy)"
      else
        bad "$user: leaves as $seen but its proxy is $expected — Fyers will reject its orders"
      fi
    fi
  done
fi

printf '\n%s──%s %s%d pass%s  %s%d warn%s  %s%d fail%s\n' \
  "$D" "$Z" "$G" "$n_pass" "$Z" "$Y" "$n_warn" "$Z" "$R" "$n_fail" "$Z"
[ "$n_fail" -gt 0 ] && exit 1
exit 0
