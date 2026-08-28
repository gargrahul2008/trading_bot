#!/usr/bin/env bash
# The one command to run after pushing changes.
#
#   deploy/deploy.sh            show what would happen
#   deploy/deploy.sh --apply    do it
#
# Pulls, onboards any account newly added to fyers_auth.json, regenerates and
# installs units, restarts the agents only if something they run has changed,
# and finishes with preflight.
#
# What it will never do:
#   - touch a bot unit's enable state. deploy/cron/start_equity_bots.sh owns the
#     bots' daily lifecycle and holds individual bots down; a unit enabled here
#     would start every one of them on the next boot, held down or not.
#   - stop or start a bot. Restarting a live bot mid-session is an operator
#     decision, never a deploy step.
#   - overwrite an existing account.env, which names a live account's
#     whitelisted IP.
set -u

cd "$(dirname "$0")/.." || exit 1
REPO="$(pwd)"
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

PY="$REPO/env/bin/python"
[ -x "$PY" ] || PY="$(command -v python3)"

if [ -t 1 ]; then G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; D=$'\033[2m'; Z=$'\033[0m'
else G=""; Y=""; R=""; D=""; Z=""; fi
step() { printf '\n%s── %s%s\n' "$D" "$1" "$Z"; }
say()  { printf '  %s\n' "$1"; }
did()  { printf '  %s%s%s\n' "$G" "$1" "$Z"; }
warn() { printf '  %s%s%s\n' "$Y" "$1" "$Z"; }
die()  { printf '  %s%s%s\n' "$R" "$1" "$Z"; exit 1; }

run() {
  if [ "$APPLY" = "1" ]; then "$@"; else say "would run: $*"; fi
}

[ "$APPLY" = "1" ] || printf '%sDry run — nothing will change. Add --apply to act.%s\n' "$Y" "$Z"

# ── 1. pull ──────────────────────────────────────────────────────────────────
step "Update"
before="$(git rev-parse HEAD)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  # A deploy from a dirty tree is not the deploy anyone reviewed. A dry run
  # should still show the plan, though — refusing to describe what would happen
  # is not the same as refusing to do it.
  if [ "$APPLY" = "1" ]; then
    die "working tree has uncommitted changes — commit or stash first"
  fi
  warn "working tree has uncommitted changes — --apply would refuse"
fi
if [ "$APPLY" = "1" ]; then
  git pull --ff-only || die "pull failed — resolve by hand, never force on a live host"
else
  git fetch --quiet >/dev/null 2>&1 || warn "could not reach the remote — comparing against the last fetch"
fi
after="$(git rev-parse HEAD)"
target="$(git rev-parse '@{u}' 2>/dev/null || echo "$after")"

changed=""
if [ "$before" = "$target" ]; then
  say "already at $(git rev-parse --short HEAD) — nothing new"
elif git merge-base --is-ancestor "$target" "$before" 2>/dev/null; then
  # Local is ahead of the remote. Deploying would be a no-op, but silently
  # printing "abc → def" with an older def reads like a downgrade.
  warn "local is ahead of origin by $(git rev-list --count "$target".."$before") commit(s) — push first"
else
  changed="$(git diff --name-only "$before" "$target")"
  say "$(git rev-parse --short "$before") → $(git rev-parse --short "$target")"
  say "$(printf '%s' "$changed" | grep -c . ) file(s) changed"
fi

touches() { printf '%s' "$changed" | grep -q "$1"; }

# ── 2. accounts ──────────────────────────────────────────────────────────────
step "Accounts"
"$PY" deploy/accounts.py || die "cannot resolve accounts"
if [ "$APPLY" = "1" ]; then
  "$PY" deploy/onboard.py --apply || warn "onboarding reported a problem — see above"
else
  "$PY" deploy/onboard.py || true
fi

# ── 3. units ─────────────────────────────────────────────────────────────────
step "Units"
run "$PY" deploy/gen_systemd_units.py
agents_changed=0
units_changed=0
for f in deploy/systemd/generated/agent-*.service; do
  [ -f "$f" ] || continue
  installed="/etc/systemd/system/$(basename "$f")"
  if [ ! -f "$installed" ] || ! cmp -s "$f" "$installed"; then
    warn "$(basename "$f") differs from what is installed"
    run cp "$f" "$installed"
    agents_changed=1
    units_changed=1
  fi
done
# Bot units are copied so the files stay current, but their enable state is
# never touched — see the header.
for f in deploy/systemd/generated/bot-*.service; do
  [ -f "$f" ] || continue
  installed="/etc/systemd/system/$(basename "$f")"
  if [ -f "$installed" ] && ! cmp -s "$f" "$installed"; then
    warn "$(basename "$f") differs from what is installed (copying; enable state untouched)"
    run cp "$f" "$installed"
    units_changed=1
  fi
done
# Reload for ANY unit change, not just an agent's: a copied bot unit that
# systemd has not re-read is still the old one as far as it is concerned.
[ "$units_changed" = "1" ] && run systemctl daemon-reload
[ "$units_changed" = "1" ] || say "installed units already match"

# ── 4. restart what changed ──────────────────────────────────────────────────
step "Agents"
if [ -z "$changed" ] && [ "$agents_changed" = "0" ]; then
  say "nothing the agents run has changed — leaving them alone"
else
  if touches '^webapp/' || [ "$agents_changed" = "1" ]; then
    units="$(ls deploy/systemd/generated/agent-*.service 2>/dev/null \
             | xargs -n1 basename | sed 's/\.service$//' | tr '\n' ' ')"
    did "restarting: $units"
    run systemctl restart $units
  else
    say "no change under webapp/ — agents left running"
  fi
fi

if touches '^deploy/cron/'; then
  # Nothing to restart: cron re-reads these from disk each run. Worth saying so,
  # because "I did not restart anything" and "it did not take effect" look alike.
  say "deploy/cron/ changed — cron picks these up on its next run, no restart needed"
fi

# ── 5. verify ────────────────────────────────────────────────────────────────
step "Preflight"
if [ "$APPLY" = "1" ]; then
  sleep 3    # let the agents finish their first poll before asking about them
fi
"$REPO/deploy/preflight.sh"
status=$?

step "Store"
"$PY" -m webapp.store.status || true

exit $status
