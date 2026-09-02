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

# ── is what is running older than the code on disk? ──────────────────────────
#
# Asked of the filesystem, never of "what did this invocation pull". Pulling by
# hand and then running this script is a normal thing to do, and a deploy that
# decides what to restart from its own git diff does nothing at all in that
# case — while reporting success, which is the failure that hides itself.

newest() {   # newest mtime, in epoch seconds, under the given paths
  find "$@" -type f -not -path '*/node_modules/*' -not -name '*.pyc' \
       -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1
}

started() {  # when a unit last entered active, in epoch seconds; 0 if it is not
  stamp="$(systemctl show -p ActiveEnterTimestamp --value "$1" 2>/dev/null)"
  [ -n "$stamp" ] && date -d "$stamp" +%s 2>/dev/null || echo 0
}

stale() {    # unit $1 is running code older than the newest file under $2...
  unit="$1"; shift
  since="$(started "$unit")"
  [ "$since" = "0" ] && return 1          # not running: not this check's problem
  code="$(newest "$@")"
  [ -n "$code" ] && [ "$code" -gt "$since" ]
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
dashboard_changed=0
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
# The dashboard runs the same webapp/ code as the agents, so it is installed and
# restarted on the same terms. Unlike the bots it is meant to be enabled.
f="deploy/systemd/generated/dashboard.service"
if [ -f "$f" ]; then
  installed="/etc/systemd/system/dashboard.service"
  if [ ! -f "$installed" ]; then
    warn "dashboard.service not installed — see docs/dashboard_https.md"
    run cp "$f" "$installed"
    units_changed=1; dashboard_changed=1
  elif ! cmp -s "$f" "$installed"; then
    warn "dashboard.service differs from what is installed"
    run cp "$f" "$installed"
    units_changed=1; dashboard_changed=1
  fi
fi

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

# ── 4. build the UI ──────────────────────────────────────────────────────────
#
# webapp/web/dist is not in git, so a pull never updates it. Without this the
# API restarts happily and keeps serving the bundle built last time — the deploy
# reports success and the browser shows the old dashboard, which is the worst
# combination of the two.
step "Dashboard UI"
UI_SRC="webapp/web/src webapp/web/index.html webapp/web/package.json webapp/web/package-lock.json"
ui_stale=0
if [ ! -f webapp/web/dist/index.html ]; then
  ui_stale=1
else
  built="$(stat -c %Y webapp/web/dist/index.html 2>/dev/null || echo 0)"
  src="$(newest $UI_SRC)"
  [ -n "$src" ] && [ "$src" -gt "$built" ] && ui_stale=1
fi

if [ ! -d webapp/web ]; then
  say "no webapp/web — nothing to build"
elif [ "$ui_stale" = "0" ]; then
  say "the built bundle is newer than every source file — nothing to build"
elif ! command -v npm >/dev/null 2>&1; then
  warn "npm not found — the dashboard will serve whatever was built last"
else
  # npm ci, not install: the lockfile is what was tested, and a deploy is not
  # the place to discover a resolved dependency has moved.
  run sh -c 'cd webapp/web && npm ci --silent && npm run build'
  if [ "$APPLY" = "1" ] && [ ! -f webapp/web/dist/index.html ]; then
    die "the UI build produced no dist/index.html — not restarting onto a broken bundle"
  fi
  [ "$APPLY" = "1" ] && did "built webapp/web/dist"
fi

# ── 5. restart what changed ──────────────────────────────────────────────────
step "Agents"
# An agent restart drops a poll and reloads a token, so it is worth doing only
# for code an agent actually runs. Everything under webapp/web/ is served by the
# dashboard and never imported by an agent — restarting them for a changed
# button is a cost paid for nothing, and during market hours it is a real one.
AGENT_CODE="webapp/agent webapp/store webapp/pnl webapp/history"
units="$(ls deploy/systemd/generated/agent-*.service 2>/dev/null \
         | xargs -n1 basename | sed 's/\.service$//' | tr '\n' ' ')"
agents_stale=0
for unit in $units; do
  stale "$unit" $AGENT_CODE && agents_stale=1
done

if [ "$agents_stale" = "1" ] || [ "$agents_changed" = "1" ]; then
  did "restarting: $units"
  run systemctl restart $units
else
  say "every agent is running the code on disk — leaving them polling"
fi

step "Dashboard"
# The dashboard serves both the API and the built bundle, so any change under
# webapp/ is a reason to restart it — including one only the UI cares about.
if ! systemctl is-enabled dashboard.service >/dev/null 2>&1; then
  if [ "$dashboard_changed" = "1" ]; then
    warn "dashboard.service installed but not enabled — systemctl enable --now dashboard"
  else
    say "dashboard.service not enabled — see docs/dashboard_https.md"
  fi
elif stale dashboard.service webapp || [ "$units_changed" = "1" ] || [ "$ui_stale" = "1" ]; then
  did "restarting: dashboard"
  run systemctl restart dashboard
else
  say "the dashboard is running the code on disk — left alone"
fi

if touches '^deploy/cron/'; then
  # Nothing to restart: cron re-reads these from disk each run. Worth saying so,
  # because "I did not restart anything" and "it did not take effect" look alike.
  say "deploy/cron/ changed — cron picks these up on its next run, no restart needed"
fi

# ── 6. verify ────────────────────────────────────────────────────────────────
step "Preflight"
if [ "$APPLY" = "1" ]; then
  sleep 3    # let the agents finish their first poll before asking about them
fi
"$REPO/deploy/preflight.sh"
status=$?

step "Trading"
# Arming is the one deploy-time decision that can lose money, so it is stated
# on every run rather than left to be inferred from a unit file.
if [ -s deploy/trading_enabled ]; then
  warn "TRADING ENABLED for: $(tr '\n' ' ' < deploy/trading_enabled | sed 's/ *$//')"
  say "these agents can place real orders — deploy/trading_enabled is what says so"
else
  say "every agent is read-only (no deploy/trading_enabled)"
fi

step "Store"
"$PY" -m webapp.store.status || true

exit $status
