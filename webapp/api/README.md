# Dashboard API

Serves the dashboard. Holds **no broker credentials and never calls Fyers** —
every figure comes from an account agent over loopback (see
`webapp/agent/README.md` for why the brokers are reached that way).

## Its own virtualenv, on purpose

The agents run inside the trading virtualenv because they import
`common.broker`. This does not: it is a web app, and the environment that places
real orders should not grow a web framework's dependency tree.

```bash
cd /root/trading_bot
python3 -m venv webapp/api/.venv
webapp/api/.venv/bin/pip install -r webapp/api/requirements.txt
```

FastAPI runs on the host's Python 3.9, so no newer interpreter is needed.

## Configuration

Two secrets, both in gitignored files. `AGENT_TOKEN` in `webapp/agent.env` is
already there from the agents — the API reads the same one.

Set the dashboard password interactively:

```bash
webapp/api/.venv/bin/python webapp/api/set_password.py
```

It prompts, so the password never appears in a command line — and therefore
never in shell history, in `ps`, or in a scrolled-back terminal. It writes
`webapp/dashboard.env` (mode 0600), generating a `SESSION_SECRET` the first
time and keeping anything already there.

Set `COOKIE_SECURE=true` in that file once the app is behind TLS. Until then it
stays `false`, and the app belongs behind an SSH tunnel: without the Secure
flag the session cookie would travel in clear.

The password hash is full of `$`, which every layer that carries an environment
variable treats as a variable reference and silently blanks — leaving a
truncated hash that can never match any password. Keeping it in a file avoids
that entirely.

## Running it

Build the UI once, then start one server — it serves both the API and the
compiled frontend, so there is one process, one port and one SSH tunnel:

```bash
cd webapp/web && npm install && npm run build && cd -
webapp/api/.venv/bin/uvicorn app.main:app --app-dir webapp/api --port 8000
```

Same single origin as production, so the session cookie behaves identically in
development. Without a build, `/` says so and points at `/docs`.

From your laptop:

```bash
ssh -L 8000:localhost:8000 root@64.227.135.117
```

then open <http://localhost:8000>.

`GET /api/health` needs no login and reports whether the pieces are wired up —
point a monitor at it. It deliberately exposes no account figures.

## One rule shapes this code

**An unreachable account must not cost you the others.** Agent calls fan out
concurrently on a short timeout, and a failure comes back as data — that
account's row is present, named, and flagged — rather than as an exception. A
page showing five accounts and saying the sixth is unreachable is useful; an
error page because one agent is restarting is not. `totals` reports
`accounts_missing` for the same reason: a total that quietly omits an account is
a wrong number presented as a right one.

## Endpoints

| Method | Path | |
|---|---|---|
| `POST` | `/api/auth/login` `/api/auth/logout` | session cookie, 4h idle timeout |
| `GET` | `/api/auth/me` | is this session still valid |
| `GET` | `/api/overview` | every account on one screen |
| `GET` | `/api/accounts` | account names and their agent ports |
| `GET` | `/api/accounts/{account}/{section}` | one section straight from its agent |
| `GET` | `/api/health` | wiring check, no login, no figures |

## Tests

```bash
webapp/api/.venv/bin/python -m pytest tests/webapp/api -q
```

Stand-in agents on real sockets cover the aggregation, a dead agent, and a
wedged one.
