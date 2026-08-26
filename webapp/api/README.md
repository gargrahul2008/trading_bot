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

Two secrets, both in gitignored files next to the agents':

```bash
# webapp/agent.env   — already created for the agents; the API reads the same one
AGENT_TOKEN=...

# webapp/dashboard.env
DASHBOARD_PASSWORD_HASH=$2b$12$...
SESSION_SECRET=...
COOKIE_SECURE=true          # false only for local http
```

Generate them:

```bash
webapp/api/.venv/bin/python -c \
  "import sys; sys.path.insert(0,'webapp/api'); from app.auth import hash_password; \
   print('DASHBOARD_PASSWORD_HASH=' + hash_password('your-password'))" >> webapp/dashboard.env
python3 -c "import secrets; print('SESSION_SECRET=' + secrets.token_urlsafe(32))" >> webapp/dashboard.env
chmod 600 webapp/dashboard.env
```

The hash is full of `$`, which every layer that carries an environment variable
treats as a variable reference and silently blanks. Keeping it in a file avoids
that entirely.

## Running it

```bash
webapp/api/.venv/bin/uvicorn app.main:app --app-dir webapp/api --port 8000
```

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
