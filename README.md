# Trading Bot Codebase (FYERS)

This repo contains:
- `common/`: shared FYERS broker adapter, DB credentials loader, inventory helpers, reject parsing, JSON state persistence.
- `strategies/`: each strategy lives in its own folder with `strategy.py`, `config.json`, and `README.md`.
- `run.py`: generic runner for **reactive** (tick-driven) and **proactive** (pre-place orders then react to fills) styles.

## Quick start (ladder)

1) Copy the example config:

```bash
cp strategies/pct_ladder/config.example.json strategies/pct_ladder/config.json
```

2) Run live:

```bash
python run.py live --config strategies/pct_ladder/config.json
```

3) State/trades are written where `paths.*` in config points.

> Credentials: use either `auth.user_id` (fetch from Postgres via `tr_db`) **or** set `FYERS_CLIENT_ID` and `FYERS_ACCESS_TOKEN` env vars.
