# Modular FYERS Strategy Runner (Common Broker + Per-Strategy Folders)

## What you get
- `common/` shared layer:
  - FYERS client wrapper (quotes/orders/orderbook/positions/holdings/funds/history)
  - DB auth helper (reads `tr_db`)
  - JSON auth helper (for small multi-user local setups without DB)
  - Sellable quantity computation with **T1/BTST auto**:
    - Treat T1 as sellable automatically for `NSE:* -EQ` and `BSE:* -A` (BTST eligible)
  - Reject parser + adaptive SELL qty retry on "insufficient qty/holdings" type rejects

- `strategies/` per-strategy folders:
  - each strategy has `strategy.py`, config, README
  - examples:
    - `pct_ladder` (reactive; market orders)
    - `order_grid_template` (managed; pre-place limit orders)

## Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run example (pct ladder)
1) Copy/modify config:
`strategies/pct_ladder/config.example.json`

2) Ensure `tr_db` file is present (for DB auth), or switch broker.auth_mode to `env`/`json`.

3) Run:
```bash
python run_strategy.py --config strategies/pct_ladder/config.example.json
```

## FYERS JSON auth
- Use this if you have a small fixed set of FYERS users and do not want any database.
- Create `fyers_auth.json` from `fyers_auth.example.json`.
- Add your 4 users with `client_id`, `secret_key`, and the registered `redirect_uri`.
- Create `dashboard_access.json` from `dashboard_access.example.json`.
- Start the multipage dashboard:
```bash
streamlit run dashboard/streamlit_app.py
```
- By default the app opens on `FYERS Auth`.
- The other page is named `Dashboard`.
- Access is controlled by `dashboard_access.json` with a 10-minute session by default.
- You can define different passwords for different pages:
  - a password with `["dashboard"]` opens only the MEXC dashboard
  - a password with `["fyers-auth"]` opens only the FYERS auth page
  - a password with `["dashboard", "fyers-auth"]` opens both pages
- Only pages allowed by the entered password are shown in the sidebar.
- For minimum interaction, set each FYERS `redirect_uri` to the exact FYERS auth page URL.
  Example: if Streamlit is running at `http://127.0.0.1:8501`, register `http://127.0.0.1:8501/fyers-auth`.
- Then the flow is:
  - click `Generate`
  - if the saved token is still valid, nothing else is needed
  - if login is required, the page redirects to FYERS
  - after you finish FYERS login, the browser returns to the auth page and the token is saved automatically
- Use a strategy config with:
```json
"broker": {
  "type": "fyers",
  "auth_mode": "json",
  "auth_file": "../../fyers_auth.json",
  "user_key": "user1"
}
```
- Example config: `strategies/pct_ladder/config.fyers.json_auth.example.json`
- The auth page stores the latest `access_token` back into `fyers_auth.json`, and `run_strategy.py` reads from that file directly.

### Dashboard access file
Example `dashboard_access.json`:
```json
{
  "session_ttl_seconds": 600,
  "passwords": [
    {
      "label": "dashboard-only",
      "password": "dashboard123",
      "pages": ["dashboard"]
    },
    {
      "label": "fyers-only",
      "password": "fyers123",
      "pages": ["fyers-auth"]
    },
    {
      "label": "full-access",
      "password": "admin123",
      "pages": ["dashboard", "fyers-auth"]
    }
  ]
}
```
- `session_ttl_seconds` controls how long one login stays active in the browser session.
- The real `dashboard_access.json` is ignored by git, so you can keep actual passwords locally.
- Optional fallback: if `dashboard_access.json` is missing, the app still accepts `STREAMLIT_APP_PASSWORD` as a full-access password for both pages.

## Proactive strategies
For strategies that pre-place orders and react to fills:
- set `"runner_type": "managed"`
- implement `desired_actions()` in strategy

If you need something more advanced (modify order, complex state machine),
it's fine to keep a custom runner inside the strategy folder while still reusing
`common/` modules.


## Crypto (MEXC Spot)
- Configure broker.type = "mexc_spot" and set broker.secrets_file to a repo-root secrets json.
- Example config: strategies/pct_ladder/config.mexc.example.json
- Reactive runner supports order_mode=marketable_limit with slippage_bps and limit_ttl_seconds.


## Proactive levels from previous close
- Strategy: strategies/prevclose_levels (runner_type: managed)
- Example config: strategies/prevclose_levels/config.fyers.rajoo.example.json

## Dashboard manual positions (persistent)
- You can persist legacy/manual holdings (for adjusted portfolio metrics/curve) using a file.
- Configure optional path in your strategy config:
  - `paths.manual_positions_file`: `"state/manual_positions.json"` (or `.csv`)
- Supported JSON format:
```json
[
  {"ts": "2026-03-01 09:15", "symbol": "BTCUSDT", "qty": 0.25, "buy_price": 58000},
  {"ts": "2026-03-05 14:30 IST", "symbol": "ETHUSDT", "qty": 1.5, "buy_price": 2800}
]
```
- Supported CSV format (header required):
```csv
ts,symbol,qty,buy_price
2026-03-01 09:15,BTCUSDT,0.25,58000
2026-03-05 14:30 IST,ETHUSDT,1.5,2800
```
- `ts` is optional. If timezone is omitted, dashboard treats it as IST.
- If config key is not set, dashboard also auto-detects `manual_positions.json` / `manual_positions.csv` in the selected run folder.

## Daily portfolio curve (dashboard)
- Runner now writes daily files in state/run folder:
  - `pnl_daily.csv` (one finalized point per UTC day)
  - `price_daily.csv` (daily symbol prices for exact adjusted curve)
- Dashboard prefers these daily files, so chart remains fast even when `pnl_points.csv` is very large.
- On first run after upgrade, runner backfills these daily files from existing `pnl_points.csv` / `price_points.jsonl` when possible.

## Capital flows (auto portfolio-start adjustment)
- Optional config path:
  - `paths.capital_flows_file`: `"state/capital_flows.json"` (or `.csv`)
- Dashboard auto-sums capital flows and adjusts portfolio PnL start accordingly.
- Use positive `delta` for add, negative `delta` for withdraw.
- `ts` accepts simple IST datetime (for example `2026-03-09 14:30`), but is currently reference-only for capital-flow PnL math (all `delta` rows are summed).
- JSON example:
```json
[
  {"ts":"2026-03-01T09:00:00Z","delta":5000,"note":"added funds"},
  {"ts":"2026-03-05T13:15:00Z","delta":-1500,"note":"withdrawn"}
]
```
- CSV example:
```csv
ts,delta,note
2026-03-01T09:00:00Z,5000,added funds
2026-03-05T13:15:00Z,-1500,withdrawn
```
