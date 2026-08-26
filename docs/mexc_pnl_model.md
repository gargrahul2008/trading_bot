# MEXC PnL & Portfolio Accounting — How It Works

_Last updated 2026-08-13. Run `env/bin/python scripts/mexc_audit.py` any time to verify no gaps._

## 1. Account structure

One MEXC spot account holds **ETH + USDC**, partitioned into independent pieces:

| Piece | What it is | Tracked in |
|---|---|---|
| **Bucket 1** | 3% grid, proactive | `state/bucket1/state_*_v1.json` |
| **Bucket 2** | 10% wide grid, reactive | `state/bucket2/state_*_v1.json` |
| **Bucket 3** | 2% tight grid, proactive | `state/bucket3/state_*_v1.json` |
| **HODL** | 17.709 ETH held outside all bots | hardcoded constant (no state) |

Each bucket runs with `isolated_cash: true` + `adopt_broker_inventory: false`, so it **only trades and reports on its own slice** — it never reads the account's total balance. The buckets are just an *accounting partition* of the one real account.

## 2. The two reconciliation invariants (must always hold)

1. **ETH:**  `Σ bucket.traded_qty + HODL_ETH  ==  live account ETH`
2. **USDC:** `Σ bucket.cash                ==  live account USDC`

If either breaks, the buckets collectively claim more/less than the account holds → sell orders get rejected, or PnL is double-counted. The audit checks both against the live balance.

## 3. How PnL is defined (the gap-proof definition)

**PnL = realized + unrealized**, grounded in the **lots** (cost basis):

- `unrealized = traded_qty × price − Σ(lot_qty × lot_price)`
- `realized`  = accumulated from sells (LIFO: `sell_qty × (sell_price − lot_price)`)

This can't have gaps because it's grounded in the lots, which reconcile to the real inventory.

### The baseline invariant

The Telegram/dashboard report a **Net = PV − `portfolio_start_value`**. For that to equal `realized + unrealized`, the baseline MUST be:

```
portfolio_start_value  =  cash + Σ(lot_qty × lot_price) − realized_pnl     (= "invested")
```

**This quantity is CONSERVED by every grid trade** (a buy's cash-out equals its added lot cost; a sell's cash-in equals `realized + reduced lot cost`). So once you set it correctly, it stays correct forever — *unless a manual state edit changes cash/lots/realized without updating it*. That is the root cause of every PnL gap we've hit.

## 4. `traded_qty` is DERIVED from `lots`

Bucket PnL uses **LIFO lot accounting**. `traded_qty` = `Σ lot_qty`. The engine **recomputes `traded_qty` from the lots on the first fill after a restart/edit**. Therefore:

> ⚠️ To change a bucket's ETH, edit the **`lots` array**, not just `traded_qty`. Editing `traded_qty` alone silently reverts on the next fill.

## 5. Seed inventory (a bucket funded with ETH, not grid-bought)

If a bucket is *seeded* with ETH (like bucket 3 got 19.71 ETH), three things must be set to its **real cost**, or PnL mis-states:

1. **State `lots`** at real cost → correct realized/unrealized + dashboard.
2. **`portfolio_start_value`** = cash + lot_cost − realized → correct Net/H in Telegram.
3. **Telegram report `--initial-eth <qty> --initial-cost <cost>`** (wired via `run_report` args 8/9 in `mexc_telegram_cron.sh`) → so sells of the seed match against it and **book PnL** (otherwise the seed sells show $0 and land in "Hidden losses").

`compute_metrics` in `mexc_telegram_report.py` seeds `open_buys` + `grid_seq` from `_initial_eth`/`_initial_cost`.

## 6. Moving capital between buckets (the CORRECT procedure)

Transferring ETH/cash from bucket A → bucket B is NOT a real capital flow (money stays in the account). To keep PnL conserved, adjust **both** baselines:

- **Bucket A (out):** remove the lots (real cost) + reduce cash → then reset `portfolio_start_value = cash + lot_cost − realized`.
- **Bucket B (in):** add the lots at the **same cost basis** they left A + add cash → set `portfolio_start_value = cash + lot_cost − realized`.

If you skip A's baseline reset, A shows a **phantom loss** (its baseline still counts the moved capital). If you skip B's, B shows a phantom loss/gain. Both bugs happened in the bucket1→bucket3 transfer (2026-07-31) and were fixed 2026-08-13.

## 7. Safe bucket stop/edit/restart

1. `crontab -e` → comment out the `mexc_watchdog.sh` line (it auto-restarts down bots every minute).
2. Find PIDs: `ps -eo pid,cmd | grep bucketN_runner` (kill by **PID**, not string — `pkill` matches your own shell).
3. `kill -9 <runner_pid>` (stops the respawn loop), then `kill -INT <bot_pid>` (graceful).
4. Edit the state **only after full stop** (shutdown-flush race).
5. Restart via `nohup scripts/mexc_bucketN_runner.sh >> logs/mexc_bucketN_runner.log 2>&1 &`.
6. Re-enable the watchdog cron.
7. Run `scripts/mexc_audit.py` to confirm green.

## 8. The audit script

`scripts/mexc_audit.py` checks, against the live account: ETH recon, USDC recon, the baseline invariant per bucket, lot integrity, and prints the total PnL. **Exit 0 = all green.** Set `HODL_COST_PRICE` at the top to include HODL PnL in the account total.

## 9. Everything wired for a bucket (checklist when adding one)

- [ ] config `config.mexc.bucketN.json`, runner `mexc_bucketN_runner.sh`
- [ ] added to `mexc_watchdog.sh`
- [ ] state seeded with **real-cost lots** + `portfolio_start_value` = cash + lot_cost − realized
- [ ] dashboard `BUCKETS` dict (`mexc_dashboard_page.py`)
- [ ] `mexc_telegram_cron.sh` `run_report` (with `--initial-eth/--initial-cost` if seeded)
- [ ] `mexc_alerts.py` `BUCKETS` dict
- [ ] `scripts/mexc_audit.py` `BUCKETS` list
- [ ] run the audit → green
