#!/bin/bash
# Daily live-vs-backtest performance comparison report
#
# DISABLED 2026-05-28: The backtest comparison was built for the old single-bot
# config (symmetric 0.4% grid with fixed_quote sizing). The new 2-bucket design
# uses fixed_qty mode and asymmetric runway for Bucket 1, plus a separate wide
# upside grid for Bucket 2. Direct comparison would produce misleading numbers
# until the backtest script understands the new structure.
#
# To re-enable: extend scripts/mexc_backtest_compare.py to support fixed_qty
# sizing and asymmetric upper_pct / lower_pct, then point --config at the
# relevant bucket config.

echo "[$(date '+%Y-%m-%d %H:%M:%S')] mexc_compare_cron disabled — see script header for context"
exit 0
