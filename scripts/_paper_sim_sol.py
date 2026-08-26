"""Replay the REAL FibLiveBot.on_bar (full-day windows, current fixed engine) over
Jan1->Jul28 on Binance ETH, and diff vs the IntradayBacktester. No live logs exist
for this period, so this simulates what paper WOULD have produced."""
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
for p in (str(ROOT/"src"), str(ROOT), str(ROOT/"scripts")):
    if p not in sys.path: sys.path.insert(0, p)
import pandas as pd, numpy as np
from datetime import time as dtime
from live_fib_bot import FibLiveBot
from intraday_research.backtester import BacktestConfig, IntradayBacktester
from intraday_research.features import FeatureEngine
from intraday_research.provider import MarketDataProvider
from intraday_research.risk import RiskManager
from intraday_research.strategies import NiftyBOSFibScalpStrategy
from intraday_research.types import InstrumentSpec
from intraday_research.costs import MEXC_ZERO_COSTS

print("loading SOL Jan1->Jul28 ...", flush=True)
prov = MarketDataProvider(repo_root=ROOT, verbose=False)
df = prov.load('SOLUSDT','crypto','2026-01-01','2026-07-28').copy()
df['timestamp'] = df['timestamp'].dt.tz_convert('UTC'); df['trade_date'] = df['timestamp'].dt.date.astype(str)
by = {d: g['close'].to_numpy() for d,g in df.groupby('trade_date',sort=True)}; ds = sorted(by)
ref = {d: float(np.median(np.concatenate([by[p] for p in ds[max(0,i-7):i]]) if i>=1 else by[d])) for i,d in enumerate(ds)}
warm = set(ds[:7])                       # first 7 dates = warmup (compare only after)
print(f"  {len(df):,} bars, {len(ds)} days", flush=True)

# ---- 1) PAPER ENGINE simulation (real on_bar, full-day windows, median qty) ----
cfg = {"symbols":["SOLUSDT"], "trade_value_usd":5000, "entry_next_bar_open":True,
       "strategy":{"variant":"BASE","min_impulse_pct":0.25,"target_ext":1.618,
                   "stop_buffer_pct":0.009,"use_trailing_stop":True,"stop_buffer_floor":0.05},
       "scaling":{"mode":"lookback","lookback_days":7,"refresh":"daily_utc"},
       "telegram":{"enabled":False}, "output_dir":"artifacts/_papersim_sol",
       "dual_feed":{"enabled":False}}
bot = FibLiveBot(cfg, paper=True)
def _ensure(sym, now_utc):                # no-network ref median from precompute
    d = str(now_utc.date()); bot.ref_median[sym] = ref.get(d); bot.ref_median_day[sym] = d
bot._ensure_ref_median = _ensure
print("replaying paper engine ...", flush=True)
for k,d in enumerate(ds):
    day = df[df['trade_date']==d].reset_index(drop=True)
    for i in range(len(day)):
        bot.on_bar('SOLUSDT', day.iloc[:i+1])
    if k % 20 == 0: print(f"  day {k}/{len(ds)} {d}", flush=True)
ptr = bot.paper_trades['SOLUSDT']
Pset = {(str(t['entry_bar'])[:16], t['side'], t['reason']) for t in ptr
        if not t['is_partial'] and str(t['entry_bar'])[:10] not in warm}
Ppnl = sum(t['gross_pnl'] for t in ptr if str(t['entry_bar'])[:10] not in warm)

# ---- 2) IntradayBacktester (notebook config, capital_based) ----
print("running IntradayBacktester ...", flush=True)
mi = {d: round(m*0.25/100,4) for d,m in ref.items()}; sb = {d: round(max(m*0.009/100,0.05),4) for d,m in ref.items()}
for d in ds[:7]: mi[d] = float('inf')
q0 = round(5000/ref[ds[7]],6)
s = NiftyBOSFibScalpStrategy(name="BOS_FIB_BASE",pivot_lookback=2,min_swing_points=2,fib_zone_low=0.50,fib_zone_high=0.618,
    stop_buffer_points=0.01,min_impulse_points=0.1,max_hold_bars=15,max_trades_per_day=500,max_consecutive_losses_per_day=200,
    entry_mode="limit_618",min_confirmation_rr=0.0,target_extension_ratio=1.618,min_impulse_by_date=mi,stop_buffer_by_date=sb)
e = IntradayBacktester(strategies=[s],feature_engine=FeatureEngine(),risk_manager=RiskManager(max_positions=1,max_daily_loss=50000,quantity_per_trade=q0),cost_model=MEXC_ZERO_COSTS)
r = e.run(df, BacktestConfig(output_dir="artifacts/_papersim_sol_bt",instrument_specs={'SOLUSDT':InstrumentSpec(quantity_mode="units",lot_size=1,instrument_type="equity")},
    initial_capital=1_000_000,max_positions=1,max_daily_loss=50000,quantity_per_trade=q0,sizing_mode="capital_based",capital_per_trade_amount=5000.0,
    first_trade_time=dtime(0,0),last_entry_time=dtime(23,59),session_end=dtime(23,59),time_exit_minutes=90,continuous_session=True,
    entry_max_bars_wait=20,entry_next_bar_open=True,use_trailing_stop=True))
t = r['trades'].copy(); t['ut'] = pd.to_datetime(t['entry_time']).dt.tz_convert('UTC')
fin = (t[~t['is_partial']] if 'is_partial' in t.columns else t)
Bset = {((x['ut']-pd.Timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M'), x['side'], x['exit_reason'])
        for _,x in fin.iterrows() if str((x['ut']).date()) not in warm}
Bpnl = t[~t['ut'].dt.date.astype(str).isin(warm)]['net_pnl'].sum()

out = {"paper_trades":len(Pset),"bt_trades":len(Bset),"matched":len(Pset&Bset),
       "only_paper":len(Pset-Bset),"only_bt":len(Bset-Pset),
       "paper_pnl":round(Ppnl,1),"bt_pnl":round(float(Bpnl),1)}
Path("artifacts/_papersim_sol").mkdir(parents=True, exist_ok=True)
json.dump(out, open("artifacts/_papersim_sol/result.json","w"), indent=2)
print("RESULT", json.dumps(out), flush=True)
