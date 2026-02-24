from __future__ import annotations
import datetime as dt
from decimal import Decimal
from zoneinfo import ZoneInfo
from typing import Optional

from common.broker.interfaces import Broker, BrokerError, to_decimal
from common.utils.logger import setup_logger

LOG = setup_logger("anchors")

def fetch_prev_close(
    broker: Broker,
    *,
    symbol: str,
    market_tz: str = "Asia/Kolkata",
    lookback_days: int = 10,
) -> Decimal:
    """Fetch previous trading day's close using broker.history() with daily resolution.
    - Works for FYERS (and any broker that implements history in FYERS-like candle format).
    - Returns Decimal close price.
    - Raises BrokerError if no suitable candle.
    """
    tz = ZoneInfo(market_tz)
    today_local = dt.datetime.now(tz).date()
    start = today_local - dt.timedelta(days=max(int(lookback_days), 3))
    end = today_local

    data = {
        "symbol": symbol,
        "resolution": "D",
        "date_format": "1",
        "range_from": start.isoformat(),
        "range_to": end.isoformat(),
        "cont_flag": "1",
    }
    resp = broker.history(data)
    candles = []
    if isinstance(resp, dict):
        candles = resp.get("candles") or []
    if not isinstance(candles, list) or not candles:
        raise BrokerError(f"History returned no candles for {symbol}", resp=resp)

    # candles: [ts, o, h, l, c, v]
    best_dt: Optional[dt.datetime] = None
    best_close: Optional[Decimal] = None

    for c in candles:
        if not isinstance(c, (list, tuple)) or len(c) < 5:
            continue
        try:
            ts = int(c[0])
            close = to_decimal(c[4])
        except Exception:
            continue
        ts_dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        ts_local_date = ts_dt_utc.astimezone(tz).date()
        if ts_local_date >= today_local:
            continue
        if best_dt is None or ts_dt_utc > best_dt:
            best_dt = ts_dt_utc
            best_close = close

    if best_close is None:
        raise BrokerError(f"No previous-close candle found for {symbol} in {start}..{end}", resp=resp)

    return best_close
