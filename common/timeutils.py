from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

def parse_hhmm(s: str) -> dt.time:
    parts = [int(x) for x in s.strip().split(":")]
    if len(parts) != 2:
        raise ValueError(f"Invalid time {s!r}. Use HH:MM")
    return dt.time(parts[0], parts[1], 0)

def parse_hhmmss(s: str) -> dt.time:
    parts = [int(x) for x in s.strip().split(":")]
    if len(parts) == 2:
        return dt.time(parts[0], parts[1], 0)
    if len(parts) == 3:
        return dt.time(parts[0], parts[1], parts[2])
    raise ValueError(f"Invalid time {s!r}. Use HH:MM or HH:MM:SS")

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def local_dt_for_today(tz_name: str, t: dt.time) -> dt.datetime:
    tz = ZoneInfo(tz_name)
    now = dt.datetime.now(tz)
    return dt.datetime(now.year, now.month, now.day, t.hour, t.minute, t.second, tzinfo=tz)

def iso_utc(dtobj: dt.datetime) -> str:
    if dtobj.tzinfo is None:
        dtobj = dtobj.replace(tzinfo=dt.timezone.utc)
    return dtobj.astimezone(dt.timezone.utc).isoformat()

def parse_iso(s: str) -> dt.datetime:
    x = dt.datetime.fromisoformat(s)
    if x.tzinfo is None:
        x = x.replace(tzinfo=dt.timezone.utc)
    return x
