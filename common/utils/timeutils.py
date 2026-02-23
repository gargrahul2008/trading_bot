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

def now_local(tz_name: str) -> dt.datetime:
    return dt.datetime.now(ZoneInfo(tz_name))

def to_utc_iso(x: dt.datetime) -> str:
    if x.tzinfo is None:
        x = x.replace(tzinfo=dt.timezone.utc)
    return x.astimezone(dt.timezone.utc).isoformat()

def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)
