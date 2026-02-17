from __future__ import annotations

import re
from typing import Tuple

try:
    import sqlalchemy
except Exception:  # pragma: no cover
    sqlalchemy = None  # type: ignore

def connect_to_traderealm_db(db_info_file: str, db_name_default: str = "traderealm"):
    """Return a SQLAlchemy connection for db info file.

    File content (whitespace separated):
        host user password [dbname]
    """
    if sqlalchemy is None:
        raise ImportError("sqlalchemy is required for auth.user_id. Install: pip install sqlalchemy psycopg2-binary")
    with open(db_info_file, mode="r", encoding="utf-8") as f:
        parts = f.read().split()
    if len(parts) < 3:
        raise ValueError(f"DB info file {db_info_file!r} must contain: host user password [dbname]")
    host, user, password = parts[0], parts[1], parts[2]
    dbname = parts[3] if len(parts) >= 4 else db_name_default

    from urllib import parse as _parse
    engine = sqlalchemy.create_engine(f"postgresql://{user}:{_parse.quote_plus(password)}@{host}/{dbname}")
    return engine.connect()

def get_fyers_creds_from_db(
    user_id: int,
    *,
    db_info_file: str,
    db_name: str = "traderealm",
    table_name: str = "nse_usercredential",
) -> Tuple[str, str]:
    """Fetch (api_key, access_token) from DB."""
    if sqlalchemy is None:
        raise ImportError("sqlalchemy is required for auth.user_id. Install: pip install sqlalchemy psycopg2-binary")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Unsafe table name: {table_name!r}")

    with connect_to_traderealm_db(db_info_file, db_name_default=db_name) as conn:
        q = sqlalchemy.text(f"SELECT api_key, access_token FROM {table_name} WHERE id=:id LIMIT 1")
        rows = conn.execute(q, {"id": int(user_id)}).fetchall()
        if not rows:
            raise RuntimeError(f"No FYERS creds found in {table_name} for id={user_id}")
        api_key, access_token = rows[0][0], rows[0][1]
        return str(api_key), str(access_token)
