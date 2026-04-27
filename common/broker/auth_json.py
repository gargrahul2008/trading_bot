from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Tuple
from urllib.parse import parse_qs, urlparse

from common.utils.json_store import atomic_write_json, load_json

try:
    from fyers_apiv3 import fyersModel  # type: ignore
except Exception:
    fyersModel = None  # type: ignore


def _require_sdk() -> None:
    if fyersModel is None:
        raise ImportError("Missing dependency fyers-apiv3. Install: pip install fyers-apiv3")


def load_auth_file(path: str) -> Dict[str, Any]:
    raw = load_json(path, default={})
    if not isinstance(raw, dict):
        raise ValueError(f"Auth file must contain a JSON object: {path}")
    users = raw.get("users")
    if users is None:
        raw["users"] = {}
        return raw
    if not isinstance(users, dict):
        raise ValueError(f"Auth file 'users' must be an object: {path}")
    return raw


def save_auth_file(path: str, data: Dict[str, Any]) -> None:
    atomic_write_json(path, data)


def list_fyers_users(path: str) -> Dict[str, Dict[str, Any]]:
    raw = load_auth_file(path)
    out: Dict[str, Dict[str, Any]] = {}
    for user_key, rec in (raw.get("users") or {}).items():
        if isinstance(user_key, str) and isinstance(rec, dict):
            out[user_key] = rec
    return out


def get_user_record(path: str, user_key: str) -> Dict[str, Any]:
    users = list_fyers_users(path)
    rec = users.get(user_key)
    if rec is None:
        raise KeyError(f"User {user_key!r} not found in auth file {path}")
    return rec


def _require_user_fields(rec: Dict[str, Any], *, fields: Tuple[str, ...], user_key: str) -> None:
    missing = [name for name in fields if not str(rec.get(name) or "").strip()]
    if missing:
        raise ValueError(f"User {user_key!r} is missing required fields: {', '.join(missing)}")


def get_fyers_creds_from_json(auth_file: str, *, user_key: str) -> Tuple[str, str]:
    rec = get_user_record(auth_file, user_key)
    _require_user_fields(rec, fields=("client_id", "access_token"), user_key=user_key)
    return str(rec["client_id"]).strip(), str(rec["access_token"]).strip()


def build_fyers_session(auth_file: str, *, user_key: str, state: str | None = None):
    _require_sdk()
    rec = get_user_record(auth_file, user_key)
    _require_user_fields(rec, fields=("client_id", "secret_key", "redirect_uri"), user_key=user_key)
    return fyersModel.SessionModel(
        client_id=str(rec["client_id"]).strip(),
        secret_key=str(rec["secret_key"]).strip(),
        redirect_uri=str(rec["redirect_uri"]).strip(),
        response_type="code",
        grant_type="authorization_code",
        state=state or str(rec.get("state") or f"fyers-auth:{user_key}"),
    )


def generate_login_url(auth_file: str, *, user_key: str, state: str | None = None) -> str:
    session = build_fyers_session(auth_file, user_key=user_key, state=state)
    url = session.generate_authcode()
    raw = load_auth_file(auth_file)
    rec = get_user_record(auth_file, user_key)
    rec["last_login_url_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    rec["state"] = state or str(rec.get("state") or f"fyers-auth:{user_key}")
    raw["users"][user_key] = rec
    save_auth_file(auth_file, raw)
    return str(url)


def exchange_auth_code_for_token(auth_file: str, *, user_key: str, auth_code: str) -> Dict[str, Any]:
    session = build_fyers_session(auth_file, user_key=user_key)
    clean_code = str(auth_code).strip()
    if not clean_code:
        raise ValueError("auth_code is required")
    session.set_token(clean_code)
    resp = session.generate_token()
    if not isinstance(resp, dict) or not str(resp.get("access_token") or "").strip():
        raise RuntimeError(f"FYERS token exchange failed: {resp!r}")

    raw = load_auth_file(auth_file)
    rec = get_user_record(auth_file, user_key)
    rec["auth_code"] = clean_code
    rec["access_token"] = str(resp["access_token"]).strip()
    rec["token_updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    if "refresh_token" in resp and resp.get("refresh_token"):
        rec["refresh_token"] = resp.get("refresh_token")
    raw["users"][user_key] = rec
    save_auth_file(auth_file, raw)
    return resp


def validate_access_token(auth_file: str, *, user_key: str, log_path: str = "") -> Dict[str, Any]:
    _require_sdk()
    client_id, access_token = get_fyers_creds_from_json(auth_file, user_key=user_key)
    fy = fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path=log_path or "",
    )
    return fy.get_profile()


def extract_auth_code(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "auth_code=" not in text:
        return text
    parsed = urlparse(text)
    qs = parse_qs(parsed.query)
    vals = qs.get("auth_code") or []
    return str(vals[0]).strip() if vals else ""


def extract_state(value: str) -> str:
    text = str(value or "").strip()
    if not text or "state=" not in text:
        return ""
    parsed = urlparse(text)
    qs = parse_qs(parsed.query)
    vals = qs.get("state") or []
    return str(vals[0]).strip() if vals else ""


def user_key_from_state(state: str) -> str:
    text = str(state or "").strip()
    prefix = "fyers-auth:"
    if text.startswith(prefix):
        return text[len(prefix):].strip()
    return ""
