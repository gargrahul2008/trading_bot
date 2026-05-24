from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib import parse
from zoneinfo import ZoneInfo

import pyotp
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.broker.auth_json import load_auth_file, save_auth_file

try:
    from fyers_apiv3 import fyersModel  # type: ignore
except Exception:
    fyersModel = None  # type: ignore


BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api-t1.fyers.in/api/v3"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"
URL_TOKEN = BASE_URL_2 + "/token"


def _require_sdk() -> None:
    if fyersModel is None:
        raise ImportError("Missing dependency fyers-apiv3. Install: pip install -r requirements.txt")


def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _parse_client_id(client_id: str) -> Tuple[str, str]:
    text = str(client_id or "").strip()
    app_id, sep, app_type = text.partition("-")
    if not app_id or not sep or not app_type:
        raise ValueError(f"Invalid client_id format {text!r}. Expected APPID-APPTYPE, for example 51MMD0KRGR-200.")
    return app_id, app_type


def _require_user_fields(rec: Dict[str, Any], *, user_key: str) -> None:
    required = ("client_id", "secret_key", "redirect_uri", "fy_id", "totp_key", "pin")
    missing = [name for name in required if not str(rec.get(name) or "").strip()]
    if missing:
        raise ValueError(f"User {user_key!r} is missing required fields: {', '.join(missing)}")


def _post_json(url: str, payload: Dict[str, Any], *, headers: Dict[str, str] | None = None) -> Dict[str, Any]:
    resp = requests.post(url=url, json=payload, headers=headers or {}, timeout=20)
    text = resp.text.strip()
    try:
        data = resp.json() if text else {}
    except json.JSONDecodeError:
        data = {"raw_text": text}
    if resp.status_code not in (200, 308):
        raise RuntimeError(f"HTTP {resp.status_code} from {url}: {data!r}")
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response from {url}: {data!r}")
    return data


def send_login_otp(*, fy_id: str, app_id_type: str) -> str:
    data = _post_json(URL_SEND_LOGIN_OTP, {"fy_id": fy_id, "app_id": app_id_type})
    request_key = str(data.get("request_key") or "").strip()
    if not request_key:
        raise RuntimeError(f"send_login_otp did not return request_key: {data!r}")
    return request_key


def generate_totp(secret: str) -> str:
    return pyotp.TOTP(secret).now()


def verify_totp(*, request_key: str, otp: str) -> str:
    data = _post_json(URL_VERIFY_TOTP, {"request_key": request_key, "otp": otp})
    next_request_key = str(data.get("request_key") or "").strip()
    if not next_request_key:
        raise RuntimeError(f"verify_otp did not return request_key: {data!r}")
    return next_request_key


def verify_pin(*, request_key: str, pin: str) -> str:
    data = _post_json(
        URL_VERIFY_PIN,
        {
            "request_key": request_key,
            "identity_type": "pin",
            "identifier": pin,
        },
    )
    access_token = str(((data.get("data") or {}) if isinstance(data.get("data"), dict) else {}).get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError(f"verify_pin did not return access token: {data!r}")
    return access_token


def request_auth_code(
    *,
    fy_id: str,
    app_id: str,
    redirect_uri: str,
    app_type: str,
    trade_access_token: str,
) -> str:
    data = _post_json(
        URL_TOKEN,
        {
            "fyers_id": fy_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "fyers-auto-auth",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True,
        },
        headers={"Authorization": f"Bearer {trade_access_token}"},
    )
    url = str(data.get("Url") or "").strip()
    if not url:
        raise RuntimeError(f"token endpoint did not return Url: {data!r}")
    auth_codes = parse.parse_qs(parse.urlparse(url).query).get("auth_code") or []
    auth_code = str(auth_codes[0]).strip() if auth_codes else ""
    if not auth_code:
        raise RuntimeError(f"token endpoint Url does not contain auth_code: {url!r}")
    return auth_code


def exchange_auth_code(*, client_id: str, secret_key: str, redirect_uri: str, auth_code: str) -> Dict[str, Any]:
    _require_sdk()
    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type="code",
        grant_type="authorization_code",
    )
    session.set_token(auth_code)
    response = session.generate_token()
    if not isinstance(response, dict) or not str(response.get("access_token") or "").strip():
        raise RuntimeError(f"FYERS token exchange failed: {response!r}")
    return response


def auto_refresh_user(auth_file: str, *, user_key: str) -> Dict[str, Any]:
    raw = load_auth_file(auth_file)
    users = raw.get("users") or {}
    rec = users.get(user_key)
    if not isinstance(rec, dict):
        raise KeyError(f"User {user_key!r} not found in {auth_file}")
    _require_user_fields(rec, user_key=user_key)

    client_id = str(rec["client_id"]).strip()
    secret_key = str(rec["secret_key"]).strip()
    redirect_uri = str(rec["redirect_uri"]).strip()
    fy_id = str(rec["fy_id"]).strip()
    totp_key = str(rec["totp_key"]).strip()
    pin = str(rec["pin"]).strip()
    app_id_type = str(rec.get("app_id_type") or "2").strip()
    app_id, app_type = _parse_client_id(client_id)

    request_key = send_login_otp(fy_id=fy_id, app_id_type=app_id_type)

    verify_key = ""
    last_error: Exception | None = None
    for _ in range(3):
        otp = generate_totp(totp_key)
        try:
            verify_key = verify_totp(request_key=request_key, otp=otp)
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            time.sleep(1.5)
    if not verify_key:
        raise RuntimeError(f"TOTP verification failed after retries: {last_error}")

    trade_access_token = verify_pin(request_key=verify_key, pin=pin)
    auth_code = request_auth_code(
        fy_id=fy_id,
        app_id=app_id,
        redirect_uri=redirect_uri,
        app_type=app_type,
        trade_access_token=trade_access_token,
    )
    token_response = exchange_auth_code(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        auth_code=auth_code,
    )

    rec["auth_code"] = auth_code
    rec["access_token"] = str(token_response["access_token"]).strip()
    rec["token_updated_at"] = _now_utc_iso()
    if token_response.get("refresh_token"):
        rec["refresh_token"] = str(token_response["refresh_token"]).strip()
    raw["users"][user_key] = rec
    save_auth_file(auth_file, raw)
    return token_response


def _next_run_delay_seconds(*, hhmm: str, tz_name: str) -> float:
    tz = ZoneInfo(tz_name)
    now = dt.datetime.now(tz)
    hour_text, minute_text = hhmm.split(":", 1)
    target = now.replace(hour=int(hour_text), minute=int(minute_text), second=0, microsecond=0)
    if target <= now:
        target = target + dt.timedelta(days=1)
    return max((target - now).total_seconds(), 0.0)


def _run_loop(*, auth_file: str, user_key: str, daily_at: str, tz_name: str) -> None:
    while True:
        delay = _next_run_delay_seconds(hhmm=daily_at, tz_name=tz_name)
        print(f"[fyers_auto_auth] next run for {user_key} at {daily_at} {tz_name} in {int(delay)}s", flush=True)
        time.sleep(delay)
        try:
            auto_refresh_user(auth_file, user_key=user_key)
            print(f"[fyers_auto_auth] token refreshed for {user_key} at {_now_utc_iso()}", flush=True)
        except Exception as exc:
            print(f"[fyers_auto_auth] refresh failed for {user_key}: {exc}", file=sys.stderr, flush=True)


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Refresh FYERS access token using stored TOTP/PIN and save it into fyers_auth.json.")
    ap.add_argument("--auth-file", default=str(REPO_ROOT / "fyers_auth.json"))
    ap.add_argument("--user-key", required=True)
    ap.add_argument("--daily-at", default="08:30", help="IST/local run time in HH:MM format when --loop is enabled.")
    ap.add_argument("--timezone", default="Asia/Kolkata")
    ap.add_argument("--loop", action="store_true", help="Keep running and refresh daily at --daily-at.")
    ap.add_argument("--once", action="store_true", help="Refresh immediately once and exit.")
    return ap


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.loop and args.once:
        raise SystemExit("Use either --loop or --once, not both.")
    if not args.loop:
        auto_refresh_user(args.auth_file, user_key=args.user_key)
        print(f"[fyers_auto_auth] token refreshed for {args.user_key} in {args.auth_file}", flush=True)
        return 0
    _run_loop(auth_file=args.auth_file, user_key=args.user_key, daily_at=args.daily_at, tz_name=args.timezone)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
