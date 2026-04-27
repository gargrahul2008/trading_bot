from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import streamlit as st
import streamlit.components.v1 as components

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.broker.auth_json import (
    exchange_auth_code_for_token,
    extract_auth_code,
    extract_state,
    generate_login_url,
    list_fyers_users,
    load_auth_file,
    user_key_from_state,
    validate_access_token,
)


def _default_auth_file() -> str:
    return os.getenv("FYERS_AUTH_FILE", str(REPO_ROOT / "fyers_auth.json"))


def _query_params() -> Dict[str, str]:
    try:
        raw = st.query_params
        return {str(k): str(raw[k]) for k in raw.keys()}
    except Exception:
        raw2 = st.experimental_get_query_params()
        out: Dict[str, str] = {}
        for k, vals in raw2.items():
            if isinstance(vals, list):
                out[str(k)] = str(vals[0]) if vals else ""
            else:
                out[str(k)] = str(vals)
        return out


def _clear_query_params() -> None:
    try:
        st.query_params.clear()
    except Exception:
        try:
            st.experimental_set_query_params()
        except Exception:
            pass


def _redirect(url: str) -> None:
    target = str(url or "").strip()
    if not target:
        raise ValueError("Redirect URL is empty.")
    st.info("Redirecting to FYERS login. If the browser blocks it, use the fallback button below.")
    st.link_button("Open FYERS Login", target, use_container_width=True)
    components.html(
        f"""
        <html>
          <head>
            <meta http-equiv="refresh" content="0; url={target}">
          </head>
          <body>
            <script>
              window.top.location.replace({json.dumps(target)});
            </script>
          </body>
        </html>
        """,
        height=0,
    )
    st.stop()


def _mask_token(token: str) -> str:
    text = str(token or "").strip()
    if len(text) <= 10:
        return "*" * len(text)
    return f"{text[:6]}...{text[-4:]}"


def _user_label(user_key: str, rec: Dict[str, Any]) -> str:
    label = str(rec.get("label") or "").strip()
    return f"{user_key} ({label})" if label else user_key


def _redact_auth_data(raw: Dict[str, Any]) -> Dict[str, Any]:
    redacted = json.loads(json.dumps(raw, default=str))
    users = redacted.get("users")
    if not isinstance(users, dict):
        return redacted
    for rec in users.values():
        if not isinstance(rec, dict):
            continue
        for key in ("secret_key", "access_token", "auth_code", "refresh_token"):
            if key in rec and rec.get(key):
                value = str(rec[key])
                rec[key] = f"{value[:4]}...{value[-4:]}" if len(value) > 10 else "********"
    return redacted


def _rerun() -> None:
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()


def render_page() -> None:
    st.title("FYERS Auth")
    st.caption("JSON-backed FYERS login for a small fixed set of users.")

    auth_file = st.text_input("Auth file", value=_default_auth_file())

    try:
        raw = load_auth_file(auth_file)
        users = list_fyers_users(auth_file)
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    if not users:
        st.warning("No users found. Create fyers_auth.json from the example file and add your users.")
        st.code(
            json.dumps(
                {
                    "users": {
                        "rajoo": {
                            "label": "Rajoo",
                            "client_id": "YOUR_APP_ID",
                            "secret_key": "YOUR_SECRET_KEY",
                            "redirect_uri": "http://127.0.0.1:8501/fyers-auth",
                            "access_token": ""
                        }
                    }
                },
                indent=2,
            ),
            language="json",
        )
        st.stop()

    params = _query_params()
    callback_auth_code = params.get("auth_code", "")
    callback_state = params.get("state", "")
    callback_user_key = user_key_from_state(callback_state)

    user_keys = list(users.keys())
    default_index = 0
    if callback_user_key in user_keys:
        default_index = user_keys.index(callback_user_key)

    selected_user = st.selectbox(
        "User",
        options=user_keys,
        index=default_index,
        format_func=lambda k: _user_label(k, users[k]),
    )
    rec = users[selected_user]

    status_message = st.session_state.pop("auth_status_message", "")
    status_kind = st.session_state.pop("auth_status_kind", "info")
    if status_message:
        getattr(st, status_kind, st.info)(status_message)

    if callback_auth_code and callback_user_key in user_keys:
        last_code = str(st.session_state.get("last_auto_auth_code") or "")
        if callback_auth_code != last_code:
            try:
                exchange_auth_code_for_token(auth_file, user_key=callback_user_key, auth_code=callback_auth_code)
                st.session_state["last_auto_auth_code"] = callback_auth_code
                st.session_state["auth_status_kind"] = "success"
                st.session_state["auth_status_message"] = (
                    f"Access token saved automatically for user '{callback_user_key}'."
                )
                _clear_query_params()
                _rerun()
            except Exception as exc:
                st.error(f"Automatic token exchange failed: {exc}")

    left, right = st.columns([1.3, 1.7])

    with left:
        st.subheader("Stored Record")
        st.write(
            {
                "user_key": selected_user,
                "label": rec.get("label") or "",
                "client_id": rec.get("client_id") or "",
                "redirect_uri": rec.get("redirect_uri") or "",
                "token_updated_at": rec.get("token_updated_at") or "",
                "last_login_url_at": rec.get("last_login_url_at") or "",
                "access_token": _mask_token(str(rec.get("access_token") or "")),
            }
        )

        if st.button("Generate", type="primary", use_container_width=True):
            try:
                profile = validate_access_token(auth_file, user_key=selected_user)
                if isinstance(profile, dict) and profile.get("s") == "ok":
                    st.success("Saved token is already valid.")
                    st.json(profile)
                else:
                    state = f"fyers-auth:{selected_user}"
                    url = generate_login_url(auth_file, user_key=selected_user, state=state)
                    _redirect(url)
            except Exception:
                state = f"fyers-auth:{selected_user}"
                try:
                    url = generate_login_url(auth_file, user_key=selected_user, state=state)
                    _redirect(url)
                except Exception as inner_exc:
                    st.error(str(inner_exc))

        if st.button("Validate Saved Token", use_container_width=True):
            try:
                profile = validate_access_token(auth_file, user_key=selected_user)
                if isinstance(profile, dict) and profile.get("s") == "ok":
                    st.success("Token is valid.")
                else:
                    st.warning("Token validation failed.")
                st.json(profile)
            except Exception as exc:
                st.error(str(exc))

    with right:
        st.subheader("Callback / Token Exchange")
        st.caption(
            "Preferred flow: click `Generate`, complete FYERS login if asked, and let this page save the token "
            "automatically when FYERS redirects back here."
        )
        if callback_auth_code:
            st.info(
                f"Callback detected for user '{callback_user_key or selected_user}'. "
                "Automatic save runs when state matches a known user. Manual exchange remains available as fallback."
            )

        callback_url = st.text_area(
            "Paste callback URL or auth_code",
            value=callback_auth_code,
            height=140,
            help="You can paste the full redirect URL or only the auth_code.",
        )

        chosen_target = selected_user
        if callback_user_key in user_keys:
            chosen_target = callback_user_key
        else:
            pasted_state = extract_state(callback_url)
            pasted_user_key = user_key_from_state(pasted_state)
            if pasted_user_key in user_keys:
                chosen_target = pasted_user_key

        st.caption(f"Token will be saved for: `{chosen_target}`")

        if st.button("Exchange And Save Token", use_container_width=True):
            try:
                auth_code = extract_auth_code(callback_url)
                if not auth_code:
                    raise ValueError("Could not find auth_code in the provided value.")
                resp = exchange_auth_code_for_token(auth_file, user_key=chosen_target, auth_code=auth_code)
                st.success(f"Access token saved for user '{chosen_target}'.")
                st.json(resp)
                if callback_auth_code:
                    _clear_query_params()
                _rerun()
            except Exception as exc:
                st.error(str(exc))

        st.markdown("Current JSON")
        st.code(json.dumps(_redact_auth_data(raw), indent=2), language="json")
