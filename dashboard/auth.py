from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Set

import streamlit as st

AUTH_TTL_SECONDS = 10 * 60
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACCESS_FILE = REPO_ROOT / "dashboard_access.json"


def _rerun() -> None:
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()


def _load_access_config() -> Dict[str, Any]:
    path = Path(os.getenv("STREAMLIT_ACCESS_FILE", str(DEFAULT_ACCESS_FILE))).resolve()
    if path.exists():
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Access file must contain a JSON object: {path}")
        data["_path"] = str(path)
        return data

    expected = (
        os.getenv("STREAMLIT_APP_PASSWORD", "").strip()
        or os.getenv("FYERS_AUTH_PAGE_PASSWORD", "").strip()
    )
    if expected:
        return {
            "_path": "environment",
            "session_ttl_seconds": AUTH_TTL_SECONDS,
            "passwords": [
                {"label": "shared", "password": expected, "pages": ["dashboard", "fyers-auth"]},
            ],
        }
    raise FileNotFoundError(
        f"Missing dashboard access file: {path}. Create it from dashboard_access.example.json."
    )


def _normalize_entries(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_entries = config.get("passwords")
    if not isinstance(raw_entries, list):
        raise ValueError("Access config requires a 'passwords' list.")
    entries: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            raise ValueError(f"Access entry #{idx + 1} must be an object.")
        password = str(item.get("password") or "").strip()
        pages = item.get("pages")
        if not password:
            raise ValueError(f"Access entry #{idx + 1} is missing 'password'.")
        if not isinstance(pages, list) or not pages:
            raise ValueError(f"Access entry #{idx + 1} must define a non-empty 'pages' list.")
        clean_pages = {str(page).strip() for page in pages if str(page).strip()}
        if not clean_pages:
            raise ValueError(f"Access entry #{idx + 1} has no valid page ids.")
        entries.append(
            {
                "label": str(item.get("label") or f"access-{idx + 1}").strip(),
                "password": password,
                "pages": clean_pages,
            }
        )
    return entries


def _allowed_pages_for_password(entries: List[Dict[str, Any]], password: str) -> Set[str]:
    allowed: Set[str] = set()
    for entry in entries:
        if password == entry["password"]:
            allowed.update(entry["pages"])
    return allowed


def require_password() -> Set[str]:
    try:
        config = _load_access_config()
        entries = _normalize_entries(config)
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    now = time.time()
    ttl_seconds = int(config.get("session_ttl_seconds") or AUTH_TTL_SECONDS)
    expires_at = float(st.session_state.get("auth_expires_at") or 0.0)
    allowed_pages = {
        str(page).strip()
        for page in (st.session_state.get("auth_allowed_pages") or [])
        if str(page).strip()
    }
    if expires_at > now and allowed_pages:
        remaining_min = max(1, int((expires_at - now + 59) // 60))
        with st.sidebar:
            st.caption(f"Session active: {remaining_min} min left")
            if st.button("Lock", use_container_width=True):
                st.session_state.pop("auth_expires_at", None)
                st.session_state.pop("auth_allowed_pages", None)
                _rerun()
        return allowed_pages

    st.session_state.pop("auth_expires_at", None)
    st.session_state.pop("auth_allowed_pages", None)
    st.title("Protected Dashboard")
    st.caption(f"Enter a dashboard password. Session lasts {max(1, ttl_seconds // 60)} minutes.")
    entered = st.text_input("Password", type="password")
    if st.button("Unlock", type="primary", use_container_width=True):
        matched_pages = _allowed_pages_for_password(entries, entered)
        if matched_pages:
            st.session_state["auth_expires_at"] = now + ttl_seconds
            st.session_state["auth_allowed_pages"] = sorted(matched_pages)
            _rerun()
        else:
            st.error("Invalid password.")
    st.stop()
