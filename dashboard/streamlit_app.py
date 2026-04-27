from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.auth import require_password
from dashboard.views.fyers_auth_page import render_page as render_fyers_auth_page
from dashboard.views.mexc_dashboard_page import render_page as render_mexc_dashboard_page


st.set_page_config(page_title="Trading Dashboard", layout="wide")
allowed_pages = require_password()

mexc_page = st.Page(render_mexc_dashboard_page, title="Dashboard", icon=":material/monitoring:")
fyers_page = st.Page(render_fyers_auth_page, title="FYERS Auth", icon=":material/key:", url_path="fyers-auth", default=True)

pages = []
if "dashboard" in allowed_pages:
    pages.append(mexc_page)
if "fyers-auth" in allowed_pages:
    pages.append(fyers_page)

if not pages:
    st.error("This password does not grant access to any page.")
    st.stop()

navigation = st.navigation(pages, position="sidebar")
navigation.run()
