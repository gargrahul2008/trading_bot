from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard.views.fyers_auth_page import render_page


st.set_page_config(page_title="FYERS Auth", layout="wide")
render_page()
