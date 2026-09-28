"""Builds the EXACT StrategyConfig the research notebook is currently using,
by parsing notebooks/scanner_universe_scan.ipynb itself at call time — so the
live scanner can never silently drift from whatever the user has tuned there
(SCANNER_OVERRIDES / MA200_FILTER_OVERRIDES / TRADE_FILTER_OVERRIDES / ... —
every override dict, verbatim). No values are copy-pasted or hardcoded
anywhere in this file.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "scanner_universe_scan.ipynb"


@dataclass
class LiveScanSetup:
    config: Any                 # StrategyConfig
    universe_file: Path
    etf_universe_file: Path
    fno_universe_file: Path
    timeframe: str
    fetch_start_date: str
    end_date: str
    repo_root: Path


def _cell_sources(notebook_path: Path) -> list[str]:
    nb = json.loads(notebook_path.read_text())
    return ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]


def _find(cells: list[str], prefix: str) -> str:
    matches = [c for c in cells if c.startswith(prefix)]
    if not matches:
        raise ValueError(
            f"notebook cell starting with {prefix!r} not found — has the notebook's "
            f"structure changed? (live scanner parses it directly, see this module's docstring)"
        )
    return matches[0]


def load_live_scan_setup(
    *, end_date: "date | str | None" = None, notebook_path: Path = NOTEBOOK_PATH,
) -> LiveScanSetup:
    """Executes the notebook's own repo-root / imports / params / config-build
    cells verbatim (same technique used throughout this codebase's own
    scratch reproductions this session) so the live scanner runs the
    IDENTICAL config the notebook would, with zero manual copy/paste to keep
    in sync.

    `end_date` overrides the notebook's own hardcoded END_DATE (a frozen
    research date) with "today" (or whatever's passed) for live use.
    START_DATE / FETCH_START_DATE are left exactly as the notebook has them —
    if the notebook's START_DATE is set to a short research window (e.g. for
    a fast backtest), the live scanner inherits that SAME shorter window and
    can lose track of a touch sequence that started earlier. Keep START_DATE
    generously early in the notebook if you want the live scanner to always
    have full sequence context.
    """
    cells = _cell_sources(notebook_path)
    ns: dict[str, Any] = {"display": lambda *a, **k: None, "__name__": "live_scan"}
    prev_cwd = os.getcwd()
    os.chdir(notebook_path.parent)
    try:
        exec(cells[0], ns)   # repo root
        exec(cells[1], ns)   # imports
        params_src = _find(cells, "# NIFTY Total Market")
        if end_date is not None:
            end_str = end_date.isoformat() if hasattr(end_date, "isoformat") else str(end_date)
            new_src, n = re.subn(r"END_DATE\s*=\s*'[^']*'", f"END_DATE   = '{end_str}'", params_src, count=1)
            if n != 1:
                raise ValueError("could not find END_DATE assignment to override in the params cell")
            params_src = new_src
        exec(params_src, ns)
        head = _find(cells, "config = StrategyConfig.from_dict").split("universe = load_universe")[0]
        exec(head, ns)
    finally:
        os.chdir(prev_cwd)

    return LiveScanSetup(
        config=ns["config"],
        universe_file=ns["UNIVERSE_FILE"],
        etf_universe_file=ns["ETF_UNIVERSE_FILE"],
        fno_universe_file=ns["FNO_UNIVERSE_FILE"],
        timeframe=ns["TIMEFRAME"],
        fetch_start_date=ns["FETCH_START_DATE"],
        end_date=ns["END_DATE"],
        repo_root=ns["REPO_ROOT"],
    )
