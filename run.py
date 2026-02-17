#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import os
from typing import Any, Dict

from common.config import load_config, resolve_path
from common.credentials_db import get_fyers_creds_from_db
from common.fyers_broker import FyersBroker
from common.logger import LOG
from runners.reactive_runner import run_live_reactive
from runners.proactive_runner import run_live_proactive

def _load_strategy(cfg: Dict[str, Any]):
    s = cfg.get("strategy") or {}
    mod_name = s.get("module")
    cls_name = s.get("class")
    if not mod_name or not cls_name:
        raise SystemExit("Config missing strategy.module or strategy.class")
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, cls_name)
    return cls(cfg)

def _load_broker(cfg: Dict[str, Any], cfg_dir: str) -> FyersBroker:
    auth = cfg.get("auth") or {}
    user_id = int(auth.get("user_id") or 0)
    client_id = str(auth.get("client_id") or os.getenv("FYERS_CLIENT_ID", "")).strip()
    access_token = str(auth.get("access_token") or os.getenv("FYERS_ACCESS_TOKEN", "")).strip()
    log_path = str(auth.get("log_path") or "")

    if user_id > 0:
        db_info_file = resolve_path(cfg_dir, str(auth.get("db_info_file") or "tr_db"))
        db_name = str(auth.get("db_name") or "traderealm")
        db_table = str(auth.get("db_table") or "nse_usercredential")
        client_id, access_token = get_fyers_creds_from_db(user_id, db_info_file=db_info_file, db_name=db_name, table_name=db_table)

    if not client_id or not access_token:
        raise SystemExit("Missing FYERS auth. Set auth.user_id or (FYERS_CLIENT_ID & FYERS_ACCESS_TOKEN).")

    return FyersBroker(client_id=client_id, access_token=access_token, log_path=log_path)

def _normalize_paths(cfg: Dict[str, Any], cfg_dir: str) -> Dict[str, Any]:
    paths = cfg.get("paths") or {}
    for k in ("state", "trades", "rejects"):
        if k in paths and paths[k]:
            paths[k] = resolve_path(cfg_dir, str(paths[k]))
    cfg["paths"] = paths
    return cfg

def main() -> None:
    p = argparse.ArgumentParser(description="Generic runner (FYERS)")
    sub = p.add_subparsers(dest="mode", required=True)

    live = sub.add_parser("live", help="Live trading")
    live.add_argument("--config", required=True, help="Path to strategy config.json")
    live.add_argument("--execution-type", choices=["reactive", "proactive"], default="", help="Override execution.type")

    args = p.parse_args()

    cfg, cfg_dir = load_config(args.config)
    cfg = _normalize_paths(cfg, cfg_dir)

    if args.execution_type:
        cfg.setdefault("execution", {})["type"] = args.execution_type

    broker = _load_broker(cfg, cfg_dir)
    strategy = _load_strategy(cfg)

    exec_type = str((cfg.get("execution") or {}).get("type") or "reactive").lower()
    if args.mode == "live":
        if exec_type == "proactive":
            run_live_proactive(strategy, broker, cfg)
        else:
            run_live_reactive(strategy, broker, cfg)

if __name__ == "__main__":
    main()
