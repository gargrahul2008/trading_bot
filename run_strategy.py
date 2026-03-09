#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import os
from typing import Any, Dict, Tuple
from decimal import Decimal

from common.broker.auth_db import get_fyers_creds_from_db
from common.broker.fyers_client import FyersClient
from common.broker.mexc_spot_client import MexcSpotClient
from common.engine.state import GlobalState
from common.engine.execution import ExecutionConfig
from common.engine.generic_runner import GenericRunner
from common.utils.logger import setup_logger
from common.broker.interfaces import to_decimal

LOG = setup_logger("main")

def _abs(path: str, base_dir: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(base_dir, path))

def load_config(path: str) -> tuple[Dict[str, Any], str]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    base_dir = os.path.dirname(os.path.abspath(path))
    return cfg, base_dir

def _load_secrets_file(secrets_path: str) -> Dict[str, Any]:
    with open(secrets_path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_broker(cfg: Dict[str, Any], base_dir: str):
    b = cfg.get("broker") or {}
    btype = (b.get("type") or "fyers").lower()

    if btype == "fyers":
        auth_mode = (b.get("auth_mode") or "env").lower()
        client_id = (b.get("client_id") or os.getenv("FYERS_CLIENT_ID", "")).strip()
        access_token = (b.get("access_token") or os.getenv("FYERS_ACCESS_TOKEN", "")).strip()

        if auth_mode == "db":
            user_id = int(b.get("user_id") or 0)
            if user_id <= 0:
                raise SystemExit("broker.auth_mode=db requires broker.user_id")
            db_info_file = _abs(str(b.get("db_info_file") or "tr_db"), base_dir)
            db_name = str(b.get("db_name") or "traderealm")
            db_table = str(b.get("db_table") or "nse_usercredential")
            client_id, access_token = get_fyers_creds_from_db(
                user_id,
                db_info_file=db_info_file,
                db_name=db_name,
                table_name=db_table,
            )

        if not client_id or not access_token:
            raise SystemExit("Missing FYERS auth. Use broker.auth_mode=db or set FYERS_CLIENT_ID/FYERS_ACCESS_TOKEN.")

        log_path = str(b.get("log_path") or "")
        return FyersClient(client_id=client_id, access_token=access_token, log_path=log_path)

    if btype == "mexc_spot":
        secrets_file = b.get("secrets_file")
        if not secrets_file:
            raise SystemExit("MEXC broker requires broker.secrets_file")
        secrets_path = _abs(str(secrets_file), base_dir)
        sec = _load_secrets_file(secrets_path)

        api_key = str(sec.get("api_key") or "").strip()
        api_secret = str(sec.get("api_secret") or "").strip()
        if not api_key or not api_secret:
            raise SystemExit("secrets_file must contain api_key and api_secret")

        base_url = str(b.get("base_url") or "https://api.mexc.com")
        recv_window_ms = int(b.get("recv_window_ms") or 5000)
        timeout_s = int(b.get("timeout_s") or 10)
        return MexcSpotClient(api_key=api_key, api_secret=api_secret, base_url=base_url, recv_window_ms=recv_window_ms, timeout_s=timeout_s)

    raise SystemExit(f"Unsupported broker type: {btype}")

def load_strategy(cfg: Dict[str, Any]):
    name = cfg.get("strategy_name") or "pct_ladder"
    module = importlib.import_module(f"strategies.{name}.strategy")
    if not hasattr(module, "create_strategy"):
        raise SystemExit(f"Strategy module strategies.{name}.strategy must export create_strategy(strategy_cfg)->Strategy")
    return module.create_strategy(cfg.get("strategy") or {})

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to strategy config JSON")
    args = ap.parse_args()

    cfg, base_dir = load_config(args.config)

    paths = cfg.get("paths") or {}
    state_path = _abs(str(paths.get("state_path") or "state.json"), base_dir)
    trades_path = _abs(str(paths.get("trades_path") or "trades.jsonl"), base_dir)
    rejects_path = _abs(str(paths.get("rejects_path") or "rejects.jsonl"), base_dir)
    manual_adjustments_path = _abs(str(paths.get("manual_adjustments_path") or "manual_adjustments.jsonl"), base_dir)
    manual_positions_file = paths.get("manual_positions_file")
    manual_positions_path = _abs(str(manual_positions_file), base_dir) if manual_positions_file else None
    capital_flows_file = paths.get("capital_flows_file")
    capital_flows_path = _abs(str(capital_flows_file), base_dir) if capital_flows_file else None

    os.makedirs(os.path.dirname(state_path), exist_ok=True)

    broker = build_broker(cfg, base_dir)
    strategy = load_strategy(cfg)

    ex = cfg.get("execution") or {}
    exec_cfg = ExecutionConfig(
        product_type=str(ex.get("product_type") or "CNC"),
        allow_btst_auto=bool(ex.get("allow_btst_auto", True)),
        order_mode=str(ex.get("order_mode") or "market"),
        slippage_bps=int(ex.get("slippage_bps") or 10),
        limit_ttl_seconds=int(ex.get("limit_ttl_seconds") or 15),
        max_place_retries=int(ex.get("max_place_retries") or 3),
        quote_reserve=to_decimal(ex.get("quote_reserve_usdt") or ex.get("quote_reserve") or 0),
        use_inventory_buffer=bool(ex.get("use_inventory_buffer", False)),
    )

    state = GlobalState.load(state_path)
    if manual_positions_path:
        state.extras["manual_positions_file"] = manual_positions_path
    if capital_flows_path:
        state.extras["capital_flows_file"] = capital_flows_path
    symbols = list((cfg.get("strategy") or {}).get("symbols") or [])
    if not symbols:
        raise SystemExit("Config must include strategy.symbols list.")
    if hasattr(broker, "self_symbols"):
        allowed = set(broker.self_symbols())
        missing = [s for s in symbols if s not in allowed]
        if missing:
            raise SystemExit(f"MEXC API key does not allow symbols: {missing}. Allowed: {sorted(allowed)}")
    state.ensure_symbols(symbols)

    runner = GenericRunner(
        broker=broker,
        state=state,
        symbols=symbols,
        exec_cfg=exec_cfg,
        trades_path=trades_path,
        rejects_path=rejects_path,
        market_tz=str(ex.get("market_tz") or "Asia/Kolkata"),
        market_open=str(ex.get("market_open") or "00:00"),   # crypto default always-on
        market_close=str(ex.get("market_close") or "23:59"),
        eod_cancel_time=str(ex.get("eod_cancel_time") or "23:59:59"),
        poll_seconds=int(ex.get("poll_seconds") or 2),
        closed_poll_seconds=int(ex.get("closed_poll_seconds") or 2),
        cancel_all_open_orders=bool(ex.get("cancel_all_open_orders") or False),
        sync_on_start=bool(ex.get("sync_on_start") or False),
        adopt_broker_inventory=bool(ex.get("adopt_broker_inventory") or False),
        manual_adjustments_path=manual_adjustments_path,
    )

    state.extras["reconcile_crypto_balances"] = bool(ex.get("reconcile_crypto_balances", False))
    state.extras["bot_only_pnl"] = bool(ex.get("bot_only_pnl", False))

    # --- cycle unit quote (per symbol) for fixed_quote ladders ---
    try:
        strat = cfg.get("strategy", {}) or {}
        sizing_mode = str(strat.get("sizing_mode") or "")
        if sizing_mode == "fixed_quote":
            per = strat.get("per_symbol") or {}
            defaults_buy = strat.get("buy_quote")
            defaults_sell = strat.get("sell_quote")

            unit_map = {}
            for sym in symbols:
                ps = per.get(sym) or {}
                b = ps.get("buy_quote", defaults_buy)
                s = ps.get("sell_quote", defaults_sell)
                if b is None or s is None:
                    continue
                unit_map[sym] = str(min(Decimal(str(b)), Decimal(str(s))))
            if unit_map:
                runner.state.extras["cycle_unit_quote_by_symbol"] = unit_map
    except Exception:
        pass

    runner_type = (cfg.get("runner_type") or "reactive").lower()
    if runner_type == "managed":
        runner.run_managed(strategy, state_path=state_path)
    else:
        runner.run_reactive(strategy, state_path=state_path)

if __name__ == "__main__":
    main()
