#!/usr/bin/env python3
"""
Regenerate per-account, per-strategy configs under accounts/<user>/<strategy>/config.json
from the live source configs in strategies/.

Why a generator (not hand-copies): the live configs drift (params get tuned). Re-run this
right before a cutover to snapshot the *current* live params into the accounts/ layout.

What it changes vs the source config:
  - broker  -> converged to local json auth (auth_file=../../../fyers_auth.json, user_key),
               dropping http token-service / db / inline-token modes.
  - paths   -> relocated into the account-strategy's own state/ folder.
Everything else (strategy params, execution, comments) is copied verbatim.

It does NOT touch state files, secrets, or anything live. Safe to run anytime.
"""
from __future__ import annotations
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "strategies" / "pct_ladder"

# account (folder) -> user_key -> {strategy folder name: source config path}
MAPPING = {
    "rahul": {
        "user_key": "user1",
        "runs": {
            "reliance": SRC / "config.reliance.mtf.json",
            "vikaseco": SRC / "config.vikaseco.json",
        },
    },
    "pratibha": {
        "user_key": "user2",
        "runs": {
            "shishind": SRC / "config.shishind.json",
            "indothai": SRC / "config.indothai.json",
            "coolcaps": SRC / "config.coolcaps.json",
            "arl":      SRC / "config.arl.json",
        },
    },
}


def build_one(account: str, user_key: str, strat: str, source: Path) -> Path:
    cfg = json.loads(source.read_text())

    # Converge auth: local json only. Drop http/db/inline token fields.
    # log_path="logs" isolates the Fyers SDK's fyersApi.log/fyersRequests.log into THIS
    # run's own folder (accounts/<user>/<strat>/logs/) — otherwise every process would
    # interleave into a single shared file at the working dir.
    cfg["broker"] = {
        "type": "fyers",
        "auth_mode": "json",
        "auth_file": "../../../fyers_auth.json",
        "user_key": user_key,
        "log_path": "logs",
    }

    # Relocate state into this run's own folder.
    cfg["paths"] = {
        "state_path": "state/state.json",
        "trades_path": "state/trades.jsonl",
        "rejects_path": "state/rejects.jsonl",
    }

    # Provenance breadcrumb.
    cfg["_migrated_from"] = str(source.relative_to(REPO))

    out_dir = REPO / "accounts" / account / strat
    (out_dir / "state").mkdir(parents=True, exist_ok=True)
    (out_dir / "logs").mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "config.json"
    out_file.write_text(json.dumps(cfg, indent=2) + "\n")
    return out_file


def main() -> None:
    for account, spec in MAPPING.items():
        for strat, source in spec["runs"].items():
            if not source.exists():
                print(f"  SKIP {account}/{strat}: source missing {source}")
                continue
            out = build_one(account, spec["user_key"], strat, source)
            print(f"  built {out.relative_to(REPO)}  (user_key={spec['user_key']}, from {source.name})")


if __name__ == "__main__":
    main()
