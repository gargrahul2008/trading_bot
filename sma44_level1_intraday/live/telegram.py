"""Minimal Telegram sender for the live 44-SMA scanner. Same wire convention
as scripts/mexc_alerts.py's own _send_telegram (plain text — alerts often
contain $, -, _ that trip Markdown's parser) but its OWN secrets file: a
shared bot/chat with the crypto grid bot would flood one channel with
unrelated alerts.
"""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_SECRETS_PATH = Path(__file__).resolve().parents[2] / "sma44_level1_intraday" / "secrets" / "telegram.json"


def send_telegram(text: str, *, secrets_path: Path = DEFAULT_SECRETS_PATH) -> bool:
    """Best-effort: logs and returns False on any failure rather than
    raising — a Telegram outage must never crash the scan/poll loop."""
    try:
        secrets = json.loads(secrets_path.read_text())
    except FileNotFoundError:
        print(
            f"[telegram] secrets file missing: {secrets_path} -- create it as "
            '{"bot_token": "<token>", "chat_id": "<id>"} (or a list of chat_ids)'
        )
        return False
    except Exception as e:
        print(f"[telegram] secrets file unreadable: {e}")
        return False

    token = secrets.get("bot_token")
    chat_ids = secrets.get("chat_id")
    if isinstance(chat_ids, str):
        chat_ids = [chat_ids]
    if not token or not chat_ids:
        print("[telegram] secrets missing bot_token/chat_id")
        return False

    ok = True
    for cid in chat_ids:
        data = urllib.parse.urlencode({"chat_id": cid, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as e:
            print(f"[telegram] send failed for chat {cid}: {e}")
            ok = False
    return ok
