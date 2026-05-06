#!/usr/bin/env python3
"""
Lightweight token server for cross-VPS Fyers auth.

Serves Fyers client_id + access_token over HTTP so bots on other VPS
can fetch credentials without needing direct access to fyers_auth.json.

Security: secret key passed in X-Token-Secret header (or ?secret= param).
Source: reads fyers_auth.json on every request (always returns latest token).

Usage:
    python3 scripts/token_server.py \
        --auth-file fyers_auth.json \
        --secret YOUR_SECRET_KEY \
        --port 8502

Endpoint:
    GET /token?user=user2
    Header: X-Token-Secret: YOUR_SECRET_KEY
    Response: {"client_id": "...", "access_token": "...", "user_key": "user2"}
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs


def load_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--auth-file", default="fyers_auth.json")
    ap.add_argument("--secret",    required=True, help="Shared secret key")
    ap.add_argument("--port",      type=int, default=8502)
    ap.add_argument("--host",      default="0.0.0.0")
    return ap.parse_args()


ARGS = None  # set in main


class TokenHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        # Quiet access log — only log errors
        pass

    def _send(self, code: int, body: dict):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path != "/token":
            self._send(404, {"error": "not found"})
            return

        # Auth: check secret in header or query param
        secret_header = self.headers.get("X-Token-Secret", "")
        params = parse_qs(parsed.query)
        secret_param = params.get("secret", [""])[0]
        provided = secret_header or secret_param

        if provided != ARGS.secret:
            self._send(403, {"error": "forbidden"})
            print(f"[token_server] 403 bad secret from {self.client_address[0]}", flush=True)
            return

        user_key = params.get("user", [""])[0]
        if not user_key:
            self._send(400, {"error": "missing ?user= param"})
            return

        # Load auth file fresh on every request (always returns latest token)
        auth_path = ARGS.auth_file
        if not os.path.isabs(auth_path):
            auth_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", auth_path)
        auth_path = os.path.normpath(auth_path)

        try:
            with open(auth_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self._send(500, {"error": f"cannot read auth file: {e}"})
            return

        users = data.get("users") or {}
        user = users.get(user_key)
        if not user:
            self._send(404, {"error": f"user '{user_key}' not found"})
            return

        client_id    = str(user.get("client_id") or "").strip()
        access_token = str(user.get("access_token") or "").strip()

        if not client_id or not access_token:
            self._send(503, {"error": "token not yet available — authenticate first"})
            return

        self._send(200, {
            "user_key":     user_key,
            "client_id":    client_id,
            "access_token": access_token,
        })
        print(f"[token_server] served token for user='{user_key}' to {self.client_address[0]}", flush=True)


def main():
    global ARGS
    ARGS = load_args()

    # Resolve auth file path
    auth_path = ARGS.auth_file
    if not os.path.isabs(auth_path):
        auth_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", auth_path)
    ARGS.auth_file = os.path.normpath(auth_path)

    if not os.path.exists(ARGS.auth_file):
        print(f"ERROR: auth file not found: {ARGS.auth_file}", file=sys.stderr)
        sys.exit(1)

    server = HTTPServer((ARGS.host, ARGS.port), TokenHandler)
    print(f"[token_server] listening on {ARGS.host}:{ARGS.port}", flush=True)
    print(f"[token_server] auth file: {ARGS.auth_file}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[token_server] stopped.", flush=True)


if __name__ == "__main__":
    main()
