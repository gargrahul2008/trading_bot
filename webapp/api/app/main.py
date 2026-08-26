"""The dashboard API.

Holds no broker credentials and never calls Fyers. Every figure it serves comes
from an account agent over loopback (see webapp/agent/README.md for why the
brokers are reached that way).
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app import overview as overview_mod
from app.agents import AgentClient
from app.auth import (
    Session,
    clear_session,
    cookie_secure,
    issue_session,
    password_hash,
    verify_password,
)
from app.config import agent_ports, get_settings, known_accounts

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("api")

app = FastAPI(
    title="Trading Dashboard API",
    version="0.1.0",
    description="Every account in one place: positions, orders, funds and P&L.",
)

settings = get_settings()
if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


class LoginBody(BaseModel):
    password: str


@app.post("/api/auth/login")
def login(body: LoginBody, response: Response) -> Dict[str, Any]:
    if not verify_password(body.password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Incorrect password")
    issue_session(response)
    return {"authenticated": True}


@app.post("/api/auth/logout")
def logout(response: Response) -> Dict[str, Any]:
    clear_session(response)
    return {"authenticated": False}


@app.get("/api/auth/me")
def me(_: str = Session) -> Dict[str, Any]:
    return {"authenticated": True}


@app.get("/api/accounts")
def accounts(_: str = Session) -> Dict[str, Any]:
    return {"accounts": known_accounts(), "ports": agent_ports()}


@app.get("/api/overview")
def get_overview(_: str = Session) -> Dict[str, Any]:
    """Every account on one screen.

    Both agent calls are fanned out at once, so the page costs one round trip
    rather than two per account, and a wedged agent costs only its own row.
    """
    client = AgentClient()
    names = known_accounts()
    if not names:
        return {"accounts": [], "totals": overview_mod.totals([]), "configured": False}

    with ThreadPoolExecutor(max_workers=2) as pool:
        books_future = pool.submit(client.fan_out, "/book", names)
        health_future = pool.submit(client.fan_out, "/health", names)
        books = {result.account: result for result in books_future.result()}
        healths = {result.account: result for result in health_future.result()}

    rows: List[Dict[str, Any]] = []
    for name in names:
        book = books.get(name)
        health = healths.get(name)
        rows.append(
            overview_mod.account_summary(
                name,
                book.data if book and book.ok else None,
                health.data if health and health.ok else None,
                error=(book.error if book else "no agent"),
            )
        )

    return {"accounts": rows, "totals": overview_mod.totals(rows), "configured": True}


@app.get("/api/accounts/{account}/{section}")
def account_section(account: str, section: str, _: str = Session) -> Dict[str, Any]:
    """One section of one account, straight from its agent."""
    if account not in agent_ports():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such account: %s" % account)
    if section not in ("positions", "orders", "holdings", "funds", "trades", "book", "health"):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such section: %s" % section)

    result = AgentClient().call(account, "/" + section)
    if not result.ok:
        # 502, not 500: the API is fine, the agent behind it is not — and the
        # message says which account so the operator knows where to look.
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, result.error or "agent error")
    return result.data


@app.get("/api/health")
def health() -> Dict[str, Any]:
    """Unauthenticated on purpose, so a monitor can reach it. Reports only
    whether the pieces are wired up, never any account figures."""
    problems = get_settings().problems()
    if not password_hash():
        problems.append("No DASHBOARD_PASSWORD_HASH set — nobody can sign in.")
    return {
        "ok": not problems,
        "problems": problems,
        "accounts": known_accounts(),
        "cookie_secure": cookie_secure(),
    }
