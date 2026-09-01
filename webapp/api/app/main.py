"""The dashboard API.

Holds no broker credentials and never calls Fyers. Every figure it serves comes
from an account agent over loopback (see webapp/agent/README.md for why the
brokers are reached that way).
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
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
from app.config import REPO, agent_ports, get_settings, known_accounts
from app.store import (
    portfolio_mod, store_book, store_capital, store_counts, store_realised,
    classify_reject, store_orders, store_realised_scrips, store_status, store_trades,
)

logging.basicConfig(level=logging.INFO)

# The Indian financial year, and the point returns are measured from.
FY_START = "2026-04-01"
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

        if book and book.ok:
            rows.append(overview_mod.account_summary(name, book.data, health.data if health and health.ok else None))
            continue

        # The agent is down or restarting. Rather than an empty row, serve what
        # it last wrote — clearly marked as from the store and visibly ageing.
        # "We cannot reach this account" and "here is what it was four minutes
        # ago" are very different answers to look at.
        stored = store_book(name)
        summary = overview_mod.account_summary(
            name, stored, store_status(name),
            error=(book.error if book else "no agent"),
        )
        if stored is not None:
            summary["agent_error"] = book.error if book else "no agent"
        rows.append(summary)

    return {"accounts": rows, "totals": overview_mod.totals(rows), "configured": True}


@app.get("/api/portfolio")
def get_portfolio(fy_start: str = FY_START, _: str = Session) -> Dict[str, Any]:
    """Capital, deployment and P&L — per account and as one book.

    The consolidated figure is the reason this dashboard exists: anyone can read
    one account in the broker's own app, nobody can read six at once.
    """
    client = AgentClient()
    names = known_accounts()
    if not names:
        return {"accounts": [], "totals": None, "configured": False}

    books = {result.account: result for result in client.fan_out("/book", names)}

    computed = []
    extras = {}
    for name in names:
        book = books.get(name)
        # Live agent preferred; the store answers when one is down, so an
        # account never disappears from a total just because its agent restarted.
        data = book.data if book and book.ok else store_book(name)
        sections = (data or {}).get("sections") or {}

        def section(kind: str) -> Any:
            return (sections.get(kind) or {}).get("data")

        realised = store_realised(name, from_date=fy_start)
        computed.append(portfolio_mod.account_portfolio(
            name,
            funds=section("funds"),
            positions=section("positions"),
            holdings=section("holdings"),
            capital_in=store_capital(name),
            # The broker's own realised figure, net of charges. Our matched
            # trades supply per-trade detail and are never added to it.
            realised=realised["net"],
            realised_is_partial=not realised["available"],
        ))
        extras[name] = {
            "realised_detail": realised,
            "from_store": bool(data and data.get("source") == "store"),
            "reachable": bool(book and book.ok),
            "error": book.error if book and not book.ok else None,
        }

    # Consolidate on the Decimal figures, then convert once — summing strings
    # would be a different and worse kind of wrong.
    totals = portfolio_mod.consolidate(computed)

    rows = []
    for row in computed:
        as_json = portfolio_mod.as_json(row)
        as_json.update(extras[row["account"]])
        rows.append(as_json)

    return {
        "accounts": rows,
        "totals": portfolio_mod.as_json(totals),
        "fy_start": fy_start,
        "configured": True,
    }


def _holding_as_position(holding: Dict[str, Any]) -> Dict[str, Any]:
    """A holding in the same shape as a position.

    Fyers keeps settled delivery stock in `holdings` and everything else in
    `positions`. That split is the broker's bookkeeping, not the trader's: both
    are shares owned and money at risk. Showing only one of them left pratibha
    with three rows out of fifteen.
    """
    qty = float(holding.get("qty") or 0)
    return {
        "symbol": holding.get("symbol", ""),
        "net_qty": qty,
        "direction": "LONG",
        "avg_price": float(holding.get("cost_price") or 0),
        "ltp": float(holding.get("ltp") or 0),
        "unrealised": float(holding.get("unrealised") or 0),
        "realised": 0.0,
        # Delivery stock, held beyond the session by definition.
        "product_type": holding.get("holding_type") or "CNC",
        "kind": "positional",
        "book": "holding",
        "carried": True,
        "raw": holding.get("raw"),
    }


@app.get("/api/positions")
def get_positions(_: str = Session) -> Dict[str, Any]:
    """Everything open right now, across every account, as one list.

    Positions *and* holdings: the broker separates them, the trader does not.
    Flattened deliberately — the point of this dashboard is to see six accounts
    at once, and a per-account grouping would put that back behind six clicks.
    """
    client = AgentClient()
    names = known_accounts()
    books = {result.account: result for result in client.fan_out("/book", names)}

    rows: List[Dict[str, Any]] = []
    missing: List[str] = []
    sold: List[Dict[str, Any]] = []
    for name in names:
        book = books.get(name)
        data = book.data if book and book.ok else store_book(name)
        if data is None:
            missing.append(name)
            continue
        sections = data.get("sections") or {}
        from_store = data.get("source") == "store"

        meta = sections.get("positions") or {}
        for position in (meta.get("data") or []):
            if not isinstance(position, dict):
                continue
            if float(position.get("net_qty") or 0) == 0:
                # A flat row is a position closed today. It belongs on the
                # Trades page, not among what is currently at risk.
                continue
            if position.get("delivery_sale"):
                # Stock sold out of holdings, awaiting settlement. There is no
                # open risk — and the broker's `unrealized_profit` on it is the
                # mark-to-market of a short that does not exist, not the trade's
                # P&L. SHRINGARMS showed +3,130 where the sale was a −6,660
                # loss. It belongs on Trades, matched against what it cost.
                sold.append({"account": name, "symbol": position.get("symbol", ""),
                             "qty": abs(float(position.get("net_qty") or 0))})
                continue
            rows.append(dict(position, account=name, book="position",
                             from_store=from_store, age_s=meta.get("age_s"),
                             stale=bool(meta.get("stale"))))

        holdings = sections.get("holdings") or {}
        for holding in (holdings.get("data") or []):
            if not isinstance(holding, dict) or not holding.get("is_open"):
                # A sold-out holding comes back with qty 0 and its old cost
                # price; it is not owned any more.
                continue
            rows.append(dict(_holding_as_position(holding), account=name,
                             from_store=from_store, age_s=holdings.get("age_s"),
                             stale=bool(holdings.get("stale"))))

    # Biggest mover first — the row you would act on is the one furthest from
    # where you wanted it, in either direction.
    rows.sort(key=lambda r: abs(float(r.get("unrealised") or 0)), reverse=True)
    # Every account queried, not just those with something open. Letting the
    # client infer the columns from the rows makes an account with nothing open
    # vanish, which is indistinguishable from one that could not be read.
    return {"positions": rows, "accounts": names, "accounts_missing": missing,
            "sold_today": sold}


@app.get("/api/trades")
def get_trades(account: Optional[str] = None, day: Optional[str] = None,
               limit: int = 500, _: str = Session) -> Dict[str, Any]:
    """Closed round trips with their own P&L, net of apportioned charges."""
    payload = store_trades(account, day, limit)
    # As with positions: an account that has closed nothing still gets a column.
    payload["accounts"] = [account] if account else known_accounts()
    return payload


@app.get("/api/orders")
def get_orders(account: Optional[str] = None, day: Optional[str] = None,
               limit: int = 500, _: str = Session) -> Dict[str, Any]:
    """Every order, including cancelled and rejected, with the reject's cause.

    Today comes from the agents so a status change shows within seconds; earlier
    days come from the store. Where both have an order the live one wins, since
    the store is only as current as the last poll.
    """
    client = AgentClient()
    names = [account] if account else known_accounts()

    live: Dict[tuple, Dict[str, Any]] = {}
    if not day:
        for result in client.fan_out("/orders", names):
            if not result.ok:
                continue
            section = result.data or {}
            for order in (section.get("data") or []):
                if not isinstance(order, dict):
                    continue
                row = dict(order, account=result.account, live=True)
                row.update(classify_reject(order.get("message")))
                live[(result.account, str(order.get("order_id")))] = row

    stored = store_orders(account, day, limit)
    rows = list(live.values())
    seen = set(live)
    for order in stored["orders"]:
        key = (order["account"], str(order["order_id"]))
        if key in seen:
            continue
        order["live"] = False
        rows.append(order)

    rows.sort(key=lambda r: (str(r.get("trading_day") or ""), str(r.get("order_id") or "")),
              reverse=True)
    return {
        "orders": rows[:limit],
        "accounts": names,
        "available": stored["available"] or bool(live),
    }


@app.get("/api/realised")
def get_realised(account: Optional[str] = None, fy_start: str = FY_START,
                 _: str = Session) -> Dict[str, Any]:
    """Realised P&L per scrip for the financial year, net of charges.

    The broker's own figures, so this is complete from 1 April — including
    shares bought years ago and sold in May, which our own matching cannot see.
    """
    payload = store_realised_scrips(account, from_date=fy_start)
    payload["accounts"] = [account] if account else known_accounts()
    payload["fy_start"] = fy_start
    return payload


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
        # Distinguishes "the dashboard is empty" from "nothing has been written
        # to the store yet", which look identical from the outside.
        "store": store_counts(),
    }


# ── The built UI ────────────────────────────────────────────────────────────
# Serving the compiled frontend from the API means one process, one port and one
# SSH tunnel rather than two — and it is the same single origin the production
# setup uses, so the session cookie behaves identically here and there.
#
# Mounted last: every /api route above is already registered, so nothing static
# can shadow them.
UI_DIST = REPO / "webapp" / "web" / "dist"

if UI_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=str(UI_DIST / "assets")), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa(full_path: str) -> FileResponse:
        """Any non-API path serves index.html, so a deep link or a refresh on
        /overview is handled by the router rather than 404ing."""
        if full_path.startswith("api/"):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "not found")
        candidate = UI_DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(UI_DIST / "index.html"))

else:
    @app.get("/", include_in_schema=False)
    def no_ui() -> Dict[str, Any]:
        return {
            "error": "The UI has not been built.",
            "fix": "cd webapp/web && npm install && npm run build",
            "api_docs": "/docs",
        }
