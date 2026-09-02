"""The dashboard API.

Holds no broker credentials and never calls Fyers. Every figure it serves comes
from an account agent over loopback (see webapp/agent/README.md for why the
brokers are reached that way).
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app import overview as overview_mod
from app import trading
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
    store_exclusions,
    store_limits,
    rms_mod,
    trading_day,
    portfolio_mod, store_book, store_capital, store_counts, store_realised,
    classify_reject, lookup_symbol, search_symbols, store_orders,
    store_realised_scrips, store_status, store_symbols, store_trades,
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


class PlaceBody(BaseModel):
    account: str
    symbol: str
    side: str
    qty: int
    product_type: str = "CNC"
    order_type: str = "LIMIT"
    limit_price: float = 0
    stop_price: float = 0
    stop_loss: float = 0
    take_profit: float = 0
    validity: str = "DAY"
    disclosed_qty: int = 0
    offline_order: bool = False


class ModifyBody(BaseModel):
    account: str
    qty: Optional[int] = None
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    order_type: Optional[str] = None


class AccountBody(BaseModel):
    account: str


_audit_ready = False


def _audit_conn():
    """A writable store handle for the audit log, or None.

    Migrates on first use. The agents normally create and migrate the store, but
    relying on that means an API started before any agent has written finds no
    audit table — and, because audit failures are swallowed so they cannot block
    a trade, that silence is invisible. It happened: the first order placed
    through the pad went to the broker and was never logged.

    Still never blocks an action: refusing to trade because a log is unavailable
    is the wrong trade-off on a live account.
    """
    global _audit_ready
    try:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from webapp.store.schema import connect as store_connect, migrate as store_migrate

        conn = store_connect()
        if not _audit_ready:
            store_migrate(conn)
            _audit_ready = True
        return conn
    except Exception as exc:
        LOG.warning("audit store unavailable: %s", exc)
        return None


def _act(action: str, account: str, path: str, method: str,
         payload: Dict[str, Any], body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run one order action against one account's agent, audited either way.

    The trade timeout, not the read one. A read is answered from the agent's
    memory, so slowness means it is wedged; a trade is the agent waiting on the
    broker, where slowness is normal. Giving up early never stopped an order —
    it only stopped us learning what became of it.
    """
    if account not in agent_ports():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such account: %s" % account)

    conn = _audit_conn()
    audit_id = trading.begin(conn, action, account, payload) if conn else None
    try:
        result = AgentClient().call(
            account, path, method=method, body=body,
            timeout=get_settings().agent_trade_timeout)
    except Exception as exc:
        if conn:
            trading.finish(conn, audit_id, False, str(exc))
            conn.close()
        raise

    if result.timed_out:
        # Not a failure. The order may be live at the broker, and the one thing
        # that must not happen next is a retry — which is exactly what the word
        # "failed" invites. Recorded as unknown, and said plainly.
        message = (
            "%s timed out after %gs waiting for %s. The order may have reached "
            "the broker — check Orders before trying again."
            % (action, get_settings().agent_trade_timeout, account)
        )
        if conn:
            trading.finish(conn, audit_id, False, "unknown: " + (result.error or "timed out"))
            if audit_id is not None:
                conn.execute("UPDATE audit SET result = 'unknown' WHERE id = ?", (audit_id,))
                conn.commit()
            conn.close()
        raise HTTPException(status.HTTP_504_GATEWAY_TIMEOUT, message)

    if not result.ok:
        if conn:
            trading.finish(conn, audit_id, False, result.error or "agent error")
            conn.close()
        # 502, not 500: this API is fine, the broker or the agent refused.
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, result.error or "agent error")

    if conn:
        trading.finish(conn, audit_id, True, json.dumps(result.data, default=str)[:500])
        conn.close()
    return {"ok": True, "account": account, "result": result.data}


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
    excluded = store_exclusions()

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
        set_aside = set(excluded.get(name, {}))
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
            excluded=set_aside,
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


def _reference_price(account: str, body: PlaceBody,
                     book: Optional[Dict[str, Any]]) -> Optional[Any]:
    """What one share will cost, for the purpose of bounding the order.

    A limit order states it. A market order does not, so the price is fetched —
    and if it cannot be, the order's value cannot be established at all, which
    is the one thing every limit here is measured in.
    """
    if body.limit_price:
        return body.limit_price
    if body.stop_price:
        return body.stop_price

    result = AgentClient().call(account, "/quote?symbols=%s" % body.symbol, timeout=1.5)
    if result.ok:
        for row in ((result.data or {}).get("d") or []):
            price = ((row or {}).get("v") or {}).get("lp")
            if price:
                return price

    # The book's own last price, seconds to a minute old. Good enough to catch a
    # quantity typed with an extra zero, which is what this is for.
    for kind in ("positions", "holdings"):
        section = ((book or {}).get("sections") or {}).get(kind) or {}
        for row in (section.get("data") or []):
            if isinstance(row, dict) and row.get("symbol") == body.symbol:
                if row.get("ltp"):
                    return row["ltp"]
    return None


def _is_reducing(body: PlaceBody, book: Optional[Dict[str, Any]]) -> bool:
    """True when this order closes rather than opens.

    A daily loss limit that stops someone cutting a losing position is not a
    risk control; it is the trap the control existed to prevent.
    """
    section = ((book or {}).get("sections") or {}).get("positions") or {}
    for row in (section.get("data") or []):
        if not isinstance(row, dict) or row.get("symbol") != body.symbol:
            continue
        net = float(row.get("net_qty") or 0)
        if net > 0 and body.side.upper() == "SELL":
            return body.qty <= net
        if net < 0 and body.side.upper() == "BUY":
            return body.qty <= abs(net)
    return False


def _orders_in_last_minute(conn, account: str) -> int:
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM audit WHERE action = ? AND account = ?"
            " AND at > ? AND result != 'refused'",
            (trading.PLACE, account, time.time() - 60)).fetchone()
        return int(row["n"])
    except Exception as exc:
        LOG.warning("could not count recent orders: %s", exc)
        return 0


def _check_risk(account: str, body: PlaceBody) -> None:
    """Refuse an order that breaks a limit, and record the refusal.

    In the API rather than the browser: this is the only path to a broker, so
    it is the only place a limit is worth putting. A rule that can be skipped by
    opening the network tab is a suggestion, not a limit.
    """
    if rms_mod is None:
        return

    limits = rms_mod.resolve(store_limits(), account)
    # Reads, on the read timeout, and both have a fallback — the risk check sits
    # in front of a trade and must not spend the trade's patience getting there.
    result = AgentClient().call(account, "/book", timeout=1.5)
    book = result.data if result.ok else store_book(account)

    price = _reference_price(account, body, book)
    if price is None:
        raise HTTPException(
            422,
            "no price available for %s, so this order's value cannot be checked "
            "against the risk limits — place it as a limit order" % body.symbol)

    conn = _audit_conn()
    recent = _orders_in_last_minute(conn, account) if conn else 0
    try:
        rms_mod.check(
            account=account, symbol=body.symbol, qty=body.qty, price=price,
            limits=limits, book=book, recent_orders=recent,
            reducing=_is_reducing(body, book))
    except rms_mod.Breach as breach:
        if conn:
            # Refusals are audited too. An order that never reached the broker
            # is still something someone tried to do, and the pattern of what
            # the limits stop is the reason to keep or change them.
            audit_id = trading.begin(conn, trading.PLACE, account, body.model_dump())
            trading.finish(conn, audit_id, False, "refused: " + breach.reason)
            conn.execute("UPDATE audit SET result = 'refused' WHERE id = ?", (audit_id,))
            conn.commit()
            conn.close()
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            breach.reason) from breach
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


@app.post("/api/orders")
def place_order(body: PlaceBody, _: str = Session) -> Dict[str, Any]:
    """Place an order in one named account.

    The account is part of the request and is never defaulted — six accounts on
    one screen is the point of this dashboard, and also exactly how an order
    ends up in the wrong one. The agent refuses unless it was started with
    --allow-trading, so this cannot reach a broker the host did not deliberately
    enable.

    Risk limits are checked first, and a refusal is audited: an order that never
    reached the broker is still something someone tried to do.
    """
    if body.account not in agent_ports():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such account: %s" % body.account)
    _check_risk(body.account, body)

    payload = body.model_dump()
    account = payload.pop("account")
    return _act(trading.PLACE, account, "/orders", "POST", payload, payload)


class LimitBody(BaseModel):
    account: str = "*"
    name: str
    value: str


@app.get("/api/limits")
def get_limits(_: str = Session) -> Dict[str, Any]:
    """The risk limits in force, per account, and what each one means."""
    if rms_mod is None:
        return {"limits": {}, "rules": {}, "available": False}
    rows = store_limits()
    return {
        "limits": {name: {rule: str(value) for rule, value in
                          rms_mod.resolve(rows, name).items()}
                   for name in known_accounts()},
        "defaults": {rule: str(value) for rule, value in rms_mod.DEFAULTS.items()},
        "rules": rms_mod.LIMITS,
        "rows": rows,
        "available": True,
    }


@app.post("/api/limits")
def set_limit(body: LimitBody, _: str = Session) -> Dict[str, Any]:
    """Change one limit. 0 turns a rule off, which is not the same as unset."""
    if rms_mod is None or body.name not in rms_mod.LIMITS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "no such limit: %s" % body.name)
    try:
        value = Decimal(body.value)
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "not a number: %s" % body.value)
    if value < 0:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "a limit cannot be negative")

    conn = _audit_conn()
    if conn is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "store unavailable")
    try:
        conn.execute(
            "INSERT INTO rms_limits (account, name, value, at) VALUES (?,?,?,?)"
            " ON CONFLICT(account, name) DO UPDATE SET value = excluded.value,"
            " at = excluded.at",
            (body.account, body.name, str(value), time.time()))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "account": body.account, "name": body.name, "value": str(value)}


@app.patch("/api/orders/{order_id}")
def modify_order(order_id: str, body: ModifyBody, _: str = Session) -> Dict[str, Any]:
    payload = {k: v for k, v in body.model_dump().items() if v is not None}
    account = payload.pop("account")
    return _act(trading.MODIFY, account, "/orders/%s" % order_id, "PATCH",
                dict(payload, order_id=order_id), payload)


@app.delete("/api/orders/{order_id}")
def cancel_order(order_id: str, account: str, _: str = Session) -> Dict[str, Any]:
    return _act(trading.CANCEL, account, "/orders/%s" % order_id, "DELETE",
                {"order_id": order_id})


@app.post("/api/positions/{position_id}/exit")
def exit_position(position_id: str, body: AccountBody, _: str = Session) -> Dict[str, Any]:
    return _act(trading.EXIT, body.account, "/positions/%s/exit" % position_id, "POST",
                {"position_id": position_id}, {})


@app.get("/api/symbols")
def get_symbols(q: str = "", limit: int = 12, _: str = Session) -> Dict[str, Any]:
    """Instruments matching a fragment, from the exchanges' own daily list.

    Searched on the server: 22,000 instruments is not a list to ship to a
    browser, and the ranking that puts NSE:RELIANCE-EQ above BSE:RELICAB-B
    belongs next to the data.
    """
    return search_symbols(q, limit)


@app.get("/api/symbols/{symbol:path}")
def get_symbol(symbol: str, _: str = Session) -> Dict[str, Any]:
    """One instrument, with its tick and lot size.

    404 means the exchanges do not list it. For the order pad that is the answer
    to "is this real?", which a one-click pad needs before it will send.
    """
    found = lookup_symbol(symbol)
    if found is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            "%s is not in the exchanges' instrument list" % symbol)
    return found


@app.get("/api/quote")
def get_quote(account: str, symbols: str, _: str = Session) -> Dict[str, Any]:
    """A live price for one or more symbols, through one account's agent.

    Costs a broker call, so the pad asks only when the symbol settles — not on
    every keystroke.
    """
    if account not in agent_ports():
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such account: %s" % account)
    result = AgentClient().call(account, "/quote?symbols=%s" % symbols)
    if not result.ok:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, result.error or "quote failed")
    return result.data


@app.get("/api/audit")
def get_audit(limit: int = 50, _: str = Session) -> Dict[str, Any]:
    """What has been done through this dashboard, most recent first."""
    conn = _audit_conn()
    if conn is None:
        return {"entries": [], "available": False}
    try:
        return {"entries": trading.recent(conn, limit), "available": True}
    except Exception as exc:
        # An unreadable log is worth reporting, not worth a 500 on a page that
        # also shows live account figures.
        LOG.warning("audit read failed: %s", exc)
        return {"entries": [], "available": False, "error": str(exc)}
    finally:
        conn.close()


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
                # The agent normalises an order without a trading day — it only
                # ever sees today's book, so it has no reason to carry one. The
                # store stamps it on the way in, which left every live row with
                # a blank Day column that had to be *inferred* to mean today.
                row = dict(order, account=result.account, live=True)
                if not row.get("trading_day") and trading_day is not None:
                    row["trading_day"] = trading_day()
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


class ExclusionBody(BaseModel):
    account: str
    symbol: str
    reason: str = ""


@app.get("/api/exclusions")
def get_exclusions(_: str = Session) -> Dict[str, Any]:
    """Scrips kept out of the working portfolio, per account."""
    return {"exclusions": store_exclusions()}


@app.post("/api/exclusions")
def add_exclusion(body: ExclusionBody, _: str = Session) -> Dict[str, Any]:
    """Set a scrip aside. Reversible, and it changes no position data."""
    conn = _audit_conn()
    if conn is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "store unavailable")
    try:
        conn.execute(
            "INSERT INTO exclusions (account, symbol, reason, at) VALUES (?,?,?,?)"
            " ON CONFLICT(account, symbol) DO UPDATE SET reason = excluded.reason",
            (body.account, body.symbol, body.reason, time.time()))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "account": body.account, "symbol": body.symbol}


@app.delete("/api/exclusions")
def drop_exclusion(account: str, symbol: str, _: str = Session) -> Dict[str, Any]:
    """Put a scrip back into the working portfolio."""
    conn = _audit_conn()
    if conn is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "store unavailable")
    try:
        conn.execute("DELETE FROM exclusions WHERE account = ? AND symbol = ?",
                     (account, symbol))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "account": account, "symbol": symbol}


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
