"""
Overlay PnL Attribution
=======================
Freezes the account state (qty + cash) at a cutoff timestamp as a baseline.
All post-cutoff trades accumulate in an overlay bucket.

  overlay_pnl(now) = overlay_cash + overlay_qty * mark_price

Baseline source (Q0/C0):
  Reads from mexc_ledger.csv — last row with ts < cutoff_ts.
  The ledger's Q_after includes manual/external ETH (not just bot-tracked).
  The ledger's cash_after correctly tracks USDC from a known checkpoint.
  Falls back to the bot-trade JSONL (strategy-only) if ledger is unavailable.

Overlay (bot trades only, starts at zero at cutoff):
  Applied via on_fill() for every post-cutoff FILL from the bot trade JSONL.

CSV schema (pnl_new.csv):
  ts, event, symbol, side, qty, fill_price,
  overlay_qty, overlay_cash, mark_price, overlay_pnl,
  baseline_qty, baseline_cash, baseline_value, account_value_est
"""
from __future__ import annotations

import csv
import json
import os
from decimal import Decimal
from typing import Any, Optional

from common.utils.logger import setup_logger

LOG = setup_logger("overlay_pnl")

D0 = Decimal("0")

FIELDS = [
    "ts", "event", "trade_type", "symbol", "side", "qty", "fill_price",
    "overlay_qty", "overlay_cash", "mark_price", "overlay_pnl",
    "rebal_overlay_qty", "rebal_overlay_cash", "rebal_pnl",
    "baseline_qty", "baseline_cash", "baseline_value", "account_value_est",
]


def _dec(x: Any) -> Decimal:
    if x is None:
        return D0
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return D0


class OverlayPnL:
    """
    Attribution overlay starting at cutoff_ts.

    Baseline (frozen at cutoff):
      baseline_qty  = Q_after from last ledger row before cutoff
                      (includes manual/external ETH held at that time)
      baseline_cash = cash_after from last ledger row before cutoff

    Overlay (starts at zero, grows with post-cutoff bot fills):
      overlay_qty / overlay_cash updated on every on_fill() call.

    Strategy PnL since cutoff:
      overlay_pnl = overlay_cash + overlay_qty * mark_price

    Equivalent identity:
      account_value_est = baseline_value + overlay_pnl
                        = (baseline_cash + baseline_qty * px)
                          + (overlay_cash + overlay_qty * px)
    """

    def __init__(
        self,
        *,
        trades_path: str,
        csv_path: str,
        state,
        ledger_path: Optional[str] = None,
        cutoff_ts: str = "2026-04-08T00:00:00+00:00",
    ) -> None:
        self.trades_path = trades_path
        self.csv_path = csv_path
        self.state = state
        self.ledger_path = ledger_path
        self.cutoff_ts = cutoff_ts
        self._ensure_csv_header()
        self._init_if_needed()

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _ensure_csv_header(self) -> None:
        if not os.path.exists(self.csv_path):
            os.makedirs(os.path.dirname(self.csv_path) or ".", exist_ok=True)
            with open(self.csv_path, "w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(FIELDS)

    def _baseline_from_ledger(self) -> Optional[tuple]:
        """
        Return (baseline_qty, baseline_cash) from the last ledger row
        whose ts < cutoff_ts, or None if ledger unavailable/empty.

        Ledger columns used: ts, Q_after, cash_after
        Timestamps in the ledger are IST (+0530); we compare as strings
        after normalising both to UTC ISO format.
        """
        if not self.ledger_path or not os.path.exists(self.ledger_path):
            return None

        import datetime as _dt
        from zoneinfo import ZoneInfo
        IST = ZoneInfo("Asia/Kolkata")
        UTC = _dt.timezone.utc

        def _to_utc_iso(ts_str: str) -> str:
            try:
                dt = _dt.datetime.fromisoformat(ts_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=IST)
                return dt.astimezone(UTC).isoformat()
            except Exception:
                return ts_str

        last_row = None
        try:
            with open(self.ledger_path, "r", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    ts_utc = _to_utc_iso(str(row.get("ts", "")))
                    if ts_utc < self.cutoff_ts:
                        last_row = row
        except Exception as e:
            LOG.warning("Overlay PnL: could not read ledger %s: %s", self.ledger_path, e)
            return None

        if last_row is None:
            return None

        baseline_qty = _dec(last_row.get("Q_after", "0"))
        baseline_cash = _dec(last_row.get("cash_after", "0"))
        LOG.info(
            "Overlay PnL: baseline from ledger row ts=%s — Q0=%s C0=%s",
            last_row.get("ts"), baseline_qty, baseline_cash,
        )
        return baseline_qty, baseline_cash

    def _baseline_from_trades(self) -> tuple:
        """
        Fallback: read strategy-only net_qty_after and cash_after from
        the last FILL in the bot trade JSONL before cutoff_ts.
        Note: this excludes manual/external ETH held outside the bot.
        """
        last_before: Optional[dict] = None
        try:
            with open(self.trades_path, "r", encoding="utf-8") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        t = json.loads(raw)
                    except Exception:
                        continue
                    if t.get("event") != "FILL":
                        continue
                    if _dec(t.get("qty", 0)) <= 0:
                        continue
                    if str(t.get("ts", "")) < self.cutoff_ts:
                        last_before = t
        except FileNotFoundError:
            pass

        if last_before is not None:
            bq = _dec(last_before.get("net_qty_after", 0))
            bc = _dec(last_before.get("cash_after", 0))
            LOG.warning(
                "Overlay PnL: using fallback (strategy-only) baseline from "
                "trade ts=%s — Q0=%s C0=%s (excludes external ETH)",
                last_before.get("ts"), bq, bc,
            )
            return bq, bc

        LOG.warning(
            "Overlay PnL: no FILL trades found before cutoff %s — baseline set to zero.",
            self.cutoff_ts,
        )
        return D0, D0

    def _init_if_needed(self) -> None:
        """
        Reconstruct baseline and replay post-cutoff bot trades.
        Skipped when extras["overlay_initialized"] is already True
        (state was loaded from the persisted state file).
        """
        if self.state.extras.get("overlay_initialized"):
            LOG.info(
                "Overlay PnL: restored from state — "
                "baseline_qty=%s baseline_cash=%s overlay_qty=%s overlay_cash=%s",
                self.state.extras.get("overlay_baseline_qty"),
                self.state.extras.get("overlay_baseline_cash"),
                self.state.extras.get("overlay_qty"),
                self.state.extras.get("overlay_cash"),
            )
            return

        # --- Step 1: Get baseline Q0/C0 ---
        result = self._baseline_from_ledger()
        if result is not None:
            baseline_qty, baseline_cash = result
        else:
            baseline_qty, baseline_cash = self._baseline_from_trades()

        # --- Step 2: Replay post-cutoff bot trades into overlay ---
        overlay_qty = D0
        overlay_cash = D0
        try:
            with open(self.trades_path, "r", encoding="utf-8") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        t = json.loads(raw)
                    except Exception:
                        continue
                    if t.get("event") != "FILL":
                        continue
                    qty = _dec(t.get("qty", 0))
                    if qty <= 0:
                        continue
                    if str(t.get("ts", "")) < self.cutoff_ts:
                        continue
                    cum_quote = _dec(t.get("cum_quote_qty", 0))
                    side = str(t.get("side", "")).upper()
                    if side == "BUY":
                        overlay_qty += qty
                        overlay_cash -= cum_quote
                    else:
                        overlay_qty -= qty
                        overlay_cash += cum_quote
        except FileNotFoundError:
            LOG.warning("Overlay PnL: trades file not found: %s", self.trades_path)

        # --- Step 3: Replay rebalance-only bucket ---
        rebal_qty  = D0
        rebal_cash = D0
        try:
            with open(self.trades_path, "r", encoding="utf-8") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        t = json.loads(raw)
                    except Exception:
                        continue
                    if t.get("event") != "FILL":
                        continue
                    qty_t = _dec(t.get("qty", 0))
                    if qty_t <= 0:
                        continue
                    if str(t.get("ts", "")) < self.cutoff_ts:
                        continue
                    reason = str(t.get("reason", ""))
                    if not reason.startswith("rebalance_"):
                        continue
                    cum_quote_t = _dec(t.get("cum_quote_qty", 0))
                    side_t = str(t.get("side", "")).upper()
                    if side_t == "BUY":
                        rebal_qty  += qty_t
                        rebal_cash -= cum_quote_t
                    else:
                        rebal_qty  -= qty_t
                        rebal_cash += cum_quote_t
        except FileNotFoundError:
            pass

        # --- Step 4: Persist into state.extras ---
        self.state.extras["overlay_cutoff_ts"] = self.cutoff_ts
        self.state.extras["overlay_baseline_qty"] = str(baseline_qty)
        self.state.extras["overlay_baseline_cash"] = str(baseline_cash)
        self.state.extras["overlay_qty"] = str(overlay_qty)
        self.state.extras["overlay_cash"] = str(overlay_cash)
        self.state.extras["rebal_overlay_qty"] = str(rebal_qty)
        self.state.extras["rebal_overlay_cash"] = str(rebal_cash)
        self.state.extras["overlay_initialized"] = True

        LOG.info(
            "Overlay PnL: initialized — baseline_qty=%s baseline_cash=%s "
            "overlay_qty=%s overlay_cash=%s",
            baseline_qty, baseline_cash, overlay_qty, overlay_cash,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_fill(
        self,
        *,
        ts: str,
        symbol: str,
        side: str,
        qty: Decimal,
        cum_quote: Decimal,
        fill_price: Decimal,
        mark_price: Decimal,
        reason: str = "",
    ) -> None:
        """
        Call after every confirmed FILL (qty > 0).
        Skips fills at or before the cutoff timestamp.
        Updates overlay state and appends a row to pnl_new.csv.
        """
        if ts < self.cutoff_ts:
            return

        is_rebal = reason.startswith("rebalance_")
        trade_type = "rebalance" if is_rebal else "ladder"

        overlay_qty  = _dec(self.state.extras.get("overlay_qty", "0"))
        overlay_cash = _dec(self.state.extras.get("overlay_cash", "0"))
        rebal_qty    = _dec(self.state.extras.get("rebal_overlay_qty", "0"))
        rebal_cash   = _dec(self.state.extras.get("rebal_overlay_cash", "0"))

        if side.upper() == "BUY":
            overlay_qty  += qty
            overlay_cash -= cum_quote
            if is_rebal:
                rebal_qty  += qty
                rebal_cash -= cum_quote
        else:
            overlay_qty  -= qty
            overlay_cash += cum_quote
            if is_rebal:
                rebal_qty  -= qty
                rebal_cash += cum_quote

        self.state.extras["overlay_qty"]        = str(overlay_qty)
        self.state.extras["overlay_cash"]       = str(overlay_cash)
        self.state.extras["rebal_overlay_qty"]  = str(rebal_qty)
        self.state.extras["rebal_overlay_cash"] = str(rebal_cash)

        self._write_row(
            ts=ts,
            event="FILL",
            trade_type=trade_type,
            symbol=symbol,
            side=side,
            qty=qty,
            fill_price=fill_price,
            mark_price=mark_price,
            overlay_qty=overlay_qty,
            overlay_cash=overlay_cash,
            rebal_qty=rebal_qty,
            rebal_cash=rebal_cash,
        )

    def current_overlay_pnl(self, mark_price: Decimal) -> Decimal:
        """Return live overlay PnL given a mark price (no CSV write)."""
        oq = _dec(self.state.extras.get("overlay_qty", "0"))
        oc = _dec(self.state.extras.get("overlay_cash", "0"))
        return oc + oq * mark_price

    def baseline_value(self, mark_price: Decimal) -> Decimal:
        bq = _dec(self.state.extras.get("overlay_baseline_qty", "0"))
        bc = _dec(self.state.extras.get("overlay_baseline_cash", "0"))
        return bc + bq * mark_price

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _to_ist(ts: str) -> str:
        """Convert any ISO timestamp to IST (Asia/Kolkata) string."""
        try:
            import datetime as _dt
            from zoneinfo import ZoneInfo
            dt = _dt.datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            return dt.astimezone(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST")
        except Exception:
            return ts

    @staticmethod
    def _r(v, d=3) -> str:
        try:
            return f"{float(v):.{d}f}"
        except Exception:
            return str(v)

    def _write_row(
        self,
        *,
        ts: str,
        event: str,
        trade_type: str,
        symbol: str,
        side: str,
        qty: Decimal,
        fill_price: Decimal,
        mark_price: Decimal,
        overlay_qty: Decimal,
        overlay_cash: Decimal,
        rebal_qty: Decimal,
        rebal_cash: Decimal,
    ) -> None:
        baseline_qty  = _dec(self.state.extras.get("overlay_baseline_qty", "0"))
        baseline_cash = _dec(self.state.extras.get("overlay_baseline_cash", "0"))

        overlay_pnl       = overlay_cash + overlay_qty * mark_price
        rebal_pnl         = rebal_cash   + rebal_qty   * mark_price
        baseline_value    = baseline_cash + baseline_qty * mark_price
        account_value_est = baseline_value + overlay_pnl

        r = self._r
        row = {
            "ts":                self._to_ist(ts),
            "event":             event,
            "trade_type":        trade_type,
            "symbol":            symbol,
            "side":              side,
            "qty":               r(qty, 6),
            "fill_price":        r(fill_price, 3),
            "overlay_qty":       r(overlay_qty, 6),
            "overlay_cash":      r(overlay_cash, 3),
            "mark_price":        r(mark_price, 3),
            "overlay_pnl":       r(overlay_pnl, 3),
            "rebal_overlay_qty": r(rebal_qty, 6),
            "rebal_overlay_cash":r(rebal_cash, 3),
            "rebal_pnl":         r(rebal_pnl, 3),
            "baseline_qty":      r(baseline_qty, 6),
            "baseline_cash":     r(baseline_cash, 3),
            "baseline_value":    r(baseline_value, 3),
            "account_value_est": r(account_value_est, 3),
        }
        try:
            with open(self.csv_path, "a", encoding="utf-8", newline="") as fh:
                csv.DictWriter(fh, fieldnames=FIELDS).writerow(row)
        except Exception as e:
            LOG.warning("Failed writing overlay PnL row: %s", e)
