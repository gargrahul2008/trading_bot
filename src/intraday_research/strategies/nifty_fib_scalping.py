from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import logging

import numpy as np
import pandas as pd

from ..strategy import SIGNAL_COLUMNS, Strategy

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Pivot:
    kind: str           # "HIGH" or "LOW"
    bar_index: int      # position in the group DataFrame
    confirmed_at: int   # bar_index + pivot_lookback; pivot is usable from this bar onward
    price: float
    timestamp: pd.Timestamp


@dataclass
class _BOSSetup:
    direction: str      # "LONG" or "SHORT"
    impulse_high: float
    impulse_low: float
    impulse_size: float
    fib_50: float       # 50% retracement level
    fib_618: float      # 61.8% retracement level (golden zone edge, preferred entry)
    stop_loss: float    # fib 1.0 level ± stop_buffer_points
    target: float       # structure-breaking extreme (impulse endpoint)
    confirmed_at: int   # BOS bar index; entries only allowed for bar_index > confirmed_at
    zone_touched: bool = False
    zone_touched_at: int = -1
    used: bool = False  # True after a signal has been emitted for this setup
    # BOS bar quality (populated when use_bos_strength_filter=True)
    bos_bar_strong: bool = False  # True if BOS bar closed in the strong direction
    bos_level: float = 0.0        # the price level that was broken (support/resistance)


@dataclass
class NiftyBOSFibScalpStrategy(Strategy):
    """
    1-minute Break-of-Structure Fibonacci scalping strategy (NIFTY_1M_BOS_FIB_SCALP).

    Logic:
      1. Detect micro-trend from confirmed pivot structure (LH→LH bearish, HL→HL bullish).
      2. Detect break of structure (BOS): price breaks the most recent confirmed pivot low
         (bearish) or high (bullish).
      3. Draw Fib on the impulse leg that caused the BOS.
      4. Enter at the 61.8% retracement (limit_618 mode) or after zone-touch confirmation.
      5. Stop at fib 1.0 / impulse origin ± stop_buffer_points.
      6. Target at the structure-breaking extreme (impulse endpoint).

    No indicators used in the base version (no VWAP, EMA, RSI, ATR filters).
    Wire BacktestConfig.time_exit_minutes = max_hold_bars (default 15) for time-based exit.
    """

    name: str = "NIFTY_1M_BOS_FIB_SCALP"

    # Pivot detection
    pivot_lookback: int = 2
    min_swing_points: int = 2          # minimum confirmed pivots of each type before trend is declared

    # Entry
    entry_mode: Literal["limit_618", "zone_touch_confirmation"] = "limit_618"
    fib_zone_low: float = 0.50         # shallow edge of golden zone
    fib_zone_high: float = 0.618       # deep edge of golden zone (preferred entry)
    stop_buffer_points: float = 2.0

    # Trade management
    max_hold_bars: int = 15            # set BacktestConfig.time_exit_minutes to this value
    max_trades_per_day: int | None = None
    max_consecutive_losses_per_day: int | None = None
    one_trade_at_a_time: bool = True

    # Optional per-trade_date threshold overrides (daily-frozen lookback scaling).
    # When supplied, these replace the scalar thresholds for each trade_date — used
    # by the research backtest to remove whole-period lookahead. Live/default passes
    # neither, so the scalar path below is used unchanged.
    min_impulse_by_date: dict | None = None
    stop_buffer_by_date: dict | None = None

    # Quality filters
    min_impulse_points: float = 0.0    # skip setups whose initial BOS impulse is smaller than this
    min_confirmation_rr: float = 0.0   # skip zone_touch_confirmation entries where reward/risk < this
                                       # (guards against confirmation bar consuming most of the move)

    # Target extension (Fibonacci extension beyond the BOS level)
    #   1.000 → target at BOS level only (original impulse endpoint)  RR ≈ 1.48:1
    #   1.272 → 127.2% extension  RR ≈ 2.33:1
    #   1.618 → 161.8% extension  RR ≈ 3.23:1
    target_extension_ratio: float = 1.0

    # Optional indicator filters (disabled by default — base strategy is pure price action)
    use_vwap_filter: bool = False
    use_ema_filter: bool = False

    # BOS bar strength filter: route strong BOS bars to immediate N+1 open entry,
    # weak BOS bars to a broken-structure retest + rejection-wick entry.
    # Requires entry_max_bars_wait >= 1 in BacktestConfig.
    use_bos_strength_filter: bool = False
    bos_strength_threshold: float = 0.6   # (close-low)/(high-low) >= threshold → strong
    bos_retest_max_bars_wait: int = 20    # max bars to wait for a retest of the BOS level

    # ── Public interface ──────────────────────────────────────────────────────

    def generate_signals(self, frame: pd.DataFrame) -> pd.DataFrame:
        signal_rows: list[dict[str, object]] = []
        for _, group in frame.groupby(["symbol", "trade_date"], sort=False):
            group = group.sort_values("timestamp").reset_index(drop=True)
            self._warn_non_1min(group)
            signal_rows.extend(self._generate_group_signals(group))
        if not signal_rows:
            return pd.DataFrame(columns=SIGNAL_COLUMNS + ["entry_price", "trail_milestones", "pending_fill_mode"])
        df = pd.DataFrame(signal_rows)
        # Ensure all standard columns exist (entry_price is optional per-row)
        for col in SIGNAL_COLUMNS:
            if col not in df.columns:
                df[col] = None
        if "pending_fill_mode" not in df.columns:
            df["pending_fill_mode"] = "config"
        return df[SIGNAL_COLUMNS + ["entry_price", "trail_milestones", "pending_fill_mode"]]

    # ── Per-day signal generation ─────────────────────────────────────────────

    def _generate_group_signals(self, group: pd.DataFrame) -> list[dict[str, object]]:
        signal_rows: list[dict[str, object]] = []
        lookback = max(int(self.pivot_lookback), 1)
        n = len(group)

        # Pre-extract columns into numpy arrays — avoids creating a pandas Series per bar
        # (group.iloc[int] takes ~0.65ms; numpy scalar access is ~1000x faster).
        arr_high  = group["high"].to_numpy(dtype=float)
        arr_low   = group["low"].to_numpy(dtype=float)
        arr_close = group["close"].to_numpy(dtype=float)
        arr_open  = group["open"].to_numpy(dtype=float)
        arr_ts    = group["timestamp"].to_numpy()
        arr_sym   = group["symbol"].to_numpy()
        arr_td    = group["trade_date"].to_numpy()
        arr_vwap  = group["vwap"].to_numpy(dtype=float)    if "vwap"     in group.columns else None
        arr_ef    = group["ema_fast"].to_numpy(dtype=float) if "ema_fast" in group.columns else None

        confirmed_highs: list[_Pivot] = []
        confirmed_lows: list[_Pivot] = []
        active_long: _BOSSetup | None = None
        active_short: _BOSSetup | None = None
        trades_today = 0
        prev_row: dict | None = None
        micro_trend: str | None = None  # cached; updated only when a pivot is confirmed

        # Resolve this day's thresholds (daily-frozen lookback override, else scalar)
        date = str(arr_td[0]) if n else None
        self._cur_min_imp = float(self.min_impulse_points)
        self._cur_stop_buf = float(self.stop_buffer_points)
        if self.min_impulse_by_date and date in self.min_impulse_by_date:
            self._cur_min_imp = float(self.min_impulse_by_date[date])
        if self.stop_buffer_by_date and date in self.stop_buffer_by_date:
            self._cur_stop_buf = float(self.stop_buffer_by_date[date])

        for idx in range(n):
            row: dict = {
                "high": arr_high[idx], "low": arr_low[idx],
                "close": arr_close[idx], "open": arr_open[idx],
                "timestamp": arr_ts[idx], "symbol": arr_sym[idx],
                "trade_date": arr_td[idx],
            }
            if arr_vwap is not None:
                row["vwap"] = arr_vwap[idx]
            if arr_ef is not None:
                row["ema_fast"] = arr_ef[idx]

            # ── Step 1: Confirm pivot at idx - lookback ───────────────────────
            cand = idx - lookback
            if cand >= lookback:
                pivot = self._try_confirm_pivot(arr_high, arr_low, arr_ts, cand, lookback, idx)
                if pivot is not None:
                    if pivot.kind == "HIGH":
                        confirmed_highs.append(pivot)
                    else:
                        confirmed_lows.append(pivot)
                    LOG.debug(
                        "%s pivot_confirmed symbol=%s kind=%s bar=%d price=%.2f ts=%s",
                        self.name, row["symbol"], pivot.kind, pivot.bar_index,
                        pivot.price, pivot.timestamp,
                    )
                    # ── Step 2: Recompute micro-trend only when pivot set changes ─
                    micro_trend = self._micro_trend(confirmed_highs, confirmed_lows)

            # ── Step 3: Detect BOS → build active setup (uses cached micro_trend) ──
            # EXT always runs for active setups — independent of micro_trend.
            # Gating EXT on micro_trend causes it to silently stop when a tiny
            # "higher high" or "lower low" momentarily flips micro_trend to None,
            # leaving the setup anchored to a stale extreme for many bars.
            # New BOS creation remains micro_trend-gated so we only take setups
            # in the direction confirmed by the pivot structure.

            # Extension for active SHORT setup
            if active_short is not None and not active_short.used:
                cur_low = float(row["low"])
                if cur_low < active_short.impulse_low:
                    # If the previous bar was bullish (a bounce during the bearish
                    # extension), use its high as the fresh impulse_high so that
                    # fib levels stay anchored to the most recent local ceiling.
                    new_imp_h = active_short.impulse_high
                    if (prev_row is not None
                            and float(prev_row["close"]) > float(prev_row["open"])
                            and float(prev_row["high"]) < active_short.impulse_high):
                        new_imp_h = float(prev_row["high"])
                    updated = self._build_short_setup(
                        new_imp_h, cur_low, active_short.confirmed_at
                    )
                    if updated is not None:
                        active_short = updated
                        LOG.debug(
                            "%s BEARISH_EXT symbol=%s bar=%d new_low=%.2f fib618=%.2f imp_h=%.2f",
                            self.name, row["symbol"], idx, cur_low, updated.fib_618, new_imp_h,
                        )

            # Extension for active LONG setup
            if active_long is not None and not active_long.used:
                cur_high = float(row["high"])
                if cur_high > active_long.impulse_high:
                    # If the previous bar was bearish (a pullback during the bullish
                    # extension), use its low as the fresh impulse_low.
                    new_imp_l = active_long.impulse_low
                    if (prev_row is not None
                            and float(prev_row["close"]) < float(prev_row["open"])
                            and float(prev_row["low"]) > active_long.impulse_low):
                        new_imp_l = float(prev_row["low"])
                    updated = self._build_long_setup(
                        new_imp_l, cur_high, active_long.confirmed_at
                    )
                    if updated is not None:
                        active_long = updated
                        LOG.debug(
                            "%s BULLISH_EXT symbol=%s bar=%d new_high=%.2f fib618=%.2f imp_l=%.2f",
                            self.name, row["symbol"], idx, cur_high, updated.fib_618, new_imp_l,
                        )

            # New BOS — gated on micro_trend
            if micro_trend == "BEARISH" and confirmed_lows:
                ref_low = confirmed_lows[-1]
                if float(row["low"]) < ref_low.price and confirmed_highs:
                    if active_short is None or active_short.used:
                        setup = self._build_short_setup(
                            confirmed_highs[-1].price, float(row["low"]), idx, bos_bar=row
                        )
                        if setup is not None:
                            active_short = setup
                            active_long = None
                            LOG.debug(
                                "%s BEARISH_BOS symbol=%s bar=%d "
                                "impulse_high=%.2f impulse_low=%.2f "
                                "fib50=%.2f fib618=%.2f stop=%.2f target=%.2f strong=%s",
                                self.name, row["symbol"], idx,
                                setup.impulse_high, setup.impulse_low,
                                setup.fib_50, setup.fib_618,
                                setup.stop_loss, setup.target, setup.bos_bar_strong,
                            )
                            # Strong BOS: emit breakout signal immediately (fills at N+1 open)
                            if (self.use_bos_strength_filter and setup.bos_bar_strong
                                    and not self._max_trades_reached(trades_today)
                                    and self._passes_filters(row, "SHORT")):
                                signal = self._signal_row(row, setup, "bos_breakout")
                                signal_rows.append(signal)
                                trades_today += 1
                                setup.used = True

            elif micro_trend == "BULLISH" and confirmed_highs:
                ref_high = confirmed_highs[-1]
                if float(row["high"]) > ref_high.price and confirmed_lows:
                    if active_long is None or active_long.used:
                        setup = self._build_long_setup(
                            confirmed_lows[-1].price, float(row["high"]), idx, bos_bar=row
                        )
                        if setup is not None:
                            active_long = setup
                            active_short = None
                            LOG.debug(
                                "%s BULLISH_BOS symbol=%s bar=%d "
                                "impulse_low=%.2f impulse_high=%.2f "
                                "fib50=%.2f fib618=%.2f stop=%.2f target=%.2f strong=%s",
                                self.name, row["symbol"], idx,
                                setup.impulse_low, setup.impulse_high,
                                setup.fib_50, setup.fib_618,
                                setup.stop_loss, setup.target, setup.bos_bar_strong,
                            )
                            # Strong BOS: emit breakout signal immediately (fills at N+1 open)
                            if (self.use_bos_strength_filter and setup.bos_bar_strong
                                    and not self._max_trades_reached(trades_today)
                                    and self._passes_filters(row, "LONG")):
                                signal = self._signal_row(row, setup, "bos_breakout")
                                signal_rows.append(signal)
                                trades_today += 1
                                setup.used = True

            # ── Step 4: Check entry on active setups ──────────────────────────
            if not self._max_trades_reached(trades_today):
                for setup in (active_long, active_short):
                    if setup is None or setup.used or idx <= setup.confirmed_at:
                        continue
                    if not self._passes_filters(row, setup.direction):
                        continue
                    signal = self._try_entry(idx, row, setup)
                    if signal is not None:
                        signal_rows.append(signal)
                        trades_today += 1
                        setup.used = True
                        LOG.debug(
                            "%s ENTRY symbol=%s side=%s bar=%d "
                            "entry_close=%.2f stop=%.2f target=%.2f trigger=%s",
                            self.name, row["symbol"], setup.direction, idx,
                            float(row["close"]), setup.stop_loss, setup.target,
                            self.entry_mode,
                        )
                        if self.one_trade_at_a_time:
                            break

            # ── Step 5: Invalidate setups where stop level has been blown ─────
            if active_long is not None and not active_long.used:
                if float(row["low"]) <= active_long.stop_loss:
                    LOG.debug(
                        "%s long_setup_invalidated symbol=%s bar=%d (stop blown pre-entry)",
                        self.name, row["symbol"], idx,
                    )
                    active_long = None

            if active_short is not None and not active_short.used:
                if float(row["high"]) >= active_short.stop_loss:
                    LOG.debug(
                        "%s short_setup_invalidated symbol=%s bar=%d (stop blown pre-entry)",
                        self.name, row["symbol"], idx,
                    )
                    active_short = None

            prev_row = row

        return signal_rows

    # ── Pivot detection ───────────────────────────────────────────────────────

    def _try_confirm_pivot(
        self,
        arr_high: np.ndarray,
        arr_low: np.ndarray,
        arr_ts: np.ndarray,
        cand: int,
        lookback: int,
        confirmed_at: int,
    ) -> _Pivot | None:
        # Python loops with early-exit beat numpy .min()/.max() for small lookback windows
        # (numpy ufunc dispatch overhead is ~23μs; a 4-comparison Python loop is ~0.5μs).
        lo_start = cand - lookback
        hi_end   = cand + lookback + 1
        low  = float(arr_low[cand])
        high = float(arr_high[cand])

        is_low = True
        for k in range(lo_start, cand):
            if float(arr_low[k]) <= low:
                is_low = False
                break
        if is_low:
            for k in range(cand + 1, hi_end):
                if float(arr_low[k]) <= low:
                    is_low = False
                    break
        if is_low:
            return _Pivot("LOW", cand, confirmed_at, low, pd.Timestamp(arr_ts[cand]))

        for k in range(lo_start, cand):
            if float(arr_high[k]) >= high:
                return None
        for k in range(cand + 1, hi_end):
            if float(arr_high[k]) >= high:
                return None
        return _Pivot("HIGH", cand, confirmed_at, high, pd.Timestamp(arr_ts[cand]))

    # ── Trend detection ───────────────────────────────────────────────────────

    def _micro_trend(
        self,
        highs: list[_Pivot],
        lows: list[_Pivot],
    ) -> str | None:
        min_pts = max(int(self.min_swing_points), 2)
        # Bearish: latest confirmed high is lower than the previous confirmed high (lower high)
        if len(highs) >= min_pts and highs[-1].price < highs[-2].price:
            return "BEARISH"
        # Bullish: latest confirmed low is higher than the previous confirmed low (higher low)
        if len(lows) >= min_pts and lows[-1].price > lows[-2].price:
            return "BULLISH"
        return None

    # ── Setup builders ────────────────────────────────────────────────────────

    def _build_short_setup(
        self,
        impulse_high: float,
        impulse_low: float,
        confirmed_at: int,
        bos_bar=None,
    ) -> _BOSSetup | None:
        size = impulse_high - impulse_low
        if size <= getattr(self, "_cur_min_imp", float(self.min_impulse_points)):
            return None
        # Retracement levels: price bouncing UP from impulse_low toward impulse_high
        fib_50 = impulse_low + 0.50 * size
        fib_618 = impulse_low + float(self.fib_zone_high) * size
        # Target extends below impulse_low by (extension_ratio - 1) × size
        target = impulse_low - (float(self.target_extension_ratio) - 1.0) * size
        bos_bar_strong = False
        if bos_bar is not None and self.use_bos_strength_filter:
            bar_range = float(bos_bar["high"]) - float(bos_bar["low"])
            if bar_range > 0:
                # Strong SHORT bar: close in the bottom portion of the range
                bos_bar_strong = (float(bos_bar["high"]) - float(bos_bar["close"])) / bar_range >= self.bos_strength_threshold
        return _BOSSetup(
            direction="SHORT",
            impulse_high=impulse_high,
            impulse_low=impulse_low,
            impulse_size=size,
            fib_50=fib_50,
            fib_618=fib_618,
            stop_loss=impulse_high + getattr(self, "_cur_stop_buf", float(self.stop_buffer_points)),
            target=target,
            confirmed_at=confirmed_at,
            bos_bar_strong=bos_bar_strong,
            bos_level=impulse_low,  # broken support level; acts as resistance on retest
        )

    def _build_long_setup(
        self,
        impulse_low: float,
        impulse_high: float,
        confirmed_at: int,
        bos_bar=None,
    ) -> _BOSSetup | None:
        size = impulse_high - impulse_low
        if size <= getattr(self, "_cur_min_imp", float(self.min_impulse_points)):
            return None
        # Retracement levels: price pulling DOWN from impulse_high toward impulse_low
        fib_50 = impulse_high - 0.50 * size
        fib_618 = impulse_high - float(self.fib_zone_high) * size
        # Target extends above impulse_high by (extension_ratio - 1) × size
        target = impulse_high + (float(self.target_extension_ratio) - 1.0) * size
        bos_bar_strong = False
        if bos_bar is not None and self.use_bos_strength_filter:
            bar_range = float(bos_bar["high"]) - float(bos_bar["low"])
            if bar_range > 0:
                # Strong LONG bar: close in the top portion of the range
                bos_bar_strong = (float(bos_bar["close"]) - float(bos_bar["low"])) / bar_range >= self.bos_strength_threshold
        return _BOSSetup(
            direction="LONG",
            impulse_high=impulse_high,
            impulse_low=impulse_low,
            impulse_size=size,
            fib_50=fib_50,
            fib_618=fib_618,
            stop_loss=impulse_low - getattr(self, "_cur_stop_buf", float(self.stop_buffer_points)),
            target=target,
            confirmed_at=confirmed_at,
            bos_bar_strong=bos_bar_strong,
            bos_level=impulse_high,  # broken resistance level; acts as support on retest
        )

    # ── Entry logic ───────────────────────────────────────────────────────────

    def _try_entry(
        self,
        idx: int,
        row: pd.Series,
        setup: _BOSSetup,
    ) -> dict[str, object] | None:
        if self.use_bos_strength_filter:
            # Strong BOS setups are emitted at the BOS bar (setup.used=True);
            # only weak setups reach here — wait for broken-structure retest.
            return self._try_retest_rejection(row, setup)
        if self.entry_mode == "limit_618":
            return self._try_limit_618(row, setup)
        return self._try_zone_confirmation(idx, row, setup)

    def _try_limit_618(
        self,
        row: pd.Series,
        setup: _BOSSetup,
    ) -> dict[str, object] | None:
        close = float(row["close"])
        if setup.direction == "LONG":
            # Bar must touch fib_618 on the low, close at or above fib_618 (zone bounce),
            # AND close > open (bullish candle). A falling bar that wicks to fib_618 and
            # closes barely above it is still bearish price action — 4 consecutive red bars
            # ending at fib_618 is a crash into the zone, not a pullback-and-bounce.
            # Requiring close > open ensures the bar actually shows bullish intent at the level.
            if (float(row["low"]) <= setup.fib_618
                    and close >= setup.fib_618
                    and close > float(row["open"])
                    and close > setup.stop_loss):
                LOG.debug(
                    "%s limit_618 LONG trigger symbol=%s ts=%s low=%.2f fib618=%.2f close=%.2f open=%.2f stop=%.2f",
                    self.name, row["symbol"], row["timestamp"],
                    row["low"], setup.fib_618, close, row["open"], setup.stop_loss,
                )
                return self._signal_row(row, setup, "limit_618")
        else:
            # Bar must touch fib_618 on the high, close at or below fib_618 (zone rejection),
            # AND close < open (bearish candle). Symmetric to LONG: entering SHORT on a bullish
            # bar that bounced up to the zone and closed green is trading against the candle.
            if (float(row["high"]) >= setup.fib_618
                    and close <= setup.fib_618
                    and close < float(row["open"])
                    and close < setup.stop_loss
                    and close > setup.target):
                LOG.debug(
                    "%s limit_618 SHORT trigger symbol=%s ts=%s high=%.2f fib618=%.2f close=%.2f stop=%.2f target=%.2f",
                    self.name, row["symbol"], row["timestamp"],
                    row["high"], setup.fib_618, close, setup.stop_loss, setup.target,
                )
                return self._signal_row(row, setup, "limit_618")
        return None

    def _try_zone_confirmation(
        self,
        idx: int,
        row: pd.Series,
        setup: _BOSSetup,
    ) -> dict[str, object] | None:
        if setup.direction == "LONG":
            if not setup.zone_touched:
                # Zone is [fib_50, fib_618]. Price pulls back DOWN into it, so it
                # enters from the TOP — hitting fib_618 first, then fib_50 if deeper.
                # Trigger zone_touched at fib_618 (the zone entry), not fib_50 (the far side).
                # Using fib_50 made ZONE_CONFIRM harder to trigger than LIMIT618.
                if float(row["low"]) <= setup.fib_618:
                    setup.zone_touched = True
                    setup.zone_touched_at = idx
                    LOG.debug(
                        "%s zone_touched LONG symbol=%s ts=%s low=%.2f fib618=%.2f fib50=%.2f",
                        self.name, row["symbol"], row["timestamp"],
                        row["low"], setup.fib_618, setup.fib_50,
                    )
            elif idx == setup.zone_touched_at + 1:
                # Confirmation bar: must be bullish, close above stop_loss, close below target,
                # and remaining reward/risk must meet min_confirmation_rr.
                # The RR guard catches the case where the confirmation bar gaps up and rallies
                # close to the target, leaving almost no reward but full risk to the stop.
                close = float(row["close"])
                reward = setup.target - close
                risk   = close - setup.stop_loss
                rr_ok  = (risk <= 0) or (self.min_confirmation_rr <= 0) or (reward / risk >= float(self.min_confirmation_rr))
                if (float(row["close"]) > float(row["open"])
                        and close > setup.stop_loss
                        and close < setup.target
                        and rr_ok):
                    LOG.debug(
                        "%s zone_confirm LONG symbol=%s ts=%s close=%.2f open=%.2f stop=%.2f target=%.2f rr=%.2f",
                        self.name, row["symbol"], row["timestamp"],
                        row["close"], row["open"], setup.stop_loss, setup.target,
                        reward / risk if risk > 0 else 0,
                    )
                    return self._signal_row(row, setup, "zone_touch_confirmation")
                # Failed validation — reset and wait for another zone touch
                LOG.debug(
                    "%s zone_confirm LONG REJECTED symbol=%s ts=%s close=%.2f rr=%.2f (min=%.2f)",
                    self.name, row["symbol"], row["timestamp"], close,
                    reward / risk if risk > 0 else 0, float(self.min_confirmation_rr),
                )
                setup.zone_touched = False
                setup.zone_touched_at = -1
            elif idx > setup.zone_touched_at + 1:
                # Confirmation window expired
                setup.zone_touched = False
                setup.zone_touched_at = -1

        else:  # SHORT
            if not setup.zone_touched:
                # Zone is [fib_618, fib_50]. Price bounces UP into it, so it
                # enters from the BOTTOM — hitting fib_618 first, then fib_50 if higher.
                # Trigger zone_touched at fib_618 (the zone entry), not fib_50 (the far side).
                if float(row["high"]) >= setup.fib_618:
                    setup.zone_touched = True
                    setup.zone_touched_at = idx
                    LOG.debug(
                        "%s zone_touched SHORT symbol=%s ts=%s high=%.2f fib618=%.2f fib50=%.2f",
                        self.name, row["symbol"], row["timestamp"],
                        row["high"], setup.fib_618, setup.fib_50,
                    )
            elif idx == setup.zone_touched_at + 1:
                # Confirmation bar: must be bearish, close above target, and RR >= min_confirmation_rr.
                # The RR guard catches the case where the bar dumps close to impulse_low (=target),
                # leaving almost no reward but full risk to the stop.
                close = float(row["close"])
                reward = close - setup.target
                risk   = setup.stop_loss - close
                rr_ok  = (risk <= 0) or (self.min_confirmation_rr <= 0) or (reward / risk >= float(self.min_confirmation_rr))
                if float(row["close"]) < float(row["open"]) and close > setup.target and rr_ok:
                    LOG.debug(
                        "%s zone_confirm SHORT symbol=%s ts=%s close=%.2f open=%.2f target=%.2f rr=%.2f",
                        self.name, row["symbol"], row["timestamp"],
                        row["close"], row["open"], setup.target,
                        reward / risk if risk > 0 else 0,
                    )
                    return self._signal_row(row, setup, "zone_touch_confirmation")
                LOG.debug(
                    "%s zone_confirm SHORT REJECTED symbol=%s ts=%s close=%.2f rr=%.2f (min=%.2f)",
                    self.name, row["symbol"], row["timestamp"], close,
                    reward / risk if risk > 0 else 0, float(self.min_confirmation_rr),
                )
                setup.zone_touched = False
                setup.zone_touched_at = -1
            elif idx > setup.zone_touched_at + 1:
                setup.zone_touched = False
                setup.zone_touched_at = -1

        return None

    def _try_retest_rejection(
        self,
        row: pd.Series,
        setup: _BOSSetup,
    ) -> dict[str, object] | None:
        """Weak BOS entry: price must retest the broken structure level and show a
        rejection wick — touching the level on the bar's wick but closing above/below
        it (in the direction of the trade) with the close in the top/bottom 50% of range."""
        bar_range = float(row["high"]) - float(row["low"])
        if bar_range <= 0:
            return None
        if setup.direction == "LONG":
            bos_level = setup.bos_level  # impulse_high: broken resistance → now support
            if (float(row["low"]) <= bos_level
                    and float(row["close"]) > bos_level
                    and float(row["close"]) > float(row["open"])  # bullish bar
                    and float(row["close"]) > setup.stop_loss):
                # Rejection wick: close in upper half of bar range
                if (float(row["close"]) - float(row["low"])) / bar_range >= 0.5:
                    LOG.debug(
                        "%s retest_rejection LONG symbol=%s ts=%s low=%.2f bos=%.2f close=%.2f",
                        self.name, row["symbol"], row["timestamp"],
                        row["low"], bos_level, row["close"],
                    )
                    return self._signal_row(row, setup, "bos_retest")
        else:
            bos_level = setup.bos_level  # impulse_low: broken support → now resistance
            if (float(row["high"]) >= bos_level
                    and float(row["close"]) < bos_level
                    and float(row["close"]) < float(row["open"])  # bearish bar
                    and float(row["close"]) < setup.stop_loss
                    and float(row["close"]) > setup.target):
                # Rejection wick: close in lower half of bar range
                if (float(row["high"]) - float(row["close"])) / bar_range >= 0.5:
                    LOG.debug(
                        "%s retest_rejection SHORT symbol=%s ts=%s high=%.2f bos=%.2f close=%.2f",
                        self.name, row["symbol"], row["timestamp"],
                        row["high"], bos_level, row["close"],
                    )
                    return self._signal_row(row, setup, "bos_retest")
        return None

    # ── Optional indicator filters (no-ops when disabled) ─────────────────────

    def _passes_filters(self, row: dict, direction: str) -> bool:
        close = row["close"]
        if self.use_vwap_filter:
            vwap = row.get("vwap")
            if vwap is None or vwap != vwap:  # None or NaN
                return False
            if direction == "LONG" and close <= vwap:
                return False
            if direction == "SHORT" and close >= vwap:
                return False
        if self.use_ema_filter:
            ema = row.get("ema_fast")
            if ema is None or ema != ema:
                return False
            if direction == "LONG" and close <= ema:
                return False
            if direction == "SHORT" and close >= ema:
                return False
        return True

    # ── Signal row builder ────────────────────────────────────────────────────

    def _signal_row(
        self,
        row: pd.Series,
        setup: _BOSSetup,
        trigger: str,
    ) -> dict[str, object]:
        if trigger == "limit_618":
            entry_price = float(setup.fib_618)
        else:
            # bos_breakout: entry_price ignored (fill at N+1 open via pending_fill_mode)
            # bos_retest: fill at this bar's close (the rejection wick bar close)
            # zone_touch_confirmation: fill at bar close
            entry_price = float(row["close"])

        if trigger == "bos_breakout":
            pending_fill_mode = "next_bar_open"
        elif trigger == "bos_retest":
            pending_fill_mode = "fill_at_limit_first_bar"
        else:
            pending_fill_mode = "config"

        # Step-up trail milestones: ordered Fib price levels from nearest to farthest.
        # The backtester steps the stop up to each confirmed level (one-bar delay).
        # The LAST milestone is always the configured target and triggers the 50% partial exit.
        ext = float(self.target_extension_ratio)
        if setup.direction == "LONG":
            milestones: list[float] = []
            if ext > 1.0:
                milestones.append(float(setup.impulse_high))
            if ext > 1.272:
                milestones.append(float(setup.impulse_high) + 0.272 * float(setup.impulse_size))
            milestones.append(float(setup.target))
        else:
            milestones = []
            if ext > 1.0:
                milestones.append(float(setup.impulse_low))
            if ext > 1.272:
                milestones.append(float(setup.impulse_low) - 0.272 * float(setup.impulse_size))
            milestones.append(float(setup.target))

        return {
            "timestamp": row["timestamp"],
            "symbol": row["symbol"],
            "direction": setup.direction,
            "strength": float(setup.impulse_size),
            "reason": (
                f"bos_fib_{setup.direction.lower()} trigger={trigger} "
                f"impulse_high={setup.impulse_high:.2f} impulse_low={setup.impulse_low:.2f} "
                f"fib50={setup.fib_50:.2f} fib618={setup.fib_618:.2f} "
                f"stop={setup.stop_loss:.2f} target={setup.target:.2f}"
            ),
            "stop_loss": float(setup.stop_loss),
            "target": float(setup.target),
            "entry_price": entry_price,
            "trail_milestones": milestones,
            "pending_fill_mode": pending_fill_mode,
            "strategy_name": self.name,
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _max_trades_reached(self, trades_today: int) -> bool:
        if self.max_trades_per_day is None:
            return False
        return trades_today >= int(self.max_trades_per_day)

    def _warn_non_1min(self, group: pd.DataFrame) -> None:
        if len(group) < 2:
            return
        deltas = pd.to_datetime(group["timestamp"]).diff().dropna()
        invalid = deltas[deltas != pd.Timedelta(minutes=1)]
        if not invalid.empty:
            LOG.warning(
                "%s expects 1-minute candles but found gap %s for %s %s",
                self.name, invalid.iloc[0], group.iloc[0]["symbol"], group.iloc[0]["trade_date"],
            )
