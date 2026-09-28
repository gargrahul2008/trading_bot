"""Hard filters, eligibility, hard failure, status and reason codes
(spec sections 15, 16, 23-27).

Convention for threshold comparisons: minimums and maximums are inclusive
(score == 75 passes, extension == 2.5 passes); price-versus-level conditions
are strict (last_price > sma_200, last_price > vwap) as written in the spec.

The global market filter is never mixed into ``stock_hard_filter_pass`` and
market OFF is never a stock-specific ``hard_fail``.
"""
from __future__ import annotations

import math

from .config import (
    MARKET_STATE_ON,
    SAME_TIME_VOLUME_STRICT,
    SelectorConfig,
)
from .models import (
    STATUS_CONTINUE_ONLY,
    STATUS_ENTRY_ELIGIBLE,
    STATUS_HARD_FAIL,
    STATUS_MARKET_BLOCKED,
    STATUS_REJECTED,
    STATUS_WATCHLIST,
)

NAN = float('nan')


def _num(value: object) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return NAN
    return result if math.isfinite(result) else NAN


def _cmp(a: object, b: object, op: str) -> bool:
    """NaN-safe comparison; missing data never passes a check."""
    a, b = _num(a), _num(b)
    if not (math.isfinite(a) and math.isfinite(b)):
        return False
    if op == 'gt':
        return a > b
    if op == 'ge':
        return a >= b
    if op == 'lt':
        return a < b
    if op == 'le':
        return a <= b
    raise ValueError(op)


def _spread_available(row: dict) -> bool:
    return math.isfinite(_num(row.get('bid_ask_spread_percent')))


def evaluate_hard_filters(row: dict, config: SelectorConfig) -> tuple[bool, list[str]]:
    """Mandatory stock-specific hard filters (spec section 15).

    Returns (stock_hard_filter_pass, rejection_reasons). Independent from the
    market-state filter by construction.
    """
    reasons: list[str] = []

    def check(passed: bool, reason: str) -> None:
        if not passed:
            reasons.append(reason)

    # data and tradeability
    check(row.get('daily_history_sessions', 0) >= config.min_daily_history, 'INSUFFICIENT_HISTORY')
    check(row.get('weekly_bars', 0) >= config.min_weekly_history, 'INSUFFICIENT_HISTORY')
    check(bool(row.get('tradable')), 'NOT_TRADABLE')
    if config.require_btst_eligibility:
        check(bool(row.get('btst_eligible')), 'BTST_NOT_ELIGIBLE')
    if config.exclude_restricted_securities:
        check(not row.get('restricted_security'), 'RESTRICTED_SECURITY')
    if config.exclude_known_event_risk:
        check(not row.get('event_risk'), 'EVENT_RISK')
    check(not row.get('corporate_action_risk'), 'CORPORATE_ACTION_RISK')
    check(bool(row.get('data_valid')), 'INVALID_DATA')
    check(bool(row.get('snapshot_valid')), row.get('snapshot_reject_reason', 'MISSING_CURRENT_SNAPSHOT'))

    # price and liquidity
    check(_cmp(row.get('last_price'), config.min_price, 'ge'), 'PRICE_BELOW_MINIMUM')
    check(bool(row.get('liquidity_pass')), 'LIQUIDITY_TOO_LOW')
    if config.exclude_circuit_locked_securities:
        check(not row.get('circuit_locked'), 'CIRCUIT_LOCKED')
    if _spread_available(row):
        check(
            _cmp(row.get('bid_ask_spread_percent'), config.max_bid_ask_spread_percent, 'le'),
            'SPREAD_TOO_WIDE',
        )

    # long-term structure
    check(_cmp(row.get('last_price'), row.get('sma_200'), 'gt'), 'BELOW_SMA200')
    check(_cmp(row.get('sma_50'), row.get('sma_200'), 'gt'), 'SMA50_NOT_ABOVE_SMA200')
    check(bool(row.get('sma_200_rising')), 'SMA200_NOT_RISING')

    # short-term structure
    check(_cmp(row.get('last_price'), row.get('ema_20'), 'gt'), 'BELOW_EMA20')
    check(bool(row.get('ema_20_rising')), 'EMA20_NOT_RISING')

    # weekly structure
    check(_cmp(row.get('weekly_close'), row.get('weekly_sma_30'), 'gt'), 'WEEKLY_TREND_NOT_BULLISH')
    check(bool(row.get('weekly_sma_30_rising')), 'WEEKLY_SMA30_NOT_RISING')

    # relative strength / price position
    check(
        _cmp(row.get('rs_composite_percentile'), config.min_rs_composite_percentile, 'ge'),
        'RELATIVE_STRENGTH_TOO_LOW',
    )
    check(
        _cmp(row.get('high_52_week_proximity'), config.min_52_week_high_proximity, 'ge'),
        'TOO_FAR_BELOW_52_WEEK_HIGH',
    )

    # pre-close confirmation
    if row.get('vwap_available', True):
        check(_cmp(row.get('last_price'), row.get('vwap_to_cutoff'), 'gt'), 'BELOW_VWAP')
    check(
        _cmp(row.get('close_location_value'), config.min_close_location_value, 'ge'),
        'WEAK_CLOSE_LOCATION',
    )
    if row.get('same_time_volume_available', False):
        check(
            _cmp(row.get('same_time_volume_ratio'), config.min_same_time_volume_ratio, 'ge'),
            'LOW_SAME_TIME_VOLUME',
        )
    elif config.same_time_volume_policy == SAME_TIME_VOLUME_STRICT:
        check(False, 'SAME_TIME_VOLUME_UNAVAILABLE')
    check(_cmp(row.get('day_return'), config.min_day_return, 'ge'), 'DAY_RETURN_TOO_LOW')
    check(_cmp(row.get('day_return'), config.max_day_return, 'le'), 'DAY_RETURN_TOO_HIGH')

    # risk control
    check(_cmp(row.get('atr_percent'), config.min_atr_percent, 'ge'), 'ATR_TOO_LOW')
    check(_cmp(row.get('atr_percent'), config.max_atr_percent, 'le'), 'ATR_TOO_HIGH')
    check(
        _cmp(row.get('extension_from_ema20_atr'), config.max_extension_from_ema20_atr, 'le'),
        'OVEREXTENDED_FROM_EMA20',
    )
    gap = _num(row.get('opening_gap'))
    check(
        math.isfinite(gap) and abs(gap) <= config.max_absolute_opening_gap,
        'OPENING_GAP_TOO_LARGE',
    )
    if not config.allow_long_calendar_gap_entry and row.get('calendar_gap_ok') is False:
        check(False, 'LONG_CALENDAR_GAP')

    if config.require_sector_bullish:
        check(bool(row.get('sector_trend_bullish')), 'SECTOR_TREND_NOT_BULLISH')
        check(bool(row.get('sector_rs_bullish')), 'SECTOR_RS_NOT_BULLISH')

    return len(reasons) == 0, reasons


def evaluate_hard_fail(row: dict, config: SelectorConfig) -> bool:
    """Stock-specific hard failure (spec section 25). Market OFF never
    contributes here."""
    if not row.get('tradable'):
        return True
    if config.require_btst_eligibility and row.get('btst_data_available') and not row.get('btst_eligible'):
        return True
    if row.get('restriction_data_available') and row.get('restricted_security'):
        return True
    if row.get('circuit_locked'):
        return True
    if row.get('event_data_available') and row.get('event_risk'):
        return True
    if row.get('corporate_action_data_available') and row.get('corporate_action_risk'):
        return True
    if not row.get('data_valid') or not row.get('snapshot_valid'):
        return True
    if row.get('daily_history_sessions', 0) < config.min_daily_history:
        return True
    if row.get('weekly_bars', 0) < config.min_weekly_history:
        return True
    median_turnover = _num(row.get('median_traded_value_20'))
    required = _num(row.get('required_turnover'))
    if math.isfinite(required) and (
        not math.isfinite(median_turnover)
        or median_turnover < config.hard_fail_liquidity_fraction * required
    ):
        return True
    if _cmp(row.get('last_price'), row.get('sma_200'), 'lt'):
        return True
    if _cmp(row.get('sma_50'), row.get('sma_200'), 'lt'):
        return True
    return False


def evaluate_continuation(row: dict, config: SelectorConfig) -> bool:
    """Continuation eligibility for stocks already held (spec section 24).
    Portfolio state is never consulted — this is only an output flag."""
    if row.get('hard_fail'):
        return False
    if not row.get('tradable'):
        return False
    if config.require_btst_eligibility and not row.get('btst_eligible'):
        return False
    if row.get('restricted_security') or row.get('event_risk') or row.get('circuit_locked'):
        return False
    if not row.get('liquidity_pass'):
        return False
    return (
        _cmp(row.get('last_price'), row.get('sma_50'), 'gt')
        and _cmp(row.get('sma_50'), row.get('sma_200'), 'gt')
        and bool(row.get('sma_200_rising'))
        and _cmp(row.get('weekly_close'), row.get('weekly_sma_30'), 'gt')
        and _cmp(row.get('rs_composite_percentile'), config.min_continuation_rs_percentile, 'ge')
        and _cmp(row.get('bullish_score'), config.min_continuation_score, 'ge')
        and _cmp(row.get('extension_from_ema20_atr'), config.max_extension_from_ema20_atr, 'le')
        and _cmp(row.get('atr_percent'), config.min_atr_percent, 'ge')
        and _cmp(row.get('atr_percent'), config.max_atr_percent, 'le')
    )


def _acceptance_reasons(row: dict, config: SelectorConfig) -> list[str]:
    reasons: list[str] = []

    def note(condition: bool, code: str) -> None:
        if condition:
            reasons.append(code)

    note(_cmp(row.get('last_price'), row.get('sma_200'), 'gt'), 'ABOVE_SMA200')
    note(_cmp(row.get('sma_50'), row.get('sma_200'), 'gt'), 'SMA50_ABOVE_SMA200')
    note(bool(row.get('sma_200_rising')), 'SMA200_RISING')
    note(_cmp(row.get('last_price'), row.get('ema_20'), 'gt'), 'ABOVE_EMA20')
    note(bool(row.get('ema_20_rising')), 'EMA20_RISING')
    note(bool(row.get('weekly_bullish_alignment')), 'WEEKLY_TREND_BULLISH')
    note(_cmp(row.get('rs_composite_percentile'), 70.0, 'ge'), 'RS_TOP_30_PERCENT')
    note(
        _cmp(row.get('high_52_week_proximity'), config.min_52_week_high_proximity, 'ge'),
        'NEAR_52_WEEK_HIGH',
    )
    note(_cmp(row.get('last_price'), row.get('vwap_to_cutoff'), 'gt'), 'ABOVE_VWAP')
    note(
        _cmp(row.get('close_location_value'), config.min_close_location_value, 'ge'),
        'STRONG_CLOSE_LOCATION',
    )
    note(
        row.get('same_time_volume_available', False)
        and _cmp(row.get('same_time_volume_ratio'), config.min_same_time_volume_ratio, 'ge'),
        'STRONG_SAME_TIME_VOLUME',
    )
    note(bool(row.get('sector_trend_bullish')), 'POSITIVE_SECTOR_TREND')
    note(_cmp(row.get('breakout_ratio'), 1.0, 'ge'), 'RECENT_HIGH_BREAKOUT')
    return reasons


def classify_stock(row: dict, config: SelectorConfig, market_state: str) -> None:
    """Mutate ``row`` in place with eligibility flags, status and reasons.

    ``row`` must already contain indicator values and sub-scores.
    """
    hard_filter_pass, rejection_reasons = evaluate_hard_filters(row, config)
    hard_fail = evaluate_hard_fail(row, config)

    row['stock_hard_filter_pass'] = hard_filter_pass
    row['hard_fail'] = hard_fail

    market_entry_allowed = (
        market_state == MARKET_STATE_ON or not config.require_market_on_for_entry
    )
    row['market_state'] = market_state
    row['market_entry_allowed'] = market_entry_allowed

    score_ok = _cmp(row.get('bullish_score'), config.min_entry_score, 'ge')
    rs_ok = _cmp(row.get('rs_composite_percentile'), config.min_entry_rs_percentile, 'ge')
    trend_ok = _cmp(row.get('trend_score'), config.min_entry_trend_score, 'ge')
    preclose_ok = _cmp(row.get('preclose_score'), config.min_entry_preclose_score, 'ge')

    if hard_filter_pass and not hard_fail:
        if not score_ok:
            rejection_reasons.append('SCORE_BELOW_ENTRY_THRESHOLD')
        if not rs_ok:
            rejection_reasons.append('RELATIVE_STRENGTH_TOO_LOW')
        if not trend_ok:
            rejection_reasons.append('TREND_SCORE_TOO_LOW')
        if not preclose_ok:
            rejection_reasons.append('PRECLOSE_SCORE_TOO_LOW')

    entry_conditions_without_market = (
        hard_filter_pass and not hard_fail
        and score_ok and rs_ok and trend_ok and preclose_ok
    )
    entry_eligible = entry_conditions_without_market and market_entry_allowed
    continue_eligible = evaluate_continuation(row, config)

    if entry_conditions_without_market and not market_entry_allowed:
        rejection_reasons.append('MARKET_FILTER_OFF')

    if hard_fail:
        status = STATUS_HARD_FAIL
    elif entry_eligible:
        status = STATUS_ENTRY_ELIGIBLE
    elif entry_conditions_without_market and not market_entry_allowed:
        status = STATUS_MARKET_BLOCKED
    elif continue_eligible:
        status = STATUS_CONTINUE_ONLY
    elif hard_filter_pass and _cmp(row.get('bullish_score'), config.min_watchlist_score, 'ge'):
        status = STATUS_WATCHLIST
    else:
        status = STATUS_REJECTED

    # deduplicate, preserving first-seen order
    seen: set[str] = set()
    rejection_reasons = [r for r in rejection_reasons if not (r in seen or seen.add(r))]

    row['entry_eligible'] = bool(entry_eligible)
    row['continue_eligible'] = bool(continue_eligible)
    row['status'] = status
    row['acceptance_reasons'] = _acceptance_reasons(row, config)
    row['rejection_reasons'] = rejection_reasons
