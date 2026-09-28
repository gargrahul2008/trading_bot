"""
All configurable parameters for the bullish BTST stock-selection engine.

Every threshold used anywhere in the selector lives here — indicator modules
never hard-code tunables. Field names are the snake_case equivalents of the
spec's UPPER_CASE parameter names (e.g. MIN_ENTRY_SCORE -> min_entry_score).
"""
from __future__ import annotations

from dataclasses import dataclass, field

DATA_MODE_PRE_CLOSE_SNAPSHOT = 'PRE_CLOSE_SNAPSHOT'
DATA_MODE_PREVIOUS_COMPLETED_DAY = 'PREVIOUS_COMPLETED_DAY'
VALID_DATA_MODES = (DATA_MODE_PRE_CLOSE_SNAPSHOT, DATA_MODE_PREVIOUS_COMPLETED_DAY)

# Policy for optional datasets that may be missing at selection time.
POLICY_FAIL_OPEN = 'fail_open'      # treat missing data as "no problem", record a warning
POLICY_FAIL_CLOSED = 'fail_closed'  # treat missing data as a rejection
VALID_MISSING_DATA_POLICIES = (POLICY_FAIL_OPEN, POLICY_FAIL_CLOSED)

# Policy for missing historical same-time intraday volume in snapshot mode.
SAME_TIME_VOLUME_STRICT = 'strict'  # reject the stock
SAME_TIME_VOLUME_WARN = 'warn'      # skip the check, zero volume points, record warning
VALID_SAME_TIME_VOLUME_POLICIES = (SAME_TIME_VOLUME_STRICT, SAME_TIME_VOLUME_WARN)

MARKET_STATE_ON = 'ON'
MARKET_STATE_OFF = 'OFF'
VALID_MARKET_STATES = (MARKET_STATE_ON, MARKET_STATE_OFF)


@dataclass
class SelectorConfig:
    # --- data / timing -----------------------------------------------------
    data_mode: str = DATA_MODE_PRE_CLOSE_SNAPSHOT
    timezone: str = 'Asia/Kolkata'
    selection_cutoff_time: str = '15:15:00'
    benchmark_symbol: str = 'NIFTY500'

    min_daily_history: int = 260
    min_weekly_history: int = 35

    min_price: float = 100.0

    # --- indicator periods -------------------------------------------------
    ema_short_period: int = 20
    sma_medium_period: int = 50
    sma_long_period: int = 200

    weekly_ema_period: int = 10
    weekly_sma_period: int = 30

    atr_period: int = 14

    rs_short_period: int = 20
    rs_medium_period: int = 63
    rs_long_period: int = 126

    recent_high_lookback: int = 20
    year_high_lookback: int = 252

    turnover_lookback: int = 20
    same_time_volume_lookback: int = 20

    # slope lookbacks (sessions / completed weeks)
    ema_slope_lookback: int = 5
    sma_50_slope_lookback: int = 20
    sma_200_slope_lookback: int = 20
    weekly_slope_lookback: int = 4
    sector_sma_slope_lookback: int = 20

    # --- liquidity ----------------------------------------------------------
    expected_max_stock_order_value: float = 200000.0
    max_order_as_pct_of_median_turnover: float = 0.002
    absolute_min_median_turnover: float = 100000000.0
    max_bid_ask_spread_percent: float = 0.005
    # liquidity below this fraction of required turnover is a "material" failure -> hard_fail
    hard_fail_liquidity_fraction: float = 0.5

    # --- hard-filter thresholds ----------------------------------------------
    min_52_week_high_proximity: float = 0.85
    min_rs_composite_percentile: float = 60.0
    min_entry_rs_percentile: float = 70.0
    min_continuation_rs_percentile: float = 50.0

    min_close_location_value: float = 0.60
    min_same_time_volume_ratio: float = 0.90

    min_day_return: float = 0.0025
    max_day_return: float = 0.045

    min_atr_percent: float = 0.008
    max_atr_percent: float = 0.060

    max_extension_from_ema20_atr: float = 2.50
    max_absolute_opening_gap: float = 0.040

    # --- score thresholds -----------------------------------------------------
    min_entry_score: float = 75.0
    min_continuation_score: float = 65.0
    min_watchlist_score: float = 60.0

    min_entry_trend_score: float = 18.0
    min_entry_preclose_score: float = 12.0

    max_output_candidates: int = 20

    # --- toggles ----------------------------------------------------------------
    require_market_on_for_entry: bool = True
    require_sector_bullish: bool = False
    exclude_known_event_risk: bool = True
    exclude_restricted_securities: bool = True
    exclude_circuit_locked_securities: bool = True
    require_btst_eligibility: bool = True

    allow_long_calendar_gap_entry: bool = False
    max_calendar_gap_days: int = 3

    enable_diversified_output: bool = True
    max_top_candidates_per_sector: int = 3

    # --- missing-data policies ----------------------------------------------------
    same_time_volume_policy: str = SAME_TIME_VOLUME_STRICT
    event_data_missing_policy: str = POLICY_FAIL_OPEN
    restriction_data_missing_policy: str = POLICY_FAIL_OPEN
    btst_eligibility_missing_policy: str = POLICY_FAIL_OPEN

    # --- scoring band parameters (see spec sections 18-22) --------------------------
    score_high_proximity_low: float = 0.85
    score_high_proximity_high: float = 1.00
    score_breakout_low: float = 0.97
    score_breakout_high: float = 1.02
    score_volume_ratio_high: float = 2.00
    score_vwap_distance_high: float = 0.01
    score_extension_full_points: float = 1.50
    score_atr_quality_low: float = 0.010
    score_atr_quality_high: float = 0.040
    score_gap_full_points: float = 0.015
    score_gap_half_points: float = 0.030

    # --- output ----------------------------------------------------------------------
    output_dir: str = 'artifacts/stock_selector'
    reason_delimiter: str = '|'

    def validate(self) -> None:
        """Raise ValueError on any invalid configuration value."""
        if self.data_mode not in VALID_DATA_MODES:
            raise ValueError(f'invalid data_mode: {self.data_mode!r}')
        parts = self.selection_cutoff_time.split(':')
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            raise ValueError(f'invalid selection_cutoff_time: {self.selection_cutoff_time!r}')
        if self.same_time_volume_policy not in VALID_SAME_TIME_VOLUME_POLICIES:
            raise ValueError(f'invalid same_time_volume_policy: {self.same_time_volume_policy!r}')
        for name in ('event_data_missing_policy', 'restriction_data_missing_policy',
                     'btst_eligibility_missing_policy'):
            if getattr(self, name) not in VALID_MISSING_DATA_POLICIES:
                raise ValueError(f'invalid {name}: {getattr(self, name)!r}')
        positive_ints = (
            'min_daily_history', 'min_weekly_history', 'ema_short_period',
            'sma_medium_period', 'sma_long_period', 'weekly_ema_period',
            'weekly_sma_period', 'atr_period', 'rs_short_period', 'rs_medium_period',
            'rs_long_period', 'recent_high_lookback', 'year_high_lookback',
            'turnover_lookback', 'same_time_volume_lookback', 'max_output_candidates',
            'ema_slope_lookback', 'sma_50_slope_lookback', 'sma_200_slope_lookback',
            'weekly_slope_lookback', 'max_top_candidates_per_sector',
        )
        for name in positive_ints:
            if getattr(self, name) <= 0:
                raise ValueError(f'{name} must be positive')
        ordered_pairs = (
            ('min_day_return', 'max_day_return'),
            ('min_atr_percent', 'max_atr_percent'),
        )
        for low, high in ordered_pairs:
            if getattr(self, low) >= getattr(self, high):
                raise ValueError(f'{low} must be below {high}')
        for name in ('min_entry_score', 'min_continuation_score', 'min_watchlist_score'):
            if not 0.0 <= getattr(self, name) <= 100.0:
                raise ValueError(f'{name} must be within [0, 100]')
        if not 0.0 <= self.min_rs_composite_percentile <= 100.0:
            raise ValueError('min_rs_composite_percentile must be within [0, 100]')
        if not 0.0 <= self.min_entry_rs_percentile <= 100.0:
            raise ValueError('min_entry_rs_percentile must be within [0, 100]')
        if self.max_order_as_pct_of_median_turnover <= 0:
            raise ValueError('max_order_as_pct_of_median_turnover must be positive')
        if self.expected_max_stock_order_value <= 0:
            raise ValueError('expected_max_stock_order_value must be positive')
        if not 0.0 < self.hard_fail_liquidity_fraction <= 1.0:
            raise ValueError('hard_fail_liquidity_fraction must be in (0, 1]')

    @property
    def required_turnover(self) -> float:
        dynamic = self.expected_max_stock_order_value / self.max_order_as_pct_of_median_turnover
        return max(self.absolute_min_median_turnover, dynamic)
