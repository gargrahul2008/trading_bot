from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from .types import CombinedSignal


SCORE_BUCKET_EDGES = [55.0, 60.0, 65.0, 70.0, 75.0]
TREND_STRENGTH_BUCKET_EDGES = [0.25, 0.5, 0.75, 1.0]
VWAP_GAP_BUCKET_EDGES = [0.25, 0.5, 0.75, 1.0]


@dataclass(frozen=True)
class BenchmarkFilterConfig:
    use_benchmark_filter: bool = False
    require_benchmark_alignment: bool = False
    allow_flat_benchmark: bool = True
    allow_against_benchmark_above_score: float | None = None
    benchmark_vwap_gap_min_atr: float | None = None
    benchmark_vwap_gap_max_atr: float | None = None
    benchmark_trend_strength_min_atr: float | None = None
    benchmark_trend_strength_max_atr: float | None = None
    symbol_vwap_gap_max_atr: float | None = None
    symbol_trend_gap_max_atr: float | None = None


def _reward_to_risk(signal: CombinedSignal) -> float | None:
    if signal.stop_price is None or signal.target_price is None:
        return None
    risk = abs(float(signal.price) - float(signal.stop_price))
    if risk <= 0:
        return None
    reward = abs(float(signal.target_price) - float(signal.price))
    return reward / risk


def _bucket_labels(edges: list[float]) -> list[str]:
    labels: list[str] = []
    lower = None
    for edge in edges:
        if lower is None:
            labels.append(f"<{edge:g}")
        else:
            labels.append(f"{lower:g}-{edge:g}")
        lower = edge
    labels.append(f"{edges[-1]:g}+")
    return labels


def _assign_bucket(series: pd.Series, edges: list[float], suffix: str = "") -> pd.Series:
    bins = [-np.inf, *edges, np.inf]
    labels = [label + suffix for label in _bucket_labels(edges)]
    return pd.cut(series, bins=bins, labels=labels, right=False)


def assign_audit_buckets(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        enriched = frame.copy()
        for column in (
            "signal_score_bucket",
            "benchmark_trend_strength_bucket",
            "benchmark_vwap_gap_bucket",
        ):
            enriched[column] = pd.Series(dtype="object")
        return enriched

    enriched = frame.copy()
    enriched["signal_score_bucket"] = _assign_bucket(enriched["signal_score"], SCORE_BUCKET_EDGES)
    enriched["benchmark_trend_strength_bucket"] = _assign_bucket(
        enriched["benchmark_trend_gap_atr_abs"],
        TREND_STRENGTH_BUCKET_EDGES,
        suffix=" ATR",
    )
    enriched["benchmark_vwap_gap_bucket"] = _assign_bucket(
        enriched["benchmark_vwap_gap_atr_abs"],
        VWAP_GAP_BUCKET_EDGES,
        suffix=" ATR",
    )
    return enriched


class BenchmarkAwareSignalFilter:
    """Notebook-oriented benchmark and symbol context filter for executable signals."""

    def __init__(self, benchmark_features: pd.DataFrame, config: BenchmarkFilterConfig) -> None:
        self.benchmark_features = benchmark_features.copy()
        self.config = config

    def context_for_signals(
        self,
        signals: Iterable[CombinedSignal],
        featured: pd.DataFrame,
    ) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for idx, signal in enumerate(signals):
            rows.append(
                {
                    "signal_index": idx,
                    "timestamp": pd.Timestamp(signal.timestamp),
                    "symbol": signal.symbol,
                    "side": signal.side.value,
                    "signal_score": float(signal.final_score),
                    "entry_price": float(signal.price),
                    "stop_price": signal.stop_price,
                    "target_price": signal.target_price,
                    "reward_to_risk": _reward_to_risk(signal),
                    "strategy_name": signal.strategy_name,
                    "explanation": signal.explanation,
                }
            )
        signal_frame = pd.DataFrame(rows)
        if signal_frame.empty:
            return signal_frame

        symbol_context = featured[
            [
                "timestamp",
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "atr",
                "ema_fast",
                "ema_slow",
                "trend_gap_atr",
            ]
        ].copy()
        benchmark_context = self.benchmark_features[
            [
                "timestamp",
                "close",
                "vwap",
                "atr",
                "ema_fast",
                "ema_slow",
                "benchmark_regime",
                "benchmark_close_change_pct",
            ]
        ].rename(
            columns={
                "close": "benchmark_close",
                "vwap": "benchmark_vwap",
                "atr": "benchmark_atr",
                "ema_fast": "benchmark_ema_fast",
                "ema_slow": "benchmark_ema_slow",
            }
        )

        enriched = signal_frame.merge(symbol_context, on=["timestamp", "symbol"], how="left")
        enriched = enriched.merge(benchmark_context, on="timestamp", how="left")

        symbol_atr = enriched["atr"].replace(0.0, np.nan)
        benchmark_atr = enriched["benchmark_atr"].replace(0.0, np.nan)
        side_sign = np.where(enriched["side"].eq("LONG"), 1.0, -1.0)

        enriched["entry_distance_from_ema_atr"] = (
            (enriched["entry_price"] - enriched["ema_fast"]).abs() / symbol_atr
        )
        enriched["entry_distance_from_vwap_atr"] = (
            (enriched["entry_price"] - enriched["vwap"]).abs() / symbol_atr
        )
        enriched["symbol_vwap_gap_atr"] = (enriched["close"] - enriched["vwap"]) / symbol_atr
        enriched["symbol_vwap_gap_atr_abs"] = enriched["symbol_vwap_gap_atr"].abs()
        enriched["symbol_trend_gap_atr_abs"] = enriched["trend_gap_atr"].abs()
        enriched["pullback_depth_atr"] = np.where(
            enriched["side"].eq("LONG"),
            (enriched["ema_fast"] - enriched["low"]) / symbol_atr,
            (enriched["high"] - enriched["ema_fast"]) / symbol_atr,
        )
        enriched["stop_distance_atr"] = (
            (enriched["entry_price"] - enriched["stop_price"]).abs() / symbol_atr
        )
        enriched["benchmark_trend_gap_atr"] = (
            (enriched["benchmark_ema_fast"] - enriched["benchmark_ema_slow"]) / benchmark_atr
        )
        enriched["benchmark_trend_gap_atr_abs"] = enriched["benchmark_trend_gap_atr"].abs()
        enriched["benchmark_vwap_gap_atr"] = (
            (enriched["benchmark_close"] - enriched["benchmark_vwap"]) / benchmark_atr
        )
        enriched["benchmark_vwap_gap_atr_abs"] = enriched["benchmark_vwap_gap_atr"].abs()
        enriched["benchmark_aligned"] = np.where(
            enriched["benchmark_regime"].eq("FLAT"),
            False,
            enriched["side"].eq(enriched["benchmark_regime"]),
        )
        enriched["benchmark_alignment_bucket"] = np.where(
            enriched["benchmark_regime"].eq("FLAT"),
            "flat_benchmark",
            np.where(enriched["benchmark_aligned"], "aligned", "against_benchmark"),
        )
        enriched["benchmark_alignment_signed"] = np.where(
            enriched["benchmark_alignment_bucket"].eq("aligned"),
            side_sign,
            np.where(enriched["benchmark_alignment_bucket"].eq("against_benchmark"), -side_sign, 0.0),
        )
        enriched["entry_hour"] = pd.to_datetime(enriched["timestamp"]).dt.strftime("%H:00")
        return assign_audit_buckets(enriched)

    def __call__(
        self,
        signals: list[CombinedSignal],
        featured: pd.DataFrame,
    ) -> tuple[list[CombinedSignal], list[dict[str, object]]]:
        signal_context = self.context_for_signals(signals, featured)
        if signal_context.empty:
            return [], []

        accepted: list[CombinedSignal] = []
        rejected_rows: list[dict[str, object]] = []

        for row in signal_context.itertuples():
            reason = self._rejection_reason(row)
            if reason is None:
                accepted.append(signals[int(row.signal_index)])
                continue
            rejected_rows.append(
                {
                    "timestamp": row.timestamp,
                    "symbol": row.symbol,
                    "strategy_name": row.strategy_name,
                    "side": row.side,
                    "signal_score": row.signal_score,
                    "reward_to_risk": row.reward_to_risk,
                    "reason": reason,
                    "stage": "benchmark_filter",
                    "benchmark_alignment_bucket": row.benchmark_alignment_bucket,
                    "benchmark_vwap_gap_bucket": row.benchmark_vwap_gap_bucket,
                    "benchmark_trend_strength_bucket": row.benchmark_trend_strength_bucket,
                    "entry_distance_from_ema_atr": row.entry_distance_from_ema_atr,
                    "entry_distance_from_vwap_atr": row.entry_distance_from_vwap_atr,
                    "pullback_depth_atr": row.pullback_depth_atr,
                    "stop_distance_atr": row.stop_distance_atr,
                    "entry_hour": row.entry_hour,
                }
            )
        return accepted, rejected_rows

    def _rejection_reason(self, row) -> str | None:
        config = self.config

        if config.symbol_vwap_gap_max_atr is not None and pd.notna(row.symbol_vwap_gap_atr_abs):
            if float(row.symbol_vwap_gap_atr_abs) > float(config.symbol_vwap_gap_max_atr):
                return f"symbol_vwap_gap_above_max_atr:{float(config.symbol_vwap_gap_max_atr):.3f}"

        if config.symbol_trend_gap_max_atr is not None and pd.notna(row.symbol_trend_gap_atr_abs):
            if float(row.symbol_trend_gap_atr_abs) > float(config.symbol_trend_gap_max_atr):
                return f"symbol_trend_gap_above_max_atr:{float(config.symbol_trend_gap_max_atr):.3f}"

        if not config.use_benchmark_filter:
            return None

        if pd.isna(row.benchmark_regime):
            return "missing_benchmark_context"

        if config.benchmark_vwap_gap_min_atr is not None and pd.notna(row.benchmark_vwap_gap_atr_abs):
            if float(row.benchmark_vwap_gap_atr_abs) < float(config.benchmark_vwap_gap_min_atr):
                return f"benchmark_vwap_gap_below_min_atr:{float(config.benchmark_vwap_gap_min_atr):.3f}"

        if config.benchmark_vwap_gap_max_atr is not None and pd.notna(row.benchmark_vwap_gap_atr_abs):
            if float(row.benchmark_vwap_gap_atr_abs) > float(config.benchmark_vwap_gap_max_atr):
                return f"benchmark_vwap_gap_above_max_atr:{float(config.benchmark_vwap_gap_max_atr):.3f}"

        if config.benchmark_trend_strength_min_atr is not None and pd.notna(row.benchmark_trend_gap_atr_abs):
            if float(row.benchmark_trend_gap_atr_abs) < float(config.benchmark_trend_strength_min_atr):
                return (
                    f"benchmark_trend_strength_below_min_atr:"
                    f"{float(config.benchmark_trend_strength_min_atr):.3f}"
                )

        if config.benchmark_trend_strength_max_atr is not None and pd.notna(row.benchmark_trend_gap_atr_abs):
            if float(row.benchmark_trend_gap_atr_abs) > float(config.benchmark_trend_strength_max_atr):
                return (
                    f"benchmark_trend_strength_above_max_atr:"
                    f"{float(config.benchmark_trend_strength_max_atr):.3f}"
                )

        if row.benchmark_alignment_bucket == "flat_benchmark" and not config.allow_flat_benchmark:
            return "flat_benchmark_blocked"

        if row.benchmark_alignment_bucket == "against_benchmark" and config.require_benchmark_alignment:
            override_score = config.allow_against_benchmark_above_score
            if override_score is not None and float(row.signal_score) >= float(override_score):
                return None
            if override_score is not None:
                return f"against_benchmark_below_override_score:{float(override_score):.3f}"
            return "against_benchmark_blocked"

        return None


def build_stop_loss_audit(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "entry_time",
                "exit_time",
                "bars_until_stop",
                "entry_distance_from_ema_atr",
                "entry_distance_from_vwap_atr",
                "pullback_depth_atr",
                "stop_distance_atr",
                "reward_to_risk",
                "benchmark_alignment_bucket",
                "benchmark_vwap_gap_bucket",
                "benchmark_trend_strength_bucket",
                "signal_score",
                "entry_hour",
            ]
        )

    stopped = trades.loc[trades["exit_reason"] == "stop_loss"].copy()
    if stopped.empty:
        return stopped

    stopped["bars_until_stop"] = np.where(
        stopped["bar_minutes"].fillna(0) > 0,
        (stopped["holding_minutes"] / stopped["bar_minutes"]).round(0),
        np.nan,
    )
    return stopped
