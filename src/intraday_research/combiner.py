from __future__ import annotations

import pandas as pd

from .strategy import SIGNAL_COLUMNS
from .types import CombinedSignal, SignalSide


class SignalCombiner:
    """Resolve multiple strategy opinions into one symbol-level action."""

    def combine(self, signals: pd.DataFrame) -> list[CombinedSignal]:
        if signals.empty:
            return []

        combined: list[CombinedSignal] = []
        grouped = signals[SIGNAL_COLUMNS].groupby(["timestamp", "symbol"], sort=True, dropna=False)
        for (timestamp, symbol), bucket in grouped:
            signed_strength = 0.0
            explanations: list[str] = []
            stop_prices: list[float] = []
            target_prices: list[float] = []
            strategy_names: list[str] = []
            price = None
            long_count = 0
            short_count = 0
            for signal in bucket.itertuples():
                direction = 1.0 if signal.direction == SignalSide.LONG.value else -1.0
                signed_strength += direction * abs(float(signal.strength))
                strategy_names.append(str(signal.strategy_name))
                if direction > 0:
                    long_count += 1
                else:
                    short_count += 1
                explanations.append(
                    f"{signal.strategy_name} {signal.direction} score={float(signal.strength):.3f} reason={signal.reason}"
                )
                if pd.notna(signal.stop_loss):
                    stop_prices.append(float(signal.stop_loss))
                if pd.notna(signal.target):
                    target_prices.append(float(signal.target))

            if signed_strength > 0:
                side = SignalSide.LONG
                stop_price = min(stop_prices) if stop_prices else None
                target_price = max(target_prices) if target_prices else None
            elif signed_strength < 0:
                side = SignalSide.SHORT
                stop_price = max(stop_prices) if stop_prices else None
                target_price = min(target_prices) if target_prices else None
            else:
                side = SignalSide.FLAT
                stop_price = None
                target_price = None

            explanation = (
                f"netted {len(bucket)} signals "
                f"(long={long_count}, short={short_count}) "
                f"to {side.value} with final_score={signed_strength:.3f}; "
                + " | ".join(explanations)
            )
            combined.append(
                CombinedSignal(
                    timestamp=timestamp,
                    symbol=symbol,
                    final_decision=side,
                    final_score=signed_strength,
                    price=float(price if price is not None else 0.0),
                    stop_price=stop_price,
                    target_price=target_price,
                    explanation=explanation,
                    strategy_name="+".join(sorted(set(strategy_names))),
                )
            )
        return combined
