from __future__ import annotations

from itertools import groupby as _groupby

import pandas as pd

from .strategy import SIGNAL_COLUMNS
from .types import CombinedSignal, SignalSide


class SignalCombiner:
    """Resolve multiple strategy opinions into one symbol-level action."""

    def combine(self, signals: pd.DataFrame) -> list[CombinedSignal]:
        if signals.empty:
            return []

        # Convert to plain dicts once (vectorized) instead of calling itertuples() per group
        # bucket.itertuples() creates a new namedtuple class per call — very slow at scale.
        records: list[dict] = signals[SIGNAL_COLUMNS].to_dict("records")
        records.sort(key=lambda r: (r["timestamp"], r["symbol"]))

        combined: list[CombinedSignal] = []
        long_val = SignalSide.LONG.value
        for (timestamp, symbol), bucket_iter in _groupby(
            records, key=lambda r: (r["timestamp"], r["symbol"])
        ):
            signed_strength = 0.0
            explanations: list[str] = []
            stop_prices: list[float] = []
            target_prices: list[float] = []
            strategy_names: list[str] = []
            long_count = 0
            short_count = 0
            for sig in bucket_iter:
                direction = 1.0 if sig["direction"] == long_val else -1.0
                signed_strength += direction * abs(float(sig["strength"]))
                strategy_names.append(str(sig["strategy_name"]))
                if direction > 0:
                    long_count += 1
                else:
                    short_count += 1
                explanations.append(
                    f"{sig['strategy_name']} {sig['direction']} "
                    f"score={float(sig['strength']):.3f} reason={sig['reason']}"
                )
                sl = sig["stop_loss"]
                if sl is not None and sl == sl:  # notna without pandas overhead
                    stop_prices.append(float(sl))
                tgt = sig["target"]
                if tgt is not None and tgt == tgt:
                    target_prices.append(float(tgt))

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
                f"netted {len(explanations)} signals "
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
                    price=0.0,
                    stop_price=stop_price,
                    target_price=target_price,
                    explanation=explanation,
                    strategy_name="+".join(sorted(set(strategy_names))),
                )
            )
        return combined
