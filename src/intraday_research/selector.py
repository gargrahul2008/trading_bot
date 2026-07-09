from __future__ import annotations

from dataclasses import dataclass

from .types import CombinedSignal


@dataclass
class SelectionDecision:
    approved: bool
    reason: str
    reward_to_risk: float | None = None


class SignalSelector:
    """Filter signals using quality thresholds rather than quotas."""

    def select_signal(
        self,
        signal: CombinedSignal,
        *,
        minimum_signal_score: float,
        minimum_reward_to_risk: float,
        minimum_signal_score_by_strategy: dict[str, float] | None = None,
        minimum_reward_to_risk_by_strategy: dict[str, float] | None = None,
    ) -> SelectionDecision:
        signal_threshold = float(
            (minimum_signal_score_by_strategy or {}).get(signal.strategy_name, minimum_signal_score)
        )
        reward_threshold = float(
            (minimum_reward_to_risk_by_strategy or {}).get(signal.strategy_name, minimum_reward_to_risk)
        )
        reward_to_risk = self._reward_to_risk(signal)

        if abs(float(signal.final_score)) < signal_threshold:
            return SelectionDecision(
                approved=False,
                reason=f"below_minimum_signal_score:{signal_threshold:.3f}",
                reward_to_risk=reward_to_risk,
            )
        if reward_threshold > 0:
            if reward_to_risk is None:
                return SelectionDecision(
                    approved=False,
                    reason="missing_reward_to_risk",
                    reward_to_risk=reward_to_risk,
                )
            if reward_to_risk < reward_threshold:
                return SelectionDecision(
                    approved=False,
                    reason=f"below_minimum_reward_to_risk:{reward_threshold:.3f}",
                    reward_to_risk=reward_to_risk,
                )
        return SelectionDecision(approved=True, reason="approved", reward_to_risk=reward_to_risk)

    @staticmethod
    def _reward_to_risk(signal: CombinedSignal) -> float | None:
        if signal.stop_price is None or signal.target_price is None:
            return None
        risk = abs(float(signal.price) - float(signal.stop_price))
        if risk <= 0:
            return None
        reward = abs(float(signal.target_price) - float(signal.price))
        return reward / risk
