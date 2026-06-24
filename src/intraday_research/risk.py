from __future__ import annotations

from dataclasses import dataclass

from .types import CombinedSignal, Position, SignalSide


@dataclass
class RiskDecision:
    approved: bool
    quantity: int = 0
    reason: str = ""


@dataclass
class RiskManager:
    max_positions: int = 1
    max_daily_loss: float = 5_000.0
    quantity_per_trade: int = 1

    def evaluate_entry(
        self,
        signal: CombinedSignal,
        open_positions: dict[tuple[str, str], Position],
        realized_daily_pnl: float,
    ) -> RiskDecision:
        if signal.side is SignalSide.FLAT:
            return RiskDecision(approved=False, reason="flat_signal")
        position_key = (signal.symbol, signal.strategy_name)
        if position_key in open_positions:
            return RiskDecision(approved=False, reason="position_already_open")
        if len(open_positions) >= self.max_positions:
            return RiskDecision(approved=False, reason="max_positions_reached")
        if realized_daily_pnl <= -abs(self.max_daily_loss):
            return RiskDecision(approved=False, reason="daily_loss_limit_hit")
        return RiskDecision(approved=True, quantity=self.quantity_per_trade, reason="approved")
