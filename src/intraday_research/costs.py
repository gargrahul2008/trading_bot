from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TransactionCostModel:
    """
    Indian equity intraday (MIS) transaction cost model.

    Rates reference: NSE equity intraday as of 2025 (Zerodha schedule).
      STT:              0.025% on sell-side turnover only
      Exchange charges: 0.00297% on total turnover (NSE slab, Oct-2024 revision)
      SEBI charges:     ₹10 / crore = 0.000001 on total turnover
      GST:              18% on (brokerage + exchange charges + SEBI charges)
      Stamp duty:       0.003% on buy-side turnover only
      Brokerage:        0.03% per side, capped at ₹20/order (discount broker)

    Slippage notes:
      - limit_618 entry is a limit order → entry slippage = 0
      - target exit is a limit order     → exit slippage = 0
      - stop_loss / time_exit are market orders → slippage_bps_per_side applies
    """

    brokerage_rate: float = 0.0003
    brokerage_cap_per_order: float = 20.0
    stt_rate: float = 0.00025           # 0.025% on sell side only
    exchange_charge_rate: float = 0.0000297  # 0.00297% NSE (post Oct-2024)
    gst_rate: float = 0.18
    sebi_charge_rate: float = 0.000001  # ₹10/crore
    stamp_duty_rate: float = 0.00003    # 0.003% on buy side only
    slippage_bps_per_side: float = 1.0  # applied only on market-order exits

    def estimate_round_trip_costs(
        self,
        entry_price: float,
        exit_price: float,
        quantity: int,
        exit_reason: str = "",
    ) -> dict[str, float]:
        entry_turnover = abs(entry_price) * quantity
        exit_turnover = abs(exit_price) * quantity
        total_turnover = entry_turnover + exit_turnover

        entry_brokerage = min(entry_turnover * self.brokerage_rate, self.brokerage_cap_per_order)
        exit_brokerage = min(exit_turnover * self.brokerage_rate, self.brokerage_cap_per_order)
        brokerage = entry_brokerage + exit_brokerage
        stt = exit_turnover * self.stt_rate
        exchange_charges = total_turnover * self.exchange_charge_rate
        sebi_charges = total_turnover * self.sebi_charge_rate
        stamp_duty = entry_turnover * self.stamp_duty_rate
        gst = (brokerage + exchange_charges + sebi_charges) * self.gst_rate

        # Entry is always a limit order (limit_618 / fib zone) → no slippage.
        # Exit slippage only on market-order exits (stop_loss, time_exit).
        # Target exits are limit orders and fill at the exact target price.
        entry_slippage = 0.0
        is_market_exit = exit_reason in ("stop_loss", "time_exit", "trail_stop") or exit_reason == ""
        exit_slippage = exit_turnover * (self.slippage_bps_per_side / 10_000.0) if is_market_exit else 0.0
        slippage = entry_slippage + exit_slippage

        return {
            "entry_brokerage": entry_brokerage,
            "exit_brokerage": exit_brokerage,
            "brokerage": brokerage,
            "stt": stt,
            "exchange_charges": exchange_charges,
            "gst": gst,
            "sebi_charges": sebi_charges,
            "stamp_duty": stamp_duty,
            "entry_slippage": entry_slippage,
            "exit_slippage": exit_slippage,
            "slippage": slippage,
            "total_cost": brokerage + stt + exchange_charges + gst + sebi_charges + stamp_duty + slippage,
        }


# Pre-built cost model instances for common exchanges / asset types.

# NSE equity intraday (Zerodha-style flat brokerage)
EQUITY_COSTS = TransactionCostModel(
    brokerage_rate=0.0003,
    brokerage_cap_per_order=20.0,
    stt_rate=0.00025,
    exchange_charge_rate=0.0000297,
    gst_rate=0.18,
    sebi_charge_rate=0.000001,
    stamp_duty_rate=0.00003,
    slippage_bps_per_side=1.0,
)

# Binance spot — 0.1% maker/taker, no Indian charges
BINANCE_COSTS = TransactionCostModel(
    brokerage_rate=0.001,
    brokerage_cap_per_order=1e9,
    stt_rate=0.0,
    exchange_charge_rate=0.0,
    gst_rate=0.0,
    sebi_charge_rate=0.0,
    stamp_duty_rate=0.0,
    slippage_bps_per_side=2.0,
)

# MEXC spot — 0% maker fee via marketable limit orders; all orders placed as
# limit orders so effective cost = 0.  Use this when backtesting strategies
# that never use plain market orders (entry at limit_618, exits as limit too).
MEXC_ZERO_COSTS = TransactionCostModel(
    brokerage_rate=0.0,
    brokerage_cap_per_order=1e9,
    stt_rate=0.0,
    exchange_charge_rate=0.0,
    gst_rate=0.0,
    sebi_charge_rate=0.0,
    stamp_duty_rate=0.0,
    slippage_bps_per_side=0.0,
)
