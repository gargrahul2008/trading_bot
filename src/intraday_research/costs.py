from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TransactionCostModel:
    """Configurable placeholder cost model for Indian market charges."""

    brokerage_rate: float = 0.0003
    brokerage_cap_per_order: float = 20.0
    stt_rate: float = 0.00025
    exchange_charge_rate: float = 0.0000345
    gst_rate: float = 0.18
    sebi_charge_rate: float = 0.000001
    stamp_duty_rate: float = 0.00003
    slippage_bps_per_side: float = 1.0

    def estimate_round_trip_costs(
        self,
        entry_price: float,
        exit_price: float,
        quantity: int,
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
        gst = (brokerage + exchange_charges) * self.gst_rate
        entry_slippage = entry_turnover * (self.slippage_bps_per_side / 10_000.0)
        exit_slippage = exit_turnover * (self.slippage_bps_per_side / 10_000.0)
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
