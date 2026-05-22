import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class Lot:
    qty: int
    price: float
    tag: str = "grid"


@dataclass
class BacktestConfig:
    symbol: str = "RELIANCE"
    chunk_qty: int = 70
    initial_qty: int = 420
    grid_pct: float = 0.005          # 0.5%
    min_profit_pct: float = 0.006    # target after cost/slippage buffer
    fee_per_share: float = 0.0       # set this if you want approximate charges
    mtf_interest_annual: float = 0.0 # example: 0.12 for 12% annual
    normal_max_qty: int = 560
    caution_max_qty: int = 840
    hard_max_qty: int = 1050
    recovery_extra_sell_qty: int = 70
    allow_repair: bool = False
    repair_profit_fraction: float = 0.50
    use_intrabar: bool = True        # use high/low triggers if OHLC exists


class GridStrategyBacktester:
    def __init__(self, config: BacktestConfig, improved: bool = True, starting_cash: Optional[float] = None):
        self.cfg = config
        self.improved = improved

        self.lots: List[Lot] = []
        # Cash is the runway. If starting_cash is provided, buys are blocked when cash is insufficient.
        # If starting_cash is None, the backtest behaves like unlimited cash except for inventory caps.
        self.cash = starting_cash if starting_cash is not None else 0.0
        self.use_cash_runway = starting_cash is not None
        self.realized_grid_pnl = 0.0
        self.total_fees = 0.0
        self.last_buy_price: Optional[float] = None

        self.trades: List[Dict[str, Any]] = []
        self.equity: List[Dict[str, Any]] = []

    @property
    def open_qty(self) -> int:
        return sum(lot.qty for lot in self.lots)

    def mode(self) -> str:
        if not self.improved:
            return "BASELINE"

        if self.open_qty >= self.cfg.hard_max_qty:
            return "RECOVERY"
        if self.open_qty >= self.cfg.caution_max_qty:
            return "CAUTION"
        return "NORMAL"

    def avg_cost(self) -> float:
        if self.open_qty == 0:
            return 0.0
        return sum(lot.qty * lot.price for lot in self.lots) / self.open_qty

    def open_cost(self) -> float:
        return sum(lot.qty * lot.price for lot in self.lots)

    def unrealized_pnl(self, price: float) -> float:
        return sum((price - lot.price) * lot.qty for lot in self.lots)

    def total_pnl(self, price: float) -> float:
        return self.realized_grid_pnl + self.unrealized_pnl(price) - self.total_fees

    def breakeven_price(self) -> float:
        if self.open_qty == 0:
            return 0.0
        return (self.open_cost() - self.realized_grid_pnl + self.total_fees) / self.open_qty

    def fee(self, qty: int) -> float:
        return qty * self.cfg.fee_per_share

    def buy(self, dt, qty: int, price: float, reason: str):
        if not self.can_buy(qty, price):
            self.trades.append({
                "datetime": dt,
                "side": "SKIP_BUY",
                "qty": qty,
                "price": price,
                "reason": "insufficient cash runway or inventory cap",
                "mode": self.mode(),
                "open_qty_after": self.open_qty,
                "realized_grid_pnl_after": self.realized_grid_pnl,
            })
            return

        cost = qty * price
        fee = self.fee(qty)
        self.cash -= cost + fee
        self.total_fees += fee
        self.lots.append(Lot(qty=qty, price=price))
        self.last_buy_price = price

        self.trades.append({
            "datetime": dt,
            "side": "BUY",
            "qty": qty,
            "price": price,
            "reason": reason,
            "mode": self.mode(),
            "open_qty_after": self.open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })

    def sell_lifo(self, dt, qty: int, price: float, reason: str):
        qty = min(qty, self.open_qty)
        if qty <= 0:
            return

        fee = self.fee(qty)
        self.cash += qty * price - fee
        self.total_fees += fee

        remaining = qty
        realized_this_sell = 0.0

        while remaining > 0 and self.lots:
            lot = self.lots[-1]
            matched = min(remaining, lot.qty)
            pnl = (price - lot.price) * matched
            self.realized_grid_pnl += pnl
            realized_this_sell += pnl

            lot.qty -= matched
            remaining -= matched

            if lot.qty == 0:
                self.lots.pop()

        self.trades.append({
            "datetime": dt,
            "side": "SELL",
            "qty": qty,
            "price": price,
            "reason": reason,
            "mode": self.mode(),
            "realized_this_sell": realized_this_sell,
            "open_qty_after": self.open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })

    def next_buy_gap(self) -> float:
        mode = self.mode()
        if mode == "CAUTION":
            return self.cfg.grid_pct * 2
        return self.cfg.grid_pct

    def can_buy(self, qty: Optional[int] = None, price: Optional[float] = None) -> bool:
        if qty is not None and price is not None and self.use_cash_runway:
            required_cash = qty * price + self.fee(qty)
            if self.cash < required_cash:
                return False

        if not self.improved:
            return True
        return self.open_qty < self.cfg.hard_max_qty

    def cash_usage_ratio(self, current_price: float) -> float:
        total_equity = self.cash + self.open_qty * current_price
        if total_equity <= 0:
            return 1.0
        invested = self.open_qty * current_price
        return invested / total_equity

    def sell_target_for_latest_lot(self) -> Optional[float]:
        if not self.lots:
            return None
        return self.lots[-1].price * (1 + self.cfg.min_profit_pct)

    def maybe_sell(self, dt, high_price: float, close_price: float):
        """
        Sell when the latest LIFO lot has reached target.
        If use_intrabar=True, high_price can trigger the sell, and fill is assumed at target.
        If use_intrabar=False, close_price must be above target, and fill is at close.
        """
        while self.lots:
            target = self.sell_target_for_latest_lot()
            if target is None:
                return

            if self.cfg.use_intrabar:
                trigger = high_price >= target
                fill_price = target
            else:
                trigger = close_price >= target
                fill_price = close_price

            if not trigger:
                return

            mode = self.mode()
            sell_qty = self.cfg.chunk_qty
            reason = "grid target hit"

            if self.improved and mode == "RECOVERY":
                sell_qty += self.cfg.recovery_extra_sell_qty
                reason = "recovery: grid sell + inventory reduction"

            self.sell_lifo(dt, sell_qty, fill_price, reason)

            # Avoid too many repeated fills on one candle if only close-based data is used.
            if not self.cfg.use_intrabar:
                return

    def maybe_buy(self, dt, low_price: float, close_price: float):
        if not self.can_buy():
            return

        if self.last_buy_price is None:
            return

        gap = self.next_buy_gap()
        next_buy_price = self.last_buy_price * (1 - gap)

        if self.cfg.use_intrabar:
            trigger = low_price <= next_buy_price
            fill_price = next_buy_price
        else:
            trigger = close_price <= next_buy_price
            fill_price = close_price

        if trigger:
            mode = self.mode()
            reason = f"{mode.lower()} grid buy"
            self.buy(dt, self.cfg.chunk_qty, fill_price, reason)

    def maybe_repair(self, dt, close_price: float):
        """
        Optional: sell highest-cost lot if booked profit can absorb the loss.
        This is NOT always recommended. It should be tested.
        """
        if not self.improved or not self.cfg.allow_repair or not self.lots:
            return
        if self.mode() != "RECOVERY":
            return
        if self.realized_grid_pnl <= 0:
            return

        highest = max(self.lots, key=lambda x: x.price)
        if highest.price <= close_price:
            return

        loss_per_share = highest.price - close_price
        profit_budget = self.realized_grid_pnl * self.cfg.repair_profit_fraction
        affordable_qty = int(profit_budget // loss_per_share)
        repair_qty = min(self.cfg.chunk_qty, highest.qty, affordable_qty)

        if repair_qty >= self.cfg.chunk_qty:
            # This still uses LIFO accounting for PnL, but the decision is based on cutting expensive inventory.
            self.sell_lifo(dt, repair_qty, close_price, "repair: use booked profit to reduce inventory")

    def mark_equity(self, dt, price: float):
        self.equity.append({
            "datetime": dt,
            "close": price,
            "mode": self.mode(),
            "open_qty": self.open_qty,
            "avg_cost": self.avg_cost(),
            "realized_grid_pnl": self.realized_grid_pnl,
            "unrealized_pnl": self.unrealized_pnl(price),
            "total_fees": self.total_fees,
            "total_pnl": self.total_pnl(price),
            "breakeven_price": self.breakeven_price(),
            "cash": self.cash,
            "cash_usage_ratio": self.cash_usage_ratio(price),
        })

    def run(self, price_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        price_df must contain:
        - datetime column OR DatetimeIndex
        - close column
        Optional if use_intrabar=True:
        - high column
        - low column
        """
        df = price_df.copy()

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime")
        else:
            df = df.reset_index().rename(columns={df.index.name or "index": "datetime"})
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime")

        if "close" not in df.columns:
            raise ValueError("price_df must contain a 'close' column")

        if "high" not in df.columns:
            df["high"] = df["close"]
        if "low" not in df.columns:
            df["low"] = df["close"]

        first = df.iloc[0]
        self.buy(first["datetime"], self.cfg.initial_qty, float(first["close"]), "initial lump-sum/base buy")
        self.mark_equity(first["datetime"], float(first["close"]))

        for _, row in df.iloc[1:].iterrows():
            dt = row["datetime"]
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])

            # In each candle, sell first if rebound hit target, then buy if fall hit next grid.
            # For more precision, use tick data because OHLC does not reveal exact sequence inside candle.
            self.maybe_sell(dt, high, close)
            self.maybe_repair(dt, close)
            self.maybe_buy(dt, low, close)
            self.mark_equity(dt, close)

        return {
            "equity": pd.DataFrame(self.equity),
            "trades": pd.DataFrame(self.trades),
        }


def max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    drawdown = equity - running_max
    return float(drawdown.min())


def summarize(equity: pd.DataFrame, trades: pd.DataFrame) -> Dict[str, Any]:
    sells = trades[trades["side"] == "SELL"].copy()
    return {
        "final_total_pnl": float(equity["total_pnl"].iloc[-1]),
        "max_drawdown": max_drawdown(equity["total_pnl"]),
        "max_open_qty": int(equity["open_qty"].max()),
        "final_open_qty": int(equity["open_qty"].iloc[-1]),
        "final_breakeven_price": float(equity["breakeven_price"].iloc[-1]),
        "realized_grid_pnl": float(equity["realized_grid_pnl"].iloc[-1]),
        "number_of_trades": int(len(trades)),
        "number_of_sells": int(len(sells)),
        "sell_win_rate": float((sells.get("realized_this_sell", pd.Series(dtype=float)) > 0).mean()) if len(sells) else np.nan,
    }


def compare_baseline_vs_improved(price_csv_path: str, output_prefix: str = "grid_backtest"):
    prices = pd.read_csv(price_csv_path)
    prices.columns = [c.strip().lower() for c in prices.columns]

    cfg = BacktestConfig()

    baseline = GridStrategyBacktester(cfg, improved=False)
    baseline_result = baseline.run(prices)

    improved = GridStrategyBacktester(cfg, improved=True)
    improved_result = improved.run(prices)

    baseline_equity = baseline_result["equity"]
    improved_equity = improved_result["equity"]
    baseline_trades = baseline_result["trades"]
    improved_trades = improved_result["trades"]

    baseline_equity.to_csv(f"{output_prefix}_baseline_equity.csv", index=False)
    improved_equity.to_csv(f"{output_prefix}_improved_equity.csv", index=False)
    baseline_trades.to_csv(f"{output_prefix}_baseline_trades.csv", index=False)
    improved_trades.to_csv(f"{output_prefix}_improved_trades.csv", index=False)

    summary = pd.DataFrame([
        {"strategy": "baseline", **summarize(baseline_equity, baseline_trades)},
        {"strategy": "improved", **summarize(improved_equity, improved_trades)},
    ])
    summary.to_csv(f"{output_prefix}_summary.csv", index=False)
    return summary


def fetch_fyers_history(symbol: str, start: str, end: str, resolution: str = "1") -> pd.DataFrame:
    """
    Fetch OHLCV history from FYERS.

    Required environment variables:
    - FYERS_CLIENT_ID
    - FYERS_ACCESS_TOKEN

    Symbol examples:
    - NSE:RELIANCE-EQ
    - NSE:SBIN-EQ

    Resolution examples:
    - "1" for 1-minute
    - "5" for 5-minute
    - "15" for 15-minute
    - "D" for daily
    """
    import os
    from fyers_apiv3 import fyersModel

    client_id = os.getenv("FYERS_CLIENT_ID")
    access_token = os.getenv("FYERS_ACCESS_TOKEN")

    if not client_id or not access_token:
        raise RuntimeError(
            "Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN environment variables. "
            "Do not hardcode credentials in the script."
        )

    fyers = fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )

    data = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1",
    }

    response = fyers.history(data=data)

    if not isinstance(response, dict) or response.get("s") != "ok":
        raise RuntimeError(f"FYERS history API error: {response}")

    candles = response.get("candles", [])
    if not candles:
        raise RuntimeError("FYERS returned no candles for the requested symbol/date range.")

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df = df[["datetime", "open", "high", "low", "close", "volume"]]
    return df.sort_values("datetime").reset_index(drop=True)


def fetch_yfinance_history(symbol: str, start: str, end: str, interval: str = "1m") -> pd.DataFrame:
    """
    Fetch OHLCV from Yahoo Finance through yfinance.

    NSE examples:
    - RELIANCE.NS
    - SBIN.NS

    Note: Yahoo intraday history is limited, so FYERS is preferred for serious backtesting.
    """
    import yfinance as yf

    data = yf.download(
        symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
    )

    if data.empty:
        raise RuntimeError("Yahoo Finance returned no data.")

    data = data.reset_index()
    data.columns = [str(c).lower().replace(" ", "_") for c in data.columns]

    datetime_col = "datetime" if "datetime" in data.columns else "date"
    data = data.rename(columns={
        datetime_col: "datetime",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    })

    return data[["datetime", "open", "high", "low", "close", "volume"]].sort_values("datetime").reset_index(drop=True)


def fetch_price_data(
    symbol: str,
    start: str,
    end: str,
    provider: str = "fyers",
    resolution: str = "1",
) -> pd.DataFrame:
    provider = provider.lower().strip()

    if provider == "fyers":
        return fetch_fyers_history(symbol=symbol, start=start, end=end, resolution=resolution)

    if provider in {"yfinance", "yf", "yahoo"}:
        interval = "1m" if resolution == "1" else f"{resolution}m" if resolution.isdigit() else "1d"
        return fetch_yfinance_history(symbol=symbol, start=start, end=end, interval=interval)

    raise ValueError("provider must be 'fyers' or 'yfinance'")


def compare_baseline_vs_improved_dynamic(
    symbol: str,
    start: str,
    end: str,
    provider: str = "fyers",
    resolution: str = "1",
    starting_cash: Optional[float] = None,
    output_prefix: Optional[str] = None,
):
    prices = fetch_price_data(
        symbol=symbol,
        start=start,
        end=end,
        provider=provider,
        resolution=resolution,
    )

    cfg = BacktestConfig(symbol=symbol)

    baseline = GridStrategyBacktester(cfg, improved=False, starting_cash=starting_cash)
    baseline_result = baseline.run(prices)

    improved = GridStrategyBacktester(cfg, improved=True, starting_cash=starting_cash)
    improved_result = improved.run(prices)

    baseline_equity = baseline_result["equity"]
    improved_equity = improved_result["equity"]
    baseline_trades = baseline_result["trades"]
    improved_trades = improved_result["trades"]

    summary = pd.DataFrame([
        {"strategy": "baseline", **summarize(baseline_equity, baseline_trades)},
        {"strategy": "improved", **summarize(improved_equity, improved_trades)},
    ])

    if output_prefix is None:
        safe_symbol = symbol.replace(":", "_").replace("-", "_").replace(".", "_")
        output_prefix = f"grid_backtest_{safe_symbol}_{start}_{end}_{provider}"

    prices.to_csv(f"{output_prefix}_prices.csv", index=False)
    baseline_equity.to_csv(f"{output_prefix}_baseline_equity.csv", index=False)
    improved_equity.to_csv(f"{output_prefix}_improved_equity.csv", index=False)
    baseline_trades.to_csv(f"{output_prefix}_baseline_trades.csv", index=False)
    improved_trades.to_csv(f"{output_prefix}_improved_trades.csv", index=False)
    summary.to_csv(f"{output_prefix}_summary.csv", index=False)

    return {
        "summary": summary,
        "prices": prices,
        "baseline_equity": baseline_equity,
        "improved_equity": improved_equity,
        "baseline_trades": baseline_trades,
        "improved_trades": improved_trades,
        "output_prefix": output_prefix,
    }


def plot_backtest_result(result: Dict[str, pd.DataFrame], output_path: Optional[str] = None):
    import matplotlib.pyplot as plt

    baseline_equity = result["baseline_equity"]
    improved_equity = result["improved_equity"]

    plt.figure(figsize=(12, 6))
    plt.plot(pd.to_datetime(baseline_equity["datetime"]), baseline_equity["total_pnl"], label="Baseline")
    plt.plot(pd.to_datetime(improved_equity["datetime"]), improved_equity["total_pnl"], label="Improved")
    plt.axhline(0, linewidth=1)
    plt.title("Grid Strategy Backtest: Baseline vs Improved")
    plt.xlabel("Date & Time")
    plt.ylabel("Total PnL")
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()

    if output_path is None:
        output_path = f"{result['output_prefix']}_equity_curve.png"

    plt.savefig(output_path, dpi=150)
    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dynamic grid strategy backtester")
    parser.add_argument("--symbol", required=True, help="Example: NSE:RELIANCE-EQ for FYERS, RELIANCE.NS for yfinance")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--provider", default="fyers", choices=["fyers", "yfinance", "yf", "yahoo"])
    parser.add_argument("--resolution", default="1", help="FYERS: 1,5,15,D etc. yfinance: 1 means 1m")
    parser.add_argument("--starting-cash", type=float, default=None, help="Optional runway cash. Example: 1000000")
    parser.add_argument("--output-prefix", default=None)
    args = parser.parse_args()

    result = compare_baseline_vs_improved_dynamic(
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        provider=args.provider,
        resolution=args.resolution,
        starting_cash=args.starting_cash,
        output_prefix=args.output_prefix,
    )
    chart_path = plot_backtest_result(result)

    print(result["summary"].to_string(index=False))
    print(f"Saved outputs with prefix: {result['output_prefix']}")
    print(f"Saved chart: {chart_path}")

