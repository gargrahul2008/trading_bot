import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common.broker.auth_json import get_fyers_creds_from_json, list_fyers_users


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
    grid_pct: float = 0.005
    min_profit_pct: float = 0.006
    fee_per_share: float = 0.0
    mtf_interest_annual: float = 0.0
    mtf_leverage: float = 1.0
    normal_max_qty: int = 560
    caution_max_qty: int = 840
    hard_max_qty: int = 1050
    recovery_extra_sell_qty: int = 70
    allow_repair: bool = False
    repair_profit_fraction: float = 0.50
    use_intrabar: bool = True
    intrabar_mode: str = "optimistic"


class GridStrategyBacktester:
    def __init__(self, config: BacktestConfig, improved: bool = True, starting_cash: Optional[float] = None):
        self.cfg = config
        self.improved = improved

        self.lots: List[Lot] = []
        self.cash = starting_cash if starting_cash is not None else 0.0
        self.use_cash_runway = starting_cash is not None
        self.realized_grid_pnl = 0.0
        self.total_fees = 0.0
        self.total_interest = 0.0
        self.last_buy_price: Optional[float] = None

        self.trades: List[Dict[str, Any]] = []
        self.equity: List[Dict[str, Any]] = []

    @property
    def open_qty(self) -> int:
        return sum(lot.qty for lot in self.lots)

    def effective_intrabar_mode(self) -> str:
        if not self.cfg.use_intrabar:
            return "close_only"
        return self.cfg.intrabar_mode

    def mode(self) -> str:
        if not self.improved:
            return "BASELINE"
        if self.open_qty >= self.cfg.caution_max_qty:
            return "RECOVERY"
        if self.open_qty >= self.cfg.normal_max_qty:
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
        return self.realized_grid_pnl + self.unrealized_pnl(price) - self.total_fees - self.total_interest

    def breakeven_price(self) -> float:
        if self.open_qty == 0:
            return 0.0
        return (self.open_cost() - self.realized_grid_pnl + self.total_fees + self.total_interest) / self.open_qty

    def fee(self, qty: int) -> float:
        return qty * self.cfg.fee_per_share

    def mtf_borrowed_fraction(self) -> float:
        if self.cfg.mtf_leverage <= 1.0:
            return 0.0
        return (self.cfg.mtf_leverage - 1.0) / self.cfg.mtf_leverage

    def mtf_borrowed_amount(self) -> float:
        return self.open_cost() * self.mtf_borrowed_fraction()

    def accrue_interest(self, dt, previous_dt) -> float:
        if self.cfg.mtf_interest_annual <= 0 or self.open_qty <= 0:
            return 0.0

        elapsed_days = (pd.Timestamp(dt) - pd.Timestamp(previous_dt)).total_seconds() / 86400.0
        if elapsed_days <= 0:
            return 0.0

        borrowed_amount = self.mtf_borrowed_amount()
        if borrowed_amount <= 0:
            return 0.0

        interest = borrowed_amount * self.cfg.mtf_interest_annual * (elapsed_days / 365.0)
        if interest <= 0:
            return 0.0

        self.cash -= interest
        self.total_interest += interest
        self.trades.append({
            "datetime": dt,
            "side": "INTEREST",
            "qty": 0,
            "price": 0.0,
            "reason": "mtf carry cost accrual",
            "mode": self.mode(),
            "interest_base_amount": borrowed_amount,
            "interest_accrued": interest,
            "open_qty_after": self.open_qty,
            "realized_grid_pnl_after": self.realized_grid_pnl,
        })
        return interest

    def buy(self, dt, qty: int, price: float, reason: str):
        if qty <= 0:
            return

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

        fee = self.fee(qty)
        self.cash -= qty * price + fee
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

    def _sell_from_lot_index(self, dt, lot_index: int, qty: int, price: float, reason: str):
        qty = min(qty, self.open_qty)
        if qty <= 0:
            return

        fee = self.fee(qty)
        self.cash += qty * price - fee
        self.total_fees += fee

        lot = self.lots[lot_index]
        matched = min(qty, lot.qty)
        realized_this_sell = (price - lot.price) * matched
        self.realized_grid_pnl += realized_this_sell
        lot.qty -= matched
        sold_from_lot_price = lot.price

        if lot.qty == 0:
            self.lots.pop(lot_index)

        self.trades.append({
            "datetime": dt,
            "side": "SELL",
            "qty": matched,
            "price": price,
            "reason": reason,
            "mode": self.mode(),
            "realized_this_sell": realized_this_sell,
            "sold_lot_price": sold_from_lot_price,
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
        if self.mode() == "CAUTION":
            return self.cfg.grid_pct * 2
        return self.cfg.grid_pct

    def can_buy(self, qty: Optional[int] = None, price: Optional[float] = None) -> bool:
        buy_qty = qty or 0

        if self.improved and buy_qty > 0 and self.open_qty + buy_qty > self.cfg.hard_max_qty:
            return False

        if qty is not None and price is not None and self.use_cash_runway:
            required_cash = qty * price + self.fee(qty)
            if self.cash < required_cash:
                return False

        return True

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

    def maybe_sell(self, dt, high_price: float, close_price: float, max_sells: Optional[int] = None) -> int:
        sells_done = 0
        intrabar_mode = self.effective_intrabar_mode()

        while self.lots:
            if max_sells is not None and sells_done >= max_sells:
                return sells_done

            target = self.sell_target_for_latest_lot()
            if target is None:
                return sells_done

            if intrabar_mode == "close_only":
                trigger = close_price >= target
                fill_price = close_price
            else:
                trigger = high_price >= target
                fill_price = target

            if not trigger:
                return sells_done

            sell_qty = self.cfg.chunk_qty
            reason = "grid target hit"

            if self.improved and self.mode() == "RECOVERY":
                sell_qty += self.cfg.recovery_extra_sell_qty
                reason = "recovery: grid sell + inventory reduction"

            self.sell_lifo(dt, sell_qty, fill_price, reason)
            sells_done += 1

            if intrabar_mode in {"close_only", "one_order_per_candle", "conservative"}:
                return sells_done

        return sells_done

    def maybe_buy(self, dt, low_price: float, close_price: float) -> bool:
        if self.last_buy_price is None:
            return False

        gap = self.next_buy_gap()
        next_buy_price = self.last_buy_price * (1 - gap)
        intrabar_mode = self.effective_intrabar_mode()

        if intrabar_mode == "close_only":
            trigger = close_price <= next_buy_price
            fill_price = close_price
        else:
            trigger = low_price <= next_buy_price
            fill_price = next_buy_price

        if not trigger:
            return False

        reason = f"{self.mode().lower()} grid buy"
        prior_trade_count = len(self.trades)
        self.buy(dt, self.cfg.chunk_qty, fill_price, reason)
        return len(self.trades) > prior_trade_count and self.trades[-1]["side"] == "BUY"

    def maybe_repair(self, dt, close_price: float) -> bool:
        if not self.improved or not self.cfg.allow_repair or not self.lots:
            return False
        if self.mode() != "RECOVERY":
            return False
        if self.realized_grid_pnl <= 0:
            return False

        highest_index = max(range(len(self.lots)), key=lambda idx: self.lots[idx].price)
        highest = self.lots[highest_index]
        if highest.price <= close_price:
            return False

        loss_per_share = highest.price - close_price
        profit_budget = self.realized_grid_pnl * self.cfg.repair_profit_fraction
        affordable_qty = int(profit_budget // loss_per_share)
        repair_qty = min(self.cfg.chunk_qty, highest.qty, affordable_qty)

        if repair_qty < self.cfg.chunk_qty:
            return False

        self._sell_from_lot_index(
            dt,
            highest_index,
            repair_qty,
            close_price,
            "repair: use booked profit to reduce highest-cost inventory",
        )
        return True

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
            "total_interest": self.total_interest,
            "mtf_borrowed_amount": self.mtf_borrowed_amount(),
            "mtf_borrowed_fraction": self.mtf_borrowed_fraction(),
            "total_pnl": self.total_pnl(price),
            "breakeven_price": self.breakeven_price(),
            "cash": self.cash,
            "cash_usage_ratio": self.cash_usage_ratio(price),
        })

    def validate_config(self):
        if self.cfg.chunk_qty <= 0:
            raise ValueError("chunk_qty must be positive")
        if self.cfg.initial_qty <= 0:
            raise ValueError("initial_qty must be positive")
        if self.cfg.mtf_leverage < 1.0:
            raise ValueError("mtf_leverage must be greater than or equal to 1.0")
        if self.cfg.normal_max_qty < self.cfg.initial_qty:
            raise ValueError("normal_max_qty must be at least initial_qty")
        if self.cfg.caution_max_qty < self.cfg.normal_max_qty:
            raise ValueError("caution_max_qty must be greater than or equal to normal_max_qty")
        if self.cfg.hard_max_qty < self.cfg.caution_max_qty:
            raise ValueError("hard_max_qty must be greater than or equal to caution_max_qty")
        if self.effective_intrabar_mode() not in {"optimistic", "one_order_per_candle", "conservative", "close_only"}:
            raise ValueError("intrabar_mode must be one of: optimistic, one_order_per_candle, conservative, close_only")

    def validate_initial_buy(self, price: float):
        if self.improved and self.cfg.initial_qty > self.cfg.hard_max_qty:
            raise ValueError("initial_qty cannot exceed hard_max_qty")

        if self.use_cash_runway:
            required_cash = self.cfg.initial_qty * price + self.fee(self.cfg.initial_qty)
            if self.cash < required_cash:
                raise ValueError("Starting cash cannot fund initial buy")

    def run(self, price_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df = price_df.copy()
        self.validate_config()

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
        first_close = float(first["close"])
        self.validate_initial_buy(first_close)
        self.buy(first["datetime"], self.cfg.initial_qty, first_close, "initial lump-sum/base buy")
        self.mark_equity(first["datetime"], first_close)
        previous_dt = first["datetime"]

        intrabar_mode = self.effective_intrabar_mode()

        for _, row in df.iloc[1:].iterrows():
            dt = row["datetime"]
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])

            self.accrue_interest(dt, previous_dt)

            if intrabar_mode == "conservative":
                buy_hit = self.last_buy_price is not None and low <= self.last_buy_price * (1 - self.next_buy_gap())
                sell_target = self.sell_target_for_latest_lot()
                sell_hit = sell_target is not None and high >= sell_target

                if buy_hit:
                    self.maybe_buy(dt, low, close)
                elif sell_hit:
                    self.maybe_sell(dt, high, close, max_sells=1)
            else:
                max_sells = 1 if intrabar_mode == "one_order_per_candle" else None
                self.maybe_sell(dt, high, close, max_sells=max_sells)
                self.maybe_repair(dt, close)
                if intrabar_mode != "one_order_per_candle":
                    self.maybe_buy(dt, low, close)

            self.mark_equity(dt, close)
            previous_dt = dt

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
    trade_events = trades[trades["side"].isin(["BUY", "SELL", "SKIP_BUY"])].copy()
    return {
        "final_total_pnl": float(equity["total_pnl"].iloc[-1]),
        "max_drawdown": max_drawdown(equity["total_pnl"]),
        "max_open_qty": int(equity["open_qty"].max()),
        "final_open_qty": int(equity["open_qty"].iloc[-1]),
        "final_breakeven_price": float(equity["breakeven_price"].iloc[-1]),
        "realized_grid_pnl": float(equity["realized_grid_pnl"].iloc[-1]),
        "total_interest": float(equity["total_interest"].iloc[-1]),
        "total_fees": float(equity["total_fees"].iloc[-1]),
        "number_of_trades": int(len(trade_events)),
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
    from fyers_apiv3 import fyersModel

    client_id = os.getenv("FYERS_CLIENT_ID")
    access_token = os.getenv("FYERS_ACCESS_TOKEN")

    if not client_id or not access_token:
        auth_file = os.getenv("FYERS_AUTH_FILE", "fyers_auth.json")
        user_key = os.getenv("FYERS_USER_KEY", "user1")
        auth_candidates = []
        raw_auth_path = os.path.expanduser(auth_file)
        if os.path.isabs(raw_auth_path):
            auth_candidates.append(raw_auth_path)
        else:
            auth_candidates.append(os.path.abspath(raw_auth_path))
            auth_candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), raw_auth_path))

        seen = set()
        auth_candidates = [path for path in auth_candidates if not (path in seen or seen.add(path))]
        last_error = None

        for candidate in auth_candidates:
            if not os.path.exists(candidate):
                last_error = f"auth file not found: {candidate}"
                continue
            try:
                client_id, access_token = get_fyers_creds_from_json(candidate, user_key=user_key)
                break
            except Exception as exc:
                try:
                    available_users = sorted(list_fyers_users(candidate).keys())
                except Exception:
                    available_users = []
                last_error = (
                    f"failed to load FYERS auth from {candidate} for {user_key}: {exc}. "
                    f"Available users: {available_users}"
                )
        else:
            raise RuntimeError(
                "Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN environment variables, "
                f"and FYERS auth fallback failed. Tried {auth_candidates}. Last error: {last_error}"
            )

    if not client_id or not access_token:
        raise RuntimeError(
            "Missing FYERS auth. Set FYERS_CLIENT_ID/FYERS_ACCESS_TOKEN or provide "
            "FYERS_AUTH_FILE and FYERS_USER_KEY for fyers_auth.json fallback."
        )

    fyers = fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )

    response = fyers.history(data={
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": start,
        "range_to": end,
        "cont_flag": "1",
    })

    if not isinstance(response, dict) or response.get("s") != "ok":
        raise RuntimeError(f"FYERS history API error: {response}")

    candles = response.get("candles", [])
    if not candles:
        raise RuntimeError("FYERS returned no candles for the requested symbol/date range.")

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    return df[["datetime", "open", "high", "low", "close", "volume"]].sort_values("datetime").reset_index(drop=True)


def fetch_yfinance_history(symbol: str, start: str, end: str, interval: str = "1m") -> pd.DataFrame:
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
    data = data.rename(columns={datetime_col: "datetime"})
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
    cfg: Optional[BacktestConfig] = None,
):
    prices = fetch_price_data(
        symbol=symbol,
        start=start,
        end=end,
        provider=provider,
        resolution=resolution,
    )

    if cfg is None:
        cfg = BacktestConfig(symbol=symbol)
    else:
        cfg.symbol = symbol

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


def plot_diagnostics(result: Dict[str, pd.DataFrame], output_path: Optional[str] = None):
    prices = result["prices"].copy()
    improved_trades = result["improved_trades"].copy()
    baseline_equity = result["baseline_equity"].copy()
    improved_equity = result["improved_equity"].copy()

    prices["datetime"] = pd.to_datetime(prices["datetime"])
    improved_trades["datetime"] = pd.to_datetime(improved_trades["datetime"])
    baseline_equity["datetime"] = pd.to_datetime(baseline_equity["datetime"])
    improved_equity["datetime"] = pd.to_datetime(improved_equity["datetime"])

    fig, axes = plt.subplots(4, 1, figsize=(14, 14), sharex=False)

    axes[0].plot(prices["datetime"], prices["close"], label="Close")
    buys = improved_trades[improved_trades["side"] == "BUY"]
    sells = improved_trades[improved_trades["side"] == "SELL"]
    if len(buys):
        axes[0].scatter(buys["datetime"], buys["price"], marker="^", label="BUY")
    if len(sells):
        axes[0].scatter(sells["datetime"], sells["price"], marker="v", label="SELL")
    axes[0].set_title("Price with improved-strategy buy/sell markers")
    axes[0].set_ylabel("Price")
    axes[0].legend()

    axes[1].plot(baseline_equity["datetime"], baseline_equity["total_pnl"], label="Baseline total PnL")
    axes[1].plot(improved_equity["datetime"], improved_equity["total_pnl"], label="Improved total PnL")
    axes[1].axhline(0, linewidth=1)
    axes[1].set_title("Total PnL equity curve")
    axes[1].set_ylabel("PnL")
    axes[1].legend()

    axes[2].plot(improved_equity["datetime"], improved_equity["realized_grid_pnl"], label="Realized grid PnL")
    axes[2].plot(improved_equity["datetime"], improved_equity["unrealized_pnl"], label="Unrealized inventory PnL")
    axes[2].plot(improved_equity["datetime"], -improved_equity["total_interest"], label="Accumulated interest cost")
    axes[2].plot(improved_equity["datetime"], -improved_equity["total_fees"], label="Accumulated fee cost")
    axes[2].plot(improved_equity["datetime"], improved_equity["total_pnl"], label="Total PnL")
    axes[2].axhline(0, linewidth=1)
    axes[2].set_title("Improved strategy PnL components")
    axes[2].set_ylabel("PnL")
    axes[2].legend()

    ax4 = axes[3]
    ax4.plot(improved_equity["datetime"], improved_equity["open_qty"], label="Open qty")
    ax4.set_title("Improved strategy inventory and breakeven")
    ax4.set_ylabel("Open qty")
    ax4b = ax4.twinx()
    ax4b.plot(improved_equity["datetime"], improved_equity["breakeven_price"], label="Breakeven price")
    ax4b.set_ylabel("Breakeven price")

    lines1, labels1 = ax4.get_legend_handles_labels()
    lines2, labels2 = ax4b.get_legend_handles_labels()
    ax4.legend(lines1 + lines2, labels1 + labels2)

    for ax in axes:
        ax.tick_params(axis="x", rotation=45)

    fig.tight_layout()

    if output_path is None:
        output_path = f"{result['output_prefix']}_diagnostics.png"

    fig.savefig(output_path, dpi=150)
    return output_path


def config_to_dict(
    cfg: BacktestConfig,
    starting_cash: Optional[float],
    provider: str,
    resolution: str,
    start: str,
    end: str,
) -> Dict[str, Any]:
    return {
        "symbol": cfg.symbol,
        "start": start,
        "end": end,
        "provider": provider,
        "resolution": resolution,
        "starting_cash": starting_cash,
        "chunk_qty": cfg.chunk_qty,
        "initial_qty": cfg.initial_qty,
        "grid_pct": cfg.grid_pct,
        "min_profit_pct": cfg.min_profit_pct,
        "fee_per_share": cfg.fee_per_share,
        "mtf_interest_annual": cfg.mtf_interest_annual,
        "mtf_leverage": cfg.mtf_leverage,
        "normal_max_qty": cfg.normal_max_qty,
        "caution_max_qty": cfg.caution_max_qty,
        "hard_max_qty": cfg.hard_max_qty,
        "recovery_extra_sell_qty": cfg.recovery_extra_sell_qty,
        "allow_repair": cfg.allow_repair,
        "repair_profit_fraction": cfg.repair_profit_fraction,
        "use_intrabar": cfg.use_intrabar,
        "intrabar_mode": cfg.intrabar_mode,
    }


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

    parser.add_argument("--chunk-qty", type=int, default=70)
    parser.add_argument("--initial-qty", type=int, default=420)
    parser.add_argument("--grid-pct", type=float, default=0.005, help="0.005 means 0.5 percent")
    parser.add_argument("--min-profit-pct", type=float, default=0.006, help="0.006 means 0.6 percent target for sell")
    parser.add_argument("--fee-per-share", type=float, default=0.0)
    parser.add_argument("--mtf-interest-annual", type=float, default=0.0, help="Example: 0.12 means 12 percent annual carry")
    parser.add_argument("--mtf-leverage", type=float, default=1.0, help="Example: 3.0 means interest is charged on 2/3 of inventory cost")
    parser.add_argument("--normal-max-qty", type=int, default=560)
    parser.add_argument("--caution-max-qty", type=int, default=840)
    parser.add_argument("--hard-max-qty", type=int, default=1050)
    parser.add_argument("--recovery-extra-sell-qty", type=int, default=70)
    parser.add_argument("--allow-repair", action="store_true")
    parser.add_argument("--repair-profit-fraction", type=float, default=0.50)
    parser.add_argument(
        "--intrabar-mode",
        default="optimistic",
        choices=["optimistic", "one_order_per_candle", "conservative", "close_only"],
        help="Intrabar execution policy for OHLC candles",
    )
    parser.add_argument("--close-only", action="store_true", help="Alias for --intrabar-mode close_only")

    args = parser.parse_args()

    intrabar_mode = "close_only" if args.close_only else args.intrabar_mode

    cfg = BacktestConfig(
        symbol=args.symbol,
        chunk_qty=args.chunk_qty,
        initial_qty=args.initial_qty,
        grid_pct=args.grid_pct,
        min_profit_pct=args.min_profit_pct,
        fee_per_share=args.fee_per_share,
        mtf_interest_annual=args.mtf_interest_annual,
        mtf_leverage=args.mtf_leverage,
        normal_max_qty=args.normal_max_qty,
        caution_max_qty=args.caution_max_qty,
        hard_max_qty=args.hard_max_qty,
        recovery_extra_sell_qty=args.recovery_extra_sell_qty,
        allow_repair=args.allow_repair,
        repair_profit_fraction=args.repair_profit_fraction,
        use_intrabar=intrabar_mode != "close_only",
        intrabar_mode=intrabar_mode,
    )

    result = compare_baseline_vs_improved_dynamic(
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        provider=args.provider,
        resolution=args.resolution,
        starting_cash=args.starting_cash,
        output_prefix=args.output_prefix,
        cfg=cfg,
    )
    chart_path = plot_backtest_result(result)
    diagnostics_path = plot_diagnostics(result)

    config_df = pd.DataFrame([config_to_dict(
        cfg=cfg,
        starting_cash=args.starting_cash,
        provider=args.provider,
        resolution=args.resolution,
        start=args.start,
        end=args.end,
    )])
    config_path = f"{result['output_prefix']}_config.csv"
    config_df.to_csv(config_path, index=False)

    print("\nCONFIG USED")
    print(config_df.T.to_string(header=False))
    print("\nSUMMARY")
    print(result["summary"].to_string(index=False))
    print(f"\nSaved outputs with prefix: {result['output_prefix']}")
    print(f"Saved equity chart: {chart_path}")
    print(f"Saved diagnostics chart: {diagnostics_path}")
    print(f"Saved config: {config_path}")
