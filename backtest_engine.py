from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Callable

import numpy as np
import pandas as pd
import yfinance as yf
from sqlalchemy import text

from db.models import engine


@dataclass
class BacktestResult:
    strategy_name: str
    ticker: str
    start_date: str
    end_date: str
    params: dict[str, object]
    total_return_pct: float
    buy_hold_return_pct: float
    max_drawdown_pct: float
    win_rate_pct: float
    sharpe_ratio: float
    profit_factor: float
    trades: int
    equity_curve: pd.DataFrame
    trade_log: pd.DataFrame


def ensure_backtest_tables() -> None:
    id_col = "INTEGER PRIMARY KEY AUTOINCREMENT" if engine.url.get_backend_name().lower() == "sqlite" else "BIGSERIAL PRIMARY KEY"
    with engine.begin() as con:
        con.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS backtest_results (
                id __ID_COL__,
                strategy_name TEXT NOT NULL,
                ticker TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                params_json TEXT,
                total_return_pct REAL,
                buy_hold_return_pct REAL,
                max_drawdown_pct REAL,
                win_rate_pct REAL,
                sharpe_ratio REAL,
                profit_factor REAL,
                trades INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
                .replace("__ID_COL__", id_col)
            )
        )


def fetch_price_data(ticker: str, start: date, end: date) -> pd.DataFrame:
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
    if df.empty:
        return pd.DataFrame()

    # yfinance may return MultiIndex columns like ('Close', 'AAPL').
    if isinstance(df.columns, pd.MultiIndex):
        try:
            df.columns = [str(c[0]) for c in df.columns]
        except Exception:
            flat_cols = []
            for col in df.columns:
                if isinstance(col, tuple) and len(col) > 0:
                    flat_cols.append(str(col[0]))
                else:
                    flat_cols.append(str(col))
            df.columns = flat_cols

    # Keep first occurrence if duplicated after flattening.
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    df = df.rename_axis("Date").reset_index()
    needed = ["Date", "Open", "High", "Low", "Close", "Volume"]
    for c in needed:
        if c not in df.columns:
            return pd.DataFrame()
    out = df[needed].copy()
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["Open", "High", "Low", "Close"]).reset_index(drop=True)
    return out


def _to_float_scalar(v: object) -> float:
    if isinstance(v, pd.Series):
        if v.empty:
            return float("nan")
        return float(pd.to_numeric(v.iloc[0], errors="coerce"))
    if isinstance(v, (list, tuple)):
        if not v:
            return float("nan")
        return float(pd.to_numeric(v[0], errors="coerce"))
    return float(pd.to_numeric(v, errors="coerce"))


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["SMA5"] = d["Close"].rolling(5).mean()
    d["SMA15"] = d["Close"].rolling(15).mean()
    d["SMA25"] = d["Close"].rolling(25).mean()
    d["SMA60"] = d["Close"].rolling(60).mean()
    d["SMA75"] = d["Close"].rolling(75).mean()
    d["SMA200"] = d["Close"].rolling(200).mean()

    delta = d["Close"].diff()
    up = delta.clip(lower=0).rolling(14).mean()
    down = (-delta.clip(upper=0)).rolling(14).mean()
    rs = up / down.replace(0, np.nan)
    d["RSI14"] = 100 - (100 / (1 + rs))

    ema12 = d["Close"].ewm(span=12, adjust=False).mean()
    ema26 = d["Close"].ewm(span=26, adjust=False).mean()
    d["MACD"] = ema12 - ema26
    d["MACD_SIGNAL"] = d["MACD"].ewm(span=9, adjust=False).mean()
    d["MACD_HIST"] = d["MACD"] - d["MACD_SIGNAL"]

    mid = d["Close"].rolling(20).mean()
    std = d["Close"].rolling(20).std()
    d["BB_UPPER"] = mid + 2 * std
    d["BB_LOWER"] = mid - 2 * std
    return d


def _crossover(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a > b) & (a.shift(1) <= b.shift(1))


def _crossunder(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a < b) & (a.shift(1) >= b.shift(1))


def build_signals(df: pd.DataFrame, strategy: str, params: dict[str, object]) -> tuple[pd.Series, pd.Series]:
    d = df.copy()
    strategy = strategy.lower()

    if strategy == "golden_cross":
        short = int(params.get("short", 15))
        long = int(params.get("long", 60))
        s = d["Close"].rolling(short).mean()
        l = d["Close"].rolling(long).mean()
        buy = _crossover(s, l)
        sell = _crossunder(s, l)
        return buy.fillna(False), sell.fillna(False)

    if strategy == "rsi_reversal":
        low = float(params.get("rsi_low", 30))
        high = float(params.get("rsi_high", 70))
        buy = d["RSI14"] < low
        sell = d["RSI14"] > high
        return buy.fillna(False), sell.fillna(False)

    if strategy == "macd_cross":
        buy = _crossover(d["MACD"], d["MACD_SIGNAL"])
        sell = _crossunder(d["MACD"], d["MACD_SIGNAL"])
        return buy.fillna(False), sell.fillna(False)

    if strategy == "bb_breakout":
        buy = d["Close"] < d["BB_LOWER"]
        sell = d["Close"] > d["BB_UPPER"]
        return buy.fillna(False), sell.fillna(False)

    if strategy == "combo":
        buy = (d["RSI14"] < float(params.get("rsi_low", 35))) & _crossover(d["MACD"], d["MACD_SIGNAL"])
        sell = (d["RSI14"] > float(params.get("rsi_high", 65))) | _crossunder(d["MACD"], d["MACD_SIGNAL"])
        return buy.fillna(False), sell.fillna(False)

    return pd.Series(False, index=d.index), pd.Series(False, index=d.index)


def run_backtest(
    df: pd.DataFrame,
    ticker: str,
    strategy_name: str,
    params: dict[str, object] | None = None,
    initial_cash: float = 1_000_000,
    commission: float = 0.001,
    stop_loss_pct: float | None = None,
    take_profit_pct: float | None = None,
    position_sizing_method: str = "all_in",
    position_size_value: float = 1.0,
) -> BacktestResult:
    params = params or {}
    d = add_indicators(df).reset_index(drop=True)
    buy_signal, sell_signal = build_signals(d, strategy_name, params)

    cash = float(initial_cash)
    shares = 0.0
    entry_price = 0.0
    equity_list: list[float] = []
    trade_rows: list[dict[str, object]] = []

    for i, row in d.iterrows():
        price = _to_float_scalar(row["Close"])
        high = _to_float_scalar(row["High"])
        low = _to_float_scalar(row["Low"])
        dt = pd.to_datetime(row["Date"])

        if shares > 0:
            forced_exit = False
            exit_price = price
            reason = "signal"
            if stop_loss_pct is not None and stop_loss_pct > 0:
                sl_price = entry_price * (1 - stop_loss_pct / 100)
                if low <= sl_price:
                    exit_price = sl_price
                    forced_exit = True
                    reason = "stop_loss"
            if (not forced_exit) and take_profit_pct is not None and take_profit_pct > 0:
                tp_price = entry_price * (1 + take_profit_pct / 100)
                if high >= tp_price:
                    exit_price = tp_price
                    forced_exit = True
                    reason = "take_profit"

            if forced_exit or bool(sell_signal.iloc[i]):
                proceeds = shares * exit_price
                fee = proceeds * commission
                cash += proceeds - fee
                pnl = (exit_price - entry_price) * shares - fee
                trade_rows.append(
                    {
                        "date": dt,
                        "type": "SELL",
                        "price": exit_price,
                        "shares": shares,
                        "pnl": pnl,
                        "reason": reason,
                    }
                )
                shares = 0.0
                entry_price = 0.0

        if shares == 0 and bool(buy_signal.iloc[i]) and cash > 0:
            # Position sizing:
            # - all_in: use all available cash
            # - fixed_pct: use cash * position_size_value (0-1)
            # - risk_pct: risk fixed % of current equity based on stop-loss distance
            buy_amount = cash
            if position_sizing_method == "fixed_pct":
                ratio = min(max(float(position_size_value), 0.01), 1.0)
                buy_amount = cash * ratio
            elif position_sizing_method == "risk_pct":
                risk_ratio = min(max(float(position_size_value), 0.001), 0.2)
                equity_now = cash + shares * price
                risk_budget = equity_now * risk_ratio
                sl_pct = float(stop_loss_pct) if stop_loss_pct is not None and stop_loss_pct > 0 else 5.0
                risk_per_share = max(price * (sl_pct / 100), 1e-9)
                qty_by_risk = risk_budget / risk_per_share
                buy_amount = min(cash, qty_by_risk * price)

            fee = buy_amount * commission
            investable = buy_amount - fee
            qty = investable / price if price > 0 else 0
            if qty > 0:
                shares = qty
                entry_price = price
                cash = 0.0
                trade_rows.append({"date": dt, "type": "BUY", "price": price, "shares": qty, "pnl": 0.0, "reason": "signal"})

        equity = cash + shares * price
        equity_list.append(equity)

    if shares > 0:
        last_price = _to_float_scalar(d.iloc[-1]["Close"])
        proceeds = shares * last_price
        fee = proceeds * commission
        cash += proceeds - fee
        pnl = (last_price - entry_price) * shares - fee
        trade_rows.append(
            {
                "date": pd.to_datetime(d.iloc[-1]["Date"]),
                "type": "SELL",
                "price": last_price,
                "shares": shares,
                "pnl": pnl,
                "reason": "final_close",
            }
        )
        shares = 0.0
        equity_list[-1] = cash

    equity_curve = pd.DataFrame({"Date": pd.to_datetime(d["Date"]), "Equity": equity_list})
    trade_log = pd.DataFrame(trade_rows)

    total_return = (equity_curve["Equity"].iloc[-1] / initial_cash - 1) * 100
    buy_hold = (_to_float_scalar(d["Close"].iloc[-1]) / _to_float_scalar(d["Close"].iloc[0]) - 1) * 100
    rolling_max = equity_curve["Equity"].cummax()
    drawdown = (equity_curve["Equity"] / rolling_max - 1) * 100
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0

    pnl_by_trade = trade_log[trade_log["type"] == "SELL"]["pnl"] if not trade_log.empty else pd.Series(dtype=float)
    wins = pnl_by_trade[pnl_by_trade > 0]
    losses = pnl_by_trade[pnl_by_trade < 0]
    win_rate = (len(wins) / len(pnl_by_trade) * 100) if len(pnl_by_trade) > 0 else 0.0
    pf = (wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0 else float("inf")

    daily_ret = equity_curve["Equity"].pct_change().dropna()
    sharpe = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() and daily_ret.std() > 0 else 0.0

    return BacktestResult(
        strategy_name=strategy_name,
        ticker=ticker,
        start_date=str(pd.to_datetime(d["Date"].iloc[0]).date()),
        end_date=str(pd.to_datetime(d["Date"].iloc[-1]).date()),
        params=params,
        total_return_pct=float(total_return),
        buy_hold_return_pct=float(buy_hold),
        max_drawdown_pct=float(max_dd),
        win_rate_pct=float(win_rate),
        sharpe_ratio=float(sharpe),
        profit_factor=float(pf if np.isfinite(pf) else 999.0),
        trades=int(len(pnl_by_trade)),
        equity_curve=equity_curve,
        trade_log=trade_log,
    )


def optimize_golden_cross(
    df: pd.DataFrame,
    ticker: str,
    short_range: range,
    long_range: range,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total = sum(1 for s in short_range for l in long_range if s < l)
    processed = 0
    for s in short_range:
        for l in long_range:
            if s >= l:
                continue
            result = run_backtest(df, ticker=ticker, strategy_name="golden_cross", params={"short": s, "long": l})
            rows.append(
                {
                    "short": s,
                    "long": l,
                    "return_pct": result.total_return_pct,
                    "max_dd_pct": result.max_drawdown_pct,
                    "win_rate_pct": result.win_rate_pct,
                    "sharpe": result.sharpe_ratio,
                    "profit_factor": result.profit_factor,
                    "trades": result.trades,
                }
            )
            processed += 1
            if progress_callback is not None:
                progress_callback(processed, total)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).sort_values(["sharpe", "return_pct"], ascending=[False, False]).reset_index(drop=True)
    return out


def compare_strategies(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    strategies = [
        ("GoldenCross", "golden_cross", {"short": 15, "long": 60}),
        ("RsiReversal", "rsi_reversal", {"rsi_low": 30, "rsi_high": 70}),
        ("MacdCross", "macd_cross", {}),
        ("BbBreakout", "bb_breakout", {}),
        ("ComboStrategy", "combo", {"rsi_low": 35, "rsi_high": 65}),
    ]
    rows: list[dict[str, object]] = []
    for label, key, params in strategies:
        r = run_backtest(df, ticker=ticker, strategy_name=key, params=params)
        rows.append(
            {
                "strategy_name": label,
                "total_return_pct": r.total_return_pct,
                "buy_hold_return_pct": r.buy_hold_return_pct,
                "max_drawdown_pct": r.max_drawdown_pct,
                "win_rate_pct": r.win_rate_pct,
                "sharpe_ratio": r.sharpe_ratio,
                "profit_factor": r.profit_factor,
                "trades": r.trades,
                "params_json": json.dumps(params, ensure_ascii=False),
            }
        )
    return pd.DataFrame(rows).sort_values("sharpe_ratio", ascending=False).reset_index(drop=True)


def save_backtest_result(result: BacktestResult) -> int:
    ensure_backtest_tables()
    sql = """
            INSERT INTO backtest_results(
                strategy_name, ticker, start_date, end_date, params_json,
                total_return_pct, buy_hold_return_pct, max_drawdown_pct, win_rate_pct,
                sharpe_ratio, profit_factor, trades, created_at
            )
            VALUES (:strategy_name, :ticker, :start_date, :end_date, :params_json,
                    :total_return_pct, :buy_hold_return_pct, :max_drawdown_pct, :win_rate_pct,
                    :sharpe_ratio, :profit_factor, :trades, :created_at)
            """
    if engine.url.get_backend_name().lower() != "sqlite":
        sql += " RETURNING id"
    params = {
        "strategy_name": result.strategy_name,
        "ticker": result.ticker,
        "start_date": result.start_date,
        "end_date": result.end_date,
        "params_json": json.dumps(result.params, ensure_ascii=False),
        "total_return_pct": result.total_return_pct,
        "buy_hold_return_pct": result.buy_hold_return_pct,
        "max_drawdown_pct": result.max_drawdown_pct,
        "win_rate_pct": result.win_rate_pct,
        "sharpe_ratio": result.sharpe_ratio,
        "profit_factor": result.profit_factor,
        "trades": result.trades,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with engine.begin() as con:
        res = con.execute(text(sql), params)
        if engine.url.get_backend_name().lower() != "sqlite":
            return int(res.scalar_one())
        return int(con.execute(text("SELECT last_insert_rowid()")).scalar_one())


def save_comparison_results(ticker: str, start_date: str, end_date: str, df: pd.DataFrame) -> int:
    ensure_backtest_tables()
    inserted = 0
    with engine.begin() as con:
        for _, row in df.iterrows():
            con.execute(
                text(
                    """
                INSERT INTO backtest_results(
                    strategy_name, ticker, start_date, end_date, params_json,
                    total_return_pct, buy_hold_return_pct, max_drawdown_pct, win_rate_pct,
                    sharpe_ratio, profit_factor, trades, created_at
                )
                VALUES (:strategy_name, :ticker, :start_date, :end_date, :params_json,
                        :total_return_pct, :buy_hold_return_pct, :max_drawdown_pct, :win_rate_pct,
                        :sharpe_ratio, :profit_factor, :trades, :created_at)
                """
                ),
                {
                    "strategy_name": str(row["strategy_name"]),
                    "ticker": ticker,
                    "start_date": start_date,
                    "end_date": end_date,
                    "params_json": str(row.get("params_json", "{}")),
                    "total_return_pct": float(row["total_return_pct"]),
                    "buy_hold_return_pct": float(row["buy_hold_return_pct"]),
                    "max_drawdown_pct": float(row["max_drawdown_pct"]),
                    "win_rate_pct": float(row["win_rate_pct"]),
                    "sharpe_ratio": float(row["sharpe_ratio"]),
                    "profit_factor": float(row["profit_factor"]),
                    "trades": int(row["trades"]),
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
            )
            inserted += 1
    return inserted


def get_backtest_history(
    strategy_name: str | None = None,
    ticker: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    ensure_backtest_tables()
    where = ["1=1"]
    params: dict[str, object] = {}
    if strategy_name and strategy_name != "すべて":
        where.append("strategy_name = :strategy_name")
        params["strategy_name"] = strategy_name
    if ticker and ticker != "すべて":
        where.append("ticker = :ticker")
        params["ticker"] = ticker.upper()
    if date_from:
        where.append("created_at >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("created_at <= :date_to")
        params["date_to"] = date_to

    sql = f"""
    SELECT id, strategy_name, ticker, start_date, end_date, params_json,
           total_return_pct, buy_hold_return_pct, max_drawdown_pct, win_rate_pct,
           sharpe_ratio, profit_factor, trades, created_at
    FROM backtest_results
    WHERE {" AND ".join(where)}
    ORDER BY id DESC
    """
    return pd.read_sql(text(sql), con=engine, params=params)


def build_ai_strategy_report(compare_df: pd.DataFrame) -> str:
    if compare_df.empty:
        return "比較対象データがありません。"
    best_balanced = compare_df.sort_values("sharpe_ratio", ascending=False).iloc[0]
    best_return = compare_df.sort_values("total_return_pct", ascending=False).iloc[0]
    safest = compare_df.sort_values("max_drawdown_pct", ascending=False).iloc[0]
    return (
        "AI戦略比較レポート\n"
        f"- バランス重視推奨: {best_balanced['strategy_name']}（シャープ {best_balanced['sharpe_ratio']:.2f}）\n"
        f"- リターン重視推奨: {best_return['strategy_name']}（総リターン {best_return['total_return_pct']:.1f}%）\n"
        f"- 安定重視推奨: {safest['strategy_name']}（最大DD {safest['max_drawdown_pct']:.1f}%）\n"
        "注意: 過学習回避のため、別期間で再検証してから運用採用してください。"
    )
