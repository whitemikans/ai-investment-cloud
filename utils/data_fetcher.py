from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PORTFOLIO_FILE = PROJECT_ROOT / "portfolio_input.csv"

PERIOD_MAP = {
    "1ヶ月": "1mo",
    "3ヶ月": "3mo",
    "6ヶ月": "6mo",
    "1年": "1y",
    "3年": "3y",
    "5年": "5y",
}


def _safe_float(value: object, default: float = np.nan) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _score_by_range(value: float, good_min: float | None = None, good_max: float | None = None) -> float:
    if np.isnan(value):
        return 50.0
    if good_min is not None and value >= good_min:
        return min(100.0, 60.0 + (value - good_min) * 2.0)
    if good_max is not None and value <= good_max:
        return min(100.0, 60.0 + (good_max - value) * 8.0)
    return max(10.0, 40.0)


@st.cache_data(ttl=300)
def fetch_price_data(ticker: str, period_label: str = "1年") -> tuple[pd.DataFrame, dict]:
    yf_ticker = yf.Ticker(ticker.strip().upper())
    period = PERIOD_MAP.get(period_label, "1y")
    hist = yf_ticker.history(period=period, auto_adjust=False)
    info = yf_ticker.info if yf_ticker.info else {}
    if hist.empty:
        return pd.DataFrame(), info
    hist = hist.reset_index()
    hist["Date"] = pd.to_datetime(hist["Date"])
    return hist, info


@st.cache_data(ttl=300)
def fetch_price_data_by_dates(ticker: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    yf_ticker = yf.Ticker(ticker.strip().upper())
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date) + pd.Timedelta(days=1)
    hist = yf_ticker.history(start=start_ts, end=end_ts, auto_adjust=False)
    info = yf_ticker.info if yf_ticker.info else {}
    if hist.empty:
        return pd.DataFrame(), info
    hist = hist.reset_index()
    hist["Date"] = pd.to_datetime(hist["Date"])
    return hist, info


def add_moving_average(df: pd.DataFrame, windows: Iterable[int] = (5, 25, 75)) -> pd.DataFrame:
    """Add simple moving average columns for the given window sizes."""
    copied = df.copy()
    for window in windows:
        copied[f"MA{window}"] = copied["Close"].rolling(window=window).mean()
    return copied


def calculate_moving_averages(df: pd.DataFrame, price_col: str = "Close") -> pd.DataFrame:
    """Calculate 5/25/75-day simple moving averages and append them to a copy of the input DataFrame."""
    copied = df.copy()
    copied["MA5"] = copied[price_col].rolling(window=5).mean()
    copied["MA25"] = copied[price_col].rolling(window=25).mean()
    copied["MA75"] = copied[price_col].rolling(window=75).mean()
    return copied


def calculate_bollinger_bands(
    df: pd.DataFrame,
    price_col: str = "Close",
    window: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    """Calculate Bollinger Bands (default: 20-day, 2σ) and append middle/upper/lower band columns."""
    copied = df.copy()
    copied["BB_MID"] = copied[price_col].rolling(window=window).mean()
    rolling_std = copied[price_col].rolling(window=window).std(ddof=0)
    copied["BB_UPPER"] = copied["BB_MID"] + num_std * rolling_std
    copied["BB_LOWER"] = copied["BB_MID"] - num_std * rolling_std
    return copied


def calculate_rsi(df: pd.DataFrame, price_col: str = "Close", window: int = 14) -> pd.DataFrame:
    """Calculate RSI (default: 14-day) and append an RSI column to a copy of the input DataFrame."""
    copied = df.copy()
    delta = copied[price_col].diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    copied["RSI"] = 100 - (100 / (1 + rs))
    return copied


def calculate_daily_returns(df: pd.DataFrame, price_col: str = "Close") -> pd.DataFrame:
    """Calculate day-over-day returns and append a DailyReturn column."""
    copied = df.copy()
    copied["DailyReturn"] = copied[price_col].pct_change()
    return copied


def calculate_monthly_returns(
    df: pd.DataFrame,
    date_col: str = "Date",
    price_col: str = "Close",
) -> pd.DataFrame:
    """Aggregate daily price data into monthly returns using month-end close prices."""
    work = df[[date_col, price_col]].copy()
    work[date_col] = pd.to_datetime(work[date_col])
    work = work.sort_values(date_col)
    monthly = work.set_index(date_col)[price_col].resample("ME").last().to_frame(name="MonthEndClose")
    monthly["MonthlyReturn"] = monthly["MonthEndClose"].pct_change()
    monthly = monthly.reset_index()
    monthly["Month"] = monthly[date_col].dt.strftime("%Y-%m")
    return monthly[["Month", "MonthEndClose", "MonthlyReturn"]]


@st.cache_data(ttl=300)
def fetch_financial_trend(ticker: str, years: int = 5) -> pd.DataFrame:
    """Fetch annual financial trend data (revenue, operating income, margin, YoY) from yfinance."""
    yf_ticker = yf.Ticker(ticker.strip().upper())

    stmt = yf_ticker.financials
    if stmt is None or stmt.empty:
        stmt = yf_ticker.income_stmt
    if stmt is None or stmt.empty:
        return pd.DataFrame()

    row_candidates = {
        "Revenue": ["Total Revenue", "TotalRevenue"],
        "OperatingIncome": ["Operating Income", "OperatingIncome"],
    }

    def _pick_row(candidates: list[str]) -> pd.Series | None:
        for name in candidates:
            if name in stmt.index:
                return stmt.loc[name]
        return None

    revenue_row = _pick_row(row_candidates["Revenue"])
    op_income_row = _pick_row(row_candidates["OperatingIncome"])
    if revenue_row is None or op_income_row is None:
        return pd.DataFrame()

    trend = pd.DataFrame(
        {
            "Date": pd.to_datetime(stmt.columns),
            "Revenue": pd.to_numeric(revenue_row.values, errors="coerce"),
            "OperatingIncome": pd.to_numeric(op_income_row.values, errors="coerce"),
        }
    )
    trend = trend.dropna(subset=["Revenue", "OperatingIncome"]).sort_values("Date")
    if trend.empty:
        return pd.DataFrame()

    trend = trend.tail(max(1, years)).reset_index(drop=True)
    trend["Year"] = trend["Date"].dt.year.astype(str)
    trend["OperatingMargin"] = np.where(
        trend["Revenue"] != 0,
        trend["OperatingIncome"] / trend["Revenue"] * 100,
        np.nan,
    )
    trend["RevenueYoY"] = trend["Revenue"].pct_change() * 100
    trend["OperatingIncomeYoY"] = trend["OperatingIncome"].pct_change() * 100
    return trend[
        ["Year", "Date", "Revenue", "OperatingIncome", "OperatingMargin", "RevenueYoY", "OperatingIncomeYoY"]
    ]


def build_market_metrics(info: dict, hist: pd.DataFrame) -> dict[str, float]:
    latest_close = _safe_float(hist["Close"].iloc[-1]) if not hist.empty else np.nan
    prev_close = _safe_float(hist["Close"].iloc[-2]) if len(hist) > 1 else latest_close
    change = latest_close - prev_close if not np.isnan(latest_close) and not np.isnan(prev_close) else np.nan
    change_pct = (change / prev_close * 100) if prev_close not in (0, np.nan) else np.nan
    return {
        "現在株価": latest_close,
        "前日比": change,
        "前日比率": change_pct,
        "出来高": _safe_float(hist["Volume"].iloc[-1]) if not hist.empty else np.nan,
        "PER": _safe_float(info.get("trailingPE")),
        "PBR": _safe_float(info.get("priceToBook")),
        "配当利回り": _safe_float(info.get("dividendYield")) * 100,
        "時価総額": _safe_float(info.get("marketCap")),
    }


def five_axis_scores(info: dict, hist: pd.DataFrame | None = None) -> dict[str, float]:
    roe = _safe_float(info.get("returnOnEquity")) * 100
    revenue_growth = _safe_float(info.get("revenueGrowth")) * 100
    debt_to_equity = _safe_float(info.get("debtToEquity"))
    pbr = _safe_float(info.get("priceToBook"))
    trailing_pe = _safe_float(info.get("trailingPE"))

    momentum_value = 0.0
    if hist is not None and len(hist) > 30:
        base = _safe_float(hist["Close"].iloc[0])
        latest = _safe_float(hist["Close"].iloc[-1])
        if base > 0:
            momentum_value = (latest / base - 1.0) * 100

    profitability = _score_by_range(roe, good_min=15.0)
    growth = _score_by_range(revenue_growth, good_min=10.0)
    safety = _score_by_range(debt_to_equity, good_max=80.0)
    value = np.nanmean([_score_by_range(pbr, good_max=1.5), _score_by_range(trailing_pe, good_max=20.0)])
    momentum = _score_by_range(momentum_value, good_min=10.0)

    return {
        "収益性": round(float(profitability), 1),
        "成長性": round(float(growth), 1),
        "安全性": round(float(safety), 1),
        "割安度": round(float(value), 1),
        "モメンタム": round(float(momentum), 1),
    }


def get_company_name(info: dict, ticker: str) -> str:
    return (
        info.get("longName")
        or info.get("shortName")
        or info.get("displayName")
        or ticker.upper()
    )


@st.cache_data(ttl=300)
def build_compare_table(tickers: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    for ticker in tickers:
        clean = ticker.strip().upper()
        if not clean:
            continue
        yf_ticker = yf.Ticker(clean)
        info = yf_ticker.info if yf_ticker.info else {}
        rows.append(
            {
                "Ticker": clean,
                "企業名": get_company_name(info, clean),
                "PER": _safe_float(info.get("trailingPE")),
                "PBR": _safe_float(info.get("priceToBook")),
                "ROE(%)": _safe_float(info.get("returnOnEquity")) * 100,
                "配当利回り(%)": _safe_float(info.get("dividendYield")) * 100,
                "時価総額": _safe_float(info.get("marketCap")),
            }
        )
    return pd.DataFrame(rows)


def load_or_init_portfolio() -> pd.DataFrame:
    if PORTFOLIO_FILE.exists():
        df = pd.read_csv(PORTFOLIO_FILE)
        if not df.empty:
            return df

    sample = pd.DataFrame(
        [
            {"ticker": "AAPL", "avg_cost": 185.0, "shares": 100, "purchase_date": "2025-01-10"},
            {"ticker": "MSFT", "avg_cost": 395.0, "shares": 50, "purchase_date": "2025-02-05"},
        ]
    )
    save_portfolio(sample)
    return sample


def save_portfolio(df: pd.DataFrame) -> None:
    df.to_csv(PORTFOLIO_FILE, index=False, encoding="utf-8-sig")


def append_holding(df: pd.DataFrame, ticker: str, avg_cost: float, shares: int, purchase_date: str) -> pd.DataFrame:
    appended = pd.concat(
        [
            df,
            pd.DataFrame(
                [
                    {
                        "ticker": ticker.strip().upper(),
                        "avg_cost": float(avg_cost),
                        "shares": int(shares),
                        "purchase_date": purchase_date,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    return appended


@st.cache_data(ttl=300)
def enrich_portfolio(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    rows: list[dict] = []
    for _, row in df.iterrows():
        ticker = str(row["ticker"]).strip().upper()
        info = yf.Ticker(ticker).info or {}
        price = _safe_float(info.get("regularMarketPrice"))
        if np.isnan(price):
            hist = yf.Ticker(ticker).history(period="5d")
            if not hist.empty:
                price = _safe_float(hist["Close"].iloc[-1])
        shares = _safe_float(row["shares"], 0.0)
        avg_cost = _safe_float(row["avg_cost"], 0.0)
        invested = avg_cost * shares
        current_value = price * shares if not np.isnan(price) else np.nan
        pnl = current_value - invested if not np.isnan(current_value) else np.nan
        pnl_pct = (pnl / invested * 100) if invested > 0 and not np.isnan(pnl) else np.nan
        annual_div_per_share = _safe_float(info.get("dividendRate"), 0.0)
        annual_div = annual_div_per_share * shares
        rows.append(
            {
                "Ticker": ticker,
                "企業名": get_company_name(info, ticker),
                "セクター": info.get("sector", "Unknown"),
                "保有数": shares,
                "取得単価": avg_cost,
                "現在値": price,
                "投資元本": invested,
                "評価額": current_value,
                "評価損益": pnl,
                "評価損益率(%)": pnl_pct,
                "年間配当(予想)": annual_div,
            }
        )
    return pd.DataFrame(rows)
