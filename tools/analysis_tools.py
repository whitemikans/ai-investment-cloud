from __future__ import annotations

import json

import numpy as np
import pandas as pd
import yfinance as yf


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


@tool("テクニカル分析")
def technical_analysis(ticker: str) -> str:
    """RSI/MACD/ボリンジャー/移動平均を計算しテクニカルスコアを返します。"""
    code = str(ticker).strip().upper()
    df = yf.Ticker(code).history(period="6mo")
    if df is None or df.empty:
        return json.dumps({"ticker": code, "error": "data not found"}, ensure_ascii=False)

    close = pd.to_numeric(df["Close"], errors="coerce").dropna()
    if len(close) < 30:
        return json.dumps({"ticker": code, "error": "insufficient history"}, ensure_ascii=False)

    ma5 = float(close.rolling(5).mean().iloc[-1])
    ma25 = float(close.rolling(25).mean().iloc[-1])
    ma75 = float(close.rolling(75).mean().iloc[-1]) if len(close) >= 75 else float(close.mean())

    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean().replace(0, np.nan)
    rsi = 100 - (100 / (1 + (gain / loss)))
    current_rsi = float(rsi.fillna(50).iloc[-1])

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = float((macd - signal).iloc[-1])

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_upper = float((bb_mid + 2 * bb_std).iloc[-1])
    bb_lower = float((bb_mid - 2 * bb_std).iloc[-1])

    trend = "もみ合い"
    score = 3
    if close.iloc[-1] > ma25 > ma75:
        trend = "上昇"
        score = 4
    elif close.iloc[-1] < ma25 < ma75:
        trend = "下降"
        score = 2
    if current_rsi < 30:
        score += 1
    elif current_rsi > 70:
        score -= 1
    score = int(max(1, min(5, score)))

    signals = []
    if ma5 > ma25 and close.rolling(5).mean().iloc[-2] <= close.rolling(25).mean().iloc[-2]:
        signals.append("ゴールデンクロス")
    if ma5 < ma25 and close.rolling(5).mean().iloc[-2] >= close.rolling(25).mean().iloc[-2]:
        signals.append("デッドクロス")
    if current_rsi > 70:
        signals.append("RSI過熱")
    if current_rsi < 30:
        signals.append("RSI冷却")

    out = {
        "ticker": code,
        "current_price": float(close.iloc[-1]),
        "rsi14": current_rsi,
        "macd_hist": macd_hist,
        "ma5": ma5,
        "ma25": ma25,
        "ma75": ma75,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "trend": trend,
        "signals": signals or ["シグナルなし"],
        "technical_score": score,
    }
    return json.dumps(out, ensure_ascii=False)


@tool("財務分析")
def fundamental_analysis(ticker: str) -> str:
    """PER/PBR/ROE/配当利回り/成長率を取得しファンダスコアを返します。"""
    code = str(ticker).strip().upper()
    info = yf.Ticker(code).info or {}
    per = info.get("trailingPE")
    pbr = info.get("priceToBook")
    div = (info.get("dividendYield") or 0.0) * 100
    roe = (info.get("returnOnEquity") or 0.0) * 100
    rev_growth = (info.get("revenueGrowth") or 0.0) * 100
    earn_growth = (info.get("earningsGrowth") or 0.0) * 100

    valuation_label = "適正"
    valuation_score = 3
    if per is not None and per < 12:
        valuation_label = "割安"
        valuation_score = 4
    elif per is not None and per > 25:
        valuation_label = "割高"
        valuation_score = 2

    growth_score = 3
    if rev_growth > 10 and earn_growth > 10:
        growth_score = 5
    elif rev_growth > 5:
        growth_score = 4
    elif rev_growth < 0:
        growth_score = 2

    out = {
        "ticker": code,
        "per": per if per is not None else None,
        "pbr": pbr if pbr is not None else None,
        "dividend_yield_pct": div,
        "roe_pct": roe,
        "revenue_growth_pct": rev_growth,
        "earnings_growth_pct": earn_growth,
        "valuation": valuation_label,
        "valuation_score": valuation_score,
        "growth_score": growth_score,
    }
    return json.dumps(out, ensure_ascii=False)


@tool("5軸評価")
def five_axis_evaluation(
    news_score: float,
    technical_score: float,
    valuation_score: float,
    growth_score: float,
    risk_score: float,
) -> str:
    """5軸スコアを加重平均して推奨度と推奨アクションを返します。"""
    weights = {
        "news": 0.20,
        "technical": 0.20,
        "valuation": 0.20,
        "growth": 0.25,
        "risk": 0.15,
    }
    total = (
        float(news_score) * weights["news"]
        + float(technical_score) * weights["technical"]
        + float(valuation_score) * weights["valuation"]
        + float(growth_score) * weights["growth"]
        + float(risk_score) * weights["risk"]
    )
    stars = max(1, min(5, int(round(total))))
    if total >= 3.8:
        action = "買い"
    elif total <= 2.4:
        action = "売り"
    else:
        action = "保持"

    return json.dumps(
        {
            "weighted_score": round(total, 2),
            "stars": stars,
            "action": action,
        },
        ensure_ascii=False,
    )

