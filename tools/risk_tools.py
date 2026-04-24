from __future__ import annotations

import json

import numpy as np
import pandas as pd
import yfinance as yf

from db.db_utils import get_portfolio_df_with_price


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


@tool("ポートフォリオリスクチェック")
def portfolio_risk_check() -> str:
    """集中リスク/相関リスク/配分上限をチェックして警告を返します。"""
    df = get_portfolio_df_with_price()
    if df.empty:
        return json.dumps({"risk_score": 0, "warnings": ["ポートフォリオデータなし"]}, ensure_ascii=False)

    work = df.copy()
    work["market_value"] = pd.to_numeric(work["market_value"], errors="coerce").fillna(0.0)
    total = float(work["market_value"].sum())
    if total <= 0:
        return json.dumps({"risk_score": 0, "warnings": ["時価総額合計が0"]}, ensure_ascii=False)
    work["weight"] = work["market_value"] / total

    warnings: list[str] = []

    sector = work.groupby("sector", dropna=False)["weight"].sum().sort_values(ascending=False)
    if not sector.empty and float(sector.iloc[0]) > 0.30:
        warnings.append(f"セクター集中: {sector.index[0]} が {sector.iloc[0] * 100:.1f}%")

    top = work.sort_values("weight", ascending=False).iloc[0]
    if float(top["weight"]) > 0.35:
        warnings.append(f"銘柄集中: {top['stock_code']} が {float(top['weight']) * 100:.1f}%")

    tickers = work["stock_code"].astype(str).tolist()
    corr_alert = ""
    try:
        price = yf.download(tickers, period="6mo", auto_adjust=True, progress=False)
        if "Close" in price.columns:
            close = price["Close"]
        else:
            close = price
        if isinstance(close, pd.Series):
            close = close.to_frame(name=tickers[0])
        ret = close.pct_change().dropna(how="all")
        corr = ret.corr().fillna(0.0)
        w_map = dict(zip(work["stock_code"], work["weight"]))
        risky_weight = 0.0
        for i, a in enumerate(corr.index):
            for b in corr.columns[i + 1 :]:
                if float(corr.loc[a, b]) >= 0.7:
                    risky_weight += float(w_map.get(a, 0.0)) + float(w_map.get(b, 0.0))
        if risky_weight >= 0.40:
            corr_alert = f"高相関リスク: 相関0.7超の組み合わせ比率合計が {risky_weight * 100:.1f}%"
            warnings.append(corr_alert)
    except Exception:
        pass

    risk_score = max(0, min(100, 85 - len(warnings) * 12))
    return json.dumps(
        {
            "risk_score": risk_score,
            "warnings": warnings or ["重大な集中リスクなし"],
            "top_sector_weight": float(sector.iloc[0]) if not sector.empty else 0.0,
            "top_stock_weight": float(top["weight"]),
        },
        ensure_ascii=False,
    )


@tool("ストレステスト")
def stress_test(scenario: str = "lehman") -> str:
    """暴落シナリオで想定損失を推定します。"""
    shocks = {
        "lehman": -0.45,
        "covid": -0.30,
    }
    shock = float(shocks.get(str(scenario).lower(), -0.35))
    df = get_portfolio_df_with_price()
    if df.empty:
        return json.dumps({"scenario": scenario, "loss": 0.0, "loss_pct": 0.0}, ensure_ascii=False)

    mv = pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0)
    total = float(mv.sum())
    loss = total * abs(shock)
    return json.dumps(
        {
            "scenario": scenario,
            "shock": shock,
            "estimated_loss": round(loss, 2),
            "estimated_loss_pct": round(abs(shock) * 100, 2),
            "post_stress_value": round(total - loss, 2),
        },
        ensure_ascii=False,
    )

