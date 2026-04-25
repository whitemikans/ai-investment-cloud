from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf


RISK_PROFILE_BUCKETS = {
    "保守的": {"Invest": 0.50, "Watch": 0.30, "Research": 0.12, "Hold": 0.08},
    "標準": {"Invest": 0.60, "Watch": 0.25, "Research": 0.10, "Hold": 0.05},
    "積極的": {"Invest": 0.70, "Watch": 0.20, "Research": 0.08, "Hold": 0.02},
}

REPRESENTATIVE_UNIVERSE = [
    {"theme": "AI", "ticker": "NVDA", "name": "NVIDIA"},
    {"theme": "AI", "ticker": "MSFT", "name": "Microsoft"},
    {"theme": "Quantum", "ticker": "IONQ", "name": "IonQ"},
    {"theme": "Quantum", "ticker": "6701.T", "name": "NEC"},
    {"theme": "Biotech", "ticker": "MRNA", "name": "Moderna"},
    {"theme": "Biotech", "ticker": "4568.T", "name": "第一三共"},
    {"theme": "Space", "ticker": "RKLB", "name": "Rocket Lab"},
    {"theme": "Space", "ticker": "7011.T", "name": "三菱重工"},
    {"theme": "Energy", "ticker": "TSLA", "name": "Tesla"},
    {"theme": "Energy", "ticker": "6752.T", "name": "パナソニック"},
    {"theme": "Robotics", "ticker": "TSLA", "name": "Tesla"},
    {"theme": "Robotics", "ticker": "6981.T", "name": "村田製作所"},
]


@dataclass
class PortfolioArtifacts:
    theme_alloc_df: pd.DataFrame
    ticker_alloc_df: pd.DataFrame
    corr_df: pd.DataFrame
    backtest_df: pd.DataFrame
    metrics: dict[str, float]


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    s = sum(float(v) for v in weights.values())
    if s <= 0:
        n = max(1, len(weights))
        return {k: 1.0 / n for k in weights}
    return {k: float(v) / s for k, v in weights.items()}


def _theme_weights_from_radar(radar_df: pd.DataFrame, risk_tolerance: str) -> pd.DataFrame:
    if radar_df is None or radar_df.empty:
        return pd.DataFrame(columns=["theme", "radar_stage", "weight"])

    buckets = RISK_PROFILE_BUCKETS.get(str(risk_tolerance), RISK_PROFILE_BUCKETS["標準"])
    work = radar_df[["tech_theme", "radar_stage"]].copy().drop_duplicates()
    work = work.rename(columns={"tech_theme": "theme"})

    rows: list[dict[str, Any]] = []
    present_classes = set(work["radar_stage"].astype(str).tolist())
    active_bucket = {k: v for k, v in buckets.items() if k in present_classes}
    if not active_bucket:
        active_bucket = {"Watch": 1.0}
    active_bucket = _normalize_weights(active_bucket)

    for cls, cls_w in active_bucket.items():
        sub = work[work["radar_stage"] == cls]
        if sub.empty:
            continue
        per = float(cls_w) / len(sub)
        for r in sub.itertuples(index=False):
            rows.append({"theme": str(getattr(r, "theme")), "radar_stage": cls, "weight": per})

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["theme", "radar_stage", "weight"])
    out["weight"] = out["weight"] / out["weight"].sum()
    return out.sort_values("weight", ascending=False).reset_index(drop=True)


def _download_adj_close(tickers: list[str], period: str = "3y") -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    uniq = sorted(set(str(t).strip().upper() for t in tickers if str(t).strip()))
    if not uniq:
        return pd.DataFrame()
    try:
        raw = yf.download(uniq, period=period, auto_adjust=True, progress=False)
    except Exception:
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" in raw.columns.get_level_values(0):
            close = raw["Close"].copy()
        else:
            close = raw.copy()
    else:
        close = raw.to_frame(name=uniq[0]) if len(uniq) == 1 else raw.copy()
    if isinstance(close, pd.Series):
        close = close.to_frame(name=uniq[0])
    close = close.dropna(how="all")
    close.columns = [str(c).upper() for c in close.columns]
    return close


def _build_theme_return_series(theme_alloc_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rep = pd.DataFrame(REPRESENTATIVE_UNIVERSE)
    merged = theme_alloc_df.merge(rep, on="theme", how="left")
    merged = merged.dropna(subset=["ticker"]).reset_index(drop=True)
    if merged.empty:
        return pd.DataFrame(), merged
    prices = _download_adj_close(merged["ticker"].astype(str).tolist(), period="3y")
    if prices.empty:
        return pd.DataFrame(), merged

    rets = prices.pct_change().dropna(how="all")
    theme_series: dict[str, pd.Series] = {}
    for theme, g in merged.groupby("theme"):
        cols = [str(t).upper() for t in g["ticker"].astype(str).tolist() if str(t).upper() in rets.columns]
        if not cols:
            continue
        theme_series[str(theme)] = rets[cols].mean(axis=1, skipna=True)
    if not theme_series:
        return pd.DataFrame(), merged
    out = pd.DataFrame(theme_series).dropna(how="all")
    return out, merged


def _backtest_from_theme_returns(theme_rets: pd.DataFrame, theme_alloc_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    if theme_rets.empty or theme_alloc_df.empty:
        return pd.DataFrame(columns=["date", "portfolio", "benchmark"]), {}

    w = theme_alloc_df.set_index("theme")["weight"].to_dict()
    common = [c for c in theme_rets.columns if c in w]
    if not common:
        return pd.DataFrame(columns=["date", "portfolio", "benchmark"]), {}

    aligned = theme_rets[common].copy().fillna(0.0)
    pw = np.array([float(w[c]) for c in common], dtype=float)
    pw = pw / pw.sum()
    port_daily = aligned.mul(pw, axis=1).sum(axis=1)

    spy = _download_adj_close(["SPY"], period="3y")
    if not spy.empty and "SPY" in spy.columns:
        spy_ret = spy["SPY"].pct_change().reindex(port_daily.index).fillna(0.0)
    else:
        spy_ret = pd.Series(np.zeros(len(port_daily)), index=port_daily.index)

    port_curve = (1.0 + port_daily).cumprod()
    spy_curve = (1.0 + spy_ret).cumprod()
    bt = pd.DataFrame({"date": port_curve.index, "portfolio": port_curve.values, "benchmark": spy_curve.values})

    years = max(1e-9, len(port_daily) / 252.0)
    total_ret = float(port_curve.iloc[-1] - 1.0)
    cagr = float((port_curve.iloc[-1] ** (1.0 / years)) - 1.0)
    vol = float(port_daily.std(ddof=0) * np.sqrt(252))
    sharpe = float((port_daily.mean() * 252) / vol) if vol > 1e-9 else 0.0
    max_dd = float((port_curve / port_curve.cummax() - 1.0).min())
    bench_ret = float(spy_curve.iloc[-1] - 1.0)
    metrics = {
        "total_return_pct": total_ret * 100.0,
        "cagr_pct": cagr * 100.0,
        "volatility_pct": vol * 100.0,
        "sharpe": sharpe,
        "max_drawdown_pct": max_dd * 100.0,
        "benchmark_return_pct": bench_ret * 100.0,
    }
    return bt, metrics


def design_cross_theme_portfolio(
    radar_df: pd.DataFrame,
    risk_tolerance: str = "標準",
    total_capital_jpy: float = 1_000_000.0,
) -> PortfolioArtifacts:
    theme_alloc = _theme_weights_from_radar(radar_df, risk_tolerance=risk_tolerance)
    if theme_alloc.empty:
        empty = pd.DataFrame()
        return PortfolioArtifacts(empty, empty, empty, empty, {})

    rep = pd.DataFrame(REPRESENTATIVE_UNIVERSE)
    ticker_alloc = theme_alloc.merge(rep, on="theme", how="left").dropna(subset=["ticker"]).copy()
    cnt = ticker_alloc.groupby("theme")["ticker"].transform("count").replace(0, 1)
    ticker_alloc["theme_amount_jpy"] = float(total_capital_jpy) * ticker_alloc["weight"]
    ticker_alloc["amount_jpy"] = ticker_alloc["theme_amount_jpy"] / cnt

    prices = _download_adj_close(ticker_alloc["ticker"].astype(str).tolist(), period="3y")
    latest_prices = prices.iloc[-1].to_dict() if not prices.empty else {}
    ticker_alloc["latest_price"] = ticker_alloc["ticker"].astype(str).str.upper().map(lambda t: float(latest_prices.get(t, np.nan)))
    ticker_alloc["shares_est"] = np.where(
        ticker_alloc["latest_price"] > 0,
        np.floor(ticker_alloc["amount_jpy"] / ticker_alloc["latest_price"]),
        np.nan,
    )

    theme_rets, _ = _build_theme_return_series(theme_alloc)
    corr = theme_rets.corr() if not theme_rets.empty else pd.DataFrame()
    bt_df, metrics = _backtest_from_theme_returns(theme_rets, theme_alloc)
    return PortfolioArtifacts(
        theme_alloc_df=theme_alloc,
        ticker_alloc_df=ticker_alloc.sort_values(["theme", "amount_jpy"], ascending=[True, False]).reset_index(drop=True),
        corr_df=corr,
        backtest_df=bt_df,
        metrics=metrics,
    )


def build_theme_allocation_pie(theme_alloc_df: pd.DataFrame) -> go.Figure:
    if theme_alloc_df is None or theme_alloc_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="テーマ別配分", height=340)
        return fig
    fig = px.pie(
        theme_alloc_df,
        names="theme",
        values="weight",
        color="radar_stage",
        template="plotly_dark",
        title="テーマ別の配分円グラフ",
        height=360,
    )
    fig.update_traces(textinfo="label+percent")
    return fig


def build_correlation_heatmap(corr_df: pd.DataFrame) -> go.Figure:
    if corr_df is None or corr_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="テーマ間相関マトリクス", height=340)
        return fig
    fig = px.imshow(
        corr_df,
        text_auto=".2f",
        color_continuous_scale="RdBu",
        zmin=-1,
        zmax=1,
        aspect="auto",
        title="テーマ間の相関マトリクス（分散効果確認）",
    )
    fig.update_layout(template="plotly_dark", height=360)
    return fig


def build_backtest_figure(backtest_df: pd.DataFrame) -> go.Figure:
    if backtest_df is None or backtest_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="バックテスト（過去3年）", height=340)
        return fig
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=backtest_df["date"], y=backtest_df["portfolio"], mode="lines", name="Portfolio"))
    fig.add_trace(go.Scatter(x=backtest_df["date"], y=backtest_df["benchmark"], mode="lines", name="SPY"))
    fig.update_layout(
        template="plotly_dark",
        title="バックテスト（過去3年）",
        xaxis_title="日付",
        yaxis_title="累積リターン倍率",
        height=360,
    )
    return fig

