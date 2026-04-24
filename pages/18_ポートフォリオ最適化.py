from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from ai_portfolio_advisor import generate_portfolio_diagnosis
from db.db_utils import get_portfolio_base_df
from utils.common import apply_global_ui_tweaks, render_footer
from utils.portfolio_optimizer import (
    build_return_stats,
    fetch_price_history,
    find_max_sharpe_portfolio,
    find_min_variance_portfolio,
    find_risk_parity_portfolio,
    generate_efficient_frontier,
    generate_random_portfolios,
    interpolate_frontier_by_risk_tolerance,
)


ETF_PRESET = ["VOO", "QQQ", "AGG", "VNQ", "GLD"]
STOCK_PRESET = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]


def _to_percent(v: float) -> str:
    return f"{v * 100:.2f}%"


@st.cache_data(ttl=600)
def load_market_signals() -> dict[str, str]:
    def _trend(ticker: str) -> float:
        df = yf.Ticker(ticker).history(period="1mo")
        if df.empty:
            return 0.0
        start = float(df["Close"].iloc[0])
        end = float(df["Close"].iloc[-1])
        return (end / start - 1.0) * 100.0 if start > 0 else 0.0

    def _last_close(ticker: str, fallback: float = 0.0) -> float:
        df = yf.Ticker(ticker).history(period="5d")
        if df.empty:
            return fallback
        return float(df["Close"].iloc[-1])

    spx_1m = _trend("^GSPC")
    vix = _last_close("^VIX", 20.0)
    us10y = _last_close("^TNX", 4.0) / 10.0
    usdjpy = _last_close("JPY=X", 150.0)

    def _signal(value: float, low: float, high: float) -> str:
        if value < low:
            return "🟢"
        if value > high:
            return "🔴"
        return "🟡"

    return {
        "spx": f"{'🟢' if spx_1m >= 0 else '🔴'} S&P500 1か月: {spx_1m:+.2f}%",
        "vix": f"{_signal(vix, 16, 25)} VIX: {vix:.2f}",
        "us10y": f"{_signal(us10y, 3.5, 4.8)} 米10年債: {us10y:.2f}%",
        "usdjpy": f"{_signal(usdjpy, 140, 155)} USD/JPY: {usdjpy:.2f}",
    }


st.set_page_config(page_title="ポートフォリオ最適化", page_icon="📊", layout="wide")
st.title("📊 ポートフォリオ最適化ダッシュボード #07")
apply_global_ui_tweaks()
st.caption(f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

mode = st.sidebar.radio("モード", options=["ETFモード", "個別株モード"], index=0)
default_tickers = ETF_PRESET if mode == "ETFモード" else STOCK_PRESET
ticker_text = st.sidebar.text_input("銘柄/ETF（カンマ区切り）", value=",".join(default_tickers))
tickers = [t.strip().upper() for t in ticker_text.split(",") if t.strip()]

risk_tolerance = st.sidebar.slider("リスク許容度", min_value=1, max_value=10, value=6)
rebalance_threshold = st.sidebar.slider("リバランス閾値(%)", min_value=1.0, max_value=10.0, value=5.0, step=0.5) / 100.0
annual_budget = st.sidebar.number_input("年間投資額（円）", min_value=100000, value=1200000, step=100000)
risk_free_rate = st.sidebar.number_input("無リスク金利(%)", min_value=0.0, max_value=10.0, value=0.5, step=0.1) / 100.0

run_opt = st.sidebar.button("最適化を実行", use_container_width=True, type="primary")
run_ai = st.sidebar.button("AI診断を実行", use_container_width=True)

if not run_opt and "opt_payload" not in st.session_state:
    st.info("サイドバーの「最適化を実行」を押してください。")
    st.stop()

if run_opt:
    start = date.today() - timedelta(days=365 * 3)
    end = date.today()
    with st.spinner("価格データ取得中..."):
        price_df = fetch_price_history(tickers, start, end)
    if price_df.empty:
        st.error("価格データを取得できませんでした。銘柄コードを確認してください。")
        st.stop()

    returns_df, mean_returns, cov_matrix = build_return_stats(price_df)
    valid_tickers = list(price_df.columns)

    with st.spinner("最適化計算中..."):
        min_var = find_min_variance_portfolio(valid_tickers, mean_returns, cov_matrix, risk_free_rate=risk_free_rate)
        max_sharpe = find_max_sharpe_portfolio(valid_tickers, mean_returns, cov_matrix, risk_free_rate=risk_free_rate)
        risk_parity = find_risk_parity_portfolio(valid_tickers, mean_returns, cov_matrix, risk_free_rate=risk_free_rate)
        frontier = generate_efficient_frontier(valid_tickers, mean_returns, cov_matrix, points=50, risk_free_rate=risk_free_rate)
        random_df = generate_random_portfolios(valid_tickers, mean_returns, cov_matrix, n_samples=2500, risk_free_rate=risk_free_rate)

    if frontier.empty or min_var is None or max_sharpe is None:
        st.error("最適化計算に失敗しました。銘柄組み合わせを変更してください。")
        st.stop()

    selected = interpolate_frontier_by_risk_tolerance(frontier, risk_tolerance)
    if selected is None:
        st.error("リスク許容度に対応する配分を計算できませんでした。")
        st.stop()

    portfolio_base = get_portfolio_base_df()
    if portfolio_base.empty:
        current_weights = {t: 1.0 / len(valid_tickers) for t in valid_tickers}
    else:
        subset = portfolio_base[portfolio_base["ticker"].isin(valid_tickers)].copy()
        if subset.empty:
            current_weights = {t: 1.0 / len(valid_tickers) for t in valid_tickers}
        else:
            subset["cost"] = subset["avg_cost"] * subset["shares"]
            total_cost = float(subset["cost"].sum())
            if total_cost <= 0:
                current_weights = {t: 1.0 / len(valid_tickers) for t in valid_tickers}
            else:
                current_weights = {t: float(subset.loc[subset["ticker"] == t, "cost"].sum() / total_cost) for t in valid_tickers}

    st.session_state["opt_payload"] = {
        "tickers": valid_tickers,
        "mean_returns": mean_returns.to_dict(),
        "cov_matrix": cov_matrix.to_dict(),
        "price_df_tail": price_df.tail(5).to_dict(),
        "current_weights": current_weights,
        "min_var": min_var,
        "max_sharpe": max_sharpe,
        "risk_parity": risk_parity,
        "frontier": frontier,
        "random_df": random_df,
        "selected": selected,
        "risk_free_rate": risk_free_rate,
    }

payload = st.session_state["opt_payload"]
tickers = payload["tickers"]
frontier = payload["frontier"]
random_df = payload["random_df"]
current_weights = payload["current_weights"]
min_var = payload["min_var"]
max_sharpe = payload["max_sharpe"]
risk_parity = payload["risk_parity"]
selected = payload["selected"]

tabs = st.tabs(["📊 効率的フロンティア", "⚖️ 最適配分", "🔄 リバランス", "🤖 AI診断", "📈 ブラック・リッターマン"])

with tabs[0]:
    fig = go.Figure()
    if not random_df.empty:
        fig.add_trace(
            go.Scatter(
                x=random_df["risk"],
                y=random_df["return"],
                mode="markers",
                marker=dict(size=4, color="rgba(150,150,150,0.25)"),
                name="ランダムPF",
                hoverinfo="skip",
            )
        )
    fig.add_trace(
        go.Scatter(
            x=frontier["risk"],
            y=frontier["return"],
            mode="lines",
            line=dict(color="#22c55e", width=3),
            name="効率的フロンティア",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[min_var.risk],
            y=[min_var.expected_return],
            mode="markers",
            marker=dict(size=14, color="#10b981", symbol="star"),
            name="最小分散",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[max_sharpe.risk],
            y=[max_sharpe.expected_return],
            mode="markers",
            marker=dict(size=14, color="#ef4444", symbol="star"),
            name="最大シャープ",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[selected["risk"]],
            y=[selected["return"]],
            mode="markers",
            marker=dict(size=14, color="#f59e0b", symbol="star"),
            name=f"推奨（許容度 {risk_tolerance}）",
        )
    )
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="リスク（年率）",
        yaxis_title="リターン（年率）",
        height=560,
    )
    st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    compare_df = pd.DataFrame(
        {
            "Ticker": tickers,
            "現在配分": [current_weights.get(t, 0.0) for t in tickers],
            "最小分散": [min_var.weights.get(t, 0.0) for t in tickers],
            "最大シャープ": [max_sharpe.weights.get(t, 0.0) for t in tickers],
            "推奨配分": [selected["weights"].get(t, 0.0) for t in tickers],
        }
    )
    melted = compare_df.melt(id_vars="Ticker", var_name="Portfolio", value_name="Weight")
    fig_bar = px.bar(melted, x="Ticker", y="Weight", color="Portfolio", barmode="group", template="plotly_dark")
    fig_bar.update_yaxes(tickformat=".1%")
    st.plotly_chart(fig_bar, use_container_width=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("推奨リターン", _to_percent(selected["return"]))
    c2.metric("推奨リスク", _to_percent(selected["risk"]))
    c3.metric("推奨シャープ", f"{selected['sharpe']:.2f}")

    pie_cols = st.columns(3)
    pie_data = {
        "現在": compare_df[["Ticker", "現在配分"]].rename(columns={"現在配分": "weight"}),
        "最小分散": compare_df[["Ticker", "最小分散"]].rename(columns={"最小分散": "weight"}),
        "最大シャープ": compare_df[["Ticker", "最大シャープ"]].rename(columns={"最大シャープ": "weight"}),
    }
    for col, (name, data) in zip(pie_cols, pie_data.items()):
        fig_pie = px.pie(data, names="Ticker", values="weight", title=name, hole=0.35)
        fig_pie.update_layout(template="plotly_dark")
        col.plotly_chart(fig_pie, use_container_width=True)

    st.dataframe(compare_df.style.format({c: "{:.2%}" for c in compare_df.columns if c != "Ticker"}), use_container_width=True)

with tabs[2]:
    rows = []
    for t in tickers:
        cur = current_weights.get(t, 0.0)
        target = selected["weights"].get(t, 0.0)
        diff = cur - target
        action = "売却" if diff > 0 else "購入"
        amount_jpy = abs(diff) * annual_budget
        rows.append(
            {
                "Ticker": t,
                "現在配分": cur,
                "目標配分": target,
                "乖離": diff,
                "閾値超過": abs(diff) >= rebalance_threshold,
                "提案アクション": action,
                "年間投資額ベース提案金額(円)": amount_jpy,
            }
        )
    rebalance_df = pd.DataFrame(rows)
    st.dataframe(
        rebalance_df.style.format(
            {
                "現在配分": "{:.2%}",
                "目標配分": "{:.2%}",
                "乖離": "{:+.2%}",
                "年間投資額ベース提案金額(円)": "¥{:,.0f}",
            }
        ),
        use_container_width=True,
    )

    if rebalance_df["閾値超過"].any():
        hit = rebalance_df[rebalance_df["閾値超過"]]
        st.warning(f"閾値 {rebalance_threshold:.1%} 超過の銘柄があります: {', '.join(hit['Ticker'])}")
    else:
        st.success("乖離は閾値内です。リバランス不要です。")

    hist = st.session_state.get("rebalance_history_rows", [])
    if st.button("リバランスチェック履歴に保存"):
        hist.append(
            {
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "threshold": rebalance_threshold,
                "needs_rebalance": bool(rebalance_df["閾値超過"].any()),
                "details": ", ".join(
                    [
                        f"{r['Ticker']} {r['乖離']:+.2%}"
                        for _, r in rebalance_df.iterrows()
                        if abs(float(r["乖離"])) >= rebalance_threshold
                    ]
                ),
            }
        )
        st.session_state["rebalance_history_rows"] = hist
        st.success("履歴を保存しました。")
    if hist:
        st.dataframe(pd.DataFrame(hist), use_container_width=True)

with tabs[3]:
    signals = load_market_signals()
    st.markdown("#### 市場環境サマリー")
    st.write(signals["spx"])
    st.write(signals["vix"])
    st.write(signals["us10y"])
    st.write(signals["usdjpy"])

    if run_ai or "ai_report" not in st.session_state:
        data_for_ai = {
            "tickers": tickers,
            "current_weights": current_weights,
            "recommended_weights": selected["weights"],
            "min_variance": {
                "return": min_var.expected_return,
                "risk": min_var.risk,
                "sharpe": min_var.sharpe,
            },
            "max_sharpe": {
                "return": max_sharpe.expected_return,
                "risk": max_sharpe.risk,
                "sharpe": max_sharpe.sharpe,
            },
            "risk_parity": (
                {
                    "return": risk_parity.expected_return,
                    "risk": risk_parity.risk,
                    "sharpe": risk_parity.sharpe,
                }
                if risk_parity is not None
                else {}
            ),
            "market_signals": signals,
        }
        with st.spinner("AI診断を生成中..."):
            st.session_state["ai_report"] = generate_portfolio_diagnosis(data_for_ai)

    st.markdown("#### AI診断レポート")
    st.write(st.session_state.get("ai_report", "AI診断は未実行です。"))

with tabs[4]:
    st.markdown("ブラック・リッターマン（簡易版）")
    st.caption("均衡リターンに対して、見通し（-5%〜+5%）と確信度を加えて調整します。")

    mean_returns = pd.Series(payload["mean_returns"])
    views = {}
    confidences = {}
    for t in tickers:
        c1, c2 = st.columns([2, 1])
        views[t] = c1.slider(f"{t} 見通し(%)", min_value=-5.0, max_value=5.0, value=0.0, step=0.5, key=f"view_{t}") / 100.0
        confidences[t] = c2.slider(f"{t} 確信度", min_value=0.0, max_value=1.0, value=0.5, step=0.1, key=f"conf_{t}")

    adjusted_returns = mean_returns.copy()
    for t in tickers:
        adjusted_returns.loc[t] = mean_returns.loc[t] + (views[t] * confidences[t])

    cov = pd.DataFrame(payload["cov_matrix"])
    adj_max_sharpe = find_max_sharpe_portfolio(tickers, adjusted_returns, cov, risk_free_rate=payload["risk_free_rate"])
    if adj_max_sharpe is None:
        st.warning("見通し反映後の最適化に失敗しました。")
    else:
        compare_ret = pd.DataFrame(
            {
                "Ticker": tickers,
                "均衡リターン": [mean_returns.loc[t] for t in tickers],
                "調整後リターン": [adjusted_returns.loc[t] for t in tickers],
            }
        )
        fig_ret = px.bar(
            compare_ret.melt(id_vars="Ticker", var_name="Type", value_name="Return"),
            x="Ticker",
            y="Return",
            color="Type",
            barmode="group",
            template="plotly_dark",
        )
        fig_ret.update_yaxes(tickformat=".1%")
        st.plotly_chart(fig_ret, use_container_width=True)

        st.markdown("#### 見通し反映後の最適配分（最大シャープ）")
        st.dataframe(
            pd.DataFrame(
                {"Ticker": tickers, "Weight": [adj_max_sharpe.weights.get(t, 0.0) for t in tickers]}
            ).style.format({"Weight": "{:.2%}"}),
            use_container_width=True,
        )

render_footer()

