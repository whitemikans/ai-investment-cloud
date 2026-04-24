from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from sqlalchemy import create_engine, text

from ai_portfolio_advisor import generate_portfolio_diagnosis
from config import get_database_url
from db.db_utils import get_portfolio_base_df
from utils.common import apply_global_ui_tweaks, render_footer
from utils.portfolio_optimizer import (
    allocate_with_nisa_constraints,
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


def _signal_icon(value: float, low: float, high: float, reverse: bool = False) -> str:
    if reverse:
        if value > high:
            return "🟢"
        if value < low:
            return "🔴"
        return "🟡"
    if value < low:
        return "🟢"
    if value > high:
        return "🔴"
    return "🟡"


@st.cache_data(ttl=600)
def load_market_environment() -> dict:
    def trend_1m(ticker: str) -> float:
        df = yf.Ticker(ticker).history(period="1mo")
        if df.empty:
            return 0.0
        start = float(df["Close"].iloc[0])
        end = float(df["Close"].iloc[-1])
        return (end / start - 1.0) * 100.0 if start > 0 else 0.0

    def last_close(ticker: str, fallback: float) -> float:
        df = yf.Ticker(ticker).history(period="5d")
        if df.empty:
            return fallback
        return float(df["Close"].iloc[-1])

    spx_1m = trend_1m("^GSPC")
    vix = last_close("^VIX", 20.0)
    us10y = last_close("^TNX", 40.0) / 10.0
    us10y_1m = trend_1m("^TNX") / 10.0
    usdjpy = last_close("JPY=X", 150.0)

    return {
        "spx_1m": spx_1m,
        "vix": vix,
        "us10y": us10y,
        "us10y_1m": us10y_1m,
        "usdjpy": usdjpy,
        "signals": {
            "S&P500": {
                "icon": _signal_icon(spx_1m, -1.0, 1.0, reverse=True),
                "text": f"1か月トレンド {spx_1m:+.2f}%",
            },
            "VIX": {
                "icon": _signal_icon(vix, 16.0, 25.0),
                "text": f"{vix:.2f}",
            },
            "米10年債": {
                "icon": _signal_icon(us10y, 3.5, 4.8),
                "text": f"{us10y:.2f}%（1か月変化 {us10y_1m:+.2f}%）",
            },
            "USD/JPY": {
                "icon": _signal_icon(usdjpy, 140.0, 155.0),
                "text": f"{usdjpy:.2f}",
            },
        },
    }


@st.cache_data(ttl=600)
def load_news_environment(days: int = 30) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    engine = create_engine(get_database_url(), future=True)
    start_dt = datetime.now() - timedelta(days=days)

    try:
        with engine.connect() as con:
            sentiment_rows = pd.read_sql(
                text(
                    """
                    SELECT
                        a.published_at,
                        COALESCE(s.sector, 'その他') AS sector,
                        COALESCE(s.sentiment_score, 0) AS sentiment_score
                    FROM news_articles a
                    LEFT JOIN news_sentiments s ON s.article_id = a.id
                    WHERE a.published_at >= :start_dt
                    """
                ),
                con,
                params={"start_dt": start_dt},
            )
            keyword_rows = pd.read_sql(
                text(
                    """
                    SELECT COALESCE(hit_keywords, '') AS hit_keywords
                    FROM alerts
                    WHERE created_at >= :start_dt
                      AND COALESCE(hit_keywords, '') <> ''
                    """
                ),
                con,
                params={"start_dt": start_dt},
            )
    except Exception as exc:
        return (
            pd.DataFrame(columns=["sector", "avg_sentiment", "count"]),
            pd.DataFrame(columns=["keyword", "count"]),
            f"ニュースDB取得に失敗: {exc}",
        )

    if sentiment_rows.empty:
        sector_summary = pd.DataFrame(columns=["sector", "avg_sentiment", "count"])
    else:
        sentiment_rows["published_at"] = pd.to_datetime(sentiment_rows["published_at"], errors="coerce")
        sentiment_rows = sentiment_rows.dropna(subset=["published_at"])
        sector_summary = (
            sentiment_rows.groupby("sector", as_index=False)
            .agg(avg_sentiment=("sentiment_score", "mean"), count=("sentiment_score", "size"))
            .sort_values("avg_sentiment", ascending=False)
        )

    if keyword_rows.empty:
        keyword_summary = pd.DataFrame(columns=["keyword", "count"])
    else:
        exploded = (
            keyword_rows["hit_keywords"]
            .fillna("")
            .astype(str)
            .str.split(",")
            .explode()
            .str.strip()
        )
        exploded = exploded[exploded != ""]
        keyword_summary = (
            exploded.value_counts().rename_axis("keyword").reset_index(name="count").sort_values("count", ascending=False)
        )

    return sector_summary, keyword_summary, ""


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    clipped = {k: max(0.0, float(v)) for k, v in weights.items()}
    total = sum(clipped.values())
    if total <= 0:
        n = len(clipped)
        return {k: 1.0 / n for k in clipped} if n else {}
    return {k: v / total for k, v in clipped.items()}


def _portfolio_stats(weights: dict[str, float], tickers: list[str], mean_returns: pd.Series, cov_matrix: pd.DataFrame, rf: float) -> dict:
    w = np.array([weights.get(t, 0.0) for t in tickers], dtype=float)
    m = mean_returns.loc[tickers].values
    c = cov_matrix.loc[tickers, tickers].values
    ret = float(np.dot(w, m))
    risk = float(np.sqrt(np.dot(w.T, np.dot(c, w))))
    sharpe = (ret - rf) / risk if risk > 0 else 0.0
    return {"return": ret, "risk": risk, "sharpe": sharpe}


def build_market_adjusted_weights(base_weights: dict[str, float], market: dict, sector_sentiment: pd.DataFrame) -> tuple[dict[str, float], list[str]]:
    adjusted = dict(base_weights)
    notes: list[str] = []

    def shift(to_ticker: str, from_tickers: list[str], amount: float) -> None:
        if to_ticker not in adjusted or amount <= 0:
            return
        sources = [t for t in from_tickers if t in adjusted and t != to_ticker and adjusted[t] > 0]
        if not sources:
            return
        each = amount / len(sources)
        moved = 0.0
        for t in sources:
            take = min(adjusted[t], each)
            adjusted[t] -= take
            moved += take
        adjusted[to_ticker] += moved

    spx = float(market["spx_1m"])
    vix = float(market["vix"])
    us10y = float(market["us10y"])
    us10y_1m = float(market["us10y_1m"])
    usdjpy = float(market["usdjpy"])

    if vix >= 25:
        shift("GLD", ["QQQ", "VOO", "VNQ"], 0.03)
        shift("AGG", ["QQQ", "VOO"], 0.02)
        notes.append("VIX高水準のため、防御的にGLD/AGGを増やしました。")
    elif spx > 1 and vix < 18:
        shift("QQQ", ["AGG", "GLD"], 0.02)
        shift("VOO", ["AGG", "GLD"], 0.015)
        notes.append("株式モメンタムが強く、VOO/QQQをやや増やしました。")

    if us10y > 4.8 or us10y_1m > 0.08:
        shift("GLD", ["AGG", "VNQ"], 0.015)
        notes.append("米長期金利の上昇圧力を考慮し、金を厚めにしました。")

    if usdjpy > 155:
        shift("AGG", ["QQQ", "VOO"], 0.01)
        notes.append("円安進行を考慮し、値動き抑制のためAGG比率を増やしました。")

    if not sector_sentiment.empty:
        tech = sector_sentiment[sector_sentiment["sector"].str.contains("Technology", case=False, na=False)]
        reit = sector_sentiment[sector_sentiment["sector"].str.contains("Real Estate|REIT", case=False, na=False)]
        if not tech.empty and float(tech["avg_sentiment"].iloc[0]) < -0.1:
            shift("AGG", ["QQQ"], 0.01)
            notes.append("テックのセンチメント低下を受け、QQQ比率を抑制しました。")
        if not reit.empty and float(reit["avg_sentiment"].iloc[0]) > 0.1:
            shift("VNQ", ["AGG", "GLD"], 0.01)
            notes.append("不動産センチメント改善を受け、VNQを増やしました。")

    adjusted = _normalize_weights(adjusted)
    if not notes:
        notes.append("市場シグナルは中立のため、通常時の最適配分を維持しました。")
    return adjusted, notes


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
annual_budget = st.sidebar.number_input("年間投資予定額（円）", min_value=100000, value=1200000, step=100000)
risk_free_rate = st.sidebar.number_input("無リスク金利(%)", min_value=0.0, max_value=10.0, value=0.5, step=0.1) / 100.0

run_opt = st.sidebar.button("最適化を実行", use_container_width=True, type="primary")
run_ai = st.sidebar.button("AI診断を実行", use_container_width=True)

if not run_opt and "opt_payload" not in st.session_state:
    st.info("サイドバーの『最適化を実行』を押してください。")
    st.stop()

if run_opt:
    start = date.today() - timedelta(days=365 * 3)
    end = date.today()
    with st.spinner("価格データ取得中..."):
        price_df = fetch_price_history(tickers, start, end)
    if price_df.empty:
        st.error("価格データを取得できませんでした。銘柄コードを確認してください。")
        st.stop()

    _, mean_returns, cov_matrix = build_return_stats(price_df)
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
            current_weights = {t: float(subset.loc[subset["ticker"] == t, "cost"].sum() / total_cost) if total_cost > 0 else 0.0 for t in valid_tickers}
            current_weights = _normalize_weights(current_weights)

    st.session_state["opt_payload"] = {
        "tickers": valid_tickers,
        "mean_returns": mean_returns.to_dict(),
        "cov_matrix": cov_matrix.to_dict(),
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
mean_returns = pd.Series(payload["mean_returns"])
cov_matrix = pd.DataFrame(payload["cov_matrix"])
frontier = payload["frontier"]
random_df = payload["random_df"]
current_weights = payload["current_weights"]
min_var = payload["min_var"]
max_sharpe = payload["max_sharpe"]
risk_parity = payload["risk_parity"]
selected = payload["selected"]

market_env = load_market_environment()
sector_sentiment_df, keyword_freq_df, news_env_error = load_news_environment(days=30)
adjusted_weights, adjustment_notes = build_market_adjusted_weights(selected["weights"], market_env, sector_sentiment_df)
selected_stats = _portfolio_stats(selected["weights"], tickers, mean_returns, cov_matrix, payload["risk_free_rate"])
adjusted_stats = _portfolio_stats(adjusted_weights, tickers, mean_returns, cov_matrix, payload["risk_free_rate"])
nisa_result = allocate_with_nisa_constraints(
    base_weights=selected["weights"],
    mean_returns=mean_returns,
    annual_investment=float(annual_budget),
    years=10,
    tax_rate=0.20,
)

tabs = st.tabs(["📊 効率的フロンティア", "⚖️ 最適配分", "🔄 リバランス", "🤖 AI診断", "📈 ブラック・リッターマン"])

with tabs[0]:
    fig = go.Figure()
    if not random_df.empty:
        fig.add_trace(go.Scatter(x=random_df["risk"], y=random_df["return"], mode="markers", marker=dict(size=4, color="rgba(150,150,150,0.25)"), name="ランダムPF", hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=frontier["risk"], y=frontier["return"], mode="lines", line=dict(color="#22c55e", width=3), name="効率的フロンティア"))
    fig.add_trace(go.Scatter(x=[min_var.risk], y=[min_var.expected_return], mode="markers", marker=dict(size=14, color="#10b981", symbol="star"), name="最小分散"))
    fig.add_trace(go.Scatter(x=[max_sharpe.risk], y=[max_sharpe.expected_return], mode="markers", marker=dict(size=14, color="#ef4444", symbol="star"), name="最大シャープ"))
    fig.add_trace(go.Scatter(x=[selected["risk"]], y=[selected["return"]], mode="markers", marker=dict(size=14, color="#f59e0b", symbol="star"), name=f"推奨（許容度 {risk_tolerance}）"))
    fig.update_layout(template="plotly_dark", xaxis_title="リスク（年率）", yaxis_title="リターン（年率）", height=560)
    st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    compare_df = pd.DataFrame({
        "Ticker": tickers,
        "現在配分": [current_weights.get(t, 0.0) for t in tickers],
        "最小分散": [min_var.weights.get(t, 0.0) for t in tickers],
        "最大シャープ": [max_sharpe.weights.get(t, 0.0) for t in tickers],
        "通常最適": [selected["weights"].get(t, 0.0) for t in tickers],
        "市場調整後": [adjusted_weights.get(t, 0.0) for t in tickers],
    })
    melted = compare_df.melt(id_vars="Ticker", var_name="Portfolio", value_name="Weight")
    fig_bar = px.bar(melted, x="Ticker", y="Weight", color="Portfolio", barmode="group", template="plotly_dark")
    fig_bar.update_yaxes(tickformat=".1%")
    st.plotly_chart(fig_bar, use_container_width=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("通常最適 リターン", _to_percent(selected_stats["return"]))
    c2.metric("通常最適 リスク", _to_percent(selected_stats["risk"]))
    c3.metric("通常最適 シャープ", f"{selected_stats['sharpe']:.2f}")

    c4, c5, c6 = st.columns(3)
    c4.metric("市場調整後 リターン", _to_percent(adjusted_stats["return"]), delta=_to_percent(adjusted_stats["return"] - selected_stats["return"]))
    c5.metric("市場調整後 リスク", _to_percent(adjusted_stats["risk"]), delta=_to_percent(adjusted_stats["risk"] - selected_stats["risk"]))
    c6.metric("市場調整後 シャープ", f"{adjusted_stats['sharpe']:.2f}", delta=f"{adjusted_stats['sharpe'] - selected_stats['sharpe']:+.2f}")

    st.dataframe(compare_df.style.format({c: "{:.2%}" for c in compare_df.columns if c != "Ticker"}), use_container_width=True)

    st.markdown("#### 新NISA制約を反映した口座配分（年間投資予定額ベース）")
    st.caption("つみたて対象: VOO / 1655 / 1343、成長投資枠: 個別株+ETF（2510 / 1540は対象外）")
    cap1, cap2, cap3 = st.columns(3)
    cap1.metric("つみたて投資枠 使用額", f"¥{nisa_result.tsumitate_used:,.0f}")
    cap2.metric("成長投資枠 使用額", f"¥{nisa_result.growth_used:,.0f}")
    cap3.metric("特定口座 配分額", f"¥{nisa_result.taxable_used:,.0f}")

    ben1, ben2 = st.columns(2)
    ben1.metric("年間 非課税メリット概算", f"¥{nisa_result.annual_tax_benefit:,.0f}")
    ben2.metric("10年間 累計メリット概算", f"¥{nisa_result.ten_year_tax_benefit:,.0f}")
    st.caption("非課税メリットは『NISA配分額 x 期待リターン x 20%』の概算です。")

    nisa_view = nisa_result.allocation_df.rename(
        columns={
            "Ticker": "銘柄",
            "Expected Return": "期待リターン",
            "Planned Amount": "年間投資予定",
            "NISA Tsumitate": "NISA(つみたて)",
            "NISA Growth": "NISA(成長)",
            "Taxable": "特定口座",
        }
    )
    st.dataframe(
        nisa_view.style.format(
            {
                "期待リターン": "{:.2%}",
                "年間投資予定": "¥{:,.0f}",
                "NISA(つみたて)": "¥{:,.0f}",
                "NISA(成長)": "¥{:,.0f}",
                "特定口座": "¥{:,.0f}",
            }
        ),
        use_container_width=True,
        height=360,
    )

with tabs[2]:
    rows = []
    for t in tickers:
        cur = current_weights.get(t, 0.0)
        target = selected["weights"].get(t, 0.0)
        diff = cur - target
        action = "売却" if diff > 0 else "購入"
        amount_jpy = abs(diff) * annual_budget
        rows.append({
            "Ticker": t,
            "現在配分": cur,
            "目標配分": target,
            "乖離": diff,
            "閾値超過": abs(diff) >= rebalance_threshold,
            "提案アクション": action,
            "年間投資額ベース提案金額(円)": amount_jpy,
        })
    rebalance_df = pd.DataFrame(rows)
    st.dataframe(rebalance_df.style.format({"現在配分": "{:.2%}", "目標配分": "{:.2%}", "乖離": "{:+.2%}", "年間投資額ベース提案金額(円)": "¥{:,.0f}"}), use_container_width=True)

    if rebalance_df["閾値超過"].any():
        hit = rebalance_df[rebalance_df["閾値超過"]]
        st.warning(f"閾値 {rebalance_threshold:.1%} 超過の銘柄があります: {', '.join(hit['Ticker'])}")
    else:
        st.success("乖離は閾値内です。リバランス不要です。")

with tabs[3]:
    st.markdown("#### 市場環境サマリー（信号機）")
    for name, s in market_env["signals"].items():
        st.write(f"{s['icon']} {name}: {s['text']}")

    st.markdown("#### 直近30日 セクター別センチメント")
    if news_env_error:
        st.warning(news_env_error)
    if sector_sentiment_df.empty:
        st.info("セクター別センチメントデータがありません。")
    else:
        fig_sector = px.bar(sector_sentiment_df.head(12), x="sector", y="avg_sentiment", color="avg_sentiment", color_continuous_scale="RdYlGn", template="plotly_dark")
        st.plotly_chart(fig_sector, use_container_width=True)
        st.dataframe(sector_sentiment_df.head(20), use_container_width=True)

    st.markdown("#### 注目キーワード出現頻度（直近30日）")
    if keyword_freq_df.empty:
        st.info("キーワード頻度データがありません。")
    else:
        fig_kw = px.bar(keyword_freq_df.head(15), x="keyword", y="count", template="plotly_dark")
        st.plotly_chart(fig_kw, use_container_width=True)
        st.dataframe(keyword_freq_df.head(20), use_container_width=True)

    st.markdown("#### 今の市場ならこうすべき（配分調整提案）")
    st.dataframe(
        pd.DataFrame(
            {
                "Ticker": tickers,
                "通常最適": [selected["weights"].get(t, 0.0) for t in tickers],
                "市場調整後": [adjusted_weights.get(t, 0.0) for t in tickers],
                "差分": [adjusted_weights.get(t, 0.0) - selected["weights"].get(t, 0.0) for t in tickers],
            }
        ).style.format({"通常最適": "{:.2%}", "市場調整後": "{:.2%}", "差分": "{:+.2%}"}),
        use_container_width=True,
    )
    for n in adjustment_notes:
        st.write(f"- {n}")

    if run_ai:
        data_for_ai = {
            "tickers": tickers,
            "current_weights": current_weights,
            "recommended_weights": selected["weights"],
            "market_adjusted_weights": adjusted_weights,
            "normal_opt_stats": selected_stats,
            "market_adjusted_stats": adjusted_stats,
            "min_variance": {"return": min_var.expected_return, "risk": min_var.risk, "sharpe": min_var.sharpe},
            "max_sharpe": {"return": max_sharpe.expected_return, "risk": max_sharpe.risk, "sharpe": max_sharpe.sharpe},
            "risk_parity": ({"return": risk_parity.expected_return, "risk": risk_parity.risk, "sharpe": risk_parity.sharpe} if risk_parity else {}),
            "market_environment": market_env,
            "sector_sentiment_30d": sector_sentiment_df.head(15).to_dict(orient="records"),
            "keyword_frequency_30d": keyword_freq_df.head(20).to_dict(orient="records"),
            "adjustment_notes": adjustment_notes,
        }
        with st.spinner("AI診断を生成中..."):
            st.session_state["ai_report"] = generate_portfolio_diagnosis(data_for_ai)

    st.markdown("#### AI診断レポート")
    st.write(st.session_state.get("ai_report", "AI診断は未実行です。サイドバーの『AI診断を実行』を押してください。"))

with tabs[4]:
    st.markdown("ブラック・リッターマン（簡易版）")
    st.caption("均衡リターンに対して、見通し（-5%〜+5%）と確信度を加えて調整します。")

    views = {}
    confidences = {}
    for t in tickers:
        c1, c2 = st.columns([2, 1])
        views[t] = c1.slider(f"{t} 見通し(%)", min_value=-5.0, max_value=5.0, value=0.0, step=0.5, key=f"view_{t}") / 100.0
        confidences[t] = c2.slider(f"{t} 確信度", min_value=0.0, max_value=1.0, value=0.5, step=0.1, key=f"conf_{t}")

    adjusted_returns = mean_returns.copy()
    for t in tickers:
        adjusted_returns.loc[t] = mean_returns.loc[t] + (views[t] * confidences[t])

    adj_max = find_max_sharpe_portfolio(tickers, adjusted_returns, cov_matrix, risk_free_rate=payload["risk_free_rate"])
    if adj_max is None:
        st.warning("見通し反映後の最適化に失敗しました。")
    else:
        compare_ret = pd.DataFrame({"Ticker": tickers, "均衡リターン": [mean_returns.loc[t] for t in tickers], "調整後リターン": [adjusted_returns.loc[t] for t in tickers]})
        fig_ret = px.bar(compare_ret.melt(id_vars="Ticker", var_name="Type", value_name="Return"), x="Ticker", y="Return", color="Type", barmode="group", template="plotly_dark")
        fig_ret.update_yaxes(tickformat=".1%")
        st.plotly_chart(fig_ret, use_container_width=True)

        st.markdown("#### 見通し反映後の最適配分（最大シャープ）")
        st.dataframe(pd.DataFrame({"Ticker": tickers, "Weight": [adj_max.weights.get(t, 0.0) for t in tickers]}).style.format({"Weight": "{:.2%}"}), use_container_width=True)

render_footer()
