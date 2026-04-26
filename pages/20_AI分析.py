from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

from ai_financial_advisor import generate_financial_advice
from ai_portfolio_advisor import generate_portfolio_diagnosis
from db.db_utils import get_portfolio_df_with_price
from db.models import engine
from db.news_utils import init_news_tables
from utils.common import apply_global_ui_tweaks, render_footer


@st.cache_data(ttl=300)
def load_news_headline_df(limit: int = 5) -> pd.DataFrame:
    sql = """
    SELECT title, COALESCE(summary_ja, '') AS summary_ja, published_at
    FROM news_articles
    ORDER BY published_at DESC, id DESC
    LIMIT :limit_n
    """
    try:
        init_news_tables()
        return pd.read_sql(text(sql), con=engine, params={"limit_n": int(limit)})
    except Exception:
        return pd.DataFrame(columns=["title", "summary_ja", "published_at"])


st.set_page_config(page_title="AI分析", page_icon="🤖", layout="wide")
apply_global_ui_tweaks()
st.title("🤖 AI分析（#04 + #07 + #08 統合）")
st.caption("ニュース・最適化・ライフプランの主要データを統合してAIコメントを生成")

col1, col2, col3 = st.columns(3)
with col1:
    st.page_link("pages/11_ニュースフィード.py", label="ニュース詳細へ", icon="📰")
with col2:
    st.page_link("pages/18_ポートフォリオ最適化.py", label="最適化詳細へ", icon="⚖️")
with col3:
    st.page_link("pages/19_ライフプラン.py", label="ライフプラン詳細へ", icon="🔥")

news_df = load_news_headline_df(limit=5)
portfolio_df = get_portfolio_df_with_price()
opt_payload = st.session_state.get("opt_payload", {})
life_sim = st.session_state.get("life_sim_result", {})
life_events = st.session_state.get("life_events", [])

st.markdown("### 入力サマリー")
left, right = st.columns(2)
with left:
    st.markdown("#### ニュース（最新5件）")
    if news_df.empty:
        st.info("ニュースデータなし")
    else:
        st.dataframe(news_df, use_container_width=True, height=220)

with right:
    st.markdown("#### ポートフォリオ")
    if portfolio_df.empty:
        st.info("ポートフォリオデータなし")
    else:
        show = portfolio_df[["stock_code", "market_value", "unrealized_pl"]].copy()
        st.dataframe(show.style.format({"market_value": "¥{:,.0f}", "unrealized_pl": "¥{:,.0f}"}), use_container_width=True, height=220)

st.markdown("### AI統合コメント")
btn1, btn2 = st.columns(2)
with btn1:
    run_port = st.button("ポートフォリオAI診断を生成", use_container_width=True)
with btn2:
    run_life = st.button("ライフプランAI診断を生成", use_container_width=True)

if run_port:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tickers": list(portfolio_df["stock_code"]) if not portfolio_df.empty else [],
        "current_weights": {},
        "recommended_weights": (opt_payload.get("selected", {}) or {}).get("weights", {}),
        "market_adjusted_weights": {},
        "normal_opt_stats": {
            "return": (opt_payload.get("selected", {}) or {}).get("return", 0.0),
            "risk": (opt_payload.get("selected", {}) or {}).get("risk", 0.0),
            "sharpe": (opt_payload.get("selected", {}) or {}).get("sharpe", 0.0),
        },
        "market_adjusted_stats": {},
        "news_headlines": news_df.to_dict(orient="records"),
    }
    with st.spinner("AI診断を生成中..."):
        st.session_state["ai_merged_portfolio"] = generate_portfolio_diagnosis(payload)

if run_life:
    life_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "profile": {
            "current_age": st.session_state.get("現在の年齢", 35),
            "annual_income": st.session_state.get("年収（円）", 6_000_000),
            "annual_expense": st.session_state.get("年間支出（円）", 3_600_000),
            "current_assets": float(portfolio_df["market_value"].sum()) if not portfolio_df.empty else 0.0,
        },
        "monte_carlo": (life_sim.get("mc") or {}) if isinstance(life_sim, dict) else {},
        "life_events": life_events,
    }
    with st.spinner("AI診断を生成中..."):
        st.session_state["ai_merged_life"] = generate_financial_advice(life_payload)

st.markdown("#### ポートフォリオAIコメント")
st.write(st.session_state.get("ai_merged_portfolio", "未生成"))

st.markdown("#### ライフプランAIコメント")
st.write(st.session_state.get("ai_merged_life", "未生成"))

render_footer()
