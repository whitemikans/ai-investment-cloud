from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ai_analyzer import summarize_news
from db.db_utils import get_portfolio, init_db
from db.news_utils import get_news_feed_df, get_stock_master_tickers, get_stock_news_df, init_news_tables
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update


def _insight_from_news(ticker: str, df: pd.DataFrame) -> str:
    if df.empty:
        return f"{ticker} に関するニュースが不足しているため、現時点で有効なインサイトを生成できません。"
    avg_sent = float(df["sentiment_score"].mean())
    total = int(len(df))
    top = df.nlargest(3, "importance_score")
    headlines = " / ".join(top["title"].astype(str).tolist())
    mood = "ポジティブ" if avg_sent > 0.15 else "ネガティブ" if avg_sent < -0.15 else "中立"
    base = (
        f"直近{total}件のニュース分析結果。\n"
        f"総合センチメント: {mood}（平均 {avg_sent:+.2f}）。\n"
        f"重要ニュース: {headlines}\n"
        "短期では重要度4以上の記事の継続有無を確認し、イベント通過後の反動に注意してください。"
    )
    return summarize_news(f"{ticker} AIインサイト", base)


st.title("🏢 銘柄別ニュースビュー")
apply_global_ui_tweaks()
st.caption("選択した銘柄のニュース件数・センチメント推移・AIインサイトを表示します。")

with st.spinner("DB初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()

portfolio_df = get_portfolio()
portfolio_codes = portfolio_df["stock_code"].astype(str).tolist() if not portfolio_df.empty else []
all_codes = get_stock_master_tickers()

left, right = st.columns([2, 3])
with left:
    selected_portfolio = st.selectbox("保有銘柄から選択", options=[""] + portfolio_codes, index=0)
with right:
    free_text = st.text_input("すべての銘柄を検索（ティッカー）", value=selected_portfolio or "")

ticker = (free_text or selected_portfolio or "").strip().upper()
if not ticker:
    st.info("銘柄コードを選択または入力してください。")
    render_footer()
    st.stop()

st.sidebar.subheader("フィルター")
period_days = st.sidebar.selectbox("期間", [7, 14, 30, 60], index=2)
sentiment = st.sidebar.selectbox("センチメント", ["すべて", "ポジティブのみ", "ネガティブのみ"])
min_importance = st.sidebar.slider("重要度", 1, 5, 1)

df = get_stock_news_df(ticker=ticker, period_days=period_days, sentiment=sentiment, min_importance=min_importance)

st.subheader(f"{ticker} のニュースサマリー")
c1, c2, c3 = st.columns(3)
c1.metric("ニュース件数", f"{len(df):,}件")
c2.metric("平均センチメント", f"{(float(df['sentiment_score'].mean()) if not df.empty else 0.0):+.2f}")
c3.metric("重要ニュース(⭐4以上)", f"{(int((df['importance_score'] >= 4).sum()) if not df.empty else 0):,}件")

if not df.empty:
    daily = (
        df.assign(d=pd.to_datetime(df["published_at"]).dt.date)
        .groupby("d", as_index=False)["sentiment_score"]
        .mean()
        .sort_values("d")
    )
    mini = px.line(daily, x="d", y="sentiment_score", template="plotly_dark", title="センチメント推移")
    mini.update_layout(height=260, xaxis_title="日付", yaxis_title="平均センチメント")
    st.plotly_chart(mini, use_container_width=True)

    st.markdown("### 最新ニュース Top3")
    for row in df.head(3).itertuples(index=False):
        st.markdown(f"- [{row.title}]({row.url})  （スコア {float(row.sentiment_score):+.2f} / 重要度 {int(row.importance_score)}）")
else:
    st.info("この条件ではニュースがありません。")

st.markdown("### AIインサイト")
if st.button("🤖 AIインサイトを生成", use_container_width=True):
    with st.spinner("分析中..."):
        st.success(_insight_from_news(ticker, df))

st.markdown("### ニュース一覧")
if not df.empty:
    st.dataframe(
        df[["published_at", "source", "title", "sentiment_score", "importance_score", "related_stocks"]],
        use_container_width=True,
        column_config={
            "sentiment_score": st.column_config.NumberColumn("sentiment_score", format="%.2f"),
            "importance_score": st.column_config.NumberColumn("importance_score", format="%d"),
        },
    )

render_footer()

