from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from db.db_utils import init_db
from db.models import engine
from db.news_utils import get_news_feed_df, get_sector_sentiment_heatmap_df, get_sentiment_trend_df, init_news_tables
from news_pipeline import process_news_pipeline
from utils.common import apply_global_ui_tweaks, render_footer, render_last_data_update, touch_last_data_update

SOURCES = ["Reuters", "Bloomberg", "WSJ", "SEC EDGAR"]


def _sentiment_bar(score: float) -> str:
    if score > 0.15:
        return "#22c55e"
    if score < -0.15:
        return "#ef4444"
    return "#9ca3af"


def _news_table_count() -> int | None:
    try:
        with engine.connect() as con:
            return int(con.execute(text("SELECT COUNT(*) FROM news_articles")).scalar_one())
    except Exception:
        return None


st.title("📰 ニュースフィード")
apply_global_ui_tweaks()
st.caption("ニュース収集・センチメント分析・重要度判定の結果を表示します。")

with st.spinner("DB初期化中..."):
    init_db()
    init_news_tables()
render_last_data_update()
backend = engine.url.get_backend_name().lower()
news_total = _news_table_count()
news_total_label = "-" if news_total is None else f"{news_total:,}"
st.caption(f"保存先DB: {backend} / news_articles: {news_total_label}")

col_run, col_space = st.columns([1, 3])
with col_run:
    if st.button("🔄 データ更新", use_container_width=True):
        with st.spinner("ニュース収集中..."):
            result = process_news_pipeline(max_articles_per_source=20)
        if bool(result.iloc[0].get("success", False)):
            touch_last_data_update()
            st.success(
                f"更新完了: {int(result.iloc[0].get('processed', 0))}件処理 / "
                f"アラート {int(result.iloc[0].get('alerts', 0))}件"
            )
            st.rerun()
        st.error(str(result.iloc[0].get("message", "更新に失敗しました。")))

st.sidebar.subheader("フィルター")
period = st.sidebar.selectbox("期間", ["今日", "直近3日", "直近1週間", "直近1ヶ月"], index=2)
sentiment = st.sidebar.selectbox("センチメント", ["すべて", "ポジティブのみ", "ネガティブのみ"], index=0)
min_importance = st.sidebar.slider("重要度", min_value=1, max_value=5, value=1)
source_selected = [s for s in SOURCES if st.sidebar.checkbox(s, value=True)]
portfolio_only = st.sidebar.toggle("保有銘柄関連のみ", value=False)

df = get_news_feed_df(
    period=period,
    sentiment=sentiment,
    min_importance=min_importance,
    sources=source_selected,
    portfolio_only=portfolio_only,
)

important_count = int((df["importance_score"] >= 4).sum()) if not df.empty else 0
st.subheader(f"📰 今日のニュース: {len(df):,}件収集 / 🚨 重要: {important_count:,}件")

if df.empty:
    st.info("条件に一致するニュースがありません。先に「データ更新」を実行してください。")
    render_footer()
    st.stop()

st.markdown("### センチメントトレンド")
trend_df = get_sentiment_trend_df(days=30)
if not trend_df.empty:
    trend = go.Figure()
    trend.add_trace(
        go.Scatter(
            x=pd.to_datetime(trend_df["d"]),
            y=trend_df["avg_sentiment"],
            mode="lines+markers",
            name="平均センチメント",
            line=dict(color="#38bdf8", width=2),
        )
    )
    trend.add_hrect(y0=0, y1=1, fillcolor="rgba(34,197,94,0.15)", line_width=0)
    trend.add_hrect(y0=-1, y1=0, fillcolor="rgba(239,68,68,0.15)", line_width=0)
    trend.update_layout(template="plotly_dark", height=320, xaxis_title="日付", yaxis_title="センチメント")
    st.plotly_chart(trend, use_container_width=True)

heat_df = get_sector_sentiment_heatmap_df(days=7)
if not heat_df.empty:
    pivot = heat_df.pivot(index="sector", columns="d", values="avg_sentiment").fillna(0.0)
    heat = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=[str(i) for i in pivot.index],
            zmid=0,
            colorscale=[[0.0, "#ef4444"], [0.5, "#f8fafc"], [1.0, "#22c55e"]],
            hovertemplate="セクター:%{y}<br>日付:%{x}<br>スコア:%{z:.2f}<extra></extra>",
        )
    )
    heat.update_layout(template="plotly_dark", height=320, title="セクター別センチメント（直近7日）")
    st.plotly_chart(heat, use_container_width=True)

st.markdown("### ニュース一覧")
for row in df.head(200).itertuples(index=False):
    color = _sentiment_bar(float(row.sentiment_score))
    stars = "⭐" * max(1, int(row.importance_score))
    with st.container(border=True):
        st.markdown(
            f"<div style='border-left:6px solid {color};padding-left:10px'>"
            f"<a href='{row.url}' target='_blank' style='font-weight:700'>{row.title}</a><br>"
            f"センチメント: {float(row.sentiment_score):+.2f} | 重要度: {stars} | 出典: {row.source}<br>"
            f"<small>{row.published_at}</small>"
            f"</div>",
            unsafe_allow_html=True,
        )
        st.write(str(row.summary_ja or "").strip())
        if str(row.related_stocks).strip():
            tags = " | ".join([t.strip() for t in str(row.related_stocks).split(",") if t.strip()])
            st.caption(f"🏷 関連銘柄: {tags}")

render_footer()
