from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from performance_tracker import load_performance_data, track_recommendation_performance
from utils.common import apply_global_ui_tweaks, render_footer


st.set_page_config(page_title="AI推奨精度追跡", page_icon="🎯", layout="wide")
apply_global_ui_tweaks()
st.title("🎯 AI推奨精度追跡")
st.caption("買い推奨の実績リターンを自動追跡し、精度の推移を可視化します。")

c1, c2 = st.columns([1, 3])
with c1:
    if st.button("最新データで再計算", use_container_width=True, type="primary"):
        with st.spinner("推奨精度を再計算中..."):
            result = track_recommendation_performance(lookback_days=540)
        st.success(f"再計算完了: {result}")
        st.rerun()
with c2:
    st.info("GitHub Actions の週次ジョブでも自動更新されます。")


df = load_performance_data(limit=5000)
if df.empty:
    st.warning("推奨精度データがありません。まず再計算を実行してください。")
    render_footer()
    st.stop()

df["recommendation_date"] = pd.to_datetime(df["recommendation_date"], errors="coerce")
df["return_1m"] = pd.to_numeric(df["return_1m"], errors="coerce")
df["return_3m"] = pd.to_numeric(df["return_3m"], errors="coerce")

buy_mask = df["ai_recommendation"].astype(str).str.lower().isin(["buy", "買い", "strong_buy", "long"])
buy_df = df[buy_mask].copy()
buy_df["buy_win_1m"] = pd.to_numeric(buy_df["buy_win_1m"], errors="coerce")

overall_win = float(buy_df["buy_win_1m"].dropna().mean() * 100.0) if not buy_df.empty else 0.0
k1, k2, k3 = st.columns(3)
k1.metric("全体勝率(1ヶ月)", f"{overall_win:.1f}%")
k2.metric("買い推奨件数", f"{len(buy_df):,}")
k3.metric("全推奨件数", f"{len(df):,}")

st.markdown("### エージェント別の精度")
agent_col = "source" if "source" in df.columns else None
if agent_col:
    agent_stats = (
        buy_df.groupby(agent_col, dropna=False)
        .agg(
            件数=("ticker", "count"),
            勝率=("buy_win_1m", "mean"),
            平均1ヶ月リターン=("return_1m", "mean"),
        )
        .reset_index()
        .rename(columns={agent_col: "区分"})
    )
    if not agent_stats.empty:
        agent_stats["勝率"] = agent_stats["勝率"] * 100.0
        st.dataframe(
            agent_stats.style.format({"勝率": "{:.1f}%", "平均1ヶ月リターン": "{:+.2f}%"}),
            use_container_width=True,
            height=220,
        )
    else:
        st.info("エージェント別集計対象データがありません。")
else:
    st.info("エージェント別の列がないため集計できません。")

st.markdown("### 推奨度別のリターン分布")
star_map = {
    "strong_buy": 5,
    "buy": 4,
    "hold": 3,
    "sell": 2,
    "strong_sell": 1,
    "買い": 4,
    "保持": 3,
    "売り": 2,
}
df["recommendation_score"] = df["ai_recommendation"].astype(str).str.lower().map(star_map).fillna(3)
fig_box = px.box(
    df,
    x="recommendation_score",
    y="return_1m",
    points="outliers",
    color="recommendation_score",
    template="plotly_dark",
    labels={"recommendation_score": "推奨度", "return_1m": "1ヶ月リターン(%)"},
)
fig_box.update_layout(height=420, showlegend=False)
st.plotly_chart(fig_box, use_container_width=True)

st.markdown("### 時系列での精度推移")
if not buy_df.empty:
    ts = buy_df.dropna(subset=["recommendation_date", "buy_win_1m"]).copy()
    if not ts.empty:
        ts["month"] = ts["recommendation_date"].dt.to_period("M").astype(str)
        monthly = (
            ts.groupby("month", dropna=False)
            .agg(勝率=("buy_win_1m", "mean"), 件数=("ticker", "count"), 平均1ヶ月リターン=("return_1m", "mean"))
            .reset_index()
        )
        monthly["勝率"] = monthly["勝率"] * 100.0
        fig_line = px.line(
            monthly,
            x="month",
            y="勝率",
            markers=True,
            template="plotly_dark",
            labels={"month": "月", "勝率": "買い推奨勝率(%)"},
        )
        fig_line.update_layout(height=360)
        st.plotly_chart(fig_line, use_container_width=True)
        st.dataframe(monthly.style.format({"勝率": "{:.1f}%", "平均1ヶ月リターン": "{:+.2f}%"}), use_container_width=True, height=220)
    else:
        st.info("時系列集計対象のデータがありません。")
else:
    st.info("買い推奨データがありません。")

render_footer()
