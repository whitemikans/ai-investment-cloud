from __future__ import annotations

import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db.tech_research_utils import (
    get_hype_history,
    get_latest_tech_papers,
    get_patent_stats,
    init_tech_research_tables,
)
from scenarios.ai_agi import get_ai_agi_layers, get_ai_agi_market_scenarios, get_theme_stock_table
from tools.arxiv_collector import collect_arxiv_papers
from tools.hype_cycle_generator import generate_hype_cycle
from tools.paper_analyzer import analyze_papers_for_investment
from tools.patent_analyzer import build_patent_stats
from tools.s_curve_analyzer import analyze_s_curve
from tools.tech_radar import build_tech_radar
from utils.common import apply_global_ui_tweaks, render_footer


st.set_page_config(page_title="テクノロジーリサーチ", page_icon="🔬", layout="wide")
apply_global_ui_tweaks()
st.title("🔬 テクノロジーリサーチ")
st.caption("#10 先端技術リサーチ: 論文・ハイプサイクル・特許・投資シナリオを統合表示")

init_tech_research_tables()

with st.sidebar:
    st.subheader("更新")
    if st.button("最新データ収集", use_container_width=True, type="primary"):
        with st.spinner("arXiv収集と分析を実行中..."):
            raw = collect_arxiv_papers(max_results_per_theme=10, days_back=45)
            analyzed = analyze_papers_for_investment(raw)
            from db.tech_research_utils import save_tech_papers, replace_hype_history, replace_patent_stats

            save_tech_papers(analyzed)
            replace_hype_history(generate_hype_cycle())
            replace_patent_stats(build_patent_stats())
        st.success(f"更新完了: papers={len(analyzed)}")
        st.rerun()

themes = ["AI", "Quantum", "Biotech", "Space", "Energy", "Robotics"]
selected_themes = st.multiselect("技術領域フィルタ", options=themes, default=themes)

hype_df = get_hype_history()
if not hype_df.empty:
    hype_df = hype_df[hype_df["tech_theme"].isin(selected_themes)]

st.markdown("### 1. ハイプサイクルチャート")
if hype_df.empty:
    st.info("ハイプサイクルデータがありません。サイドバーの「最新データ収集」を実行してください。")
else:
    fig_hype = px.scatter(
        hype_df,
        x="as_of_date",
        y="hype_index",
        color="tech_theme",
        text="phase",
        template="plotly_dark",
        height=430,
    )
    fig_hype.update_traces(textposition="top center")
    st.plotly_chart(fig_hype, use_container_width=True)

st.markdown("### 2. テクノロジーレーダー")
radar_df = build_tech_radar()
if radar_df.empty:
    st.info("レーダーデータがありません。")
else:
    radar_df = radar_df[radar_df["tech_theme"].isin(selected_themes)]
    st.dataframe(radar_df, use_container_width=True, height=240)

st.markdown("### 3. 注目論文フィード")
paper_df = get_latest_tech_papers(limit=60)
if paper_df.empty:
    st.info("論文データがありません。")
else:
    paper_df = paper_df[paper_df["tech_theme"].isin(selected_themes)]
    paper_df["impact_score"] = pd.to_numeric(paper_df["impact_score"], errors="coerce").fillna(0.0)
    paper_df = paper_df.sort_values(["impact_score", "published_at"], ascending=[False, False]).head(15)
    for row in paper_df.itertuples(index=False):
        title = str(getattr(row, "title", ""))
        theme = str(getattr(row, "tech_theme", ""))
        score = float(getattr(row, "impact_score", 0.0))
        rec = str(getattr(row, "recommendation", ""))
        with st.expander(f"[{theme}] ⭐{score:.1f} {title} ({rec})", expanded=False):
            st.write(str(getattr(row, "summary", "")))
            pdf = str(getattr(row, "pdf_url", "") or "")
            if pdf:
                st.link_button("PDF", pdf)
            rel = str(getattr(row, "related_tickers", "") or "")
            if rel:
                st.caption(f"関連銘柄: {rel}")

st.markdown("### 4. 特許ランキング")
pat_df = get_patent_stats()
if pat_df.empty:
    st.info("特許データがありません。")
else:
    pat_df = pat_df[pat_df["tech_theme"].isin(selected_themes)]
    col1, col2 = st.columns(2)
    with col1:
        fig_pat = px.bar(
            pat_df.sort_values("patent_count", ascending=False),
            x="company",
            y="patent_count",
            color="tech_theme",
            template="plotly_dark",
            title="企業別 特許数",
            height=360,
        )
        st.plotly_chart(fig_pat, use_container_width=True)
    with col2:
        avg = pat_df.groupby("tech_theme", as_index=False)["innovation_score"].mean()
        fig_radar = go.Figure(
            data=go.Scatterpolar(
                r=avg["innovation_score"].tolist() + [avg["innovation_score"].tolist()[0]],
                theta=avg["tech_theme"].tolist() + [avg["tech_theme"].tolist()[0]],
                fill="toself",
                name="Innovation",
            )
        )
        fig_radar.update_layout(template="plotly_dark", height=360, title="テーマ別 イノベーションスコア")
        st.plotly_chart(fig_radar, use_container_width=True)

st.markdown("### 5. テーマ別銘柄一覧")
stock_df = get_theme_stock_table()
stock_df = stock_df[stock_df["theme"].isin(selected_themes)]
st.dataframe(stock_df, use_container_width=True, height=220)

st.markdown("### 6. Sカーブ分析")
s_points, s_summary = analyze_s_curve()
s_points = s_points[s_points["tech_theme"].isin(selected_themes)]
s_summary = s_summary[s_summary["tech_theme"].isin(selected_themes)]
if s_points.empty:
    st.info("Sカーブデータがありません。")
else:
    fig_s = px.line(
        s_points,
        x="year",
        y="adoption_pct",
        color="tech_theme",
        template="plotly_dark",
        title="技術普及曲線 (Sカーブ)",
        height=380,
    )
    st.plotly_chart(fig_s, use_container_width=True)
    st.dataframe(s_summary, use_container_width=True, height=200)

st.markdown("### 参考: AI・AGIシナリオ")
sc_df = get_ai_agi_market_scenarios()
ly_df = get_ai_agi_layers()
c1, c2 = st.columns(2)
with c1:
    st.dataframe(sc_df, use_container_width=True, height=180)
with c2:
    st.dataframe(ly_df, use_container_width=True, height=180)

render_footer()

