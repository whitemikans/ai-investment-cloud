from __future__ import annotations

import hashlib
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from ai_analyzer import translate_to_japanese
from db.tech_research_utils import (
    get_hype_history,
    get_latest_tech_papers,
    get_patent_stats,
    get_patent_yearly,
    init_tech_research_tables,
)
from scenarios.ai_agi import (
    get_ai_agi_layers,
    get_ai_agi_market_scenarios,
    get_ai_investment_milestones,
    get_japan_ai_related_stocks,
    get_theme_stock_table,
)
from scenarios.quantum import (
    get_quantum_investment_universe,
    get_quantum_milestones,
    get_quantum_risks,
)
from scenarios.bio_healthcare import (
    evaluate_bio_pipeline,
    get_bio_subthemes,
)
from scenarios.nextgen_energy import (
    get_nextgen_energy_milestones,
    get_nextgen_energy_subthemes,
)
from tools.arxiv_collector import collect_arxiv_papers
from tools.hype_cycle_generator import build_hype_cycle_figure, generate_hype_cycle
from tools.paper_analyzer import analyze_papers_for_investment
from tools.patent_analyzer import (
    build_patent_bar_figure,
    build_patent_growth_figure,
    build_patent_stats,
    build_patent_yearly_stats,
    get_top_patent_companies,
)
from tools.patent_network import build_patent_citation_network_figure
from tools.paper_trends import (
    build_country_share_area_figure,
    build_keyword_cloud_figure,
    build_theme_trend_figure,
    detect_emerging_keywords,
    get_country_share_trends,
    get_paper_trends,
)
from tools.s_curve_analyzer import analyze_s_curve
from tools.tech_radar import build_tech_radar, build_tech_radar_figure
from tools.innovation_scorer import (
    build_innovation_radar_figure,
    build_innovation_ranking_figure,
    compute_innovation_score,
)
from tools.cross_theme_portfolio import (
    build_backtest_figure,
    build_correlation_heatmap,
    build_theme_allocation_pie,
    design_cross_theme_portfolio,
)
from utils.common import apply_global_ui_tweaks, render_footer


st.set_page_config(page_title="テクノロジーリサーチ", page_icon="🔬", layout="wide")
apply_global_ui_tweaks()
st.title("🔬 テクノロジーリサーチ")
st.caption("#10 先端技術リサーチ: 論文・ハイプサイクル・特許・投資シナリオを統合表示")

init_tech_research_tables()


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def _translate_text_ja(text: str) -> str:
    src = str(text or "").strip()
    if not src:
        return ""
    return str(translate_to_japanese(src, timeout_sec=8) or src)

with st.sidebar:
    st.subheader("??")
    if st.button("???????", use_container_width=True, type="primary"):
        with st.spinner("??????..."):
            from db.tech_research_utils import (
                save_tech_papers,
                replace_hype_history,
                replace_patent_stats,
                replace_patent_yearly,
            )

            analyzed = pd.DataFrame()
            papers_saved = 0
            hype_saved = 0
            patent_saved = 0

            # Collect papers (best-effort)
            try:
                raw = collect_arxiv_papers(max_results_per_theme=10, days_back=45)
                analyzed = analyze_papers_for_investment(raw)
                papers_saved = int(save_tech_papers(analyzed))
            except Exception as e:
                st.warning(f"????????????: {e}")

            # Build hype cycle (best-effort)
            try:
                hype_saved = int(replace_hype_history(generate_hype_cycle()))
            except Exception as e:
                st.warning(f"??????????????: {e}")

            # Build patent data (best-effort)
            try:
                patent_stats_df = build_patent_stats()
                patent_yearly_df = build_patent_yearly_stats(start_year=2018)
                patent_saved = int(replace_patent_stats(patent_stats_df.drop(columns=["source"], errors="ignore")))
                replace_patent_yearly(patent_yearly_df)
            except Exception as e:
                st.warning(f"????????????: {e}")

        st.success(f"????: papers={papers_saved}, hype={hype_saved}, patents={patent_saved}")
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
    fig_hype = build_hype_cycle_figure(hype_df)
    st.plotly_chart(fig_hype, use_container_width=True)

st.markdown("### 2. テクノロジーレーダー")
radar_df = build_tech_radar()
if radar_df.empty:
    st.info("??????????????")
else:
    radar_df = radar_df[radar_df["tech_theme"].isin(selected_themes)]
    st.plotly_chart(build_tech_radar_figure(radar_df), use_container_width=True)
    st.dataframe(
        radar_df.rename(
            columns={
                "tech_theme": "??",
                "phase": "???????",
                "hype_index": "?????",
                "stage": "S???????",
                "paper_impact": "???????",
                "radar_score": "?????",
                "radar_stage": "??",
                "investment_signal": "??",
            }
        ),
        use_container_width=True,
        height=260,
    )
st.markdown("### 3. 注目論文フィード")
paper_df = get_latest_tech_papers(limit=60)
if paper_df.empty:
    st.info("論文データがありません。")
else:
    paper_df = paper_df[paper_df["tech_theme"].isin(selected_themes)]
    paper_df["impact_score"] = pd.to_numeric(paper_df["impact_score"], errors="coerce").fillna(0.0)
    paper_df = paper_df.sort_values(["impact_score", "published_at"], ascending=[False, False]).head(15)

    if "paper_translations" not in st.session_state:
        st.session_state["paper_translations"] = {}

    for row in paper_df.itertuples(index=False):
        title = str(getattr(row, "title", ""))
        theme = str(getattr(row, "tech_theme", ""))
        score = float(getattr(row, "impact_score", 0.0))
        rec = str(getattr(row, "recommendation", ""))
        summary = str(getattr(row, "summary", "") or "")
        row_sig = f"{theme}|{title}|{getattr(row, 'published_at', '')}"
        row_key = hashlib.md5(row_sig.encode("utf-8")).hexdigest()[:12]

        with st.expander(f"[{theme}] score={score:.1f} {title} ({rec})", expanded=False):
            st.write(summary)

            if st.button("この論文を日本語翻訳", key=f"translate_paper_{row_key}", use_container_width=True):
                with st.spinner("翻訳中..."):
                    st.session_state["paper_translations"][row_key] = {
                        "title_ja": _translate_text_ja(title),
                        "summary_ja": _translate_text_ja(summary),
                    }

            translated = st.session_state["paper_translations"].get(row_key)
            if translated:
                st.markdown("**日本語翻訳**")
                title_ja = str(translated.get("title_ja", "") or "").strip()
                summary_ja = str(translated.get("summary_ja", "") or "").strip()
                if title_ja:
                    st.write(f"タイトル: {title_ja}")
                if summary_ja:
                    st.write(summary_ja)

            pdf = str(getattr(row, "pdf_url", "") or "")
            if pdf:
                st.link_button("PDF", pdf)
            rel = str(getattr(row, "related_tickers", "") or "")
            if rel:
                st.caption(f"関連銘柄: {rel}")
st.markdown("### 4. 特許ランキング")
pat_df = get_patent_stats()
pat_yearly_df = get_patent_yearly()
# Fallback seed: if patent table is empty, insert built-in sample stats.
if pat_df.empty:
    try:
        from db.tech_research_utils import replace_patent_stats, replace_patent_yearly

        seeded_stats = build_patent_stats(use_live=False)
        seeded_yearly = build_patent_yearly_stats(start_year=2018, use_live=False)
        replace_patent_stats(seeded_stats.drop(columns=["source"], errors="ignore"))
        replace_patent_yearly(seeded_yearly)
        pat_df = get_patent_stats()
        pat_yearly_df = get_patent_yearly()
    except Exception:
        pass
if pat_df.empty:
    st.info("????????????")
else:
    pat_df = pat_df[pat_df["tech_theme"].isin(selected_themes)]
    if not pat_yearly_df.empty:
        pat_yearly_df = pat_yearly_df[pat_yearly_df["tech_theme"].isin(selected_themes)]

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(build_patent_bar_figure(pat_df), use_container_width=True)
    with col2:
        top5_df = get_top_patent_companies(pat_df, top_n=5)
        st.dataframe(
            top5_df.rename(
                columns={
                    "tech_theme": "????",
                    "company": "??",
                    "patent_count": "???",
                    "innovation_score": "???",
                }
            ),
            use_container_width=True,
            height=360,
        )

    if pat_yearly_df.empty:
        st.info("??????????????")
    else:
        st.plotly_chart(build_patent_growth_figure(pat_yearly_df), use_container_width=True)


st.markdown("### 4.5 ??????????")
if pat_df.empty:
    st.info("??????????????????????")
else:
    fig_net, cent_tbl, cl_tbl = build_patent_citation_network_figure(pat_df)
    st.plotly_chart(fig_net, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        if cent_tbl.empty:
            st.info("?????????????")
        else:
            st.dataframe(
                cent_tbl.rename(
                    columns={
                        "company": "??",
                        "country": "??",
                        "patent_count": "???",
                        "degree_centrality": "???(Degree)",
                        "betweenness_centrality": "?????",
                        "eigenvector_centrality": "?????????",
                        "cluster_id": "?????",
                    }
                ),
                use_container_width=True,
                height=260,
            )
    with c2:
        if cl_tbl.empty:
            st.info("?????????????????")
        else:
            st.dataframe(
                cl_tbl.rename(columns={"company": "??", "cluster_id": "?????", "country": "??"}),
                use_container_width=True,
                height=260,
            )

st.markdown("### 5. テーマ別銘柄一覧")
stock_df = get_theme_stock_table()
stock_df = stock_df[stock_df["theme"].isin(selected_themes)]
st.dataframe(stock_df, use_container_width=True, height=220)

st.markdown("### 6. S?????")
s_points, s_summary = analyze_s_curve()
if s_points.empty:
    st.info("S?????????????")
else:
    fig_s = px.line(
        s_points,
        x="year",
        y="adoption_pct",
        color="tech_theme",
        template="plotly_dark",
        title="?????? (S???)",
        height=400,
    )
    current_pts = s_points[s_points["is_current"] == 1]
    if not current_pts.empty:
        fig_s.add_trace(
            go.Scatter(
                x=current_pts["year"],
                y=current_pts["adoption_pct"],
                mode="markers",
                marker={"symbol": "star", "size": 13, "color": "#f59e0b"},
                name="???",
                hovertemplate="???=%{text}<br>?=%{x}<br>???=%{y:.1f}%<extra></extra>",
                text=current_pts["tech_theme"],
            )
        )
    st.plotly_chart(fig_s, use_container_width=True)
    st.dataframe(
        s_summary.rename(
            columns={
                "tech_theme": "??",
                "L_max": "?????L",
                "k_growth": "????k",
                "t0_inflection": "???t0",
                "current_adoption_pct": "???????(%)",
                "estimated_50pct_year": "50%?????",
                "stage": "??????",
            }
        ),
        use_container_width=True,
        height=220,
    )

st.markdown("### ??: AI?AGI????")
st.markdown("### ??: AI?AGI????")
sc_df = get_ai_agi_market_scenarios()
ly_df = get_ai_agi_layers()
jp_df = get_japan_ai_related_stocks()
ms_df = get_ai_investment_milestones()

c1, c2 = st.columns(2)
with c1:
    st.dataframe(
        sc_df.rename(
            columns={
                "scenario": "????",
                "agi_timing": "AGI????",
                "market_usd_t": "????(?USD)",
                "agi_year": "AGI?",
                "note": "??",
            }
        ),
        use_container_width=True,
        height=200,
    )
with c2:
    st.dataframe(
        ly_df.rename(
            columns={
                "layer": "????",
                "focus": "????",
                "companies": "????",
                "risk": "???",
                "strategy": "??",
            }
        ),
        use_container_width=True,
        height=200,
    )

c3, c4 = st.columns(2)
with c3:
    st.markdown("**???AI????**")
    st.dataframe(jp_df, use_container_width=True, height=220)
with c4:
    st.markdown("**?????????**")
    st.dataframe(ms_df, use_container_width=True, height=220)


st.markdown("### 6.5 ??????????????")
qm_df = get_quantum_milestones()
qi_df = get_quantum_investment_universe()
qr_df = get_quantum_risks()

q1, q2 = st.columns(2)
with q1:
    st.markdown("**??????????????**")
    st.dataframe(
        qm_df.rename(
            columns={
                "milestone": "???????",
                "expected_window": "????",
                "trigger": "??????",
                "investment_judgement": "????",
            }
        ),
        use_container_width=True,
        height=220,
    )
with q2:
    st.markdown("**???????????/??/ETF?**")
    st.dataframe(
        qi_df.rename(
            columns={
                "region": "??",
                "asset_type": "??",
                "name": "??",
                "ticker": "?????",
                "theme": "?????",
            }
        ),
        use_container_width=True,
        height=220,
    )

st.markdown("**?????**")
st.dataframe(
    qr_df.rename(
        columns={
            "risk": "???",
            "description": "??",
            "monitoring_point": "????????",
        }
    ),
    use_container_width=True,
    height=190,
)


st.markdown("### 6.7 ??????????????")
bio_df = get_bio_subthemes()
pipe_df = evaluate_bio_pipeline()

b1, b2 = st.columns(2)
with b1:
    st.markdown("**3???????**")
    st.dataframe(
        bio_df.rename(
            columns={
                "subtheme": "?????",
                "thesis": "????",
                "companies": "????",
            }
        ),
        use_container_width=True,
        height=220,
    )
with b2:
    st.markdown("**?????????Phase3???**")
    st.dataframe(
        pipe_df.rename(
            columns={
                "company": "??",
                "subtheme": "?????",
                "phase1_count": "Phase1",
                "phase2_count": "Phase2",
                "phase3_count": "Phase3",
                "pipeline_score": "??????",
                "commercialization_view": "??????",
            }
        ),
        use_container_width=True,
        height=220,
    )

st.caption("????: ????3????????????????????????????????????????")


st.markdown("### 6.8 ??????????????")
ng_df = get_nextgen_energy_subthemes()
ng_ms_df = get_nextgen_energy_milestones()

n1, n2 = st.columns(2)
with n1:
    st.markdown("**3???????**")
    st.dataframe(
        ng_df.rename(
            columns={
                "subtheme": "?????",
                "thesis": "????",
                "companies": "????",
                "judgement": "????",
                "reason": "????",
                "window": "????",
            }
        ),
        use_container_width=True,
        height=240,
    )
with n2:
    st.markdown("**?????????**")
    st.dataframe(
        ng_ms_df.rename(
            columns={
                "subtheme": "?????",
                "milestone": "???????",
                "trigger": "??????",
            }
        ),
        use_container_width=True,
        height=240,
    )

st.markdown("### 7. ????????")
trend_df = get_paper_trends(months=36)
kw_df = detect_emerging_keywords(months_back=6, baseline_months=30, top_n=40)
country_df = get_country_share_trends(months=36)

col_t1, col_t2 = st.columns([1.4, 1.0])
with col_t1:
    st.plotly_chart(build_theme_trend_figure(trend_df), use_container_width=True)
with col_t2:
    st.plotly_chart(build_keyword_cloud_figure(kw_df), use_container_width=True)

st.plotly_chart(build_country_share_area_figure(country_df), use_container_width=True)
if not kw_df.empty:
    st.dataframe(
        kw_df.head(20).rename(
            columns={
                "keyword": "?????",
                "recent_count": "??6??",
                "baseline_count": "????",
                "score": "??????",
            }
        ),
        use_container_width=True,
        height=260,
    )

st.markdown("### 8. ?????????????")
inv = compute_innovation_score()
inv_df = inv.ranking_df
if inv_df.empty:
    st.info("?????????????????????")
else:
    inv_df = inv_df[inv_df["tech_theme"].isin(selected_themes)] if "selected_themes" in locals() else inv_df
    theme_opts = sorted(inv_df["tech_theme"].dropna().astype(str).unique().tolist())
    selected_inv_theme = st.selectbox("?????????", options=theme_opts, index=0) if theme_opts else None

    st.plotly_chart(build_innovation_ranking_figure(inv_df), use_container_width=True)
    if selected_inv_theme:
        st.plotly_chart(build_innovation_radar_figure(inv_df, selected_inv_theme, top_n=5), use_container_width=True)

    st.dataframe(
        inv_df.rename(
            columns={
                "tech_theme": "????",
                "company": "??",
                "innovation_score": "Innovation Score",
                "rd_ratio_pct": "R&D??(%)",
                "patent_count": "???",
                "paper_count": "???",
                "patent_growth_pct": "?????(%)",
                "pe_ratio": "PER",
                "undervalued": "??????",
            }
        ),
        use_container_width=True,
        height=280,
    )

    underv = inv.undervalued_df
    if not underv.empty:
        underv = underv[underv["tech_theme"].isin(selected_themes)] if "selected_themes" in locals() else underv
    st.markdown("**?????????????????? + ???**")
    if underv.empty:
        st.caption("????")
    else:
        st.dataframe(
            underv[["tech_theme", "company", "innovation_score", "pe_ratio"]].rename(
                columns={
                    "tech_theme": "????",
                    "company": "??",
                    "innovation_score": "Innovation Score",
                    "pe_ratio": "PER",
                }
            ),
            use_container_width=True,
            height=180,
        )

st.markdown("### 9. 6?????????????????")
if radar_df.empty:
    st.info("??????????????????????????????")
else:
    p1, p2 = st.columns([1, 1])
    with p1:
        risk_tolerance = st.selectbox("??????", options=["???", "??", "???"], index=1)
    with p2:
        total_capital = st.number_input("???????", min_value=100000, max_value=1000000000, value=1000000, step=100000)

    art = design_cross_theme_portfolio(
        radar_df=radar_df,
        risk_tolerance=risk_tolerance,
        total_capital_jpy=float(total_capital),
    )

    if art.theme_alloc_df.empty:
        st.info("????????????????????")
    else:
        c1, c2 = st.columns([1.1, 1.2])
        with c1:
            st.plotly_chart(build_theme_allocation_pie(art.theme_alloc_df), use_container_width=True)
        with c2:
            st.dataframe(
                art.ticker_alloc_df.rename(
                    columns={
                        "theme": "???",
                        "radar_stage": "??",
                        "ticker": "?????",
                        "name": "??",
                        "weight": "?????",
                        "theme_amount_jpy": "?????(?)",
                        "amount_jpy": "????(?)",
                        "latest_price": "????",
                        "shares_est": "????",
                    }
                ),
                use_container_width=True,
                height=300,
            )

        c3, c4 = st.columns(2)
        with c3:
            st.plotly_chart(build_correlation_heatmap(art.corr_df), use_container_width=True)
        with c4:
            st.plotly_chart(build_backtest_figure(art.backtest_df), use_container_width=True)

        if art.metrics:
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("?????", f"{art.metrics.get('total_return_pct', 0.0):.1f}%")
                st.metric("CAGR", f"{art.metrics.get('cagr_pct', 0.0):.1f}%")
            with m2:
                st.metric("???????", f"{art.metrics.get('volatility_pct', 0.0):.1f}%")
                st.metric("Sharpe", f"{art.metrics.get('sharpe', 0.0):.2f}")
            with m3:
                st.metric("??DD", f"{art.metrics.get('max_drawdown_pct', 0.0):.1f}%")
                st.metric("SPY??", f"{art.metrics.get('benchmark_return_pct', 0.0):.1f}%")


render_footer()
