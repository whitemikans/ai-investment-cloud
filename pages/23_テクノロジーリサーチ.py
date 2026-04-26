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
    st.subheader("データ更新")
    if st.button("最新データを収集", use_container_width=True, type="primary"):
        with st.spinner("データ収集中..."):
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
                st.warning(f"論文データの収集に失敗しました: {e}")

            # Build hype cycle (best-effort)
            try:
                hype_saved = int(replace_hype_history(generate_hype_cycle()))
            except Exception as e:
                st.warning(f"ハイプサイクル生成に失敗しました: {e}")

            # Build patent data (best-effort)
            try:
                patent_stats_df = build_patent_stats()
                patent_yearly_df = build_patent_yearly_stats(start_year=2018)
                patent_saved = int(replace_patent_stats(patent_stats_df.drop(columns=["source"], errors="ignore")))
                replace_patent_yearly(patent_yearly_df)
            except Exception as e:
                st.warning(f"特許データの生成に失敗しました: {e}")

        st.success(f"更新完了: papers={papers_saved}, hype={hype_saved}, patents={patent_saved}")
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
    st.info("データがありません。最新データを収集してください。")
else:
    radar_df = radar_df[radar_df["tech_theme"].isin(selected_themes)]
    st.plotly_chart(build_tech_radar_figure(radar_df), use_container_width=True)
    radar_view = radar_df[
        [
            "tech_theme",
            "phase",
            "hype_index",
            "stage",
            "paper_impact",
            "radar_score",
            "radar_stage",
            "investment_signal",
        ]
    ].rename(
        columns={
            "tech_theme": "技術テーマ",
            "phase": "ハイプサイクル",
            "hype_index": "ハイプ指数",
            "stage": "Sカーブ段階",
            "paper_impact": "論文インパクト",
            "radar_score": "レーダースコア",
            "radar_stage": "投資分類",
            "investment_signal": "シグナル",
        }
    )
    st.dataframe(
        radar_view,
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
    st.info("特許データがありません。")
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
            top5_df[["tech_theme", "company", "patent_count", "innovation_score"]].rename(
                columns={
                    "tech_theme": "技術テーマ",
                    "company": "企業",
                    "patent_count": "特許数",
                    "innovation_score": "イノベーションスコア",
                }
            ),
            use_container_width=True,
            height=360,
        )

    if pat_yearly_df.empty:
        st.info("特許の年次推移データがありません。")
    else:
        st.plotly_chart(build_patent_growth_figure(pat_yearly_df), use_container_width=True)


st.markdown("### 4.5 特許引用ネットワーク")
if pat_df.empty:
    st.info("特許データがないため、引用ネットワークを表示できません。")
else:
    fig_net, cent_tbl, cl_tbl = build_patent_citation_network_figure(pat_df)
    st.plotly_chart(fig_net, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        if cent_tbl.empty:
            st.info("中心性データがありません。")
        else:
            st.dataframe(
                cent_tbl[
                    [
                        "company",
                        "country",
                        "patent_count",
                        "degree_centrality",
                        "betweenness_centrality",
                        "eigenvector_centrality",
                        "cluster_id",
                    ]
                ].rename(
                    columns={
                        "company": "企業",
                        "country": "国",
                        "patent_count": "特許数",
                        "degree_centrality": "次数中心性",
                        "betweenness_centrality": "媒介中心性",
                        "eigenvector_centrality": "固有ベクトル中心性",
                        "cluster_id": "クラスタ",
                    }
                ),
                use_container_width=True,
                height=260,
            )
    with c2:
        if cl_tbl.empty:
            st.info("クラスターデータがありません。")
        else:
            st.dataframe(
                cl_tbl[["company", "cluster_id", "country"]].rename(
                    columns={"company": "企業", "cluster_id": "クラスタ", "country": "国"}
                ),
                use_container_width=True,
                height=260,
            )

st.markdown("### 5. テーマ別銘柄一覧")
stock_df = get_theme_stock_table()
stock_df = stock_df[stock_df["theme"].isin(selected_themes)]
st.dataframe(stock_df, use_container_width=True, height=220)

st.markdown("### 6. Sカーブ分析")
s_points, s_summary = analyze_s_curve()
if s_points.empty:
    st.info("Sカーブデータがありません。")
else:
    fig_s = px.line(
        s_points,
        x="year",
        y="adoption_pct",
        color="tech_theme",
        template="plotly_dark",
        title="技術普及曲線（Sカーブ）",
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
                name="現在地",
                hovertemplate="技術=%{text}<br>年=%{x}<br>普及率=%{y:.1f}%<extra></extra>",
                text=current_pts["tech_theme"],
            )
        )
    st.plotly_chart(fig_s, use_container_width=True)
    st.dataframe(
        s_summary.rename(
            columns={
                "tech_theme": "技術テーマ",
                "L_max": "最大普及率L",
                "k_growth": "成長速度k",
                "t0_inflection": "変曲点t0",
                "current_adoption_pct": "現在普及率(%)",
                "estimated_50pct_year": "50%到達年",
                "stage": "投資ステージ",
            }
        ),
        use_container_width=True,
        height=220,
    )

st.markdown("### 6.1 AI/AGI投資シナリオ")
sc_df = get_ai_agi_market_scenarios()
ly_df = get_ai_agi_layers()
jp_df = get_japan_ai_related_stocks()
ms_df = get_ai_investment_milestones()

c1, c2 = st.columns(2)
with c1:
    st.dataframe(
        sc_df.rename(
            columns={
                "scenario": "シナリオ",
                "agi_timing": "AGI時期",
                "market_usd_t": "市場規模(兆USD)",
                "agi_year": "AGI年",
                "note": "メモ",
            }
        ),
        use_container_width=True,
        height=200,
    )
with c2:
    st.dataframe(
        ly_df.rename(
            columns={
                "layer": "レイヤー",
                "focus": "注目領域",
                "companies": "企業",
                "risk": "リスク",
                "strategy": "戦略",
            }
        ),
        use_container_width=True,
        height=200,
    )

c3, c4 = st.columns(2)
with c3:
    st.markdown("**日本のAI関連銘柄**")
    st.dataframe(jp_df, use_container_width=True, height=220)
with c4:
    st.markdown("**投資マイルストーン**")
    st.dataframe(ms_df, use_container_width=True, height=220)


st.markdown("### 6.5 量子コンピュータ投資シナリオ")
qm_df = get_quantum_milestones()
qi_df = get_quantum_investment_universe()
qr_df = get_quantum_risks()

q1, q2 = st.columns(2)
with q1:
    st.markdown("**技術マイルストーンと投資判断**")
    st.dataframe(
        qm_df.rename(
            columns={
                "milestone": "マイルストーン",
                "expected_window": "想定時期",
                "trigger": "投資トリガー",
                "investment_judgement": "投資判断",
            }
        ),
        use_container_width=True,
        height=220,
    )
with q2:
    st.markdown("**投資先（米国・日本・ETF）**")
    st.dataframe(
        qi_df.rename(
            columns={
                "region": "地域",
                "asset_type": "資産タイプ",
                "name": "名称",
                "ticker": "ティッカー",
                "theme": "テーマ",
            }
        ),
        use_container_width=True,
        height=220,
    )

st.markdown("**リスク**")
st.dataframe(
    qr_df.rename(
        columns={
            "risk": "リスク",
            "description": "説明",
            "monitoring_point": "監視ポイント",
        }
    ),
    use_container_width=True,
    height=190,
)


st.markdown("### 6.7 バイオ・ヘルスケア投資シナリオ")
bio_df = get_bio_subthemes()
pipe_df = evaluate_bio_pipeline()

b1, b2 = st.columns(2)
with b1:
    st.markdown("**3つのサブテーマ**")
    st.dataframe(
        bio_df.rename(
            columns={
                "subtheme": "サブテーマ",
                "thesis": "投資仮説",
                "companies": "企業",
            }
        ),
        use_container_width=True,
        height=220,
    )
with b2:
    st.markdown("**パイプライン分析（Phase3重視）**")
    st.dataframe(
        pipe_df.rename(
            columns={
                "company": "企業",
                "subtheme": "サブテーマ",
                "phase1_count": "Phase1",
                "phase2_count": "Phase2",
                "phase3_count": "Phase3",
                "pipeline_score": "パイプラインスコア",
                "commercialization_view": "商用化見通し",
            }
        ),
        use_container_width=True,
        height=220,
    )

st.caption("見方: Phase3に近いパイプラインほど商用化が近いと評価します。")


st.markdown("### 6.8 次世代エネルギー投資シナリオ")
ng_df = get_nextgen_energy_subthemes()
ng_ms_df = get_nextgen_energy_milestones()

n1, n2 = st.columns(2)
with n1:
    st.markdown("**3つのサブテーマ**")
    st.dataframe(
        ng_df.rename(
            columns={
                "subtheme": "サブテーマ",
                "thesis": "投資仮説",
                "companies": "企業",
                "judgement": "投資判断",
                "reason": "理由",
                "window": "想定時期",
            }
        ),
        use_container_width=True,
        height=240,
    )
with n2:
    st.markdown("**投資マイルストーン**")
    st.dataframe(
        ng_ms_df.rename(
            columns={
                "subtheme": "サブテーマ",
                "milestone": "マイルストーン",
                "trigger": "投資トリガー",
            }
        ),
        use_container_width=True,
        height=240,
    )

st.markdown("### 7. 論文トレンド分析")
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
                "keyword": "キーワード",
                "recent_count": "直近6ヶ月",
                "baseline_count": "比較期間",
                "score": "新興度スコア",
            }
        ),
        use_container_width=True,
        height=260,
    )

st.markdown("### 8. イノベーション力ランキング")
inv = compute_innovation_score()
inv_df = inv.ranking_df
if inv_df.empty:
    st.info("イノベーションスコアを計算するデータがありません。")
else:
    inv_df = inv_df[inv_df["tech_theme"].isin(selected_themes)] if "selected_themes" in locals() else inv_df
    theme_opts = sorted(inv_df["tech_theme"].dropna().astype(str).unique().tolist())
    selected_inv_theme = st.selectbox("分析テーマ", options=theme_opts, index=0) if theme_opts else None

    st.plotly_chart(build_innovation_ranking_figure(inv_df), use_container_width=True)
    if selected_inv_theme:
        st.plotly_chart(build_innovation_radar_figure(inv_df, selected_inv_theme, top_n=5), use_container_width=True)

    st.dataframe(
        inv_df.rename(
            columns={
                "tech_theme": "技術テーマ",
                "company": "企業",
                "innovation_score": "Innovation Score",
                "rd_ratio_pct": "R&D比率(%)",
                "patent_count": "特許数",
                "paper_count": "論文数",
                "patent_growth_pct": "特許成長率(%)",
                "pe_ratio": "PER",
                "undervalued": "過小評価候補",
            }
        ),
        use_container_width=True,
        height=280,
    )

    underv = inv.undervalued_df
    if not underv.empty:
        underv = underv[underv["tech_theme"].isin(selected_themes)] if "selected_themes" in locals() else underv
    st.markdown("**過小評価されている可能性がある企業**")
    if underv.empty:
        st.caption("該当なし")
    else:
        st.dataframe(
            underv[["tech_theme", "company", "innovation_score", "pe_ratio"]].rename(
                columns={
                    "tech_theme": "技術テーマ",
                    "company": "企業",
                    "innovation_score": "Innovation Score",
                    "pe_ratio": "PER",
                }
            ),
            use_container_width=True,
            height=180,
        )

st.markdown("### 9. 6大技術テーマ横断ポートフォリオ")
if radar_df.empty:
    st.info("テクノロジーレーダーのデータがないため、ポートフォリオを作成できません。")
else:
    p1, p2 = st.columns([1, 1])
    with p1:
        risk_tolerance = st.selectbox("リスク許容度", options=["保守的", "標準", "積極的"], index=1)
    with p2:
        total_capital = st.number_input("投資予定額", min_value=100000, max_value=1000000000, value=1000000, step=100000)

    art = design_cross_theme_portfolio(
        radar_df=radar_df,
        risk_tolerance=risk_tolerance,
        total_capital_jpy=float(total_capital),
    )

    if art.theme_alloc_df.empty:
        st.info("ポートフォリオ配分データがありません。")
    else:
        c1, c2 = st.columns([1.1, 1.2])
        with c1:
            st.plotly_chart(build_theme_allocation_pie(art.theme_alloc_df), use_container_width=True)
        with c2:
            st.dataframe(
                art.ticker_alloc_df.rename(
                    columns={
                        "theme": "テーマ",
                        "radar_stage": "分類",
                        "ticker": "ティッカー",
                        "name": "名称",
                        "weight": "投資比率",
                        "theme_amount_jpy": "テーマ金額(円)",
                        "amount_jpy": "購入金額(円)",
                        "latest_price": "現在価格",
                        "shares_est": "推定株数",
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
                st.metric("総リターン", f"{art.metrics.get('total_return_pct', 0.0):.1f}%")
                st.metric("CAGR", f"{art.metrics.get('cagr_pct', 0.0):.1f}%")
            with m2:
                st.metric("ボラティリティ", f"{art.metrics.get('volatility_pct', 0.0):.1f}%")
                st.metric("Sharpe", f"{art.metrics.get('sharpe', 0.0):.2f}")
            with m3:
                st.metric("最大DD", f"{art.metrics.get('max_drawdown_pct', 0.0):.1f}%")
                st.metric("SPY比較", f"{art.metrics.get('benchmark_return_pct', 0.0):.1f}%")


render_footer()
