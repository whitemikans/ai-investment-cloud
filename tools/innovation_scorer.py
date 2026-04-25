from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf

from db.tech_research_utils import get_latest_tech_papers, get_patent_stats, get_patent_yearly


COMPANY_TICKER = {
    "IBM": "IBM",
    "Google": "GOOGL",
    "Microsoft": "MSFT",
    "Intel": "INTC",
    "Apple": "AAPL",
    "テスラ": "TSLA",
    "CATL": None,
    "三星SDI": None,
    "東芝": None,
    "NEC": "6701.T",
    "パナソニック": "6752.T",
    "ファナック": "6954.T",
    "Boston Dynamics": None,
    "Commonwealth Fusion": None,
    "TAE Technologies": None,
    "京都フュージョニアリング": None,
}

RND_FALLBACK_PCT = {
    "IBM": 7.5,
    "Google": 15.0,
    "Microsoft": 13.5,
    "Intel": 24.0,
    "Apple": 8.0,
    "テスラ": 4.8,
    "CATL": 7.2,
    "三星SDI": 7.8,
    "東芝": 5.0,
    "NEC": 6.2,
    "パナソニック": 4.2,
    "ファナック": 6.8,
    "Boston Dynamics": 14.0,
    "Commonwealth Fusion": 30.0,
    "TAE Technologies": 28.0,
    "京都フュージョニアリング": 26.0,
}

PE_FALLBACK = {
    "IBM": 21.0,
    "Google": 24.0,
    "Microsoft": 33.0,
    "Intel": 31.0,
    "Apple": 29.0,
    "テスラ": 58.0,
    "NEC": 18.0,
    "パナソニック": 13.0,
    "ファナック": 26.0,
}

COMPANY_ALIASES = {
    "IBM": ["ibm"],
    "Google": ["google", "alphabet", "deepmind"],
    "Microsoft": ["microsoft", "msft"],
    "Intel": ["intel"],
    "Apple": ["apple", "aapl"],
    "テスラ": ["tesla", "tsla"],
    "CATL": ["catl"],
    "三星SDI": ["samsung sdi", "三星sdi"],
    "東芝": ["toshiba", "東芝"],
    "NEC": ["nec"],
    "パナソニック": ["panasonic", "パナソニック"],
    "ファナック": ["fanuc", "ファナック"],
    "Boston Dynamics": ["boston dynamics"],
    "Commonwealth Fusion": ["commonwealth fusion"],
    "TAE Technologies": ["tae technologies"],
    "京都フュージョニアリング": ["kyoto fusioneering", "京都フュージョニアリング"],
}


@dataclass
class InnovationArtifacts:
    ranking_df: pd.DataFrame
    radar_source_df: pd.DataFrame
    undervalued_df: pd.DataFrame


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _normalize_group(df: pd.DataFrame, value_col: str, group_col: str) -> pd.Series:
    out = []
    for _, g in df.groupby(group_col, sort=False):
        vals = pd.to_numeric(g[value_col], errors="coerce").fillna(0.0).astype(float)
        vmin, vmax = float(vals.min()), float(vals.max())
        if vmax - vmin <= 1e-9:
            norm = pd.Series([50.0] * len(vals), index=g.index)
        else:
            norm = ((vals - vmin) / (vmax - vmin) * 100.0).clip(0.0, 100.0)
        out.append(norm)
    return pd.concat(out).sort_index()


def _fetch_rd_ratio(company: str) -> float:
    ticker = COMPANY_TICKER.get(company)
    if not ticker:
        return _safe_float(RND_FALLBACK_PCT.get(company, 5.0), 5.0)
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
        rd = _safe_float(info.get("researchDevelopment", 0.0), 0.0)
        rev = _safe_float(info.get("totalRevenue", 0.0), 0.0)
        if rd > 0 and rev > 0:
            return max(0.0, min(80.0, (rd / rev) * 100.0))
    except Exception:
        pass
    return _safe_float(RND_FALLBACK_PCT.get(company, 5.0), 5.0)


def _fetch_pe_ratio(company: str) -> float | None:
    ticker = COMPANY_TICKER.get(company)
    if ticker:
        try:
            info = yf.Ticker(ticker).info or {}
            pe = _safe_float(info.get("trailingPE"), float("nan"))
            if pe == pe and pe > 0:
                return pe
        except Exception:
            pass
    fb = PE_FALLBACK.get(company)
    return float(fb) if fb is not None else None


def _paper_count_by_company() -> pd.DataFrame:
    papers = get_latest_tech_papers(limit=5000)
    if papers.empty:
        return pd.DataFrame(columns=["company", "paper_count"])
    work = papers.copy()
    work["blob"] = (
        work.get("title", "").astype(str).str.lower()
        + " "
        + work.get("summary", "").astype(str).str.lower()
        + " "
        + work.get("authors", "").astype(str).str.lower()
    )
    rows: list[dict[str, Any]] = []
    for company, aliases in COMPANY_ALIASES.items():
        mask = pd.Series(False, index=work.index)
        for a in aliases:
            mask = mask | work["blob"].str.contains(str(a).lower(), regex=False, na=False)
        count = int(mask.sum())
        rows.append({"company": company, "paper_count": count})
    return pd.DataFrame(rows)


def _theme_yoy_growth() -> pd.DataFrame:
    y = get_patent_yearly()
    if y.empty:
        return pd.DataFrame(columns=["tech_theme", "patent_growth_pct"])
    work = y.copy()
    work["year"] = pd.to_numeric(work["year"], errors="coerce")
    work["yoy_growth_pct"] = pd.to_numeric(work["yoy_growth_pct"], errors="coerce").fillna(0.0)
    latest = (
        work.sort_values(["tech_theme", "year"])
        .groupby("tech_theme", as_index=False)
        .tail(1)
        .rename(columns={"yoy_growth_pct": "patent_growth_pct"})
    )
    return latest[["tech_theme", "patent_growth_pct"]]


def compute_innovation_score() -> InnovationArtifacts:
    pat = get_patent_stats()
    if pat.empty:
        empty = pd.DataFrame()
        return InnovationArtifacts(empty, empty, empty)

    base = pat[["tech_theme", "company", "patent_count"]].copy()
    base["patent_count"] = pd.to_numeric(base["patent_count"], errors="coerce").fillna(0.0)
    base["rd_ratio_pct"] = base["company"].map(lambda c: _fetch_rd_ratio(str(c)))

    papers = _paper_count_by_company()
    if not papers.empty:
        base = base.merge(papers, on="company", how="left")
    base["paper_count"] = pd.to_numeric(base.get("paper_count", 0.0), errors="coerce").fillna(0.0)

    growth = _theme_yoy_growth()
    if not growth.empty:
        base = base.merge(growth, on="tech_theme", how="left")
    base["patent_growth_pct"] = pd.to_numeric(base.get("patent_growth_pct", 0.0), errors="coerce").fillna(0.0)

    base["rd_norm"] = _normalize_group(base, "rd_ratio_pct", "tech_theme")
    base["patent_norm"] = _normalize_group(base, "patent_count", "tech_theme")
    base["paper_norm"] = _normalize_group(base, "paper_count", "tech_theme")
    base["growth_norm"] = _normalize_group(base, "patent_growth_pct", "tech_theme")

    base["innovation_score"] = (
        base["rd_norm"] * 0.30
        + base["patent_norm"] * 0.30
        + base["paper_norm"] * 0.20
        + base["growth_norm"] * 0.20
    ).round(2)

    base["pe_ratio"] = base["company"].map(lambda c: _fetch_pe_ratio(str(c)))
    pe_avail = base["pe_ratio"].dropna()
    pe_cut = float(pe_avail.quantile(0.40)) if not pe_avail.empty else 20.0
    score_cut = float(base["innovation_score"].quantile(0.70)) if not base.empty else 70.0
    base["undervalued"] = (
        (base["innovation_score"] >= score_cut)
        & (base["pe_ratio"].notna())
        & (base["pe_ratio"] <= pe_cut)
    ).astype(int)

    ranking = base.sort_values(["tech_theme", "innovation_score"], ascending=[True, False]).reset_index(drop=True)
    undervalued = ranking[ranking["undervalued"] == 1].copy()
    return InnovationArtifacts(ranking_df=ranking, radar_source_df=ranking.copy(), undervalued_df=undervalued)


def build_innovation_ranking_figure(ranking_df: pd.DataFrame, theme: str | None = None) -> go.Figure:
    use = ranking_df.copy() if ranking_df is not None else pd.DataFrame()
    if use.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="イノベーションランキング", height=380)
        return fig
    if theme:
        use = use[use["tech_theme"] == theme]
    if use.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="イノベーションランキング", height=380)
        return fig
    fig = px.bar(
        use.sort_values("innovation_score", ascending=False),
        x="company",
        y="innovation_score",
        color="tech_theme",
        template="plotly_dark",
        title="技術領域別 イノベーションランキング",
        height=380,
    )
    fig.update_layout(xaxis_title="企業", yaxis_title="Innovation Score")
    return fig


def build_innovation_radar_figure(ranking_df: pd.DataFrame, theme: str, top_n: int = 5) -> go.Figure:
    use = ranking_df.copy() if ranking_df is not None else pd.DataFrame()
    if use.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="企業別4指標レーダー", height=420)
        return fig
    if theme:
        use = use[use["tech_theme"] == theme]
    if use.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="企業別4指標レーダー", height=420)
        return fig
    use = use.sort_values("innovation_score", ascending=False).head(max(1, int(top_n)))

    cats = ["R&D比率", "特許数", "論文数", "特許成長率"]
    fig = go.Figure()
    for r in use.itertuples(index=False):
        vals = [
            float(getattr(r, "rd_norm", 0.0)),
            float(getattr(r, "patent_norm", 0.0)),
            float(getattr(r, "paper_norm", 0.0)),
            float(getattr(r, "growth_norm", 0.0)),
        ]
        fig.add_trace(
            go.Scatterpolar(
                r=vals + [vals[0]],
                theta=cats + [cats[0]],
                fill="toself",
                name=str(getattr(r, "company", "")),
            )
        )
    fig.update_layout(
        template="plotly_dark",
        title=f"{theme} 企業別4指標レーダー",
        height=430,
        polar={"radialaxis": {"visible": True, "range": [0, 100]}},
    )
    return fig

