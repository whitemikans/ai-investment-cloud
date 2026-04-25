from __future__ import annotations

import math
import re
from collections import Counter
from typing import Iterable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from db.tech_research_utils import get_latest_tech_papers


STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that", "these", "those", "into", "onto", "over", "under",
    "using", "based", "study", "paper", "approach", "method", "methods", "results", "analysis", "toward", "towards",
    "via", "new", "novel", "large", "small", "multi", "single", "data", "model", "models", "system", "systems",
    "learning", "deep", "neural", "network", "networks", "machine", "language", "framework", "frameworks",
    "arxiv", "preprint", "research", "application", "applications", "performance", "efficient", "improving",
}

REGION_KEYWORDS = {
    "United States": ["usa", "u.s.", "us ", "united states", "california", "mit", "stanford", "harvard", "cmu", "nyu"],
    "China": ["china", "chinese", "beijing", "tsinghua", "peking", "shanghai", "tencent", "alibaba"],
    "Japan": ["japan", "japanese", "tokyo", "kyoto", "osaka", "riken", "utokyo", "waseda", "keio"],
    "Europe": [
        "europe", "european", "eu ", "uk ", "united kingdom", "germany", "france", "italy", "spain", "netherlands",
        "switzerland", "oxford", "cambridge", "imperial college", "eth zurich", "epfl", "max planck",
    ],
}


def _base_papers(limit: int = 12000) -> pd.DataFrame:
    df = get_latest_tech_papers(limit=max(1000, int(limit)))
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["published_at"] = pd.to_datetime(work["published_at"], errors="coerce", utc=True)
    work = work.dropna(subset=["published_at"])
    if work.empty:
        return pd.DataFrame()
    work["month"] = work["published_at"].dt.strftime("%Y-%m")
    work["title"] = work["title"].astype(str)
    work["summary"] = work["summary"].astype(str)
    work["tech_theme"] = work.get("tech_theme", "").astype(str)
    return work


def get_paper_trends(months: int = 36) -> pd.DataFrame:
    """Return monthly paper counts by theme for the requested period."""
    work = _base_papers()
    if work.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "paper_count"])
    cutoff = pd.Timestamp.now(tz="UTC") - pd.DateOffset(months=max(1, int(months)))
    work = work[work["published_at"] >= cutoff]
    if work.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "paper_count"])
    out = work.groupby(["month", "tech_theme"], as_index=False).size().rename(columns={"size": "paper_count"})
    return out.sort_values(["month", "tech_theme"]).reset_index(drop=True)


def _tokenize(text: str) -> list[str]:
    src = re.sub(r"[^a-zA-Z0-9\-\s]", " ", str(text or "").lower())
    tokens = [t.strip("-") for t in src.split() if len(t.strip("-")) >= 3]
    out: list[str] = []
    for t in tokens:
        if t in STOPWORDS:
            continue
        if t.isdigit():
            continue
        out.append(t)
    return out


def detect_emerging_keywords(months_back: int = 6, baseline_months: int = 30, top_n: int = 40) -> pd.DataFrame:
    """Detect keywords that became frequent recently compared with historical baseline."""
    work = _base_papers()
    if work.empty:
        return pd.DataFrame(columns=["keyword", "recent_count", "baseline_count", "score"])

    recent_cutoff = pd.Timestamp.now(tz="UTC") - pd.DateOffset(months=max(1, int(months_back)))
    baseline_cutoff = recent_cutoff - pd.DateOffset(months=max(1, int(baseline_months)))

    recent = work[work["published_at"] >= recent_cutoff]
    base = work[(work["published_at"] >= baseline_cutoff) & (work["published_at"] < recent_cutoff)]
    if recent.empty:
        return pd.DataFrame(columns=["keyword", "recent_count", "baseline_count", "score"])

    recent_counter: Counter[str] = Counter()
    for row in recent.itertuples(index=False):
        recent_counter.update(_tokenize(f"{getattr(row, 'title', '')} {getattr(row, 'summary', '')}"))

    base_counter: Counter[str] = Counter()
    for row in base.itertuples(index=False):
        base_counter.update(_tokenize(f"{getattr(row, 'title', '')} {getattr(row, 'summary', '')}"))

    rows = []
    for kw, r_cnt in recent_counter.items():
        if r_cnt < 3:
            continue
        b_cnt = int(base_counter.get(kw, 0))
        # Score new/surging words higher while penalizing already-common terms.
        score = float(r_cnt) / float(1 + b_cnt) * math.log(2 + r_cnt)
        rows.append({"keyword": kw, "recent_count": int(r_cnt), "baseline_count": b_cnt, "score": round(score, 4)})

    if not rows:
        return pd.DataFrame(columns=["keyword", "recent_count", "baseline_count", "score"])
    out = pd.DataFrame(rows).sort_values(["score", "recent_count"], ascending=[False, False]).head(max(5, int(top_n)))
    return out.reset_index(drop=True)


def _infer_region(text: str) -> str:
    src = str(text or "").lower()
    scores: dict[str, int] = {}
    for region, kws in REGION_KEYWORDS.items():
        scores[region] = sum(1 for kw in kws if kw in src)
    best = max(scores, key=scores.get)
    return best if scores.get(best, 0) > 0 else "Other"


def get_country_share_trends(months: int = 36) -> pd.DataFrame:
    """Estimate country/region research share trends from paper metadata text signals."""
    work = _base_papers()
    if work.empty:
        return pd.DataFrame(columns=["month", "region", "paper_count", "share"])
    cutoff = pd.Timestamp.now(tz="UTC") - pd.DateOffset(months=max(1, int(months)))
    work = work[work["published_at"] >= cutoff]
    if work.empty:
        return pd.DataFrame(columns=["month", "region", "paper_count", "share"])

    def _concat_text(r: pd.Series) -> str:
        return f"{r.get('title', '')} {r.get('summary', '')} {r.get('authors', '')} {r.get('categories', '')}"

    work = work.copy()
    work["region"] = work.apply(lambda r: _infer_region(_concat_text(r)), axis=1)
    counts = work.groupby(["month", "region"], as_index=False).size().rename(columns={"size": "paper_count"})
    totals = counts.groupby("month", as_index=False)["paper_count"].sum().rename(columns={"paper_count": "month_total"})
    out = counts.merge(totals, on="month", how="left")
    out["share"] = out["paper_count"] / out["month_total"].replace(0, 1)
    return out.sort_values(["month", "region"]).reset_index(drop=True)


def build_theme_trend_figure(trend_df: pd.DataFrame) -> go.Figure:
    """Plotly line chart for monthly paper counts by theme."""
    if trend_df is None or trend_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="分野別論文数推移", height=380)
        return fig
    fig = px.line(
        trend_df,
        x="month",
        y="paper_count",
        color="tech_theme",
        markers=True,
        template="plotly_dark",
        title="分野別論文数推移（過去3年・月次）",
        height=380,
    )
    fig.update_layout(xaxis_title="年月", yaxis_title="論文数")
    return fig


def _spiral_positions(n: int) -> Iterable[tuple[float, float]]:
    for i in range(max(1, n)):
        angle = i * 0.78
        radius = 0.28 + i * 0.035
        yield radius * math.cos(angle), radius * math.sin(angle)


def build_keyword_cloud_figure(keyword_df: pd.DataFrame) -> go.Figure:
    """Pseudo word-cloud chart using Plotly text traces with size by keyword score."""
    fig = go.Figure()
    if keyword_df is None or keyword_df.empty:
        fig.update_layout(template="plotly_dark", title="新興キーワード", height=420)
        return fig

    use = keyword_df.head(40).copy()
    s_min, s_max = float(use["score"].min()), float(use["score"].max())
    denom = max(1e-9, s_max - s_min)
    use["font_size"] = 16 + (use["score"] - s_min) / denom * 30

    xs, ys = zip(*list(_spiral_positions(len(use))))
    fig.add_trace(
        go.Scatter(
            x=list(xs),
            y=list(ys),
            mode="text",
            text=use["keyword"],
            textfont={"size": use["font_size"], "color": "#7dd3fc"},
            hovertemplate=(
                "keyword=%{text}<br>"
                + "score=%{customdata[0]:.2f}<br>"
                + "recent=%{customdata[1]}<br>"
                + "baseline=%{customdata[2]}<extra></extra>"
            ),
            customdata=use[["score", "recent_count", "baseline_count"]].values,
        )
    )
    fig.update_layout(
        template="plotly_dark",
        title="新興キーワードクラウド（過去6ヶ月）",
        height=420,
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
    )
    return fig


def build_country_share_area_figure(country_df: pd.DataFrame) -> go.Figure:
    """Stacked area chart for country/region share changes."""
    if country_df is None or country_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="国別シェア推移", height=380)
        return fig

    use = country_df.copy()
    fig = px.area(
        use,
        x="month",
        y="share",
        color="region",
        groupnorm="fraction",
        template="plotly_dark",
        title="国別研究シェアの変化（推定）",
        height=380,
    )
    fig.update_layout(xaxis_title="年月", yaxis_title="シェア")
    fig.update_yaxes(tickformat=".0%")
    return fig
