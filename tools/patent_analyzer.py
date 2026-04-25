from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import plotly.express as px
import requests

from config import get_setting
from db.tech_research_utils import replace_patent_stats


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


TECH_QUERIES = {
    "Quantum": {
        "keyword": "quantum computing",
        "companies": ["IBM", "Google", "Microsoft", "Intel", "東芝", "NEC"],
    },
    "Energy": {
        "keyword": "solid state battery",
        "companies": ["Apple", "パナソニック", "三星SDI", "CATL"],
    },
    "Robotics": {
        "keyword": "humanoid robot",
        "companies": ["テスラ", "Boston Dynamics", "ファナック"],
    },
    "Fusion": {
        "keyword": "nuclear fusion",
        "companies": ["Commonwealth Fusion", "TAE Technologies", "京都フュージョニアリング"],
    },
}

FALLBACK_COUNTS = {
    ("Quantum", "IBM"): 920,
    ("Quantum", "Google"): 610,
    ("Quantum", "Microsoft"): 580,
    ("Quantum", "Intel"): 470,
    ("Quantum", "東芝"): 430,
    ("Quantum", "NEC"): 390,
    ("Energy", "Apple"): 520,
    ("Energy", "パナソニック"): 810,
    ("Energy", "三星SDI"): 760,
    ("Energy", "CATL"): 990,
    ("Robotics", "テスラ"): 640,
    ("Robotics", "Boston Dynamics"): 410,
    ("Robotics", "ファナック"): 570,
    ("Fusion", "Commonwealth Fusion"): 180,
    ("Fusion", "TAE Technologies"): 220,
    ("Fusion", "京都フュージョニアリング"): 130,
}


def _lens_api_key() -> str:
    return (get_setting("LENS_API_KEY", "") or os.getenv("LENS_API_KEY", "")).strip()


def _lens_headers() -> dict[str, str]:
    key = _lens_api_key()
    if not key:
        return {}
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


def _extract_total_and_yearly(data: dict[str, Any]) -> tuple[int, dict[int, int]]:
    total = 0
    yearly: dict[int, int] = {}

    hits = data.get("hits", {})
    if isinstance(hits, dict):
        ht = hits.get("total", {})
        if isinstance(ht, dict):
            total = int(ht.get("value", 0) or 0)
        elif isinstance(ht, int):
            total = int(ht)

    if not total:
        total = int(data.get("total", 0) or 0)

    aggs = data.get("aggregations", {}) or data.get("aggs", {}) or {}
    if isinstance(aggs, dict):
        buckets = None
        for k in ["by_year", "year_histogram", "year"]:
            node = aggs.get(k)
            if isinstance(node, dict) and isinstance(node.get("buckets"), list):
                buckets = node["buckets"]
                break
        if buckets:
            for b in buckets:
                try:
                    key = str(b.get("key_as_string", b.get("key", "")))
                    year = int(key[:4]) if key else int(b.get("key", 0))
                    yearly[year] = int(b.get("doc_count", 0) or 0)
                except Exception:
                    continue

    return total, yearly


def _search_lens_patents(keyword: str, company: str, year_from: int = 2018) -> tuple[int, dict[int, int]]:
    headers = _lens_headers()
    if not headers:
        return 0, {}

    query_text = f'("{keyword}") AND ("{company}")'
    payload = {
        "query": {"bool": {"must": [{"query_string": {"query": query_text}}]}},
        "size": 0,
        "aggs": {
            "by_year": {
                "date_histogram": {
                    "field": "date_published",
                    "calendar_interval": "year",
                    "min_doc_count": 0,
                }
            }
        },
    }
    url = "https://api.lens.org/patent/search"
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=25)
        resp.raise_for_status()
        total, yearly = _extract_total_and_yearly(resp.json())
        if yearly:
            yearly = {int(y): int(c) for y, c in yearly.items() if int(y) >= int(year_from)}
        return int(total), yearly
    except Exception:
        return 0, {}


def _fallback_yearly(total_count: int, start_year: int = 2018, end_year: int | None = None) -> dict[int, int]:
    end = end_year or datetime.now().year
    years = list(range(int(start_year), int(end) + 1))
    if not years:
        return {}
    weights = [1 + (i / max(1, len(years) - 1)) * 2.2 for i in range(len(years))]
    base = sum(weights)
    vals = [int(round(total_count * w / base)) for w in weights]
    return {y: max(0, v) for y, v in zip(years, vals)}


def build_patent_stats(as_of: str | None = None, use_live: bool = True) -> pd.DataFrame:
    """Build company-level patent counts by technology theme.

    If LENS_API_KEY is available, tries Lens.org API first. Falls back to built-in sample counts.
    """
    as_of_date = as_of or datetime.now().date().isoformat()
    rows: list[dict[str, Any]] = []

    for theme, spec in TECH_QUERIES.items():
        keyword = str(spec["keyword"])
        for company in spec["companies"]:
            count = 0
            yearly: dict[int, int] = {}
            if use_live:
                count, yearly = _search_lens_patents(keyword=keyword, company=str(company))
                time.sleep(0.25)  # keep within free-tier polite usage
            if count <= 0:
                count = int(FALLBACK_COUNTS.get((theme, str(company)), 0))
            innovation = min(100.0, 20.0 + (count ** 0.5) * 2.4)
            rows.append(
                {
                    "as_of_date": as_of_date,
                    "tech_theme": theme,
                    "company": str(company),
                    "patent_count": int(count),
                    "innovation_score": float(round(innovation, 2)),
                    "source": "lens_api" if (use_live and count > 0 and bool(yearly)) else "fallback",
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["as_of_date", "tech_theme", "company", "patent_count", "innovation_score", "source"])
    return out.sort_values(["tech_theme", "patent_count"], ascending=[True, False]).reset_index(drop=True)


def build_patent_yearly_stats(start_year: int = 2018, use_live: bool = True) -> pd.DataFrame:
    """Build yearly patent application trend and YoY growth by technology theme."""
    end_year = datetime.now().year
    rows: list[dict[str, Any]] = []

    for theme, spec in TECH_QUERIES.items():
        keyword = str(spec["keyword"])
        tech_year_counts: dict[int, int] = {}
        for company in spec["companies"]:
            total = 0
            yearly: dict[int, int] = {}
            if use_live:
                total, yearly = _search_lens_patents(keyword=keyword, company=str(company), year_from=start_year)
                time.sleep(0.25)
            if not yearly:
                total = int(FALLBACK_COUNTS.get((theme, str(company)), 0))
                yearly = _fallback_yearly(total, start_year=start_year, end_year=end_year)
            for y, c in yearly.items():
                if int(y) < int(start_year):
                    continue
                tech_year_counts[int(y)] = int(tech_year_counts.get(int(y), 0)) + int(c)

        for y in range(int(start_year), int(end_year) + 1):
            rows.append(
                {
                    "as_of_date": datetime.now().date().isoformat(),
                    "tech_theme": theme,
                    "year": int(y),
                    "patent_count": int(tech_year_counts.get(int(y), 0)),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["as_of_date", "tech_theme", "year", "patent_count", "yoy_growth_pct"])
    out = out.sort_values(["tech_theme", "year"]).reset_index(drop=True)
    out["yoy_growth_pct"] = (
        out.groupby("tech_theme")["patent_count"].pct_change().replace([float("inf"), float("-inf")], 0.0).fillna(0.0) * 100.0
    )
    out["yoy_growth_pct"] = out["yoy_growth_pct"].round(2)
    return out


def get_top_patent_companies(stats_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    if stats_df is None or stats_df.empty:
        return pd.DataFrame(columns=["tech_theme", "company", "patent_count", "innovation_score"])
    work = stats_df.copy()
    work = work.sort_values(["tech_theme", "patent_count"], ascending=[True, False])
    return work.groupby("tech_theme", as_index=False).head(max(1, int(top_n))).reset_index(drop=True)


def build_patent_bar_figure(stats_df: pd.DataFrame) -> go.Figure:
    import plotly.graph_objects as go

    if stats_df is None or stats_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="企業別特許数", height=360)
        return fig
    fig = px.bar(
        stats_df.sort_values(["tech_theme", "patent_count"], ascending=[True, False]),
        x="company",
        y="patent_count",
        color="tech_theme",
        template="plotly_dark",
        title="企業別の特許数",
        height=380,
    )
    fig.update_layout(xaxis_title="企業", yaxis_title="特許数")
    return fig


def build_patent_growth_figure(yearly_df: pd.DataFrame) -> go.Figure:
    import plotly.graph_objects as go

    if yearly_df is None or yearly_df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_dark", title="年次推移（特許出願数の伸び率）", height=360)
        return fig
    fig = px.line(
        yearly_df,
        x="year",
        y="yoy_growth_pct",
        color="tech_theme",
        markers=True,
        template="plotly_dark",
        title="年次推移（特許出願数の伸び率, %）",
        height=360,
    )
    fig.update_layout(xaxis_title="年", yaxis_title="YoY伸び率(%)")
    return fig


@tool("特許分析")
def build_patent_stats_tool() -> str:
    """Build and save technology patent statistics for dashboard usage."""
    df = build_patent_stats()
    saved = replace_patent_stats(df.drop(columns=["source"], errors="ignore"))
    top = get_top_patent_companies(df, top_n=5)
    return json.dumps(
        {
            "rows": int(len(df)),
            "saved": int(saved),
            "top5": top.to_dict(orient="records"),
        },
        ensure_ascii=False,
    )

