from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text

from db.models import engine
from db.tech_research_utils import replace_hype_history
from tools.paper_trends import get_paper_trends


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


THEMES = ["AI", "Quantum", "Biotech", "Space", "Energy", "Robotics"]
TREND_KEYWORDS = {
    "AI": "artificial intelligence",
    "Quantum": "quantum computing",
    "Biotech": "biotechnology",
    "Space": "space technology",
    "Energy": "clean energy",
    "Robotics": "robotics",
}
NEWS_KEYWORDS = {
    "AI": ["ai", "artificial intelligence", "llm", "agent", "生成ai"],
    "Quantum": ["quantum", "qubit", "量子"],
    "Biotech": ["biotech", "drug", "gene", "mrna", "バイオ"],
    "Space": ["space", "satellite", "rocket", "宇宙", "衛星"],
    "Energy": ["battery", "energy", "fusion", "solar", "エネルギー", "蓄電池"],
    "Robotics": ["robot", "robotics", "humanoid", "ロボット"],
}


@dataclass
class PhaseRule:
    overhype_level: float = 72.0
    stable_level: float = 66.0
    enlighten_low: float = 45.0
    enlighten_high: float = 74.0
    rise_thr: float = 1.0
    fall_thr: float = -1.0
    flat_thr: float = 0.9


def _month_index(months: int = 36) -> pd.DatetimeIndex:
    end = pd.Timestamp.utcnow().normalize().replace(day=1)
    start = end - pd.DateOffset(months=max(1, int(months) - 1))
    return pd.date_range(start=start, end=end, freq="MS", tz="UTC")


def _normalize_0_100(s: pd.Series) -> pd.Series:
    vals = pd.to_numeric(s, errors="coerce").fillna(0.0).astype(float)
    vmin, vmax = float(vals.min()), float(vals.max())
    if vmax - vmin <= 1e-9:
        if vmax > 0:
            return pd.Series([50.0] * len(vals), index=vals.index)
        return pd.Series([0.0] * len(vals), index=vals.index)
    return ((vals - vmin) / (vmax - vmin) * 100.0).clip(0.0, 100.0)


def _safe_read_news_articles() -> pd.DataFrame:
    sql = text(
        """
        SELECT
          COALESCE(published_at, '') AS published_at,
          COALESCE(title, '') AS title,
          COALESCE(summary_ja, '') AS summary_ja,
          COALESCE(content, '') AS content
        FROM news_articles
        """
    )
    try:
        with engine.connect() as con:
            return pd.read_sql(sql, con=con)
    except Exception:
        return pd.DataFrame(columns=["published_at", "title", "summary_ja", "content"])


def _news_monthly_counts(months: int = 36) -> pd.DataFrame:
    df = _safe_read_news_articles()
    if df.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "news_count"])

    work = df.copy()
    work["published_at"] = pd.to_datetime(work["published_at"], errors="coerce", utc=True)
    work = work.dropna(subset=["published_at"])
    if work.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "news_count"])
    cutoff = pd.Timestamp.utcnow() - pd.DateOffset(months=max(1, int(months)))
    work = work[work["published_at"] >= cutoff]
    if work.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "news_count"])

    work["month"] = work["published_at"].dt.strftime("%Y-%m")
    work["blob"] = (
        work["title"].astype(str).str.lower()
        + " "
        + work["summary_ja"].astype(str).str.lower()
        + " "
        + work["content"].astype(str).str.lower()
    )

    rows: list[dict[str, Any]] = []
    for theme, kws in NEWS_KEYWORDS.items():
        mask = pd.Series(False, index=work.index)
        for kw in kws:
            mask = mask | work["blob"].str.contains(str(kw).lower(), regex=False, na=False)
        tmp = work[mask]
        if tmp.empty:
            continue
        agg = tmp.groupby("month", as_index=False).size().rename(columns={"size": "news_count"})
        agg["tech_theme"] = theme
        rows.extend(agg.to_dict(orient="records"))
    if not rows:
        return pd.DataFrame(columns=["month", "tech_theme", "news_count"])
    out = pd.DataFrame(rows)
    return out[["month", "tech_theme", "news_count"]].sort_values(["month", "tech_theme"]).reset_index(drop=True)


def _google_trends_monthly(months: int = 36) -> pd.DataFrame:
    try:
        from pytrends.request import TrendReq
    except Exception:
        return pd.DataFrame(columns=["month", "tech_theme", "search_volume"])

    idx = _month_index(months=max(12, int(months)))
    start = idx.min().strftime("%Y-%m-%d")
    end = (idx.max() + pd.offsets.MonthEnd(1)).strftime("%Y-%m-%d")
    timeframe = f"{start} {end}"
    rows: list[dict[str, Any]] = []

    try:
        py = TrendReq(hl="en-US", tz=360)
    except Exception:
        return pd.DataFrame(columns=["month", "tech_theme", "search_volume"])

    for theme in THEMES:
        kw = TREND_KEYWORDS.get(theme, theme)
        try:
            py.build_payload([kw], timeframe=timeframe, geo="", gprop="")
            df = py.interest_over_time()
            if df is None or df.empty or kw not in df.columns:
                continue
            s = df[[kw]].copy().rename(columns={kw: "search_volume"})
            s = s.reset_index().rename(columns={"date": "published_at"})
            s["published_at"] = pd.to_datetime(s["published_at"], errors="coerce", utc=True)
            s = s.dropna(subset=["published_at"])
            if s.empty:
                continue
            s["month"] = s["published_at"].dt.strftime("%Y-%m")
            m = s.groupby("month", as_index=False)["search_volume"].mean()
            m["tech_theme"] = theme
            rows.extend(m.to_dict(orient="records"))
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["month", "tech_theme", "search_volume"])
    out = pd.DataFrame(rows)
    return out[["month", "tech_theme", "search_volume"]].sort_values(["month", "tech_theme"]).reset_index(drop=True)


def _fallback_search_from_papers(papers: pd.DataFrame) -> pd.DataFrame:
    if papers.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "search_volume"])
    out = papers.rename(columns={"paper_count": "search_volume"})[["month", "tech_theme", "search_volume"]].copy()
    out["search_volume"] = pd.to_numeric(out["search_volume"], errors="coerce").fillna(0.0) * 3.0
    return out


def _phase_from_index(index_value: float, slope: float, prev_slope: float, rule: PhaseRule) -> str:
    if slope >= rule.rise_thr and index_value >= rule.overhype_level:
        return "②過度な期待"
    if slope <= rule.fall_thr:
        return "③幻滅の谷"
    if slope >= rule.rise_thr and prev_slope < 0 and rule.enlighten_low <= index_value <= rule.enlighten_high:
        return "④啓蒙活動期"
    if abs(slope) <= rule.flat_thr and index_value >= rule.stable_level:
        return "⑤安定期"
    return "①黎明期"


def generate_hype_cycle(months: int = 36) -> pd.DataFrame:
    """Generate monthly data-driven hype index and phase per technology theme."""
    paper_df = get_paper_trends(months=max(12, int(months)))
    news_df = _news_monthly_counts(months=max(12, int(months)))
    search_df = _google_trends_monthly(months=max(12, int(months)))
    if search_df.empty:
        search_df = _fallback_search_from_papers(paper_df)

    idx = _month_index(months=max(12, int(months)))
    idx_df = pd.DataFrame({"as_of_date": idx.tz_convert(None).date.astype(str)})
    idx_df["month"] = pd.to_datetime(idx_df["as_of_date"]).dt.strftime("%Y-%m")

    rows: list[dict[str, Any]] = []
    rule = PhaseRule()
    for theme in THEMES:
        base = idx_df.copy()
        p = paper_df[paper_df["tech_theme"] == theme][["month", "paper_count"]] if not paper_df.empty else pd.DataFrame()
        n = news_df[news_df["tech_theme"] == theme][["month", "news_count"]] if not news_df.empty else pd.DataFrame()
        s = search_df[search_df["tech_theme"] == theme][["month", "search_volume"]] if not search_df.empty else pd.DataFrame()
        if not p.empty:
            base = base.merge(p, on="month", how="left")
        else:
            base["paper_count"] = 0.0
        if not n.empty:
            base = base.merge(n, on="month", how="left")
        else:
            base["news_count"] = 0.0
        if not s.empty:
            base = base.merge(s, on="month", how="left")
        else:
            base["search_volume"] = 0.0

        for col in ["paper_count", "news_count", "search_volume"]:
            base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0.0)

        base["search_norm"] = _normalize_0_100(base["search_volume"])
        base["paper_norm"] = _normalize_0_100(base["paper_count"])
        base["news_norm"] = _normalize_0_100(base["news_count"])
        base["hype_index"] = (
            base["search_norm"] * 0.4 + base["paper_norm"] * 0.3 + base["news_norm"] * 0.3
        ).clip(0.0, 100.0)
        base["slope"] = base["hype_index"].diff().fillna(0.0)
        base["prev_slope"] = base["slope"].shift(1).fillna(0.0)
        base["phase"] = base.apply(
            lambda r: _phase_from_index(
                float(r["hype_index"]),
                float(r["slope"]),
                float(r["prev_slope"]),
                rule,
            ),
            axis=1,
        )

        for r in base.itertuples(index=False):
            rows.append(
                {
                    "as_of_date": str(getattr(r, "as_of_date")),
                    "tech_theme": theme,
                    "hype_index": round(float(getattr(r, "hype_index", 0.0)), 2),
                    "phase": str(getattr(r, "phase", "①黎明期")),
                    "source_breakdown_json": json.dumps(
                        {
                            "search": round(float(getattr(r, "search_norm", 0.0)), 2),
                            "papers": round(float(getattr(r, "paper_norm", 0.0)), 2),
                            "news": round(float(getattr(r, "news_norm", 0.0)), 2),
                            "raw_search_volume": round(float(getattr(r, "search_volume", 0.0)), 2),
                            "raw_paper_count": round(float(getattr(r, "paper_count", 0.0)), 2),
                            "raw_news_count": round(float(getattr(r, "news_count", 0.0)), 2),
                        },
                        ensure_ascii=False,
                    ),
                }
            )

    return pd.DataFrame(rows)


def build_hype_cycle_figure(hype_df: pd.DataFrame) -> go.Figure:
    """Build a single multi-line hype cycle chart with phase backgrounds and current labels."""
    fig = go.Figure()
    if hype_df is None or hype_df.empty:
        fig.update_layout(template="plotly_dark", title="ハイプサイクル", height=460)
        return fig

    work = hype_df.copy()
    work["as_of_date"] = pd.to_datetime(work["as_of_date"], errors="coerce")
    work = work.dropna(subset=["as_of_date"]).sort_values(["as_of_date", "tech_theme"])
    if work.empty:
        fig.update_layout(template="plotly_dark", title="ハイプサイクル", height=460)
        return fig

    # Phase bands (shared y-ranges)
    bands = [
        ("③幻滅の谷", 0, 35, "rgba(59,130,246,0.12)"),     # blue
        ("④啓蒙活動期", 35, 65, "rgba(34,197,94,0.10)"),   # green
        ("②過度な期待", 65, 100, "rgba(239,68,68,0.10)"),  # red
    ]
    for name, y0, y1, color in bands:
        fig.add_hrect(y0=y0, y1=y1, fillcolor=color, line_width=0, annotation_text=name, annotation_position="left")

    for theme in THEMES:
        tdf = work[work["tech_theme"] == theme].sort_values("as_of_date")
        if tdf.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=tdf["as_of_date"],
                y=tdf["hype_index"],
                mode="lines",
                name=theme,
                hovertemplate="テーマ=%{fullData.name}<br>日付=%{x|%Y-%m}<br>指数=%{y:.1f}<extra></extra>",
            )
        )
        last = tdf.iloc[-1]
        fig.add_annotation(
            x=last["as_of_date"],
            y=float(last["hype_index"]),
            text=f"{theme}: {last['phase']}",
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font={"size": 11},
        )

    fig.update_layout(
        template="plotly_dark",
        title="データ駆動ハイプサイクル（6大技術）",
        height=500,
        xaxis_title="時間",
        yaxis_title="ハイプ指数",
        yaxis={"range": [0, 100]},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        margin={"l": 40, "r": 20, "t": 70, "b": 40},
    )
    return fig


@tool("ハイプサイクル生成")
def generate_hype_cycle_tool() -> str:
    """Generate and store monthly data-driven hype cycle values for each tech theme."""
    df = generate_hype_cycle(months=36)
    saved = replace_hype_history(df)
    return json.dumps({"rows": int(len(df)), "saved": int(saved)}, ensure_ascii=False)

