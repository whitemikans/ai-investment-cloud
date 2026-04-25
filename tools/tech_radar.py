from __future__ import annotations

import math

import pandas as pd
import plotly.graph_objects as go

from db.tech_research_utils import get_hype_history, get_latest_tech_papers
from tools.s_curve_analyzer import analyze_s_curve


RING_MAP = {"Invest": 1.0, "Watch": 2.0, "Research": 3.0, "Hold": 4.0}
ICON_MAP = {"Invest": "🟢", "Watch": "🔵", "Research": "🟡", "Hold": "🔴"}


def _phase_score(phase: str) -> float:
    p = str(phase or "")
    if "安定" in p:
        return 4.0
    if "啓蒙" in p:
        return 3.2
    if "過度な期待" in p:
        return 2.4
    if "幻滅" in p:
        return 1.6
    return 1.2


def _stage_score(stage: str) -> float:
    s = str(stage or "")
    if "成熟" in s:
        return 4.0
    if "急成長" in s:
        return 3.1
    if "導入" in s:
        return 1.8
    return 2.2


def _impact_score_bucket(v: float) -> float:
    x = float(v)
    if x >= 4.3:
        return 4.0
    if x >= 3.5:
        return 3.0
    if x >= 2.7:
        return 2.0
    return 1.0


def _classify(score: float) -> str:
    if score >= 3.45:
        return "Invest"
    if score >= 2.65:
        return "Watch"
    if score >= 1.95:
        return "Research"
    return "Hold"


def _latest_hype() -> pd.DataFrame:
    hype = get_hype_history()
    if hype.empty:
        return pd.DataFrame(columns=["tech_theme", "phase", "hype_index"])
    latest = (
        hype.sort_values(["as_of_date", "tech_theme"])
        .groupby("tech_theme", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    latest["hype_index"] = pd.to_numeric(latest["hype_index"], errors="coerce").fillna(0.0)
    return latest[["tech_theme", "phase", "hype_index"]]


def _paper_impact_by_theme(limit: int = 500) -> pd.DataFrame:
    papers = get_latest_tech_papers(limit=max(100, int(limit)))
    if papers.empty:
        return pd.DataFrame(columns=["tech_theme", "paper_impact"])
    work = papers.copy()
    work["impact_score"] = pd.to_numeric(work["impact_score"], errors="coerce").fillna(0.0)
    agg = (
        work.groupby("tech_theme", as_index=False)["impact_score"]
        .mean()
        .rename(columns={"impact_score": "paper_impact"})
    )
    return agg


def build_tech_radar() -> pd.DataFrame:
    hype = _latest_hype()
    _, s_summary = analyze_s_curve()
    impacts = _paper_impact_by_theme()
    if hype.empty:
        return pd.DataFrame(
            columns=[
                "tech_theme",
                "phase",
                "hype_index",
                "stage",
                "paper_impact",
                "radar_score",
                "radar_stage",
                "investment_signal",
                "radar_ring",
            ]
        )

    merged = hype.merge(s_summary[["tech_theme", "stage"]], on="tech_theme", how="left")
    merged = merged.merge(impacts, on="tech_theme", how="left")
    merged["paper_impact"] = pd.to_numeric(merged["paper_impact"], errors="coerce").fillna(2.5)

    scores: list[float] = []
    stages: list[str] = []
    signals: list[str] = []
    rings: list[float] = []

    for r in merged.itertuples(index=False):
        ps = _phase_score(str(getattr(r, "phase", "")))
        ss = _stage_score(str(getattr(r, "stage", "")))
        iscore = _impact_score_bucket(float(getattr(r, "paper_impact", 0.0)))
        total = 0.35 * ps + 0.35 * ss + 0.30 * iscore
        label = _classify(total)
        scores.append(round(float(total), 3))
        stages.append(label)
        signals.append(ICON_MAP.get(label, "🟡"))
        rings.append(float(RING_MAP.get(label, 4.0)))

    merged["radar_score"] = scores
    merged["radar_stage"] = stages
    merged["investment_signal"] = signals
    merged["radar_ring"] = rings
    return merged[
        [
            "tech_theme",
            "phase",
            "hype_index",
            "stage",
            "paper_impact",
            "radar_score",
            "radar_stage",
            "investment_signal",
            "radar_ring",
        ]
    ]


def build_tech_radar_figure(radar_df: pd.DataFrame) -> go.Figure:
    """Concentric radar-like chart: center=Invest, outer=Hold."""
    fig = go.Figure()
    if radar_df is None or radar_df.empty:
        fig.update_layout(template="plotly_dark", title="投資家向けテクノロジーレーダー", height=450)
        return fig

    work = radar_df.copy().sort_values("tech_theme").reset_index(drop=True)
    n = len(work)
    if n == 0:
        fig.update_layout(template="plotly_dark", title="投資家向けテクノロジーレーダー", height=450)
        return fig

    # θ in degrees for a polar scatter. Keep one point per technology.
    work["theta_deg"] = [i * (360.0 / n) for i in range(n)]

    color_map = {"Invest": "#22c55e", "Watch": "#3b82f6", "Research": "#eab308", "Hold": "#ef4444"}
    for cls in ["Invest", "Watch", "Research", "Hold"]:
        sub = work[work["radar_stage"] == cls]
        if sub.empty:
            continue
        fig.add_trace(
            go.Scatterpolar(
                r=sub["radar_ring"],
                theta=sub["theta_deg"],
                mode="markers+text",
                text=sub["tech_theme"],
                textposition="top center",
                marker={"size": 13, "color": color_map[cls]},
                name=f"{ICON_MAP[cls]} {cls}",
                customdata=sub[["phase", "stage", "paper_impact", "radar_score"]].values,
                hovertemplate=(
                    "技術=%{text}<br>"
                    "分類=" + cls + "<br>"
                    "ハイプ=%{customdata[0]}<br>"
                    "Sカーブ=%{customdata[1]}<br>"
                    "論文impact=%{customdata[2]:.2f}<br>"
                    "総合score=%{customdata[3]:.2f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        template="plotly_dark",
        title="投資家向けテクノロジーレーダー（中心=Invest / 外周=Hold）",
        height=480,
        polar={
            "radialaxis": {
                "range": [0.8, 4.2],
                "tickmode": "array",
                "tickvals": [1, 2, 3, 4],
                "ticktext": ["Invest", "Watch", "Research", "Hold"],
                "angle": 90,
            },
            "angularaxis": {"showticklabels": False, "ticks": ""},
        },
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        margin={"l": 20, "r": 20, "t": 70, "b": 20},
    )
    return fig

