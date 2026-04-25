from __future__ import annotations

import pandas as pd

from db.tech_research_utils import get_hype_history
from tools.s_curve_analyzer import analyze_s_curve


def build_tech_radar() -> pd.DataFrame:
    hype = get_hype_history()
    _, s_summary = analyze_s_curve()
    if hype.empty:
        return pd.DataFrame(columns=["tech_theme", "phase", "hype_index", "radar_stage", "investment_signal"])
    latest = (
        hype.sort_values(["as_of_date", "tech_theme"])
        .groupby("tech_theme", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    merged = latest.merge(s_summary[["tech_theme", "stage"]], on="tech_theme", how="left")

    stages: list[str] = []
    signals: list[str] = []
    for r in merged.itertuples(index=False):
        phase = str(getattr(r, "phase", ""))
        stage = str(getattr(r, "stage", ""))
        if "④" in phase or "⑤" in phase:
            radar = "Invest"
            signal = "🟢"
        elif "③" in phase:
            radar = "Watch"
            signal = "🔵"
        elif stage == "導入期":
            radar = "Research"
            signal = "🟡"
        else:
            radar = "Hold"
            signal = "⚪"
        stages.append(radar)
        signals.append(signal)
    merged["radar_stage"] = stages
    merged["investment_signal"] = signals
    return merged[["tech_theme", "phase", "hype_index", "stage", "radar_stage", "investment_signal"]]

