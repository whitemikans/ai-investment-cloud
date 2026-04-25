from __future__ import annotations

import pandas as pd

from db.tech_research_utils import get_latest_tech_papers


def get_paper_trends(months: int = 12) -> pd.DataFrame:
    df = get_latest_tech_papers(limit=3000)
    if df.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "paper_count"])
    work = df.copy()
    work["published_at"] = pd.to_datetime(work["published_at"], errors="coerce")
    work = work.dropna(subset=["published_at"])
    if work.empty:
        return pd.DataFrame(columns=["month", "tech_theme", "paper_count"])
    cutoff = pd.Timestamp.now(tz=None) - pd.DateOffset(months=max(1, int(months)))
    work = work[work["published_at"] >= cutoff]
    work["month"] = work["published_at"].dt.strftime("%Y-%m")
    out = work.groupby(["month", "tech_theme"], as_index=False).size().rename(columns={"size": "paper_count"})
    return out.sort_values(["month", "tech_theme"]).reset_index(drop=True)

