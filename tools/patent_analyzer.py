from __future__ import annotations

import json
from datetime import datetime

import pandas as pd

from db.tech_research_utils import replace_patent_stats


try:
    from crewai.tools import tool
except Exception:
    def tool(_name: str):  # type: ignore
        def deco(func):
            return func
        return deco


SAMPLE_PATENTS = [
    ("AI", "NVIDIA", 1240, 88.0),
    ("AI", "Microsoft", 980, 84.0),
    ("Quantum", "IBM", 760, 86.0),
    ("Quantum", "IONQ", 240, 71.0),
    ("Biotech", "Moderna", 620, 82.0),
    ("Biotech", "CRISPR", 410, 78.0),
    ("Space", "SpaceX", 300, 77.0),
    ("Space", "Rocket Lab", 210, 72.0),
    ("Energy", "Toyota", 1350, 87.0),
    ("Energy", "Idemitsu", 420, 75.0),
    ("Robotics", "Tesla", 560, 80.0),
    ("Robotics", "ABB", 520, 79.0),
]


def build_patent_stats(as_of: str | None = None) -> pd.DataFrame:
    as_of_date = as_of or datetime.now().date().isoformat()
    rows = [
        {
            "as_of_date": as_of_date,
            "tech_theme": t,
            "company": c,
            "patent_count": int(cnt),
            "innovation_score": float(score),
        }
        for t, c, cnt, score in SAMPLE_PATENTS
    ]
    return pd.DataFrame(rows)


@tool("特許分析")
def build_patent_stats_tool() -> str:
    df = build_patent_stats()
    saved = replace_patent_stats(df)
    return json.dumps({"rows": int(len(df)), "saved": int(saved)}, ensure_ascii=False)

