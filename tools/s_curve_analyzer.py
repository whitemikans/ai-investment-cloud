from __future__ import annotations

import math

import pandas as pd


BASE_ADOPTION = [
    {"tech_theme": "AI", "current_adoption_pct": 35.0, "k": 0.28, "t0": 2029},
    {"tech_theme": "Quantum", "current_adoption_pct": 0.7, "k": 0.18, "t0": 2038},
    {"tech_theme": "Biotech", "current_adoption_pct": 16.0, "k": 0.22, "t0": 2033},
    {"tech_theme": "Space", "current_adoption_pct": 9.0, "k": 0.20, "t0": 2035},
    {"tech_theme": "Energy", "current_adoption_pct": 28.0, "k": 0.24, "t0": 2031},
    {"tech_theme": "Robotics", "current_adoption_pct": 6.0, "k": 0.23, "t0": 2036},
]


def _logistic(year: int, k: float, t0: float, L: float = 100.0) -> float:
    return float(L / (1.0 + math.exp(-k * (year - t0))))


def analyze_s_curve(start_year: int = 2025, end_year: int = 2040) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict] = []
    summary: list[dict] = []
    years = list(range(int(start_year), int(end_year) + 1))
    for item in BASE_ADOPTION:
        theme = str(item["tech_theme"])
        k = float(item["k"])
        t0 = float(item["t0"])
        for y in years:
            rows.append({"year": y, "tech_theme": theme, "adoption_pct": _logistic(y, k, t0)})
        current = float(item["current_adoption_pct"])
        if current < 10:
            stage = "導入期"
        elif current < 50:
            stage = "急成長期"
        else:
            stage = "成熟期"
        summary.append(
            {
                "tech_theme": theme,
                "current_adoption_pct": current,
                "estimated_50pct_year": int(round(t0)),
                "stage": stage,
            }
        )
    return pd.DataFrame(rows), pd.DataFrame(summary)

