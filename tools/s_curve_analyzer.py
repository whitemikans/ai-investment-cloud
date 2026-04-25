from __future__ import annotations

import math
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit


# Hardcoded adoption proxy data (lecture-style seed values)
# AI: enterprise AI adoption ratio (proxy from public surveys such as McKinsey trends)
# Quantum: share/index of research orgs with practical quantum access (normalized to %)
# EV: EV sales ratio
# Renewable: share of renewable power generation
OBSERVED_SERIES = {
    "AI": {
        "years": [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
        "adoption": [8, 10, 13, 18, 25, 33, 39, 45],
    },
    "Quantum": {
        "years": [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
        "adoption": [0.2, 0.3, 0.5, 0.8, 1.3, 2.0, 2.8, 3.7],
    },
    "EV": {
        "years": [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
        "adoption": [2.1, 2.3, 3.4, 6.1, 9.8, 14.2, 18.0, 22.5],
    },
    "Renewable": {
        "years": [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
        "adoption": [20, 21, 23, 25, 28, 31, 34, 37],
    },
}


def logistic(t: np.ndarray | float, L: float, k: float, t0: float) -> np.ndarray | float:
    """Logistic adoption curve: L / (1 + e^(-k(t-t0)))."""
    return L / (1.0 + np.exp(-k * (np.asarray(t) - t0)))


def _fit_logistic(years: list[int], adoption: list[float]) -> tuple[float, float, float]:
    x = np.asarray(years, dtype=float)
    y = np.asarray(adoption, dtype=float)
    # Initial guess: L near upper bound, modest growth speed, midpoint around median year.
    p0 = [95.0, 0.22, float(np.median(x))]
    bounds = ([60.0, 0.01, x.min() - 10], [120.0, 2.0, x.max() + 20])
    try:
        params, _ = curve_fit(logistic, x, y, p0=p0, bounds=bounds, maxfev=20000)
        L, k, t0 = [float(v) for v in params]
        return L, k, t0
    except Exception:
        # Fallback to a conservative shape if fitting fails.
        return 100.0, 0.20, float(np.median(x) + 4)


def _stage_from_adoption(current_pct: float) -> str:
    if current_pct < 15.0:
        return "導入期"
    if current_pct < 55.0:
        return "急成長期"
    return "成熟期"


def _year_reach_50(L: float, k: float, t0: float) -> int | None:
    # If asymptote never reaches 50, 50% is not reachable.
    if L <= 50.0 or k <= 0:
        return None
    # Solve 50 = L/(1+exp(-k(t-t0)))
    # t = t0 - ln(L/50 - 1)/k
    try:
        t = t0 - math.log((L / 50.0) - 1.0) / k
        return int(round(t))
    except Exception:
        return None


def analyze_s_curve(start_year: int = 2018, end_year: int = 2045) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit logistic S-curves for each technology and return points + summary."""
    years = np.arange(int(start_year), int(end_year) + 1, dtype=float)
    current_year = int(datetime.now().year)

    points: list[dict[str, Any]] = []
    summary: list[dict[str, Any]] = []

    for tech, obs in OBSERVED_SERIES.items():
        y_obs = [float(v) for v in obs["adoption"]]
        x_obs = [int(v) for v in obs["years"]]
        L, k, t0 = _fit_logistic(x_obs, y_obs)

        y_fit = logistic(years, L, k, t0)
        y_fit = np.clip(np.asarray(y_fit, dtype=float), 0.0, 100.0)

        for yy, val in zip(years, y_fit):
            is_current = int(yy == current_year)
            points.append(
                {
                    "year": int(yy),
                    "tech_theme": tech,
                    "adoption_pct": round(float(val), 2),
                    "is_current": is_current,
                }
            )

        # Current point: use fitted value at current_year; if out of range, use latest observed.
        current_fit = float(np.clip(logistic(float(current_year), L, k, t0), 0.0, 100.0))
        stage = _stage_from_adoption(current_fit)
        y50 = _year_reach_50(L, k, t0)

        summary.append(
            {
                "tech_theme": tech,
                "L_max": round(L, 2),
                "k_growth": round(k, 4),
                "t0_inflection": round(t0, 2),
                "current_adoption_pct": round(current_fit, 2),
                "estimated_50pct_year": y50,
                "stage": stage,
            }
        )

    points_df = pd.DataFrame(points).sort_values(["tech_theme", "year"]).reset_index(drop=True)
    summary_df = pd.DataFrame(summary).sort_values("tech_theme").reset_index(drop=True)
    return points_df, summary_df

