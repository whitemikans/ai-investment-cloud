from __future__ import annotations

import pandas as pd


def calc_pension_monthly(
    pension_type: str,
    years_joined: int,
    avg_annual_income: float,
    start_age: int,
    base_national_pension: float = 816_000,
) -> float:
    """Calculate estimated monthly pension under simplified 2026 assumptions."""
    months = max(0, min(40, int(years_joined))) * 12
    basic_annual = base_national_pension * (months / 480)

    employees_annual = 0.0
    if pension_type == "厚生年金":
        avg_monthly_income = max(0.0, float(avg_annual_income)) / 12.0
        employees_annual = avg_monthly_income * (5.481 / 1000.0) * months

    annual = basic_annual + employees_annual

    if start_age < 65:
        months_diff = (65 - start_age) * 12
        annual *= max(0.0, 1.0 - 0.004 * months_diff)
    elif start_age > 65:
        months_diff = (start_age - 65) * 12
        annual *= 1.0 + 0.007 * months_diff

    return annual / 12.0


def build_pension_table(
    pension_type: str,
    years_joined: int,
    avg_annual_income: float,
) -> pd.DataFrame:
    """Build monthly/annual pension table for start age 60-75."""
    base_monthly = calc_pension_monthly(pension_type, years_joined, avg_annual_income, 65)
    rows = []
    for age in range(60, 76):
        monthly = calc_pension_monthly(pension_type, years_joined, avg_annual_income, age)
        annual = monthly * 12
        delta = (monthly / base_monthly - 1.0) if base_monthly > 0 else 0.0
        rows.append(
            {
                "受給開始年齢": age,
                "月額(円)": monthly,
                "年額(円)": annual,
                "65歳比": delta,
            }
        )
    return pd.DataFrame(rows)


def calc_break_even_age(monthly_early: float, age_early: int, monthly_late: float, age_late: int, max_age: int = 100) -> int | None:
    """Find break-even age where delayed pension catches up cumulative total."""
    cum_early = 0.0
    cum_late = 0.0
    for age in range(min(age_early, age_late), max_age + 1):
        if age >= age_early:
            cum_early += monthly_early * 12
        if age >= age_late:
            cum_late += monthly_late * 12
        if cum_late >= cum_early:
            return age
    return None
