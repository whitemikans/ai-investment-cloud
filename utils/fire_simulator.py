from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class FireSimulationInput:
    current_age: int
    annual_income: float
    annual_expense: float
    current_assets: float
    annual_return: float = 0.07
    inflation_rate: float = 0.02
    safe_withdrawal_rate: float = 0.04
    part_time_income_annual: float = 0.0
    max_age: int = 90


EVENT_TYPE_ONE_TIME_EXPENSE = "一時支出"
EVENT_TYPE_RECURRING_EXPENSE = "継続支出"
EVENT_TYPE_INCOME_CHANGE = "収入変化"


def _event_adjustments(age: int, events_df: pd.DataFrame) -> tuple[float, float, float]:
    one_time = 0.0
    recurring_expense = 0.0
    income_delta = 0.0
    if events_df is None or events_df.empty:
        return one_time, recurring_expense, income_delta

    for _, row in events_df.iterrows():
        ev_age = int(row.get("age", age))
        ev_type = str(row.get("event_type", "")).strip()
        amount = float(row.get("amount", 0.0))
        amount_unit = str(row.get("amount_unit", "円")).strip()
        frequency = str(row.get("frequency", "年額")).strip()
        if amount_unit == "万円":
            amount *= 10_000.0
        # Backward compatibility: very small amounts are often entered as 万円 by mistake.
        elif amount_unit == "円" and 0 < abs(amount) <= 10_000:
            amount *= 10_000.0
        if frequency == "月額":
            amount *= 12.0
        duration = int(row.get("duration_years", 1))
        if ev_type == EVENT_TYPE_ONE_TIME_EXPENSE and age == ev_age:
            one_time += max(0.0, amount)
        elif ev_type == EVENT_TYPE_RECURRING_EXPENSE and ev_age <= age < (ev_age + max(1, duration)):
            recurring_expense += max(0.0, amount)
        elif ev_type == EVENT_TYPE_INCOME_CHANGE and age >= ev_age:
            income_delta += amount
    return one_time, recurring_expense, income_delta


def simulate_fire_deterministic(
    sim_input: FireSimulationInput,
    events_df: pd.DataFrame | None = None,
    pension_annual: float = 0.0,
) -> dict:
    """Run deterministic FIRE simulation and return yearly records."""
    real_return = float(sim_input.annual_return - sim_input.inflation_rate)
    years = list(range(sim_input.current_age, sim_input.max_age + 1))
    assets = float(sim_input.current_assets)

    records: list[dict] = []
    fire_age = None

    for age in years:
        one_time, recurring_expense, income_delta = _event_adjustments(age, events_df if events_df is not None else pd.DataFrame())

        annual_expense_adj = float(sim_input.annual_expense + recurring_expense)
        fire_target = max(0.0, (annual_expense_adj - sim_input.part_time_income_annual - pension_annual) / max(1e-9, sim_input.safe_withdrawal_rate))

        is_fire = fire_age is not None or assets >= fire_target
        if fire_age is None and assets >= fire_target:
            fire_age = age

        if not is_fire:
            annual_cashflow = float(sim_input.annual_income + income_delta - annual_expense_adj)
        else:
            annual_cashflow = float(sim_input.part_time_income_annual + pension_annual + income_delta - annual_expense_adj)

        annual_return_amt = assets * real_return
        next_assets = assets + annual_return_amt + annual_cashflow - one_time
        next_assets = max(0.0, float(next_assets))

        records.append(
            {
                "age": age,
                "assets": assets,
                "fire_target": fire_target,
                "annual_cashflow": annual_cashflow,
                "annual_return_amount": annual_return_amt,
                "one_time_expense": one_time,
                "recurring_expense": recurring_expense,
                "income_delta": income_delta,
                "is_fire_phase": is_fire,
            }
        )

        assets = next_assets

    df = pd.DataFrame(records)
    return {
        "fire_age": fire_age,
        "fire_achieved": fire_age is not None,
        "records": df,
    }


def simulate_fire_monte_carlo(
    sim_input: FireSimulationInput,
    events_df: pd.DataFrame | None = None,
    pension_annual: float = 0.0,
    n_sims: int = 5000,
    return_std: float = 0.15,
    seed: int = 42,
) -> dict:
    """Run Monte Carlo FIRE simulation with random annual returns."""
    rng = np.random.default_rng(seed)
    ages = np.arange(sim_input.current_age, sim_input.max_age + 1)
    n_years = len(ages)
    all_assets = np.zeros((n_sims, n_years), dtype=float)
    fire_ages: list[int | None] = []

    for sim in range(n_sims):
        assets = float(sim_input.current_assets)
        fire_age = None
        for i, age in enumerate(ages):
            one_time, recurring_expense, income_delta = _event_adjustments(int(age), events_df if events_df is not None else pd.DataFrame())
            annual_expense_adj = float(sim_input.annual_expense + recurring_expense)
            fire_target = max(0.0, (annual_expense_adj - sim_input.part_time_income_annual - pension_annual) / max(1e-9, sim_input.safe_withdrawal_rate))
            is_fire = fire_age is not None or assets >= fire_target
            if fire_age is None and assets >= fire_target:
                fire_age = int(age)

            if not is_fire:
                annual_cashflow = float(sim_input.annual_income + income_delta - annual_expense_adj)
            else:
                annual_cashflow = float(sim_input.part_time_income_annual + pension_annual + income_delta - annual_expense_adj)

            sampled_return = float(rng.normal(sim_input.annual_return, return_std))
            real_return = sampled_return - sim_input.inflation_rate
            assets = max(0.0, assets * (1.0 + real_return) + annual_cashflow - one_time)
            all_assets[sim, i] = assets

        fire_ages.append(fire_age)

    pct = {
        "p5": np.percentile(all_assets, 5, axis=0),
        "p25": np.percentile(all_assets, 25, axis=0),
        "p50": np.percentile(all_assets, 50, axis=0),
        "p75": np.percentile(all_assets, 75, axis=0),
        "p95": np.percentile(all_assets, 95, axis=0),
    }
    pct_df = pd.DataFrame({"age": ages, **pct})

    valid_fire_ages = [a for a in fire_ages if a is not None]
    fire_probability = len(valid_fire_ages) / max(1, n_sims)

    return {
        "fire_probability": fire_probability,
        "fire_age_median": float(np.median(valid_fire_ages)) if valid_fire_ages else None,
        "fire_age_p10": float(np.percentile(valid_fire_ages, 10)) if valid_fire_ages else None,
        "fire_age_p90": float(np.percentile(valid_fire_ages, 90)) if valid_fire_ages else None,
        "percentiles": pct_df,
    }


def build_what_if_scenarios(base_params: dict) -> list[dict]:
    """Return default what-if scenario parameter overrides."""
    annual_income = float(base_params.get("annual_income", 0))
    annual_expense = float(base_params.get("annual_expense", 0))
    annual_investment = max(0.0, annual_income - annual_expense)
    estimated_assets_if_started_5y_earlier = annual_investment * 5.0 * 1.25
    return [
        {"name": "ベースケース", "overrides": {}},
        {"name": "もし年収が100万円上がったら？", "overrides": {"annual_income": annual_income + 1_000_000}},
        {"name": "もし5年早く投資を始めていたら？", "overrides": {"current_assets_add": estimated_assets_if_started_5y_earlier}},
        {"name": "もし子供が2人になったら？", "overrides": {"annual_expense_add": 1_000_000}},
        {"name": "もし住宅を購入しなかったら？（賃貸継続）", "overrides": {"annual_expense_add": -600_000}},
        {"name": "もしBarista FIREを選んだら？", "overrides": {"part_time_income_annual": 1_200_000}},
        {"name": "もしリターンが5%しかなかったら？", "overrides": {"annual_return": 0.05}},
        {"name": "もし55歳で早期退職制度を使ったら？", "overrides": {"early_retire_at_55": True}},
    ]
