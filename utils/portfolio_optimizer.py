from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import yfinance as yf
try:
    from scipy.optimize import minimize
    HAS_SCIPY = True
except Exception:
    minimize = None
    HAS_SCIPY = False


ANNUAL_TRADING_DAYS = 252


@dataclass
class OptimizationResult:
    weights: dict[str, float]
    expected_return: float
    risk: float
    sharpe: float


def fetch_price_history(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """Fetch adjusted close prices from yfinance."""
    clean = [t.strip().upper() for t in tickers if t.strip()]
    if not clean:
        return pd.DataFrame()

    df = yf.download(
        clean,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
    )
    if df.empty:
        return pd.DataFrame()

    if "Close" in df.columns:
        close_df = df["Close"].copy()
    else:
        close_df = df.copy()

    if isinstance(close_df, pd.Series):
        close_df = close_df.to_frame(name=clean[0])

    close_df = close_df.dropna(how="all")
    close_df = close_df.ffill().dropna(how="any")
    return close_df


def build_return_stats(price_df: pd.DataFrame) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    """Return daily returns, annualized mean returns and annualized covariance."""
    returns = price_df.pct_change().dropna(how="any")
    mean_returns = returns.mean() * ANNUAL_TRADING_DAYS
    cov_matrix = returns.cov() * ANNUAL_TRADING_DAYS
    return returns, mean_returns, cov_matrix


def _portfolio_return(weights: np.ndarray, mean_returns: np.ndarray) -> float:
    return float(np.dot(weights, mean_returns))


def _portfolio_risk(weights: np.ndarray, cov_matrix: np.ndarray) -> float:
    return float(np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights))))


def _weights_to_dict(tickers: list[str], weights: np.ndarray) -> dict[str, float]:
    return {ticker: float(weight) for ticker, weight in zip(tickers, weights)}


def _build_result(
    tickers: list[str],
    weights: np.ndarray,
    mean_returns: np.ndarray,
    cov_matrix: np.ndarray,
    risk_free_rate: float = 0.005,
) -> OptimizationResult:
    exp_return = _portfolio_return(weights, mean_returns)
    risk = _portfolio_risk(weights, cov_matrix)
    sharpe = (exp_return - risk_free_rate) / risk if risk > 0 else 0.0
    return OptimizationResult(
        weights=_weights_to_dict(tickers, weights),
        expected_return=exp_return,
        risk=risk,
        sharpe=float(sharpe),
    )


def _random_weights(n_assets: int, n_samples: int = 12000) -> np.ndarray:
    w = np.random.random((n_samples, n_assets))
    w = w / w.sum(axis=1, keepdims=True)
    return w


def _base_constraints(n_assets: int) -> tuple[list[dict], tuple[tuple[float, float], ...], np.ndarray]:
    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
    bounds = tuple((0.0, 1.0) for _ in range(n_assets))
    x0 = np.array([1.0 / n_assets] * n_assets)
    return constraints, bounds, x0


def minimize_risk(
    tickers: list[str],
    mean_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    target_return: float,
    risk_free_rate: float = 0.005,
) -> OptimizationResult | None:
    """Find minimum-risk portfolio under target return constraint."""
    if len(tickers) == 0:
        return None

    mean_vals = mean_returns.loc[tickers].values
    cov_vals = cov_matrix.loc[tickers, tickers].values
    constraints, bounds, x0 = _base_constraints(len(tickers))
    constraints.append({"type": "eq", "fun": lambda w: np.dot(w, mean_vals) - target_return})

    if HAS_SCIPY:
        result = minimize(
            lambda w: np.dot(w.T, np.dot(cov_vals, w)),
            x0=x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
        )
        if not result.success:
            return None
        return _build_result(tickers, result.x, mean_vals, cov_vals, risk_free_rate=risk_free_rate)

    # Fallback without scipy: random search around target return
    samples = _random_weights(len(tickers), n_samples=15000)
    rets = samples @ mean_vals
    vars_ = np.einsum("ij,jk,ik->i", samples, cov_vals, samples)
    mask = np.abs(rets - target_return) <= 0.003
    if not np.any(mask):
        idx = int(np.argmin(np.abs(rets - target_return)))
        return _build_result(tickers, samples[idx], mean_vals, cov_vals, risk_free_rate=risk_free_rate)
    candidate_idx = np.where(mask)[0]
    idx = int(candidate_idx[np.argmin(vars_[candidate_idx])])
    return _build_result(tickers, samples[idx], mean_vals, cov_vals, risk_free_rate=risk_free_rate)


def find_min_variance_portfolio(
    tickers: list[str],
    mean_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    risk_free_rate: float = 0.005,
) -> OptimizationResult | None:
    """Find minimum-variance portfolio."""
    if len(tickers) == 0:
        return None

    mean_vals = mean_returns.loc[tickers].values
    cov_vals = cov_matrix.loc[tickers, tickers].values
    constraints, bounds, x0 = _base_constraints(len(tickers))

    if HAS_SCIPY:
        result = minimize(
            lambda w: np.dot(w.T, np.dot(cov_vals, w)),
            x0=x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
        )
        if not result.success:
            return None
        return _build_result(tickers, result.x, mean_vals, cov_vals, risk_free_rate=risk_free_rate)

    samples = _random_weights(len(tickers), n_samples=15000)
    vars_ = np.einsum("ij,jk,ik->i", samples, cov_vals, samples)
    idx = int(np.argmin(vars_))
    return _build_result(tickers, samples[idx], mean_vals, cov_vals, risk_free_rate=risk_free_rate)


def find_max_sharpe_portfolio(
    tickers: list[str],
    mean_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    risk_free_rate: float = 0.005,
) -> OptimizationResult | None:
    """Find maximum Sharpe-ratio portfolio."""
    if len(tickers) == 0:
        return None

    mean_vals = mean_returns.loc[tickers].values
    cov_vals = cov_matrix.loc[tickers, tickers].values
    constraints, bounds, x0 = _base_constraints(len(tickers))

    def neg_sharpe(weights: np.ndarray) -> float:
        risk = _portfolio_risk(weights, cov_vals)
        if risk <= 0:
            return 1e9
        ret = _portfolio_return(weights, mean_vals)
        return -((ret - risk_free_rate) / risk)

    if HAS_SCIPY:
        result = minimize(
            neg_sharpe,
            x0=x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
        )
        if not result.success:
            return None
        return _build_result(tickers, result.x, mean_vals, cov_vals, risk_free_rate=risk_free_rate)

    samples = _random_weights(len(tickers), n_samples=20000)
    rets = samples @ mean_vals
    risks = np.sqrt(np.einsum("ij,jk,ik->i", samples, cov_vals, samples))
    sharpes = np.where(risks > 0, (rets - risk_free_rate) / risks, -1e9)
    idx = int(np.argmax(sharpes))
    return _build_result(tickers, samples[idx], mean_vals, cov_vals, risk_free_rate=risk_free_rate)


def find_risk_parity_portfolio(
    tickers: list[str],
    mean_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    risk_free_rate: float = 0.005,
) -> OptimizationResult | None:
    """Approximate risk parity by inverse-volatility weighting."""
    if len(tickers) == 0:
        return None
    cov_vals = cov_matrix.loc[tickers, tickers].values
    mean_vals = mean_returns.loc[tickers].values
    vol = np.sqrt(np.diag(cov_vals))
    inv_vol = np.where(vol > 0, 1.0 / vol, 0.0)
    if inv_vol.sum() == 0:
        return None
    weights = inv_vol / inv_vol.sum()
    return _build_result(tickers, weights, mean_vals, cov_vals, risk_free_rate=risk_free_rate)


def generate_efficient_frontier(
    tickers: list[str],
    mean_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    points: int = 50,
    risk_free_rate: float = 0.005,
) -> pd.DataFrame:
    """Generate efficient frontier points by sweeping target returns."""
    if len(tickers) == 0:
        return pd.DataFrame()

    mean_vals = mean_returns.loc[tickers]
    ret_min = float(mean_vals.min())
    ret_max = float(mean_vals.max())
    targets = np.linspace(ret_min, ret_max, points)

    rows: list[dict] = []
    for target in targets:
        optimized = minimize_risk(
            tickers=tickers,
            mean_returns=mean_returns,
            cov_matrix=cov_matrix,
            target_return=float(target),
            risk_free_rate=risk_free_rate,
        )
        if optimized is None:
            continue
        rows.append(
            {
                "return": optimized.expected_return,
                "risk": optimized.risk,
                "sharpe": optimized.sharpe,
                "weights": optimized.weights,
            }
        )
    return pd.DataFrame(rows)


def generate_random_portfolios(
    tickers: list[str],
    mean_returns: pd.Series,
    cov_matrix: pd.DataFrame,
    n_samples: int = 10000,
    risk_free_rate: float = 0.005,
) -> pd.DataFrame:
    """Generate random long-only portfolios for background scatter."""
    if len(tickers) == 0:
        return pd.DataFrame()

    mean_vals = mean_returns.loc[tickers].values
    cov_vals = cov_matrix.loc[tickers, tickers].values
    n_assets = len(tickers)
    rows: list[dict] = []
    for _ in range(n_samples):
        w = np.random.random(n_assets)
        w = w / w.sum()
        ret = _portfolio_return(w, mean_vals)
        risk = _portfolio_risk(w, cov_vals)
        sharpe = (ret - risk_free_rate) / risk if risk > 0 else 0.0
        rows.append({"return": ret, "risk": risk, "sharpe": sharpe, "weights": _weights_to_dict(tickers, w)})
    return pd.DataFrame(rows)


def interpolate_frontier_by_risk_tolerance(frontier_df: pd.DataFrame, risk_tolerance: int) -> dict | None:
    """Pick a point on frontier based on risk tolerance (1-10)."""
    if frontier_df.empty:
        return None
    sorted_df = frontier_df.sort_values("risk").reset_index(drop=True)
    alpha = (max(1, min(10, risk_tolerance)) - 1) / 9.0
    index = int(round(alpha * (len(sorted_df) - 1)))
    row = sorted_df.iloc[index]
    return {
        "return": float(row["return"]),
        "risk": float(row["risk"]),
        "sharpe": float(row["sharpe"]),
        "weights": dict(row["weights"]),
    }
