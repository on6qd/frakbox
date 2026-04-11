"""
cross_section_momentum.py — Cross-section 12-1 momentum factor sort on large-caps.

First concrete experiment in the `cross_section` hypothesis class (previously
untouched — zero prior work in this class). Tests the canonical Jegadeesh-Titman
winners-minus-losers spread on a large-cap US equity universe.

Design:
- Universe: ~60 large-cap US names that traded through 2015-2026 (survivorship-
  biased by construction, see caveats below).
- Factor: 12-month trailing return with the most recent month skipped
  (momentum_12_1 = return from t-252 to t-21, in trading days).
- Rebalance: monthly (21 trading days).
- Quintiles: 5 portfolios. Long Q4 (top 20% momentum) - short Q0 (bottom 20%).
- IS / OOS split: IS 2020-01 to 2023-12, OOS 2024-01 to 2026-04.
- Equal-weight portfolios.

Known caveats recorded in output:
- Survivorship bias: universe is today's survivors. The real effect is weaker
  in a point-in-time universe. Treat effect sizes as OPTIMISTIC.
- Look-ahead bias: monthly rebalance uses factor computed from t-252..t-21
  which is purely past data, so no look-ahead on signals themselves.
- Transaction costs: NOT modeled. A ~10% annual turnover charge eats most of
  the raw spread at monthly frequency.

Usage: python3 tools/cross_section_momentum.py
Output: JSON to stdout. Writes result_id to task_results table.
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sps

# Ensure repo root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.yfinance_utils import get_close_prices
import db


# 60 large-cap names that traded continuously from 2015 to 2026.
# Chosen to span sectors and include the original momentum literature's target
# (liquid US large-caps). Known survivorship bias — this is a universe of
# survivors.
UNIVERSE = [
    # Tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "ADBE", "ORCL", "CRM",
    "AVGO", "QCOM", "INTC", "CSCO", "IBM", "TXN",
    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "V", "MA", "AXP", "BLK", "C",
    # Healthcare
    "JNJ", "UNH", "PFE", "MRK", "ABBV", "LLY", "TMO", "ABT", "DHR", "BMY",
    # Consumer
    "WMT", "HD", "PG", "KO", "PEP", "MCD", "NKE", "COST", "LOW", "SBUX",
    # Energy / Industrials
    "XOM", "CVX", "COP", "CAT", "HON", "UPS", "LMT", "RTX", "BA", "DE",
    # Utilities / Telecom / REIT
    "NEE", "DUK", "T", "VZ", "AMT",
]


def compute_momentum_12_1(prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:
    """12-1 momentum: return from t-252 to t-21, skipping the most recent month."""
    target_start_idx = prices.index.get_indexer([as_of], method="pad")[0]
    if target_start_idx < 252:
        return pd.Series(dtype=float)
    # t-21 price and t-252 price
    skip_idx = target_start_idx - 21
    look_idx = target_start_idx - 252
    p_skip = prices.iloc[skip_idx]
    p_look = prices.iloc[look_idx]
    mom = (p_skip / p_look) - 1.0
    mom.name = "mom_12_1"
    return mom.dropna()


def forward_return(prices: pd.DataFrame, start: pd.Timestamp, days: int) -> pd.Series:
    """Forward return from `start` to `start + days` trading days."""
    i0 = prices.index.get_indexer([start], method="pad")[0]
    i1 = i0 + days
    if i1 >= len(prices.index):
        return pd.Series(dtype=float)
    p0 = prices.iloc[i0]
    p1 = prices.iloc[i1]
    return (p1 / p0) - 1.0


def run_backtest(prices: pd.DataFrame, start: str, end: str, n_quantiles: int = 5, hold_days: int = 21) -> dict:
    """
    Monthly-rebalance quintile sort. Returns dict with portfolio-level stats.
    """
    prices = prices.dropna(axis=1, how="any")  # only stocks with full history
    dates = prices.index
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    # Rebalance every `hold_days`
    rebal_idx = []
    i = 0
    while i < len(dates):
        if dates[i] >= start_ts and dates[i] <= end_ts:
            rebal_idx.append(i)
            i += hold_days
        else:
            i += 1

    q_returns_by_rebal = {q: [] for q in range(n_quantiles)}
    n_periods = 0
    long_short_returns = []
    rebal_dates_used = []

    for ri in rebal_idx:
        rebal_date = dates[ri]
        if ri < 252 or ri + hold_days >= len(dates):
            continue
        mom = compute_momentum_12_1(prices, rebal_date)
        if len(mom) < n_quantiles * 3:
            continue
        try:
            labels = pd.qcut(mom, n_quantiles, labels=False, duplicates="drop")
        except ValueError:
            continue
        labels = labels.dropna()
        # Forward return over next hold_days
        fwd = forward_return(prices, rebal_date, hold_days)
        if fwd.empty:
            continue

        common = labels.index.intersection(fwd.index)
        labels = labels.loc[common]
        fwd = fwd.loc[common]

        period_q_rets = {}
        for q in range(n_quantiles):
            stocks = labels[labels == q].index.tolist()
            if not stocks:
                continue
            period_q_rets[q] = float(fwd.loc[stocks].mean())
            q_returns_by_rebal[q].append(period_q_rets[q])

        if 0 in period_q_rets and (n_quantiles - 1) in period_q_rets:
            ls = period_q_rets[n_quantiles - 1] - period_q_rets[0]  # winners - losers
            long_short_returns.append(ls)
            rebal_dates_used.append(rebal_date)
            n_periods += 1

    if n_periods < 3:
        return {"error": f"Insufficient rebalances ({n_periods})"}

    ls_arr = np.array(long_short_returns)
    mean_ls = float(ls_arr.mean())
    std_ls = float(ls_arr.std(ddof=1))
    t_stat, p_value = sps.ttest_1samp(ls_arr, 0)

    # Annualize: rebalance every hold_days, so ~252/hold_days rebalances per year
    periods_per_year = 252.0 / hold_days
    ann_ls = mean_ls * periods_per_year
    ann_sharpe = (mean_ls / std_ls) * np.sqrt(periods_per_year) if std_ls > 0 else None

    quantile_stats = {}
    for q in range(n_quantiles):
        arr = np.array(q_returns_by_rebal[q])
        if len(arr) == 0:
            continue
        quantile_stats[f"Q{q}"] = {
            "mean_per_period_pct": float(arr.mean() * 100),
            "annualized_pct": float(arr.mean() * 100 * periods_per_year),
            "std_per_period_pct": float(arr.std(ddof=1) * 100) if len(arr) > 1 else None,
            "n_periods": int(len(arr)),
        }

    # Win rate (fraction of rebalances where LS > 0)
    win_rate = float((ls_arr > 0).mean())

    return {
        "n_rebalances": n_periods,
        "start": start,
        "end": end,
        "hold_days": hold_days,
        "long_short_mean_per_period_pct": mean_ls * 100,
        "long_short_annualized_pct": ann_ls * 100,
        "long_short_std_per_period_pct": std_ls * 100,
        "long_short_sharpe": ann_sharpe,
        "long_short_win_rate": win_rate,
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "significant_5pct": bool(p_value < 0.05),
        "quantile_stats": quantile_stats,
        "first_rebal": str(rebal_dates_used[0].date()) if rebal_dates_used else None,
        "last_rebal": str(rebal_dates_used[-1].date()) if rebal_dates_used else None,
    }


def main():
    print("Fetching prices for", len(UNIVERSE), "tickers 2019-01-01 to 2026-04-12...", file=sys.stderr)
    # Need a year of history before IS start (2020-01) for 12-1 momentum
    prices = get_close_prices(UNIVERSE, start="2019-01-01", end="2026-04-12")
    print(f"Got {prices.shape[0]} days x {prices.shape[1]} tickers", file=sys.stderr)

    # Drop any ticker missing too much data
    valid = prices.columns[prices.isna().mean() < 0.05].tolist()
    prices = prices[valid].ffill().dropna(axis=0, how="any")
    print(f"After cleaning: {prices.shape[0]} days x {prices.shape[1]} tickers", file=sys.stderr)

    is_result = run_backtest(prices, "2020-01-01", "2023-12-31")
    oos_result = run_backtest(prices, "2024-01-01", "2026-04-11")

    summary = {
        "experiment": "cross_section_momentum_12_1_largecap",
        "hypothesis_class": "cross_section",
        "universe_size": len(valid),
        "universe_sample": valid[:10],
        "factor": "momentum_12_1 (t-252 to t-21)",
        "rebalance_days": 21,
        "n_quantiles": 5,
        "long_minus_short": "Q4 - Q0 (winners - losers)",
        "IS": is_result,
        "OOS": oos_result,
        "caveats": [
            "Survivorship bias: universe is 2026 survivors of 2020 starting list",
            "Transaction costs NOT subtracted (~10% annual turnover impact)",
            "Equal-weight, no sector constraints",
            "Monthly rebalance at close-to-close, no slippage modeled",
        ],
    }

    # Store in task_results
    try:
        db.init_db()
        result_id = f"T-xsmom-{uuid.uuid4().hex[:8]}"
        db.store_task_result(
            result_id=result_id,
            task_type="cross_section_momentum",
            parameters={"universe": UNIVERSE, "factor": "momentum_12_1"},
            result=summary,
            summary=f"IS Sharpe={is_result.get('long_short_sharpe')}, OOS Sharpe={oos_result.get('long_short_sharpe')}",
        )
        summary["result_id"] = result_id
    except Exception as e:
        summary["store_error"] = str(e)

    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
