"""
cross_section_momentum_broader.py — Second experiment in cross_section class.

Addresses the three weaknesses identified in the first experiment (60-stock
universe, tools/cross_section_momentum.py — DEAD END, regime sign-flip):
  (a) Broader universe (~200 large+mid cap names, not 60 concentrated names)
  (b) Regime filter on SPY 6-month drawdown (skip momentum in crashes —
      Daniel-Moskowitz momentum crashes pattern)
  (c) Report winners-minus-losers spread BOTH unconditionally AND split by
      regime, so we can see whether the sign-flip is regime-driven.

Hypothesis (pre-registered before touching data):
  H: On a broader 200-name US large/mid-cap universe, a 12-1 momentum
     quintile sort (winners Q4 - losers Q0, monthly rebalance) produces
     a POSITIVE annualized spread in the IS window 2016-2023, confirmed
     by the same sign in the OOS window 2024-2026, WHEN CONDITIONED on
     non-crash regime (SPY 6m return > -10%).

  Success criteria (LOCKED IN before run):
    1. IS (non-crash regime) Sharpe > 0.3 and p<0.10
    2. OOS (non-crash regime) SAME SIGN as IS (positive)
    3. Crash regime spread can be anything (we just report it)
    4. n_rebalances >= 30 per regime to avoid small-sample noise

If these fail, record as second cross_section dead_end.

Design:
- Universe: ~200 S&P 500 names chosen for liquidity + sector balance
  (still survivorship biased — noted in caveats)
- Factor: 12-1 momentum (t-252 to t-21), same as first experiment
- Rebalance: monthly (21 trading days)
- Quintiles: 5, long Q4 - short Q0
- IS: 2016-01 -> 2023-12 (longer window than first experiment)
- OOS: 2024-01 -> 2026-04

Regime definition:
- At each rebalance date, compute SPY 6m trailing return (126 trading days).
- If SPY_6m < -10%: CRASH regime (skip or report separately)
- Else: NON-CRASH regime (trade as usual)

This captures the key Daniel-Moskowitz finding: momentum crashes during
market recoveries, because the "losers" during the crash become the
"winners" during the recovery, flipping the spread negative.
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


# Broader S&P 500 universe — ~200 names across sectors. Still survivorship
# biased (they exist in 2026). Sector tagging is for reference only; we
# don't use it in the backtest.
UNIVERSE = [
    # Tech / Software (35)
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "ADBE", "ORCL",
    "CRM", "AVGO", "QCOM", "INTC", "CSCO", "IBM", "TXN", "AMD", "MU", "AMAT",
    "LRCX", "KLAC", "NOW", "INTU", "ADSK", "CDNS", "SNPS", "PANW", "FTNT",
    "MSI", "ROP", "TEL", "APH", "GLW", "FIS", "PAYX",
    # Financials (30)
    "JPM", "BAC", "WFC", "GS", "MS", "V", "MA", "AXP", "BLK", "C", "SCHW",
    "USB", "PNC", "TFC", "COF", "BK", "STT", "ICE", "CME", "SPGI", "MCO",
    "AON", "MMC", "PGR", "TRV", "AIG", "ALL", "MET", "PRU", "AFL",
    # Healthcare (30)
    "JNJ", "UNH", "PFE", "MRK", "ABBV", "LLY", "TMO", "ABT", "DHR", "BMY",
    "AMGN", "GILD", "CVS", "CI", "ELV", "HUM", "ISRG", "SYK", "MDT", "BDX",
    "BSX", "EW", "REGN", "VRTX", "ZTS", "BIIB", "IDXX", "IQV", "A", "MCK",
    # Consumer Discretionary / Staples (35)
    "WMT", "HD", "PG", "KO", "PEP", "MCD", "NKE", "COST", "LOW", "SBUX",
    "TGT", "TJX", "DG", "DLTR", "KR", "CL", "KMB", "MDLZ", "GIS", "K",
    "HSY", "STZ", "MO", "PM", "CLX", "CHD", "SYY", "ADM", "BF-B",
    "YUM", "CMG", "MAR", "HLT", "BKNG", "ABNB",
    # Energy / Industrials (35)
    "XOM", "CVX", "COP", "EOG", "SLB", "PSX", "VLO", "MPC", "OXY", "HES",
    "CAT", "HON", "UPS", "LMT", "RTX", "BA", "DE", "GE", "MMM", "EMR",
    "ETN", "ITW", "ROK", "PH", "FDX", "NSC", "UNP", "CSX", "WM", "RSG",
    "LHX", "GD", "NOC", "URI", "CARR",
    # Utilities / Telecom / REITs (25)
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "XEL", "SRE", "PEG", "ED",
    "T", "VZ", "TMUS", "CMCSA", "CHTR",
    "AMT", "PLD", "CCI", "EQIX", "PSA", "SPG", "O", "WELL", "DLR", "EXR",
]


def compute_momentum_12_1(prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:
    target_idx = prices.index.get_indexer([as_of], method="pad")[0]
    if target_idx < 252:
        return pd.Series(dtype=float)
    skip_idx = target_idx - 21
    look_idx = target_idx - 252
    p_skip = prices.iloc[skip_idx]
    p_look = prices.iloc[look_idx]
    mom = (p_skip / p_look) - 1.0
    return mom.dropna()


def compute_spy_6m_return(spy: pd.Series, as_of: pd.Timestamp) -> float:
    idx = spy.index.get_indexer([as_of], method="pad")[0]
    if idx < 126:
        return float("nan")
    return float(spy.iloc[idx] / spy.iloc[idx - 126] - 1.0)


def forward_return(prices: pd.DataFrame, start: pd.Timestamp, days: int) -> pd.Series:
    i0 = prices.index.get_indexer([start], method="pad")[0]
    i1 = i0 + days
    if i1 >= len(prices.index):
        return pd.Series(dtype=float)
    return (prices.iloc[i1] / prices.iloc[i0]) - 1.0


def run_backtest(
    prices: pd.DataFrame,
    spy: pd.Series,
    start: str,
    end: str,
    n_quantiles: int = 5,
    hold_days: int = 21,
    crash_threshold: float = -0.10,
) -> dict:
    prices = prices.dropna(axis=1, how="any")
    dates = prices.index
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    rebal_idx = []
    i = 0
    while i < len(dates):
        if dates[i] >= start_ts and dates[i] <= end_ts:
            rebal_idx.append(i)
            i += hold_days
        else:
            i += 1

    # Track regime-split LS returns
    ls_noncrash = []
    ls_crash = []
    all_ls = []
    quantile_all = {q: [] for q in range(n_quantiles)}

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
        fwd = forward_return(prices, rebal_date, hold_days)
        if fwd.empty:
            continue
        common = labels.index.intersection(fwd.index)
        labels = labels.loc[common]
        fwd = fwd.loc[common]

        period_q = {}
        for q in range(n_quantiles):
            stocks = labels[labels == q].index.tolist()
            if stocks:
                period_q[q] = float(fwd.loc[stocks].mean())
                quantile_all[q].append(period_q[q])

        if 0 not in period_q or (n_quantiles - 1) not in period_q:
            continue
        ls = period_q[n_quantiles - 1] - period_q[0]
        all_ls.append(ls)
        spy_6m = compute_spy_6m_return(spy, rebal_date)
        if np.isnan(spy_6m):
            continue
        if spy_6m < crash_threshold:
            ls_crash.append(ls)
        else:
            ls_noncrash.append(ls)

    def stats_block(arr_list: list[float]) -> dict:
        if len(arr_list) < 3:
            return {"n": len(arr_list), "insufficient": True}
        arr = np.array(arr_list)
        periods_per_year = 252.0 / hold_days
        mean = float(arr.mean())
        std = float(arr.std(ddof=1))
        t, p = sps.ttest_1samp(arr, 0)
        return {
            "n": int(len(arr)),
            "mean_per_period_pct": mean * 100,
            "annualized_pct": mean * 100 * periods_per_year,
            "std_per_period_pct": std * 100,
            "sharpe_annualized": (mean / std) * np.sqrt(periods_per_year) if std > 0 else None,
            "t_stat": float(t),
            "p_value": float(p),
            "win_rate": float((arr > 0).mean()),
        }

    # Quintile summary across all rebalances
    q_summary = {}
    periods_per_year = 252.0 / hold_days
    for q in range(n_quantiles):
        if quantile_all[q]:
            arr = np.array(quantile_all[q])
            q_summary[f"Q{q}"] = {
                "annualized_pct": float(arr.mean() * 100 * periods_per_year),
                "n": int(len(arr)),
            }

    return {
        "start": start,
        "end": end,
        "hold_days": hold_days,
        "all_regimes": stats_block(all_ls),
        "noncrash_regime": stats_block(ls_noncrash),
        "crash_regime": stats_block(ls_crash),
        "quantile_annualized": q_summary,
    }


def main():
    print(f"Universe size: {len(UNIVERSE)}", file=sys.stderr)
    print("Fetching prices 2014-06-01 -> 2026-04-12 ...", file=sys.stderr)
    # 1.5 years of history before IS start for 12-1 momentum computation
    prices = get_close_prices(UNIVERSE, start="2014-06-01", end="2026-04-12")
    print(f"Got {prices.shape[0]} days x {prices.shape[1]} tickers", file=sys.stderr)

    valid = prices.columns[prices.isna().mean() < 0.05].tolist()
    prices_clean = prices[valid].ffill().dropna(axis=0, how="any")
    print(f"After cleaning: {prices_clean.shape[0]} days x {prices_clean.shape[1]} tickers", file=sys.stderr)

    # Fetch SPY for regime filter
    spy_df = get_close_prices(["SPY"], start="2014-06-01", end="2026-04-12")
    spy = spy_df["SPY"].dropna()
    print(f"SPY: {len(spy)} days", file=sys.stderr)

    is_result = run_backtest(prices_clean, spy, "2016-01-01", "2023-12-31")
    oos_result = run_backtest(prices_clean, spy, "2024-01-01", "2026-04-11")

    summary = {
        "experiment": "cross_section_momentum_12_1_broader",
        "hypothesis_class": "cross_section",
        "universe_size_requested": len(UNIVERSE),
        "universe_size_after_clean": prices_clean.shape[1],
        "factor": "momentum_12_1 (t-252 to t-21)",
        "rebalance_days": 21,
        "n_quantiles": 5,
        "regime_filter": "SPY 6m trailing return < -10% = CRASH",
        "IS_2016_2023": is_result,
        "OOS_2024_2026": oos_result,
        "pre_registered_success_criteria": {
            "IS_noncrash_sharpe_gt_0.3": None,
            "IS_noncrash_p_lt_0.10": None,
            "OOS_noncrash_same_sign_positive": None,
            "n_rebalances_per_regime_gte_30": None,
        },
        "caveats": [
            "Survivorship bias: 2026 survivors chosen for universe",
            "Transaction costs NOT modeled (~10% annual at monthly turnover)",
            "Equal-weight, no sector constraints",
            "Regime filter uses SPY 6m RETURN not drawdown from peak; simpler but similar",
        ],
    }

    # Evaluate pre-registered criteria
    crit = summary["pre_registered_success_criteria"]
    is_nc = is_result.get("noncrash_regime", {})
    oos_nc = oos_result.get("noncrash_regime", {})
    crit["IS_noncrash_sharpe_gt_0.3"] = bool(
        (is_nc.get("sharpe_annualized") or 0) > 0.3
    )
    crit["IS_noncrash_p_lt_0.10"] = bool((is_nc.get("p_value") or 1.0) < 0.10)
    crit["OOS_noncrash_same_sign_positive"] = bool(
        (oos_nc.get("mean_per_period_pct") or 0) > 0
    )
    crit["n_rebalances_per_regime_gte_30"] = bool(
        is_nc.get("n", 0) >= 30
    )
    summary["all_criteria_pass"] = all(v is True for v in crit.values())

    try:
        db.init_db()
        result_id = f"T-xsmom2-{uuid.uuid4().hex[:8]}"
        db.store_task_result(
            result_id=result_id,
            task_type="cross_section_momentum_broader",
            parameters={"universe_size": len(UNIVERSE), "factor": "momentum_12_1",
                        "regime_filter": "SPY_6m_lt_-10pct"},
            result=summary,
            summary=(
                f"IS nc sharpe={is_nc.get('sharpe_annualized')}, "
                f"OOS nc annualized={oos_nc.get('annualized_pct')}, "
                f"pass={summary['all_criteria_pass']}"
            ),
        )
        summary["result_id"] = result_id
    except Exception as e:
        summary["store_error"] = str(e)

    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
