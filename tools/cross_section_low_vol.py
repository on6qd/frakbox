"""
cross_section_low_vol.py — Third cross_section experiment: low-volatility anomaly.

After two momentum dead ends (60-stock and 200-stock 12-1 momentum, both
regime-dependent), test a FUNDAMENTALLY DIFFERENT cross-section factor:
realized volatility. Academic literature (Ang-Hodrick-Xing-Zhang 2006,
Frazzini-Pedersen 2014 "Betting Against Beta", Baker-Bradley-Wurgler 2011
"Benchmarks as limits to arbitrage") consistently documents that LOW-VOL
stocks produce HIGHER risk-adjusted returns than high-vol stocks, contrary
to CAPM. This is the "low-vol anomaly" and is tradable via SPLV / USMV.

Pre-registered hypothesis:
  H: On a 200-name US large/mid-cap universe, ranking by 60-day realized
     volatility (daily stdev), the lowest-vol quintile (Q0) produces a
     HIGHER Sharpe than the highest-vol quintile (Q4) over the next 21
     trading days. In mean return terms, Q0 should be close to or above Q4
     while having lower stdev.

  Success criteria (LOCKED IN before run):
    1. IS 2016-2023: Sharpe(Q0) > Sharpe(Q4) AND the difference is positive
    2. OOS 2024-2026: Same direction as IS (Sharpe Q0 > Sharpe Q4)
    3. Q0 annualized stdev MUST be lower than Q4 (sanity check — otherwise
       the sort is broken)
    4. n_rebalances >= 30 IS

Design:
- Same 200-name universe as cross_section_momentum_broader.py
- Factor: 60-day realized volatility (stdev of daily log returns)
- Rebalance: monthly (21 trading days)
- Quintiles: 5, sorted ASCENDING (Q0 = lowest vol, Q4 = highest vol)
- IS 2016-01 -> 2023-12, OOS 2024-01 -> 2026-04
- Equal-weight portfolios
- NO regime filter (the low-vol anomaly is typically regime-robust — but
  we report quintile-level stats so we can see the regime behaviour).

Note: We also report Q0-Q4 MEAN return spread for comparison with momentum
experiments. In Frazzini-Pedersen, the low-vol leg in a "beta-neutralized"
construction actually BEATS high-vol in raw mean. In our simpler un-leveraged
version the Sharpe gap is the cleaner metric.
"""
from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sps

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.yfinance_utils import get_close_prices
import db


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


def compute_realized_vol(prices: pd.DataFrame, as_of: pd.Timestamp, window: int = 60) -> pd.Series:
    """60-day realized volatility: stdev of daily log returns."""
    idx = prices.index.get_indexer([as_of], method="pad")[0]
    if idx < window:
        return pd.Series(dtype=float)
    start = idx - window
    slice_ = prices.iloc[start:idx]
    rets = np.log(slice_ / slice_.shift(1)).dropna(how="all")
    vol = rets.std(ddof=1)
    return vol.dropna()


def forward_return(prices: pd.DataFrame, start: pd.Timestamp, days: int) -> pd.Series:
    i0 = prices.index.get_indexer([start], method="pad")[0]
    i1 = i0 + days
    if i1 >= len(prices.index):
        return pd.Series(dtype=float)
    return (prices.iloc[i1] / prices.iloc[i0]) - 1.0


def run_backtest(
    prices: pd.DataFrame,
    start: str,
    end: str,
    n_quantiles: int = 5,
    hold_days: int = 21,
    vol_window: int = 60,
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

    quantile_returns = {q: [] for q in range(n_quantiles)}
    n_periods = 0

    for ri in rebal_idx:
        rebal_date = dates[ri]
        if ri < vol_window or ri + hold_days >= len(dates):
            continue
        vol = compute_realized_vol(prices, rebal_date, window=vol_window)
        if len(vol) < n_quantiles * 3:
            continue
        try:
            # qcut with ascending order: Q0 = lowest vol, Q4 = highest vol
            labels = pd.qcut(vol, n_quantiles, labels=False, duplicates="drop")
        except ValueError:
            continue
        labels = labels.dropna()
        fwd = forward_return(prices, rebal_date, hold_days)
        if fwd.empty:
            continue
        common = labels.index.intersection(fwd.index)
        labels = labels.loc[common]
        fwd = fwd.loc[common]

        for q in range(n_quantiles):
            stocks = labels[labels == q].index.tolist()
            if stocks:
                quantile_returns[q].append(float(fwd.loc[stocks].mean()))
        n_periods += 1

    periods_per_year = 252.0 / hold_days

    quantile_stats = {}
    for q in range(n_quantiles):
        arr = np.array(quantile_returns[q])
        if len(arr) < 3:
            continue
        mean = float(arr.mean())
        std = float(arr.std(ddof=1))
        quantile_stats[f"Q{q}"] = {
            "n": int(len(arr)),
            "mean_per_period_pct": mean * 100,
            "annualized_pct": mean * 100 * periods_per_year,
            "std_per_period_pct": std * 100,
            "annualized_stdev_pct": std * 100 * np.sqrt(periods_per_year),
            "sharpe_annualized": (mean / std) * np.sqrt(periods_per_year) if std > 0 else None,
        }

    # Low minus high spread (Q0 - Q4)
    if len(quantile_returns[0]) >= 3 and len(quantile_returns[n_quantiles - 1]) >= 3:
        q0 = np.array(quantile_returns[0])
        q4 = np.array(quantile_returns[n_quantiles - 1])
        n_common = min(len(q0), len(q4))
        spread = q0[:n_common] - q4[:n_common]
        t, p = sps.ttest_1samp(spread, 0)
        spread_mean = float(spread.mean())
        spread_std = float(spread.std(ddof=1))
        spread_stats = {
            "n": int(len(spread)),
            "mean_per_period_pct": spread_mean * 100,
            "annualized_pct": spread_mean * 100 * periods_per_year,
            "std_per_period_pct": spread_std * 100,
            "sharpe_annualized": (spread_mean / spread_std) * np.sqrt(periods_per_year) if spread_std > 0 else None,
            "t_stat": float(t),
            "p_value": float(p),
            "win_rate": float((spread > 0).mean()),
        }
    else:
        spread_stats = {"insufficient": True}

    # Sharpe gap: Sharpe(Q0) - Sharpe(Q4)
    sq0 = quantile_stats.get("Q0", {}).get("sharpe_annualized")
    sq4 = quantile_stats.get(f"Q{n_quantiles-1}", {}).get("sharpe_annualized")
    sharpe_gap = (sq0 - sq4) if (sq0 is not None and sq4 is not None) else None

    return {
        "start": start,
        "end": end,
        "hold_days": hold_days,
        "vol_window": vol_window,
        "n_rebalances": n_periods,
        "quantile_stats": quantile_stats,
        "low_minus_high_spread": spread_stats,
        "sharpe_gap_Q0_minus_Q4": sharpe_gap,
    }


def main():
    print(f"Universe size: {len(UNIVERSE)}", file=sys.stderr)
    print("Fetching prices 2014-06-01 -> 2026-04-12 ...", file=sys.stderr)
    prices = get_close_prices(UNIVERSE, start="2014-06-01", end="2026-04-12")
    print(f"Got {prices.shape[0]} days x {prices.shape[1]} tickers", file=sys.stderr)

    valid = prices.columns[prices.isna().mean() < 0.05].tolist()
    prices_clean = prices[valid].ffill().dropna(axis=0, how="any")
    print(f"After cleaning: {prices_clean.shape[0]} days x {prices_clean.shape[1]} tickers", file=sys.stderr)

    is_result = run_backtest(prices_clean, "2016-01-01", "2023-12-31")
    oos_result = run_backtest(prices_clean, "2024-01-01", "2026-04-11")

    # Pre-registered criterion evaluation
    def pass_crit(r):
        qs = r.get("quantile_stats", {})
        sq0 = qs.get("Q0", {}).get("sharpe_annualized")
        sq4 = qs.get("Q4", {}).get("sharpe_annualized")
        stdq0 = qs.get("Q0", {}).get("annualized_stdev_pct")
        stdq4 = qs.get("Q4", {}).get("annualized_stdev_pct")
        return {
            "sharpe_Q0_gt_Q4": (sq0 is not None and sq4 is not None and sq0 > sq4),
            "std_Q0_lt_Q4": (stdq0 is not None and stdq4 is not None and stdq0 < stdq4),
            "n_rebalances_gte_30": r.get("n_rebalances", 0) >= 30,
        }

    is_crit = pass_crit(is_result)
    oos_crit = pass_crit(oos_result)

    summary = {
        "experiment": "cross_section_low_volatility",
        "hypothesis_class": "cross_section",
        "factor": "60-day realized volatility (stdev of daily log returns)",
        "universe_size_requested": len(UNIVERSE),
        "universe_size_after_clean": prices_clean.shape[1],
        "n_quantiles": 5,
        "rebalance_days": 21,
        "long_minus_short": "Q0 (low vol) - Q4 (high vol)",
        "IS_2016_2023": is_result,
        "OOS_2024_2026": oos_result,
        "pre_registered_criteria_IS": is_crit,
        "pre_registered_criteria_OOS": oos_crit,
        "criteria_all_pass_IS": all(is_crit.values()),
        "criteria_all_pass_OOS": all(oos_crit.values()),
        "oos_confirms_is": (
            (is_result.get("sharpe_gap_Q0_minus_Q4") or 0) > 0
            and (oos_result.get("sharpe_gap_Q0_minus_Q4") or 0) > 0
        ),
        "caveats": [
            "Survivorship bias: 200 names that survived to 2026",
            "Transaction costs NOT modeled (~10% annual turnover at monthly rebalance)",
            "Equal-weight construction; Frazzini-Pedersen uses beta-neutralized leverage",
            "No sector constraints — low-vol quintile may be utility-heavy",
        ],
    }

    try:
        db.init_db()
        result_id = f"T-xslowvol-{uuid.uuid4().hex[:8]}"
        db.store_task_result(
            result_id=result_id,
            task_type="cross_section_low_volatility",
            parameters={"universe_size": len(UNIVERSE), "factor": "60d_realized_vol"},
            result=summary,
            summary=(
                f"IS sharpe_gap={is_result.get('sharpe_gap_Q0_minus_Q4')}, "
                f"OOS sharpe_gap={oos_result.get('sharpe_gap_Q0_minus_Q4')}, "
                f"pass_IS={summary['criteria_all_pass_IS']}, pass_OOS={summary['criteria_all_pass_OOS']}, "
                f"confirm={summary['oos_confirms_is']}"
            ),
        )
        summary["result_id"] = result_id
    except Exception as e:
        summary["store_error"] = str(e)

    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
