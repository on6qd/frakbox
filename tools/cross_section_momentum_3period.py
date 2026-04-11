"""
Quick robustness: split the momentum 12-1 test into three regime-sliced periods
to check the 'momentum crashes' hypothesis:
  - Pre-COVID trending: 2016-2019
  - Crisis/whiplash:    2020-2023
  - Post-whiplash:      2024-2026
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.yfinance_utils import get_close_prices
from tools.cross_section_momentum import UNIVERSE, run_backtest
import json


def main():
    # Fetch 2014 to have 1y of pre-history for the 2016-01 rebalance date's 12-1 momentum
    prices = get_close_prices(UNIVERSE, start="2014-01-01", end="2026-04-12")
    valid = prices.columns[prices.isna().mean() < 0.05].tolist()
    prices = prices[valid].ffill().dropna(axis=0, how="any")
    print(f"Universe after cleaning: {prices.shape[1]} tickers, {prices.shape[0]} days", file=sys.stderr)

    periods = {
        "pre_covid_trending_2016_2019": ("2016-01-01", "2019-12-31"),
        "crisis_whiplash_2020_2023": ("2020-01-01", "2023-12-31"),
        "post_whiplash_2024_2026": ("2024-01-01", "2026-04-11"),
    }

    out = {}
    for name, (s, e) in periods.items():
        r = run_backtest(prices, s, e)
        out[name] = {
            "n_rebalances": r.get("n_rebalances"),
            "spread_annual_pct": r.get("long_short_annualized_pct"),
            "sharpe": r.get("long_short_sharpe"),
            "t_stat": r.get("t_stat"),
            "p_value": r.get("p_value"),
            "win_rate": r.get("long_short_win_rate"),
            "Q0_annual": r.get("quantile_stats", {}).get("Q0", {}).get("annualized_pct"),
            "Q4_annual": r.get("quantile_stats", {}).get("Q4", {}).get("annualized_pct"),
            "monotonic": None,
        }
        # Check quintile monotonicity (winners > losers in strict order)
        qs = r.get("quantile_stats", {})
        if all(f"Q{i}" in qs for i in range(5)):
            series = [qs[f"Q{i}"]["annualized_pct"] for i in range(5)]
            is_mono = all(series[i] <= series[i+1] for i in range(4))
            out[name]["monotonic"] = is_mono
            out[name]["quintile_series"] = series

    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
