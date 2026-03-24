"""
Seasonal pattern analyzer — monthly, day-of-week, and calendar anomalies.

Uses existing yfinance data via safe_download. No new dependencies or API keys.

Usage:
    from tools.seasonal_analyzer import monthly_seasonality, sell_in_may_backtest
    table = monthly_seasonality("SPY", years=20)
    result = sell_in_may_backtest("SPY", years=20)
"""

import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from scipy.stats import ttest_1samp, ttest_rel

try:
    from tools.yfinance_utils import safe_download
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tools.yfinance_utils import safe_download

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def monthly_seasonality(symbol, years=20):
    """Compute average return by calendar month with significance tests.

    Returns DataFrame indexed by month name with columns:
    mean_pct, median_pct, std_pct, n, t_stat, p_value, pct_positive
    """
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    df = safe_download(symbol, start=start, end=end)
    if df.empty or len(df) < 60:
        return pd.DataFrame()

    # Monthly returns: resample to month-end, compute return per month
    monthly = df["Close"].resample("ME").last()
    monthly_ret = monthly.pct_change().dropna() * 100  # in percent

    rows = []
    for month in range(1, 13):
        rets = monthly_ret[monthly_ret.index.month == month]
        if len(rets) < 3:
            rows.append({
                "month": MONTH_NAMES[month - 1],
                "mean_pct": None, "median_pct": None, "std_pct": None,
                "n": len(rets), "t_stat": None, "p_value": None, "pct_positive": None,
            })
            continue
        t_stat, p_val = ttest_1samp(rets, 0)
        rows.append({
            "month": MONTH_NAMES[month - 1],
            "mean_pct": round(rets.mean(), 3),
            "median_pct": round(rets.median(), 3),
            "std_pct": round(rets.std(), 3),
            "n": len(rets),
            "t_stat": round(t_stat, 3),
            "p_value": round(p_val, 4),
            "pct_positive": round((rets > 0).mean() * 100, 1),
        })

    result = pd.DataFrame(rows).set_index("month")
    print(f"[seasonal] {symbol} monthly seasonality ({years}yr, {len(monthly_ret)} months):", file=sys.stderr)
    sig = result[result["p_value"].notna() & (result["p_value"] < 0.05)]
    if not sig.empty:
        for idx, row in sig.iterrows():
            print(f"  {idx}: {row['mean_pct']:+.2f}% (p={row['p_value']:.3f})", file=sys.stderr)
    else:
        print("  No statistically significant months (p<0.05)", file=sys.stderr)
    return result


def day_of_week_effect(symbol, years=20):
    """Compute average return by day of week with significance tests.

    Returns DataFrame indexed by weekday name with same columns as monthly_seasonality.
    """
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    df = safe_download(symbol, start=start, end=end)
    if df.empty or len(df) < 60:
        return pd.DataFrame()

    daily_ret = df["Close"].pct_change().dropna() * 100

    rows = []
    for dow in range(5):  # 0=Monday to 4=Friday
        rets = daily_ret[daily_ret.index.dayofweek == dow]
        if len(rets) < 10:
            rows.append({
                "day": DAY_NAMES[dow],
                "mean_pct": None, "median_pct": None, "std_pct": None,
                "n": len(rets), "t_stat": None, "p_value": None, "pct_positive": None,
            })
            continue
        t_stat, p_val = ttest_1samp(rets, 0)
        rows.append({
            "day": DAY_NAMES[dow],
            "mean_pct": round(rets.mean(), 4),
            "median_pct": round(rets.median(), 4),
            "std_pct": round(rets.std(), 4),
            "n": len(rets),
            "t_stat": round(t_stat, 3),
            "p_value": round(p_val, 4),
            "pct_positive": round((rets > 0).mean() * 100, 1),
        })

    return pd.DataFrame(rows).set_index("day")


def sell_in_may_backtest(symbol, years=20):
    """Compare Nov-Apr ('winter') vs May-Oct ('summer') returns.

    Returns dict with average returns, win rates, and paired t-test.
    """
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    df = safe_download(symbol, start=start, end=end)
    if df.empty:
        return {"error": "No data"}

    close = df["Close"]
    yearly_detail = []
    winters = []
    summers = []

    # Get unique years in data
    all_years = sorted(close.index.year.unique())

    for yr in all_years:
        # Winter = Nov(yr-1) through Apr(yr)
        winter_start = f"{yr - 1}-11-01"
        winter_end = f"{yr}-04-30"
        # Summer = May(yr) through Oct(yr)
        summer_start = f"{yr}-05-01"
        summer_end = f"{yr}-10-31"

        w = close[winter_start:winter_end]
        s = close[summer_start:summer_end]

        if len(w) < 20 or len(s) < 20:
            continue

        w_ret = (w.iloc[-1] / w.iloc[0] - 1) * 100
        s_ret = (s.iloc[-1] / s.iloc[0] - 1) * 100
        winters.append(w_ret)
        summers.append(s_ret)
        yearly_detail.append({
            "year": yr,
            "winter_pct": round(w_ret, 2),
            "summer_pct": round(s_ret, 2),
            "spread_pct": round(w_ret - s_ret, 2),
        })

    if len(winters) < 3:
        return {"error": f"Only {len(winters)} complete year-pairs"}

    winters = np.array(winters)
    summers = np.array(summers)
    t_stat, p_val = ttest_rel(winters, summers)

    return {
        "symbol": symbol,
        "years_analyzed": len(winters),
        "winter_avg_return_pct": round(winters.mean(), 2),
        "summer_avg_return_pct": round(summers.mean(), 2),
        "winter_win_rate": round((winters > 0).mean() * 100, 1),
        "summer_win_rate": round((summers > 0).mean() * 100, 1),
        "spread_pct": round((winters - summers).mean(), 2),
        "t_stat": round(t_stat, 3),
        "p_value": round(p_val, 4),
        "yearly_detail": yearly_detail,
    }


def january_effect(years=20):
    """Compare small-cap (IWM) vs large-cap (SPY) January returns.

    Tests the hypothesis that small-caps outperform large-caps in January.
    Returns dict with average returns, spread, and t-test.
    """
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    iwm = safe_download("IWM", start=start, end=end)
    spy = safe_download("SPY", start=start, end=end)
    if iwm.empty or spy.empty:
        return {"error": "Could not download IWM or SPY data"}

    yearly_detail = []
    spreads = []
    iwm_jans = []
    spy_jans = []

    all_years = sorted(set(iwm.index.year) & set(spy.index.year))
    for yr in all_years:
        jan_start = f"{yr}-01-01"
        jan_end = f"{yr}-01-31"
        i = iwm["Close"][jan_start:jan_end]
        s = spy["Close"][jan_start:jan_end]
        if len(i) < 10 or len(s) < 10:
            continue
        i_ret = (i.iloc[-1] / i.iloc[0] - 1) * 100
        s_ret = (s.iloc[-1] / s.iloc[0] - 1) * 100
        spread = i_ret - s_ret
        iwm_jans.append(i_ret)
        spy_jans.append(s_ret)
        spreads.append(spread)
        yearly_detail.append({
            "year": yr,
            "iwm_jan_pct": round(i_ret, 2),
            "spy_jan_pct": round(s_ret, 2),
            "spread_pct": round(spread, 2),
        })

    if len(spreads) < 3:
        return {"error": f"Only {len(spreads)} January pairs"}

    spreads = np.array(spreads)
    t_stat, p_val = ttest_1samp(spreads, 0)

    return {
        "years_analyzed": len(spreads),
        "iwm_jan_avg_pct": round(np.mean(iwm_jans), 2),
        "spy_jan_avg_pct": round(np.mean(spy_jans), 2),
        "spread_avg_pct": round(spreads.mean(), 2),
        "spread_positive_rate": round((spreads > 0).mean() * 100, 1),
        "t_stat": round(t_stat, 3),
        "p_value": round(p_val, 4),
        "yearly_detail": yearly_detail,
    }


if __name__ == "__main__":
    print("=== Seasonal Analyzer Test ===\n")

    print("--- SPY Monthly Seasonality (20yr) ---")
    table = monthly_seasonality("SPY", years=20)
    if not table.empty:
        print(table.to_string())

    print("\n--- SPY Day-of-Week Effect (20yr) ---")
    dow = day_of_week_effect("SPY", years=20)
    if not dow.empty:
        print(dow.to_string())

    print("\n--- Sell in May (SPY, 20yr) ---")
    sim = sell_in_may_backtest("SPY", years=20)
    for k, v in sim.items():
        if k != "yearly_detail":
            print(f"  {k}: {v}")

    print("\n--- January Effect (IWM vs SPY, 20yr) ---")
    jan = january_effect(years=20)
    for k, v in jan.items():
        if k != "yearly_detail":
            print(f"  {k}: {v}")
