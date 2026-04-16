"""
Sub-divide the VIX>20 XLK OOS period further:
  - 2020-2021 (COVID era)
  - 2022-2023 (rate hiking era)
  - 2024-2026 (current regime)

Check consistency. Also test longer buffer (10d) and per-observation distribution.
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import safe_download

def find_crossings(vix_series, threshold, buffer_days=5):
    above = vix_series > threshold
    crossings = []
    for i in range(buffer_days, len(vix_series)):
        if above.iloc[i] and not above.iloc[i-buffer_days:i].any():
            crossings.append(vix_series.index[i])
    return crossings

def compute_abnormal(target_series, bench_series, entry_date, hold_days=10):
    try:
        t_idx = target_series.index.get_loc(entry_date)
        if t_idx + hold_days >= len(target_series):
            return None
        t_ret = (target_series.iloc[t_idx + hold_days] / target_series.iloc[t_idx] - 1) * 100
        b_ret = (bench_series.iloc[t_idx + hold_days] / bench_series.iloc[t_idx] - 1) * 100
        return t_ret - b_ret
    except (KeyError, IndexError):
        return None

def main():
    print('Downloading data...')
    vix = safe_download('^VIX', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    spy = safe_download('SPY', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    xlk = safe_download('XLK', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    common = vix.index.intersection(spy.index).intersection(xlk.index)
    vix = vix.loc[common]
    spy = spy.loc[common]
    xlk = xlk.loc[common]

    # VIX>20 crossings
    crossings = find_crossings(vix, 20)
    results = []
    for dt in crossings:
        abn = compute_abnormal(xlk, spy, dt, 10)
        if abn is not None:
            results.append({'date': dt, 'abn_10d': abn, 'vix_at_cross': vix.loc[dt]})
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year

    def bucket(y):
        if y < 2010: return 'a_2005-2009'
        if y < 2015: return 'b_2010-2014'
        if y < 2020: return 'c_2015-2019'
        if y < 2022: return 'd_2020-2021'
        if y < 2024: return 'e_2022-2023'
        return 'f_2024-2026'
    df['regime'] = df['year'].apply(bucket)

    print('\n=== VIX>20 XLK 10-day abnormal return by regime ===')
    for reg in sorted(df['regime'].unique()):
        sub = df[df['regime'] == reg]
        abn = sub['abn_10d']
        if len(sub) < 2:
            val = abn.iloc[0] if len(sub) == 1 else float('nan')
            print(f"{reg}: n={len(sub)} {val:+.2f}%")
            continue
        t, p = stats.ttest_1samp(abn, 0)
        direction = (abn > 0).mean()
        print(f"{reg}: n={len(sub)} mean={abn.mean():+.2f}% med={abn.median():+.2f}% dir={direction:.0%} p={p:.4f}")

    # Strip out 2020 Q1 COVID crash to see if recent effect survives
    print('\n=== OOS Excluding COVID 2020 Q1 ===')
    oos_ex_covid = df[df['date'] >= '2020-04-01']
    if len(oos_ex_covid) >= 2:
        abn = oos_ex_covid['abn_10d']
        t, p = stats.ttest_1samp(abn, 0)
        direction = (abn > 0).mean()
        print(f"OOS excl 2020Q1: n={len(oos_ex_covid)} mean={abn.mean():+.2f}% med={abn.median():+.2f}% dir={direction:.0%} p={p:.4f}")

    # Last 3 years (fresh OOS: 2023-2026)
    print('\n=== Freshest OOS 2023-2026 ===')
    fresh = df[df['date'] >= '2023-01-01']
    if len(fresh) >= 2:
        abn = fresh['abn_10d']
        t, p = stats.ttest_1samp(abn, 0)
        direction = (abn > 0).mean()
        print(f"2023+: n={len(fresh)} mean={abn.mean():+.2f}% med={abn.median():+.2f}% dir={direction:.0%} p={p:.4f}")
        print('Individual observations:')
        for _, row in fresh.iterrows():
            d = row['date'].strftime('%Y-%m-%d')
            print(f"  {d}: VIX={row['vix_at_cross']:.1f} abn_10d={row['abn_10d']:+.2f}%")

    # Print all OOS observations sorted
    print('\n=== All OOS (2020+) observations ===')
    oos = df[df['date'] >= '2020-01-01'].sort_values('date')
    for _, row in oos.iterrows():
        d = row['date'].strftime('%Y-%m-%d')
        print(f"  {d}: VIX={row['vix_at_cross']:.1f} abn_10d={row['abn_10d']:+.2f}%")

    # Current VIX status
    print('\n=== Current State (2026-04-16) ===')
    latest_date = vix.index[-1].strftime('%Y-%m-%d')
    print(f"Latest VIX close: {vix.iloc[-1]:.2f} on {latest_date}")
    print('Latest VIX history (last 15 days):')
    print(vix.tail(15).to_string())

if __name__ == '__main__':
    main()
