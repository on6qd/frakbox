"""
VIX>30 -> TLT Short Threshold Investigation
============================================
Scan hit ab0a058a claimed: VIX>30 first close -> TLT -1.30% avg over 10d (t=-2.59, p=0.0148, n=30).
Counter-intuitive: TLT expected to rally on flight-to-safety, but reportedly falls.

Full investigation:
1. Use canonical VIX>30 spike dates (30-day cluster, 2000-2026, n=54 events)
2. Compute TLT abnormal returns (TLT - SPY) at 5d, 10d, 20d
3. Pre/post-2022 regime split (rate hiking regime)
4. Discovery 2000-2019 vs OOS 2020-2026

Note: TLT listed 2002-07-30. Events before that excluded.
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import safe_download, get_close_prices

# Canonical VIX>30 spike dates from knowledge base
# Generate from ^VIX data directly (more reliable than hardcoded list)
def find_vix30_spike_dates(start='2000-01-01', end='2026-04-17', cluster_days=30):
    """First close above 30 after being below 30 for `cluster_days` calendar days."""
    vix = get_close_prices('^VIX', start=start, end=end)
    if hasattr(vix, 'columns'):
        vix = vix.iloc[:, 0]
    above = vix[vix > 30.0]
    if len(above) == 0:
        return []
    dates = []
    last_above = None  # last calendar day with VIX>30 (any)
    for d in above.index:
        if last_above is None or (d - last_above).days >= cluster_days:
            dates.append(d)
        last_above = d
    return dates


def measure_abnormal_return(target_prices, spy_prices, entry_idx, hold_days):
    """Returns (target_ret, spy_ret, abnormal_ret) as percentages."""
    end_idx = entry_idx + hold_days
    if end_idx >= len(target_prices) or end_idx >= len(spy_prices):
        return None
    t_entry = float(target_prices.iloc[entry_idx])
    t_exit = float(target_prices.iloc[end_idx])
    s_entry = float(spy_prices.iloc[entry_idx])
    s_exit = float(spy_prices.iloc[end_idx])
    t_ret = (t_exit / t_entry - 1) * 100
    s_ret = (s_exit / s_entry - 1) * 100
    return t_ret, s_ret, t_ret - s_ret


def analyze(returns, label):
    if not returns:
        print(f"  {label}: no data")
        return
    arr = np.array(returns)
    mean = arr.mean()
    median = np.median(arr)
    t, p = stats.ttest_1samp(arr, 0)
    neg_rate = (arr < 0).mean() * 100
    pos_rate = (arr > 0).mean() * 100
    print(f"  {label}: n={len(arr):3d}  mean={mean:+.2f}%  median={median:+.2f}%  t={t:+.2f}  p={p:.4f}  neg_rate={neg_rate:.0f}%")


def run():
    print("Finding VIX>30 canonical spike dates (2000-2026, 30-day cluster)...")
    spike_dates = find_vix30_spike_dates('2000-01-01', '2026-04-17', 30)
    print(f"Found {len(spike_dates)} events")

    print("Downloading TLT, SPY (2002-07-30 to 2026-04-17)...")
    tlt = get_close_prices('TLT', start='2002-07-30', end='2026-04-17')
    spy = get_close_prices('SPY', start='2002-07-30', end='2026-04-17')
    # Coerce to Series (squeeze DataFrame if needed)
    if hasattr(tlt, 'columns'):
        tlt = tlt.iloc[:, 0]
    if hasattr(spy, 'columns'):
        spy = spy.iloc[:, 0]

    # Align indexes
    common = tlt.index.intersection(spy.index)
    tlt = tlt.reindex(common)
    spy = spy.reindex(common)

    horizons = [5, 10, 20]
    buckets = {
        'all': [],
        'pre_2022': [],
        'post_2022': [],
        'discovery_2002_2019': [],
        'oos_2020_2026': [],
    }
    for h in horizons:
        for k in list(buckets.keys()):
            buckets[f"{k}_h{h}"] = []

    skipped_early = 0
    usable_events = []
    for dt in spike_dates:
        if dt < common[0]:
            skipped_early += 1
            continue
        # Find entry trading day = next trading day >= spike date + 1 (use open of next day)
        # Using close prices, the spike day close IS our entry reference
        # We enter next day's close to avoid lookahead
        idxs = common[common >= dt]
        if len(idxs) < max(horizons) + 2:
            continue
        entry_date = idxs[1]  # Next trading day after spike
        entry_idx = common.get_loc(entry_date)
        usable_events.append((dt, entry_date))

        for h in horizons:
            res = measure_abnormal_return(tlt, spy, entry_idx, h)
            if res is None:
                continue
            t_ret, s_ret, abn = res
            rec = {'spike': str(dt.date()), 'entry': str(entry_date.date()),
                   'tlt': t_ret, 'spy': s_ret, 'abnormal': abn}
            buckets[f'all_h{h}'].append(abn)
            if dt < pd.Timestamp('2022-01-01'):
                buckets[f'pre_2022_h{h}'].append(abn)
            else:
                buckets[f'post_2022_h{h}'].append(abn)
            if dt < pd.Timestamp('2020-01-01'):
                buckets[f'discovery_2002_2019_h{h}'].append(abn)
            else:
                buckets[f'oos_2020_2026_h{h}'].append(abn)

    print(f"\nUsable events: {len(usable_events)} (skipped {skipped_early} pre-TLT-listing)")
    print(f"First: {usable_events[0]}  Last: {usable_events[-1]}")

    print("\n=== TLT ABNORMAL RETURN (TLT - SPY) AFTER VIX>30 ===")
    for h in horizons:
        print(f"\nHorizon: {h}d")
        for bucket in ['all', 'pre_2022', 'post_2022', 'discovery_2002_2019', 'oos_2020_2026']:
            analyze(buckets[f'{bucket}_h{h}'], bucket)

    print("\n=== EVENT-LEVEL DETAIL (last 15) ===")
    # Quick detail table for last 15 events @10d
    h = 10
    print(f"spike | entry | TLT ret | SPY ret | abnormal (h={h}d)")
    for dt, entry_date in usable_events[-15:]:
        idx = common.get_loc(entry_date)
        res = measure_abnormal_return(tlt, spy, idx, h)
        if res:
            t_ret, s_ret, abn = res
            print(f"{dt.date()} | {entry_date.date()} | {t_ret:+6.2f}% | {s_ret:+6.2f}% | {abn:+6.2f}%")


if __name__ == '__main__':
    run()
