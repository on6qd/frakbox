"""Find dates where DXY had large daily moves and test EEM response."""
from tools.yfinance_utils import safe_download
from tools.timeseries import fetch_series
import pandas as pd
import numpy as np

# Fetch DXY and EEM
dxy = safe_download("DX-Y.NYB", start="2020-01-01", end="2026-04-09")
eem = safe_download("EEM", start="2020-01-01", end="2026-04-09")
spy = safe_download("SPY", start="2020-01-01", end="2026-04-09")

if dxy is None or eem is None or spy is None:
    print("ERROR: Could not fetch data")
    exit(1)

# Calculate daily returns
dxy_ret = dxy['Close'].pct_change()
eem_ret = eem['Close'].pct_change()
spy_ret = spy['Close'].pct_change()

# Align on common dates
common = dxy_ret.index.intersection(eem_ret.index).intersection(spy_ret.index)
dxy_ret = dxy_ret.loc[common]
eem_ret = eem_ret.loc[common]
spy_ret = spy_ret.loc[common]

# Find DXY spike days (>0.5% daily return)
spike_threshold = 0.005  # 0.5%
spike_dates = dxy_ret[dxy_ret > spike_threshold].index

print(f"Total trading days: {len(common)}")
print(f"DXY spike days (>{spike_threshold*100}%): {len(spike_dates)}")

# For each spike date, measure EEM return over next 1, 3, 5 days
results = []
eem_close = eem['Close'].loc[common]
spy_close = spy['Close'].loc[common]

for date in spike_dates:
    idx = common.get_loc(date)
    if idx + 5 >= len(common):
        continue
    
    for horizon in [1, 3, 5]:
        future_idx = idx + horizon
        eem_fwd = (eem_close.iloc[future_idx] / eem_close.iloc[idx]) - 1
        spy_fwd = (spy_close.iloc[future_idx] / spy_close.iloc[idx]) - 1
        abnormal = eem_fwd - spy_fwd
        results.append({
            'date': date.strftime('%Y-%m-%d'),
            'dxy_move': dxy_ret.iloc[idx] * 100,
            'horizon': horizon,
            'eem_return': eem_fwd * 100,
            'spy_return': spy_fwd * 100,
            'abnormal': abnormal * 100
        })

df = pd.DataFrame(results)

# Summary by horizon
print("\n=== EEM response to DXY spikes (>0.5%) ===")
for h in [1, 3, 5]:
    sub = df[df['horizon'] == h]
    from scipy import stats
    t_stat, p_val = stats.ttest_1samp(sub['abnormal'], 0)
    neg_pct = (sub['abnormal'] < 0).mean() * 100
    print(f"\n{h}d: n={len(sub)}, mean_abnormal={sub['abnormal'].mean():.3f}%, "
          f"median={sub['abnormal'].median():.3f}%, direction(neg)={neg_pct:.1f}%, "
          f"t={t_stat:.2f}, p={p_val:.4f}")

# Split into discovery (pre-2024) and validation (2024+)
print("\n=== OOS Split (pre-2024 / 2024+) ===")
for h in [1, 3, 5]:
    sub = df[df['horizon'] == h]
    is_data = sub[sub['date'] < '2024-01-01']
    oos_data = sub[sub['date'] >= '2024-01-01']
    if len(is_data) > 5 and len(oos_data) > 5:
        is_mean = is_data['abnormal'].mean()
        oos_mean = oos_data['abnormal'].mean()
        _, oos_p = stats.ttest_1samp(oos_data['abnormal'], 0)
        print(f"{h}d: IS(n={len(is_data)})={is_mean:.3f}%, OOS(n={len(oos_data)})={oos_mean:.3f}% p={oos_p:.4f}")

# Also test the reverse: DXY drops (<-0.5%) → EEM rises?
drop_dates = dxy_ret[dxy_ret < -spike_threshold].index
drop_results = []
for date in drop_dates:
    idx = common.get_loc(date)
    if idx + 5 >= len(common):
        continue
    eem_fwd = (eem_close.iloc[idx + 5] / eem_close.iloc[idx]) - 1
    spy_fwd = (spy_close.iloc[idx + 5] / spy_close.iloc[idx]) - 1
    drop_results.append(eem_fwd - spy_fwd)

if drop_results:
    arr = np.array(drop_results) * 100
    t_stat, p_val = stats.ttest_1samp(arr, 0)
    print(f"\nReverse (DXY drop >0.5%): n={len(arr)}, mean_abnormal={arr.mean():.3f}%, p={p_val:.4f}")
