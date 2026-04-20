"""Audit: is the +6.3% post-VIX>30 SPY 20d return in pre-sold subset a real edge,
or merely regression-to-mean for any pre-sold period?

Design:
- Treatment: VIX>30 first-cluster close AND SPY 20d_pre < -5%, excl. COVID 2020
- Control:   SPY 20d_pre < -5%, excl. COVID 2020 AND excl. VIX>30 trigger days
- Outcome:   SPY raw 20d forward return (matching original analysis frame)
- Test:      t-test of treatment vs control means
"""

import sys
sys.path.insert(0, '.')
from tools.yfinance_utils import safe_download
import pandas as pd
import numpy as np
from scipy import stats

START = "2016-01-01"
END   = "2026-04-19"
PRE_DAYS = 20
POST_DAYS = 20
PRE_THRESHOLD = -0.05   # SPY 20d_pre < -5%
VIX_THRESHOLD = 30
CLUSTER_DAYS = 30       # first-close-in-cluster definition

# Fetch
spy = safe_download("SPY", start=START, end=END)
vix = safe_download("^VIX", start=START, end=END)
if spy is None or vix is None or spy.empty or vix.empty:
    print("ERROR: data fetch failed"); sys.exit(1)

spy_close = spy['Close'].squeeze()
vix_close = vix['Close'].squeeze()

# Align
df = pd.concat({'spy': spy_close, 'vix': vix_close}, axis=1).dropna()

# Compute pre-event 20d return and post-event 20d return on SPY
df['ret_pre_20d']  = df['spy'].pct_change(PRE_DAYS)
df['ret_post_20d'] = df['spy'].shift(-POST_DAYS) / df['spy'] - 1.0

# Identify VIX>30 first-close clusters
vix_high = (df['vix'] > VIX_THRESHOLD).astype(int)
df['vix_high'] = vix_high
# first close means: vix > 30 today AND no vix > 30 in previous CLUSTER_DAYS days
df['cluster_first'] = False
high_idx = df.index[df['vix_high'] == 1]
last_high = None
firsts = []
for ts in high_idx:
    if last_high is None or (ts - last_high).days > CLUSTER_DAYS:
        firsts.append(ts)
    last_high = ts
df.loc[firsts, 'cluster_first'] = True

# COVID-2020 exclusion: drop Feb–Dec 2020 (covers the COVID spike + bounce window)
covid_mask = (df.index >= '2020-02-01') & (df.index <= '2020-12-31')

# Treatment: VIX>30 first-cluster + 20d_pre < -5%, excl. COVID
treat_mask = df['cluster_first'] & (df['ret_pre_20d'] < PRE_THRESHOLD) & ~covid_mask & df['ret_post_20d'].notna()
treat = df.loc[treat_mask, ['ret_pre_20d', 'ret_post_20d']].copy()

# Control: 20d_pre < -5%, excl. COVID, excl. VIX>30 first-cluster days, excl. days inside ±20d of any cluster_first
# (to avoid contamination from the same event)
buffer = pd.Timedelta(days=POST_DAYS+5)
near_cluster = pd.Series(False, index=df.index)
for ts in firsts:
    near_cluster |= (df.index >= ts - buffer) & (df.index <= ts + buffer)

ctrl_mask = (df['ret_pre_20d'] < PRE_THRESHOLD) & ~covid_mask & ~near_cluster & df['ret_post_20d'].notna()
ctrl = df.loc[ctrl_mask, ['ret_pre_20d', 'ret_post_20d']].copy()

print(f"=== VIX presold regime audit ===")
print(f"Window: {START} to {END}")
print(f"Treatment events (VIX>30 first-cluster + SPY pre<-5%, excl COVID): n={len(treat)}")
if len(treat) > 0:
    print(f"  mean post_20d: {treat['ret_post_20d'].mean()*100:+.2f}%")
    print(f"  median post_20d: {treat['ret_post_20d'].median()*100:+.2f}%")
    print(f"  positive_rate: {(treat['ret_post_20d']>0).mean()*100:.1f}%")
    print(f"  std: {treat['ret_post_20d'].std()*100:.2f}%")
    print(f"  events:")
    for ts, row in treat.iterrows():
        print(f"    {ts.date()}  pre={row['ret_pre_20d']*100:+.1f}%  post={row['ret_post_20d']*100:+.1f}%")

print()
print(f"Control days (SPY pre<-5%, no VIX>30 cluster, excl COVID): n={len(ctrl)}")
if len(ctrl) > 0:
    print(f"  mean post_20d: {ctrl['ret_post_20d'].mean()*100:+.2f}%")
    print(f"  median post_20d: {ctrl['ret_post_20d'].median()*100:+.2f}%")
    print(f"  positive_rate: {(ctrl['ret_post_20d']>0).mean()*100:.1f}%")
    print(f"  std: {ctrl['ret_post_20d'].std()*100:.2f}%")

# t-test
if len(treat) >= 2 and len(ctrl) >= 5:
    t, p = stats.ttest_ind(treat['ret_post_20d'], ctrl['ret_post_20d'], equal_var=False)
    diff = treat['ret_post_20d'].mean() - ctrl['ret_post_20d'].mean()
    print()
    print(f"=== TEST ===")
    print(f"  Mean diff (treat - ctrl): {diff*100:+.2f}%")
    print(f"  Welch t-stat: {t:.3f}")
    print(f"  p-value (two-sided): {p:.4f}")
    if abs(diff)*100 < 1.5 and p > 0.05:
        verdict = "REGIME_ARTIFACT_KILL"
    elif diff*100 > 2.0 and p < 0.05 and len(treat) >= 5:
        verdict = "REAL_EDGE"
    else:
        verdict = "INCONCLUSIVE"
    print(f"  Verdict: {verdict}")
else:
    print("Insufficient sample for t-test.")

# Bonus: bucket controls by depth of pre-selloff to see if depth alone explains the bounce
print()
print("=== Control bucketed by pre-selloff depth ===")
buckets = [(-0.05, -0.07), (-0.07, -0.10), (-0.10, -1.0)]
for lo, hi in buckets:
    sub = ctrl[(ctrl['ret_pre_20d'] >= hi) & (ctrl['ret_pre_20d'] < lo)]
    if len(sub) > 0:
        print(f"  pre in [{hi*100:.0f}%, {lo*100:.0f}%]: n={len(sub):4d}  mean post_20d={sub['ret_post_20d'].mean()*100:+.2f}%  pos%={(sub['ret_post_20d']>0).mean()*100:.0f}%")

# Compare like-for-like: control matched on pre-selloff depth bucket of each treat event
if len(treat) > 0:
    print()
    print("=== Like-for-like matched control (same pre-selloff bucket as each treat) ===")
    matched_means = []
    for ts, row in treat.iterrows():
        depth = row['ret_pre_20d']
        # find controls within ±2% pre depth
        sub = ctrl[(ctrl['ret_pre_20d'] >= depth - 0.02) & (ctrl['ret_pre_20d'] <= depth + 0.02)]
        if len(sub) > 0:
            print(f"  treat {ts.date()} pre={depth*100:+.1f}% post={row['ret_post_20d']*100:+.1f}% | matched ctrl n={len(sub)} mean post={sub['ret_post_20d'].mean()*100:+.2f}%")
            matched_means.append((row['ret_post_20d'], sub['ret_post_20d'].mean()))
    if matched_means:
        treat_arr = np.array([m[0] for m in matched_means])
        ctrl_arr  = np.array([m[1] for m in matched_means])
        diff_arr  = treat_arr - ctrl_arr
        print(f"  Avg paired diff (treat - matched_ctrl): {diff_arr.mean()*100:+.2f}%")
        print(f"  Median paired diff: {np.median(diff_arr)*100:+.2f}%")
