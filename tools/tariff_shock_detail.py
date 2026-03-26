"""
Deeper drill: VIX>25 + SPY -3% decomposition, outlier sensitivity,
and tariff-specific vs generic comparison.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import safe_download
import warnings
warnings.filterwarnings("ignore")

spy_raw = safe_download("SPY", start="2018-01-01", end="2026-03-27")
vix_raw = safe_download("^VIX", start="2018-01-01", end="2026-03-27")

if isinstance(spy_raw.columns, pd.MultiIndex):
    spy = spy_raw["Close"]["SPY"].dropna()
else:
    spy = spy_raw["Close"].dropna()

if isinstance(vix_raw.columns, pd.MultiIndex):
    vix = vix_raw["Close"]["^VIX"].dropna()
else:
    vix = vix_raw["Close"].dropna()

spy_5d = spy.pct_change(5) * 100

def nth_day_after(dt, series, n):
    pos = series.index.get_loc(dt)
    tp = pos + n
    return series.iloc[tp] if tp < len(series) else np.nan

def get_deduped(condition_fn, gap_days=10):
    qualifying = [dt for dt in spy.index
                  if dt in spy_5d.index and dt in vix.index
                  and not pd.isna(spy_5d.loc[dt]) and not pd.isna(vix.loc[dt])
                  and condition_fn(spy_5d.loc[dt], vix.loc[dt])]
    deduped, last = [], None
    for dt in qualifying:
        if last is None or (dt - last).days >= gap_days:
            deduped.append(dt)
            last = dt
    return deduped

# ── A. Decompose the VIX>25 broad signal by VIX bucket ──────────────────────
print("=" * 70)
print("VIX BUCKET DECOMPOSITION: SPY -3% in 5d, by VIX level")
print("=" * 70)

buckets = [
    ("25-27.5", lambda r, v: r < -3 and 25.0 <= v < 27.5),
    ("27.5-30", lambda r, v: r < -3 and 27.5 <= v < 30.0),
    ("30-35",   lambda r, v: r < -3 and 30.0 <= v < 35.0),
    ("35-45",   lambda r, v: r < -3 and 35.0 <= v < 45.0),
    ("45+",     lambda r, v: r < -3 and v >= 45.0),
]

for label, fn in buckets:
    dd = get_deduped(fn)
    if not dd:
        print(f"  VIX {label:<10}  N=0")
        continue
    rets = [nth_day_after(dt, spy, 20) / spy.loc[dt] * 100 - 100 for dt in dd
            if not pd.isna(nth_day_after(dt, spy, 20))]
    arr = np.array(rets)
    t, p = stats.ttest_1samp(arr, 0)
    print(f"  VIX {label:<10}  N={len(arr):>2}  mean={np.mean(arr):+6.2f}%  "
          f"median={np.median(arr):+6.2f}%  pos={100*(arr>0).mean():.0f}%  p={p:.3f}")

# ── B. Outlier sensitivity for VIX 25-30 band ────────────────────────────────
print()
print("=" * 70)
print("OUTLIER SENSITIVITY: VIX 25-30 + SPY -3% (remove worst outlier)")
print("=" * 70)

dd_25_30 = get_deduped(lambda r, v: r < -3 and 25.0 <= v < 30.0)
rets_25_30 = [(dt, nth_day_after(dt, spy, 20) / spy.loc[dt] * 100 - 100)
              for dt in dd_25_30
              if not pd.isna(nth_day_after(dt, spy, 20))]
rets_25_30.sort(key=lambda x: x[1])
print("All instances sorted by 20d return:")
for dt, r in rets_25_30:
    v = vix.loc[dt]
    pre = spy_5d.loc[dt]
    print(f"  {str(dt.date()):<12}  VIX={v:.1f}  pre5d={pre:+.2f}%  post20d={r:+.2f}%")

arr_25_30 = np.array([r for _, r in rets_25_30])
# Remove worst outlier (2020-02-24 COVID crash)
arr_trimmed = arr_25_30[1:]  # remove smallest
t_t, p_t = stats.ttest_1samp(arr_trimmed, 0)
print(f"\nWith outlier:    N={len(arr_25_30)}  mean={np.mean(arr_25_30):+.2f}%  p={stats.ttest_1samp(arr_25_30,0)[1]:.3f}")
print(f"Without worst:   N={len(arr_trimmed)}  mean={np.mean(arr_trimmed):+.2f}%  p={p_t:.3f}  pos={100*(arr_trimmed>0).mean():.0f}%")

# ── C. VIX>30 + SPY decline filter (tariff-style) ────────────────────────────
print()
print("=" * 70)
print("COMBINED SIGNAL: VIX>30 AND SPY -3% in 5d (tariff-specific)")
print("=" * 70)

dd_30 = get_deduped(lambda r, v: r < -3 and v >= 30.0)
rets_30 = []
for dt in dd_30:
    fwd = nth_day_after(dt, spy, 20)
    if not pd.isna(fwd):
        rets_30.append((dt, fwd / spy.loc[dt] * 100 - 100))

print(f"N (deduped, 10d gap) = {len(rets_30)}")
for dt, r in sorted(rets_30, key=lambda x: x[0]):
    v = vix.loc[dt]
    pre = spy_5d.loc[dt]
    print(f"  {str(dt.date()):<12}  VIX={v:.1f}  pre5d={pre:+.2f}%  post20d={r:+.2f}%")

if rets_30:
    arr30 = np.array([r for _, r in rets_30])
    t30, p30 = stats.ttest_1samp(arr30, 0)
    print(f"\nMean={np.mean(arr30):+.2f}%  Median={np.median(arr30):+.2f}%  "
          f"Pos={100*(arr30>0).mean():.0f}%  p={p30:.3f}")

# ── D. Tariff-event window: was VIX elevated at each event? ──────────────────
print()
print("=" * 70)
print("TARIFF EVENT DATES: VIX at event vs. 20d forward SPY return")
print("=" * 70)

tariff_events = [
    ("2018-03-01", "Steel/Al 232"),
    ("2018-07-06", "China $34B"),
    ("2019-05-10", "China 25% rate"),
    ("2019-08-01", "Trump $300B tweet"),
    ("2025-04-02", "Liberation Day"),
    ("2025-04-09", "Tariff Pause"),
]

for ds, label in tariff_events:
    td = pd.Timestamp(ds)
    cands = spy.index[spy.index >= td]
    if len(cands) == 0:
        continue
    td = cands[0]
    vix_v = vix.loc[td] if td in vix.index else np.nan
    pre5 = spy_5d.loc[td] if td in spy_5d.index else np.nan
    fwd20 = nth_day_after(td, spy, 20)
    r20 = fwd20 / spy.loc[td] * 100 - 100 if not pd.isna(fwd20) else np.nan
    fwd10 = nth_day_after(td, spy, 10)
    r10 = fwd10 / spy.loc[td] * 100 - 100 if not pd.isna(fwd10) else np.nan
    fwd5 = nth_day_after(td, spy, 5)
    r5 = fwd5 / spy.loc[td] * 100 - 100 if not pd.isna(fwd5) else np.nan
    print(f"  {ds:<12} {label:<20} VIX={vix_v:.1f}  pre5d={pre5:+.2f}%  "
          f"fwd5d={r5:+.2f}%  fwd10d={r10:+.2f}%  fwd20d={r20:+.2f}%")

# ── E. Key question: what happens if VIX is 27.4 at Liberation Day aftermath? -
print()
print("=" * 70)
print("CURRENT SITUATION ANALOG: VIX currently 27.4")
print("Historical instances where VIX was 25-30 AND market had just dropped")
print("=" * 70)

# After a big shock (like Liberation Day Apr 2) VIX settles 25-30 in aftermath
# What's the forward return from VIX 25-30 stabilization days?
# Proxy: VIX 25-30, SPY was lower than 20 days ago by >5%
spy_20d = spy.pct_change(20) * 100

analog_instances = []
for dt in spy.index:
    if dt not in vix.index or dt not in spy_20d.index:
        continue
    v = vix.loc[dt]
    trailing_20 = spy_20d.loc[dt]
    if pd.isna(v) or pd.isna(trailing_20):
        continue
    if 25.0 <= v < 32.0 and trailing_20 < -5.0:
        analog_instances.append(dt)

analog_deduped = []
last = None
for dt in analog_instances:
    if last is None or (dt - last).days >= 15:
        analog_deduped.append(dt)
        last = dt

print(f"\nVIX 25-32, SPY down >5% in trailing 20d: {len(analog_instances)} days, {len(analog_deduped)} deduped")
fwd_analog = []
for dt in analog_deduped:
    fwd = nth_day_after(dt, spy, 20)
    if not pd.isna(fwd):
        r = fwd / spy.loc[dt] * 100 - 100
        fwd_analog.append((dt, r, vix.loc[dt], spy_20d.loc[dt]))

print(f"\n{'Date':<12}  {'VIX':>5}  {'Trailing20d':>12}  {'Fwd20d':>9}")
for dt, r, v, t20 in fwd_analog:
    print(f"{str(dt.date()):<12}  {v:>5.1f}  {t20:>+12.2f}%  {r:>+9.2f}%")

if fwd_analog:
    arr_a = np.array([r for _, r, _, _ in fwd_analog])
    t_a, p_a = stats.ttest_1samp(arr_a, 0)
    print(f"\nN={len(arr_a)}  Mean={np.mean(arr_a):+.2f}%  Median={np.median(arr_a):+.2f}%  "
          f"Pos={100*(arr_a>0).mean():.0f}%  p={p_a:.3f}")

print("\nDONE")
