"""
Tariff shock analysis: SPY and VIX behavior after major US tariff escalation events.
Examines 5d, 10d, 20d forward returns and VIX levels at each event date.
Also tests: SPY -3%+ weekly decline + VIX>25 recovery pattern.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from tools.yfinance_utils import safe_download, get_close_prices
import warnings
warnings.filterwarnings("ignore")

# ── 1. Download data ──────────────────────────────────────────────────────────
print("Downloading SPY and VIX data 2018-01-01 to 2026-03-26...")
spy_raw = safe_download("SPY", start="2018-01-01", end="2026-03-27")
vix_raw = safe_download("^VIX", start="2018-01-01", end="2026-03-27")

# Extract close prices
if isinstance(spy_raw.columns, pd.MultiIndex):
    spy = spy_raw["Close"]["SPY"].dropna()
    spy_open = spy_raw["Open"]["SPY"].dropna()
else:
    spy = spy_raw["Close"].dropna()
    spy_open = spy_raw["Open"].dropna()

if isinstance(vix_raw.columns, pd.MultiIndex):
    vix = vix_raw["Close"]["^VIX"].dropna()
else:
    vix = vix_raw["Close"].dropna()

print(f"SPY: {len(spy)} trading days  ({spy.index[0].date()} to {spy.index[-1].date()})")
print(f"VIX: {len(vix)} trading days  ({vix.index[0].date()} to {vix.index[-1].date()})")

# ── 2. Define tariff shock dates ──────────────────────────────────────────────
tariff_events = [
    ("2018-03-01", "Steel/Aluminum tariffs announced (Section 232)"),
    ("2018-07-06", "First China tariffs implemented ($34B, Section 301)"),
    ("2019-05-10", "China tariff rate raised to 25% on $200B goods"),
    ("2019-08-01", "Trump tweets new tariffs on $300B China goods"),
    ("2025-04-02", "Liberation Day - broad tariff announcements"),
    ("2025-04-09", "Tariff pause announced (partial rollback)"),
]

# ── 3. Helper: find nearest trading day at or after target date ───────────────
def nearest_trading_day(target_str, price_series, direction="forward"):
    target = pd.Timestamp(target_str)
    if direction == "forward":
        candidates = price_series.index[price_series.index >= target]
    else:
        candidates = price_series.index[price_series.index <= target]
    if len(candidates) == 0:
        return None
    return candidates[0]

def nth_trading_day_after(base_idx, price_series, n):
    """Return the index position n trading days after base_idx."""
    pos = price_series.index.get_loc(base_idx)
    target_pos = pos + n
    if target_pos >= len(price_series):
        return None
    return price_series.index[target_pos]

# ── 4. Event-level analysis ───────────────────────────────────────────────────
print("\n" + "="*80)
print("TARIFF SHOCK EVENT ANALYSIS")
print("="*80)
print(f"{'Date':<12} {'Event':<50} {'VIX':>5} {'5d%':>7} {'10d%':>7} {'20d%':>7}")
print("-"*80)

results = []
for date_str, label in tariff_events:
    trade_day = nearest_trading_day(date_str, spy)
    if trade_day is None:
        print(f"{date_str:<12} {label[:50]:<50}  N/A (no data)")
        continue

    # VIX at event date
    vix_day = nearest_trading_day(date_str, vix)
    vix_val = vix.loc[vix_day] if vix_day is not None else np.nan

    # Entry = close on event day (or open if after-hours announcement)
    entry = spy.loc[trade_day]

    # Forward returns
    fwd = {}
    for n in [5, 10, 20]:
        future_day = nth_trading_day_after(trade_day, spy, n)
        if future_day is not None:
            fwd[n] = (spy.loc[future_day] / entry - 1) * 100
        else:
            fwd[n] = np.nan

    row = {
        "date": date_str,
        "trade_day": trade_day,
        "label": label,
        "vix": vix_val,
        "ret_5d": fwd.get(5, np.nan),
        "ret_10d": fwd.get(10, np.nan),
        "ret_20d": fwd.get(20, np.nan),
    }
    results.append(row)

    print(f"{date_str:<12} {label[:50]:<50} {vix_val:>5.1f} "
          f"{fwd.get(5,np.nan):>+7.2f}% {fwd.get(10,np.nan):>+7.2f}% {fwd.get(20,np.nan):>+7.2f}%")

# ── 5. SPY pre-event context: 5-day return BEFORE each event ──────────────────
print("\n" + "="*80)
print("PRE-EVENT CONTEXT: SPY 5-day return BEFORE each tariff shock")
print("="*80)
print(f"{'Date':<12} {'VIX':>5} {'Pre-5d%':>9} {'Pre-10d%':>10} {'Post-20d%':>10}")
print("-"*80)

for row in results:
    trade_day = row["trade_day"]
    pos = spy.index.get_loc(trade_day)

    pre5 = (spy.loc[trade_day] / spy.iloc[pos - 5] - 1) * 100 if pos >= 5 else np.nan
    pre10 = (spy.loc[trade_day] / spy.iloc[pos - 10] - 1) * 100 if pos >= 10 else np.nan
    print(f"{row['date']:<12} {row['vix']:>5.1f} {pre5:>+9.2f}% {pre10:>+10.2f}% {row['ret_20d']:>+10.2f}%")

# ── 6. Broader study: SPY weekly -3% + VIX>25 recovery ──────────────────────
print("\n" + "="*80)
print("BROADER SIGNAL: SPY 5-day decline >3% AND VIX>25 (but <30)")
print("Recovery pattern across all qualifying dates 2018-2026")
print("="*80)

# Compute rolling 5-day return
spy_5d_ret = spy.pct_change(5) * 100  # 5-trading-day return

# Find qualifying dates: SPY 5d-return < -3%, VIX between 25 and 30
qualifying = []
spy_dates = spy.index
for i, dt in enumerate(spy_dates):
    if dt not in spy_5d_ret.index or dt not in vix.index:
        continue
    ret5 = spy_5d_ret.loc[dt]
    vix_val = vix.loc[dt] if dt in vix.index else np.nan
    if pd.isna(ret5) or pd.isna(vix_val):
        continue
    if ret5 < -3.0 and 25.0 <= vix_val < 30.0:
        qualifying.append(dt)

# De-duplicate: keep only first signal in any 10-day cluster
deduped = []
last_signal = None
for dt in qualifying:
    if last_signal is None or (dt - last_signal).days >= 10:
        deduped.append(dt)
        last_signal = dt

print(f"\nAll qualifying dates (SPY -3% in 5d, VIX 25-30): {len(qualifying)}")
print(f"After 10-day de-duplication: {len(deduped)}")

if deduped:
    fwd_20d = []
    for dt in deduped:
        future = nth_trading_day_after(dt, spy, 20)
        if future is not None:
            ret = (spy.loc[future] / spy.loc[dt] - 1) * 100
            fwd_20d.append(ret)

    fwd_arr = np.array(fwd_20d)
    print(f"\n20-day forward returns (N={len(fwd_arr)}):")
    print(f"  Mean:    {np.mean(fwd_arr):+.2f}%")
    print(f"  Median:  {np.median(fwd_arr):+.2f}%")
    print(f"  Stdev:   {np.std(fwd_arr):.2f}%")
    print(f"  % Positive: {(fwd_arr > 0).mean()*100:.1f}%")
    print(f"  Min/Max: {np.min(fwd_arr):+.2f}% / {np.max(fwd_arr):+.2f}%")

    # t-test against zero
    from scipy import stats
    t_stat, p_val = stats.ttest_1samp(fwd_arr, 0)
    print(f"  t-stat={t_stat:.2f}, p={p_val:.4f}")

    # Show all dates
    print(f"\n  {'Date':<12} {'VIX':>5} {'5d pre':>8} {'20d fwd':>9}")
    for dt, ret20 in zip(deduped, fwd_20d):
        v = vix.loc[dt] if dt in vix.index else np.nan
        pos = spy.index.get_loc(dt)
        pre5 = spy_5d_ret.loc[dt]
        print(f"  {str(dt.date()):<12} {v:>5.1f} {pre5:>+8.2f}% {ret20:>+9.2f}%")

# ── 7. VIX>25 (any level above 25) version ────────────────────────────────────
print("\n" + "="*80)
print("SIGNAL VARIANT: SPY 5-day decline >3% AND VIX>25 (ALL levels above 25)")
print("="*80)

qualifying_v2 = []
for i, dt in enumerate(spy_dates):
    if dt not in spy_5d_ret.index or dt not in vix.index:
        continue
    ret5 = spy_5d_ret.loc[dt]
    vix_val = vix.loc[dt] if dt in vix.index else np.nan
    if pd.isna(ret5) or pd.isna(vix_val):
        continue
    if ret5 < -3.0 and vix_val >= 25.0:
        qualifying_v2.append(dt)

deduped_v2 = []
last_signal = None
for dt in qualifying_v2:
    if last_signal is None or (dt - last_signal).days >= 10:
        deduped_v2.append(dt)
        last_signal = dt

print(f"\nAll qualifying dates (SPY -3% in 5d, VIX>=25): {len(qualifying_v2)}")
print(f"After 10-day de-duplication: {len(deduped_v2)}")

if deduped_v2:
    fwd_20d_v2 = []
    for dt in deduped_v2:
        future = nth_trading_day_after(dt, spy, 20)
        if future is not None:
            fwd_20d_v2.append((spy.loc[future] / spy.loc[dt] - 1) * 100)

    arr2 = np.array(fwd_20d_v2)
    print(f"\n20-day forward returns (N={len(arr2)}):")
    print(f"  Mean:    {np.mean(arr2):+.2f}%")
    print(f"  Median:  {np.median(arr2):+.2f}%")
    print(f"  Stdev:   {np.std(arr2):.2f}%")
    print(f"  % Positive: {(arr2 > 0).mean()*100:.1f}%")
    from scipy import stats
    t2, p2 = stats.ttest_1samp(arr2, 0)
    print(f"  t-stat={t2:.2f}, p={p2:.4f}")

# ── 8. Compare against generic VIX>30 signal (for context) ───────────────────
print("\n" + "="*80)
print("REFERENCE: Generic VIX>30 signal (all VIX spikes, no SPY decline filter)")
print("="*80)

vix_above_30 = []
for dt in spy_dates:
    if dt not in vix.index:
        continue
    v = vix.loc[dt]
    if v >= 30.0:
        vix_above_30.append(dt)

deduped_vix30 = []
last_signal = None
for dt in vix_above_30:
    if last_signal is None or (dt - last_signal).days >= 10:
        deduped_vix30.append(dt)
        last_signal = dt

print(f"\nVIX>30 qualifying dates: {len(vix_above_30)}, deduped: {len(deduped_vix30)}")

if deduped_vix30:
    fwd_vix30 = []
    for dt in deduped_vix30:
        future = nth_trading_day_after(dt, spy, 20)
        if future is not None:
            fwd_vix30.append((spy.loc[future] / spy.loc[dt] - 1) * 100)

    arr30 = np.array(fwd_vix30)
    print(f"20-day forward returns (N={len(arr30)}):")
    print(f"  Mean:    {np.mean(arr30):+.2f}%")
    print(f"  Median:  {np.median(arr30):+.2f}%")
    print(f"  % Positive: {(arr30 > 0).mean()*100:.1f}%")
    from scipy import stats
    t30, p30 = stats.ttest_1samp(arr30, 0)
    print(f"  t-stat={t30:.2f}, p={p30:.4f}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
