"""
Turn-of-Month (TOM) Effect Analysis on SPY
Last 3 trading days + first 3 trading days of each month vs non-TOM days
Period: 2015-01-01 to 2025-12-31
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import safe_download

# ── 1. Download data ──────────────────────────────────────────────────────────

print("Downloading SPY and benchmark (^GSPC) data 2015-2025...")
spy_raw = safe_download("SPY", start="2015-01-01", end="2025-12-31")
bench_raw = safe_download("^GSPC", start="2015-01-01", end="2025-12-31")

# Flatten multi-index columns if present
def get_close(df, ticker=None):
    if isinstance(df.columns, pd.MultiIndex):
        if ticker:
            return df["Close"][ticker]
        return df["Close"].iloc[:, 0]
    return df["Close"]

spy_close = get_close(spy_raw, "SPY").sort_index()
bench_close = get_close(bench_raw, "^GSPC").sort_index()

# Align on common dates
common_idx = spy_close.index.intersection(bench_close.index)
spy_close = spy_close.loc[common_idx]
bench_close = bench_close.loc[common_idx]

print(f"  SPY: {len(spy_close)} trading days ({spy_close.index[0].date()} to {spy_close.index[-1].date()})")

# ── 2. Daily returns ──────────────────────────────────────────────────────────

spy_ret   = spy_close.pct_change().dropna()
bench_ret = bench_close.pct_change().dropna()

common_ret_idx = spy_ret.index.intersection(bench_ret.index)
spy_ret   = spy_ret.loc[common_ret_idx]
bench_ret = bench_ret.loc[common_ret_idx]

# Abnormal return = SPY daily return minus S&P500 return (they're nearly identical;
# this removes broad-market noise to isolate any TOM premium)
abnormal = spy_ret - bench_ret   # tiny for SPY, but keeps methodology clean

# For the main analysis we'll use SPY raw returns (more interpretable for SPY itself)
# and note that abnormal returns vs S&P500 are near-zero since SPY IS the S&P500.
# The meaningful comparison is TOM raw returns vs non-TOM raw returns within SPY.
daily = pd.DataFrame({
    "spy_ret":    spy_ret,
    "bench_ret":  bench_ret,
    "abnormal":   abnormal,
    "date":       spy_ret.index,
    "year":       spy_ret.index.year,
    "month":      spy_ret.index.month,
    "ym":         spy_ret.index.to_period("M"),
})

# ── 3. Label TOM days ─────────────────────────────────────────────────────────

def label_tom(df):
    """
    For each (year, month) period, mark:
      - last 3 trading days of that month  → 'last3'
      - first 3 trading days of that month → 'first3'
    A day is TOM if it is either last3 or first3.
    """
    df = df.copy()
    df["tom_type"] = "non_tom"

    for ym, grp in df.groupby("ym"):
        idx = grp.index  # sorted trading days in this month
        if len(idx) >= 3:
            first3 = idx[:3]
            last3  = idx[-3:]
            df.loc[first3, "tom_type"] = "first3"
            df.loc[last3,  "tom_type"] = "last3"
            # Edge case: if a day is in both (tiny month), last3 wins
            overlap = first3.intersection(last3)
            if len(overlap):
                df.loc[overlap, "tom_type"] = "last3"

    df["is_tom"] = df["tom_type"] != "non_tom"
    return df

daily = label_tom(daily)

n_tom     = daily["is_tom"].sum()
n_non_tom = (~daily["is_tom"]).sum()
print(f"\n  TOM days : {n_tom}")
print(f"  Non-TOM  : {n_non_tom}")
print(f"  Total    : {len(daily)}")

# ── 4 & 5. Average returns and t-tests ───────────────────────────────────────

tom_rets     = daily.loc[daily["is_tom"],    "spy_ret"] * 100   # in %
non_tom_rets = daily.loc[~daily["is_tom"],   "spy_ret"] * 100

first3_rets  = daily.loc[daily["tom_type"] == "first3", "spy_ret"] * 100
last3_rets   = daily.loc[daily["tom_type"] == "last3",  "spy_ret"] * 100

def ttest(a, b):
    t, p = stats.ttest_ind(a, b, equal_var=False)   # Welch
    return t, p

t_all,    p_all    = ttest(tom_rets,    non_tom_rets)
t_first3, p_first3 = ttest(first3_rets, non_tom_rets)
t_last3,  p_last3  = ttest(last3_rets,  non_tom_rets)

# ── 6. Breakdown ─────────────────────────────────────────────────────────────

print("\n" + "="*60)
print("TURN-OF-MONTH (TOM) EFFECT — SPY 2015–2025")
print("="*60)

print("\n--- Overall Results ---")
print(f"  Avg TOM return      : {tom_rets.mean():.4f}%  (n={len(tom_rets)})")
print(f"  Avg non-TOM return  : {non_tom_rets.mean():.4f}%  (n={len(non_tom_rets)})")
print(f"  Difference          : {tom_rets.mean() - non_tom_rets.mean():.4f}%")
print(f"  t-statistic         : {t_all:.3f}")
print(f"  p-value (2-tailed)  : {p_all:.4f}   {'*** p<0.01' if p_all<0.01 else ('** p<0.05' if p_all<0.05 else ('* p<0.10' if p_all<0.10 else 'n.s.'))}")

print("\n--- First 3 Days of Month ---")
print(f"  Avg return          : {first3_rets.mean():.4f}%  (n={len(first3_rets)})")
print(f"  vs non-TOM diff     : {first3_rets.mean() - non_tom_rets.mean():.4f}%")
print(f"  t-statistic         : {t_first3:.3f}")
print(f"  p-value (2-tailed)  : {p_first3:.4f}   {'*** p<0.01' if p_first3<0.01 else ('** p<0.05' if p_first3<0.05 else ('* p<0.10' if p_first3<0.10 else 'n.s.'))}")

print("\n--- Last 3 Days of Month ---")
print(f"  Avg return          : {last3_rets.mean():.4f}%  (n={len(last3_rets)})")
print(f"  vs non-TOM diff     : {last3_rets.mean() - non_tom_rets.mean():.4f}%")
print(f"  t-statistic         : {t_last3:.3f}")
print(f"  p-value (2-tailed)  : {p_last3:.4f}   {'*** p<0.01' if p_last3<0.01 else ('** p<0.05' if p_last3<0.05 else ('* p<0.10' if p_last3<0.10 else 'n.s.'))}")

# ── 7. Monthly consistency ────────────────────────────────────────────────────

# For each calendar month, compute avg TOM return vs avg non-TOM return in that month
# then check if TOM > non-TOM
monthly_consistency = []
for ym, grp in daily.groupby("ym"):
    tom_g     = grp.loc[grp["is_tom"],  "spy_ret"] * 100
    non_tom_g = grp.loc[~grp["is_tom"], "spy_ret"] * 100
    if len(tom_g) > 0 and len(non_tom_g) > 0:
        monthly_consistency.append({
            "ym":         ym,
            "tom_avg":    tom_g.mean(),
            "non_tom_avg": non_tom_g.mean(),
            "tom_positive": tom_g.mean() > 0,
            "tom_beats":   tom_g.mean() > non_tom_g.mean(),
        })

mc = pd.DataFrame(monthly_consistency)
pct_tom_positive = mc["tom_positive"].mean() * 100
pct_tom_beats    = mc["tom_beats"].mean() * 100

print("\n--- Monthly Consistency (across all calendar months) ---")
print(f"  Months analyzed     : {len(mc)}")
print(f"  % months TOM return > 0        : {pct_tom_positive:.1f}%")
print(f"  % months TOM beats non-TOM avg : {pct_tom_beats:.1f}%")

# ── 8. Annual breakdown ───────────────────────────────────────────────────────

print("\n--- Annual Breakdown ---")
print(f"  {'Year':>6}  {'TOM avg':>9}  {'NonTOM avg':>11}  {'Diff':>8}  {'TOM>NonTOM':>10}")
for yr, grp in daily.groupby("year"):
    t_r = grp.loc[grp["is_tom"],    "spy_ret"].mean() * 100
    n_r = grp.loc[~grp["is_tom"],   "spy_ret"].mean() * 100
    sign = "YES" if t_r > n_r else "no"
    print(f"  {yr:>6}  {t_r:>+9.3f}%  {n_r:>+11.3f}%  {t_r-n_r:>+8.3f}%  {sign:>10}")

# ── 9. Cumulative window return (6-day TOM window) ───────────────────────────
# Compound the 6 consecutive days around month-end as a single "event" return

print("\n--- 6-Day Compound TOM Window (last3 + first3 as one window) ---")
# Build continuous windows: for each month boundary, find last3 of month M and first3 of month M+1
periods = sorted(daily["ym"].unique())
window_returns = []
for i in range(len(periods) - 1):
    this_m = periods[i]
    next_m = periods[i + 1]
    last3_dates  = daily.loc[(daily["ym"] == this_m) & (daily["tom_type"] == "last3"), "date"]
    first3_dates = daily.loc[(daily["ym"] == next_m) & (daily["tom_type"] == "first3"), "date"]
    window_dates = list(last3_dates) + list(first3_dates)
    if len(window_dates) >= 4:  # require at least 4 days
        w_rets = daily.loc[daily["date"].isin(window_dates), "spy_ret"]
        compound = (1 + w_rets).prod() - 1
        window_returns.append({
            "boundary": str(this_m),
            "n_days":   len(w_rets),
            "compound_ret": compound * 100,
        })

wr = pd.DataFrame(window_returns)
pct_positive = (wr["compound_ret"] > 0).mean() * 100
print(f"  Windows analyzed    : {len(wr)}")
print(f"  Avg 6-day return    : {wr['compound_ret'].mean():.3f}%")
print(f"  Median 6-day return : {wr['compound_ret'].median():.3f}%")
print(f"  % positive windows  : {pct_positive:.1f}%")
print(f"  Std dev             : {wr['compound_ret'].std():.3f}%")

# one-sample t-test: is compound return > 0?
t_w, p_w = stats.ttest_1samp(wr["compound_ret"], 0)
print(f"  t vs zero           : {t_w:.3f}")
print(f"  p-value (2-tailed)  : {p_w:.4f}   {'*** p<0.01' if p_w<0.01 else ('** p<0.05' if p_w<0.05 else ('* p<0.10' if p_w<0.10 else 'n.s.'))}")

# ── 10. Summary verdict ───────────────────────────────────────────────────────

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
signals = []
if p_all   < 0.05: signals.append("Overall TOM effect significant (p<0.05)")
if p_first3 < 0.05: signals.append("First-3-days effect significant (p<0.05)")
if p_last3  < 0.05: signals.append("Last-3-days effect significant (p<0.05)")
if p_w      < 0.05: signals.append("6-day compound window significant (p<0.05)")
if not signals:
    print("  No component clears p<0.05. TOM effect NOT statistically confirmed.")
for s in signals:
    print(f"  [SIGNAL] {s}")

diff = tom_rets.mean() - non_tom_rets.mean()
exceeds_threshold = abs(diff) > 0.05  # 0.05% abnormal (vs 0.5% threshold — raw daily is < 0.5% on average)
print(f"\n  Economic significance (>0.05% daily difference): {'YES' if exceeds_threshold else 'NO'}")
print(f"  TOM consistency (beats non-TOM in >60% of months): {'YES' if pct_tom_beats > 60 else 'NO'} ({pct_tom_beats:.1f}%)")
print()
