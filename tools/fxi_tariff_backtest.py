"""
FXI Tariff Escalation Backtest
-------------------------------
Research-only script. Does NOT touch the database.

For each tariff event date (first market open after announcement),
compute FXI abnormal returns vs SPY at 1d, 3d, 5d, 10d, and 20d horizons.
Report mean abnormal return, direction consistency, and t-test p-values.

Abnormal return = FXI cumulative return - SPY cumulative return over same window.
Direction = FXI < SPY (positive for a short-FXI position).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from scipy.stats import ttest_1samp
from tools.yfinance_utils import get_close_prices

# ---------------------------------------------------------------------------
# Event dates — first market open on or after each announcement
# We'll find the actual trading date by indexing into the downloaded price series
# ---------------------------------------------------------------------------
EVENT_LABELS = [
    "2018-03-23  Sec232 steel/aluminum",
    "2018-07-06  First $34B tariffs effective",
    "2018-09-24  $200B tranche at 10%",
    "2019-05-13  Escalation to 25%",
    "2019-08-05  Yuan devaluation + escalation",
    "2020-09-01  $300B consumer goods tranche",
    "2025-01-21  Trump 2025 10% China EO",
    "2025-04-03  Liberation Day tariffs",
]

# Requested event dates (first market open after announcement)
EVENT_DATES_RAW = [
    "2018-03-23",
    "2018-07-06",
    "2018-09-24",
    "2019-05-13",
    "2019-08-05",
    "2020-09-01",
    "2025-01-21",
    "2025-04-03",
]

HORIZONS = [1, 3, 5, 10, 20]  # trading days forward
TICKERS = ["FXI", "SPY"]

# ---------------------------------------------------------------------------
# Download price data — wide enough window to cover all events + 20d forward
# ---------------------------------------------------------------------------
print("Downloading FXI and SPY prices (2018-01-01 to 2025-06-01)...")
closes = get_close_prices(TICKERS, start="2018-01-01", end="2025-06-01")
print(f"  Loaded {len(closes)} trading days. Columns: {list(closes.columns)}")
print()

# ---------------------------------------------------------------------------
# For each event, find the actual trading date on-or-after the requested date
# ---------------------------------------------------------------------------
def find_trading_date(target_str: str, index: pd.DatetimeIndex) -> pd.Timestamp | None:
    """Return the first index date >= target_str, or None if beyond data."""
    target = pd.Timestamp(target_str)
    future = index[index >= target]
    return future[0] if len(future) > 0 else None


trading_index = closes.index

# ---------------------------------------------------------------------------
# Compute abnormal returns for each event and horizon
# ---------------------------------------------------------------------------
results = []   # list of dicts, one per event

for raw_date, label in zip(EVENT_DATES_RAW, EVENT_LABELS):
    entry_date = find_trading_date(raw_date, trading_index)
    if entry_date is None:
        print(f"  SKIP {label}: no trading date found on/after {raw_date}")
        continue

    # Entry price = close on the event date (day 0)
    try:
        entry_pos = closes.index.get_loc(entry_date)
    except KeyError:
        print(f"  SKIP {label}: {entry_date} not in index")
        continue

    entry_fxi = closes["FXI"].iloc[entry_pos]
    entry_spy = closes["SPY"].iloc[entry_pos]

    if pd.isna(entry_fxi) or pd.isna(entry_spy):
        print(f"  SKIP {label}: NaN entry price on {entry_date.date()}")
        continue

    row = {
        "label": label,
        "raw_date": raw_date,
        "entry_date": entry_date.date(),
        "entry_fxi": round(entry_fxi, 2),
        "entry_spy": round(entry_spy, 2),
    }

    # Compute returns at each horizon
    for h in HORIZONS:
        exit_pos = entry_pos + h
        if exit_pos >= len(closes):
            row[f"fxi_ret_{h}d"] = np.nan
            row[f"spy_ret_{h}d"] = np.nan
            row[f"abnormal_{h}d"] = np.nan
            continue

        exit_fxi = closes["FXI"].iloc[exit_pos]
        exit_spy = closes["SPY"].iloc[exit_pos]
        exit_date = closes.index[exit_pos].date()

        if pd.isna(exit_fxi) or pd.isna(exit_spy):
            row[f"fxi_ret_{h}d"] = np.nan
            row[f"spy_ret_{h}d"] = np.nan
            row[f"abnormal_{h}d"] = np.nan
            continue

        fxi_ret = (exit_fxi / entry_fxi - 1) * 100  # pct
        spy_ret = (exit_spy / entry_spy - 1) * 100
        abnormal = fxi_ret - spy_ret

        row[f"fxi_ret_{h}d"] = round(fxi_ret, 2)
        row[f"spy_ret_{h}d"] = round(spy_ret, 2)
        row[f"abnormal_{h}d"] = round(abnormal, 2)

    results.append(row)

df = pd.DataFrame(results)

# ---------------------------------------------------------------------------
# Print per-event detail table
# ---------------------------------------------------------------------------
print("=" * 100)
print("PER-EVENT DETAIL")
print("=" * 100)
header = f"{'Event':<45} {'Entry':>8} {'FXI':>7} {'SPY':>7}"
for h in HORIZONS:
    header += f"  {'AR@'+str(h)+'d':>7}"
print(header)
print("-" * 100)

for _, row in df.iterrows():
    line = f"{row['label']:<45} {str(row['entry_date']):>8} {row['entry_fxi']:>7.2f} {row['entry_spy']:>7.2f}"
    for h in HORIZONS:
        val = row[f"abnormal_{h}d"]
        if pd.isna(val):
            line += f"  {'N/A':>7}"
        else:
            line += f"  {val:>+7.2f}"
    print(line)
    # Also print FXI and SPY raw returns for transparency
    detail = f"  {'':45} {'':8} {'FXI%':>7} {'SPY%':>7}"
    for h in HORIZONS:
        fxi_r = row[f"fxi_ret_{h}d"]
        if pd.isna(fxi_r):
            detail += f"  {'N/A':>7}"
        else:
            detail += f"  {fxi_r:>+7.2f}"
    print(detail)
    detail2 = f"  {'':45} {'':8} {'':>7} {'':>7}"
    for h in HORIZONS:
        spy_r = row[f"spy_ret_{h}d"]
        if pd.isna(spy_r):
            detail2 += f"  {'N/A':>7}"
        else:
            detail2 += f"  {spy_r:>+7.2f}"
    print(detail2)
    print()

# ---------------------------------------------------------------------------
# Aggregate statistics per horizon
# ---------------------------------------------------------------------------
print("=" * 80)
print("AGGREGATE STATISTICS (Abnormal Returns = FXI - SPY, all horizons)")
print("Short FXI signal: negative abnormal return = favorable for short")
print("=" * 80)

summary_rows = []
for h in HORIZONS:
    col = f"abnormal_{h}d"
    vals = df[col].dropna().values
    n = len(vals)
    if n < 2:
        print(f"  {h:2d}d: n={n} — insufficient data")
        continue

    mean_ar = np.mean(vals)
    median_ar = np.median(vals)
    std_ar = np.std(vals, ddof=1)
    sem = std_ar / np.sqrt(n)

    # t-test: H0 = mean AR = 0 (two-tailed, then we check direction)
    t_stat, p_two = ttest_1samp(vals, popmean=0)
    p_one = p_two / 2  # one-tailed (negative direction)

    # Direction: negative abnormal return = FXI underperformed SPY = short wins
    n_negative = np.sum(vals < 0)
    n_neg_exceeds_threshold = np.sum(vals < -0.5)  # >0.5% abnormal, direction matters
    direction_pct = n_negative / n * 100

    # What fraction exceeded the 0.5% direction threshold?
    threshold_pct = n_neg_exceeds_threshold / n * 100

    summary_rows.append({
        "horizon": h,
        "n": n,
        "mean_ar": mean_ar,
        "median_ar": median_ar,
        "std_ar": std_ar,
        "t_stat": t_stat,
        "p_two": p_two,
        "p_one_neg": p_one if mean_ar < 0 else 1 - p_one,
        "direction_pct": direction_pct,
        "threshold_pct": threshold_pct,
    })

    print(f"\n{h:2d}d horizon (n={n}):")
    print(f"  Mean abnormal return : {mean_ar:+.2f}%")
    print(f"  Median abnormal return: {median_ar:+.2f}%")
    print(f"  Std dev              : {std_ar:.2f}%")
    print(f"  t-statistic          : {t_stat:.3f}")
    print(f"  p-value (two-tailed) : {p_two:.4f}")
    print(f"  p-value (one-tailed, H1: mean < 0): {p_one if mean_ar < 0 else 1-p_one:.4f}")
    print(f"  Direction %          : {direction_pct:.0f}% of events FXI < SPY")
    print(f"  > 0.5% threshold     : {threshold_pct:.0f}% of events AR < -0.5%")

    # Individual values for transparency
    print(f"  Raw AR values        : {[round(v,2) for v in vals]}")

# ---------------------------------------------------------------------------
# Multiple testing correction check (Bonferroni)
# ---------------------------------------------------------------------------
print()
print("=" * 80)
print("MULTIPLE TESTING (Bonferroni correction for 5 horizons)")
print("=" * 80)
alpha = 0.05
bonferroni_alpha = alpha / len(HORIZONS)
print(f"  Uncorrected alpha: {alpha}")
print(f"  Bonferroni alpha : {bonferroni_alpha:.4f}")
print()
passes = []
for row in summary_rows:
    p = row["p_two"]
    passed = p < bonferroni_alpha and row["mean_ar"] < 0
    flag = "PASS" if passed else "fail"
    passes.append(passed)
    print(f"  {row['horizon']:2d}d: p={p:.4f}  mean_ar={row['mean_ar']:+.2f}%  Bonferroni: {flag}")

# ---------------------------------------------------------------------------
# Summary verdict
# ---------------------------------------------------------------------------
print()
print("=" * 80)
print("TRADABILITY ASSESSMENT")
print("=" * 80)

any_bonferroni = any(passes)
best_row = min(summary_rows, key=lambda r: r["p_two"]) if summary_rows else None
best_dir = max(summary_rows, key=lambda r: r["direction_pct"]) if summary_rows else None

if best_row:
    print(f"\nBest horizon by p-value  : {best_row['horizon']}d  "
          f"(p={best_row['p_two']:.4f}, mean AR={best_row['mean_ar']:+.2f}%)")
if best_dir:
    print(f"Best horizon by direction: {best_dir['horizon']}d  "
          f"({best_dir['direction_pct']:.0f}% FXI < SPY)")

print()
if any_bonferroni:
    print("VERDICT: Signal passes Bonferroni-corrected multiple testing at >= 1 horizon.")
    print("Warrants formal hypothesis creation and out-of-sample validation.")
else:
    # Check looser: 2+ horizons at uncorrected p<0.05
    n_pass_uncorrected = sum(1 for r in summary_rows if r["p_two"] < 0.05 and r["mean_ar"] < 0)
    if n_pass_uncorrected >= 2:
        print(f"VERDICT: Signal passes uncorrected p<0.05 at {n_pass_uncorrected} horizons.")
        print("Meets methodology.json threshold (2+ horizons p<0.05).")
        print("Warrants formal hypothesis creation.")
    else:
        print(f"VERDICT: Signal does NOT meet statistical thresholds.")
        print(f"  Uncorrected p<0.05 at only {n_pass_uncorrected} horizon(s).")
        print("  Consider as dead end or sharpen hypothesis before proceeding.")

print()
print("Note: n=8 is small. Power is limited. Even significant results need")
print("more instances for robust out-of-sample validation.")
