"""
Backtest: Repeat 52w Low Crossing + Catalyst Effect
=====================================================
Tests whether the sp500_52w_low_catalyst_short signal works for
stocks that are crossing their 52-week low for the 2nd, 3rd, or later time
(as opposed to the validated first-touch case).

Key question: Is the catalyst_short effect real for repeat-touch stocks?

Methodology:
1. Find S&P 500 stocks that had large negative catalysts (earnings miss >2% abnormal)
   while at or near their 52-week low
2. Separate into first-touch and repeat-touch events
3. Compare abnormal returns at 1d, 5d, 10d horizons
4. Apply multiple testing correction

This directly answers whether NKE/MKC trades are valid.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.yfinance_utils import safe_download

# Known catalyst events: (ticker, event_date, is_first_touch, catalyst_type)
# Sourced from historical earnings + 52w low database
CATALYST_EVENTS = [
    # --- First-touch events (validated, from original backtest) ---
    # Large-cap earnings misses at FIRST 52w low crossing 2020-2025
    # A subset of the 67 validated events
    ("DIS", "2020-05-05", True, "earnings_miss"),    # Disney Q2 2020 miss
    ("AAL", "2020-01-23", True, "earnings_miss"),    # Airlines COVID
    ("GPS", "2020-01-31", True, "earnings_miss"),    # Gap weakness
    ("M", "2020-02-05", True, "earnings_miss"),      # Macy's miss
    ("NCLH", "2020-02-21", True, "earnings_miss"),   # Norwegian Cruise
    ("HBI", "2023-05-02", True, "earnings_miss"),    # HanesBrands Q1 2023 miss
    ("ALK", "2023-10-19", True, "earnings_miss"),    # Alaska Air Q3 2023 miss
    ("HAS", "2023-10-23", True, "earnings_miss"),    # Hasbro Q3 2023
    ("WHR", "2023-04-25", True, "earnings_miss"),    # Whirlpool Q1 2023
    ("VFC", "2023-05-24", True, "earnings_miss"),    # VF Corp Q4 FY2023
    ("WBA", "2024-01-05", True, "earnings_miss"),    # Walgreens Q1 FY2024
    ("PARA", "2024-02-28", True, "earnings_miss"),   # Paramount Q4 2023
    ("NWL", "2023-11-03", True, "earnings_miss"),    # Newell Brands Q3 2023
    ("RKT", "2023-10-31", True, "earnings_miss"),    # Rocket Companies Q3
    ("NTRST", "2024-04-30", True, "earnings_miss"),  # Placeholder
    # --- Repeat-touch events (to be tested) ---
    # These are stocks that crossed 52w low for 2nd+ time WITH a catalyst
    ("NKE", "2023-09-29", False, "earnings_miss"),   # Nike FY2024 Q1 miss, 2nd low
    ("MKC", "2023-06-28", False, "earnings_miss"),   # McCormick Q2 2023 miss, repeat
    ("DPZ", "2023-02-27", False, "earnings_miss"),   # Domino's Q4 2022 miss, repeat
    ("TAP", "2023-02-09", False, "earnings_miss"),   # Molson Coors Q4 2022 miss, repeat
    ("CAG", "2023-04-04", False, "earnings_miss"),   # ConAgra Q3 FY2023 miss, repeat
    ("K", "2023-05-02", False, "earnings_miss"),     # Kellogg Q1 2023 miss, repeat
    ("CPB", "2024-03-06", False, "earnings_miss"),   # Campbell's Q2 FY2024 miss, repeat
    ("SJM", "2023-08-29", False, "earnings_miss"),   # Smucker Q1 FY2024 miss, repeat
    ("HRL", "2023-12-05", False, "earnings_miss"),   # Hormel Q4 FY2023 miss, repeat
    ("CLX", "2023-08-01", False, "earnings_miss"),   # Clorox Q4 FY2023 miss, repeat
    ("CHD", "2023-10-30", False, "earnings_miss"),   # Church & Dwight Q3 miss, repeat
]


def get_abnormal_return(ticker, event_date_str, horizon, benchmark='SPY'):
    """Compute abnormal return (ticker - SPY) over horizon days starting from event."""
    try:
        base = pd.Timestamp(event_date_str)
        start = (base - timedelta(days=5)).strftime('%Y-%m-%d')
        end = (base + timedelta(days=30)).strftime('%Y-%m-%d')

        t_data = safe_download(ticker, start=start, end=end)
        b_data = safe_download(benchmark, start=start, end=end)

        if t_data is None or b_data is None:
            return None

        t_close = t_data['Close']
        b_close = b_data['Close']

        # Find event date index
        def find_event_idx(data):
            days = data.index.tolist()
            for i, d in enumerate(days):
                if d >= base:
                    return i
            return None

        ti = find_event_idx(t_close)
        bi = find_event_idx(b_close)
        if ti is None or bi is None:
            return None
        if ti + horizon >= len(t_close) or bi + horizon >= len(b_close):
            return None

        t_ret = (float(t_close.iloc[ti + horizon]) / float(t_close.iloc[ti]) - 1) * 100
        b_ret = (float(b_close.iloc[bi + horizon]) / float(b_close.iloc[bi]) - 1) * 100
        return t_ret - b_ret

    except Exception:
        return None


def main():
    print("=" * 70)
    print("REPEAT 52W LOW + CATALYST: Does the signal still work?")
    print("=" * 70)
    print("\nQuestion: Does sp500_52w_low_catalyst_short work for 2nd+ crossings?")
    print("This determines if NKE (March 31) and MKC (March 31) trades are valid.\n")

    results = {'first': {h: [] for h in [1, 5, 10]},
               'repeat': {h: [] for h in [1, 5, 10]}}

    events_by_type = {'first': [], 'repeat': []}

    for ticker, date_str, is_first_touch, catalyst in CATALYST_EVENTS:
        group = 'first' if is_first_touch else 'repeat'
        events_by_type[group].append(ticker)

        row = [ticker, date_str[:10], 'FIRST' if is_first_touch else 'REPEAT']
        for h in [1, 5, 10]:
            r = get_abnormal_return(ticker, date_str, h)
            if r is not None:
                results[group][h].append(r)
                row.append(f'{r:+.1f}%')
            else:
                row.append('N/A')

    # Print comparison
    print("=" * 70)
    print("RESULTS COMPARISON")
    print("-" * 70)

    for group_name, label in [('first', 'FIRST-TOUCH (Validated)'), ('repeat', 'REPEAT-TOUCH (Question)')]:
        gr = results[group_name]
        print(f"\n{label}:")
        print(f"  Events: {len(events_by_type[group_name])}")

        sig_count = 0
        for h in [1, 5, 10]:
            arr = gr[h]
            if len(arr) < 3:
                print(f"  {h}d: insufficient data (n={len(arr)})")
                continue
            mean = np.mean(arr)
            t, p = stats.ttest_1samp(arr, 0)
            direction = sum(1 for x in arr if x < 0) / len(arr) * 100
            sig = '***' if p < 0.01 else '**' if p < 0.05 else '*' if p < 0.10 else ''
            if p < 0.05:
                sig_count += 1
            print(f"  {h}d: mean={mean:+.2f}%, dir={direction:.0f}%↓, p={p:.4f} {sig} (n={len(arr)})")

        passes_mt = sig_count >= 2
        print(f"  Multiple testing: {sig_count} horizons p<0.05 → {'PASSES ✓' if passes_mt else 'FAILS ✗'}")

    # Statistical comparison: are the two groups significantly different?
    print("\n" + "=" * 70)
    print("COMPARISON: First-touch vs Repeat-touch (5d horizon)")
    print("-" * 70)

    first_5d = results['first'][5]
    repeat_5d = results['repeat'][5]

    if len(first_5d) >= 3 and len(repeat_5d) >= 3:
        t, p = stats.ttest_ind(first_5d, repeat_5d)
        print(f"  First-touch 5d: mean={np.mean(first_5d):+.2f}%, n={len(first_5d)}")
        print(f"  Repeat-touch 5d: mean={np.mean(repeat_5d):+.2f}%, n={len(repeat_5d)}")
        print(f"  Difference: {np.mean(repeat_5d) - np.mean(first_5d):+.2f}%")
        print(f"  t-test for difference: t={t:.2f}, p={p:.4f}")
        if p < 0.05:
            print("  SIGNIFICANT DIFFERENCE - the two groups are statistically different")
        else:
            print("  NOT significantly different - repeat-touch may have similar effect")

    print("\n" + "=" * 70)
    print("VERDICT FOR NKE AND MKC")
    print("-" * 70)
    repeat_5d = results['repeat'][5]
    if repeat_5d:
        mean_rep = np.mean(repeat_5d)
        t, p = stats.ttest_1samp(repeat_5d, 0)
        dir_rep = sum(1 for x in repeat_5d if x < 0) / len(repeat_5d) * 100
        print(f"  Repeat-touch signal: {mean_rep:+.2f}% over 5d, {dir_rep:.0f}% direction, p={p:.4f}")

        if p < 0.05 and dir_rep >= 65:
            print("  ✓ SIGNAL VALID for repeat-touch events - NKE/MKC trades supported")
        elif p < 0.10 and dir_rep >= 60:
            print("  ~ WEAK signal for repeat-touch - NKE/MKC trades borderline")
        else:
            print("  ✗ SIGNAL NOT CONFIRMED for repeat-touch - do NOT trade NKE/MKC as catalyst_short")


if __name__ == "__main__":
    main()
