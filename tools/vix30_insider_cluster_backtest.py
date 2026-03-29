"""
VIX>30 Insider Cluster Backtest
================================
Research question: Do n=6-9 insider clusters still generate positive abnormal
returns when VIX > 30 (Tier 4 / crisis regime)?

Pre-defined success criteria:
  VALID if: N>=10 events, avg 3d abnormal return > 0%, pos_rate > 55%, p < 0.10
  UNDERPOWERED if: N < 10

Usage:
  python3 tools/vix30_insider_cluster_backtest.py
  python3 tools/vix30_insider_cluster_backtest.py --fast  (load from cache if available)
"""

import sys
import os
import pickle
import warnings
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import yfinance as yf
from scipy import stats

warnings.filterwarnings('ignore')
sys.path.insert(0, str(Path(__file__).parent.parent))

CACHE_DIR = Path(__file__).parent.parent / "data" / "sec_form4_cache"
RESULTS_CACHE = Path(__file__).parent.parent / "data" / "vix30_cluster_backtest_cache.pkl"
VIX_CRISIS_THRESHOLD = 30.0
DIRECTION_THRESHOLD = 0.5  # 0.5% abnormal return to count as positive


def load_vix_history():
    """Load VIX history for regime classification."""
    vix = yf.Ticker("^VIX").history(start="2019-01-01", end="2026-03-30")
    vix.index = pd.to_datetime(vix.index).tz_localize(None).normalize()
    return vix['Close'].rename('vix')


def run_analysis(use_cache=False):
    print("=" * 70)
    print("VIX>30 Insider Cluster Backtest")
    print("Question: Does the n=6-9 signal hold at VIX>30?")
    print("=" * 70)
    print()

    # Load cached results if available
    if use_cache and RESULTS_CACHE.exists():
        print("Loading cached results...")
        with open(RESULTS_CACHE, 'rb') as f:
            results_df = pickle.load(f)
        print(f"Loaded {len(results_df)} events from cache")
    else:
        # Step 1: Load VIX history
        print("Loading VIX history...")
        vix_series = load_vix_history()
        print(f"  VIX data: {len(vix_series)} days, {(vix_series > 30).sum()} days VIX>30")

        # Step 2: Load insider clusters from SEC data
        print("\nLoading insider clusters from SEC Form 4 data (2020-2024)...")
        print("(Using cached data — should be fast)")

        from tools.insider_cluster_detector import identify_cluster_events

        clusters = identify_cluster_events(
            year_start=2020,
            year_end=2024,
            min_purchase_value=50_000,
            cluster_window_days=30,
            min_insiders_in_cluster=3,
        )

        if clusters.empty:
            print("ERROR: No clusters found")
            return None

        print(f"\nTotal clusters found: {len(clusters)}")
        print(f"Columns: {list(clusters.columns)}")

        # Step 3: Add VIX
        clusters['cluster_date'] = pd.to_datetime(clusters['cluster_date']).dt.normalize()
        clusters['vix'] = clusters['cluster_date'].map(lambda d: vix_series.get(d, np.nan))

        # Fill missing VIX with ±1 day
        for i, row in clusters[clusters['vix'].isna()].iterrows():
            d = row['cluster_date']
            for offset in [1, -1, 2, -2]:
                nd = d + pd.Timedelta(days=offset)
                if nd in vix_series.index:
                    clusters.at[i, 'vix'] = vix_series[nd]
                    break

        clusters = clusters.dropna(subset=['vix'])
        print(f"After VIX mapping: {len(clusters)} events")

        # Step 4: VIX regime
        clusters['vix_regime'] = 'calm'
        clusters.loc[clusters['vix'] >= 20, 'vix_regime'] = 'elevated'
        clusters.loc[clusters['vix'] >= 25, 'vix_regime'] = 'high'
        clusters.loc[clusters['vix'] >= 30, 'vix_regime'] = 'crisis'

        print("\nVIX regime distribution:")
        print(clusters['vix_regime'].value_counts())

        # Step 5: Filter to n=6-9
        n69 = clusters[clusters['n_insiders'].between(6, 9)].copy()
        print(f"\nn=6-9 clusters: {len(n69)}")
        print("VIX regime for n=6-9:")
        print(n69['vix_regime'].value_counts())

        # Step 6: Measure returns
        print(f"\nMeasuring returns for {len(n69)} n=6-9 clusters...")

        # Load SPY for benchmark
        spy = yf.Ticker("SPY").history(start="2019-12-01", end="2026-03-30")['Close']
        spy.index = pd.to_datetime(spy.index).tz_localize(None).normalize()

        results = []
        for idx, (_, row) in enumerate(n69.iterrows()):
            if idx % 50 == 0:
                print(f"  Progress: {idx}/{len(n69)}")

            ticker = str(row['ticker']).upper()
            entry_date = row['cluster_date']

            try:
                end_date = entry_date + timedelta(days=30)
                prices = yf.Ticker(ticker).history(
                    start=entry_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d')
                )['Close']
                prices.index = pd.to_datetime(prices.index).tz_localize(None).normalize()

                if len(prices) < 2:
                    continue

                entry_price = float(prices.iloc[0])
                if entry_price <= 0:
                    continue

                # Returns at 3d and 5d
                def get_return(n_days):
                    if len(prices) > n_days:
                        return (float(prices.iloc[n_days]) / entry_price - 1) * 100
                    elif len(prices) > 1:
                        return (float(prices.iloc[-1]) / entry_price - 1) * 100
                    return np.nan

                # SPY benchmark
                spy_slice = spy[spy.index >= entry_date].iloc[:15]
                if spy_slice.empty:
                    continue
                spy_entry = float(spy_slice.iloc[0])

                def get_spy_return(n_days):
                    if len(spy_slice) > n_days:
                        return (float(spy_slice.iloc[n_days]) / spy_entry - 1) * 100
                    return np.nan

                stock_3d = get_return(3)
                stock_5d = get_return(5)
                spy_3d = get_spy_return(3)
                spy_5d = get_spy_return(5)

                abnormal_3d = stock_3d - spy_3d if not (np.isnan(stock_3d) or np.isnan(spy_3d)) else np.nan
                abnormal_5d = stock_5d - spy_5d if not (np.isnan(stock_5d) or np.isnan(spy_5d)) else np.nan

                results.append({
                    'ticker': ticker,
                    'entry_date': entry_date,
                    'n_insiders': int(row['n_insiders']),
                    'total_value': row.get('total_value', np.nan),
                    'vix': row['vix'],
                    'vix_regime': row['vix_regime'],
                    'stock_3d': stock_3d,
                    'stock_5d': stock_5d,
                    'spy_3d': spy_3d,
                    'spy_5d': spy_5d,
                    'abnormal_3d': abnormal_3d,
                    'abnormal_5d': abnormal_5d,
                })
            except Exception:
                pass

        results_df = pd.DataFrame(results)
        print(f"\nMeasured {len(results_df)} / {len(n69)} events")

        # Save cache
        os.makedirs(RESULTS_CACHE.parent, exist_ok=True)
        with open(RESULTS_CACHE, 'wb') as f:
            pickle.dump(results_df, f)
        print("Results cached.")

    # =====================
    # ANALYSIS
    # =====================
    print("\n" + "=" * 70)
    print("RESULTS: n=6-9 Clusters by VIX Regime")
    print("=" * 70)

    for regime, label in [
        ('calm', 'VIX <20 (calm)'),
        ('elevated', 'VIX 20-25'),
        ('high', 'VIX 25-30'),
        ('crisis', 'VIX >30 (crisis)'),
    ]:
        sub = results_df[results_df['vix_regime'] == regime].dropna(subset=['abnormal_3d'])
        if len(sub) == 0:
            print(f"\n{label}: N=0")
            continue

        n = len(sub)
        avg_3d = sub['abnormal_3d'].mean()
        pos_rate = (sub['abnormal_3d'] > DIRECTION_THRESHOLD).mean()

        if n >= 3:
            t_stat, p_val = stats.ttest_1samp(sub['abnormal_3d'], 0)
        else:
            t_stat, p_val = np.nan, np.nan

        print(f"\n{label}:")
        print(f"  3d: N={n}, avg={avg_3d:+.2f}%, pos_rate={pos_rate:.0%}, p={p_val:.3f}")

        sub5 = sub.dropna(subset=['abnormal_5d'])
        if len(sub5) >= 3:
            avg_5d = sub5['abnormal_5d'].mean()
            pos5 = (sub5['abnormal_5d'] > DIRECTION_THRESHOLD).mean()
            t5, p5 = stats.ttest_1samp(sub5['abnormal_5d'], 0)
            print(f"  5d: N={len(sub5)}, avg={avg_5d:+.2f}%, pos_rate={pos5:.0%}, p={p5:.3f}")

    # CRISIS DEEP DIVE
    print("\n" + "=" * 70)
    print("CRISIS (VIX>30) DEEP DIVE")
    print("=" * 70)

    crisis = results_df[results_df['vix_regime'] == 'crisis']
    print(f"All VIX>30 n=6-9 events: {len(crisis)}")

    if len(crisis) > 0:
        print("\nIndividual events:")
        print(f"{'Date':12} {'Ticker':8} {'VIX':6} {'N':4} {'3d_abn':8} {'5d_abn':8}")
        print("-" * 55)
        for _, row in crisis.sort_values('entry_date').iterrows():
            a3 = f"{row['abnormal_3d']:+.1f}%" if not np.isnan(row['abnormal_3d']) else "  N/A"
            a5 = f"{row['abnormal_5d']:+.1f}%" if not np.isnan(row['abnormal_5d']) else "  N/A"
            print(f"{str(row['entry_date'].date()):12} {row['ticker']:8} {row['vix']:5.1f} {row['n_insiders']:4.0f} {a3:8} {a5:8}")

    # VERDICT
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    crisis_valid = crisis.dropna(subset=['abnormal_3d'])
    n = len(crisis_valid)

    print(f"\nPre-defined success criteria:")
    print(f"  VALID if: N>=10, avg_3d>0%, pos_rate>55%, p<0.10")
    print(f"  UNDERPOWERED if: N<10")
    print()

    if n < 10:
        print(f"RESULT: UNDERPOWERED (N={n} < 10)")
        print("Cannot determine if VIX>30 threshold should change.")
        print("RECOMMENDATION: Keep Tier 4 DO NOT TRADE rule (VIX>30) unchanged.")
        print("MONITOR: Track future VIX>30 events. Need N>=10 to conclude.")
    else:
        avg_3d = crisis_valid['abnormal_3d'].mean()
        pos = (crisis_valid['abnormal_3d'] > DIRECTION_THRESHOLD).mean()
        _, p = stats.ttest_1samp(crisis_valid['abnormal_3d'], 0)

        meets_criteria = (avg_3d > 0 and pos > 0.55 and p < 0.10)

        print(f"RESULT: N={n}, avg_3d={avg_3d:+.2f}%, pos_rate={pos:.0%}, p={p:.3f}")
        print()
        if meets_criteria:
            print("SIGNAL EXISTS at VIX>30 for n=6-9 clusters")
            print("RECOMMENDATION: Lower Tier 4 threshold from VIX>30 to VIX>40")
            print("  (Allow trading n=6-9 clusters when VIX is 30-40)")
        else:
            print("NO RELIABLE SIGNAL at VIX>30 for n=6-9 clusters")
            print("RECOMMENDATION: Keep Tier 4 DO NOT TRADE rule (VIX>30) unchanged.")

    return results_df


if __name__ == "__main__":
    use_cache = "--fast" in sys.argv
    run_analysis(use_cache=use_cache)
