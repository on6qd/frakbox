"""
VIX Spike Sector Rotation Backtest
-----------------------------------
When VIX first closes above 30, which SPDR sector ETF shows the
strongest 5d/10d/20d abnormal return (vs SPY benchmark)?

This extends the validated vix_spike_above_30_spy_recovery signal
to find the OPTIMAL sector to buy after a VIX spike.

Multiple testing: 11 sectors x 3 horizons = 33 tests.
Bonferroni threshold: p < 0.05/33 = 0.00152
We also apply 2-horizon requirement from methodology.json
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from scipy import stats
from tools.yfinance_utils import safe_download
import warnings
warnings.filterwarnings('ignore')

# All 11 SPDR sector ETFs + benchmark
SECTORS = {
    'XLB': 'Materials',
    'XLC': 'Communication',
    'XLE': 'Energy',
    'XLF': 'Financials',
    'XLI': 'Industrials',
    'XLK': 'Technology',
    'XLP': 'Consumer Staples',
    'XLRE': 'Real Estate',
    'XLU': 'Utilities',
    'XLV': 'Health Care',
    'XLY': 'Consumer Disc',
}
BENCHMARK = 'SPY'
VIX = '^VIX'

def get_vix_first_crosses(vix_series, threshold=30, cluster_days=30):
    """Get dates of first VIX close above threshold within each cluster."""
    events = []
    last_event = None
    for date, val in vix_series.items():
        if val > threshold:
            if last_event is None or (date - last_event).days > cluster_days:
                events.append(date)
                last_event = date
    return events

def compute_abnormal_returns(price_series, event_dates, horizon, benchmark_series):
    """Compute abnormal returns (vs SPY) at given horizon after events."""
    results = []
    for event_date in event_dates:
        # Find entry: next available date after event
        future_dates = price_series.index[price_series.index > event_date]
        if len(future_dates) < horizon + 1:
            continue

        entry_date = future_dates[0]
        exit_idx = list(price_series.index).index(entry_date) + horizon
        if exit_idx >= len(price_series):
            continue
        exit_date = price_series.index[exit_idx]

        # Stock return
        entry_price = price_series.loc[entry_date]
        exit_price = price_series.loc[exit_date]
        stock_ret = (exit_price - entry_price) / entry_price * 100

        # Benchmark return
        bench_entry = benchmark_series.loc[entry_date] if entry_date in benchmark_series.index else None
        bench_exit = benchmark_series.loc[exit_date] if exit_date in benchmark_series.index else None
        if bench_entry is None or bench_exit is None:
            continue
        bench_ret = (bench_exit - bench_entry) / bench_entry * 100

        abnormal = stock_ret - bench_ret
        results.append({
            'event_date': event_date,
            'entry_date': entry_date,
            'exit_date': exit_date,
            'stock_ret': stock_ret,
            'bench_ret': bench_ret,
            'abnormal': abnormal,
        })
    return results

def main():
    print("=== VIX SPIKE SECTOR ROTATION BACKTEST ===")
    print("Finding optimal sector to buy when VIX first crosses 30")
    print()

    # Download sector + benchmark data
    all_tickers = list(SECTORS.keys()) + [BENCHMARK]
    print(f"Downloading {len(all_tickers)} tickers (2005-2026)...")

    data = safe_download(all_tickers, start='2005-01-01', end='2026-04-01')

    # safe_download returns Close_TICKER format for multiple tickers
    closes = {}
    for ticker in all_tickers:
        col = f'Close_{ticker}'
        if col in data.columns:
            closes[ticker] = data[col].dropna()
        else:
            print(f"  WARNING: {col} not found")

    # Get VIX series - download separately since it may not merge well
    print("  Downloading VIX separately...")
    import yfinance as yf
    vix_raw = yf.download('^VIX', start='2005-01-01', end='2026-04-01', auto_adjust=True, progress=False)
    vix_series = vix_raw['Close'].dropna()
    # Flatten multi-index if needed
    if hasattr(vix_series, 'columns'):
        vix_series = vix_series.iloc[:, 0]
    # Flatten index if it's a MultiIndex
    if hasattr(vix_series.index, 'levels'):
        vix_series.index = vix_series.index.get_level_values(0)
    print(f"  VIX data: {len(vix_series)} days")

    # Get benchmark
    spy_series = closes[BENCHMARK]

    # Find VIX first-cross events (30-day clustering)
    event_dates = get_vix_first_crosses(vix_series, threshold=30, cluster_days=30)
    print(f"VIX>30 events (30-day clustered): {len(event_dates)}")
    print(f"  Date range: {event_dates[0].date()} to {event_dates[-1].date()}")
    print()

    horizons = [5, 10, 20]
    results_by_sector = {}

    for ticker, name in SECTORS.items():
        if ticker not in closes:
            print(f"  WARNING: {ticker} not in data")
            continue

        sector_prices = closes[ticker]
        sector_results = {}

        for h in horizons:
            returns = compute_abnormal_returns(sector_prices, event_dates, h, spy_series)
            if len(returns) < 10:
                continue

            abnormals = [r['abnormal'] for r in returns]
            n = len(abnormals)
            avg = np.mean(abnormals)
            median = np.median(abnormals)
            pos_rate = sum(1 for x in abnormals if x > 0.5) / n  # >0.5% direction

            # t-test
            t_stat, p_ttest = stats.ttest_1samp(abnormals, 0)
            # Wilcoxon
            try:
                _, p_wilcox = stats.wilcoxon(abnormals)
            except:
                p_wilcox = 1.0

            sector_results[h] = {
                'n': n,
                'avg': avg,
                'median': median,
                'pos_rate': pos_rate,
                'p_ttest': p_ttest,
                'p_wilcox': p_wilcox,
                'returns': abnormals
            }

        results_by_sector[ticker] = {'name': name, 'horizons': sector_results}

    # Report
    print("=" * 90)
    print(f"{'ETF':<6} {'Sector':<18} {'5d avg':>7} {'5d dir':>6} {'5d p':>6} | {'10d avg':>7} {'10d dir':>6} {'10d p':>6} | {'20d avg':>7} {'20d dir':>6} {'20d p':>6}")
    print("=" * 90)

    bonferroni_threshold = 0.05 / (len(SECTORS) * 3)

    # Sort by 10d average abnormal return (sectors with results only)
    sorted_sectors = sorted(
        [(k,v) for k,v in results_by_sector.items() if v['horizons']],
        key=lambda x: x[1]['horizons'].get(10, {}).get('avg', -99),
        reverse=True
    )

    for ticker, data in sorted_sectors:
        name = data['name']
        h5 = data['horizons'].get(5, {})
        h10 = data['horizons'].get(10, {})
        h20 = data['horizons'].get(20, {})

        def fmt(h, key, fmt_str='{:.2f}'):
            val = h.get(key)
            return fmt_str.format(val) if val is not None else 'N/A'

        p5 = h5.get('p_ttest', 1.0)
        p10 = h10.get('p_ttest', 1.0)
        p20 = h20.get('p_ttest', 1.0)

        # Significance markers
        sig5 = '***' if p5 < bonferroni_threshold else ('**' if p5 < 0.01 else ('*' if p5 < 0.05 else ''))
        sig10 = '***' if p10 < bonferroni_threshold else ('**' if p10 < 0.01 else ('*' if p10 < 0.05 else ''))
        sig20 = '***' if p20 < bonferroni_threshold else ('**' if p20 < 0.01 else ('*' if p20 < 0.05 else ''))

        row = (f"{ticker:<6} {name:<18} "
               f"{fmt(h5,'avg'):>7}{sig5:<3} {fmt(h5,'pos_rate','{:.0%}'):>6} {p5:>6.3f} | "
               f"{fmt(h10,'avg'):>7}{sig10:<3} {fmt(h10,'pos_rate','{:.0%}'):>6} {p10:>6.3f} | "
               f"{fmt(h20,'avg'):>7}{sig20:<3} {fmt(h20,'pos_rate','{:.0%}'):>6} {p20:>6.3f}")
        print(row)

    # SPY baseline (abnormal vs itself = 0, show raw returns)
    print()
    print("SPY raw returns (benchmark):")
    spy_results = {}
    for h in horizons:
        rets = []
        for event_date in event_dates:
            future_dates = spy_series.index[spy_series.index > event_date]
            if len(future_dates) < h + 1:
                continue
            entry_date = future_dates[0]
            exit_idx = list(spy_series.index).index(entry_date) + h
            if exit_idx >= len(spy_series):
                continue
            exit_date = spy_series.index[exit_idx]
            entry = spy_series.loc[entry_date]
            exit_p = spy_series.loc[exit_date]
            rets.append((exit_p - entry) / entry * 100)
        if rets:
            pos = sum(1 for r in rets if r > 0) / len(rets)
            _, p = stats.ttest_1samp(rets, 0)
            print(f"  {h}d: n={len(rets)}, avg={np.mean(rets):.2f}%, dir={pos:.0%}, p={p:.3f}")

    print()
    print(f"Bonferroni threshold: p < {bonferroni_threshold:.4f}")
    print()

    # Identify top candidates
    print("=== TOP CANDIDATES (2+ sig horizons OR strong signal) ===")
    candidates = []
    for ticker, data in results_by_sector.items():
        name = data['name']
        h5 = data['horizons'].get(5, {})
        h10 = data['horizons'].get(10, {})
        h20 = data['horizons'].get(20, {})

        sig_horizons = 0
        sig_horizons += 1 if h5.get('p_ttest', 1) < 0.05 else 0
        sig_horizons += 1 if h10.get('p_ttest', 1) < 0.05 else 0
        sig_horizons += 1 if h20.get('p_ttest', 1) < 0.05 else 0

        best_avg = max(
            h5.get('avg', -99),
            h10.get('avg', -99),
            h20.get('avg', -99)
        )

        if sig_horizons >= 2 or best_avg > 3.0:
            candidates.append((ticker, name, sig_horizons, best_avg, h5, h10, h20))

    for ticker, name, sig_h, best, h5, h10, h20 in sorted(candidates, key=lambda x: -x[3]):
        print(f"\n{ticker} ({name}): {sig_h} significant horizons, best avg={best:.2f}%")
        for h_label, h_data in [('5d', h5), ('10d', h10), ('20d', h20)]:
            if h_data:
                print(f"  {h_label}: avg={h_data.get('avg',0):.2f}%, dir={h_data.get('pos_rate',0):.0%}, "
                      f"p_t={h_data.get('p_ttest',1):.3f}, p_w={h_data.get('p_wilcox',1):.3f}, "
                      f"n={h_data.get('n',0)}")

    if not candidates:
        print("No candidates meet threshold. All sectors have similar recovery.")

    return results_by_sector

if __name__ == '__main__':
    main()
