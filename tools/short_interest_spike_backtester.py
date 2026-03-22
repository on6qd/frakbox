"""
Short Interest Spike Signal Backtester

Uses FINRA REGSHO daily short volume data to detect spikes in short volume ratio.
Tests whether >2-sigma spikes in daily short volume ratio predict subsequent
negative abnormal stock returns (Boehmer et al 2008 methodology).

Data source: https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
Available: 2020-present (consolidated tape, NASDAQ/NYSE/CBOE)

Usage:
    python tools/short_interest_spike_backtester.py --year 2022
    python tools/short_interest_spike_backtester.py --year 2022 --symbols AAPL MSFT NVDA
"""

import argparse
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import sys
import os
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def fetch_finra_regsho_day(date_str):
    """Fetch FINRA REGSHO daily short volume data for a given date (YYYYMMDD)."""
    url = f'https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt'
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        lines = resp.text.strip().split('\n')
        # Format: Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
        rows = []
        for line in lines[1:]:  # skip header
            line = line.strip()
            if not line:
                continue
            parts = line.split('|')
            if len(parts) < 5:
                continue
            try:
                rows.append({
                    'date': parts[0],
                    'symbol': parts[1],
                    'short_vol': int(parts[2]),
                    'total_vol': int(parts[4]),
                })
            except (ValueError, IndexError):
                continue
        return pd.DataFrame(rows) if rows else None
    except Exception as e:
        return None


def get_trading_days(year):
    """Get all trading days for a year using a known ETF."""
    try:
        spy = yf.download('SPY', start=f'{year}-01-01', end=f'{year}-12-31', progress=False)
        return [d.strftime('%Y%m%d') for d in spy.index]
    except Exception:
        return []


def load_finra_for_symbols(symbols, start_date, end_date, cache_dir='/tmp/finra_cache'):
    """
    Load FINRA REGSHO daily short volume for a list of symbols over a date range.
    Caches daily files to avoid repeated downloads.
    Returns dict: symbol -> pd.DataFrame with columns [date, short_ratio]
    """
    os.makedirs(cache_dir, exist_ok=True)

    # Get trading days in range
    spy = yf.download('SPY', start=start_date, end=end_date, progress=False)
    trading_days = [d.strftime('%Y%m%d') for d in spy.index]

    print(f"Loading {len(trading_days)} trading days of FINRA data ({start_date} to {end_date})...")

    # Download daily files (with caching)
    all_data = []
    symbol_set = set(symbols)

    for i, day in enumerate(trading_days):
        if i % 50 == 0:
            print(f"  {i}/{len(trading_days)} days loaded...", end='\r')

        cache_file = os.path.join(cache_dir, f'CNMS_{day}.parquet')

        if os.path.exists(cache_file):
            try:
                df = pd.read_parquet(cache_file)
            except Exception:
                df = None
        else:
            df = fetch_finra_regsho_day(day)
            if df is not None:
                try:
                    df.to_parquet(cache_file, index=False)
                except Exception:
                    pass
            time.sleep(0.05)  # Be respectful of FINRA servers

        if df is not None and not df.empty:
            # Filter to symbols of interest
            filtered = df[df['symbol'].isin(symbol_set)].copy()
            if not filtered.empty:
                all_data.append(filtered)

    print(f"  {len(trading_days)}/{len(trading_days)} days loaded.   ")

    if not all_data:
        return {}

    combined = pd.concat(all_data, ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date'], format='%Y%m%d')
    combined = combined[combined['total_vol'] > 0]
    combined['short_ratio'] = combined['short_vol'] / combined['total_vol']

    # Group by symbol
    result = {}
    for sym in symbols:
        sym_data = combined[combined['symbol'] == sym].copy()
        if len(sym_data) >= 30:
            sym_data = sym_data.sort_values('date').set_index('date')
            result[sym] = sym_data['short_ratio']

    return result


def detect_spike_events(short_ratio_series, spike_threshold_sigma=2.0,
                         lookback=20, min_ratio=0.15):
    """
    Detect spike events where short ratio > lookback mean + spike_threshold_sigma * std.
    Only count days where short_ratio exceeds min_ratio (filters low-activity days).
    Returns list of (date, short_ratio, z_score) tuples.
    """
    events = []
    sr = short_ratio_series.copy()

    for i in range(lookback, len(sr)):
        current_date = sr.index[i]
        current_ratio = sr.iloc[i]

        if current_ratio < min_ratio:
            continue

        window = sr.iloc[i-lookback:i]
        mean = window.mean()
        std = window.std()

        if std < 0.001:  # Avoid division by near-zero
            continue

        z_score = (current_ratio - mean) / std

        if z_score >= spike_threshold_sigma:
            events.append({
                'date': current_date,
                'short_ratio': current_ratio,
                'z_score': z_score,
                'baseline_mean': mean
            })

    return events


def measure_post_spike_returns(symbol, events, benchmark='SPY', horizons=[5, 10, 20]):
    """
    Measure abnormal returns at multiple horizons after spike events.
    Returns list of dicts with returns per event.
    """
    if not events:
        return []

    # Get price data with buffer
    first_date = min(e['date'] for e in events)
    last_date = max(e['date'] for e in events)

    start = (first_date - timedelta(days=5)).strftime('%Y-%m-%d')
    end = (last_date + timedelta(days=max(horizons) + 10)).strftime('%Y-%m-%d')

    try:
        prices = yf.download(symbol, start=start, end=end, progress=False)
        bench = yf.download(benchmark, start=start, end=end, progress=False)

        if prices.empty or bench.empty:
            return []

        # Flatten MultiIndex if present
        if isinstance(prices.columns, pd.MultiIndex):
            prices.columns = prices.columns.get_level_values(0)
        if isinstance(bench.columns, pd.MultiIndex):
            bench.columns = bench.columns.get_level_values(0)

        price_close = prices['Close']
        bench_close = bench['Close']
    except Exception:
        return []

    results = []
    for event in events:
        event_date = event['date']

        # Find entry: next trading day open after the spike
        try:
            price_dates = price_close.index
            future_dates = price_dates[price_dates > event_date]
            if len(future_dates) < max(horizons) + 1:
                continue

            entry_date = future_dates[0]
            entry_price = prices.loc[entry_date, 'Open'] if 'Open' in prices.columns else prices.loc[entry_date, 'Close']

            row = {
                'symbol': symbol,
                'spike_date': event_date.strftime('%Y-%m-%d'),
                'entry_date': entry_date.strftime('%Y-%m-%d'),
                'short_ratio': round(event['short_ratio'], 4),
                'z_score': round(event['z_score'], 2),
            }

            for h in horizons:
                if len(future_dates) > h:
                    exit_date = future_dates[h]
                    exit_price = price_close.loc[exit_date]

                    raw_return = (exit_price / entry_price - 1) * 100

                    # Benchmark return
                    bench_dates = bench_close.index
                    bench_future = bench_dates[bench_dates >= entry_date]
                    if len(bench_future) > h:
                        bench_exit = bench_close.loc[bench_future[h]]
                        bench_entry = bench_close.loc[bench_future[0]] if bench_future[0] in bench_close.index else bench_close.loc[entry_date]
                        bench_return = (bench_exit / bench_entry - 1) * 100
                        abnormal = raw_return - bench_return
                    else:
                        abnormal = raw_return

                    row[f'raw_{h}d'] = round(raw_return, 3)
                    row[f'abnormal_{h}d'] = round(abnormal, 3)

            results.append(row)
        except Exception:
            continue

    return results


def run_backtest(symbols, start_date, end_date, spike_sigma=2.0, lookback=20,
                 horizons=[5, 10, 20], verbose=True):
    """
    Run full short interest spike backtest for a list of symbols.
    Returns DataFrame of all spike events and their subsequent returns.
    """
    # Load FINRA data
    short_ratios = load_finra_for_symbols(symbols, start_date, end_date)

    if verbose:
        print(f"\nLoaded short ratio data for {len(short_ratios)}/{len(symbols)} symbols")

    all_events = []

    for symbol, sr_series in short_ratios.items():
        # Detect spikes
        events = detect_spike_events(sr_series, spike_threshold_sigma=spike_sigma,
                                      lookback=lookback)
        if not events:
            continue

        # Measure returns
        results = measure_post_spike_returns(symbol, events, horizons=horizons)
        all_events.extend(results)

    if not all_events:
        return pd.DataFrame()

    df = pd.DataFrame(all_events)
    return df


def compute_stats(df, horizons=[5, 10, 20]):
    """Compute summary statistics for the backtest results."""
    from scipy import stats

    stats_output = {}
    for h in horizons:
        col = f'abnormal_{h}d'
        if col not in df.columns:
            continue
        series = df[col].dropna()
        if len(series) < 5:
            continue

        mean = series.mean()
        median = series.median()
        std = series.std()
        pos_rate = (series > 0).mean() * 100
        n = len(series)

        # T-test
        t_stat, p_val = stats.ttest_1samp(series, 0)

        # Wilcoxon
        try:
            w_stat, w_p = stats.wilcoxon(series)
        except Exception:
            w_p = 1.0

        stats_output[f'{h}d'] = {
            'n': n,
            'mean': round(mean, 3),
            'median': round(median, 3),
            'std': round(std, 3),
            'pos_rate_pct': round(pos_rate, 1),
            'p_value': round(p_val, 4),
            'wilcoxon_p': round(w_p, 4),
        }

    return stats_output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Short interest spike signal backtester')
    parser.add_argument('--start', default='2022-01-01', help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default='2023-12-31', help='End date YYYY-MM-DD')
    parser.add_argument('--symbols', nargs='+', default=None,
                        help='Symbols to test (default: S&P 500 subset)')
    parser.add_argument('--sigma', type=float, default=2.0,
                        help='Z-score threshold for spike detection (default: 2.0)')
    parser.add_argument('--lookback', type=int, default=20,
                        help='Rolling window for baseline (default: 20 days)')
    parser.add_argument('--output', default=None, help='Save events to CSV file')
    args = parser.parse_args()

    # Default symbol universe: diverse S&P 500 stocks
    DEFAULT_SYMBOLS = [
        # Mega-cap tech
        'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'AMZN',
        # Financials
        'JPM', 'BAC', 'WFC', 'GS', 'MS',
        # Healthcare
        'JNJ', 'UNH', 'PFE', 'ABBV', 'MRK',
        # Consumer
        'AMZN', 'TSLA', 'HD', 'NKE', 'MCD',
        # Energy
        'XOM', 'CVX', 'COP',
        # Industrials
        'BA', 'CAT', 'DE', 'UNP',
        # Retail / discretionary
        'TGT', 'WMT', 'COST',
        # Semis
        'AMD', 'INTC', 'QCOM', 'AVGO',
    ]

    symbols = args.symbols if args.symbols else DEFAULT_SYMBOLS
    # Deduplicate
    symbols = list(dict.fromkeys(symbols))

    print(f"Short Interest Spike Signal Backtest")
    print(f"Symbols: {len(symbols)}")
    print(f"Period: {args.start} to {args.end}")
    print(f"Sigma threshold: {args.sigma}")
    print(f"Lookback: {args.lookback} days")
    print()

    df = run_backtest(
        symbols=symbols,
        start_date=args.start,
        end_date=args.end,
        spike_sigma=args.sigma,
        lookback=args.lookback,
        horizons=[5, 10, 20],
        verbose=True
    )

    if df.empty:
        print("No spike events found.")
        sys.exit(0)

    print(f"\nTotal spike events: {len(df)}")
    print(f"Symbols with events: {df['symbol'].nunique()}")
    print()

    # Summary stats
    stats = compute_stats(df)
    print("=== SUMMARY STATISTICS ===")
    print(f"{'Horizon':<10} {'N':<8} {'Mean%':<10} {'Median%':<10} {'Pos%':<8} {'p-val':<10} {'Wilcoxon_p':<12}")
    print("-" * 70)
    for h_label, s in stats.items():
        direction = 'NEGATIVE SIGNAL' if s['mean'] < -1.5 and s['p_value'] < 0.05 else \
                    'POSITIVE SIGNAL' if s['mean'] > 1.5 and s['p_value'] < 0.05 else 'NOISE'
        print(f"{h_label:<10} {s['n']:<8} {s['mean']:<10.3f} {s['median']:<10.3f} {s['pos_rate_pct']:<8.1f} {s['p_value']:<10.4f} {s['wilcoxon_p']:<12.4f}  {direction}")

    # Per-symbol summary
    print("\n=== PER-SYMBOL EVENT COUNT ===")
    sym_counts = df.groupby('symbol').size().sort_values(ascending=False)
    print(sym_counts.to_string())

    if args.output:
        df.to_csv(args.output, index=False)
        print(f"\nResults saved to {args.output}")
