#!/usr/bin/env python3
"""
Insider Cluster Multi-Horizon Analysis
=======================================
Given the cached 5d feature analysis (5,364 clusters), compute 10d and 20d
abnormal returns for the CEO+3-5 insiders subset. Also filters by market cap
to assess large-cap signal strength.

Reads from: /tmp/insider_cluster_features.json (5d cache)
Outputs to: /tmp/insider_cluster_horizons.json

Market cap is fetched at current price (best available) — introduces slight
lookahead bias for older clusters but acceptable for signal characterization.
"""

import json
import os
import sys
import time
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.yfinance_utils import safe_download

OUTPUT_PATH = "/tmp/insider_cluster_horizons.json"
INPUT_PATH = "/tmp/insider_cluster_features.json"


def get_market_cap(ticker):
    """Get current market cap in millions. Returns None on failure."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        mcap = info.get("marketCap")
        if mcap and mcap > 0:
            return mcap / 1e6
    except Exception:
        pass
    return None


def compute_multi_horizon_returns(clusters, horizons=[5, 10, 20]):
    """Compute abnormal returns at multiple horizons for a list of clusters."""
    tickers = list(set(c['ticker'] for c in clusters))
    print(f"Fetching prices for {len(tickers)} tickers + SPY...", file=sys.stderr)

    # Date range with buffer for 20d hold
    dates = [datetime.strptime(c['cluster_date'], '%Y-%m-%d') for c in clusters]
    min_date = min(dates) - timedelta(days=10)
    max_date = max(dates) + timedelta(days=35)  # 20d + buffer

    # Fetch SPY
    spy = safe_download('SPY', start=min_date.strftime('%Y-%m-%d'), end=max_date.strftime('%Y-%m-%d'))
    if spy is None or spy.empty:
        print("ERROR: Cannot fetch SPY data", file=sys.stderr)
        return []
    spy_close = spy['Close']

    # Fetch stock prices in batches
    price_cache = {}
    fetched = 0
    for ticker in tickers:
        try:
            df = safe_download(ticker, start=min_date.strftime('%Y-%m-%d'), end=max_date.strftime('%Y-%m-%d'))
            if df is not None and not df.empty:
                price_cache[ticker] = df['Close']
                fetched += 1
        except Exception:
            pass
        if fetched % 100 == 0 and fetched > 0:
            print(f"  Fetched {fetched}/{len(tickers)} tickers", file=sys.stderr)

    print(f"  Price data for {len(price_cache)}/{len(tickers)} tickers", file=sys.stderr)

    results = []
    for c in clusters:
        ticker = c['ticker']
        if ticker not in price_cache:
            continue

        prices = price_cache[ticker]
        cdate_str = c['cluster_date']
        cdate = datetime.strptime(cdate_str, '%Y-%m-%d')

        # Find entry
        future_prices = prices[prices.index >= cdate_str]
        spy_future = spy_close[spy_close.index >= cdate_str]

        if len(future_prices) < 2 or len(spy_future) < 2:
            continue

        entry_price = float(future_prices.iloc[0])
        spy_entry = float(spy_future.iloc[0])

        row = {
            'ticker': ticker,
            'cluster_date': cdate_str,
            'n_insiders': c['n_insiders'],
            'has_ceo_cfo': c['has_ceo_cfo'],
            'total_value': c['total_value'],
            'vix_at_cluster': c.get('vix_at_cluster'),
        }

        for h in horizons:
            if len(future_prices) > h:
                exit_price = float(future_prices.iloc[h])
                raw_ret = (exit_price - entry_price) / entry_price * 100

                if len(spy_future) > h:
                    spy_exit = float(spy_future.iloc[h])
                    spy_ret = (spy_exit - spy_entry) / spy_entry * 100
                else:
                    spy_ret = 0.0

                row[f'abnormal_{h}d'] = round(raw_ret - spy_ret, 2)
            else:
                row[f'abnormal_{h}d'] = None

        results.append(row)

    return results


def analyze_by_horizon(results, horizons=[5, 10, 20]):
    """Analyze returns by horizon, with large-cap filter."""
    analysis = {}

    for h in horizons:
        key = f'{h}d'
        valid = [r for r in results if r.get(f'abnormal_{h}d') is not None]
        returns = [r[f'abnormal_{h}d'] for r in valid]

        if not returns:
            analysis[key] = {'n': 0}
            continue

        arr = np.array(returns)
        analysis[key] = {
            'n': len(arr),
            'avg': round(float(np.mean(arr)), 2),
            'median': round(float(np.median(arr)), 2),
            'positive_rate': round(float(np.mean(arr > 0.5) * 100), 1),
            'std': round(float(np.std(arr)), 2),
            'p_value': _ttest_p(arr),
        }

    return analysis


def _ttest_p(arr):
    """One-sample t-test p-value."""
    from scipy import stats
    if len(arr) < 3:
        return 1.0
    t, p = stats.ttest_1samp(arr, 0)
    return round(float(p), 6)


def main():
    # Load cached 5d results
    print(f"Loading {INPUT_PATH}...", file=sys.stderr)
    with open(INPUT_PATH) as f:
        data = json.load(f)

    clusters = data['clusters']
    print(f"Total clusters: {len(clusters)}", file=sys.stderr)

    # Filter to CEO+3-5 (our trading filter)
    ceo_3_5 = [c for c in clusters if c['has_ceo_cfo'] and 3 <= c['n_insiders'] <= 5]
    print(f"CEO+3-5 insiders: {len(ceo_3_5)}", file=sys.stderr)

    # Compute multi-horizon returns
    results = compute_multi_horizon_returns(ceo_3_5, horizons=[5, 10, 20])
    print(f"\nResults with valid prices: {len(results)}", file=sys.stderr)

    # Overall analysis
    overall = analyze_by_horizon(results)
    print(f"\n=== CEO+3-5 Overall (N={len(results)}) ===", file=sys.stderr)
    for h in [5, 10, 20]:
        a = overall[f'{h}d']
        print(f"  {h}d: avg={a.get('avg')}% median={a.get('median')}% pos={a.get('positive_rate')}% p={a.get('p_value')}", file=sys.stderr)

    # Large-cap analysis (use tickers that have market cap data in the results)
    # For efficiency, fetch market caps for unique tickers
    tickers = list(set(r['ticker'] for r in results))
    print(f"\nFetching market caps for {len(tickers)} tickers...", file=sys.stderr)

    mcap_cache = {}
    for i, ticker in enumerate(tickers):
        mcap = get_market_cap(ticker)
        if mcap is not None:
            mcap_cache[ticker] = mcap
        if (i + 1) % 50 == 0:
            print(f"  Market cap: {i+1}/{len(tickers)}", file=sys.stderr)
            time.sleep(0.5)  # Rate limit yfinance

    print(f"  Market cap data for {len(mcap_cache)}/{len(tickers)} tickers", file=sys.stderr)

    # Split by market cap
    largecap = [r for r in results if mcap_cache.get(r['ticker'], 0) >= 500]
    smallcap = [r for r in results if r['ticker'] in mcap_cache and mcap_cache[r['ticker']] < 500]

    largecap_analysis = analyze_by_horizon(largecap)
    smallcap_analysis = analyze_by_horizon(smallcap)

    print(f"\n=== Large-cap (>$500M): N={len(largecap)} ===", file=sys.stderr)
    for h in [5, 10, 20]:
        a = largecap_analysis[f'{h}d']
        print(f"  {h}d: avg={a.get('avg')}% median={a.get('median')}% pos={a.get('positive_rate')}% p={a.get('p_value')}", file=sys.stderr)

    print(f"\n=== Small-cap (<$500M): N={len(smallcap)} ===", file=sys.stderr)
    for h in [5, 10, 20]:
        a = smallcap_analysis[f'{h}d']
        print(f"  {h}d: avg={a.get('avg')}% median={a.get('median')}% pos={a.get('positive_rate')}% p={a.get('p_value')}", file=sys.stderr)

    # VIX tier analysis at each horizon
    vix_tiers = {
        'vix_lt20': lambda r: r.get('vix_at_cluster') is not None and r['vix_at_cluster'] < 20,
        'vix_20_25': lambda r: r.get('vix_at_cluster') is not None and 20 <= r['vix_at_cluster'] < 25,
        'vix_25_30': lambda r: r.get('vix_at_cluster') is not None and 25 <= r['vix_at_cluster'] < 30,
        'vix_gt30': lambda r: r.get('vix_at_cluster') is not None and r['vix_at_cluster'] >= 30,
    }

    vix_analysis = {}
    for tier_name, tier_filter in vix_tiers.items():
        tier_results = [r for r in results if tier_filter(r)]
        vix_analysis[tier_name] = {
            'n': len(tier_results),
            **analyze_by_horizon(tier_results)
        }
        print(f"\n=== {tier_name} (N={len(tier_results)}) ===", file=sys.stderr)
        for h in [5, 10, 20]:
            a = vix_analysis[tier_name].get(f'{h}d', {})
            if a.get('n', 0) > 0:
                print(f"  {h}d: avg={a.get('avg')}% pos={a.get('positive_rate')}% p={a.get('p_value')}", file=sys.stderr)

    # Save output
    output = {
        'timestamp': datetime.now().isoformat(),
        'ceo_3_5_overall': {'n': len(results), **overall},
        'largecap_gt500m': {'n': len(largecap), **largecap_analysis},
        'smallcap_lt500m': {'n': len(smallcap), **smallcap_analysis},
        'vix_tiers': vix_analysis,
        'mcap_coverage': f"{len(mcap_cache)}/{len(tickers)}",
    }

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to {OUTPUT_PATH}", file=sys.stderr)

    # Print summary to stdout for piping
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
