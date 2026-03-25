#!/usr/bin/env python3
"""
First-Touch 52-Week Low Scanner
Scans S&P 500 stocks for TRUE first-touch 52-week low crossings.
A first-touch is when a stock's CLOSING price crosses below its 252-day rolling minimum
for the first time in 2+ years (using 45-day debounce to cluster nearby events).

Usage:
    python3 tools/first_touch_52w_low_scanner.py
    python3 tools/first_touch_52w_low_scanner.py --days 5  # Check last N days
    python3 tools/first_touch_52w_low_scanner.py --ticker BSX  # Check specific ticker
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from typing import Optional


def get_sp500_tickers() -> list:
    """Get S&P 500 tickers from Wikipedia."""
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = tables[0]
        return df['Symbol'].str.replace('.', '-').tolist()
    except Exception as e:
        print(f"Warning: Could not fetch S&P 500 list: {e}")
        # Fallback to common large caps
        return ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NVDA', 'TSLA', 'BRK-B', 
                'JPM', 'JNJ', 'V', 'UNH', 'HD', 'PG', 'BAC', 'MA', 'XOM', 'CVX']


def check_first_touch(ticker: str, lookback_days: int = 730, 
                      debounce_days: int = 45) -> dict:
    """
    Check if a stock has had a true first-touch 52w low crossing recently.
    
    Args:
        ticker: Stock ticker
        lookback_days: Days of history to analyze (default 2 years)
        debounce_days: Window to cluster crossings as same event
    
    Returns:
        Dict with keys:
            - ticker: str
            - is_first_touch: bool (True if current or recent crossing is first ever)
            - crossing_count: int (0 = never crossed, 1 = first-touch, N = repeated)
            - first_crossing_date: date or None
            - last_crossing_date: date or None
            - current_close: float
            - current_52w_low: float
            - pct_above_52w_low: float
            - market_cap_b: float or None
    """
    try:
        tk = yf.Ticker(ticker)
        start = str(date.today() - timedelta(days=lookback_days + 30))
        end = str(date.today() + timedelta(days=1))
        hist = tk.history(start=start, end=end)
        
        if len(hist) < 252:
            return {'ticker': ticker, 'error': 'insufficient_data', 'crossing_count': -1}
        
        # Rolling 252-day minimum of closing prices
        hist['rolling_52w_low'] = hist['Close'].rolling(252, min_periods=252).min()
        # Prior day's 52w low (to avoid lookahead)
        hist['prev_52w_low'] = hist['rolling_52w_low'].shift(1)
        
        # Find all crossings (close < prior 52w low)
        all_crossings = hist[hist['Close'] < hist['prev_52w_low']].copy()
        
        if len(all_crossings) == 0:
            current_close = float(hist['Close'].iloc[-1])
            current_52w_low = float(hist['rolling_52w_low'].iloc[-1])
            pct_above = (current_close / current_52w_low - 1) * 100
            return {
                'ticker': ticker,
                'is_first_touch': False,
                'crossing_count': 0,
                'first_crossing_date': None,
                'last_crossing_date': None,
                'current_close': round(current_close, 2),
                'current_52w_low': round(current_52w_low, 2),
                'pct_above_52w_low': round(pct_above, 2),
            }
        
        # Apply debouncing: cluster crossings within debounce_days as same event
        crossings = all_crossings.index.tolist()
        debounced_clusters = []
        cluster_start = crossings[0]
        for i in range(1, len(crossings)):
            days_since_last = (crossings[i] - crossings[i-1]).days
            if days_since_last > debounce_days:
                debounced_clusters.append(cluster_start)
                cluster_start = crossings[i]
        debounced_clusters.append(cluster_start)
        
        n_distinct_events = len(debounced_clusters)
        first_event = debounced_clusters[0].date()
        last_event = debounced_clusters[-1].date()
        
        current_close = float(hist['Close'].iloc[-1])
        current_52w_low = float(hist['rolling_52w_low'].iloc[-1])
        pct_above = (current_close / current_52w_low - 1) * 100
        
        # Is this a recent first touch? 
        # True if: only 1 event AND last crossing is within 10 trading days
        recent_threshold = date.today() - timedelta(days=14)
        is_first_touch = (n_distinct_events == 1) and (last_event >= recent_threshold)
        
        return {
            'ticker': ticker,
            'is_first_touch': is_first_touch,
            'crossing_count': n_distinct_events,
            'first_crossing_date': str(first_event),
            'last_crossing_date': str(last_event),
            'current_close': round(current_close, 2),
            'current_52w_low': round(current_52w_low, 2),
            'pct_above_52w_low': round(pct_above, 2),
        }
    except Exception as e:
        return {'ticker': ticker, 'error': str(e), 'crossing_count': -1}


def scan_for_first_touches(tickers: list, days_window: int = 7) -> list:
    """
    Scan a list of tickers for recent first-touch 52w low events.
    
    Returns list of dicts for tickers with recent first-touch crossings.
    """
    results = []
    for i, ticker in enumerate(tickers):
        if i % 50 == 0:
            print(f"  Scanning {i}/{len(tickers)}...")
        result = check_first_touch(ticker)
        
        # Filter: must be close to or at 52w low, and not a repeated event
        if result.get('crossing_count', -1) == 0:
            # Never crossed - approaching?
            if result.get('pct_above_52w_low', 999) < 2.0:
                result['status'] = 'approaching_never_crossed'
                results.append(result)
        elif result.get('crossing_count') == 1:
            result['status'] = 'first_touch_event'
            results.append(result)
    
    return sorted(results, key=lambda x: x.get('pct_above_52w_low', 999))


def main():
    parser = argparse.ArgumentParser(description='First-Touch 52W Low Scanner')
    parser.add_argument('--days', type=int, default=7, help='Days window for recent crossings')
    parser.add_argument('--ticker', type=str, help='Check specific ticker')
    parser.add_argument('--full-scan', action='store_true', help='Scan full S&P 500')
    args = parser.parse_args()
    
    if args.ticker:
        result = check_first_touch(args.ticker)
        print(f"\n{args.ticker} Analysis:")
        for k, v in result.items():
            print(f"  {k}: {v}")
        return
    
    # Quick check of common 52w low candidates
    candidates = [
        'SYK', 'KHC', 'HD', 'ABT', 'BAX', 'V', 'ADBE', 'DPZ', 'SBAC', 'CPB', 
        'BSX', 'OTIS', 'NKE', 'MKC', 'CAG', 'PGR', 'PG', 'POOL', 'WMT', 'AMZN',
        'JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', 'BX', 'KKR', 'SCHW', 'CME',
        'META', 'GOOGL', 'MSFT', 'AAPL', 'TSLA', 'NVDA', 'AMD', 'INTC', 'QCOM',
        'UPS', 'FDX', 'DAL', 'UAL', 'AAL', 'LUV', 'XOM', 'CVX', 'COP', 'OXY',
        'MDT', 'STE', 'TMO', 'ZBH', 'EW', 'RMD', 'DXCM', 'HOLX', 'IQV',
        'COST', 'TGT', 'LOW', 'DG', 'DLTR', 'KR', 'ACI', 'SFM',
        'RTX', 'LMT', 'NOC', 'GD', 'BA', 'HII', 'TDG', 'HEI',
        'BIIB', 'REGN', 'VRTX', 'GILD', 'AMGN', 'ABBV', 'BMY', 'MRK', 'PFE',
        'JNPR', 'HPE', 'IBM', 'ACN', 'CSCO', 'ANET', 'FTNT', 'PANW', 'CRWD',
        'HOG', 'F', 'GM', 'TSLA', 'APTV', 'BWA', 'LEA', 'MOD'
    ]
    
    if args.full_scan:
        print("Fetching full S&P 500 list...")
        candidates = get_sp500_tickers()
    
    print(f"Scanning {len(candidates)} tickers for first-touch 52w low events...")
    results = scan_for_first_touches(candidates, args.days)
    
    print(f"\n=== FIRST-TOUCH OR APPROACHING-NEVER-CROSSED ({len(results)} found) ===")
    for r in results:
        status = r.get('status', 'unknown')
        crossings = r.get('crossing_count', '?')
        pct = r.get('pct_above_52w_low', '?')
        print(f"  {r['ticker']}: {status} | crossings={crossings} | {pct}% above 52w_low | "
              f"close={r.get('current_close')} | first={r.get('first_crossing_date')} | last={r.get('last_crossing_date')}")


if __name__ == '__main__':
    main()
