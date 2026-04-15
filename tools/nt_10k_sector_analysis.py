#!/usr/bin/env python3
"""Analyze NT 10-K late filing signal by sector.
Checks if some sectors have stronger negative drift than others."""

import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.nt_filing_scanner import scan_nt_filings, tag_first_time_filers
from tools.yfinance_utils import safe_download
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

def get_sector(ticker):
    """Get sector for a ticker using yfinance."""
    try:
        info = yf.Ticker(ticker).info
        return info.get('sector', 'Unknown')
    except Exception:
        return 'Unknown'

def main():
    # Get all NT 10-K filings 2022-2025, large-cap, first-time only
    print("Scanning NT 10-K filings 2022-2025...", file=sys.stderr)
    filings = scan_nt_filings('2022-01-01', '2025-12-31', filter_largecap=True, min_cap=500)
    filings = tag_first_time_filers(filings)
    first_time = [f for f in filings if f.get('is_first_time_filer', False)]
    print(f"Found {len(first_time)} first-time NT 10-K filers (large-cap)", file=sys.stderr)
    
    # Get sectors for each ticker
    tickers = list(set(f['ticker'] for f in first_time))
    print(f"Looking up sectors for {len(tickers)} tickers...", file=sys.stderr)
    sector_map = {}
    for t in tickers:
        sector_map[t] = get_sector(t)
    
    # Compute abnormal returns by sector
    sector_returns = defaultdict(list)
    for f in first_time:
        ticker = f['ticker']
        date = f['file_date']
        sector = sector_map.get(ticker, 'Unknown')
        
        # Get prices
        start = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
        end = (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=15)).strftime('%Y-%m-%d')
        
        stock_df = safe_download(ticker, start=start, end=end)
        spy_df = safe_download('SPY', start=start, end=end)
        
        if stock_df is None or spy_df is None or len(stock_df) < 3 or len(spy_df) < 3:
            continue
        
        # Find entry day (filing date or next trading day)
        file_dt = datetime.strptime(date, '%Y-%m-%d')
        entry_idx = stock_df.index.searchsorted(pd.Timestamp(file_dt))
        if entry_idx >= len(stock_df):
            continue
            
        entry_price = float(stock_df['Close'].iloc[entry_idx])
        spy_entry = float(spy_df['Close'].iloc[min(entry_idx, len(spy_df)-1)])
        
        # 3-day return
        if entry_idx + 3 < len(stock_df):
            stock_ret_3d = (float(stock_df['Close'].iloc[entry_idx + 3]) - entry_price) / entry_price * 100
            spy_ret_3d = (float(spy_df['Close'].iloc[min(entry_idx + 3, len(spy_df)-1)]) - spy_entry) / spy_entry * 100
            abnormal_3d = stock_ret_3d - spy_ret_3d
            sector_returns[sector].append(abnormal_3d)
    
    # Print results by sector
    print("\n=== NT 10-K 3d Abnormal Return by Sector ===")
    results = {}
    for sector in sorted(sector_returns.keys()):
        returns = sector_returns[sector]
        n = len(returns)
        if n < 3:
            continue
        avg = sum(returns) / n
        neg_rate = sum(1 for r in returns if r < -0.5) / n * 100
        results[sector] = {"n": n, "avg_3d": round(avg, 2), "neg_rate_pct": round(neg_rate, 1)}
        print(f"  {sector:25s}: n={n:3d}, avg={avg:+6.2f}%, neg_rate={neg_rate:5.1f}%")
    
    print(json.dumps(results))

if __name__ == '__main__':
    main()
