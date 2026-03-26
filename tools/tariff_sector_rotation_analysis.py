"""
Tariff Sector Rotation Analysis
================================
Analyze how different sector ETFs perform 1d, 5d, 10d, 20d after tariff escalation events.
Goal: Find additional sectors to trade around April 2 Liberation Day.

Tariff escalation events used:
 - 2018-03-01: Steel/aluminum tariffs announced
 - 2018-07-06: $34B China tariffs take effect
 - 2018-09-17: $200B China tariffs announced
 - 2019-05-05: Trump tweets tariff escalation tweet
 - 2019-08-01: New $300B tariffs tweet
 - 2025-02-01: Mexico/Canada tariff announcement
 - 2025-03-04: Mexico/Canada tariffs take effect
 - 2026-03-06: Broad tariff signals (VIX hit 29.5)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta

# Sector ETFs to test
SECTOR_ETFS = {
    'XLK': 'Technology',
    'XLF': 'Financials',
    'XLV': 'Healthcare',
    'XLY': 'Consumer Discretionary',
    'XLP': 'Consumer Staples',
    'XLI': 'Industrials',
    'XLB': 'Materials',
    'XLE': 'Energy',
    'XLRE': 'Real Estate',
    'XLU': 'Utilities',
    'XLC': 'Communications',
    'GLD': 'Gold ETF',
    'TLT': 'Long-Term Bonds',
    'IYT': 'Transportation',
    'XHB': 'Homebuilders',
    'KRE': 'Regional Banks',
    'ITB': 'Homebuilding',
    'SOXX': 'Semiconductors',
}

# Historical tariff escalation dates
TARIFF_EVENTS = [
    '2018-03-01',  # Steel/aluminum tariffs
    '2018-07-06',  # $34B China tariffs
    '2018-09-17',  # $200B China tariffs
    '2019-05-05',  # Trump tariff escalation tweet
    '2019-08-01',  # $300B tariffs tweet
    '2025-02-01',  # Mexico/Canada tariff announcement
    '2025-03-04',  # Mexico/Canada tariffs take effect
    '2026-03-06',  # Broad tariff signals (VIX spike)
]

def get_abnormal_return(ticker, event_date, horizon_days, spy_data):
    """Calculate abnormal return vs SPY over horizon_days starting from event_date."""
    try:
        ticker_data = yf.download(ticker, 
                                   start=(pd.Timestamp(event_date) - timedelta(days=5)).strftime('%Y-%m-%d'),
                                   end=(pd.Timestamp(event_date) + timedelta(days=horizon_days + 10)).strftime('%Y-%m-%d'),
                                   auto_adjust=True, progress=False)
        
        if isinstance(ticker_data.columns, pd.MultiIndex):
            ticker_close = ticker_data['Close'][ticker].dropna()
        else:
            ticker_close = ticker_data['Close'].dropna()
        
        if len(ticker_close) < 2:
            return None
        
        # Find entry price (close on event date, or next trading day)
        event_ts = pd.Timestamp(event_date)
        ticker_close.index = pd.to_datetime(ticker_close.index).tz_localize(None)
        
        # Find the entry date (event date or next available)
        future_dates = ticker_close.index[ticker_close.index >= event_ts]
        if len(future_dates) == 0:
            return None
        entry_date = future_dates[0]
        
        # Find exit date (horizon_days trading days later)
        entry_idx = ticker_close.index.get_loc(entry_date)
        exit_idx = entry_idx + horizon_days
        if exit_idx >= len(ticker_close):
            return None
        exit_date = ticker_close.index[exit_idx]
        
        # Calculate returns
        ticker_return = (ticker_close.iloc[exit_idx] / ticker_close.iloc[entry_idx]) - 1
        
        # Get SPY return for same period
        spy_close = spy_data.copy()
        spy_close.index = pd.to_datetime(spy_close.index).tz_localize(None)
        spy_future = spy_close[spy_close.index >= event_ts]
        if len(spy_future) == 0:
            return None
        spy_entry = spy_future.iloc[0]
        spy_exit_dates = spy_close[spy_close.index >= exit_date]
        if len(spy_exit_dates) == 0:
            return None
        spy_exit = spy_exit_dates.iloc[0]
        spy_return = (spy_exit / spy_entry) - 1
        
        return ticker_return - spy_return
        
    except Exception as e:
        return None

def main():
    print("=" * 70)
    print("TARIFF SECTOR ROTATION ANALYSIS")
    print(f"Events: {len(TARIFF_EVENTS)} tariff escalation dates")
    print("=" * 70)
    
    # Download SPY once
    spy_data = yf.download('SPY', start='2017-01-01', end=datetime.now().strftime('%Y-%m-%d'),
                           auto_adjust=True, progress=False)
    if isinstance(spy_data.columns, pd.MultiIndex):
        spy_close = spy_data['Close']['SPY'].dropna()
    else:
        spy_close = spy_data['Close'].dropna()
    
    print(f"\nSPY data loaded: {spy_close.index[0].date()} to {spy_close.index[-1].date()}")
    
    results = {}
    
    for ticker, sector in SECTOR_ETFS.items():
        results[ticker] = {}
        for horizon in [1, 5, 10, 20]:
            returns = []
            for event_date in TARIFF_EVENTS:
                ret = get_abnormal_return(ticker, event_date, horizon, spy_close)
                if ret is not None:
                    returns.append(ret)
            
            if len(returns) >= 4:
                arr = np.array(returns)
                stat, p_val = stats.wilcoxon(arr) if len(arr) >= 5 else (None, None)
                if stat is None:
                    _, p_val = stats.ttest_1samp(arr, 0)
                direction = (arr > 0.005).sum() / len(arr)  # >0.5% threshold
                results[ticker][horizon] = {
                    'n': len(arr),
                    'avg': arr.mean() * 100,
                    'direction': direction,
                    'p_val': p_val,
                    'returns': returns
                }
    
    # Print results sorted by 5d average
    print("\n--- SECTOR RETURNS AFTER TARIFF ESCALATIONS ---")
    print(f"{'Sector':<12} {'Name':<25} {'1d avg':>8} {'5d avg':>8} {'10d avg':>9} {'20d avg':>9} {'5d dir':>7} {'5d p':>7}")
    print("-" * 90)
    
    # Sort by 5d average
    sector_avgs = []
    for ticker, data in results.items():
        if 5 in data:
            sector_avgs.append((ticker, data[5]['avg']))
    sector_avgs.sort(key=lambda x: x[1])
    
    for ticker, avg5d in sector_avgs:
        data = results[ticker]
        r1 = f"{data.get(1, {}).get('avg', float('nan')):.2f}%" if 1 in data else "N/A"
        r5 = f"{data.get(5, {}).get('avg', float('nan')):.2f}%" if 5 in data else "N/A"
        r10 = f"{data.get(10, {}).get('avg', float('nan')):.2f}%" if 10 in data else "N/A"
        r20 = f"{data.get(20, {}).get('avg', float('nan')):.2f}%" if 20 in data else "N/A"
        dir5 = f"{data.get(5, {}).get('direction', 0):.0%}" if 5 in data else "N/A"
        p5 = f"{data.get(5, {}).get('p_val', 1):.3f}" if 5 in data else "N/A"
        name = SECTOR_ETFS[ticker]
        print(f"{ticker:<12} {name:<25} {r1:>8} {r5:>8} {r10:>9} {r20:>9} {dir5:>7} {p5:>7}")
    
    print("\n--- STRONG SIGNALS (5d |avg| > 0.5%, p < 0.10) ---")
    for ticker, data in results.items():
        if 5 in data:
            d = data[5]
            if abs(d['avg']) > 0.5 and (d['p_val'] or 1) < 0.10:
                print(f"\n{ticker} ({SECTOR_ETFS[ticker]}): avg_5d={d['avg']:.2f}%, direction={d['direction']:.0%}, p={d['p_val']:.3f}, n={d['n']}")
                # Show by horizon
                for h in [1, 5, 10, 20]:
                    if h in data[ticker] if ticker in data else h in data:
                        hd = data[h]
                        print(f"  {h}d: avg={hd['avg']:.2f}%, direction={hd['direction']:.0%}, p={hd.get('p_val', 'N/A')}")
    
    return results

if __name__ == '__main__':
    main()
