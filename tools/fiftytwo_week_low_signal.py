"""
52-Week Low First-Touch Signal Backtest

Tests whether S&P 500 large-cap stocks show mean-reversion (bounce)
after first breaking to a new 52-week low (after 30+ days above).

Signal rationale:
1. Behavioral: forced sellers (margin calls, stop-losses) create oversold conditions
2. Value: long-term investors view 52-week lows as entry points
3. Literature: De Bondt & Thaler (1985) found 3-5 year reversal for extreme losers;
   George & Hwang (2004) used 52-week high as reference — the inverse (52-week low)
   implies pessimistic anchoring that can revert.

Event definition: stock's daily close falls BELOW 52-week low for first time
after at least 30 consecutive days above the 52-week low level.

Entry: next day open (stock is already at low, don't chase further decline)
Hold: 5, 10, 20 trading days
Benchmark: SPY abnormal return over same period
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import time
import market_data


# S&P 500 large-cap sample - covering different sectors
TICKERS = [
    # Tech
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO',
    'IBM', 'CSCO', 'ORCL', 'CRM', 'NOW', 'ADBE',
    # Financials
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'USB', 'PNC', 'TFC', 'AXP',
    # Healthcare
    'JNJ', 'PFE', 'MRK', 'ABT', 'BMY', 'ABBV', 'LLY', 'MDT', 'UNH',
    # Consumer
    'AMZN', 'WMT', 'TGT', 'HD', 'LOW', 'NKE', 'MCD', 'SBUX', 'KO', 'PEP',
    # Energy
    'XOM', 'CVX', 'COP', 'SLB', 'EOG',
    # Industrials
    'BA', 'GE', 'MMM', 'CAT', 'HON', 'LMT', 'RTX', 'UPS', 'FDX',
    # Communication
    'VZ', 'T', 'CMCSA', 'DIS',
    # Materials / Other
    'FCX', 'NUE', 'DD',
]


def find_52w_low_events(ticker: str, start: str = '2020-01-01', end: str = '2025-12-31',
                         debounce_days: int = 30) -> list:
    """
    Find all first-touch 52-week low events for a ticker.
    
    Returns list of dicts: {'ticker': ..., 'event_date': ..., 'close': ...}
    """
    try:
        df = yf.download(ticker, start=start, end=end, progress=False)
        if df.empty:
            return []
        
        # Flatten MultiIndex if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        if 'Close' not in df.columns:
            return []
            
        # Rolling 252-day low (shift(1) = exclude today, use only past data)
        df['52w_low'] = df['Close'].shift(1).rolling(252, min_periods=100).min()
        
        # Is today's close at or below 52-week low?
        df['at_52w_low'] = df['Close'] <= df['52w_low']
        
        # First touch: at_52w_low today but NOT in prior debounce_days
        df['days_since_last_low'] = (
            df['at_52w_low'].shift(1)
            .rolling(debounce_days, min_periods=1)
            .sum()
        )
        df['first_touch'] = (
            df['at_52w_low'] & 
            (df['days_since_last_low'] == 0) &
            df['52w_low'].notna()
        )
        
        events = []
        for idx in df[df['first_touch']].index:
            events.append({
                'ticker': ticker,
                'event_date': idx.strftime('%Y-%m-%d'),
                'close': float(df.loc[idx, 'Close']),
                '52w_low_level': float(df.loc[idx, '52w_low']),
                'pct_below_52w_low': float((df.loc[idx, 'Close'] / df.loc[idx, '52w_low'] - 1) * 100),
            })
        return events
    except Exception as e:
        print(f"  ERROR {ticker}: {e}")
        return []


def main():
    print("=== 52-Week Low First-Touch Signal Backtest ===")
    print(f"Universe: {len(TICKERS)} S&P 500 large-cap stocks")
    print("Period: 2020-01-01 to 2025-12-31")
    print()
    
    all_events = []
    for i, ticker in enumerate(TICKERS):
        events = find_52w_low_events(ticker)
        if events:
            all_events.extend(events)
            print(f"  {ticker}: {len(events)} events")
        else:
            print(f"  {ticker}: 0 events")
        time.sleep(0.05)  # Rate limit
    
    if not all_events:
        print("No events found!")
        return
    
    df = pd.DataFrame(all_events)
    df.sort_values('event_date', inplace=True)
    print(f"\nTotal events: {len(df)}")
    print(f"Date range: {df['event_date'].min()} to {df['event_date'].max()}")
    print(f"Unique tickers: {df['ticker'].nunique()}")
    
    # Save events for inspection
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', '52w_low_events.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\nEvents saved to: {output_path}")
    
    # Prepare events for market_data.measure_event_impact()
    event_dates = [
        {'symbol': row['ticker'], 'date': row['event_date']}
        for _, row in df.iterrows()
    ]
    
    print(f"\nRunning backtest on {len(event_dates)} events...")
    print("Entry: next day open (entry_price='open')")
    print()
    
    result = market_data.measure_event_impact(
        event_dates=event_dates,
        benchmark='SPY',
        sector_etf=None,
        entry_price='open',
        estimate_costs=True,
        event_type='52w_low_first_touch',
    )
    
    print("\n=== RESULTS ===")
    print(f"Events measured: {result['events_measured']}")
    print(f"Data quality warning: {result.get('data_quality_warning')}")
    print()
    
    for h in [1, 3, 5, 10, 20]:
        avg = result.get(f'avg_abnormal_{h}d', 'N/A')
        med = result.get(f'median_abnormal_{h}d', 'N/A')
        pos = result.get(f'positive_rate_abnormal_{h}d', 'N/A')
        p = result.get(f'wilcoxon_p_abnormal_{h}d', result.get(f'p_value_abnormal_{h}d', 'N/A'))
        if avg != 'N/A':
            print(f"  {h:2d}d: avg={avg:+.2f}%  median={med:+.2f}%  pos_rate={pos:.1f}%  p={p:.4f}")
    
    print(f"\nPasses multiple testing: {result.get('passes_multiple_testing')}")
    print(f"Bootstrap CI 5d: {result.get('bootstrap_ci_abnormal_5d', {})}")
    
    return result


if __name__ == '__main__':
    main()
