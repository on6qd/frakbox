"""
Post-Earnings Announcement Drift (PEAD) Backtest
=================================================
Tests whether stocks that significantly beat earnings continue to drift upward
over 1, 3, 5, and 10 trading days.

Signal: EPS surprise > threshold (e.g., >5%, >10%)
Direction: LONG for positive surprise, SHORT for negative surprise
Benchmark: SPY abnormal return

Academic: Ball & Brown (1968), Bernard & Thomas (1989) - PEAD is one of the
most robust market anomalies in finance literature.

Causal mechanism:
1. Actors/Incentives: Institutional investors slow to rebalance, analysts revise
   gradually, retail investors discover results over days
2. Transmission channel: Gradual repricing as more investors read/act on results
3. Academic: PEAD is the "anomaly of anomalies" - extensively documented
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tools.yfinance_utils import safe_download, get_close_prices
from tools.largecap_filter import filter_to_largecap

def get_sp500_tickers():
    """Load S&P 500 tickers from local file."""
    try:
        with open('tools/sp500_tickers.txt', 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except:
        # Fallback: use a broad ETF-based list
        return None

def collect_earnings_surprises(tickers, lookback_years=3, min_surprise_pct=5.0):
    """Collect earnings events with significant EPS surprises."""
    events = []
    start_date = (datetime.now() - timedelta(days=lookback_years*365)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    processed = 0
    errors = 0
    
    for ticker in tickers[:100]:  # Cap at 100 for speed
        try:
            t = yf.Ticker(ticker)
            dates = t.earnings_dates
            if dates is None or len(dates) == 0:
                continue
            
            # Filter to historical dates (not future)
            dates = dates[dates.index < pd.Timestamp.now(tz='America/New_York')]
            
            for dt, row in dates.iterrows():
                eps_est = row.get('EPS Estimate')
                eps_act = row.get('Reported EPS')
                surprise_pct = row.get('Surprise(%)')
                
                if pd.isna(eps_est) or pd.isna(eps_act) or pd.isna(surprise_pct):
                    continue
                
                # Only significant surprises
                if abs(surprise_pct) < min_surprise_pct:
                    continue
                
                # Convert tz-aware to tz-naive date string
                event_date = dt.strftime('%Y-%m-%d')
                if event_date < start_date:
                    continue
                
                direction = 'long' if surprise_pct > 0 else 'short'
                events.append({
                    'ticker': ticker,
                    'event_date': event_date,
                    'eps_estimate': eps_est,
                    'eps_actual': eps_act,
                    'surprise_pct': surprise_pct,
                    'direction': direction,
                })
            
            processed += 1
        except Exception as e:
            errors += 1
    
    print(f"Processed {processed} tickers, {errors} errors, {len(events)} events found")
    return events


def measure_pead_returns(events, horizons=[1, 3, 5, 10]):
    """Measure abnormal returns after each earnings event."""
    results = []
    
    # Get SPY data for benchmark
    spy = safe_download('SPY', start='2022-01-01', end=datetime.now().strftime('%Y-%m-%d'))
    if spy is None:
        print("ERROR: Could not get SPY data")
        return []
    spy_closes = spy['Close']
    
    for ev in events:
        ticker = ev['ticker']
        event_date = ev['event_date']
        
        try:
            # Get stock data
            start = (pd.Timestamp(event_date) - timedelta(days=5)).strftime('%Y-%m-%d')
            end = (pd.Timestamp(event_date) + timedelta(days=25)).strftime('%Y-%m-%d')
            
            df = safe_download(ticker, start=start, end=end)
            if df is None or len(df) < 5:
                continue
            
            df.index = pd.to_datetime(df.index).tz_localize(None)
            
            # Find the trading day of or after event (earnings often released pre-market)
            event_ts = pd.Timestamp(event_date)
            future_dates = df.index[df.index >= event_ts]
            if len(future_dates) == 0:
                continue
            
            # Use the OPEN of next trading day as entry (like a realistic trade)
            entry_date = future_dates[0]
            entry_idx = df.index.get_loc(entry_date)
            
            if entry_idx >= len(df):
                continue
            
            entry_price = df['Open'].iloc[entry_idx]
            
            for horizon in horizons:
                exit_idx = entry_idx + horizon
                if exit_idx >= len(df):
                    continue
                
                exit_price = df['Close'].iloc[exit_idx]
                stock_return = (exit_price - entry_price) / entry_price
                
                # SPY benchmark
                spy_filtered = spy_closes[spy_closes.index >= entry_date]
                spy_entry = spy_filtered.iloc[0] if len(spy_filtered) > 0 else None
                
                if spy_entry is None:
                    continue
                    
                spy_future = spy_filtered.iloc[horizon] if len(spy_filtered) > horizon else None
                if spy_future is None:
                    spy_return = 0
                else:
                    spy_return = (spy_future - spy_entry) / spy_entry
                
                # For short direction, flip the return
                if ev['direction'] == 'long':
                    abnormal = stock_return - spy_return
                else:
                    abnormal = -(stock_return - spy_return)
                
                results.append({
                    'ticker': ticker,
                    'event_date': event_date,
                    'horizon': horizon,
                    'direction': ev['direction'],
                    'surprise_pct': ev['surprise_pct'],
                    'stock_return': stock_return * 100,
                    'spy_return': spy_return * 100,
                    'abnormal_return': abnormal * 100,
                    'direction_correct': abnormal > 0,
                })
        except Exception as e:
            continue
    
    return results


def analyze_results(results, min_surprise=5.0):
    """Analyze PEAD results with statistical tests."""
    from scipy import stats
    
    df = pd.DataFrame(results)
    if df.empty:
        print("No results to analyze")
        return
    
    print(f"\n=== PEAD Backtest Results (surprise >= {min_surprise}%) ===")
    print(f"Total observations: {len(df)}")
    print(f"Unique tickers: {df['ticker'].nunique()}")
    print(f"Date range: {df['event_date'].min()} to {df['event_date'].max()}")
    print()
    
    for horizon in sorted(df['horizon'].unique()):
        h_df = df[df['horizon'] == horizon]
        n = len(h_df)
        mean_ab = h_df['abnormal_return'].mean()
        std_ab = h_df['abnormal_return'].std()
        direction_pct = h_df['direction_correct'].mean() * 100
        
        t_stat, p_val = stats.ttest_1samp(h_df['abnormal_return'], 0)
        
        long_df = h_df[h_df['direction'] == 'long']
        short_df = h_df[h_df['direction'] == 'short']
        
        print(f"Horizon {horizon}d: n={n} | mean={mean_ab:+.2f}% | dir={direction_pct:.0f}% | p={p_val:.3f}")
        if len(long_df) > 0:
            print(f"  LONG (beats): n={len(long_df)} | mean={long_df['abnormal_return'].mean():+.2f}% | dir={long_df['direction_correct'].mean()*100:.0f}%")
        if len(short_df) > 0:
            print(f"  SHORT (misses): n={len(short_df)} | mean={short_df['abnormal_return'].mean():+.2f}% | dir={short_df['direction_correct'].mean()*100:.0f}%")
    
    # Test by surprise magnitude
    print("\n=== By Surprise Magnitude (LONG events only) ===")
    long_df = df[(df['direction'] == 'long') & (df['horizon'] == 5)]
    for threshold in [5, 10, 15, 20]:
        subset = long_df[long_df['surprise_pct'] >= threshold]
        if len(subset) < 10:
            continue
        t_stat, p_val = stats.ttest_1samp(subset['abnormal_return'], 0)
        print(f"  Surprise >= {threshold}%: n={len(subset)} | mean={subset['abnormal_return'].mean():+.2f}% | dir={subset['direction_correct'].mean()*100:.0f}% | p={p_val:.3f}")


if __name__ == '__main__':
    print("=== PEAD (Post-Earnings Announcement Drift) Backtest ===")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print()
    
    # Load large-cap S&P 500 tickers
    try:
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        tickers = sp500['Symbol'].tolist()
        print(f"Loaded {len(tickers)} S&P 500 tickers from Wikipedia")
    except Exception as e:
        print(f"Wikipedia failed ({e}), using local file...")
        try:
            with open('tools/sp500_tickers.txt', 'r') as f:
                tickers = [line.strip() for line in f if line.strip()]
            print(f"Loaded {len(tickers)} tickers from local file")
        except:
            # Hardcode a broad set
            tickers = ['AAPL','MSFT','AMZN','GOOGL','META','NVDA','BRK.B','JPM','V','UNH',
                      'HD','PG','MA','AVGO','CVX','LLY','PFE','ABBV','KO','MRK','PEP','TMO',
                      'COST','BAC','MCD','ACN','CSCO','NKE','WMT','IBM','XOM','VZ','T','GS',
                      'MS','C','WFC','USB','AXP','BLK','SPGI','CME','ICE','COF','TRV','MMC',
                      'AON','MET','PRU','AFL','ALL','CB','HIG','LNC','UNM','SYF','DFS','SYK',
                      'ABT','DHR','MDT','BSX','EW','ISRG','BDX','ZBH','HOLX','BAX','RMD','IQV']
            print(f"Using hardcoded {len(tickers)} tickers")
    
    # Use tickers directly - all hardcoded ones are large-cap
    large_cap = tickers[:80]
    print(f"Tickers to process: {len(large_cap)}")
    
    # Collect earnings events
    print("\nCollecting earnings surprises (>=5% EPS surprise)...")
    events = collect_earnings_surprises(large_cap, lookback_years=3, min_surprise_pct=5.0)
    
    if not events:
        print("No events found!")
        sys.exit(1)
    
    print(f"\nFound {len(events)} earnings events")
    beats = sum(1 for e in events if e['direction'] == 'long')
    misses = sum(1 for e in events if e['direction'] == 'short')
    print(f"Beats (long): {beats}, Misses (short): {misses}")
    
    # Measure returns
    print("\nMeasuring abnormal returns...")
    results = measure_pead_returns(events, horizons=[1, 3, 5, 10])
    
    # Analyze
    analyze_results(results, min_surprise=5.0)
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv('tools/pead_results.csv', index=False)
    print(f"\nResults saved to tools/pead_results.csv")
