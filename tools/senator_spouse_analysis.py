"""
senator_spouse_analysis.py

Downloads Senate Stock Watcher data (2012-2020) and backtests:
1. Spouse trades vs self/joint trades 
2. Historical signal (pre-2023) - do senators have predictive signal in earlier regime?

Data source: github.com/timothycarambat/senate-stock-watcher-data
"""

import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from tools.yfinance_utils import get_close_prices

GITHUB_URL = "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json"
CACHE_PATH = os.path.join(os.path.dirname(__file__), "senate_stock_watcher_cache.json")

def download_data():
    """Download or load cached Senate Stock Watcher data."""
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH) as f:
            data = json.load(f)
        print(f"Loaded {len(data)} records from cache")
        return data
    
    print(f"Downloading from GitHub...")
    resp = requests.get(GITHUB_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    
    with open(CACHE_PATH, 'w') as f:
        json.dump(data, f)
    print(f"Downloaded and cached {len(data)} records")
    return data

def parse_amount(amount_str):
    """Parse amount range string to midpoint."""
    if not amount_str or amount_str == '--':
        return None
    # "$1,001 - $15,000" -> midpoint
    import re
    nums = re.findall(r'[\d,]+', amount_str.replace('$', ''))
    nums = [int(n.replace(',', '')) for n in nums]
    if len(nums) == 2:
        return (nums[0] + nums[1]) / 2
    elif len(nums) == 1:
        return nums[0]
    return None

def prepare_trades(data):
    """Parse raw records into clean DataFrame."""
    records = []
    for item in data:
        ticker = item.get('ticker', '').strip()
        if not ticker or ticker == '--' or len(ticker) > 5:
            continue
        if not ticker.isalpha():
            continue
        
        ttype = item.get('type', '').lower()
        if 'purchase' in ttype:
            direction = 'buy'
        elif 'sale' in ttype:
            direction = 'sell'
        else:
            continue
        
        # Parse date: "11/10/2020" format
        tx_date_str = item.get('transaction_date', '')
        try:
            tx_date = datetime.strptime(tx_date_str, '%m/%d/%Y').date()
        except:
            continue
        
        owner = item.get('owner', 'N/A').strip()
        senator = item.get('senator', '').strip()
        amount = parse_amount(item.get('amount'))
        
        records.append({
            'senator': senator,
            'ticker': ticker,
            'direction': direction,
            'transaction_date': tx_date,
            'owner': owner,
            'amount': amount,
        })
    
    df = pd.DataFrame(records)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    return df

def measure_returns(df, horizons=[3, 5, 10, 20], entry='close_same_day'):
    """Measure forward returns for a set of trades."""
    results = []
    errors = 0
    
    # Get unique tickers
    tickers = df['ticker'].unique().tolist()
    
    # Download price data for all tickers
    min_date = df['transaction_date'].min() - timedelta(days=5)
    max_date = df['transaction_date'].max() + timedelta(days=30)
    
    print(f"  Fetching prices for {len(tickers)} tickers ({min_date.date()} to {max_date.date()})...")
    price_data = {}
    
    # Process in chunks
    for i, ticker in enumerate(tickers):
        if i % 50 == 0:
            print(f"    {i}/{len(tickers)} tickers...")
        try:
            prices = get_close_prices([ticker], str(min_date.date()), str(max_date.date()))
            if prices is not None and len(prices) > 0:
                price_data[ticker] = prices
        except:
            pass
    
    # Also get SPY for benchmark
    spy_prices = get_close_prices(['SPY'], str(min_date.date()), str(max_date.date()))
    
    print(f"  Got prices for {len(price_data)}/{len(tickers)} tickers")
    print(f"  Measuring returns for {len(df)} trades...")
    
    for _, row in df.iterrows():
        ticker = row['ticker']
        trade_date = row['transaction_date']
        direction = row['direction']
        
        if ticker not in price_data:
            errors += 1
            continue
        
        prices = price_data[ticker]
        spy = spy_prices
        
        # Find entry price (next trading day close)
        try:
            future_prices = prices[prices.index >= trade_date]
            future_spy = spy[spy.index >= trade_date] if spy is not None else None
            
            if len(future_prices) < 2:
                errors += 1
                continue
            
            entry_price = future_prices.iloc[0]
            entry_spy = future_spy.iloc[0] if future_spy is not None and len(future_spy) > 0 else None
            
            row_result = {
                'senator': row['senator'],
                'ticker': ticker,
                'direction': direction,
                'owner': row['owner'],
                'trade_date': trade_date,
                'amount': row['amount'],
            }
            
            for h in horizons:
                if len(future_prices) > h:
                    exit_price = future_prices.iloc[h]
                    raw_return = (exit_price / entry_price - 1) * 100
                    
                    # Abnormal return
                    if entry_spy is not None and future_spy is not None and len(future_spy) > h:
                        exit_spy = future_spy.iloc[h]
                        spy_return = (exit_spy / entry_spy - 1) * 100
                        abnormal = raw_return - spy_return
                    else:
                        abnormal = raw_return
                    
                    # For sells, flip sign (we'd be shorting)
                    if direction == 'sell':
                        abnormal = -abnormal
                    
                    row_result[f'ret_{h}d'] = abnormal
            
            results.append(row_result)
        except Exception as e:
            errors += 1
    
    print(f"  Measured {len(results)} trades ({errors} errors)")
    return pd.DataFrame(results)


def run_backtest():
    """Main analysis."""
    print("=== SENATOR SPOUSE TRADING BACKTEST ===\n")
    
    # Download data
    data = download_data()
    df = prepare_trades(data)
    
    print(f"\nTrade records parsed: {len(df)}")
    print(f"Date range: {df['transaction_date'].min().date()} to {df['transaction_date'].max().date()}")
    print(f"\nOwner distribution:")
    print(df['owner'].value_counts().to_string())
    print(f"\nDirection distribution:")
    print(df['direction'].value_counts().to_string())
    
    # Filter to purchases only for cleaner signal
    purchases = df[df['direction'] == 'buy'].copy()
    print(f"\nPurchases: {len(purchases)}")
    
    # Measure returns
    results = measure_returns(purchases, horizons=[3, 5, 10, 20])
    
    if len(results) == 0:
        print("No results!")
        return
    
    print("\n=== RESULTS BY OWNER TYPE ===")
    from scipy import stats
    
    for horizon in ['ret_3d', 'ret_5d', 'ret_10d', 'ret_20d']:
        if horizon not in results.columns:
            continue
        h = int(horizon.split('_')[1].replace('d', ''))
        print(f"\n--- {h}-day Abnormal Returns ---")
        
        for owner_group, label in [
            (['Spouse'], 'SPOUSE'),
            (['Self'], 'SELF'),
            (['Joint'], 'JOINT'),
            (['Spouse', 'Joint'], 'SPOUSE+JOINT'),
        ]:
            subset = results[results['owner'].isin(owner_group)][horizon].dropna().astype(float)
            if len(subset) < 10:
                continue
            
            t_stat, p_val = stats.ttest_1samp(subset, 0)
            direction_pct = (subset > 0.5).mean() * 100
            
            print(f"  {label} (n={len(subset)}): mean={subset.mean():.2f}% p={p_val:.4f} dir>{0.5}%: {direction_pct:.0f}%")
    
    # Save detailed results
    results.to_csv(os.path.join(os.path.dirname(__file__), 'senate_spouse_results.csv'), index=False)
    print("\nDetailed results saved to tools/senate_spouse_results.csv")
    
    return results


if __name__ == '__main__':
    results = run_backtest()
