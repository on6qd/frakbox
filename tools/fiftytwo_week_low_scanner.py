"""
Daily Scanner: 52-Week Low First-Touch SHORT Signal

Runs against the S&P 500 large-cap universe to detect when any stock
first breaks below its 52-week low (after 30+ days above that level).

Hypothesis: sp500_52w_low_momentum_short (ID: 86d28864)
Expected: SHORT at next-day open, hold 5 trading days, expected -1.68% abnormal

Usage:
    python tools/fiftytwo_week_low_scanner.py
    python tools/fiftytwo_week_low_scanner.py --dry-run    # show detections only
"""

import sys
import os
import argparse
import json
from datetime import datetime, timedelta
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd

# Universe of S&P 500 large-cap stocks (>$500M market cap)
UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO',
    'IBM', 'CSCO', 'ORCL', 'CRM', 'NOW', 'ADBE',
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'USB', 'PNC', 'TFC', 'AXP',
    'JNJ', 'PFE', 'MRK', 'ABT', 'BMY', 'ABBV', 'LLY', 'MDT', 'UNH',
    'AMZN', 'WMT', 'TGT', 'HD', 'LOW', 'NKE', 'MCD', 'SBUX', 'KO', 'PEP',
    'XOM', 'CVX', 'COP', 'SLB', 'EOG',
    'BA', 'GE', 'MMM', 'CAT', 'HON', 'LMT', 'RTX', 'UPS', 'FDX',
    'VZ', 'T', 'CMCSA', 'DIS',
    'FCX', 'NUE', 'DD',
]

HYPOTHESIS_ID = '86d28864'
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs', '52w_low_scanner_state.json')


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {'last_triggered': {}}  # ticker -> last trigger date


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def check_stock(ticker: str, lookback_days: int = 400, debounce_days: int = 30) -> dict | None:
    """
    Check if ticker is at a new 52-week low (first touch after 30+ day gap).
    
    Returns detection dict or None if no signal.
    """
    try:
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        df = yf.download(ticker, start=start, end=end, progress=False)
        
        if df.empty or len(df) < 100:
            return None
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # Rolling 252-day low (exclude today)
        df['52w_low'] = df['Close'].shift(1).rolling(252, min_periods=100).min()
        df['at_52w_low'] = df['Close'] <= df['52w_low']
        
        # Days since last 52-week low in prior debounce_days
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
        
        # Check the most recent trading day
        if len(df) == 0:
            return None
        
        last_row = df.iloc[-1]
        last_date = df.index[-1].strftime('%Y-%m-%d')
        
        if not last_row['first_touch']:
            return None
        
        return {
            'ticker': ticker,
            'date': last_date,
            'close': float(last_row['Close']),
            '52w_low': float(last_row['52w_low']),
            'pct_below': float((last_row['Close'] / last_row['52w_low'] - 1) * 100),
            'hypothesis_id': HYPOTHESIS_ID,
            'action': 'SHORT at next market open',
            'hold_days': 5,
        }
    except Exception as e:
        print(f"  ERROR {ticker}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='52-Week Low First-Touch SHORT Scanner')
    parser.add_argument('--dry-run', action='store_true', help='Show detections without triggering trades')
    args = parser.parse_args()
    
    print(f"=== 52-Week Low Scanner ({datetime.now().strftime('%Y-%m-%d %H:%M')}) ===")
    print(f"Universe: {len(UNIVERSE)} stocks")
    print(f"Hypothesis: {HYPOTHESIS_ID} (sp500_52w_low_momentum_short)")
    print()
    
    state = load_state()
    detections = []
    
    for ticker in UNIVERSE:
        result = check_stock(ticker)
        if result:
            # Check debounce: don't re-trigger same stock within 30 days
            last_trig = state['last_triggered'].get(ticker)
            if last_trig:
                days_since = (datetime.now() - datetime.strptime(last_trig, '%Y-%m-%d')).days
                if days_since < 30:
                    print(f"  {ticker}: SKIP (already triggered {days_since}d ago)")
                    continue
            
            detections.append(result)
            print(f"  SIGNAL: {ticker} on {result['date']}")
            print(f"    Close: {result['close']:.2f}, 52W Low: {result['52w_low']:.2f}")
            print(f"    Breach: {result['pct_below']:.1f}%")
            print(f"    Action: {result['action']}")
        time.sleep(0.05)
    
    if not detections:
        print("No signals detected.")
        save_state(state)
        return
    
    print(f"\n{len(detections)} signal(s) detected.")
    
    if args.dry_run:
        print("DRY RUN: No triggers set.")
        return
    
    # Set triggers for each detection
    import research
    hypotheses = research.load_hypotheses()
    
    for det in detections:
        print(f"\nSetting trigger for {det['ticker']}...")
        
        # Find the hypothesis and set trigger
        for h in hypotheses:
            if h['id'] == HYPOTHESIS_ID and h['status'] == 'pending':
                h['expected_symbol'] = det['ticker']
                h['trigger'] = 'next_market_open'
                h['trigger_position_size'] = 5000
                h['trigger_stop_loss_pct'] = 8  # Stop loss at 8% (smaller for short)
                h['trigger_take_profit_pct'] = 10
                h['trigger_notes'] = f"52-week low first touch on {det['date']}, breach={det['pct_below']:.1f}%"
                print(f"  Trigger set: {det['ticker']} SHORT at next open")
                state['last_triggered'][det['ticker']] = det['date']
                break
        else:
            print(f"  WARNING: Could not find pending hypothesis {HYPOTHESIS_ID}")
    
    research.save_hypotheses(hypotheses)
    save_state(state)
    print("\nDone. trade_loop.py will execute at next market open.")


if __name__ == '__main__':
    main()
