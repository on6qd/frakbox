"""
Record proper post-mortem for auto-closed hypotheses.

After trade_loop deadline-closes a hypothesis, it stores only raw_return_pct.
This tool retrieves the actual SPY-adjusted abnormal return and calls
research.complete_hypothesis() with a proper post-mortem.

Usage:
    python tools/record_hypothesis_close.py --hypothesis-id <id>
    python tools/record_hypothesis_close.py --scan-recent   # scan all recently auto-closed
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import db
import trader
import market_data
import research


def calculate_abnormal_return(symbol, entry_price, entry_date, exit_price, exit_date):
    """Calculate SPY-adjusted abnormal return."""
    try:
        import yfinance as yf
        import pandas as pd

        # Get SPY prices for the same period
        start = (datetime.strptime(entry_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
        end = (datetime.strptime(exit_date, '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d')
        
        spy = yf.download('SPY', start=start, end=end, auto_adjust=True, progress=False)
        if spy.empty:
            return None, None

        closes = spy['Close'] if not isinstance(spy.columns, pd.MultiIndex) else spy['Close']['SPY']
        closes = closes.dropna()
        
        # Find SPY price at entry and exit
        entry_dt = pd.Timestamp(entry_date)
        exit_dt = pd.Timestamp(exit_date)
        
        # Get nearest close to entry and exit
        spy_at_entry = None
        spy_at_exit = None
        
        for i, (idx, val) in enumerate(closes.items()):
            if idx >= entry_dt and spy_at_entry is None:
                spy_at_entry = val
            if idx >= exit_dt and spy_at_exit is None:
                spy_at_exit = val

        if spy_at_entry is None or spy_at_exit is None:
            return None, None

        stock_return = (exit_price - entry_price) / entry_price * 100
        spy_return = (spy_at_exit - spy_at_entry) / spy_at_entry * 100
        abnormal = stock_return - spy_return
        
        return stock_return, abnormal, spy_return

    except Exception as e:
        print(f"Error calculating abnormal return: {e}")
        return None, None, None


def process_hypothesis(h):
    """Process a single auto-closed hypothesis."""
    hyp_id = h['id']
    symbol = h.get('target_symbol') or h.get('expected_symbol')
    
    if not symbol:
        print(f"  {hyp_id[:8]}: No symbol, skipping")
        return
    
    # Get trade data
    trade = h.get('trade', {}) or {}
    result = h.get('result', {}) or {}
    
    entry_price = trade.get('entry_price')
    entry_time = trade.get('entry_time', '')
    entry_date = entry_time[:10] if entry_time else None
    
    exit_price = result.get('exit_price')
    exit_time = result.get('exit_time', '')
    exit_date = exit_time[:10] if exit_time else None
    raw_return = result.get('raw_return_pct')
    
    if not all([entry_price, exit_price, entry_date, exit_date]):
        print(f"  {hyp_id[:8]} {symbol}: Missing trade data (entry={entry_price}, exit={exit_price})")
        return
    
    print(f"  {hyp_id[:8]} {symbol}: entry={entry_date} ${entry_price:.2f} → exit={exit_date} ${exit_price:.2f} | raw={raw_return:+.1f}%")
    
    # Calculate abnormal return
    returns = calculate_abnormal_return(symbol, entry_price, entry_date, exit_price, exit_date)
    if len(returns) == 3:
        stock_ret, abnormal, spy_ret = returns
    else:
        print(f"    Could not calculate abnormal return")
        return
    
    if stock_ret is None:
        print(f"    Could not calculate abnormal return")
        return
    
    print(f"    Stock: {stock_ret:+.2f}% | SPY: {spy_ret:+.2f}% | Abnormal: {abnormal:+.2f}%")
    
    direction = h.get('expected_direction', 'short')
    expected_mag = h.get('expected_magnitude_pct', 1.5)
    
    # Determine hit/miss for short position
    if direction == 'short':
        # Short: we profit when price falls, so abnormal should be negative (stock underperforms)
        is_correct_direction = abnormal < -0.5  # Stock underperformed SPY by >0.5%
        is_magnitude_hit = abs(abnormal) >= expected_mag * 0.5  # At least half expected mag
    else:  # long
        is_correct_direction = abnormal > 0.5
        is_magnitude_hit = abnormal >= expected_mag * 0.5
    
    outcome = 'correct' if is_correct_direction else 'incorrect'
    
    print(f"    Direction: {'HIT' if is_correct_direction else 'MISS'} | Magnitude: {'HIT' if is_magnitude_hit else 'MISS'}")
    print(f"    Outcome: {outcome}")
    
    return {
        'abnormal': abnormal,
        'stock_return': stock_ret,
        'spy_return': spy_ret,
        'outcome': outcome,
        'is_correct': is_correct_direction
    }


def scan_recent():
    """Scan for recently auto-closed hypotheses that need post-mortem."""
    db.init_db()
    hyps = db.load_hypotheses()
    
    # Find completed hypotheses without proper post-mortem
    candidates = []
    for h in hyps:
        if h.get('status') != 'completed':
            continue
        result = h.get('result', {}) or {}
        # Auto-closed = has 'auto_closed': True in result
        if not result.get('auto_closed'):
            continue
        # Check if post-mortem is set
        if h.get('post_mortem') and len(str(h.get('post_mortem', ''))) > 50:
            continue  # Already has post-mortem
        candidates.append(h)
    
    print(f"Found {len(candidates)} auto-closed hypotheses needing post-mortem")
    for h in candidates:
        symbol = h.get('target_symbol') or h.get('expected_symbol')
        result = h.get('result', {}) or {}
        print(f"  {h['id'][:8]} {symbol}: {result.get('exit_reason','?')[:80]}")
    
    return candidates


def main():
    parser = argparse.ArgumentParser(description='Record proper post-mortem for auto-closed hypotheses')
    parser.add_argument('--hypothesis-id', help='Specific hypothesis ID to process')
    parser.add_argument('--scan-recent', action='store_true', help='Scan and report recently auto-closed')
    parser.add_argument('--complete', action='store_true', help='Actually call complete_hypothesis (requires --hypothesis-id)')
    args = parser.parse_args()
    
    db.init_db()
    
    if args.scan_recent:
        candidates = scan_recent()
        return
    
    if args.hypothesis_id:
        h = db.get_hypothesis_by_id(args.hypothesis_id)
        if not h:
            print(f"Hypothesis {args.hypothesis_id} not found")
            return
        result = process_hypothesis(h)
        if result and args.complete:
            print("\nWould call complete_hypothesis() with above data")
            # TODO: call research.complete_hypothesis() if confirmed
    else:
        # Default: scan
        candidates = scan_recent()
        if candidates:
            print("\nRun with --hypothesis-id <id> --complete to process one")


if __name__ == '__main__':
    main()
