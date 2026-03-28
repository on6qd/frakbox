"""
Systemic 52W Low Short — OOS Tracker for March 27, 2026 Event
=============================================================
March 27, 2026: SPY -1.71%, 28 stocks at first-ever 52w lows
Signal fired but NOT activated (capacity conflict + Liberation Day correlation)
This tool tracks the informal OOS result for future hypothesis validation.

Signal: sp500_52w_low_systemic_short (sp500_52w_low_momentum_short family)
Expected: -1.88% abnormal return over 5 trading days per stock

Usage:
  python3 tools/systemic_short_oos_tracker.py            # check current results
  python3 tools/systemic_short_oos_tracker.py --record   # save to knowledge base

Run after April 9, 2026 (5 trading days after March 31 entry date)
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import db
from tools.yfinance_utils import safe_download
import pandas as pd
import numpy as np

EVENT_DATE = '2026-03-27'
ENTRY_DATE = '2026-03-31'  # next trading day after signal fired
HOLD_DAYS = 5

# 28 stocks at first-ever 52w lows on March 27, 2026
CANDIDATE_STOCKS = [
    'ADBE', 'QCOM', 'SNPS', 'NOW', 'PTC', 'ZBRA', 'TTWO', 'UBER', 'CPRT', 'WHR',
    'AXP', 'COF', 'V', 'MA', 'GPN', 'MET', 'EQH', 'RJF', 'BAX', 'A', 'ADP',
    'J', 'AVB', 'ESS', 'BXP', 'VNO', 'HIW'
    # SYK excluded — already had active short
]


def calc_5d_return(ticker: str, entry_date_str: str) -> tuple:
    """Calculate 5-day abnormal return for a ticker from entry date."""
    entry_date = pd.Timestamp(entry_date_str)
    from datetime import timedelta
    start = (entry_date - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    end = (entry_date + pd.Timedelta(days=20)).strftime('%Y-%m-%d')

    try:
        df_stock = safe_download(ticker, start=start, end=end)
        df_spy = safe_download('SPY', start=start, end=end)
        if df_stock is None or df_spy is None or df_stock.empty or df_spy.empty:
            return None, None, None

        stock_close = df_stock['Close'].dropna()
        spy_close = df_spy['Close'].dropna()

        # Find entry date
        entry_dates = stock_close.index[stock_close.index >= entry_date]
        spy_entry_dates = spy_close.index[spy_close.index >= entry_date]
        if len(entry_dates) == 0 or len(spy_entry_dates) == 0:
            return None, None, None

        stock_entry = entry_dates[0]
        spy_entry = spy_entry_dates[0]

        # Find exit date at 5 trading days later
        stock_idx = list(stock_close.index).index(stock_entry)
        spy_idx = list(spy_close.index).index(spy_entry)

        if stock_idx + HOLD_DAYS >= len(stock_close) or spy_idx + HOLD_DAYS >= len(spy_close):
            return None, None, None

        stock_exit = stock_close.index[stock_idx + HOLD_DAYS]
        spy_exit = spy_close.index[spy_idx + HOLD_DAYS]

        stock_ret = (stock_close.loc[stock_exit] / stock_close.loc[stock_entry] - 1) * 100
        spy_ret = (spy_close.loc[spy_exit] / spy_close.loc[spy_entry] - 1) * 100
        abnormal = stock_ret - spy_ret

        return float(abnormal), float(stock_ret), float(spy_ret)
    except Exception as e:
        print(f"  Error for {ticker}: {e}")
        return None, None, None


def check_status():
    """Check current OOS results for March 27 systemic event."""
    print("=" * 65)
    print("SYSTEMIC SHORT OOS TRACKER — March 27, 2026 Event")
    print("=" * 65)
    print(f"Event date: {EVENT_DATE} (SPY -1.71%, 28 first-touch 52w lows)")
    print(f"Entry date: {ENTRY_DATE} (next trading day)")
    print(f"Hold: {HOLD_DAYS} trading days → exit ~April 9, 2026")
    print(f"Expected: -1.88% abnormal per stock (in-sample), NOT OOS-validated yet")
    print()

    results = []
    correct_direction = 0
    available = 0

    for ticker in CANDIDATE_STOCKS:
        abnormal, raw, spy = calc_5d_return(ticker, ENTRY_DATE)
        if abnormal is not None:
            available += 1
            is_correct = abnormal < 0
            if is_correct:
                correct_direction += 1
            results.append((ticker, abnormal, raw, spy, is_correct))
        else:
            results.append((ticker, None, None, None, None))

    if not results or available == 0:
        print("No data available yet. Check after April 9, 2026.")
        return

    # Sort by abnormal return
    with_data = [(t, a, r, s, c) for t, a, r, s, c in results if a is not None]
    with_data.sort(key=lambda x: x[1])

    print(f"Results ({available} stocks with data):")
    for ticker, abnormal, raw, spy, correct in with_data[:15]:
        direction = "✓" if correct else "✗"
        print(f"  {ticker:6}: {abnormal:+.1f}% abnormal ({raw:+.1f}% raw, SPY {spy:+.1f}%) {direction}")

    if available > 15:
        print(f"  ... and {available - 15} more")

    print()
    print(f"Summary: {correct_direction}/{available} correct direction ({correct_direction/available*100:.0f}%)")
    avgs = [a for _, a, _, _, _ in with_data if a is not None]
    if avgs:
        avg_abnormal = np.mean(avgs)
        print(f"  Average abnormal return: {avg_abnormal:+.2f}%")
        print(f"  Expected: -1.88% (in-sample)")
        print(f"  OOS confirmed: {'YES' if avg_abnormal < -1.0 and correct_direction/available >= 0.6 else 'PARTIAL/NO'}")

    return with_data


def record_results():
    """Record informal OOS results in knowledge base."""
    with_data = check_status()
    if not with_data:
        print("Cannot record — insufficient data")
        return

    db.init_db()
    k = db.load_knowledge()
    known = k.get('known_effects', {})
    item = known.get('sp500_52w_low_systemic_short', {})

    avgs = [a for _, a, _, _, _ in with_data if a is not None]
    correct = sum(1 for _, a, _, _, c in with_data if c)
    available = len([a for _, a, _, _, _ in with_data if a is not None])

    avg_abnormal = float(np.mean(avgs)) if avgs else None
    direction_pct = correct / available if available > 0 else None

    item['march_27_2026_oos_result'] = {
        'n_stocks': available,
        'avg_abnormal': avg_abnormal,
        'direction_pct': direction_pct,
        'stocks': {t: {'abnormal': a} for t, a, _, _, _ in with_data if a is not None},
        'note': 'INFORMAL OOS — signal fired but not activated due to capacity conflict with Liberation Day'
    }
    item['last_updated'] = datetime.now().isoformat()
    known['sp500_52w_low_systemic_short'] = item
    k['known_effects'] = known
    db.save_knowledge(k)
    print(f"\nRecorded March 27 informal OOS: avg={avg_abnormal:+.2f}%, dir={direction_pct*100:.0f}%")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--record', action='store_true')
    args = parser.parse_args()

    if args.record:
        record_results()
    else:
        check_status()
    print()
    print("Run: python3 tools/systemic_short_oos_tracker.py --record (after April 9)")
