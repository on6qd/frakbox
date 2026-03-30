"""
AEP (American Electric Power) Tariff-Defensive Signal — OOS Observer
=====================================================================
Signal: AEP long after major US tariff escalation
Status: VALIDATED (passes MT correction) — READY_TO_PREREGISTER when slot opens

Key stats:
  10d: avg=+3.79%, dir=90%, p=0.035 ✓ SIGNIFICANT
  20d: avg=+4.20%, dir=80%, p=0.046 ✓ SIGNIFICANT
  Passes MT: True (2 horizons at p<0.05)
  n=10 training events (2018-2025)
  mechanism: regulated utility, no tariff exposure, safe-haven in trade war

Observing Liberation Day: April 2, 2026 (Event #11)
Entry: April 7, 2026 (Monday open after Good Friday holiday)
Exit targets: 10 trading days = April 21; 20 trading days = May 5

Usage:
  python3 tools/aep_liberation_day_observer.py           # check status
  python3 tools/aep_liberation_day_observer.py --record  # record to knowledge base
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import db
from tools.yfinance_utils import safe_download
import pandas as pd
import numpy as np
from datetime import timedelta

EVENT_DATE = '2026-04-02'
ENTRY_DATE = '2026-04-07'  # Monday open (April 3 = Good Friday, closed)
EXIT_10D = '2026-04-21'    # 10 trading days from April 7
EXIT_20D = '2026-05-05'    # ~20 trading days from April 7

SYMBOL = 'AEP'
BENCHMARK = 'SPY'
KNOWLEDGE_KEY = 'tariff_aep_utility_long'

# Training baseline
TRAINING_10D_AVG = 3.79
TRAINING_20D_AVG = 4.20
TRAINING_10D_DIR = 0.90
TRAINING_20D_DIR = 0.80


def get_prices():
    """Download prices for AEP and SPY around Liberation Day."""
    start = (pd.Timestamp(ENTRY_DATE) - timedelta(days=3)).strftime('%Y-%m-%d')
    end = (pd.Timestamp(EXIT_20D) + timedelta(days=5)).strftime('%Y-%m-%d')

    prices = {}
    for ticker in [SYMBOL, BENCHMARK]:
        try:
            df = safe_download(ticker, start=start, end=end)
            if df is not None and not df.empty:
                prices[ticker] = df['Close']
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")

    return prices


def compute_abnormal_returns(prices, entry_date, n_days):
    """Compute abnormal return vs SPY from entry date."""
    entry_ts = pd.Timestamp(entry_date)
    spy = prices.get(BENCHMARK)
    aep = prices.get(SYMBOL)

    if spy is None or aep is None:
        return None

    spy_after = spy[spy.index >= entry_ts]
    aep_after = aep[aep.index >= entry_ts]

    if len(spy_after) < n_days or len(aep_after) < n_days:
        return None

    spy_entry = spy_after.iloc[0]
    aep_entry = aep_after.iloc[0]
    spy_exit = spy_after.iloc[n_days - 1]
    aep_exit = aep_after.iloc[n_days - 1]

    raw_ret = (aep_exit / aep_entry - 1) * 100
    spy_ret = (spy_exit / spy_entry - 1) * 100
    abn_ret = raw_ret - spy_ret

    return {
        'n_days': n_days,
        'entry_date': str(spy_after.index[0].date()),
        'exit_date': str(spy_after.index[n_days - 1].date()),
        'aep_entry': aep_entry,
        'aep_exit': aep_exit,
        'raw_ret': raw_ret,
        'spy_ret': spy_ret,
        'abnormal_ret': abn_ret
    }


def main():
    today = pd.Timestamp.now().normalize()
    record_mode = '--record' in sys.argv

    print("=" * 65)
    print("AEP TARIFF-DEFENSIVE — LIBERATION DAY 2026 OBSERVER")
    print("=" * 65)
    print(f"Signal: AEP long after tariff escalation")
    print(f"Status: VALIDATED (10d p=0.035, 20d p=0.046, MT=True)")
    print(f"Training: n=10, 10d avg=+{TRAINING_10D_AVG}%, dir={TRAINING_10D_DIR:.0%}")
    print(f"Cannot formally trade: signal type cap exceeded")
    print()

    event_ts = pd.Timestamp(EVENT_DATE)
    entry_ts = pd.Timestamp(ENTRY_DATE)
    exit_10d_ts = pd.Timestamp(EXIT_10D)

    if today < event_ts:
        print(f"Event date {EVENT_DATE} has not yet occurred (today: {today.date()})")
        print("No observation available yet.")
        print()
        print("NEXT ACTIONS:")
        print(f"  April 10 (5d): python3 tools/aep_liberation_day_observer.py")
        print(f"  April 21 (10d): python3 tools/aep_liberation_day_observer.py")
        print(f"  May 5 (20d): python3 tools/aep_liberation_day_observer.py --record")
        print(f"  If OOS confirmed: pre-register when tariff trades complete (April 14-27)")
        return

    print(f"Observing Liberation Day: {EVENT_DATE}")
    print(f"Entry: {ENTRY_DATE} (next market open after Good Friday)")
    print()

    prices = get_prices()

    if SYMBOL not in prices or BENCHMARK not in prices:
        print(f"ERROR: Could not download price data for {SYMBOL} or {BENCHMARK}")
        return

    # Check entry availability
    spy = prices[BENCHMARK]
    spy_after_entry = spy[spy.index >= pd.Timestamp(ENTRY_DATE)]

    if len(spy_after_entry) == 0:
        print(f"Entry date {ENTRY_DATE} not yet reached")
        return

    entry_price_aep = prices[SYMBOL][prices[SYMBOL].index >= pd.Timestamp(ENTRY_DATE)].iloc[0]
    entry_price_spy = spy_after_entry.iloc[0]
    print(f"Entry prices: AEP={entry_price_aep:.2f}, SPY={entry_price_spy:.2f}")
    print()

    n_trading_days_since_entry = len(spy_after_entry)
    print(f"Trading days since entry: {n_trading_days_since_entry}")
    print()

    results = {}
    for n_days in [5, 10, 20]:
        r = compute_abnormal_returns(prices, ENTRY_DATE, n_days)
        if r:
            results[n_days] = r
            direction = "✓" if r['abnormal_ret'] > 0.5 else ("✗" if r['abnormal_ret'] < -0.5 else "~")
            expected = TRAINING_10D_AVG if n_days <= 10 else TRAINING_20D_AVG
            print(f"  {n_days}d: raw={r['raw_ret']:+.1f}%, spy={r['spy_ret']:+.1f}%, "
                  f"abn={r['abnormal_ret']:+.1f}% {direction} (expected ~+{expected:.1f}%)")
        else:
            print(f"  {n_days}d: NOT YET AVAILABLE")

    if results:
        print()
        correct = sum(1 for r in results.values() if r['abnormal_ret'] > 0.5)
        total = len(results)
        print(f"Direction correct: {correct}/{total} available horizons")

        if record_mode and 10 in results and 20 in results:
            print()
            print("Recording to knowledge base...")
            existing = db.get_known_effect(KNOWLEDGE_KEY)
            existing_data = existing if existing else {}
            if not isinstance(existing_data, dict):
                import json
                try:
                    existing_data = json.loads(str(existing_data))
                except:
                    existing_data = {}

            # Update with live evidence
            existing_data['liberation_day_2026_oos'] = {
                'event': EVENT_DATE,
                'entry': ENTRY_DATE,
                '10d_abn': round(results[10]['abnormal_ret'], 2) if 10 in results else 'PENDING',
                '20d_abn': round(results[20]['abnormal_ret'], 2) if 20 in results else 'PENDING',
                'direction_correct': f"{correct}/{total}",
                'note': 'Liberation Day 2026 informal OOS observation'
            }

            db.record_known_effect(KNOWLEDGE_KEY, existing_data)
            print(f"Recorded Liberation Day 2026 OOS result to {KNOWLEDGE_KEY}")
        elif record_mode:
            print("Cannot record yet — need at least 10d and 20d data")

    print()
    print("=" * 65)
    print("NEXT STEPS:")
    print(f"  10d check (April 21): python3 tools/aep_liberation_day_observer.py")
    print(f"  20d record (May 5):   python3 tools/aep_liberation_day_observer.py --record")
    print(f"  Pre-register when slot opens: tariff_utility_aep_long, 10d hold, n=10->11")


if __name__ == '__main__':
    main()
