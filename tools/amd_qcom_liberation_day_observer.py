"""
AMD/QCOM Semiconductor Basket — Liberation Day 2026 OOS Observer
================================================================
Purpose: Capture the 3rd out-of-sample validation instance for the
         tariff_amd_qcom_semiconductor_basket_short signal.

Background:
  - Training: 6 events (2018-2019), 10d avg=-6.16%, 100% direction, p=0.0008
  - OOS1 (2025-03-26): basket=-5.9% at 10d ✓
  - OOS2 (2025-04-02): basket=-2.2% at 10d ✓
  - OOS3: Liberation Day 2026-04-02 (THIS observation)

  NOTE: This signal CANNOT be formally pre-registered for Liberation Day 2026
  because the signal type cap (max 3) is already exceeded with 13 active types.
  This tool records the result as informal OOS evidence in the knowledge base.
  After 3rd OOS confirms, signal can be formally pre-registered for next tariff event
  when signal type slots free up (several tariff hypotheses complete post-April 2).

Usage:
  python3 tools/amd_qcom_liberation_day_observer.py                    # check current state
  python3 tools/amd_qcom_liberation_day_observer.py --event 2026-04-02 # specify event date
  python3 tools/amd_qcom_liberation_day_observer.py --record            # record results in KB

Signal definition:
  - Basket: 50% AMD + 50% QCOM (equal weight)
  - Entry: close of Liberation Day (2026-04-02) — use close price as reference
  - Measurement: 5d and 10d abnormal return vs SPY
  - Expected: basket > -2% abnormal return at 10d (baseline -6.16% avg)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import db
from tools.yfinance_utils import safe_download
import pandas as pd
import numpy as np


KNOWLEDGE_KEY = 'tariff_amd_qcom_semiconductor_basket_short'
LIBERATION_DATE = '2026-04-02'
TICKERS = ['AMD', 'QCOM', 'SPY']


def get_prices_from(event_date_str: str, lookback_days: int = 5, lookahead_days: int = 25):
    """Download prices around the event date."""
    event_date = pd.Timestamp(event_date_str)
    start = (event_date - timedelta(days=lookback_days * 2)).strftime('%Y-%m-%d')
    end = (event_date + timedelta(days=lookahead_days * 2)).strftime('%Y-%m-%d')

    prices = {}
    for ticker in TICKERS:
        df = safe_download(ticker, start=start, end=end)
        if df is not None and not df.empty:
            prices[ticker] = df['Close'].dropna()
        else:
            print(f"WARNING: Could not download {ticker}")
            prices[ticker] = pd.Series(dtype=float)
    return prices


def calc_basket_return(prices: dict, event_date_str: str, horizon_days: int):
    """Calculate basket abnormal return vs SPY at given horizon."""
    event_date = pd.Timestamp(event_date_str)

    for ticker in TICKERS:
        if prices[ticker].empty:
            return None, None, None

    # Find the trading day AT or AFTER event date (entry = close of that day)
    spy_dates = prices['SPY'].index
    entry_dates = spy_dates[spy_dates >= event_date]
    if len(entry_dates) == 0:
        print(f"No trading days on/after {event_date_str}")
        return None, None, None
    entry_date = entry_dates[0]

    # Find exit date at horizon_days later
    entry_idx = list(spy_dates).index(entry_date)
    if entry_idx + horizon_days >= len(spy_dates):
        print(f"Not enough data for {horizon_days}d horizon (need {horizon_days} more days after {entry_date.date()})")
        return None, None, None
    exit_date = spy_dates[entry_idx + horizon_days]

    results = {}
    for ticker in ['AMD', 'QCOM', 'SPY']:
        s = prices[ticker]
        if entry_date not in s.index or exit_date not in s.index:
            print(f"Missing {ticker} data at {entry_date.date()} or {exit_date.date()}")
            return None, None, None
        entry_px = s.loc[entry_date]
        exit_px = s.loc[exit_date]
        results[ticker] = (exit_px / entry_px - 1) * 100

    # Equal-weight basket return
    basket_raw = (results['AMD'] + results['QCOM']) / 2
    spy_raw = results['SPY']
    abnormal = basket_raw - spy_raw

    return abnormal, basket_raw, spy_raw


def check_status():
    """Check current observation status."""
    db.init_db()
    k = db.load_knowledge()
    known = k.get('known_effects', {})
    item = known.get(KNOWLEDGE_KEY, {})

    print("=" * 65)
    print("AMD/QCOM SEMICONDUCTOR BASKET — OOS OBSERVER")
    print("=" * 65)
    print(f"Signal: {item.get('description', 'N/A')}")
    print(f"Status: {item.get('status', 'N/A')}")
    print(f"OOS to date: {item.get('oos_validation', 'none')}")
    print(f"Training: {item.get('n_events', 0)} events, "
          f"10d avg={item.get('10d', {}).get('avg', 'N/A')}%")
    print()

    event_date = LIBERATION_DATE
    print(f"Observing Liberation Day: {event_date}")

    # Try to get current data
    prices = get_prices_from(event_date, lookback_days=3, lookahead_days=15)

    today = pd.Timestamp.today()
    event_ts = pd.Timestamp(event_date)

    if today < event_ts:
        print(f"Event date {event_date} has not yet occurred (today: {today.date()})")
        print("No observation available yet.")
        return

    # Calculate available horizons
    available_horizons = []
    for h in [5, 10]:
        abnormal, basket, spy = calc_basket_return(prices, event_date, h)
        if abnormal is not None:
            available_horizons.append((h, abnormal, basket, spy))
            direction = "✓ SHORT" if abnormal < 0 else "✗ WRONG"
            print(f"  {h}d: abnormal={abnormal:+.2f}% (basket={basket:+.2f}%, SPY={spy:+.2f}%) [{direction}]")

    if not available_horizons:
        days_elapsed = (today - event_ts).days
        print(f"  {days_elapsed} calendar days since event. Check back in {max(0, 14-days_elapsed)} days for 10d result.")
        return

    # Print summary
    print()
    prev_oos = "OOS1 (2025-03-26): -5.9% ✓, OOS2 (2025-04-02): -2.2% ✓"
    print(f"Prior OOS: {prev_oos}")
    print(f"This OOS (2026-04-02): {'Complete' if len(available_horizons) == 2 else 'Partial'}")

    return available_horizons


def record_results(event_date: str = LIBERATION_DATE):
    """Record Liberation Day observation in knowledge base."""
    db.init_db()
    prices = get_prices_from(event_date, lookback_days=3, lookahead_days=15)

    results = {}
    for h in [5, 10]:
        abnormal, basket, spy = calc_basket_return(prices, event_date, h)
        if abnormal is not None:
            results[h] = {'abnormal': abnormal, 'basket': basket, 'spy': spy}

    if not results:
        print("Cannot record — insufficient data (event hasn't happened or too recent)")
        return

    k = db.load_knowledge()
    known = k.get('known_effects', {})
    item = known.get(KNOWLEDGE_KEY, {})

    # Build OOS3 note
    oos3_parts = []
    for h, r in sorted(results.items()):
        direction = "✓" if r['abnormal'] < 0 else "✗"
        oos3_parts.append(f"{h}d={r['abnormal']:+.1f}% {direction}")
    oos3_note = f"2026-04-02 (Liberation Day): {', '.join(oos3_parts)}"

    # Update OOS validation field
    prev_oos = "2025-03-26: basket=-5.9% correct; 2025-04-02: basket=-2.2% correct (2/2 OOS)"
    new_oos = prev_oos + f"; {oos3_note}"

    # Determine if 3-OOS threshold met
    if 10 in results and results[10]['abnormal'] < 0:
        new_status = "OOS3_CONFIRMED — 3/3 OOS positive. READY_TO_PREREGISTER at next tariff event when signal slot opens."
        preregister_note = "3 OOS instances confirmed. Can formally pre-register at next tariff escalation event."
    elif 10 in results:
        new_status = f"OOS3_FAILED — 3rd OOS did NOT confirm (10d={results.get(10,{}).get('abnormal','N/A'):+.1f}%). Review if signal is degrading."
        preregister_note = "3rd OOS failed. Do NOT pre-register. Investigate if regime change or if 2025-2026 is different."
    else:
        new_status = "OOS3_PARTIAL — 10d data not yet available. Check back."
        preregister_note = "Partial result. Wait for 10d before concluding."

    item['oos_validation'] = new_oos
    item['status'] = new_status
    item['liberation_day_2026'] = results
    item['preregister_note'] = preregister_note
    item['last_updated'] = datetime.now().isoformat()

    known[KNOWLEDGE_KEY] = item
    k['known_effects'] = known

    try:
        db.save_knowledge(k)
        print(f"Recorded OOS3 in knowledge base:")
        print(f"  {oos3_note}")
        print(f"  Status: {new_status}")
    except Exception as e:
        print(f"ERROR saving: {e}")
        # Try using update_known_effect if available
        try:
            db.update_known_effect(KNOWLEDGE_KEY, {
                'oos_validation': new_oos,
                'status': new_status,
                'liberation_day_2026': str(results),
                'preregister_note': preregister_note
            })
            print("Saved via update_known_effect")
        except Exception as e2:
            print(f"Also failed update_known_effect: {e2}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='AMD/QCOM Liberation Day OOS Observer')
    parser.add_argument('--event', default=LIBERATION_DATE, help='Event date (default: 2026-04-02)')
    parser.add_argument('--record', action='store_true', help='Record results in knowledge base')
    args = parser.parse_args()

    if args.record:
        record_results(args.event)
    else:
        check_status()

    print()
    print("=" * 65)
    print("NEXT ACTIONS:")
    print("  April 8-9 (5d): python3 tools/amd_qcom_liberation_day_observer.py")
    print("  April 14-15 (10d): python3 tools/amd_qcom_liberation_day_observer.py --record")
    print("  If 3 OOS confirmed: open slot by completing tariff hypotheses, then pre-register")
    print("=" * 65)


if __name__ == '__main__':
    main()
