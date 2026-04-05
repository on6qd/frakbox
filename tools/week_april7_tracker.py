#!/usr/bin/env python3
"""
Week of April 7, 2026 — Unified OOS Tracker
============================================
Runs after market close each day (April 7-10) to capture ALL active OOS observations.

Tracks:
1. STLD OOS (days 3-5, baseline March 31)
2. Q1/Q2 seasonal SPY (days 3-5, baseline April 1)
3. Auto tariff (TM, HMC) — 5d final April 9
4. Tariff signal panel (AEP, AMD, QCOM, GLD, KO, KRE, XLU, COST) — informal OOS
5. ZBIO insider cluster (if activated)
6. SPY VIX spike position

Usage:
    python3 tools/week_april7_tracker.py           # Run all trackers
    python3 tools/week_april7_tracker.py --dry-run  # Print without saving
"""
import sys, os, json, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from tools.yfinance_utils import safe_download
from datetime import datetime, timedelta
import numpy as np

db.init_db()

def get_latest_close(symbol):
    """Get most recent close price."""
    data = safe_download(symbol, start=(datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'),
                         end=(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'))
    if data is not None and not data.empty:
        return float(data['Close'].iloc[-1])
    return None

def get_close_on_date(symbol, target_date):
    """Get close price on a specific date."""
    start = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
    end = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=3)).strftime('%Y-%m-%d')
    data = safe_download(symbol, start=start, end=end)
    if data is not None and not data.empty:
        import pandas as pd
        idx = data.index[data.index <= pd.Timestamp(target_date)]
        if len(idx) > 0:
            return float(data['Close'].loc[idx[-1]])
    return None

def compute_abnormal(stock_return, spy_return):
    """Compute abnormal return (stock minus benchmark)."""
    return round(stock_return - spy_return, 2)

def track_all(dry_run=False):
    today = datetime.now().strftime('%Y-%m-%d')
    weekday = datetime.now().strftime('%A')

    print(f"=" * 70)
    print(f"WEEKLY OOS TRACKER — {today} ({weekday})")
    print(f"=" * 70)

    # Get SPY latest
    spy_close = get_latest_close('SPY')
    if spy_close is None:
        print("ERROR: Cannot get SPY price. Market may not have closed yet.")
        return

    print(f"\nSPY latest close: ${spy_close:.2f}")

    results = {}

    # === 1. STLD OOS ===
    print(f"\n--- STLD OOS (baseline March 31: $180.00, SPY $650.34) ---")
    stld_baseline = 180.00
    spy_baseline_stld = 650.34
    stld_close = get_latest_close('STLD')
    if stld_close:
        stld_ret = (stld_close / stld_baseline - 1) * 100
        spy_ret = (spy_close / spy_baseline_stld - 1) * 100
        stld_abn = compute_abnormal(stld_ret, spy_ret)
        print(f"  STLD: ${stld_close:.2f} ({stld_ret:+.2f}% raw, {stld_abn:+.2f}% abnormal)")
        print(f"  Target: -2.58% abnormal in 5d. Current: {stld_abn:+.2f}%")
        results['stld'] = {'close': stld_close, 'raw': stld_ret, 'abnormal': stld_abn}

    # === 2. Q1/Q2 Seasonal SPY ===
    print(f"\n--- Q1/Q2 Seasonal SPY (baseline April 1: $655.24) ---")
    spy_baseline_q1q2 = 655.24
    spy_ret_q1q2 = (spy_close / spy_baseline_q1q2 - 1) * 100
    print(f"  SPY: ${spy_close:.2f} ({spy_ret_q1q2:+.2f}% from April 1)")
    results['q1q2_spy'] = {'close': spy_close, 'raw': spy_ret_q1q2}

    # === 3. Auto Tariff OOS (TM, HMC) ===
    print(f"\n--- Auto Tariff OOS (baseline April 1: TM=$209.67, HMC=$24.31, SPY=$655.24) ---")
    auto_baselines = {'TM': 209.67, 'HMC': 24.31}
    spy_auto_baseline = 655.24
    spy_ret_auto = (spy_close / spy_auto_baseline - 1) * 100
    for sym, baseline in auto_baselines.items():
        close = get_latest_close(sym)
        if close:
            ret = (close / baseline - 1) * 100
            abn = compute_abnormal(ret, spy_ret_auto)
            print(f"  {sym}: ${close:.2f} ({ret:+.2f}% raw, {abn:+.2f}% abnormal)")
            results[f'auto_{sym.lower()}'] = {'close': close, 'raw': ret, 'abnormal': abn}

    # === 4. Tariff Signal Panel (informal OOS from April 2) ===
    print(f"\n--- Tariff Signal Panel (entry April 2: SPY=$655.83) ---")
    spy_tariff_entry = 655.83
    spy_ret_tariff = (spy_close / spy_tariff_entry - 1) * 100

    tariff_signals = {
        'AEP':  ('long',  '+3.79% at 10d'),
        'AMD':  ('short', 'semiconductor basket'),
        'QCOM': ('short', 'semiconductor basket'),
        'GLD':  ('long',  '+3.87% at 20d'),
        'KO':   ('long',  '+4.4% at 10d'),
        'KRE':  ('short', '-3.1% at 10d'),
        'XLU':  ('long',  '+3.4% at 20d'),
        'COST': ('long',  '+3.57% at 10d'),
        'GDX':  ('long',  '+5.5% at 10d'),
    }

    amd_abn = qcom_abn = None
    for sym, (direction, expected) in tariff_signals.items():
        close = get_latest_close(sym)
        if close:
            entry = get_close_on_date(sym, '2026-04-02')
            if entry:
                ret = (close / entry - 1) * 100
                abn = compute_abnormal(ret, spy_ret_tariff)
                # Flip sign for shorts (positive = short profitable)
                display_abn = -abn if direction == 'short' else abn
                correct = '✓' if display_abn > 0.5 else ('✗' if display_abn < -0.5 else '~')
                print(f"  {sym:>5} {direction:>5}: {display_abn:+.2f}% abnormal {correct}  (expected {expected})")
                results[f'tariff_{sym.lower()}'] = {'close': close, 'raw': ret, 'abnormal': abn, 'direction': direction}
                if sym == 'AMD': amd_abn = abn
                if sym == 'QCOM': qcom_abn = abn

    # AMD/QCOM basket average
    if amd_abn is not None and qcom_abn is not None:
        basket_abn = (amd_abn + qcom_abn) / 2
        basket_display = -basket_abn  # short
        correct = '✓' if basket_display > 0.5 else ('✗' if basket_display < -0.5 else '~')
        print(f"  BASKET short: {basket_display:+.2f}% abnormal {correct}  (expected -2.03% at 5d)")
        results['tariff_basket'] = {'abnormal': basket_abn, 'display': basket_display}

    # === 5. ZBIO Position Check ===
    print(f"\n--- ZBIO Insider Cluster ---")
    zbio_close = get_latest_close('ZBIO')
    if zbio_close:
        print(f"  ZBIO: ${zbio_close:.2f} (pre-activation monitoring)")
        results['zbio'] = {'close': zbio_close}

    # === 6. SPY VIX Position ===
    print(f"\n--- SPY VIX Long (entry $639.74, 20d hold → April 27) ---")
    spy_entry_vix = 639.74
    spy_vix_ret = (spy_close / spy_entry_vix - 1) * 100
    print(f"  SPY: ${spy_close:.2f} ({spy_vix_ret:+.2f}% from entry)")
    results['spy_vix'] = {'close': spy_close, 'raw': spy_vix_ret}

    # Save results
    if not dry_run:
        db.record_known_effect(f'weekly_tracker_{today}', {
            'date': today,
            'spy_close': spy_close,
            'results': results,
            'last_updated': datetime.now().isoformat()
        })
        print(f"\n✓ Results saved to known_effects: weekly_tracker_{today}")
    else:
        print(f"\n[DRY RUN] Would save to weekly_tracker_{today}")

    print()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    track_all(dry_run=args.dry_run)
