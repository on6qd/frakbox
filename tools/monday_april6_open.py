#!/usr/bin/env python3
"""
Monday April 6, 2026 — Market Open Tasks
Run this script after 9:35 AM ET (give 5 min for order fill).

Tasks:
1. Complete SYK short (buy order db5e3b80 fills at open)
2. Check SPY VIX long position status
3. Record Q1/Q2 seasonal day 3 (April 6 close) — run AFTER close
4. Check ZBIO regime filter (SPY vs 20d MA)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import trader, research, db, json
from datetime import datetime

db.init_db()
api = trader.get_api()

print("=" * 60)
print("MONDAY APRIL 6, 2026 — MARKET OPEN TASKS")
print("=" * 60)

# --- Task 1: Complete SYK short ---
print("\n--- TASK 1: SYK Short Completion ---")
try:
    order = api.get_order('db5e3b80-739e-4d25-ad33-207254db303e')
    print(f'Order status: {order.status}')
    print(f'Filled avg price: {order.filled_avg_price}')

    if order.status == 'filled':
        exit_price = float(order.filled_avg_price)
        entry_price = 326.229333
        pnl_pct = (entry_price - exit_price) / entry_price * 100  # short: profit when exit < entry
        print(f'Entry: ${entry_price:.2f}, Exit: ${exit_price:.2f}, PnL: {pnl_pct:.2f}%')

        result = research.complete_hypothesis(
            hypothesis_id='5b09b097',
            exit_price=exit_price,
            mechanism_validated=False,
            post_mortem=f'SYK 52w low momentum short: Entry=${entry_price:.2f}, Exit=${exit_price:.2f} ({pnl_pct:.2f}%). sp500_52w_low_momentum_short signal previously marked DEAD_END_OOS_INVERTED. This was a final validation trade confirming the dead end.',
        )
        print('✓ SYK hypothesis completed!')
    else:
        print(f'⚠ Order not yet filled (status={order.status}). Wait until after 9:35 AM ET.')
except Exception as e:
    print(f'ERROR with SYK order: {e}')

# --- Task 2: SPY VIX Long Status ---
print("\n--- TASK 2: SPY VIX Long Position ---")
try:
    positions = api.list_positions()
    spy_pos = [p for p in positions if p.symbol == 'SPY']
    if spy_pos:
        p = spy_pos[0]
        print(f'SPY: {p.qty} shares @ ${float(p.avg_entry_price):.2f}')
        print(f'Current: ${float(p.current_price):.2f}, P&L: ${float(p.unrealized_pl):.2f} ({float(p.unrealized_plpc)*100:.2f}%)')
    else:
        print('⚠ No SPY position found!')

    h = db.get_hypothesis_by_id('b63a0168')
    print(f'Hypothesis status: {h["status"]}, target deadline: April 27')
except Exception as e:
    print(f'ERROR checking SPY: {e}')

# --- Task 3: ZBIO Regime Check ---
print("\n--- TASK 3: ZBIO Regime Filter ---")
try:
    from tools.yfinance_utils import safe_download
    import pandas as pd

    # Get SPY 20d MA
    spy_data = safe_download('SPY', period='30d')
    if spy_data is not None and len(spy_data) >= 20:
        spy_close = spy_data['Close'].squeeze()
        ma_20 = spy_close.rolling(20).mean().iloc[-1]
        last_close = spy_close.iloc[-1]
        print(f'SPY close: ${last_close:.2f}, 20d MA: ${ma_20:.2f}')
        if last_close > ma_20:
            print('✓ ZBIO regime filter PASSES — SPY above 20d MA')
        else:
            print(f'⚠ ZBIO regime filter FAILS — SPY ${last_close - ma_20:.2f} below 20d MA')
            print('  Consider delaying ZBIO trigger beyond April 14')
    else:
        print('⚠ Insufficient SPY data for 20d MA calculation')
except Exception as e:
    print(f'ERROR checking ZBIO regime: {e}')

print("\n" + "=" * 60)
print("AFTER MARKET CLOSE (4:15 PM ET):")
print("- Record April 6 SPY close for Q1/Q2 OOS (day 3 of 5)")
print("- Check SYK final close for completion reconciliation")
print("- Run: python3 data_tasks.py price-history --symbol SPY --days 5")
print("=" * 60)
