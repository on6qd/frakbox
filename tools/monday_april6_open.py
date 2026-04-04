#!/usr/bin/env python3
"""
Monday April 6, 2026 — Market Open Tasks
Run this script after 9:35 AM ET.

Tasks:
1. SYK short ALREADY COMPLETED (closed by trade_loop April 1 @ $327.32, -0.33%)
2. Check SPY VIX long position status
3. Check ZBIO regime filter (SPY vs 20d MA)
4. Record Q1/Q2 seasonal day 3 (April 6 close) — run AFTER close via record_q1q2_oos_daily.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import trader, db, json
from datetime import datetime

db.init_db()
api = trader.get_api()

print("=" * 60)
print("MONDAY APRIL 6, 2026 — MARKET OPEN TASKS")
print("=" * 60)

# --- Task 1: SYK Already Done ---
print("\n--- TASK 1: SYK Short — ALREADY COMPLETED ---")
print("Closed by trade_loop on April 1 at $327.32. Entry $326.23, PnL: -0.33%.")
print("Hypothesis 5b09b097 completed in weekend session April 4.")

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
    trade = h.get('trade', {})
    entry = trade.get('entry_price', 0)
    stop = trade.get('stop_loss_pct', 8)
    stop_price = entry * (1 - stop/100)
    print(f'Stop loss: ${stop_price:.2f} ({stop}% below ${entry:.2f})')
except Exception as e:
    print(f'ERROR checking SPY: {e}')

# --- Task 3: ZBIO Regime Check ---
print("\n--- TASK 3: ZBIO Regime Filter ---")
try:
    from tools.yfinance_utils import safe_download

    # Get SPY 20d MA
    spy_data = safe_download('SPY', period='30d')
    if spy_data is not None and len(spy_data) >= 20:
        spy_close = spy_data['Close'].squeeze()
        ma_20 = spy_close.rolling(20).mean().iloc[-1]
        last_close = spy_close.iloc[-1]
        print(f'SPY close: ${last_close:.2f}, 20d MA: ${ma_20:.2f}')
        if last_close > ma_20:
            print('✓ ZBIO regime filter PASSES — SPY above 20d MA')
            print('  ZBIO trigger set for April 14 — proceed as planned')
        else:
            print(f'⚠ ZBIO regime filter FAILS — SPY ${last_close - ma_20:.2f} below 20d MA')
            print('  Consider delaying ZBIO trigger beyond April 14')
    else:
        print('⚠ Insufficient SPY data for 20d MA calculation')
except Exception as e:
    print(f'ERROR checking ZBIO regime: {e}')

print("\n" + "=" * 60)
print("AFTER MARKET CLOSE (4:15 PM ET):")
print("- Run: python3 tools/record_q1q2_oos_daily.py  (day 3 of 5)")
print("- April 7: run again (day 4)")
print("- April 8: run again (day 5 — OOS completes)")
print("- April 9: Run python3 tools/measure_auto_tariff_event2.py")
print("- April 10: REPL FDA PDUFA — monitor for activation/abandonment")
print("=" * 60)
