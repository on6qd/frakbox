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

    # Get SPY 20d MA and VIX
    spy_data = safe_download('SPY', period='30d')
    vix_data = safe_download('^VIX', period='5d')

    if spy_data is not None and len(spy_data) >= 20:
        spy_close = spy_data['Close'].squeeze()
        ma_20 = spy_close.rolling(20).mean().iloc[-1]
        last_close = spy_close.iloc[-1]
        print(f'SPY close: ${last_close:.2f}, 20d MA: ${ma_20:.2f}')

        vix_level = None
        if vix_data is not None and len(vix_data) > 0:
            vix_level = float(vix_data['Close'].squeeze().iloc[-1])
            print(f'VIX: {vix_level:.2f}')

        if last_close > ma_20:
            print('✓ SPY above 20d MA')
        else:
            print(f'⚠ SPY ${last_close - ma_20:.2f} below 20d MA')

        # ZBIO activation decision
        # REVISED 2026-04-05: VIX gate relaxed from <20 to <25.
        # Rationale: VIX tier data (N=hundreds) shows VIX 20-25 = +3.24% avg, 61% pos.
        # Nearly identical to VIX <20 (+3.04%). OOS validation (p=0.0096) confirms overall.
        # ZBIO knowledge entry says "VIX<30" is acceptable per backtest.
        # Keep SPY>MA as secondary filter (trend confirmation).
        if vix_level and vix_level < 25 and last_close > ma_20:
            print('✓ ZBIO VIX gate PASSES (VIX < 25) — SET TRIGGER for April 14')
            print(f'  VIX tier data: <20 = +3.04%, 20-25 = +3.24% (current regime)')
        elif vix_level and vix_level < 30 and last_close > ma_20:
            print('⚠ ZBIO VIX at 25-30 (acceptable but elevated).')
            print('  VIX tier data: 25-30 = +6.79% (actually STRONGEST tier)')
            print('  Decision: ACTIVATE — VIX 25-30 historically enhances signal.')
        elif vix_level and vix_level < 30:
            print(f'⚠ ZBIO: VIX OK ({vix_level:.1f}) but SPY below MA (bearish trend)')
            print('  Decision: WAIT — SPY below MA suggests continued selling pressure')
        else:
            print('⚠ ZBIO regime filter FAILS — VIX too high (≥30)')
            print('  Consider delaying ZBIO beyond April 14 or abandoning')
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
