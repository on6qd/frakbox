#!/usr/bin/env python3
"""
Measure Event 2 of auto_import_tariff_short signal.
Event: April 2, 2026 - 25% auto import tariffs announced (Liberation Day 2026)
Baselines (April 2 close): TM=209.67, HMC=24.31, F=11.68, GM=75.04, SPY=655.24
Run on April 9, 2026 (5 trading days after April 2)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import yfinance as yf
import pandas as pd
import db; db.init_db()

BASELINES = {
    'TM': 209.67,
    'HMC': 24.31,
    'F': 11.68,
    'GM': 75.04,
    'SPY': 655.24
}

EVENT_DATE = '2026-04-02'
MEASUREMENT_DATE = '2026-04-09'  # 5 trading days after

def get_latest_close(ticker):
    t = yf.Ticker(ticker)
    h = t.history(period='5d', interval='1d', auto_adjust=True)
    if len(h) == 0:
        return None
    return float(h['Close'].iloc[-1])

print(f'Auto-Import Tariff Signal - Event 2 Measurement')
print(f'Event date: {EVENT_DATE}')
print(f'Measurement: 5-day returns')
print()

spy_current = get_latest_close('SPY')
spy_baseline = BASELINES['SPY']
spy_ret = (spy_current - spy_baseline) / spy_baseline * 100 if spy_current else 0

print(f'SPY baseline: {spy_baseline:.2f}, current: {spy_current:.2f}, return: {spy_ret:.2f}%')
print()

results = {}
for ticker, baseline in BASELINES.items():
    if ticker == 'SPY':
        continue
    current = get_latest_close(ticker)
    if current:
        ret = (current - baseline) / baseline * 100
        abnormal = ret - spy_ret
        results[ticker] = {'baseline': baseline, 'current': current, 'return': ret, 'abnormal': abnormal}
        direction_ok = abnormal < -0.5  # hypothesis: abnormal should be < -0.5%
        print(f'{ticker}: {baseline:.2f} -> {current:.2f} = {ret:.2f}% (abnormal: {abnormal:.2f}%) {"✓ DIRECTION OK" if direction_ok else "✗ WRONG DIRECTION"}')

print()
# Calculate basket avg (TM/HMC are primary autos, F/GM are secondary)
auto_tickers = ['TM', 'HMC']  # primary
basket_avg = sum(results[t]['abnormal'] for t in auto_tickers if t in results) / 2 if all(t in results for t in auto_tickers) else None
if basket_avg is not None:
    print(f'TM/HMC basket avg abnormal: {basket_avg:.2f}%')
    print(f'Hypothesis expects: < -2.0% (Event 1 was avg -4.2%)')
    print(f'Result: {"SIGNAL CONFIRMED" if basket_avg < -2.0 else "WEAK SIGNAL" if basket_avg < -0.5 else "SIGNAL FAILED"}')

# Save results
db.record_known_effect('auto_import_tariff_event2_5d', {
    'measurement_date': MEASUREMENT_DATE,
    'spy_return': spy_ret,
    'results': {t: {'return': r['return'], 'abnormal': r['abnormal']} for t, r in results.items()},
    'basket_avg_abnormal': basket_avg
})
print('Results saved to knowledge base as auto_import_tariff_event2_5d')
