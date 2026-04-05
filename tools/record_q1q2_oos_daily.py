#!/usr/bin/env python3
"""
Record daily SPY close for Q1/Q2 April first-5-trading-days OOS 2026.

Trading days: April 1, 2, 6, 7, 8 (April 3=Good Friday, 4-5=weekend)
Baseline: March 31 close = $650.34
Signal: historically 8/8 positive, avg +2.23%

Run after market close each day April 6-8.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import research, db
from tools.yfinance_utils import safe_download
from datetime import datetime, timedelta

db.init_db()

BASELINE = 650.34

# Get latest SPY data
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
spy_data = safe_download('SPY', start_date, end_date)
if spy_data is None or len(spy_data) == 0:
    print("ERROR: No SPY data available")
    sys.exit(1)

spy_close = spy_data['Close'].squeeze()
print("Recent SPY closes:")
for date, price in spy_close.tail(7).items():
    ret = (price - BASELINE) / BASELINE * 100
    print(f"  {date.strftime('%Y-%m-%d')}: ${price:.2f} ({ret:+.2f}% from baseline)")

# Get the latest close
last_date = spy_close.index[-1].strftime('%Y-%m-%d')
last_price = float(spy_close.iloc[-1])
cumulative_return = (last_price - BASELINE) / BASELINE * 100

# Count trading days in April
april_closes = {d.strftime('%Y-%m-%d'): float(p) for d, p in spy_close.items() if d.month == 4}
n_april_days = len(april_closes)

print(f"\nApril trading days recorded: {n_april_days}/5")
print(f"Cumulative return from March 31: {cumulative_return:+.2f}%")
print(f"Signal threshold: historically +2.23% avg over 5 days")

# Build status
if n_april_days >= 5:
    status = 'OOS_COMPLETE'
    verdict = 'PASS' if cumulative_return > 0 else 'FAIL'
    print(f"\n*** OOS COMPLETE: {verdict} (return: {cumulative_return:+.2f}%, threshold: >0%) ***")
else:
    status = 'OOS_IN_PROGRESS'

# Update knowledge base
effect_data = {
    'notes': f'Q1->Q2 April first-5d OOS 2026. Signal historically 8/8 positive (avg +2.23%). {n_april_days}/5 trading days recorded.',
    'status': status,
    'march31_baseline': BASELINE,
    'april_closes': april_closes,
    'cumulative_return_pct': round(cumulative_return, 2),
    'trading_days_recorded': n_april_days,
    'updated': last_date,
}

if n_april_days >= 5:
    effect_data['final_verdict'] = verdict
    effect_data['final_return_pct'] = round(cumulative_return, 2)
    effect_data['historical_avg'] = 2.23
    effect_data['is_positive'] = cumulative_return > 0

research.record_known_effect('q1_q2_april_first_week_spy_oos_2026', effect_data)
print(f"\nKnowledge base updated: q1_q2_april_first_week_spy_oos_2026 -> {status}")
