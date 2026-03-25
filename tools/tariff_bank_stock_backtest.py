"""
Tariff Impact on Individual Bank Stocks
Tests large banks (JPM, BAC, WFC, C, GS, MS) after tariff events.
If magnitude > 1.5%, creates pre-registerable hypothesis.
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.yfinance_utils import safe_download
from scipy import stats

BANK_STOCKS = ['JPM', 'BAC', 'WFC', 'C', 'GS', 'MS']
BENCHMARK = 'SPY'

TARIFF_DATES = [
    '2018-03-01', '2018-06-15', '2018-07-06', '2018-09-24',
    '2019-05-10', '2019-08-05', '2025-02-01', '2025-03-04',
]

DISCOVERY = TARIFF_DATES[:4]  # 2018
VALIDATION = TARIFF_DATES[4:]  # 2019 + 2025

def analyze_banks():
    tickers = BANK_STOCKS + [BENCHMARK]
    data = safe_download(tickers, start='2017-01-01', end='2026-03-25', progress=False)
    
    if isinstance(data.columns, pd.MultiIndex):
        close = data['Close']
    else:
        close_cols = {c.replace('Close_', ''): data[c] for c in data.columns if c.startswith('Close_')}
        close = pd.DataFrame(close_cols, index=data.index)
    
    print("=" * 60)
    print("BANK STOCK ABNORMAL RETURNS AFTER TARIFF EVENTS")
    print("=" * 60)
    
    results = {stock: {'discovery': [], 'validation': []} for stock in BANK_STOCKS}
    
    for event_date in TARIFF_DATES:
        event_dt = pd.Timestamp(event_date)
        future = close.index[close.index >= event_dt]
        if len(future) == 0:
            continue
        entry_idx = close.index.get_loc(future[0])
        exit_idx = entry_idx + 5
        if exit_idx >= len(close):
            continue
        
        spy_ret = (close[BENCHMARK].iloc[exit_idx] / close[BENCHMARK].iloc[entry_idx] - 1) * 100
        phase = 'discovery' if event_date in DISCOVERY else 'validation'
        
        print(f"\n{event_date} [{phase}] SPY 5d: {spy_ret:+.1f}%")
        for stock in BANK_STOCKS:
            if stock not in close.columns:
                continue
            s_ret = (close[stock].iloc[exit_idx] / close[stock].iloc[entry_idx] - 1) * 100
            abn = s_ret - spy_ret
            results[stock][phase].append(abn)
            print(f"  {stock}: raw={s_ret:+.1f}% abn={abn:+.1f}%")
    
    print("\n" + "=" * 60)
    print("AGGREGATE (5d abnormal vs SPY)")
    print("=" * 60)
    print(f"{'Stock':6s} | {'Disc avg':>10s} | {'Val avg':>10s} | {'All avg':>10s} | {'Dir':>6s} | {'p-val':>8s}")
    print("-" * 60)
    
    for stock in BANK_STOCKS:
        disc = results[stock]['discovery']
        val = results[stock]['validation']
        all_vals = disc + val
        if len(all_vals) < 4:
            continue
        disc_avg = np.mean(disc) if disc else 0
        val_avg = np.mean(val) if val else 0
        all_avg = np.mean(all_vals)
        direction = sum(1 for v in all_vals if v < 0) / len(all_vals)  # % negative
        t, p = stats.ttest_1samp(all_vals, 0)
        print(f"{stock:6s} | {disc_avg:+9.2f}% | {val_avg:+9.2f}% | {all_avg:+9.2f}% | {direction:>5.0%} | {p:.4f}")
    
    print()
    print("SIGNALS (magnitude > 1.5%, direction > 70% negative):")
    for stock in BANK_STOCKS:
        all_vals = results[stock]['discovery'] + results[stock]['validation']
        val = results[stock]['validation']
        if len(all_vals) < 4:
            continue
        avg = np.mean(all_vals)
        val_avg = np.mean(val) if val else 0
        direction = sum(1 for v in all_vals if v < 0) / len(all_vals)
        t, p = stats.ttest_1samp(all_vals, 0)
        if abs(avg) > 1.5 and direction >= 0.70:
            print(f"SIGNAL: {stock} avg={avg:+.2f}% val_avg={val_avg:+.2f}% direction={direction:.0%} p={p:.4f}")
            val_n = len(val)
            val_dir = sum(1 for v in val if v < 0) / val_n if val_n > 0 else 0
            print(f"  OOS (n={val_n}): avg={val_avg:+.2f}% direction={val_dir:.0%}")

if __name__ == '__main__':
    analyze_banks()
