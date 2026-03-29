"""
Sector Recovery After VIX>30 Spikes
=====================================
Which sector ETFs outperform SPY in the 20-30 days after VIX first crosses 30?
Tests: XLK, XLV, XLF, XLU, XLP, XLI, XLB, XLE, GLD, TLT
Uses same spike dates as vix_normalization_analysis.py
"""
import sys
sys.path.insert(0, '.')
import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats

SECTORS = ['XLK', 'XLV', 'XLF', 'XLU', 'XLP', 'XLI', 'XLB', 'XLE', 'GLD', 'TLT', 'QQQ', 'IWM']
SPIKE_DATES = [
    '2010-05-06', '2010-06-07', '2011-08-04', '2011-09-06', '2011-10-07', '2011-11-09',
    '2015-08-24', '2018-02-05', '2018-12-21', '2020-02-27', '2020-03-30', '2020-04-30',
    '2020-06-11', '2020-07-13', '2020-09-03', '2020-10-26', '2021-01-27', '2021-12-01',
    '2022-01-25', '2022-02-28', '2022-04-26', '2022-06-13', '2022-09-26', '2024-08-05',
    '2025-04-03'
]
TARIFF_DATES = ['2018-02-05', '2018-12-21', '2025-04-03']

def run():
    print("Downloading sector ETFs (2010-2026)...")
    all_tickers = SECTORS + ['SPY']
    data = yf.download(all_tickers, start='2010-01-01', end='2026-03-29', progress=False)['Close']
    
    spike_dates = pd.to_datetime(SPIKE_DATES)
    tariff_dates = pd.to_datetime(TARIFF_DATES)
    
    results = {s: {'10d': [], '20d': [], '30d': []} for s in SECTORS}
    spy_returns = {'10d': [], '20d': [], '30d': []}
    
    for spike_dt in spike_dates:
        # Find nearest trading day on or after spike
        future = data.loc[data.index >= spike_dt]
        if len(future) < 30:
            continue
        
        spy_entry = float(future['SPY'].iloc[0])
        spy_10d = float(future['SPY'].iloc[min(10, len(future)-1)])
        spy_20d = float(future['SPY'].iloc[min(20, len(future)-1)])
        spy_30d = float(future['SPY'].iloc[min(30, len(future)-1)])
        
        spy_r10 = (spy_10d / spy_entry - 1) * 100
        spy_r20 = (spy_20d / spy_entry - 1) * 100
        spy_r30 = (spy_30d / spy_entry - 1) * 100
        spy_returns['10d'].append(spy_r10)
        spy_returns['20d'].append(spy_r20)
        spy_returns['30d'].append(spy_r30)
        
        for sector in SECTORS:
            if sector not in data.columns:
                continue
            s_entry = float(future[sector].iloc[0])
            s_10d = float(future[sector].iloc[min(10, len(future)-1)])
            s_20d = float(future[sector].iloc[min(20, len(future)-1)])
            s_30d = float(future[sector].iloc[min(30, len(future)-1)])
            
            results[sector]['10d'].append((s_10d / s_entry - 1) * 100 - spy_r10)
            results[sector]['20d'].append((s_20d / s_entry - 1) * 100 - spy_r20)
            results[sector]['30d'].append((s_30d / s_entry - 1) * 100 - spy_r30)
    
    print(f"\nABNORMAL RETURNS vs SPY after VIX>30 first cross (N={len(spike_dates)} events)")
    print(f"{'Sector':<8} {'10d_avg':>8} {'10d_dir':>8} {'20d_avg':>8} {'20d_dir':>8} {'30d_avg':>8} {'30d_dir':>8}")
    print("-" * 64)
    
    sector_20d = {}
    for sector in SECTORS:
        r = results[sector]
        n = len(r['10d'])
        if n == 0:
            continue
        
        avg10 = np.mean(r['10d'])
        dir10 = np.mean([x > 0 for x in r['10d']]) * 100
        avg20 = np.mean(r['20d'])
        dir20 = np.mean([x > 0 for x in r['20d']]) * 100
        avg30 = np.mean(r['30d'])
        dir30 = np.mean([x > 0 for x in r['30d']]) * 100
        
        sector_20d[sector] = avg20
        print(f"{sector:<8} {avg10:>+7.2f}% {dir10:>7.0f}%  {avg20:>+7.2f}% {dir20:>7.0f}%  {avg30:>+7.2f}% {dir30:>7.0f}%")
    
    # Sort by 20d
    print()
    best = sorted(sector_20d.items(), key=lambda x: x[1], reverse=True)
    print("RANKED BY 20d ABNORMAL RETURN:")
    for s, r in best:
        print(f"  {s}: {r:+.2f}%")
    
    # Tariff subset
    print(f"\nTARIFF SUBGROUP (2018-02-05, 2018-12-21, 2025-04-03):")
    for sector in ['GLD', 'XLP', 'XLU', 'XLV', 'XLK', 'XLF']:
        vals = []
        for i, spike_dt in enumerate(spike_dates):
            if spike_dt in tariff_dates:
                if i < len(results[sector]['20d']):
                    vals.append(results[sector]['20d'][i])
        if vals:
            print(f"  {sector}: 20d abnormal avg = {np.mean(vals):+.2f}% (n={len(vals)})")

if __name__ == '__main__':
    run()
