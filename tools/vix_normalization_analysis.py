"""
VIX Normalization Analysis
==========================
After VIX spikes above 30, how many days until it falls back below:
- 25 (insider cluster re-enable threshold)
- 20 (primary signal threshold)

Also: what does SPY do while waiting for VIX to normalize?
"""
import sys
sys.path.insert(0, '.')
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def run():
    print("Downloading VIX and SPY (2010-2026)...")
    vix = yf.download('^VIX', start='2010-01-01', end='2026-03-29', progress=False)['Close'].squeeze()
    spy = yf.download('SPY', start='2010-01-01', end='2026-03-29', progress=False)['Close'].squeeze()
    
    above_30 = vix >= 30
    transitions = above_30 & ~above_30.shift(1).fillna(False)
    spike_dates = transitions[transitions].index.tolist()
    
    # De-duplicate: events within 30 days = same spike cluster
    deduped = []
    last_date = None
    for d in spike_dates:
        if last_date is None or (d - last_date).days > 30:
            deduped.append(d)
            last_date = d
    
    print(f"VIX>30 first-cross events (30d deduplicated): n={len(deduped)}")
    print()
    
    results = []
    for spike_date in deduped:
        vix_val = float(vix.loc[spike_date])
        
        future_vix = vix.loc[vix.index > spike_date]
        days_to_25 = None
        days_to_20 = None
        
        for i, (date, val) in enumerate(future_vix.items()):
            if days_to_25 is None and val < 25:
                days_to_25 = i + 1
            if days_to_20 is None and val < 20:
                days_to_20 = i + 1
                break
        
        spy_forward = spy.loc[spy.index >= spike_date]
        spy_5d = (float(spy_forward.iloc[min(5, len(spy_forward)-1)]) / float(spy_forward.iloc[0]) - 1) * 100 if len(spy_forward) > 5 else None
        spy_10d = (float(spy_forward.iloc[min(10, len(spy_forward)-1)]) / float(spy_forward.iloc[0]) - 1) * 100 if len(spy_forward) > 10 else None
        spy_20d = (float(spy_forward.iloc[min(20, len(spy_forward)-1)]) / float(spy_forward.iloc[0]) - 1) * 100 if len(spy_forward) > 20 else None
        
        print(f"  {spike_date.date()}: VIX={vix_val:.1f} | d_to_<25={days_to_25} | d_to_<20={days_to_20} | SPY 5d={f'{spy_5d:+.1f}%' if spy_5d else 'N/A'} 10d={f'{spy_10d:+.1f}%' if spy_10d else 'N/A'} 20d={f'{spy_20d:+.1f}%' if spy_20d else 'N/A'}")
        
        results.append({
            'date': spike_date,
            'vix': vix_val,
            'days_to_25': days_to_25,
            'days_to_20': days_to_20,
            'spy_5d': spy_5d,
            'spy_10d': spy_10d,
            'spy_20d': spy_20d
        })
    
    df = pd.DataFrame(results)
    df_valid = df.dropna(subset=['days_to_25'])
    
    print()
    print("=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    print(f"N total spike events: {len(df)}")
    print(f"N with VIX<25 resolved: {len(df_valid)}")
    print()
    print(f"Days to VIX<25 (insider cluster re-enable):")
    print(f"  Median: {df_valid['days_to_25'].median():.0f} days")
    print(f"  Mean: {df_valid['days_to_25'].mean():.1f} days")
    print(f"  25th pct: {df_valid['days_to_25'].quantile(0.25):.0f} days")
    print(f"  75th pct: {df_valid['days_to_25'].quantile(0.75):.0f} days")
    print(f"  Resolved within 10d: {(df_valid['days_to_25'] <= 10).mean()*100:.0f}%")
    print(f"  Resolved within 20d: {(df_valid['days_to_25'] <= 20).mean()*100:.0f}%")
    print(f"  Resolved within 30d: {(df_valid['days_to_25'] <= 30).mean()*100:.0f}%")
    print()
    df_v20 = df.dropna(subset=['days_to_20'])
    if len(df_v20) > 0:
        print(f"Days to VIX<20 (primary signal zone):")
        print(f"  Median: {df_v20['days_to_20'].median():.0f} days")
        print(f"  Mean: {df_v20['days_to_20'].mean():.1f} days")
        print()
    
    print("SPY returns from VIX first-cross-above-30:")
    print(f"  5d avg: {df['spy_5d'].mean():+.1f}%, pos rate: {(df['spy_5d']>0).mean()*100:.0f}%")
    print(f"  10d avg: {df['spy_10d'].mean():+.1f}%, pos rate: {(df['spy_10d']>0).mean()*100:.0f}%")
    print(f"  20d avg: {df['spy_20d'].mean():+.1f}%, pos rate: {(df['spy_20d']>0).mean()*100:.0f}%")
    print()
    
    # Tariff events subset
    tariff_dates = [d for d in deduped if d.year in [2018, 2019, 2025, 2026]]
    print(f"TARIFF-RELATED SUBGROUP (2018-2019, 2025-2026): n={len(tariff_dates)}")
    df_tariff = df[df['date'].isin(tariff_dates)]
    if len(df_tariff) > 0:
        dv = df_tariff.dropna(subset=['days_to_25'])
        if len(dv) > 0:
            print(f"  Median days to VIX<25: {dv['days_to_25'].median():.0f}")
            print(f"  SPY 20d avg: {df_tariff['spy_20d'].mean():+.1f}%")
    
    print()
    med = int(df_valid['days_to_25'].median())
    p75 = int(df_valid['days_to_25'].quantile(0.75))
    base = datetime(2026, 3, 27)
    print(f"CURRENT SPIKE (2026-03-27, VIX=31.05):")
    print(f"  Median expectation: VIX<25 by ~{(base + timedelta(days=med)).strftime('%Y-%m-%d')} (+{med}d)")
    print(f"  75th pct (slower): VIX<25 by ~{(base + timedelta(days=p75)).strftime('%Y-%m-%d')} (+{p75}d)")
    print(f"  Implication: insider cluster signal likely re-enabled {(base + timedelta(days=med)).strftime('%Y-%m-%d')}-{(base + timedelta(days=p75)).strftime('%Y-%m-%d')}")

if __name__ == '__main__':
    run()
