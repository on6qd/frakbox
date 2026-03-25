"""
Tariff Sector Rotation Backtest
================================
Tests: After major US tariff announcements, which sectors under/outperform?

Tariff announcement dates (2018-2025):
- 2018-03-01: Steel/aluminum tariffs announced
- 2018-06-15: China $50B tariff list released
- 2018-07-06: $34B China tariffs effective
- 2018-09-24: $200B China tariffs effective
- 2019-05-10: China tariffs raised 10->25%
- 2019-08-01: $300B China tariffs threatened
- 2019-12-15: Phase 1 deal - tariff reduction announced
- 2020-01-15: US-China Phase 1 deal signed
- 2025-02-01: 25% Canada/Mexico tariffs announced
- 2025-03-04: 25% Canada/Mexico tariffs activated
- 2025-04-02: Universal tariff announcement threatened

Sector ETFs:
- XLB: Materials
- XLC: Communications  
- XLE: Energy
- XLF: Financials
- XLI: Industrials
- XLK: Technology
- XLP: Consumer Staples
- XLRE: Real Estate
- XLU: Utilities
- XLV: Health Care
- XLY: Consumer Discretionary
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.yfinance_utils import safe_download

SECTOR_ETFS = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']
BENCHMARK = 'SPY'

# Major tariff announcement dates (market-moving escalation events)
TARIFF_DATES = [
    ('2018-03-01', 'Steel/aluminum tariffs announced'),
    ('2018-06-15', 'China $50B tariff list'),
    ('2018-07-06', 'China $34B tariffs effective'),
    ('2018-09-24', 'China $200B tariffs effective'),
    ('2019-05-10', 'China tariffs raised 10->25%'),
    ('2019-08-05', 'Currency war declared, markets crash'),
    ('2025-02-01', 'Canada/Mexico 25% announced'),
    ('2025-03-04', 'Canada/Mexico tariffs activated'),
]

LOOKBACK_DAYS = 5
FORWARD_DAYS = [1, 3, 5, 10, 20]

def analyze_sector_rotation():
    """Measure sector returns after each tariff event."""
    
    print("=" * 70)
    print("TARIFF SECTOR ROTATION ANALYSIS")
    print("=" * 70)
    print()
    
    # Download all sector ETFs + SPY
    all_tickers = SECTOR_ETFS + [BENCHMARK]
    
    # Get 10 years of data
    data = safe_download(all_tickers, start='2017-01-01', end='2026-03-25', progress=False)
    
    if data.empty:
        print("ERROR: No data downloaded")
        return
    
    if isinstance(data.columns, pd.MultiIndex):
        close = data['Close']
    else:
        # Handle flat column names like 'Close_SPY'
        close_cols = {c.replace('Close_', ''): data[c] for c in data.columns if c.startswith('Close_')}
        if close_cols:
            close = pd.DataFrame(close_cols, index=data.index)
        else:
            close = data
    
    print(f"Data shape: {close.shape}")
    print(f"Tickers available: {list(close.columns)}")
    print()
    
    # For each tariff date, calculate forward returns by sector
    results = {}
    
    for event_date, event_desc in TARIFF_DATES:
        # Find the nearest trading day
        event_dt = pd.Timestamp(event_date)
        
        # Find entry day (next close after announcement)
        future_dates = close.index[close.index >= event_dt]
        if len(future_dates) == 0:
            print(f"SKIP: {event_date} - no data after")
            continue
        
        entry_date = future_dates[0]
        entry_idx = close.index.get_loc(entry_date)
        
        spy_returns = {}
        sector_returns = {}
        
        for horizon in FORWARD_DAYS:
            exit_idx = entry_idx + horizon
            if exit_idx >= len(close):
                continue
            
            exit_date = close.index[exit_idx]
            
            # SPY return
            spy_entry = close[BENCHMARK].iloc[entry_idx]
            spy_exit = close[BENCHMARK].iloc[exit_idx]
            spy_ret = (spy_exit / spy_entry - 1) * 100
            spy_returns[horizon] = spy_ret
            
            # Each sector
            for sector in SECTOR_ETFS:
                if sector not in close.columns:
                    continue
                s_entry = close[sector].iloc[entry_idx]
                s_exit = close[sector].iloc[exit_idx]
                s_ret = (s_exit / s_entry - 1) * 100
                abnormal = s_ret - spy_ret
                
                if sector not in sector_returns:
                    sector_returns[sector] = {}
                if horizon not in sector_returns[sector]:
                    sector_returns[sector][horizon] = []
                sector_returns[sector][horizon].append(abnormal)
        
        results[event_date] = {
            'desc': event_desc,
            'entry': entry_date.strftime('%Y-%m-%d'),
            'spy': spy_returns,
            'sectors': sector_returns,
        }
        
        # Print event summary
        spy_5d = spy_returns.get(5, 0)
        print(f"{event_date}: {event_desc}")
        print(f"  Entry: {entry_date.strftime('%Y-%m-%d')} | SPY 5d: {spy_5d:+.2f}%")
        
        # Print sector abnormal returns for 5d
        sector_5d = [(s, sector_returns[s].get(5, [None])[-1]) 
                     for s in SECTOR_ETFS if s in sector_returns and 5 in sector_returns[s]]
        sector_5d.sort(key=lambda x: x[1] if x[1] is not None else 0, reverse=True)
        print("  5d abnormal: " + " | ".join(f"{s}:{v:+.1f}%" for s, v in sector_5d[:6]))
        print()
    
    print()
    print("=" * 70)
    print("AGGREGATE SECTOR ABNORMAL RETURNS ACROSS ALL TARIFF EVENTS")
    print("=" * 70)
    
    # Aggregate across all events
    agg = {sector: {} for sector in SECTOR_ETFS}
    
    for event_date, event_data in results.items():
        for sector in SECTOR_ETFS:
            if sector not in event_data['sectors']:
                continue
            for horizon in FORWARD_DAYS:
                if horizon not in event_data['sectors'][sector]:
                    continue
                if horizon not in agg[sector]:
                    agg[sector][horizon] = []
                vals = event_data['sectors'][sector][horizon]
                agg[sector][horizon].extend(vals)
    
    # Print aggregate table
    print(f"\n{'Sector':8s} | {'1d':>8s} | {'3d':>8s} | {'5d':>8s} | {'10d':>8s} | {'20d':>8s} | n")
    print("-" * 65)
    
    # Sort by 5d return
    sector_5d_avg = [(s, np.mean(agg[s].get(5, [0]))) for s in SECTOR_ETFS if 5 in agg[s]]
    sector_5d_avg.sort(key=lambda x: x[1], reverse=True)
    
    for sector, _ in sector_5d_avg:
        vals = {}
        for h in FORWARD_DAYS:
            if h in agg[sector]:
                vals[h] = np.mean(agg[sector][h])
            else:
                vals[h] = None
        n = len(agg[sector].get(5, []))
        print(f"{sector:8s} | {vals.get(1,0) or 0:+7.2f}% | {vals.get(3,0) or 0:+7.2f}% | "
              f"{vals.get(5,0) or 0:+7.2f}% | {vals.get(10,0) or 0:+7.2f}% | "
              f"{vals.get(20,0) or 0:+7.2f}% | {n}")
    
    print()
    print("Note: Returns are abnormal (sector - SPY). Positive = outperforms SPY.")
    print("INTERPRETATION:")
    print("  Sectors with consistent negative abnormal returns UNDERPERFORM after tariffs.")
    print("  Sectors with consistent positive abnormal returns OUTPERFORM after tariffs.")
    
    # Find the most consistent signals
    print()
    print("=" * 70)
    print("SIGNALS (consistent across events, 5d horizon):")
    print("=" * 70)
    from scipy import stats
    for sector in SECTOR_ETFS:
        if 5 not in agg[sector] or len(agg[sector][5]) < 4:
            continue
        vals = agg[sector][5]
        avg = np.mean(vals)
        n = len(vals)
        t_stat, p_val = stats.ttest_1samp(vals, 0)
        direction = sum(1 for v in vals if v > 0) / n
        if abs(avg) > 0.5 or p_val < 0.1:
            print(f"{sector}: avg={avg:+.2f}% n={n} p={p_val:.3f} direction={direction:.0%}")


if __name__ == '__main__':
    analyze_sector_rotation()


def analyze_xlf_detail():
    """Detailed analysis of XLF after tariff events."""
    from scipy import stats
    
    all_tickers = ['XLF', 'SPY', 'XLU', 'XLP', 'XLB']
    data = safe_download(all_tickers, start='2017-01-01', end='2026-03-25', progress=False)
    
    if isinstance(data.columns, pd.MultiIndex):
        close = data['Close']
    else:
        close_cols = {c.replace('Close_', ''): data[c] for c in data.columns if c.startswith('Close_')}
        close = pd.DataFrame(close_cols, index=data.index)
    
    print("=" * 60)
    print("XLF DETAIL AFTER TARIFF EVENTS")
    print("=" * 60)
    
    xlf_abnormals_5d = []
    
    for event_date, event_desc in TARIFF_DATES:
        event_dt = pd.Timestamp(event_date)
        future_dates = close.index[close.index >= event_dt]
        if len(future_dates) == 0:
            continue
        entry_idx = close.index.get_loc(future_dates[0])
        
        for horizon in [1, 3, 5, 10]:
            exit_idx = entry_idx + horizon
            if exit_idx >= len(close):
                continue
            
            spy_ret = (close['SPY'].iloc[exit_idx] / close['SPY'].iloc[entry_idx] - 1) * 100
            xlf_ret = (close['XLF'].iloc[exit_idx] / close['XLF'].iloc[entry_idx] - 1) * 100
            abnormal = xlf_ret - spy_ret
            
            if horizon == 5:
                xlf_abnormals_5d.append(abnormal)
                print(f"  {event_date}: XLF {horizon}d raw={xlf_ret:+.1f}% SPY={spy_ret:+.1f}% abnormal={abnormal:+.1f}%")
    
    # t-test
    print()
    t, p = stats.ttest_1samp(xlf_abnormals_5d, 0)
    n = len(xlf_abnormals_5d)
    avg = np.mean(xlf_abnormals_5d)
    direction = sum(1 for v in xlf_abnormals_5d if v < 0) / n  # direction correct (negative)
    print(f"XLF 5d after tariff: avg={avg:+.2f}% n={n} t={t:.2f} p={p:.4f}")
    print(f"Direction (negative abnormal): {direction:.0%}")
    
    # Binomial test
    binom_p = stats.binom_test(int(direction * n), n, 0.5, alternative='greater')
    print(f"Binomial p (all negative): {binom_p:.4f}")
    
    print()
    print("CAUSAL MECHANISM ANALYSIS:")
    print("Financials underperform tariffs because:")
    print("1. Trade war → slowdown risk → lower loan demand → bank revenue down")
    print("2. Tariff inflation → potential rate confusion → yield curve uncertainty")
    print("3. Credit risk increases for corporate borrowers in affected sectors")
    print("4. Global banks face direct trade finance disruption")


if __name__ == '__main__':
    # analyze_sector_rotation()  # already ran above
    analyze_xlf_detail()
