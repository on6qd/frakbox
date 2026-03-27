"""
PEAD (Post-Earnings Announcement Drift) Backtest
=================================================
Tests whether large-cap S&P 500 stocks that beat EPS consensus by >=10%
show measurable positive drift (abnormal return vs SPY) over 5-10 days.

Usage:
  python tools/pead_backtest.py
"""

import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from tools.yfinance_utils import safe_download

# Configuration
MIN_SURPRISE_PCT = 10.0    # Minimum EPS beat (%)
MAX_SURPRISE_PCT = 200.0   # Exclude likely GAAP one-time items
MIN_MARKET_CAP_B = 10.0    # $10B+ (large-cap)
DISCOVERY_END = '2024-01-01'  # 2020-2023 = discovery
OOS_START = '2024-01-01'      # 2024-2025 = OOS validation

# Large-cap S&P 500 stocks to test (mega-cap + large-cap, diverse sectors)
UNIVERSE = [
    # Tech
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'AMZN', 'ADBE', 'CRM', 'ORCL', 'CSCO',
    'AMD', 'QCOM', 'INTC', 'TXN', 'AMAT', 'MU', 'KLAC', 'LRCX', 'CDNS', 'SNPS',
    # Healthcare
    'UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'ABT', 'TMO', 'DHR', 'BMY', 'AMGN',
    'GILD', 'ISRG', 'SYK', 'BSX', 'MDT', 'HCA', 'CI', 'CVS', 'ELV', 'HUM',
    # Financials
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'AXP', 'COF', 'USB', 'PNC',
    'V', 'MA', 'PYPL', 'BRK-B', 'ICE', 'CME', 'SPGI', 'MCO', 'FIS', 'FISV',
    # Consumer
    'HD', 'MCD', 'NKE', 'SBUX', 'TGT', 'COST', 'WMT', 'LOW', 'TJX', 'ROST',
    'PG', 'KO', 'PEP', 'PM', 'MO', 'KHC', 'GIS', 'K', 'CPB', 'CAG',
    # Industrials
    'HON', 'GE', 'CAT', 'DE', 'MMM', 'EMR', 'ETN', 'ITW', 'PH', 'ROK',
    # Energy
    'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'HAL', 'PSX', 'VLO', 'MPC',
    # Real Estate / Other
    'AMT', 'PLD', 'EQIX', 'CCI', 'SPG',
]


def get_earnings_beats(symbol: str, min_pct: float = MIN_SURPRISE_PCT,
                        max_pct: float = MAX_SURPRISE_PCT) -> list[dict]:
    """Get all EPS beats >= min_pct for a symbol from yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        ed = ticker.earnings_dates
        if ed is None or len(ed) == 0:
            return []
        
        # Filter: actual beats in range
        beats = ed[
            (ed['Surprise(%)'] >= min_pct) & 
            (ed['Surprise(%)'] <= max_pct)
        ].dropna(subset=['Surprise(%)', 'Reported EPS', 'EPS Estimate'])
        
        results = []
        for dt, row in beats.iterrows():
            date_str = str(dt)[:10]
            results.append({
                'symbol': symbol,
                'date': date_str,
                'surprise_pct': round(float(row['Surprise(%)']), 1),
                'reported_eps': float(row['Reported EPS']),
                'est_eps': float(row['EPS Estimate']),
            })
        return results
    except Exception as e:
        return []


def measure_abnormal_return(symbol: str, event_date: str, horizon_days: int) -> float | None:
    """Measure abnormal return vs SPY over horizon_days starting from next trading day."""
    # Get price data for symbol and SPY around event
    start = pd.Timestamp(event_date) - pd.Timedelta(days=5)
    end = pd.Timestamp(event_date) + pd.Timedelta(days=horizon_days + 10)
    
    sym_data = safe_download(symbol, start=str(start)[:10], end=str(end)[:10])
    spy_data = safe_download('SPY', start=str(start)[:10], end=str(end)[:10])
    
    if sym_data is None or spy_data is None:
        return None
    
    sym_closes = sym_data['Close']
    spy_closes = spy_data['Close']
    
    # Find the first trading day on or after event_date
    event_ts = pd.Timestamp(event_date)
    
    sym_dates = sym_closes.index
    spy_dates = spy_closes.index
    
    # Common dates only
    common_dates = sym_dates.intersection(spy_dates)
    common_dates = sorted([d for d in common_dates if pd.Timestamp(str(d)[:10]) >= event_ts])
    
    if len(common_dates) < horizon_days + 1:
        return None
    
    entry_date = common_dates[0]
    
    # Find exit date approximately horizon_days trading days later
    exit_idx = min(horizon_days, len(common_dates) - 1)
    exit_date = common_dates[exit_idx]
    
    sym_entry = float(sym_closes.loc[entry_date]) if entry_date in sym_closes.index else None
    sym_exit = float(sym_closes.loc[exit_date]) if exit_date in sym_closes.index else None
    spy_entry = float(spy_closes.loc[entry_date]) if entry_date in spy_closes.index else None
    spy_exit = float(spy_closes.loc[exit_date]) if exit_date in spy_closes.index else None
    
    if None in (sym_entry, sym_exit, spy_entry, spy_exit) or sym_entry == 0 or spy_entry == 0:
        return None
    
    sym_return = (sym_exit / sym_entry - 1) * 100
    spy_return = (spy_exit / spy_entry - 1) * 100
    return round(sym_return - spy_return, 3)


def run_pead_backtest():
    print("=" * 70)
    print("PEAD EARNINGS BEAT BACKTEST")
    print("=" * 70)
    print(f"Universe: {len(UNIVERSE)} stocks")
    print(f"Min EPS beat: {MIN_SURPRISE_PCT}%  |  Max: {MAX_SURPRISE_PCT}% (excludes GAAP anomalies)")
    print(f"Discovery: 2020-2023  |  OOS: 2024-2025")
    print()
    
    all_events = []
    
    # Step 1: Collect all earnings beats
    print("Collecting earnings data...")
    failed = 0
    for i, sym in enumerate(UNIVERSE):
        beats = get_earnings_beats(sym)
        all_events.extend(beats)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(UNIVERSE)}: {len(all_events)} beats found so far")
    
    print(f"\nTotal EPS beats found: {len(all_events)}")
    
    if len(all_events) < 20:
        print("ERROR: Too few events found. Cannot run backtest.")
        return
    
    # Step 2: Measure abnormal returns
    print("\nMeasuring abnormal returns (5d and 10d)...")
    results = []
    for i, ev in enumerate(all_events):
        if (i + 1) % 25 == 0:
            print(f"  {i+1}/{len(all_events)} events processed")
        
        abn_5d = measure_abnormal_return(ev['symbol'], ev['date'], 5)
        abn_10d = measure_abnormal_return(ev['symbol'], ev['date'], 10)
        
        if abn_5d is not None:
            results.append({
                **ev,
                'abn_5d': abn_5d,
                'abn_10d': abn_10d,
            })
    
    print(f"Valid results: {len(results)}")
    
    if len(results) < 20:
        print("ERROR: Too few valid results.")
        return
    
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'])
    
    # Step 3: Discovery set analysis
    disc = df[df['date'] < DISCOVERY_END].copy()
    oos = df[df['date'] >= OOS_START].copy()
    
    print(f"\nDiscovery set: {len(disc)} events")
    print(f"OOS set: {len(oos)} events")
    
    def analyze(data, label):
        if len(data) < 5:
            print(f"\n{label}: Too few events ({len(data)})")
            return None
        
        a5 = data['abn_5d'].dropna()
        avg5 = a5.mean()
        dir5 = (a5 > 0.5).mean() * 100
        t5, p5 = stats.ttest_1samp(a5, 0)
        
        a10 = data['abn_10d'].dropna()
        avg10 = a10.mean()
        dir10 = (a10 > 0.5).mean() * 100
        t10, p10 = stats.ttest_1samp(a10, 0)
        
        print(f"\n{label} (n={len(data)}):")
        print(f"  5d:  avg={avg5:+.2f}%, dir={dir5:.0f}%, p={p5:.4f}")
        print(f"  10d: avg={avg10:+.2f}%, dir={dir10:.0f}%, p={p10:.4f}")
        
        # Multiple testing: 2+ horizons at p<0.05 or 1 at p<0.01
        passes_mt = (p5 < 0.05 and p10 < 0.05) or (p5 < 0.01) or (p10 < 0.01)
        print(f"  Multiple testing: {'PASS' if passes_mt else 'FAIL'}")
        
        # Direction threshold (>50% must exceed 0.5%)
        dir_pass = dir5 > 50 and dir10 > 50
        print(f"  Direction (>50% exceed 0.5%): {'PASS' if dir_pass else 'FAIL'}")
        
        return {'avg5': avg5, 'avg10': avg10, 'dir5': dir5, 'dir10': dir10,
                'p5': p5, 'p10': p10, 'passes_mt': passes_mt, 'n': len(data)}
    
    disc_res = analyze(disc, "DISCOVERY (2020-2023)")
    oos_res = analyze(oos, "OOS VALIDATION (2024-2025)")
    
    # Surprise bucket analysis
    print("\n--- By EPS Surprise Bucket ---")
    for lo, hi, label in [(10, 20, '10-20%'), (20, 50, '20-50%'), (50, 200, '50-200%')]:
        bucket = df[(df['surprise_pct'] >= lo) & (df['surprise_pct'] < hi)]
        if len(bucket) > 5:
            a5 = bucket['abn_5d'].dropna()
            print(f"  {label}: n={len(bucket)}, avg5d={a5.mean():+.2f}%, dir={((a5>0.5).mean()*100):.0f}%")
    
    # Best/worst events
    print("\n--- Top 5 Best Events (5d abnormal) ---")
    top = df.nlargest(5, 'abn_5d')[['symbol','date','surprise_pct','abn_5d','abn_10d']]
    for _, row in top.iterrows():
        print(f"  {row['symbol']} {str(row['date'])[:10]}: surprise={row['surprise_pct']:.1f}%, "
              f"5d={row['abn_5d']:+.2f}%, 10d={row.get('abn_10d', float('nan')):+.2f}%")
    
    print("\n--- Top 5 Worst Events (5d abnormal) ---")
    bot = df.nsmallest(5, 'abn_5d')[['symbol','date','surprise_pct','abn_5d','abn_10d']]
    for _, row in bot.iterrows():
        print(f"  {row['symbol']} {str(row['date'])[:10]}: surprise={row['surprise_pct']:.1f}%, "
              f"5d={row['abn_5d']:+.2f}%, 10d={row.get('abn_10d', float('nan')):+.2f}%")
    
    # Final verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)
    
    if disc_res:
        valid = disc_res['passes_mt'] and disc_res['avg5'] > 1.0
        print(f"Discovery: {'VALID' if valid else 'INVALID'}")
        print(f"  avg5d={disc_res['avg5']:+.2f}%, avg10d={disc_res['avg10']:+.2f}%")
        print(f"  p5={disc_res['p5']:.4f}, p10={disc_res['p10']:.4f}, MT={'PASS' if disc_res['passes_mt'] else 'FAIL'}")
    
    if oos_res:
        oos_valid = oos_res['avg5'] > 0.5 and oos_res['dir5'] > 50
        print(f"OOS Validation: {'CONFIRMS' if oos_valid else 'REFUTES'}")
        print(f"  avg5d={oos_res['avg5']:+.2f}%, avg10d={oos_res['avg10']:+.2f}%")
    
    # Save results
    results_dict = {
        'generated': datetime.now().isoformat(),
        'n_total': len(df),
        'n_discovery': len(disc),
        'n_oos': len(oos),
        'discovery': disc_res,
        'oos': oos_res,
        'all_events': results,
    }
    out_path = Path('tools/pead_backtest_results.json')
    with open(out_path, 'w') as f:
        json.dump(results_dict, f, indent=2, default=str)
    print(f"\nFull results saved to {out_path}")


if __name__ == '__main__':
    run_pead_backtest()
