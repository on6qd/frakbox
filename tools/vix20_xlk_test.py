"""
Test scan hit: VIX>20 crossings trigger XLK mean-reversion (+1.43% 10d, n=97).

Methodology:
- Identify first-close-above-20 VIX events (cluster boundary: must be <20 for prior 5 days)
- For each crossing, compute 10-day abnormal return (XLK - SPY)
- Split IS/OOS at 2020-01-01 (pre/post COVID regime)
- Also test VIX>25 and VIX>30 on XLK for comparison
- Also test VIX>20 on SPY directly (baseline)

Report: n, mean abnormal, median, direction_rate, p-value for each regime.
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import safe_download

def find_crossings(vix_series, threshold, buffer_days=5):
    """Find first-close-above-threshold events. Requires prior buffer_days of closes below."""
    above = vix_series > threshold
    crossings = []
    for i in range(buffer_days, len(vix_series)):
        if above.iloc[i] and not above.iloc[i-buffer_days:i].any():
            crossings.append(vix_series.index[i])
    return crossings

def compute_abnormal(target_series, bench_series, entry_date, hold_days=10):
    """Compute abnormal return entering at close on entry_date, exit at close +hold_days."""
    try:
        t_idx = target_series.index.get_loc(entry_date)
        if t_idx + hold_days >= len(target_series):
            return None
        t_ret = (target_series.iloc[t_idx + hold_days] / target_series.iloc[t_idx] - 1) * 100
        b_ret = (bench_series.iloc[t_idx + hold_days] / bench_series.iloc[t_idx] - 1) * 100
        return t_ret - b_ret
    except (KeyError, IndexError):
        return None

def test_threshold(vix, target, bench, threshold, hold_days=10, target_name='XLK'):
    crossings = find_crossings(vix, threshold)
    results = []
    for dt in crossings:
        abn = compute_abnormal(target, bench, dt, hold_days)
        if abn is not None:
            results.append({'date': dt, 'abnormal_pct': abn})
    df = pd.DataFrame(results)
    if len(df) == 0:
        return None
    df['is_oos'] = pd.to_datetime(df['date']) >= pd.Timestamp('2020-01-01')
    return df

def summarize(df, label):
    if df is None or len(df) == 0:
        print(f'{label}: no data')
        return
    abn = df['abnormal_pct']
    direction = (abn > 0).mean()
    t, p = stats.ttest_1samp(abn, 0) if len(abn) > 1 else (0, 1)
    print(f'{label}: n={len(df)} mean={abn.mean():+.2f}% med={abn.median():+.2f}% dir={direction:.0%} p={p:.4f}')

def main():
    print('Downloading VIX, SPY, QQQ, XLK, XLF, XLI (2005-2026)...')
    vix = safe_download('^VIX', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    spy = safe_download('SPY', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    xlk = safe_download('XLK', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    qqq = safe_download('QQQ', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    xlf = safe_download('XLF', start='2005-01-01', end='2026-04-16')['Close'].squeeze()
    xli = safe_download('XLI', start='2005-01-01', end='2026-04-16')['Close'].squeeze()

    # Align to VIX index
    common = vix.index.intersection(spy.index).intersection(xlk.index)
    vix = vix.loc[common]
    spy = spy.loc[common]
    xlk = xlk.loc[common]
    qqq = qqq.loc[common.intersection(qqq.index)]
    xlf = xlf.loc[common.intersection(xlf.index)]
    xli = xli.loc[common.intersection(xli.index)]

    print(f'\n=== VIX>20 (first cluster close after 5d below) ===')
    for tgt_name, tgt in [('XLK', xlk), ('QQQ', qqq), ('SPY', spy), ('XLF', xlf), ('XLI', xli)]:
        df = test_threshold(vix, tgt, spy, 20, 10, tgt_name)
        if df is None: continue
        print(f'\n--- {tgt_name} vs SPY, 10d abnormal ---')
        summarize(df, f'{tgt_name} overall')
        summarize(df[~df['is_oos']], f'{tgt_name} IS (pre-2020)')
        summarize(df[df['is_oos']], f'{tgt_name} OOS (2020+)')

    print(f'\n=== VIX>25 (comparison threshold) ===')
    for tgt_name, tgt in [('XLK', xlk), ('SPY', spy)]:
        df = test_threshold(vix, tgt, spy, 25, 10, tgt_name)
        if df is None: continue
        summarize(df, f'{tgt_name} overall')
        summarize(df[~df['is_oos']], f'{tgt_name} IS')
        summarize(df[df['is_oos']], f'{tgt_name} OOS')

    print(f'\n=== VIX>30 (baseline: our existing SPY signal) ===')
    for tgt_name, tgt in [('XLK', xlk), ('SPY', spy)]:
        df = test_threshold(vix, tgt, spy, 30, 10, tgt_name)
        if df is None: continue
        summarize(df, f'{tgt_name} overall')
        summarize(df[~df['is_oos']], f'{tgt_name} IS')
        summarize(df[df['is_oos']], f'{tgt_name} OOS')

if __name__ == '__main__':
    main()
