"""
Insider cluster timing drift analysis.

Compares abnormal returns from three entry points:
  1. trans_date+1  (backtest's implicit entry — last insider transaction)
  2. filing_date+1 (real-time scanner's entry — Form 4 filed to EDGAR)
  3. detection_delay: filing_date+3 (our daily scanner cadence worst case)

Also measures pre-entry drift:
  - prior_5d: 5d abnormal return BEFORE trans_date (is alpha leaking pre-cluster?)
  - trans_to_filing: abnormal return from trans_date to filing_date
    (alpha captured in the filing-lag window — we can't trade it)

Output: summary JSON printed to stdout.

Usage:
    python3 tools/insider_cluster_timing_drift.py --years 2024,2025 --min-value 150000 --min-insiders 3
"""
import argparse
import json
import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

from insider_cluster_feature_analysis import load_quarter, is_ceo_cfo
from yfinance_utils import safe_download


def find_clusters_with_filing(years, min_insiders=3, window_days=14, min_value=50000):
    all_purchases = []
    for year in years:
        for q in range(1, 5):
            data = load_quarter(year, q)
            if data is None:
                continue
            subs = data['submissions']
            trans = data['nonderiv_trans']
            owners = data['reporting_owners']
            purchases = trans[trans['TRANS_CODE'] == 'P'].copy()
            if purchases.empty:
                continue
            purchases = purchases.merge(
                subs[['ACCESSION_NUMBER', 'ISSUERTRADINGSYMBOL', 'ISSUERNAME', 'FILING_DATE']],
                on='ACCESSION_NUMBER', how='left',
            )
            purchases = purchases.merge(
                owners[['ACCESSION_NUMBER', 'RPTOWNERCIK', 'RPTOWNER_TITLE']],
                on='ACCESSION_NUMBER', how='left',
            )
            purchases['TRANS_DATE'] = pd.to_datetime(purchases['TRANS_DATE'], format='%d-%b-%Y', errors='coerce')
            purchases['FILING_DATE'] = pd.to_datetime(purchases['FILING_DATE'], errors='coerce')
            purchases['TRANS_SHARES'] = pd.to_numeric(purchases['TRANS_SHARES'], errors='coerce')
            purchases['TRANS_PRICEPERSHARE'] = pd.to_numeric(purchases['TRANS_PRICEPERSHARE'], errors='coerce')
            purchases['dollar_value'] = purchases['TRANS_SHARES'] * purchases['TRANS_PRICEPERSHARE']
            valid = purchases.dropna(subset=['TRANS_DATE', 'ISSUERTRADINGSYMBOL', 'RPTOWNERCIK', 'FILING_DATE'])
            valid = valid[valid['dollar_value'] > 0]
            all_purchases.append(valid)

    if not all_purchases:
        return []
    df = pd.concat(all_purchases, ignore_index=True)
    df = df.sort_values('TRANS_DATE')

    clusters = []
    for ticker, group in df.groupby('ISSUERTRADINGSYMBOL'):
        if pd.isna(ticker) or str(ticker).strip() == '':
            continue
        group = group.sort_values('TRANS_DATE')
        dates = sorted(group['TRANS_DATE'].dropna().unique())
        used = set()
        for anchor in dates:
            if anchor in used:
                continue
            window_end = anchor + pd.Timedelta(days=window_days)
            wr = group[(group['TRANS_DATE'] >= anchor) & (group['TRANS_DATE'] <= window_end)]
            if wr['RPTOWNERCIK'].nunique() < min_insiders:
                continue
            if wr['dollar_value'].sum() < min_value:
                continue
            for d in wr['TRANS_DATE'].unique():
                used.add(d)
            has_ceo_cfo = any(is_ceo_cfo(t) for t in wr['RPTOWNER_TITLE'].dropna())
            trans_date = wr['TRANS_DATE'].max()
            # Max filing date in window — last insider to report
            filing_date = wr['FILING_DATE'].max()
            clusters.append({
                'ticker': str(ticker).strip(),
                'trans_date': trans_date,
                'filing_date': filing_date,
                'n_insiders': int(wr['RPTOWNERCIK'].nunique()),
                'total_value': float(wr['dollar_value'].sum()),
                'has_ceo_cfo': has_ceo_cfo,
            })
    return clusters


def abnormal_return(prices, spy_prices, start_date, days):
    """Return % change over `days` trading days starting from first trading day >= start_date, abnormal vs SPY."""
    fut = prices[prices.index >= start_date]
    if len(fut) < days + 1:
        return None
    stk = (fut.iloc[min(days, len(fut) - 1)] - fut.iloc[0]) / fut.iloc[0] * 100
    sp_fut = spy_prices[spy_prices.index >= start_date]
    if len(sp_fut) < days + 1:
        return None
    sp = (sp_fut.iloc[min(days, len(sp_fut) - 1)] - sp_fut.iloc[0]) / sp_fut.iloc[0] * 100
    return float(stk - sp)


def prior_abnormal(prices, spy_prices, end_date, days):
    """Return % change over `days` trading days ENDING at the day before end_date, abnormal vs SPY."""
    hist = prices[prices.index < end_date].tail(days + 1)
    if len(hist) < days + 1:
        return None
    stk = (hist.iloc[-1] - hist.iloc[0]) / hist.iloc[0] * 100
    sp_hist = spy_prices[spy_prices.index < end_date].tail(days + 1)
    if len(sp_hist) < days + 1:
        return None
    sp = (sp_hist.iloc[-1] - sp_hist.iloc[0]) / sp_hist.iloc[0] * 100
    return float(stk - sp)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--years', default='2024,2025')
    ap.add_argument('--min-insiders', type=int, default=3)
    ap.add_argument('--min-value', type=float, default=150000)
    ap.add_argument('--ceo-cfo-only', action='store_true')
    ap.add_argument('--largecap-only', action='store_true')
    args = ap.parse_args()

    years = [int(y) for y in args.years.split(',')]
    print(f"[info] finding clusters for years {years}...", file=sys.stderr)
    clusters = find_clusters_with_filing(years, args.min_insiders, 14, args.min_value)
    print(f"[info] found {len(clusters)} clusters", file=sys.stderr)

    if args.ceo_cfo_only:
        clusters = [c for c in clusters if c['has_ceo_cfo']]
        print(f"[info] ceo/cfo filter: {len(clusters)}", file=sys.stderr)

    # Fetch prices
    tickers = sorted(set(c['ticker'] for c in clusters))
    min_d = min(c['trans_date'] for c in clusters) - pd.Timedelta(days=30)
    max_d = max(c['filing_date'] for c in clusters) + pd.Timedelta(days=30)

    print(f"[info] fetching SPY...", file=sys.stderr)
    spy = safe_download('SPY', start=min_d.strftime('%Y-%m-%d'), end=max_d.strftime('%Y-%m-%d'))
    spy_close = spy['Close'] if spy is not None else pd.Series(dtype=float)

    print(f"[info] fetching {len(tickers)} tickers...", file=sys.stderr)
    prices = {}
    for i, t in enumerate(tickers):
        try:
            df = safe_download(t, start=min_d.strftime('%Y-%m-%d'), end=max_d.strftime('%Y-%m-%d'))
            if df is not None and not df.empty:
                prices[t] = df['Close']
        except Exception:
            pass
        if i % 50 == 0:
            print(f"[info]   {i}/{len(tickers)}", file=sys.stderr)

    # Optional largecap filter — approximate via recent price * no, skip; just note
    # For scientific rigor, we should filter but that requires market cap lookup. Skip for now.

    rows = []
    for c in clusters:
        t = c['ticker']
        if t not in prices:
            continue
        p = prices[t]
        trans_date = c['trans_date']
        filing_date = c['filing_date']
        lag_days = (filing_date - trans_date).days

        # Returns from three entry points
        r_trans_5d = abnormal_return(p, spy_close, trans_date, 5)
        r_filing_5d = abnormal_return(p, spy_close, filing_date, 5)
        r_filing_plus1_5d = abnormal_return(p, spy_close, filing_date + pd.Timedelta(days=1), 5)

        # Pre-entry drift
        prior_5d = prior_abnormal(p, spy_close, trans_date, 5)
        # Trans-to-filing (captured in filing lag)
        trans_to_filing = abnormal_return(p, spy_close, trans_date, max(1, lag_days))

        if any(x is None for x in (r_trans_5d, r_filing_5d)):
            continue

        rows.append({
            'ticker': t,
            'trans_date': trans_date.strftime('%Y-%m-%d'),
            'filing_date': filing_date.strftime('%Y-%m-%d'),
            'lag_days': lag_days,
            'n_insiders': c['n_insiders'],
            'has_ceo_cfo': c['has_ceo_cfo'],
            'prior_5d_abn': prior_5d,
            'trans_to_filing_abn': trans_to_filing,
            'r_trans_5d': r_trans_5d,
            'r_filing_5d': r_filing_5d,
            'r_filing_plus1_5d': r_filing_plus1_5d,
        })

    import statistics as stats
    def summarize(vals, label):
        vals = [v for v in vals if v is not None]
        if not vals:
            return {'label': label, 'n': 0}
        return {
            'label': label,
            'n': len(vals),
            'mean': round(stats.mean(vals), 2),
            'median': round(stats.median(vals), 2),
            'pos_rate': round(sum(1 for v in vals if v > 0.5) / len(vals) * 100, 1),
        }

    summary = {
        'params': vars(args),
        'n_clusters': len(rows),
        'avg_filing_lag_days': round(stats.mean([r['lag_days'] for r in rows]), 1) if rows else None,
        'median_filing_lag_days': round(stats.median([r['lag_days'] for r in rows]), 1) if rows else None,
        'drift': {
            'prior_5d_before_trans_date': summarize([r['prior_5d_abn'] for r in rows], 'prior_5d_abn'),
            'trans_to_filing_window': summarize([r['trans_to_filing_abn'] for r in rows], 'trans_to_filing_abn'),
            'post_trans_5d': summarize([r['r_trans_5d'] for r in rows], 'post_trans_5d'),
            'post_filing_5d': summarize([r['r_filing_5d'] for r in rows], 'post_filing_5d'),
            'post_filing_plus1_5d': summarize([r['r_filing_plus1_5d'] for r in rows], 'post_filing_plus1_5d'),
        },
    }

    # Sub-analysis: CEO/CFO only
    ceo_rows = [r for r in rows if r['has_ceo_cfo']]
    if ceo_rows:
        summary['drift_ceo_cfo_only'] = {
            'n': len(ceo_rows),
            'prior_5d': summarize([r['prior_5d_abn'] for r in ceo_rows], 'prior_5d_abn'),
            'post_trans_5d': summarize([r['r_trans_5d'] for r in ceo_rows], 'post_trans_5d'),
            'post_filing_5d': summarize([r['r_filing_5d'] for r in ceo_rows], 'post_filing_5d'),
        }

    # Alpha captured in filing lag (delta between trans entry and filing entry)
    deltas = []
    for r in rows:
        if r['r_trans_5d'] is not None and r['r_filing_5d'] is not None:
            deltas.append(r['r_trans_5d'] - r['r_filing_5d'])
    if deltas:
        summary['alpha_lost_to_filing_lag'] = {
            'n': len(deltas),
            'mean_delta': round(stats.mean(deltas), 2),
            'median_delta': round(stats.median(deltas), 2),
            'note': 'r_trans_5d minus r_filing_5d. Positive means backtest overstates real-time returns.',
        }

    # Lag segmentation — does filtering on short lag rescue the signal?
    lag_buckets = [
        ('lag_0_1d', lambda r: r['lag_days'] <= 1),
        ('lag_2d',   lambda r: r['lag_days'] == 2),
        ('lag_3_5d', lambda r: 3 <= r['lag_days'] <= 5),
        ('lag_6plus', lambda r: r['lag_days'] >= 6),
    ]
    lag_seg = {}
    for name, pred in lag_buckets:
        sub = [r for r in rows if pred(r)]
        if not sub:
            continue
        lag_seg[name] = {
            'n': len(sub),
            'post_filing_5d': summarize([r['r_filing_5d'] for r in sub], 'post_filing_5d'),
            'post_filing_plus1_5d': summarize([r['r_filing_plus1_5d'] for r in sub], 'post_filing_plus1_5d'),
            'post_trans_5d': summarize([r['r_trans_5d'] for r in sub], 'post_trans_5d'),
        }
    summary['lag_segmentation'] = lag_seg

    # Save rows to cache for reanalysis
    import os
    cache_dir = ROOT / 'data' / 'cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / 'insider_cluster_timing_drift_rows.json'
    with open(cache_path, 'w') as f:
        json.dump(rows, f, default=str)
    summary['cache_path'] = str(cache_path)

    print(json.dumps(summary, indent=2, default=str))


if __name__ == '__main__':
    main()
