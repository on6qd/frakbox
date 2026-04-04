#!/usr/bin/env python3
"""
Insider Cluster Screening Pipeline

Automates the full evaluation of EDGAR insider clusters:
1. Scan EDGAR for insider buying clusters (3+ insiders, $50K+ each)
2. Filter to large-cap ($500M+ market cap)
3. Check for CEO/CFO involvement (strongest signal per OOS validation)
4. Check VIX regime (validated: VIX<20 = +3.04%, 20-25 = +3.24%, 25-30 = +6.79%)
5. Check if cluster is already known in knowledge base
6. Check entry window (within 5 trading days)
7. Output qualified candidates with recommended action

Usage:
    python3 tools/screen_insider_clusters.py [--days 14] [--output json|table]
"""

import os
import sys
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
from tools.edgar_insider_scanner_v2 import scan_insider_clusters
from tools.largecap_filter import filter_to_largecap


def get_vix_level():
    """Get current VIX level (works on weekends too)."""
    try:
        from tools.yfinance_utils import safe_download
        end = datetime.now()
        start = end - timedelta(days=10)
        vix_data = safe_download('^VIX', start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        if vix_data is not None and len(vix_data) > 0:
            return float(vix_data['Close'].squeeze().iloc[-1])
    except Exception:
        pass
    return None


def get_spy_vs_ma20():
    """Check if SPY is above 20d MA (works on weekends too)."""
    try:
        from tools.yfinance_utils import safe_download
        end = datetime.now()
        start = end - timedelta(days=40)
        spy = safe_download('SPY', start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        if spy is not None and len(spy) >= 20:
            close = spy['Close'].squeeze()
            ma20 = float(close.rolling(20).mean().iloc[-1])
            last = float(close.iloc[-1])
            return last > ma20, last, ma20
    except Exception:
        pass
    return None, None, None


def check_known_cluster(ticker):
    """Check if this cluster is already tracked in knowledge base."""
    db.init_db()
    # Try common naming patterns
    patterns = [
        f'{ticker.lower()}_insider_cluster',
        f'{ticker.lower()}_insider_cluster_march2026',
        f'{ticker.lower()}_insider_cluster_april2026',
        f'{ticker.lower()}_insider_cluster_may2026',
        f'{ticker.lower()}_ceo_cluster_march2026',
        f'{ticker.lower()}_ceo_cluster_april2026',
        f'{ticker.lower()}_cfo_cluster_march2026',
    ]
    for key in patterns:
        effect = db.get_known_effect(key)
        if effect:
            return key, effect
    return None, None


def check_csuite_involvement(insiders):
    """Check if any insider is CEO, CFO, President, or COO."""
    csuite_titles = {'CEO', 'CFO', 'COO', 'PRESIDENT', 'CHIEF EXECUTIVE',
                     'CHIEF FINANCIAL', 'CHIEF OPERATING', 'PRES', 'TREASURER'}
    csuite = []
    for ins in insiders:
        title = (ins.get('title', '') or '').upper()
        if any(t in title for t in csuite_titles):
            csuite.append(ins)
    return csuite


def check_entry_window(insiders, max_days=7):
    """Check if the most recent insider purchase is within entry window."""
    today = datetime.now().date()
    most_recent = None
    for ins in insiders:
        for d in ins.get('dates', []):
            try:
                filing_date = datetime.strptime(d, '%Y-%m-%d').date()
                if most_recent is None or filing_date > most_recent:
                    most_recent = filing_date
            except (ValueError, TypeError):
                continue
    if most_recent:
        days_ago = (today - most_recent).days
        return days_ago <= max_days, days_ago, most_recent
    return False, None, None


def screen_clusters(days=14, output='table'):
    """Full screening pipeline."""
    db.init_db()

    print(f"{'='*60}")
    print(f"INSIDER CLUSTER SCREENING — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    # Step 1: Get VIX and market regime
    vix = get_vix_level()
    spy_above_ma, spy_close, spy_ma20 = get_spy_vs_ma20()
    print(f"\nMarket Regime:")
    if vix:
        tier = "OPTIMAL" if vix < 20 else "OK" if vix < 25 else "ELEVATED" if vix < 30 else "HIGH"
        print(f"  VIX: {vix:.1f} ({tier})")
    if spy_close and spy_ma20:
        status = "ABOVE" if spy_above_ma else "BELOW"
        print(f"  SPY: ${spy_close:.2f} vs 20d MA ${spy_ma20:.2f} ({status})")

    if vix and vix >= 40:
        print(f"\n⚠ VIX >= 40 — REGIME BLOCKED. No insider cluster trades in extreme volatility.")
        return []

    # Step 2: Scan EDGAR
    print(f"\nScanning EDGAR (last {days} days)...")
    try:
        clusters = scan_insider_clusters(days=days, min_insiders=3, min_value_per_insider=50000)
    except Exception as e:
        print(f"ERROR scanning EDGAR: {e}")
        return []
    print(f"  Found {len(clusters)} raw clusters")

    if not clusters:
        print("\nNo clusters found.")
        return []

    # Step 3: Extract tickers and filter
    ticker_map = {}
    for c in clusters:
        ticker = c.get('ticker', '')
        # Clean up exchange prefix
        if ':' in ticker:
            ticker = ticker.split(':')[1]
        if ticker and ticker not in ('N/A', 'NONE', ''):
            ticker_map[ticker] = c

    if not ticker_map:
        print("\nNo valid tickers in clusters.")
        return []

    # Step 4: Large-cap filter
    tickers = list(ticker_map.keys())
    print(f"\nFiltering {len(tickers)} tickers for large-cap ($500M+)...")
    try:
        largecap = filter_to_largecap(tickers)
        print(f"  Large-cap: {largecap}")
    except Exception as e:
        print(f"  Warning: largecap filter failed: {e}")
        largecap = tickers  # fallback: keep all

    # Step 5: Evaluate each qualifying cluster
    candidates = []
    for ticker in largecap:
        cluster = ticker_map[ticker]
        insiders = cluster.get('insiders', [])

        # Check if already known
        known_key, known_data = check_known_cluster(ticker)

        # Check C-suite
        csuite = check_csuite_involvement(insiders)

        # Check entry window
        in_window, days_ago, most_recent = check_entry_window(insiders)

        # Build evaluation
        eval_result = {
            'ticker': ticker,
            'company': cluster.get('issuer_name', '?'),
            'n_insiders': cluster.get('n_insiders', 0),
            'total_value': cluster.get('total_value', 0),
            'csuite': [f"{i['name']} ({i['title']})" for i in csuite],
            'has_csuite': len(csuite) > 0,
            'in_window': in_window,
            'days_ago': days_ago,
            'most_recent_filing': str(most_recent) if most_recent else '?',
            'already_known': known_key is not None,
            'known_key': known_key,
        }

        # Score the candidate
        score = 0
        reasons = []

        if eval_result['has_csuite']:
            score += 3
            reasons.append(f"CEO/CFO buying ({len(csuite)} C-suite)")
        if eval_result['n_insiders'] >= 5:
            score += 2
            reasons.append(f"{eval_result['n_insiders']} insiders (large cluster)")
        elif eval_result['n_insiders'] >= 3:
            score += 1
            reasons.append(f"{eval_result['n_insiders']} insiders")
        if eval_result['total_value'] >= 5_000_000:
            score += 2
            reasons.append(f"${eval_result['total_value']/1e6:.1f}M total value")
        elif eval_result['total_value'] >= 1_000_000:
            score += 1
            reasons.append(f"${eval_result['total_value']/1e6:.1f}M total value")
        if eval_result['in_window']:
            score += 2
            reasons.append(f"Entry window open ({days_ago}d ago)")
        if vix and vix < 20:
            score += 1
            reasons.append("VIX < 20 (optimal)")

        # Penalties
        if eval_result['already_known']:
            score -= 5
            reasons.append(f"ALREADY TRACKED ({known_key})")
        if not eval_result['in_window'] and days_ago:
            reasons.append(f"⚠ Entry window CLOSED ({days_ago}d ago)")
        if not eval_result['has_csuite']:
            reasons.append("⚠ No CEO/CFO (weaker signal)")

        eval_result['score'] = score
        eval_result['reasons'] = reasons

        # Determine action
        if eval_result['already_known']:
            eval_result['action'] = 'SKIP (already tracked)'
        elif not eval_result['in_window']:
            eval_result['action'] = 'SKIP (entry window closed)'
        elif score >= 5:
            eval_result['action'] = '★ CREATE HYPOTHESIS'
        elif score >= 3:
            eval_result['action'] = 'CONSIDER (moderate signal)'
        else:
            eval_result['action'] = 'WEAK (monitor only)'

        candidates.append(eval_result)

    # Sort by score descending
    candidates.sort(key=lambda x: x['score'], reverse=True)

    # Output
    print(f"\n{'='*60}")
    print(f"SCREENING RESULTS ({len(candidates)} large-cap clusters)")
    print(f"{'='*60}")

    for c in candidates:
        print(f"\n{'─'*50}")
        print(f"  {c['ticker']} — {c['company']}")
        print(f"  Insiders: {c['n_insiders']} | Value: ${c['total_value']:,.0f} | Score: {c['score']}")
        print(f"  C-suite: {', '.join(c['csuite']) if c['csuite'] else 'None'}")
        print(f"  Most recent filing: {c['most_recent_filing']} ({c['days_ago']}d ago)")
        print(f"  >>> ACTION: {c['action']}")
        for r in c['reasons']:
            print(f"      • {r}")

    # Summary
    actionable = [c for c in candidates if c['action'].startswith('★')]
    if actionable:
        print(f"\n{'='*60}")
        print(f"★ {len(actionable)} ACTIONABLE CLUSTER(S) FOUND")
        for c in actionable:
            print(f"  → {c['ticker']}: {c['n_insiders']} insiders, ${c['total_value']:,.0f}, score={c['score']}")
        print(f"{'='*60}")
    else:
        print(f"\nNo new actionable clusters. Next scan recommended in 24h.")

    if output == 'json':
        print(json.dumps(candidates, indent=2, default=str))

    return candidates


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Screen insider buying clusters')
    parser.add_argument('--days', type=int, default=14, help='Lookback days (default: 14)')
    parser.add_argument('--output', choices=['table', 'json'], default='table')
    args = parser.parse_args()

    screen_clusters(days=args.days, output=args.output)
