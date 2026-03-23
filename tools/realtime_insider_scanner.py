# DEPRECATED: This scanner times out repeatedly due to SEC EDGAR API changes.
# Use tools/openinsider_scraper.py instead for fresh insider cluster signals.
# See friction_log.jsonl for details (3+ timeout occurrences logged).
import warnings
warnings.warn(
    "realtime_insider_scanner.py is deprecated. Use openinsider_scraper.py instead.",
    DeprecationWarning,
    stacklevel=1
)

"""
Real-Time Insider Cluster Scanner

DEPRECATED — USE openinsider_scraper.py INSTEAD.

This tool parses EDGAR XML for Form 4 filings but requires ~6 minutes of
sequential HTTP requests (500 CIKs × 2 requests × 0.35s each) and consistently
times out in automated sessions. It has been superseded by:

    python tools/openinsider_scraper.py

openinsider_scraper.py finds the same clusters (and more) in under 5 seconds
by querying OpenInsider's pre-processed cluster screener.

This file is retained for reference only.

---

Original description:
Uses EDGAR quarterly full-index (form.idx) to find recent Form 4 filings,
then parses XML to identify open-market purchase clusters (2+ or 3+ insiders
each buying >$50K within 30 days of each other).

Usage (deprecated):
    python tools/realtime_insider_scanner.py --min-insiders 3 --days 30 --min-value 50000

Data source: https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/form.idx
"""

import os
import re
import sys
import time
import json
import pickle
import requests
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

USER_AGENT = os.environ.get("SEC_USER_AGENT", "Financial Research Bot contact@example.com")
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "realtime_cache")
BASE_ARCHIVE = "https://www.sec.gov/Archives/edgar"

# SEC EDGAR rate limit: ~10 requests/second is safe; we'll do ~3/second with retry
SEC_REQUEST_DELAY = 0.3    # seconds between requests (reduced from 0.35)
SEC_MAX_RETRIES = 3
SEC_RETRY_BACKOFF = 2.0    # exponential backoff multiplier


def sec_get(url, timeout=15):
    """GET request to SEC EDGAR with rate limiting and retry."""
    headers = {"User-Agent": USER_AGENT}
    delay = SEC_REQUEST_DELAY
    for attempt in range(SEC_MAX_RETRIES):
        try:
            time.sleep(delay)
            resp = requests.get(url, headers=headers, timeout=timeout)
            return resp
        except requests.exceptions.Timeout:
            if attempt < SEC_MAX_RETRIES - 1:
                delay *= SEC_RETRY_BACKOFF
                continue
            return None
        except requests.exceptions.RequestException:
            if attempt < SEC_MAX_RETRIES - 1:
                delay *= SEC_RETRY_BACKOFF
                continue
            return None
    return None


def get_current_quarter():
    """Return (year, quarter) for today's date."""
    now = datetime.now()
    q = (now.month - 1) // 3 + 1
    return now.year, q


def download_form_idx(year, quarter, cache=True, quiet=False):
    """Download and parse the EDGAR form.idx for a given quarter.

    Returns list of dicts: {date, cik, path, form_type}
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"form_idx_{year}q{quarter}.pkl")

    if cache and os.path.exists(cache_path):
        # Use cache if less than 24 hours old
        if time.time() - os.path.getmtime(cache_path) < 86400:
            with open(cache_path, "rb") as f:
                return pickle.load(f)

    url = f"{BASE_ARCHIVE}/full-index/{year}/QTR{quarter}/form.idx"
    if not quiet:
        print(f"Downloading EDGAR form.idx {year}Q{quarter}...")
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=120)  # Large file — long timeout, no retry

    if resp is None or resp.status_code == 404:
        if not quiet:
            print(f"  {year}Q{quarter} form.idx not found (404)")
        return []

    resp.raise_for_status()

    entries = []
    for line in resp.text.split('\n'):
        if not line.strip() or '---' in line or 'Form Type' in line:
            continue
        parts = line.split()
        if not parts or parts[0] not in ['4', '4/A']:
            continue

        date_match = re.search(r'\b(\d{4}-\d{2}-\d{2})\b', line)
        path_match = re.search(r'(edgar/data/(\d+)/\S+)', line)

        if date_match and path_match:
            entries.append({
                'date': date_match.group(1),
                'cik': path_match.group(2),
                'path': path_match.group(1),
                'form_type': parts[0],
            })

    if not quiet:
        print(f"  Parsed {len(entries)} Form 4 entries")

    with open(cache_path, "wb") as f:
        pickle.dump(entries, f)

    return entries


def parse_form4_xml(cik, path):
    """Fetch and parse a Form 4 XML filing.

    Returns dict with transaction info if it's an open market purchase by an officer/director,
    else returns None.
    """
    # Get filing index page
    idx_url = f"{BASE_ARCHIVE}/{path}"
    resp = sec_get(idx_url)
    if resp is None or resp.status_code != 200:
        return None

    # Find XML file in index
    xml_match = re.search(r'href="(/Archives/edgar/[^"]+\.xml)"', resp.text, re.IGNORECASE)
    if not xml_match:
        xml_match = re.search(r'href="([^"]+\.xml)"', resp.text, re.IGNORECASE)
        if not xml_match:
            return None

    xml_href = xml_match.group(1)
    if xml_href.startswith('/'):
        xml_url = f"https://www.sec.gov{xml_href}"
    else:
        base_path = '/'.join(path.split('/')[:4])
        xml_url = f"{BASE_ARCHIVE}/{base_path}/{xml_href}"

    xml_resp = sec_get(xml_url)
    if xml_resp is None or xml_resp.status_code != 200:
        return None

    xml = xml_resp.text

    # Check for open market purchase (P transaction code)
    if '<transactionCode>P</transactionCode>' not in xml:
        return None

    # Check if filer is officer or director
    is_officer = '<isOfficer>1</isOfficer>' in xml
    is_director = '<isDirector>1</isDirector>' in xml

    if not (is_officer or is_director):
        return None

    # Extract transaction details
    shares_match = re.search(
        r'<transactionShares>.*?<value>([\d.]+)</value>', xml, re.DOTALL)
    price_match = re.search(
        r'<transactionPricePerShare>.*?<value>([\d.]+)</value>', xml, re.DOTALL)
    reporter_match = re.search(r'<rptOwnerName>(.*?)</rptOwnerName>', xml)
    company_match = re.search(r'<issuerName>(.*?)</issuerName>', xml)
    ticker_match = re.search(r'<issuerTradingSymbol>(.*?)</issuerTradingSymbol>', xml)
    date_match = re.search(
        r'<transactionDate>.*?<value>(\d{4}-\d{2}-\d{2})</value>', xml, re.DOTALL)
    role_title_match = re.search(r'<officerTitle>(.*?)</officerTitle>', xml)
    is_10b5_match = re.search(r'<rule10b5One>(1|0)</rule10b5One>', xml)

    if not (shares_match and price_match):
        return None

    shares = float(shares_match.group(1))
    price = float(price_match.group(1))
    value = shares * price
    is_10b5 = is_10b5_match and is_10b5_match.group(1) == '1'

    return {
        'reporter': reporter_match.group(1) if reporter_match else 'unknown',
        'company': company_match.group(1) if company_match else 'unknown',
        'ticker': ticker_match.group(1) if ticker_match else 'unknown',
        'cik': cik,
        'value': value,
        'shares': shares,
        'price': price,
        'is_officer': is_officer,
        'is_director': is_director,
        'officer_title': role_title_match.group(1) if role_title_match else None,
        'trans_date': date_match.group(1) if date_match else None,
        'is_10b5_plan': is_10b5,
    }


def find_clusters(
    lookback_days=30,
    min_insiders=3,
    min_purchase_value=50000,
    max_to_check=100,
    filter_10b5=True,
    quiet=False,
):
    """Find insider purchase clusters in the current quarter's EDGAR data.

    Args:
        lookback_days: How many days back to search for Form 4 filings
        min_insiders: Minimum number of unique insiders for a cluster
        min_purchase_value: Minimum purchase value per insider ($)
        max_to_check: Maximum number of CIK clusters to parse (speed limit, default 100)
        filter_10b5: If True, exclude 10b5-1 pre-planned trades
        quiet: If True, suppress all progress output

    Returns:
        List of cluster dicts: {ticker, company, cluster_date, n_insiders,
                                 total_value, purchases: [...]}
    """
    def log(*args, **kwargs):
        if not quiet:
            print(*args, **kwargs)

    year, quarter = get_current_quarter()
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')

    log(f"Scanning EDGAR {year}Q{quarter} for insider clusters...")
    log(f"  Period: {cutoff_date} to {today}")
    log(f"  Min insiders: {min_insiders}, Min value: ${min_purchase_value:,}")

    # Download quarterly index
    entries = download_form_idx(year, quarter, quiet=quiet)
    if not entries:
        # Try previous quarter if current not available
        prev_q = quarter - 1
        prev_y = year
        if prev_q == 0:
            prev_q = 4
            prev_y -= 1
        log(f"Trying previous quarter {prev_y}Q{prev_q}...")
        entries = download_form_idx(prev_y, prev_q, quiet=quiet)

    # Filter to recent filings
    recent = [e for e in entries if e['date'] >= cutoff_date]
    log(f"  Recent Form 4 filings: {len(recent)}")

    # Group by CIK (issuing company)
    cik_filings = defaultdict(list)
    for entry in recent:
        cik_filings[entry['cik']].append(entry)

    # Filter to CIKs with enough filings to potentially be clusters
    potential_ciks = {
        cik: filings for cik, filings in cik_filings.items()
        if len(filings) >= min_insiders
    }
    log(f"  CIKs with {min_insiders}+ Form 4 filings: {len(potential_ciks)}")

    # Sort by filing count (most filings first = most likely cluster)
    sorted_ciks = sorted(potential_ciks.items(), key=lambda x: -len(x[1]))

    # Parse XML for each potential cluster
    log(f"\nParsing Form 4 XML files (checking up to {max_to_check} CIKs)...")

    cik_purchases = defaultdict(list)  # cik -> list of purchase dicts
    checked = 0

    for cik, filings in sorted_ciks[:max_to_check]:
        for filing in filings:
            result = parse_form4_xml(cik, filing['path'])
            if result and result['value'] >= min_purchase_value:
                if filter_10b5 and result.get('is_10b5_plan'):
                    continue  # Skip pre-planned trades
                cik_purchases[cik].append({
                    'filing_date': filing['date'],
                    **result
                })

        checked += 1
        if not quiet and checked % 50 == 0:
            print(f"  Checked {checked}/{min(max_to_check, len(sorted_ciks))}...", end='\r')

    log(f"\n  Checked {checked} CIKs")

    # Find clusters (2+ unique insiders in the window)
    clusters = []
    for cik, purchases in cik_purchases.items():
        # Check date window within 30 days
        unique_reporters = set(p['reporter'] for p in purchases)
        if len(unique_reporters) < min_insiders:
            continue

        # Find the earliest and latest purchase dates
        trans_dates = [p.get('trans_date', p['filing_date']) for p in purchases]
        trans_dates = [d for d in trans_dates if d]
        if trans_dates:
            earliest = min(trans_dates)
            latest = max(trans_dates)
            days_span = (datetime.strptime(latest, '%Y-%m-%d') -
                        datetime.strptime(earliest, '%Y-%m-%d')).days
            if days_span > 30:
                continue  # Purchases span more than 30 days

        ticker = purchases[0].get('ticker', 'UNK')
        company = purchases[0].get('company', 'unknown')
        total_val = sum(p['value'] for p in purchases)
        cluster_date = max(p['filing_date'] for p in purchases)  # Last filing date

        clusters.append({
            'cik': cik,
            'ticker': ticker,
            'company': company,
            'cluster_date': cluster_date,
            'n_insiders': len(unique_reporters),
            'n_purchases': len(purchases),
            'total_value': total_val,
            'purchases': purchases,
        })

    # Sort by most recent cluster date, then by n_insiders
    clusters.sort(key=lambda x: (-len(x['cluster_date']), -x['n_insiders']))

    log(f"\nClusters found ({min_insiders}+ insiders): {len(clusters)}")
    return clusters


def main():
    parser = argparse.ArgumentParser(description='Real-time insider cluster scanner')
    parser.add_argument('--min-insiders', type=int, default=3,
                       help='Minimum number of unique insiders in cluster (default: 3)')
    parser.add_argument('--days', type=int, default=30,
                       help='Lookback days (default: 30)')
    parser.add_argument('--min-value', type=float, default=50000,
                       help='Minimum purchase value per insider (default: 50000)')
    parser.add_argument('--max-check', type=int, default=100,
                       help='Max CIKs to check XML for (speed limit, default: 100)')
    parser.add_argument('--no-10b5-filter', action='store_true',
                       help='Do not filter 10b5-1 pre-planned trades')
    parser.add_argument('--output', type=str, default=None,
                       help='Output JSON file for cluster results')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress progress output (for automated scripts)')
    args = parser.parse_args()

    clusters = find_clusters(
        lookback_days=args.days,
        min_insiders=args.min_insiders,
        min_purchase_value=args.min_value,
        max_to_check=args.max_check,
        filter_10b5=not args.no_10b5_filter,
        quiet=args.quiet,
    )

    print(f"\n{'='*60}")
    print(f"INSIDER CLUSTER REPORT — {datetime.now().strftime('%Y-%m-%d')}")
    print(f"{'='*60}")
    print(f"Criteria: {args.min_insiders}+ insiders, >${args.min_value:,.0f} each, within {args.days} days")
    print(f"{'='*60}\n")

    for cluster in clusters:
        print(f"★ {cluster['ticker']} ({cluster['company'].split('(')[0].strip()})")
        print(f"  Cluster date: {cluster['cluster_date']}")
        print(f"  Insiders: {cluster['n_insiders']}, Total value: ${cluster['total_value']:,.0f}")
        for p in cluster['purchases']:
            role = 'Officer' if p['is_officer'] else 'Director'
            title = p.get('officer_title', '') or ''
            print(f"  - {p['reporter']} ({role}{': '+title if title else ''}): "
                  f"${p['value']:,.0f} on {p.get('trans_date', p['filing_date'])}")
        print()

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(clusters, f, indent=2, default=str)
        print(f"Results saved to {args.output}")

    return clusters


if __name__ == '__main__':
    main()
