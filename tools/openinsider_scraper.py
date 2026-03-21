"""
OpenInsider Cluster Buy Scraper

Scrapes the OpenInsider.com cluster buy screener to find recent insider
cluster buying events (2+ or 3+ insiders buying same company within N days).

This is faster than parsing EDGAR XML directly and provides pre-filtered
cluster data with deduplication already done.

Usage:
    from tools.openinsider_scraper import get_cluster_buys
    
    clusters = get_cluster_buys(min_insiders=3, days=30, min_value_k=50)
    for c in clusters:
        print(c['ticker'], c['n_insiders'], c['filing_date'])
"""

import re
import requests
import json
import time
from datetime import datetime, timedelta
from typing import Optional


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE_URL = "http://openinsider.com"
CACHE_TTL_SECONDS = 3600  # 1 hour

# NOTE: The grouped screener (/screener?grp=1) uses JavaScript rendering and
# returns empty data in static HTML. Use /latest-cluster-buys instead.
CLUSTER_URL = f"{BASE_URL}/latest-cluster-buys"

_cache = {}


def get_cluster_buys(
    min_insiders: int = 3,
    days: int = 30,
    min_value_k: int = 50,
    exclude_funds: bool = True,
    count: int = 100,
) -> list[dict]:
    """
    Fetch recent insider cluster buying events from OpenInsider.

    Uses /latest-cluster-buys which shows companies with multiple insiders
    buying in the same period. Results are pre-filtered by OpenInsider.

    Args:
        min_insiders: Minimum number of unique insiders in cluster
        days: Filter post-hoc by filing date (not URL parameter)
        min_value_k: Minimum purchase value in thousands (used in URL)
        exclude_funds: If True, exclude closed-end funds (BDCs, CEFs)
        count: Max number of results to return

    Returns:
        List of cluster events, each with:
            ticker, company, industry, n_insiders, filing_date, trade_date,
            price_per_share, shares_purchased, total_value_k, flags
    """
    cache_key = f"latest_clusters_{min_insiders}_{min_value_k}"
    if cache_key in _cache:
        cached_at, data = _cache[cache_key]
        if time.time() - cached_at < CACHE_TTL_SECONDS:
            return data

    try:
        r = requests.get(CLUSTER_URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch OpenInsider data: {e}")

    all_clusters = _parse_cluster_rows(r.text, exclude_funds, min_insiders, min_value_k)

    # Filter by days (post-hoc)
    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        all_clusters = [c for c in all_clusters if c['filing_date'] and c['filing_date'] >= cutoff]

    clusters = all_clusters[:count]
    _cache[cache_key] = (time.time(), clusters)
    return clusters


def get_ticker_detail(ticker: str) -> list[dict]:
    """
    Get all recent transactions for a specific ticker.
    
    Returns list of transactions with: name, title, filing_date, trade_date,
    price, shares, value, transaction_type
    """
    try:
        r = requests.get(f"{BASE_URL}/{ticker}", headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch ticker page: {e}")
    
    return _parse_ticker_transactions(r.text, ticker)


def _parse_cluster_rows(html: str, exclude_funds: bool, min_insiders: int = 2, min_value_k: int = 0) -> list[dict]:
    """Parse the cluster screener HTML into structured data."""
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    
    fund_keywords = ['Fund', 'Capital Corp', 'Investment Corp', 'BDC', 'CEF', 
                     'Closed-End', 'Lending Corp', 'Finance, Inc', 'Churchill']
    
    clusters = []
    for row in rows:
        # Skip header rows (no ticker href)
        tickers_in_row = re.findall(r'href="/([A-Z]{1,5})"', row)
        if not tickers_in_row:
            continue
        ticker = tickers_in_row[0]
        
        # Get all cell text
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        clean = []
        for c in cells:
            text = re.sub(r'<[^>]+>', '', c).strip()
            text = text.replace('&nbsp;', ' ').replace('&amp;', '&').strip()
            if text:
                clean.append(text)
        
        if not clean:
            continue
        
        # Find key fields
        filing_date = _find_date(clean, 0)
        trade_date = _find_date(clean, 1)
        company = _find_company_name(row)
        industry = _find_industry(row)
        n_insiders = _find_count(clean, min_val=2)
        price = _find_price(clean)
        shares = _find_shares(clean)
        flags = _find_flags(clean)
        
        if not ticker or not n_insiders or n_insiders < min_insiders:
            continue

        # Filter by purchase type
        if not any('Purchase' in c for c in clean):
            continue

        # Filter by minimum value
        if price and shares and min_value_k > 0:
            if price * shares / 1000 < min_value_k:
                continue
        
        # Exclude funds if requested
        if exclude_funds and industry:
            if any(kw.lower() in industry.lower() for kw in ['fund', 'closed-end']):
                continue
            if company and any(kw.lower() in company.lower() for kw in 
                               ['fund', 'capital corp', 'investment corp', 'lending corp']):
                continue
        
        total_value_k = None
        if price and shares:
            total_value_k = round(price * shares / 1000, 0)
        
        clusters.append({
            'ticker': ticker,
            'company': company or ticker,
            'industry': industry or 'Unknown',
            'n_insiders': n_insiders,
            'filing_date': filing_date,
            'trade_date': trade_date,
            'price_per_share': price,
            'shares_purchased': shares,
            'total_value_k': total_value_k,
            'flags': flags,
        })
    
    return clusters


def _parse_ticker_transactions(html: str, ticker: str) -> list[dict]:
    """Parse individual ticker page for recent transactions."""
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    transactions = []
    
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        clean = []
        for c in cells:
            text = re.sub(r'<[^>]+>', '', c).strip()
            text = text.replace('&nbsp;', ' ').replace('&amp;', '&').strip()
            if text:
                clean.append(text)
        
        if not clean or '2026' not in str(clean) + str(row):
            continue
        if not any('Purchase' in c for c in clean):
            continue
        
        filing_date = _find_date(clean, 0)
        trade_date = _find_date(clean, 1)
        price = _find_price(clean)
        shares = _find_shares_signed(clean)
        
        # Find person name and title (usually cells after ticker)
        name = None
        title = None
        for i, c in enumerate(clean):
            if len(c) > 8 and ' ' in c and not re.match(r'\d{4}-', c) and not re.match(r'\$', c):
                if name is None:
                    name = c
                elif title is None and any(role in c for role in ['Dir', 'CEO', 'CFO', 'EVP', 'SVP', 'Pres', 'COO', 'VP']):
                    title = c
                    break
        
        if filing_date and price:
            value = (price * abs(shares)) if shares else None
            transactions.append({
                'ticker': ticker,
                'name': name,
                'title': title,
                'filing_date': filing_date,
                'trade_date': trade_date,
                'price': price,
                'shares': abs(shares) if shares else None,
                'value': value,
                'meets_50k': value >= 50000 if value else False,
            })
    
    return transactions


def _find_date(cells: list, nth: int = 0) -> Optional[str]:
    """Find the nth date in the form YYYY-MM-DD."""
    count = 0
    for c in cells:
        m = re.match(r'(2\d{3}-\d{2}-\d{2})', c)
        if m:
            if count == nth:
                return m.group(1)
            count += 1
    return None


def _find_company_name(row: str) -> Optional[str]:
    """Extract company name from link text.

    HTML structure: <td><a href="/TICK">Company Full Name</a></td>
    The company name link is a simple <a href> without onmouseover.
    """
    # Match ONLY simple <a href="/TICKER"> links (no other attributes)
    # The company name link does NOT have onmouseover etc.
    matches = re.findall(r'<a\s+href="/[A-Z]{1,5}">([^<]+)</a>', row)
    for m in matches:
        text = m.strip()
        # Skip dates
        if re.match(r'\d{4}-\d{2}-\d{2}', text):
            continue
        # Skip pure ticker symbols (1-5 uppercase letters only)
        if re.match(r'^[A-Z]{1,5}$', text):
            continue
        # Skip very short strings or pure numbers
        if len(text) <= 3 or text.isdigit():
            continue
        return text
    return None


def _find_industry(row: str) -> Optional[str]:
    """Extract industry from industry link."""
    m = re.search(r'href="/industry/[^"]+">([^<]+)</a>', row)
    return m.group(1).strip() if m else None


def _find_count(cells: list, min_val: int = 2, max_val: int = 100) -> Optional[int]:
    """Find the insider count (a small integer in the cells)."""
    for c in cells:
        if c.isdigit():
            n = int(c)
            if min_val <= n <= max_val:
                return n
    return None


def _find_price(cells: list) -> Optional[float]:
    """Find share price (dollar amount)."""
    for c in cells:
        m = re.match(r'^\$([\d,]+\.?\d*)$', c)
        if m:
            return float(m.group(1).replace(',', ''))
    return None


def _find_shares(cells: list) -> Optional[int]:
    """Find shares purchased (positive number with +/- sign)."""
    for c in cells:
        m = re.match(r'^\+([\d,]+)$', c)
        if m:
            return int(m.group(1).replace(',', ''))
    return None


def _find_shares_signed(cells: list) -> Optional[int]:
    """Find signed share count."""
    for c in cells:
        m = re.match(r'^([+-])([\d,]+)$', c)
        if m:
            sign = 1 if m.group(1) == '+' else -1
            return sign * int(m.group(2).replace(',', ''))
    return None


def _find_flags(cells: list) -> list[str]:
    """Find transaction flags (M, D, A, E)."""
    flags = []
    for c in cells[:3]:  # Flags usually in first few cells
        if c in ('M', 'D', 'A', 'E', 'DM', 'MD'):
            flags.append(c)
    return flags


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='OpenInsider cluster screener')
    parser.add_argument('--min-insiders', type=int, default=3)
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--min-value-k', type=int, default=50)
    parser.add_argument('--ticker', type=str, help='Get details for specific ticker')
    args = parser.parse_args()
    
    if args.ticker:
        print(f"\n{args.ticker} recent insider transactions:")
        txns = get_ticker_detail(args.ticker)
        for t in txns:
            print(f"  {t['filing_date']} | {t['name'][:25]} | {t['title'][:10]} | ${t['price']} × {t['shares']:,} = ${t.get('value',0):,.0f}")
    else:
        print(f"\nCluster buy screener (min {args.min_insiders} insiders, last {args.days} days):")
        clusters = get_cluster_buys(
            min_insiders=args.min_insiders,
            days=args.days,
            min_value_k=args.min_value_k
        )
        print(f"Found {len(clusters)} clusters:")
        for c in clusters:
            print(f"  {c['ticker']:6} | {c['n_insiders']}x | filed {c['filing_date']} | trade {c['trade_date']} | {c['company'][:35]}")
