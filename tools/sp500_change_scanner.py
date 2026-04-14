"""
S&P 500 Index Change Scanner

Monitors for S&P 500 index composition changes (additions and deletions).
Checks S&P Dow Jones press releases and EDGAR 8-K filings for official announcements.

Schedule: Run around the first Friday of March, June, September, December
          (quarterly rebalance announcement days), plus any off-cycle checks.

Usage:
    python tools/sp500_change_scanner.py
    python tools/sp500_change_scanner.py --check-now  # Force immediate check
    python tools/sp500_change_scanner.py --days 7     # Check last 7 days
"""

import sys
import os
import json
import requests
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'logs', 'sp500_scanner.log')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db as _db


def log(msg):
    """Write timestamped log message."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')


def load_state():
    """Load scanner state from SQLite."""
    _db.init_db()
    return _db.get_state('sp500_change_scanner') or {"last_check": None, "seen_announcements": []}


def save_state(state):
    """Save scanner state to SQLite."""
    _db.init_db()
    _db.set_state('sp500_change_scanner', state)


def is_quarterly_announcement_week():
    """
    Check if today is within the window for quarterly rebalance announcements.
    S&P announces on the FIRST FRIDAY of March, June, September, December.
    We watch from 3 days before to 2 days after.
    """
    today = datetime.now()
    month = today.month
    if month not in (3, 6, 9, 12):
        return False

    # Find first Friday of the month
    first_day = today.replace(day=1)
    # weekday(): Monday=0, Friday=4
    days_to_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_to_friday)

    # Are we within the announcement window (3 days before to 2 days after)?
    days_from_friday = abs((today - first_friday).days)
    return days_from_friday <= 3


def check_spglobal_rss():
    """
    Check S&P Global press release RSS feed for index change announcements.
    Returns list of relevant announcements.

    Note: RSS feed returns 403 as of April 2026 (Akamai WAF block).
    Falls back to S&P DJI announcements page scraping.
    """
    import re

    # Try multiple S&P Global URLs — RSS blocked since ~2026-03
    urls_to_try = [
        "https://www.spglobal.com/spdji/en/rss/sp-indices-news.xml",
        "https://www.spglobal.com/spdji/en/media-center/press-releases/",
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    announcements = []
    errors = []

    for url in urls_to_try:
        try:
            resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            if resp.status_code != 200:
                errors.append(f"{url}: HTTP {resp.status_code}")
                continue

            content = resp.text

            # Check for S&P 500 rebalance/change keywords
            sp500_keywords = ['S&P 500', 'S&P Composite 1500']
            change_keywords = ['addition', 'added to', 'removed from', 'rebalanc',
                               'change', 'recompos', 'reconstitut', 'deletion']

            has_sp500 = any(k in content for k in sp500_keywords)
            has_change = any(k in content.lower() for k in change_keywords)

            if has_sp500 and has_change:
                # Try XML parsing (RSS)
                items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
                for item in items:
                    title_match = re.search(r'<title>(.*?)</title>', item)
                    date_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                    link_match = re.search(r'<link>(.*?)</link>', item)

                    title = title_match.group(1) if title_match else ''
                    pub_date = date_match.group(1) if date_match else ''
                    link = link_match.group(1) if link_match else ''

                    if any(k in title for k in sp500_keywords) and \
                       any(k in title.lower() for k in change_keywords):
                        announcements.append({
                            'title': title.strip(),
                            'date': pub_date.strip(),
                            'link': link.strip(),
                            'source': 'spglobal_rss'
                        })

                # Try HTML parsing (press releases page)
                if not announcements:
                    # Look for press release links with S&P 500 mentions
                    links = re.findall(
                        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
                        content, re.DOTALL
                    )
                    for href, text in links:
                        text_clean = re.sub(r'<[^>]+>', '', text).strip()
                        if any(k in text_clean for k in sp500_keywords) and \
                           any(k in text_clean.lower() for k in change_keywords):
                            announcements.append({
                                'title': text_clean[:200],
                                'date': '',
                                'link': href if href.startswith('http') else f'https://www.spglobal.com{href}',
                                'source': 'spglobal_web'
                            })

            if announcements:
                return announcements, None

        except Exception as e:
            errors.append(f"{url}: {e}")

    error_msg = "; ".join(errors) if errors else None
    return announcements if announcements else None, error_msg


def check_edgar_8k_for_sp500(days_back=14):
    """
    Check EDGAR for recent 8-K filings mentioning S&P 500 addition.
    Many added companies file 8-Ks about the addition.
    Uses EDGAR full-text search (EFTS).

    Searches for both "S&P 500" and "Standard & Poor's 500" in 8-K filings.
    Also checks 8-K12B (shell company events) and 8-K/A (amendments).
    """
    headers = {
        'User-Agent': 'research-agent admin@example.com',
        'Accept-Encoding': 'gzip, deflate',
    }

    today = datetime.now()
    start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    # Use the proper EFTS search-index endpoint with correct params
    search_queries = [
        '"added to the S&P 500"',
        '"inclusion in the S&P 500"',
        '"S&P 500" AND "index change"',
    ]

    all_additions = []
    seen_ids = set()

    for query in search_queries:
        try:
            resp = requests.get(
                'https://efts.sec.gov/LATEST/search-index',
                params={
                    'q': query,
                    'dateRange': 'custom',
                    'startdt': start_date,
                    'enddt': end_date,
                    'forms': '8-K,8-K12B,8-K/A',
                },
                headers=headers,
                timeout=15
            )

            if resp.status_code != 200:
                log(f"EDGAR EFTS returned HTTP {resp.status_code} for query: {query[:50]}")
                continue

            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])
            for hit in hits[:10]:
                doc_id = hit.get('_id', '')
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                src = hit.get('_source', {})
                # display_names is more reliable than entity_name
                names = src.get('display_names', [])
                company = names[0] if names else src.get('entity_name', 'Unknown')

                all_additions.append({
                    'company': company,
                    'date': src.get('file_date', 'Unknown'),
                    'form': src.get('form_type', '8-K'),
                    'ciks': src.get('ciks', []),
                    'doc_id': doc_id,
                })

            # Rate limit between EDGAR queries
            time.sleep(0.15)

        except Exception as e:
            log(f"EDGAR EFTS query failed: {e}")
            continue

    if all_additions:
        return all_additions, None
    return [], None


def check_wikipedia_sp500_changes():
    """
    Check Wikipedia's 'List of S&P 500 companies' page for recent changes.
    Wikipedia maintains a 'Selected changes' table that is updated promptly.
    Returns list of recent changes or error.
    """
    import re

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (research-agent; +https://github.com/) Gecko/20100101',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None, f"Wikipedia HTTP {resp.status_code}"

        content = resp.text

        # Look for the "Selected changes" section
        # The table has columns: Date, Added (ticker, security), Removed (ticker, security), Reason
        changes = []
        current_year = datetime.now().year

        # Find rows in the changes table
        # Pattern: date in YYYY-MM-DD or Month DD, YYYY format followed by ticker symbols
        table_pattern = re.findall(
            r'<tr>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>'
            r'\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>',
            content, re.DOTALL
        )

        for row in table_pattern:
            date_text = re.sub(r'<[^>]+>', '', row[0]).strip()
            # Check if this is from the current year or last 30 days
            if str(current_year) in date_text or str(current_year - 1) in date_text:
                added_ticker = re.sub(r'<[^>]+>', '', row[1]).strip()
                added_name = re.sub(r'<[^>]+>', '', row[2]).strip()
                removed_ticker = re.sub(r'<[^>]+>', '', row[3]).strip()
                removed_name = re.sub(r'<[^>]+>', '', row[4]).strip()

                if added_ticker or removed_ticker:
                    changes.append({
                        'date': date_text,
                        'added_ticker': added_ticker,
                        'added_name': added_name,
                        'removed_ticker': removed_ticker,
                        'removed_name': removed_name,
                        'source': 'wikipedia'
                    })

        return changes[:20], None  # Cap at 20 most recent

    except Exception as e:
        return None, str(e)


def check_known_addition_tickers(tickers_to_check, days_back=14):
    """
    Check if any given tickers show unusual price movement in the last N days.
    High abnormal return could indicate recent S&P 500 addition announcement.

    Returns: list of tickers with >5% abnormal return suggesting recent announcement.
    """
    try:
        import yfinance as yf
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

        # We can't easily measure abnormal return without the benchmark
        # Just check if they've moved >5% vs SPY recently
        spy = yf.download('SPY', period=f'{days_back}d', progress=False)
        if spy.empty:
            return []
        spy_return = float(spy['Close'].iloc[-1] / spy['Close'].iloc[0] - 1) * 100

        signals = []
        for ticker in tickers_to_check:
            try:
                df = yf.download(ticker, period=f'{days_back}d', progress=False)
                if df.empty or len(df) < 2:
                    continue
                stock_return = float(df['Close'].iloc[-1] / df['Close'].iloc[0] - 1) * 100
                abnormal = stock_return - spy_return
                if abnormal > 5.0:
                    signals.append({
                        'symbol': ticker,
                        'abnormal_return_pct': round(abnormal, 1),
                        'stock_return_pct': round(stock_return, 1)
                    })
            except:
                pass

        return signals

    except Exception as e:
        return []


def run_scan(args=None):
    """Main scan function.

    Data sources (in order):
    1. EDGAR EFTS — 8-K filings mentioning S&P 500 addition (always runs)
    2. S&P Global website — press releases via RSS or web scraping (announcement window / forced)
    3. Wikipedia — 'List of S&P 500 companies' recent changes table (announcement window / forced)
    """
    log("=== S&P 500 Change Scanner ===")

    force_check = args and '--check-now' in args
    days = 14
    if args and '--days' in args:
        idx = args.index('--days')
        if idx + 1 < len(args):
            days = int(args[idx + 1])

    state = load_state()

    in_window = is_quarterly_announcement_week()
    if in_window:
        log("IN quarterly rebalance announcement window")
    elif force_check:
        log("Forced check (--check-now flag)")
    else:
        log("Not in announcement window. Use --check-now to force scan.")

    results_summary = {
        'scan_date': datetime.now().isoformat(),
        'in_window': in_window,
        'edgar_8k_additions': [],
        'spglobal_announcements': [],
        'wikipedia_changes': [],
        'new_announcements': [],
    }

    # 1. Always check EDGAR 8-K filings (most reliable, free, works)
    log(f"Checking EDGAR 8-K filings (last {days} days)...")
    edgar_additions, edgar_error = check_edgar_8k_for_sp500(days_back=days)

    if edgar_error:
        log(f"EDGAR check failed: {edgar_error}")
    elif edgar_additions:
        log(f"Found {len(edgar_additions)} potential S&P 500 addition 8-Ks:")
        for a in edgar_additions:
            log(f"  {a['company']} ({a['date']}) [{a['form']}]")
        results_summary['edgar_8k_additions'] = edgar_additions
    else:
        log(f"No S&P 500 addition 8-Ks found in last {days} days")

    # 2. Check S&P Global website (announcement window or forced)
    if in_window or force_check:
        log("Checking S&P Global press releases...")
        spglobal_news, rss_error = check_spglobal_rss()

        if rss_error:
            log(f"S&P Global check failed: {rss_error}")
        elif spglobal_news:
            log(f"Found {len(spglobal_news)} relevant S&P press releases:")
            for item in spglobal_news:
                log(f"  [{item.get('date','')}] {item['title']}")
            results_summary['spglobal_announcements'] = spglobal_news
        else:
            log("No relevant S&P 500 change press releases found")

    # 3. Check Wikipedia recent changes (announcement window or forced)
    if in_window or force_check:
        log("Checking Wikipedia S&P 500 changes table...")
        wiki_changes, wiki_error = check_wikipedia_sp500_changes()

        if wiki_error:
            log(f"Wikipedia check failed: {wiki_error}")
        elif wiki_changes:
            current_year = datetime.now().year
            recent = [c for c in wiki_changes if str(current_year) in c.get('date', '')]
            if recent:
                log(f"Found {len(recent)} S&P 500 changes in {current_year}:")
                for c in recent[:5]:
                    added = c.get('added_ticker', '')
                    removed = c.get('removed_ticker', '')
                    log(f"  {c['date']}: +{added} / -{removed}")
                results_summary['wikipedia_changes'] = recent
            else:
                log("No current-year S&P 500 changes on Wikipedia")
        else:
            log("No Wikipedia S&P 500 changes parsed")

    # Deduplicate: find truly new announcements not in state
    seen = set(state.get('seen_announcements', []))
    all_signals = []

    for a in results_summary.get('edgar_8k_additions', []):
        key = f"edgar:{a.get('doc_id', a['company'])}"
        if key not in seen:
            all_signals.append({**a, 'source': 'edgar', '_key': key})

    for a in results_summary.get('spglobal_announcements', []):
        key = f"spglobal:{a.get('link', a['title'])}"
        if key not in seen:
            all_signals.append({**a, '_key': key})

    for c in results_summary.get('wikipedia_changes', []):
        key = f"wiki:{c.get('added_ticker', '')}:{c.get('date', '')}"
        if key not in seen:
            all_signals.append({**c, 'source': 'wikipedia', '_key': key})

    results_summary['new_announcements'] = all_signals

    # Update state
    for sig in all_signals:
        seen.add(sig['_key'])
    state['last_check'] = datetime.now().isoformat()
    state['seen_announcements'] = list(seen)[-200:]  # Keep last 200
    save_state(state)

    # Summarize
    log("")
    if all_signals:
        log(f"ALERT: {len(all_signals)} NEW potential S&P 500 change(s) detected!")
        log("ACTION: Review and create hypothesis if confirmed addition.")
        log("  - Enter LONG at next market open on each confirmed addition")
        log("  - $5,000 per position, 5-day hold")
    else:
        log("No new S&P 500 changes detected.")
        if in_window:
            log("Still in announcement window — re-run tomorrow if no announcement yet.")

    return results_summary


if __name__ == "__main__":
    run_scan(sys.argv[1:])
