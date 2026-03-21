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
STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          'logs', 'sp500_scanner_state.json')


def log(msg):
    """Write timestamped log message."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')


def load_state():
    """Load scanner state (last seen announcements)."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_check": None, "seen_announcements": []}


def save_state(state):
    """Save scanner state."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


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
    """
    # S&P Dow Jones Indices press releases RSS
    # Note: This URL may change - verify periodically
    rss_url = "https://www.spglobal.com/spdji/en/rss/sp-indices-news.xml"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    }

    try:
        resp = requests.get(rss_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"

        content = resp.text

        # Parse for S&P 500 addition/deletion mentions
        announcements = []

        # Simple parsing for relevant items
        if 'S&P 500' in content and ('addition' in content.lower() or 'added to' in content.lower()
                                      or 'removed from' in content.lower()):
            # Extract items
            import re
            items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
            for item in items:
                title_match = re.search(r'<title>(.*?)</title>', item)
                date_match = re.search(r'<pubDate>(.*?)</pubDate>', item)
                link_match = re.search(r'<link>(.*?)</link>', item)

                title = title_match.group(1) if title_match else ''
                date = date_match.group(1) if date_match else ''
                link = link_match.group(1) if link_match else ''

                if ('S&P 500' in title or 'S&P Composite' in title) and (
                    'addition' in title.lower() or 'rebalanc' in title.lower() or
                    'change' in title.lower() or 'recompos' in title.lower()):
                    announcements.append({
                        'title': title.strip(),
                        'date': date.strip(),
                        'link': link.strip()
                    })

        return announcements, None

    except Exception as e:
        return None, str(e)


def check_edgar_8k_for_sp500():
    """
    Check EDGAR for recent 8-K filings mentioning S&P 500 addition.
    Many added companies file 8-Ks about the addition.
    Uses EDGAR full-text search.
    """
    # Check EDGAR EFTS for recent 8-Ks mentioning S&P 500 addition
    headers = {
        'User-Agent': 'research-agent admin@example.com',
        'Accept-Encoding': 'gzip, deflate',
    }

    today = datetime.now()
    start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')

    # Search for 8-Ks mentioning "added to the S&P 500"
    params = {
        'q': '"added to the S&P 500" OR "added to the Standard & Poor\'s 500"',
        'dateRange': 'custom',
        'startdt': start_date,
        'enddt': today.strftime('%Y-%m-%d'),
        'forms': '8-K',
        'hits.hits.total.value': 5,
        'hits.hits._source': 'period_of_report,entity_name,file_date,form_type'
    }

    try:
        resp = requests.get(
            'https://efts.sec.gov/LATEST/search-index?q=%22added+to+the+S%26P+500%22&dateRange=custom'
            f'&startdt={start_date}&enddt={today.strftime("%Y-%m-%d")}&forms=8-K',
            headers=headers,
            timeout=10
        )

        if resp.status_code == 200:
            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])
            additions = []
            for hit in hits[:10]:
                src = hit.get('_source', {})
                additions.append({
                    'company': src.get('entity_name', 'Unknown'),
                    'date': src.get('file_date', 'Unknown'),
                    'form': src.get('form_type', '8-K')
                })
            return additions, None
        else:
            return None, f"EDGAR HTTP {resp.status_code}"

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
    """Main scan function."""
    log("=== S&P 500 Change Scanner ===")

    force_check = args and '--check-now' in args
    days = 7
    if args and '--days' in args:
        idx = args.index('--days')
        if idx + 1 < len(args):
            days = int(args[idx + 1])

    state = load_state()

    in_window = is_quarterly_announcement_week()
    if in_window:
        log(f"IN quarterly rebalance announcement window - checking S&P press releases")
    elif force_check:
        log(f"Forced check (--check-now flag)")
    else:
        log(f"Not in announcement window. Use --check-now to force scan.")

    results_summary = {
        'scan_date': datetime.now().isoformat(),
        'in_window': in_window,
        'edgar_8k_additions': [],
        'spglobal_announcements': [],
        'new_announcements': []
    }

    # 1. Check EDGAR for 8-K filings about S&P 500 additions
    log(f"Checking EDGAR 8-K filings (last {days} days)...")
    edgar_additions, edgar_error = check_edgar_8k_for_sp500()

    if edgar_error:
        log(f"EDGAR check failed: {edgar_error}")
    elif edgar_additions:
        log(f"Found {len(edgar_additions)} potential S&P 500 addition 8-Ks:")
        for a in edgar_additions:
            log(f"  {a['company']} ({a['date']})")
        results_summary['edgar_8k_additions'] = edgar_additions
    else:
        log("No S&P 500 addition 8-Ks found in last 7 days")

    # 2. Check S&P Global RSS feed (when in announcement window or forced)
    if in_window or force_check:
        log("Checking S&P Global press releases RSS...")
        spglobal_news, rss_error = check_spglobal_rss()

        if rss_error:
            log(f"RSS check failed: {rss_error}")
        elif spglobal_news:
            log(f"Found {len(spglobal_news)} relevant S&P press releases:")
            for item in spglobal_news:
                log(f"  [{item['date']}] {item['title']}")
            results_summary['spglobal_announcements'] = spglobal_news
        else:
            log("No relevant S&P 500 change press releases found")

    # Update state
    state['last_check'] = datetime.now().isoformat()
    save_state(state)

    # Summarize
    log("")
    if results_summary['edgar_8k_additions'] or results_summary['spglobal_announcements']:
        log("ALERT: Potential S&P 500 changes detected!")
        log("ACTION: Review manually and set trigger on hypothesis 061ae3a8 if confirmed")
        log("  - Enter LONG at next market open on each confirmed addition")
        log("  - $5,000 per position, 5-day hold")
        log("  - Hypothesis ID: 061ae3a8")
    else:
        log("No S&P 500 changes detected in current scan window.")
        if in_window:
            log("Still in announcement window - re-run tomorrow if no announcement yet.")

    return results_summary


if __name__ == "__main__":
    run_scan(sys.argv[1:])
