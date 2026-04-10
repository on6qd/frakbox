#!/usr/bin/env python3
"""NT 10-K / NT 10-Q late filing scanner.

When a company can't file their 10-K or 10-Q on time, they file an NT (Non-Timely)
notification with the SEC. This signals potential accounting problems, internal
control weaknesses, or restatement risk.

Hypothesis: NT filings predict negative abnormal returns of -3% to -6%.

This scanner:
1. Fetches NT 10-K and NT 10-Q filings from EDGAR EFTS
2. Maps CIKs to tickers via display_name extraction
3. Filters to large-cap (>$500M market cap)
4. Returns events suitable for backtesting

Usage:
    # Historical backtest: fetch all NT filings in a date range
    python tools/nt_filing_scanner.py --start 2022-01-01 --end 2025-12-31

    # Recent scan (real-time monitoring)
    python tools/nt_filing_scanner.py --days 7

    # Output as JSON events for data_tasks.py backtest
    python tools/nt_filing_scanner.py --start 2022-01-01 --end 2025-12-31 --json-events
"""
import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, '/Users/frakbox/Bots/financial_researcher')

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None

HEADERS = {"User-Agent": "financial-researcher research@example.com"}
SEC_DELAY = 0.15  # 150ms between requests
MIN_MARKET_CAP = 500_000_000  # $500M

# EDGAR EFTS max results per page
EFTS_PAGE_SIZE = 100
EFTS_MAX_RESULTS = 1000  # EFTS hard limit


def search_nt_filings(start_date: str, end_date: str, form_type: str = "NT 10-K") -> list[dict]:
    """Search EDGAR EFTS for NT filings in date range.

    form_type: "NT 10-K" or "NT 10-Q"
    """
    # URL-encode the form type (space -> %20, no quotes)
    encoded_form = form_type.replace(" ", "%20")

    base_url = (
        f"https://efts.sec.gov/LATEST/search-index"
        f"?forms={encoded_form}"
        f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
    )

    all_hits = []

    # First request to get total
    url = base_url + f"&from=0&size={EFTS_PAGE_SIZE}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"EFTS error for {form_type}: {resp.status_code}", file=sys.stderr)
        return []

    data = resp.json()
    total = data.get("hits", {}).get("total", {}).get("value", 0)
    hits = data.get("hits", {}).get("hits", [])
    all_hits.extend(hits)

    print(f"  {form_type}: {total} total filings found ({start_date} to {end_date})", file=sys.stderr)

    # Paginate (EFTS caps at 10000 total but we rarely need more than 1000 per chunk)
    fetched = len(hits)
    max_to_fetch = min(total, 10000)
    while fetched < max_to_fetch:
        time.sleep(SEC_DELAY)
        url = base_url + f"&from={fetched}&size={EFTS_PAGE_SIZE}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  Pagination error at offset {fetched}: {resp.status_code}", file=sys.stderr)
            break
        page_hits = resp.json().get("hits", {}).get("hits", [])
        if not page_hits:
            break
        all_hits.extend(page_hits)
        fetched += len(page_hits)

    results = []
    for h in all_hits:
        src = h.get("_source", {})
        ciks = src.get("ciks", [])
        names = src.get("display_names", [])
        file_date = src.get("file_date", "")

        # Extract ticker from display_name: "Company Name  (TICK)  (CIK ...)"
        ticker = None
        if names:
            m = re.search(r'\(([A-Z]{1,5})\)', names[0])
            if m:
                ticker = m.group(1)

        results.append({
            "cik": ciks[0].lstrip("0") if ciks else "",
            "display_name": names[0] if names else "",
            "ticker": ticker,
            "file_date": file_date,
            "form_type": form_type,
        })

    return results


def get_market_cap(ticker: str) -> float | None:
    """Get current market cap for a ticker using yfinance."""
    if not yf:
        return None
    try:
        info = yf.Ticker(ticker).info
        return info.get("marketCap")
    except Exception:
        return None


def filter_to_largecap(filings: list[dict], min_cap: float = MIN_MARKET_CAP) -> list[dict]:
    """Filter filings to large-cap companies (>$500M market cap).

    Uses batch approach: check unique tickers, cache results.
    """
    # Get unique tickers
    tickers = set(f["ticker"] for f in filings if f["ticker"])
    print(f"  Checking market caps for {len(tickers)} unique tickers...", file=sys.stderr)

    cap_cache = {}
    checked = 0
    for t in sorted(tickers):
        cap = get_market_cap(t)
        cap_cache[t] = cap
        checked += 1
        if checked % 20 == 0:
            print(f"    Checked {checked}/{len(tickers)} tickers", file=sys.stderr)
        time.sleep(0.1)  # Rate limit yfinance

    filtered = []
    for f in filings:
        t = f["ticker"]
        if not t:
            continue
        cap = cap_cache.get(t)
        if cap and cap >= min_cap:
            f["market_cap"] = cap
            filtered.append(f)

    return filtered


def deduplicate_events(filings: list[dict]) -> list[dict]:
    """Deduplicate: same ticker + same filing date = one event.
    Also remove same ticker within 30 days (quarterly + annual double-filing).
    """
    # Sort by date
    filings.sort(key=lambda x: (x.get("ticker", ""), x.get("file_date", "")))

    seen = {}  # ticker -> last filing date
    deduped = []
    for f in filings:
        t = f["ticker"]
        d = f["file_date"]
        if not t or not d:
            continue

        key = t
        if key in seen:
            last_date = datetime.strptime(seen[key], "%Y-%m-%d")
            this_date = datetime.strptime(d, "%Y-%m-%d")
            if (this_date - last_date).days < 30:
                continue  # Skip duplicate within 30 days

        seen[key] = d
        deduped.append(f)

    return deduped


def scan_nt_filings(start_date: str, end_date: str, filter_largecap: bool = True, min_cap: float = MIN_MARKET_CAP) -> list[dict]:
    """Main scanner: fetch all NT 10-K and NT 10-Q filings in range.

    Returns deduplicated, optionally large-cap filtered events.
    """
    all_filings = []

    # EFTS has a 1000 result limit. For large date ranges, chunk by quarter.
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Chunk into 3-month windows
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=90), end)
        s = current.strftime("%Y-%m-%d")
        e = chunk_end.strftime("%Y-%m-%d")

        for form_type in ["NT 10-K", "NT 10-Q"]:
            filings = search_nt_filings(s, e, form_type)
            all_filings.extend(filings)
            time.sleep(SEC_DELAY)

        current = chunk_end + timedelta(days=1)

    print(f"\n  Total NT filings found: {len(all_filings)}", file=sys.stderr)

    # Filter to those with tickers
    with_ticker = [f for f in all_filings if f["ticker"]]
    print(f"  With tickers: {len(with_ticker)}", file=sys.stderr)

    # Deduplicate
    deduped = deduplicate_events(with_ticker)
    print(f"  After dedup (30-day window): {len(deduped)}", file=sys.stderr)

    # Filter to large-cap
    if filter_largecap:
        filtered = filter_to_largecap(deduped, min_cap=min_cap)
        print(f"  Large-cap (>{min_cap/1e6:.0f}M): {len(filtered)}", file=sys.stderr)
        return filtered

    return deduped


def to_backtest_events(filings: list[dict]) -> list[dict]:
    """Convert filings to backtest event format."""
    return [
        {"symbol": f["ticker"], "date": f["file_date"]}
        for f in filings
        if f["ticker"] and f["file_date"]
    ]


def main():
    parser = argparse.ArgumentParser(description="NT 10-K/10-Q Late Filing Scanner")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=7, help="Days to look back (if no --start/--end)")
    parser.add_argument("--no-filter", action="store_true", help="Skip large-cap filter")
    parser.add_argument("--json-events", action="store_true", help="Output as JSON events for backtest")
    parser.add_argument("--min-cap", type=float, default=MIN_MARKET_CAP, help="Min market cap (default 500M)")
    args = parser.parse_args()

    min_cap_val = args.min_cap

    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    filings = scan_nt_filings(start_date, end_date, filter_largecap=not args.no_filter, min_cap=min_cap_val)

    if args.json_events:
        events = to_backtest_events(filings)
        print(json.dumps(events))
    else:
        print(f"\n{'='*70}")
        print(f"NT FILING EVENTS: {len(filings)} found ({start_date} to {end_date})")
        print(f"{'='*70}")
        for f in filings:
            cap_str = f"${f.get('market_cap', 0)/1e9:.1f}B" if f.get('market_cap') else "?"
            print(f"  {f['file_date']}  {f['ticker']:6s}  {f['form_type']:10s}  {cap_str:>8s}  {f['display_name'][:50]}")

        # Summary stats
        print(f"\nSummary: {len(filings)} events")
        nt10k = sum(1 for f in filings if f["form_type"] == "NT 10-K")
        nt10q = sum(1 for f in filings if f["form_type"] == "NT 10-Q")
        print(f"  NT 10-K: {nt10k}")
        print(f"  NT 10-Q: {nt10q}")

        if filings:
            # Unique tickers
            tickers = set(f["ticker"] for f in filings)
            print(f"  Unique tickers: {len(tickers)}")

            # Frequency
            dates = [f["file_date"] for f in filings]
            if len(dates) >= 2:
                first = datetime.strptime(min(dates), "%Y-%m-%d")
                last = datetime.strptime(max(dates), "%Y-%m-%d")
                years = max((last - first).days / 365.25, 0.1)
                print(f"  Frequency: {len(filings)/years:.0f}/year")


if __name__ == "__main__":
    main()
