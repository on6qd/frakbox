"""
EDGAR 8-K CEO/CFO Departure Scanner

Scans EDGAR for 8-K filings mentioning Item 5.02 (departure of principal officers)
to find CEO and CFO sudden departures from large-cap companies.

Target signal: CEO performance failure departure → SHORT next-day open
Current exploratory finding: N=9, 0% positive rate, p=0.0039 at 1d
Need: 3 more events (N=12) to form a formal hypothesis

Usage:
    python tools/edgar_ceo_departure_scanner.py --start 2019-01-01 --end 2022-12-31 --min-cap 2000
    python tools/edgar_ceo_departure_scanner.py --start 2019-01-01 --end 2022-12-31 --check-direction
"""

import sys
import requests
import time
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.largecap_filter import get_market_cap

EDGAR_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions"
EDGAR_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts"

HEADERS = {
    "User-Agent": "financial-research-bot contact@example.com",
    "Accept": "application/json"
}


def search_8k_item502(start_date: str, end_date: str, max_results: int = 200) -> list:
    """
    Search EDGAR full-text for 8-K filings with Item 5.02 departures.
    Looks for keywords indicating CEO/CFO sudden departure.

    Returns list of dicts with: cik, company, filing_date, accession, description
    """
    print(f"Searching EDGAR 8-K Item 5.02 filings {start_date} to {end_date}...")

    # Search for 8-K filings with departure keywords
    # Item 5.02 specifically covers "Departure of Directors or Certain Officers"
    queries = [
        '"Chief Executive Officer" "departed" OR "resigned" OR "terminated"',
        '"CEO" "stepped down" OR "effective immediately"',
    ]

    results = []
    seen_accessions = set()

    for query in queries:
        params = {
            "q": query,
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "forms": "8-K",
            "_source": "period_of_report,entity_name,file_date,form_type,period_of_report",
            "hits.hits.total.relation": "eq",
            "hits.hits._source.period_of_report": True,
        }

        try:
            url = "https://efts.sec.gov/LATEST/search-index"
            # Use standard EDGAR search API
            search_url = "https://efts.sec.gov/LATEST/search-index"

            # Try the EDGAR full-text search
            api_url = f"https://efts.sec.gov/LATEST/search-index?q={requests.utils.quote(query)}&dateRange=custom&startdt={start_date}&enddt={end_date}&forms=8-K"

            resp = requests.get(api_url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"  HTTP {resp.status_code} for query: {query[:50]}...")
                continue

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            print(f"  Found {len(hits)} hits for: {query[:60]}...")

            for hit in hits:
                src = hit.get("_source", {})
                accession = hit.get("_id", "")
                if accession in seen_accessions:
                    continue
                seen_accessions.add(accession)

                results.append({
                    "company": src.get("entity_name", ""),
                    "cik": src.get("ciks", [""])[0] if src.get("ciks") else "",
                    "filing_date": src.get("file_date", ""),
                    "accession": accession,
                    "form_type": src.get("form_type", "8-K"),
                })

            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            print(f"  Error: {e}")
            continue

    print(f"  Total unique filings: {len(results)}")
    return results


def get_ticker_for_cik(cik: str) -> str | None:
    """Get stock ticker from CIK via EDGAR submissions API."""
    try:
        # CIK must be 10 digits, zero-padded
        cik_padded = str(cik).zfill(10)
        url = f"{EDGAR_SUBMISSIONS_URL}/CIK{cik_padded}.json"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            tickers = data.get("tickers", [])
            if tickers:
                return tickers[0]
    except Exception:
        pass
    return None


def filter_by_market_cap(events: list, min_cap_m: float = 2000) -> list:
    """Filter events to large-cap companies only."""
    print(f"Filtering to market cap >= ${min_cap_m:.0f}M...")
    filtered = []
    cache = {}

    for ev in events:
        ticker = ev.get("ticker")
        if not ticker:
            continue

        cap = get_market_cap(ticker, cache)
        if cap and cap >= min_cap_m:
            ev["market_cap_m"] = cap
            filtered.append(ev)

    print(f"  Kept {len(filtered)} / {len(events)} events with cap >= ${min_cap_m:.0f}M")
    return filtered


def check_stock_direction(events: list) -> list:
    """
    For each event, check if the stock FELL on the day after announcement.
    This filters to "performance failure" type departures (not relief rallies).
    """
    import yfinance as yf

    print("Checking stock direction on departure announcement day...")
    qualified = []

    for ev in events:
        ticker = ev.get("ticker")
        date_str = ev.get("filing_date")
        if not ticker or not date_str:
            continue

        try:
            date = datetime.strptime(date_str[:10], "%Y-%m-%d")
            # Get price data around the event
            start = (date - timedelta(days=5)).strftime("%Y-%m-%d")
            end = (date + timedelta(days=5)).strftime("%Y-%m-%d")

            hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if hist.empty:
                continue

            hist.index = hist.index.tz_localize(None)
            hist.index = hist.index.normalize()

            # Find the next trading day after announcement
            event_dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            next_days = hist[hist.index > event_dt]
            prev_close = hist[hist.index <= event_dt]

            if next_days.empty or prev_close.empty:
                continue

            day_after_open = float(next_days.iloc[0]["Open"])
            day_after_close = float(next_days.iloc[0]["Close"])
            prev_close_price = float(prev_close.iloc[-1]["Close"])

            # 1-day return from open to close (intraday)
            intraday_return = (day_after_close - day_after_open) / day_after_open
            # Return from pre-event close to next-day close (overnight + intraday)
            overnight_return = (day_after_close - prev_close_price) / prev_close_price

            ev["day_after_open"] = day_after_open
            ev["day_after_close"] = day_after_close
            ev["prev_close"] = prev_close_price
            ev["intraday_return_pct"] = intraday_return * 100
            ev["overnight_return_pct"] = overnight_return * 100

            # Flag as "fell" if overnight return is negative
            ev["stock_fell"] = overnight_return < -0.01  # More than 1% decline
            qualified.append(ev)

            direction = "FELL" if ev["stock_fell"] else "ROSE"
            print(f"  {ticker} {date_str}: {direction} ({overnight_return*100:.1f}% overnight)")

            time.sleep(0.2)

        except Exception as e:
            print(f"  {ticker} {date_str}: error — {e}")
            continue

    fell_events = [e for e in qualified if e.get("stock_fell")]
    print(f"  {len(fell_events)} / {len(qualified)} events where stock fell")
    return qualified


def scan_ceo_departures(
    start_date: str = "2019-01-01",
    end_date: str = "2024-12-31",
    min_cap_m: float = 2000,
    check_direction: bool = False,
    output_file: str = None
) -> list:
    """
    Main scanner: find large-cap CEO departures where stock fell.

    Steps:
    1. Search EDGAR for 8-K Item 5.02 filings
    2. Get ticker for each CIK
    3. Filter to large-cap companies
    4. Optionally check if stock fell on announcement
    """

    # Step 1: Search EDGAR
    raw_results = search_8k_item502(start_date, end_date)

    # Step 2: Get tickers
    print("Resolving tickers from CIK...")
    ticker_events = []
    for ev in raw_results[:200]:  # Limit to prevent too many API calls
        if ev.get("cik"):
            ticker = get_ticker_for_cik(ev["cik"])
            if ticker:
                ev["ticker"] = ticker.upper()
                ticker_events.append(ev)
                time.sleep(0.1)

    print(f"  Resolved {len(ticker_events)} tickers")

    # Step 3: Filter by market cap
    if ticker_events:
        large_cap_events = filter_by_market_cap(ticker_events, min_cap_m)
    else:
        large_cap_events = []

    # Step 4: Check stock direction (optional)
    if check_direction and large_cap_events:
        large_cap_events = check_stock_direction(large_cap_events)

    # Output
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(large_cap_events, f, indent=2, default=str)
        print(f"Saved {len(large_cap_events)} events to {output_file}")

    return large_cap_events


def print_candidate_events(events: list, filter_fell: bool = True):
    """Pretty-print candidate events for manual review."""
    if filter_fell:
        events = [e for e in events if e.get("stock_fell", True)]

    print(f"\n{'='*70}")
    print(f"CEO Departure Candidates (filter_fell={filter_fell}): {len(events)} events")
    print(f"{'='*70}")
    print(f"{'Company':<30} {'Ticker':<8} {'Date':<12} {'Return':<10} {'Cap ($M)'}")
    print("-" * 70)

    for ev in sorted(events, key=lambda x: x.get("filing_date", "")):
        company = ev.get("company", "")[:28]
        ticker = ev.get("ticker", "")
        date = ev.get("filing_date", "")[:10]
        ret = ev.get("overnight_return_pct", float("nan"))
        cap = ev.get("market_cap_m", float("nan"))

        ret_str = f"{ret:.1f}%" if isinstance(ret, float) and not (ret != ret) else "?"
        cap_str = f"${cap:.0f}M" if isinstance(cap, float) and not (cap != cap) else "?"

        print(f"{company:<30} {ticker:<8} {date:<12} {ret_str:<10} {cap_str}")


# Known confirmed events (from manual research) for cross-reference
CONFIRMED_PERFORMANCE_FAILURE_EVENTS = [
    # Discovery period (2019-2022)
    {"symbol": "UAA",  "date": "2022-05-18", "ceo": "Patrik Frisk"},
    {"symbol": "MCD",  "date": "2019-11-03", "ceo": "Steve Easterbrook"},
    {"symbol": "VFC",  "date": "2022-12-13", "ceo": "Steve Rendle"},
    # Validation period (2023-2024)
    {"symbol": "WBA",  "date": "2023-09-01", "ceo": "Rosalind Brewer"},
    {"symbol": "INTC", "date": "2024-12-02", "ceo": "Pat Gelsinger"},
    {"symbol": "CVS",  "date": "2024-10-18", "ceo": "Karen Lynch"},
    {"symbol": "STLA", "date": "2024-12-02", "ceo": "Carlos Tavares"},
    {"symbol": "PARA", "date": "2024-04-29", "ceo": "Bob Bakish"},
    {"symbol": "AAP",  "date": "2023-08-24", "ceo": "Tom Greco"},
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan EDGAR for CEO departure events")
    parser.add_argument("--start", default="2019-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--min-cap", type=float, default=2000, help="Min market cap in millions")
    parser.add_argument("--check-direction", action="store_true", help="Check stock direction")
    parser.add_argument("--output", help="Output JSON file path")
    parser.add_argument("--show-confirmed", action="store_true", help="Show confirmed events")
    args = parser.parse_args()

    if args.show_confirmed:
        print("Confirmed performance failure departure events (N=9):")
        for ev in CONFIRMED_PERFORMANCE_FAILURE_EVENTS:
            print(f"  {ev['symbol']:<6} {ev['date']}  {ev['ceo']}")
        print()

    events = scan_ceo_departures(
        start_date=args.start,
        end_date=args.end,
        min_cap_m=args.min_cap,
        check_direction=args.check_direction,
        output_file=args.output
    )

    if events:
        print_candidate_events(events)
