"""
EDGAR 8-K CEO/CFO Departure Scanner

Scans EDGAR for 8-K filings mentioning Item 5.02 (departure of principal officers)
to find CEO and CFO sudden departures from large-cap companies.

Target signal: CEO performance failure departure → SHORT next-day open, 1d hold
FORMAL HYPOTHESIS: 5dbcfb37
N=12, avg_abnormal_1d=-3.25%, p=0.0034, CI=[-5.16, -1.52] excludes zero
Discovery (2019-2022): 4 events, 100% negative
Validation (2023-2024): 8 events, 75% negative (2 near-zero positives: CVS, STLA)

Key exclusion: relief-rally departures (PTON, VSCO, LYFT, SBUX) — stock RISES, do NOT short

Usage:
    python tools/edgar_ceo_departure_scanner.py --start 2025-01-01 --end 2025-12-31 --min-cap 2000
    python tools/edgar_ceo_departure_scanner.py --start 2025-01-01 --end 2025-12-31 --check-direction
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


def fetch_8k_text(cik: str, accession: str) -> str | None:
    """
    Fetch the text content of an 8-K filing from EDGAR.
    Returns the plain text content, or None if unavailable.
    """
    try:
        cik_clean = str(cik).lstrip("0")
        acc_nodash = accession.replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_nodash}/{accession}-index.htm"
        resp = requests.get(index_url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        # Find 8-K document link
        import re
        # Look for links that contain the filing document (not exhibits)
        links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.htm)"', resp.text, re.I)
        # Prioritize the main 8-K document (not ex10, ex99)
        main_links = [l for l in links if not any(ex in l.lower() for ex in ['ex10', 'ex99', 'ex-10', 'ex-99'])]
        doc_link = main_links[0] if main_links else (links[0] if links else None)
        if not doc_link:
            return None

        doc_url = "https://www.sec.gov" + doc_link
        doc_resp = requests.get(doc_url, headers=HEADERS, timeout=15)
        if doc_resp.status_code != 200:
            return None

        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', doc_resp.text)
        text = re.sub(r'&[a-z#0-9]+;', ' ', text)  # Remove HTML entities
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:10000]  # Limit to first 10k chars
    except Exception:
        return None


def classify_departure_type(filing_text: str) -> str:
    """
    Classify CEO departure as 'planned', 'performance_failure', or 'unknown'
    based on 8-K text analysis.

    Returns:
        'planned' - retirement, voluntary transition, succession planning
        'performance_failure' - terminated for cause, mutual agreement, abrupt/unexplained
        'unknown' - insufficient text to classify
    """
    if not filing_text:
        return "unknown"

    text_lower = filing_text.lower()

    # Planned departure signals (EXCLUDE from trading signal)
    planned_signals = [
        "retire", "retirement", "transition", "succession",
        "planned", "effective september", "effective october", "effective november",
        "effective december", "effective january", "effective february",
        "step down and transition", "end of the year", "end of his tenure",
        "after serving", "years of service", "years of leadership",
        "to pursue other opportunities",  # could be either
    ]

    # Performance failure signals (INCLUDE for trading signal)
    performance_signals = [
        "effective immediately", "immediately",
        "mutual agreement", "separation agreement",
        "transition agreement",  # sometimes used for polite firings
        "resigned effective",  # not announced in advance
        "notified the company of his resignation",
        "notified the company of her resignation",
        "unexpectedly",
    ]

    # Relief rally exclusions (from known hypothesis exclusion list)
    relief_exclusions = [
        # Companies where CEO departure caused relief rally - don't short these
    ]

    planned_score = sum(1 for sig in planned_signals if sig in text_lower)
    performance_score = sum(1 for sig in performance_signals if sig in text_lower)

    # Strong planned signals override
    if any(sig in text_lower for sig in ["retire", "retirement", "years of service", "planned succession"]):
        return "planned"

    # "Effective immediately" with no retirement language = performance
    if "effective immediately" in text_lower and "retire" not in text_lower:
        return "performance_failure"

    if performance_score > planned_score:
        return "performance_failure"
    elif planned_score > performance_score:
        return "planned"
    else:
        return "unknown"


def filter_by_departure_type(events: list, cik_map: dict = None) -> list:
    """
    Filter events to only include performance failure departures
    (not planned retirements or succession transitions).

    Fetches 8-K text for each event and classifies departure type.
    """
    print("Classifying departure types from 8-K text...")
    qualified = []

    for ev in events:
        cik = ev.get("cik")
        accession = ev.get("accession", "")
        ticker = ev.get("ticker", "?")

        if not cik or not accession:
            ev["departure_type"] = "unknown"
            qualified.append(ev)
            continue

        # Extract accession from compound ID (e.g., "0001234567-26-000001:filename.htm")
        pure_accession = accession.split(":")[0] if ":" in accession else accession

        text = fetch_8k_text(cik, pure_accession)
        departure_type = classify_departure_type(text)
        ev["departure_type"] = departure_type

        if departure_type == "planned":
            print(f"  {ticker}: SKIP (planned retirement/succession)")
        elif departure_type == "performance_failure":
            print(f"  {ticker}: KEEP (performance failure)")
            qualified.append(ev)
        else:
            print(f"  {ticker}: KEEP (unknown — manual review needed)")
            qualified.append(ev)  # Keep unknowns for manual review

        time.sleep(0.3)  # Rate limiting

    filtered_out = len(events) - len(qualified)
    print(f"  Removed {filtered_out} planned departures, kept {len(qualified)} for further review")
    return qualified


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
            import pandas as pd
            date = datetime.strptime(date_str[:10], "%Y-%m-%d")
            # Get price data around the event
            start = (date - timedelta(days=5)).strftime("%Y-%m-%d")
            end = (date + timedelta(days=5)).strftime("%Y-%m-%d")

            hist = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if hist.empty:
                continue

            # Flatten MultiIndex columns from newer yfinance (e.g. ('Open','AAPL') -> 'Open')
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            hist.index = hist.index.tz_localize(None)
            hist.index = hist.index.normalize()

            # Find the next trading day after announcement
            event_dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            next_days = hist[hist.index > event_dt]
            prev_close = hist[hist.index <= event_dt]

            if next_days.empty or prev_close.empty:
                continue

            day_after_open = float(next_days["Open"].iloc[0])
            day_after_close = float(next_days["Close"].iloc[0])
            prev_close_price = float(prev_close["Close"].iloc[-1])

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
    filter_departure_type: bool = True,
    output_file: str = None
) -> list:
    """
    Main scanner: find large-cap CEO departures where stock fell.

    Steps:
    1. Search EDGAR for 8-K Item 5.02 filings
    2. Get ticker for each CIK
    3. Filter to large-cap companies
    4. Filter out planned retirements (fetch 8-K text and classify)
    5. Optionally check if stock fell on announcement

    Args:
        filter_departure_type: If True (default), fetch 8-K text and remove planned
            retirements/succession events. Only keeps performance failures and unknowns.
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

    # Step 2b: Deduplicate by (ticker, filing_date[:10]) — EDGAR often returns
    # multiple documents per event (main 8-K + amendment/exhibit)
    seen_ticker_dates = set()
    deduped = []
    for ev in ticker_events:
        key = (ev.get("ticker", ""), ev.get("filing_date", "")[:10])
        if key not in seen_ticker_dates:
            seen_ticker_dates.add(key)
            deduped.append(ev)
    if len(deduped) < len(ticker_events):
        print(f"  Deduped {len(ticker_events)} -> {len(deduped)} events (same ticker+date)")
    ticker_events = deduped

    # Step 3: Filter by market cap
    if ticker_events:
        large_cap_events = filter_by_market_cap(ticker_events, min_cap_m)
    else:
        large_cap_events = []

    # Step 3b: Filter out planned retirements by fetching 8-K text
    if filter_departure_type and large_cap_events:
        large_cap_events = filter_by_departure_type(large_cap_events)

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


# Known confirmed events (N=12 formal hypothesis 5dbcfb37) for cross-reference
# Discovery (2019-2022): 4 events, 100% negative 1d
# Validation (2023-2024): 8 events, 75% negative 1d
# Overall: avg abnormal 1d = -3.25%, p=0.0034, CI excludes zero
# Entry: NEXT-DAY OPEN (not announcement day close)
CONFIRMED_PERFORMANCE_FAILURE_EVENTS = [
    # Discovery period (2019-2022)
    {"symbol": "MCD",  "date": "2019-11-04", "ceo": "Steve Easterbrook", "abnormal_1d": -1.20},
    {"symbol": "UAA",  "date": "2022-05-19", "ceo": "Patrik Frisk",      "abnormal_1d": -6.22},
    {"symbol": "DOCU", "date": "2022-06-14", "ceo": "Dan Springer",       "abnormal_1d": -4.20},
    {"symbol": "VFC",  "date": "2022-12-14", "ceo": "Steve Rendle",       "abnormal_1d": -0.64},
    # Validation period (2023-2024)
    {"symbol": "WBA",  "date": "2023-09-01", "ceo": "Rosalind Brewer",   "abnormal_1d": -7.09},
    {"symbol": "AAP",  "date": "2023-08-24", "ceo": "Tom Greco",         "abnormal_1d": -0.98},
    {"symbol": "PARA", "date": "2024-04-30", "ceo": "Bob Bakish",        "abnormal_1d": -4.33},
    {"symbol": "CVS",  "date": "2024-10-18", "ceo": "Karen Lynch",       "abnormal_1d": +0.95},  # miss
    {"symbol": "STLA", "date": "2024-12-02", "ceo": "Carlos Tavares",    "abnormal_1d": +0.62},  # miss
    {"symbol": "INTC", "date": "2024-12-02", "ceo": "Pat Gelsinger",     "abnormal_1d": -3.84},
    {"symbol": "FIVE", "date": "2024-07-17", "ceo": "Joel Anderson",     "abnormal_1d": -9.97},
    {"symbol": "BA",   "date": "2024-03-25", "ceo": "Dave Calhoun",      "abnormal_1d": -2.07},
]

# Relief-rally departures to EXCLUDE (new CEO welcomed = stock RISES)
# Do NOT short these patterns
RELIEF_RALLY_DEPARTURES = [
    {"symbol": "PTON", "date": "2022-02-08", "ceo": "John Foley",       "abnormal_1d": +22.59},
    {"symbol": "VSCO", "date": "2024-10-11", "ceo": "Martin Waters",    "abnormal_1d":  +3.23},
    {"symbol": "DIS",  "date": "2022-11-21", "ceo": "Bob Chapek",       "abnormal_1d":  -2.18},  # Iger return
    {"symbol": "LYFT", "date": "2023-04-13", "ceo": "Logan Green",      "abnormal_1d":  +3.46},
    {"symbol": "SBUX", "date": "2024-09-10", "ceo": "Laxman Narasimhan","abnormal_1d":  +3.51},
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
