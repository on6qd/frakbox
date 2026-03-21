"""
EDGAR Clinical Failure Scanner
================================
Scans SEC EDGAR for recent 8-K filings mentioning Phase 2/3 clinical failure
or FDA CRL events. Run at session start to catch qualifying d302c84b events.

Usage:
  python tools/edgar_clinical_failure_scanner.py
  python tools/edgar_clinical_failure_scanner.py --days 7   # last 7 days
  python tools/edgar_clinical_failure_scanner.py --days 30  # last 30 days

Returns potential qualifying events for hypothesis d302c84b.
"""

import re
import requests
import json
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent.parent))

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
HEADERS = {"User-Agent": "financial-researcher-bot contact@example.com"}

# Keywords that suggest primary endpoint failure
FAILURE_QUERIES = [
    '"did not meet" "primary endpoint"',
    '"primary endpoint was not met"',
    '"failed to meet" "primary endpoint"',
    '"complete response letter" clinical',
    '"clinical" "complete response letter" efficacy',
]

# Keywords that suggest continuation (not failure)
EXCLUSION_KEYWORDS = [
    "manufacturing", "CMC", "facility inspection", "chemistry, manufacturing",
    "met its primary endpoint", "achieved its primary endpoint",
    "manufacturing deficiency"
]

def search_edgar(query: str, start_date: str, end_date: str) -> list:
    """Search EDGAR full-text for 8-K filings matching query."""
    params = {
        "q": query,
        "forms": "8-K",
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
    }
    try:
        resp = requests.get(EDGAR_SEARCH_URL, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("hits", {}).get("hits", [])
        else:
            print(f"[EDGAR] HTTP {resp.status_code} for query: {query[:50]}")
            return []
    except Exception as e:
        print(f"[EDGAR] Error: {e}")
        return []

def extract_ticker_and_company(hit: dict) -> tuple:
    """Extract ticker and company from EDGAR hit display_names field."""
    src = hit.get("_source", {})
    display_names = src.get("display_names", [])
    if display_names:
        name_str = display_names[0]
        # Format: "Company Name  (TICKER)  (CIK 0001234567)"
        ticker_match = re.search(r'\(([A-Z]{1,5})\)', name_str)
        ticker = ticker_match.group(1) if ticker_match else "?"
        company = name_str.split("(")[0].strip()
        return ticker, company
    return "?", "Unknown"

def is_likely_exclusion(hit: dict) -> bool:
    """Check if the hit looks like a CMC/manufacturing CRL rather than clinical."""
    src = hit.get("_source", {})
    # Items 8.01 = material events; 7.01 = reg FD. Manufacturing CRLs often appear in 8.01 too.
    # We can't easily get the text snippet here, so just return False for now.
    return False

def format_result(hit: dict) -> dict:
    """Format a raw EDGAR hit into a structured result."""
    src = hit.get("_source", {})
    ticker, company = extract_ticker_and_company(hit)
    cik = src.get("ciks", ["?"])[0] if src.get("ciks") else "?"
    adsh = src.get("adsh", "?")
    return {
        "ticker": ticker,
        "company": company,
        "filing_date": src.get("file_date", "?")[:10],
        "accession": adsh,
        "cik": cik.lstrip("0"),
        "items": src.get("items", []),
    }

def main():
    parser = argparse.ArgumentParser(description="Scan EDGAR for clinical failure events")
    parser.add_argument("--days", type=int, default=14, help="Look back N days (default: 14)")
    args = parser.parse_args()

    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")
    
    print(f"=== EDGAR Clinical Failure Scanner ===")
    print(f"Search window: {start_date} to {end_date}")
    print()

    all_hits = {}
    for query in FAILURE_QUERIES:
        hits = search_edgar(query, start_date, end_date)
        for hit in hits:
            src = hit.get("_source", {})
            acc = src.get("adsh", hit.get("_id", "?"))
            if acc not in all_hits:
                all_hits[acc] = hit

    if not all_hits:
        print("No matching 8-K filings found.")
        return

    results = []
    for acc, hit in all_hits.items():
        result = format_result(hit)
        results.append(result)

    # Sort by filing date descending
    results.sort(key=lambda x: x["filing_date"], reverse=True)

    print(f"Found {len(results)} potential clinical failure 8-K filings:\n")
    print(f"{'Ticker':<8} {'Company':<40} {'Date':<12} {'Items'}")
    print("-" * 80)
    for r in results:
        ticker = r["ticker"]
        company = r["company"][:38]
        date = r["filing_date"]
        items = ",".join(r.get("items", []))
        print(f"{ticker:<8} {company:<40} {date:<12} {items}")

    print()
    print("Next steps for each candidate:")
    print("1. Check if company is single-pipeline small-cap (<$500M market cap)")
    print("2. Check if stock crashed >55% on the announcement date")
    print("3. Check if crash was not pre-anticipated (stock not down >20% in prior 10 days)")
    print("4. Check if post-crash price > $0.50/share")
    print("5. Verify company has NO stated path forward for the failed drug")
    print()
    print("Use tools/verify_event_date.py to find exact crash date if needed.")
    
    return results

if __name__ == "__main__":
    main()
