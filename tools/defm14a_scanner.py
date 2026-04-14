#!/usr/bin/env python3
"""
DEFM14A Proxy Statement Scanner
================================
Scans SEC EDGAR EFTS for DEFM14A (definitive merger proxy statement) filings.

When a company files a DEFM14A, it signals an imminent shareholder vote on a
merger deal. The hypothesis is that the spread between the target's current price
and the deal price compresses in the 5-10 days following the filing, because the
proxy mailing triggers institutional risk reduction and the vote date becomes
concrete.

Data source: EDGAR EFTS full-text search API
    https://efts.sec.gov/LATEST/search-index?forms=DEFM14A&dateRange=custom&startdt=...&enddt=...

Usage:
    # Scan 2024-2025 DEFM14A filings
    python3 tools/defm14a_scanner.py --start-date 2024-01-01 --end-date 2025-12-31 --limit 50

    # Full JSON output
    python3 tools/defm14a_scanner.py --start-date 2024-01-01 --end-date 2025-12-31 --json

    # Programmatic use
    from tools.defm14a_scanner import scan_defm14a_filings
    events = scan_defm14a_filings(start_date="2024-01-01", end_date="2025-12-31")

Design notes:
- EDGAR EFTS returns up to 100 hits per page; this tool paginates with the `from` param.
- Tickers are extracted from EDGAR display_names via regex (same pattern as merger_catalyst_scraper).
- When display_names lacks a ticker, we fall back to the EDGAR submissions API (CIK -> ticker).
- Results are deduped by (ticker, filing_date) to collapse duplicate filings.
"""

import re
import sys
import time
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions"

EDGAR_HEADERS = {
    "User-Agent": "Frakbox Research bart.de.lepeleer@gmail.com",
    "Accept": "application/json",
}

PAGE_SIZE = 100         # EDGAR EFTS returns up to 100 hits per page
MAX_PAGES = 50          # safety guard: max pages (5000 results)
RATE_LIMIT_SECS = 0.15  # 150ms between EDGAR requests (~6.6 req/s, under 10/s limit)

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EDGAR helpers
# ---------------------------------------------------------------------------

def _build_search_url(start_date: str, end_date: str, offset: int = 0) -> str:
    """Build an EDGAR EFTS search URL for DEFM14A filings."""
    return (
        f"{EDGAR_SEARCH_URL}"
        f"?forms=DEFM14A"
        f"&dateRange=custom"
        f"&startdt={start_date}"
        f"&enddt={end_date}"
        f"&from={offset}"
    )


def _fetch_page(start_date: str, end_date: str, offset: int = 0, retries: int = 2) -> dict:
    """Fetch a single page of EDGAR EFTS results. Returns parsed JSON or raises.

    EDGAR EFTS sometimes returns 500 at high offsets; we retry once before giving up.
    """
    url = _build_search_url(start_date, end_date, offset)
    for attempt in range(retries + 1):
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
        if resp.status_code == 500 and attempt < retries:
            time.sleep(1.0)  # back off before retry
            continue
        resp.raise_for_status()
        return resp.json()
    # Should not reach here, but just in case
    resp.raise_for_status()
    return resp.json()


def _extract_ticker(display_name: str) -> Optional[str]:
    """
    Extract the primary ticker symbol from an EDGAR display_names entry.

    EDGAR display_names formats:
      - "Acme Corp  (ACME)  (CIK 0001234567)"           -> ACME
      - "Acme Corp  (CIK 0001234567)"                   -> None
      - "Acme Corp  (GLP, GLP-PB)  (CIK 0001323468)"    -> GLP (first of multi-class)

    Strategy:
      1. Find all parenthetical groups.
      2. Skip any group starting with "CIK".
      3. Return the first token that looks like a ticker (1-5 uppercase letters).
    """
    parens = re.findall(r'\(([^)]+)\)', display_name)
    for group in parens:
        group = group.strip()
        if group.upper().startswith("CIK"):
            continue
        tokens = [t.strip() for t in group.split(",")]
        for token in tokens:
            if re.fullmatch(r'[A-Z]{1,5}', token):
                return token
    return None


def _get_ticker_for_cik(cik: str) -> Optional[str]:
    """
    Resolve a stock ticker from CIK via the EDGAR submissions API.

    Falls back to None if the CIK has no associated tickers (e.g., private companies,
    SPACs that have de-listed, or foreign filers).
    """
    try:
        cik_padded = str(cik).zfill(10)
        url = f"{EDGAR_SUBMISSIONS_URL}/CIK{cik_padded}.json"
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            tickers = data.get("tickers", [])
            if tickers:
                return tickers[0].upper()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Core scanner
# ---------------------------------------------------------------------------

def _parse_hits(hits: list) -> list[dict]:
    """
    Parse raw EDGAR EFTS hit list into structured filing dicts.

    Each hit's _source contains:
      - display_names: list of company name strings with ticker/CIK
      - ciks: list of CIK strings
      - file_date: filing date string (YYYY-MM-DD)
      - root_forms: list of form types
      - file_num: filing number
    """
    results = []
    for hit in hits:
        src = hit.get("_source", {})
        file_date = src.get("file_date", "")[:10]
        if not file_date:
            continue

        display_names = src.get("display_names", [])
        ciks = src.get("ciks", [])
        accession = hit.get("_id", "")

        # Extract company name and ticker from the first display_name
        company = ""
        ticker = None
        if display_names:
            company = display_names[0].split("(")[0].strip()
            ticker = _extract_ticker(display_names[0])

        # Get CIK (first one, strip leading zeros for readability)
        cik = ciks[0].lstrip("0") if ciks else ""

        results.append({
            "filing_date": file_date,
            "company": company,
            "cik": cik,
            "ticker": ticker,
            "accession": accession,
            "display_name_raw": display_names[0] if display_names else "",
        })

    return results


def scan_defm14a_filings(
    start_date: str = "2024-01-01",
    end_date: str = "2025-12-31",
    resolve_missing_tickers: bool = True,
    limit: Optional[int] = None,
    verbose: bool = True,
) -> list[dict]:
    """
    Scan EDGAR EFTS for DEFM14A filings in the given date range.

    Parameters
    ----------
    start_date : str
        ISO date string, inclusive lower bound for filing date.
    end_date : str
        ISO date string, inclusive upper bound for filing date.
    resolve_missing_tickers : bool
        If True, attempt to resolve tickers from CIK via EDGAR submissions API
        for filings where display_names doesn't include a ticker.
    limit : int or None
        Max results to return (None = all).
    verbose : bool
        Print progress to stderr.

    Returns
    -------
    list of dicts, each with keys:
        filing_date, company, cik, ticker, accession, display_name_raw
    """
    all_results: list[dict] = []
    offset = 0

    if verbose:
        print(f"Searching EDGAR EFTS for DEFM14A filings...", file=sys.stderr)
        print(f"Date range: {start_date} to {end_date}", file=sys.stderr)

    for page_num in range(MAX_PAGES):
        try:
            data = _fetch_page(start_date, end_date, offset)
        except requests.HTTPError as e:
            logger.warning(f"EDGAR HTTP error at offset={offset}: {e}")
            break
        except requests.RequestException as e:
            logger.warning(f"EDGAR request error at offset={offset}: {e}")
            break

        hits_wrapper = data.get("hits", {})
        hits = hits_wrapper.get("hits", [])
        if not hits:
            break

        page_results = _parse_hits(hits)
        all_results.extend(page_results)

        # Check total for pagination
        total_info = hits_wrapper.get("total", {})
        if isinstance(total_info, dict):
            total_value = total_info.get("value", 0)
        else:
            total_value = int(total_info or 0)

        if verbose and page_num == 0:
            print(f"Total DEFM14A filings found: {total_value}", file=sys.stderr)

        offset += PAGE_SIZE
        if offset >= total_value:
            break

        time.sleep(RATE_LIMIT_SECS)

    if verbose:
        print(f"Fetched {len(all_results)} raw filing records", file=sys.stderr)

    # -----------------------------------------------------------------------
    # Resolve missing tickers via EDGAR submissions API
    # -----------------------------------------------------------------------
    if resolve_missing_tickers:
        missing_ticker_count = sum(1 for r in all_results if r["ticker"] is None)
        if missing_ticker_count > 0 and verbose:
            print(
                f"Resolving tickers for {missing_ticker_count} filings via EDGAR submissions API...",
                file=sys.stderr,
            )

        resolved = 0
        for r in all_results:
            if r["ticker"] is None and r["cik"]:
                ticker = _get_ticker_for_cik(r["cik"])
                if ticker:
                    r["ticker"] = ticker
                    resolved += 1
                time.sleep(RATE_LIMIT_SECS)

        if verbose and resolved > 0:
            print(f"  Resolved {resolved} additional tickers", file=sys.stderr)

    # -----------------------------------------------------------------------
    # Deduplicate by (ticker, filing_date)
    # -----------------------------------------------------------------------
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for r in all_results:
        key = (r.get("ticker") or r.get("cik", ""), r["filing_date"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    if len(deduped) < len(all_results) and verbose:
        print(
            f"Deduped {len(all_results)} -> {len(deduped)} filings (same ticker+date)",
            file=sys.stderr,
        )
    all_results = deduped

    # Sort by filing date descending (most recent first)
    all_results.sort(key=lambda x: x["filing_date"], reverse=True)

    # Apply limit
    if limit is not None:
        all_results = all_results[:limit]

    return all_results


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def to_event_impact_format(events: list[dict]) -> list[dict]:
    """
    Convert scanner output to the list-of-dicts format expected by
    market_data.measure_event_impact().

    Only includes events with a resolved ticker.

    Returns list of dicts with keys: symbol, date, timing
    """
    return [
        {
            "symbol": ev["ticker"],
            "date": ev["filing_date"],
            "timing": "market_hours",
        }
        for ev in events
        if ev.get("ticker")
    ]


def print_table(events: list[dict], limit: int = 50):
    """Pretty-print events as a table to stdout."""
    display = events[:limit]
    print(f"\nDEFM14A Filings ({len(events)} total, showing {len(display)}):")
    print()
    header = f"{'#':<4} {'Date':<12} {'Ticker':<8} {'CIK':<12} {'Company'}"
    print(header)
    print("-" * 90)
    for i, ev in enumerate(display, 1):
        ticker = ev.get("ticker") or "?"
        cik = ev.get("cik", "")
        company = ev.get("company", "")[:50]
        print(f"{i:<4} {ev['filing_date']:<12} {ticker:<8} {cik:<12} {company}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(
        description=(
            "Scan EDGAR EFTS for DEFM14A (merger proxy statement) filings. "
            "DEFM14A filings signal an imminent shareholder vote on a merger deal."
        )
    )
    parser.add_argument(
        "--start-date",
        default="2024-01-01",
        help="Start date YYYY-MM-DD (default: 2024-01-01)",
    )
    parser.add_argument(
        "--end-date",
        default="2025-12-31",
        help="End date YYYY-MM-DD (default: 2025-12-31)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max results to return (default: 50)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output full JSON instead of a table",
    )
    parser.add_argument(
        "--no-resolve",
        action="store_true",
        help="Skip ticker resolution via EDGAR submissions API (faster but may have missing tickers)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    events = scan_defm14a_filings(
        start_date=args.start_date,
        end_date=args.end_date,
        resolve_missing_tickers=not args.no_resolve,
        limit=args.limit,
        verbose=True,
    )

    if args.json:
        print(json.dumps(events, indent=2))
    else:
        print_table(events, limit=args.limit)

        # Summary stats
        with_ticker = sum(1 for e in events if e.get("ticker"))
        without_ticker = sum(1 for e in events if not e.get("ticker"))
        print()
        print(f"With ticker: {with_ticker}  |  Without ticker: {without_ticker}")

        if events:
            print(f"\nTo use with measure_event_impact():")
            print(f"  from tools.defm14a_scanner import scan_defm14a_filings, to_event_impact_format")
            print(f"  events = scan_defm14a_filings(start_date='{args.start_date}', end_date='{args.end_date}')")
            print(f"  impact_events = to_event_impact_format(events)")

    # Also output the JSON summary to stdout when not in --json mode
    # for programmatic consumption
    if not args.json:
        print(f"\n--- JSON Summary ---")
        summary = {
            "total_filings": len(events),
            "with_ticker": with_ticker,
            "without_ticker": without_ticker,
            "date_range": {"start": args.start_date, "end": args.end_date},
            "filings": events,
        }
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    _cli()
