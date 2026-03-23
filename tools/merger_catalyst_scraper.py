"""
merger_catalyst_scraper.py

Scrapes EDGAR EFTS for HSR early termination 8-K filings.
These signal antitrust clearance for pending M&A deals — the moment the FTC or DOJ
grants early termination of the HSR waiting period, a key binary risk on the merger
closing is resolved.

The target company's stock typically compresses toward the deal price when antitrust
risk is removed, so this can serve as a post-announcement drift / spread-compression
signal.

Usage:
    from tools.merger_catalyst_scraper import scrape_hsr_terminations

    events = scrape_hsr_terminations(start_date="2020-01-01", end_date="2024-12-31")
    # Returns list of dicts:
    # {
    #   "symbol": str,          # ticker of the filing company
    #   "date": str,            # 8-K filing date (YYYY-MM-DD)
    #   "company": str,         # full company display name
    #   "timing": str,          # always "market_hours" for 8-K filings
    #   "query": str,           # which search query matched
    #   "display_name_raw": str # raw EDGAR display_names entry for debugging
    # }

CLI usage:
    python tools/merger_catalyst_scraper.py --start 2022-01-01 --end 2022-06-30 --limit 10

Design notes:
- EDGAR EFTS returns up to 100 hits per page; this tool paginates with the `from` param.
- Both primary query ("early termination" "Hart-Scott-Rodino") and fallback query
  ("HSR waiting period" "terminated") are tried; results are deduped by (ticker, date).
- We filter out obvious non-targets using heuristics (see filter_likely_targets()).
- Tickers are extracted from EDGAR display_names via regex per CLAUDE.md convention.
"""

import re
import time
import json
import argparse
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_HEADERS = {"User-Agent": "financial-researcher-bot contact@example.com"}

# Primary and fallback EDGAR full-text queries (each must appear as phrase)
HSR_QUERIES = [
    '"early termination" "Hart-Scott-Rodino"',
    '"HSR waiting period" "terminated"',
    '"early termination" "HSR"',
]

PAGE_SIZE = 100         # EDGAR EFTS returns up to 100 hits per page
MAX_PAGES = 20          # safety guard: max pages per query (2000 results)
RATE_LIMIT_SECS = 0.5   # polite delay between EDGAR requests

# Tickers that appear in HSR filings but are almost always the acquirer or
# a financial sponsor — not a clean M&A target for spread compression.
# This is a best-effort heuristic; manual review is still recommended.
KNOWN_ACQUIRER_PATTERNS = [
    r"^BRK[AB]$",   # Berkshire
    r"^BX$",        # Blackstone
    r"^APO$",       # Apollo
    r"^KKR$",
    r"^CG$",        # Carlyle
    r"^TPR$",
    r"^TPG$",
]

# Tokens in company names that suggest the filer is an investment fund,
# SPAC shell, or acquirer vehicle rather than an operating M&A target.
ACQUIRER_NAME_TOKENS = [
    "merger sub",
    "merger corp",
    "holdings llc",
    "partners lp",
    "fund ",
    "capital llc",
    "capital lp",
    "ventures lp",
    "private equity",
    "investment fund",
    # SPAC / blank-check shells — these are de-SPAC filings, not traditional M&A
    "acquisition corp",
    "acquisition co",
    "acquisition inc",
    "spac",
    "blank check",
]

# Company name suffixes common in SPAC vehicles (applied as endswith checks after
# stripping the ticker/CIK parentheticals)
SPAC_SUFFIXES = [
    " acquisition corp",
    " acquisition co",
    " acquisition inc",
    " acquisition company",
    " acquisition lp",
    " acquisition llc",
]

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# EDGAR helpers
# ---------------------------------------------------------------------------

def _build_url(query: str, start_date: str, end_date: str, offset: int = 0) -> str:
    """Build an EDGAR EFTS search URL for 8-K filings."""
    import urllib.parse
    encoded_query = urllib.parse.quote(query)
    return (
        f"{EDGAR_SEARCH_URL}"
        f"?q={encoded_query}"
        f"&forms=8-K"
        f"&dateRange=custom"
        f"&startdt={start_date}"
        f"&enddt={end_date}"
        f"&from={offset}"
        f"&hits.hits.total.relation=eq"
    )


def _fetch_page(query: str, start_date: str, end_date: str, offset: int = 0) -> dict:
    """Fetch a single page of EDGAR EFTS results. Returns parsed JSON or raises."""
    url = _build_url(query, start_date, end_date, offset)
    resp = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_ticker(display_name: str) -> Optional[str]:
    """
    Extract the primary (common-stock) ticker symbol from an EDGAR display_names entry.

    EDGAR display_names can take several forms:
      - "Acme Corp  (ACME)  (CIK 0001234567)"           -> ACME
      - "Acme Corp  (CIK 0001234567)"                   -> None (no ticker listed)
      - "Acme Corp  (GLP, GLP-PB)  (CIK 0001323468)"    -> GLP  (first of multi-class)
      - "Acme Corp  (FHN, FHN-PB, FHN-PC)  (CIK ...)"  -> FHN  (first of multi-class)

    Strategy:
      1. Find all parenthetical groups.
      2. Skip any group that starts with "CIK" (the SEC entity ID).
      3. From the remaining groups, split on commas and return the first token that
         looks like a ticker: 1-5 uppercase letters (possibly followed by hyphen+class).
      4. Return None if nothing qualifies.
    """
    # Find all parenthetical contents
    parens = re.findall(r'\(([^)]+)\)', display_name)
    for group in parens:
        group = group.strip()
        # Skip CIK groups
        if group.upper().startswith("CIK"):
            continue
        # Split on commas (handles multi-class share structures)
        tokens = [t.strip() for t in group.split(",")]
        for token in tokens:
            # Accept plain ticker (ACME) or hyphenated preferred (GLP-PB)
            # We want the common share = first token, and only pure alpha tickers
            # for clean backtest compatibility (avoid GLP-PB which yfinance may not handle)
            if re.fullmatch(r'[A-Z]{1,5}', token):
                return token
    return None


def _parse_hits(hits: list, query_label: str) -> list[dict]:
    """
    Parse raw EDGAR hit list into structured event dicts.

    Each hit may have multiple display_names entries (target + filer).
    We take the FIRST entry that yields a ticker — per CLAUDE.md convention this
    is typically the target company for dual-party filings.
    """
    results = []
    for hit in hits:
        src = hit.get("_source", {})
        file_date = src.get("file_date", "")[:10]
        if not file_date:
            continue

        display_names = src.get("display_names", [])
        accession = src.get("file_num", "") or hit.get("_id", "")

        for dn in display_names:
            ticker = _extract_ticker(dn)
            if ticker:
                results.append({
                    "symbol": ticker,
                    "date": file_date,
                    "company": dn,
                    "timing": "market_hours",   # 8-K is filed during market hours
                    "query": query_label,
                    "display_name_raw": dn,
                    "accession": accession,
                })
                break   # first valid ticker per filing

    return results


# ---------------------------------------------------------------------------
# Core scraper
# ---------------------------------------------------------------------------

def _scrape_single_query(
    query: str,
    start_date: str,
    end_date: str,
    max_pages: int = MAX_PAGES,
) -> list[dict]:
    """
    Paginate through all EDGAR EFTS results for a single query + date range.
    Returns raw (un-deduped) list of event dicts.
    """
    all_results = []
    offset = 0

    for page_num in range(max_pages):
        try:
            data = _fetch_page(query, start_date, end_date, offset)
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

        page_results = _parse_hits(hits, query_label=query)
        all_results.extend(page_results)

        # Check if more pages remain
        total = hits_wrapper.get("total", {})
        if isinstance(total, dict):
            total_value = total.get("value", 0)
        else:
            total_value = int(total or 0)

        offset += PAGE_SIZE
        if offset >= total_value:
            break

        # Polite rate limiting
        time.sleep(RATE_LIMIT_SECS)

    return all_results


def scrape_hsr_terminations(
    start_date: str = "2021-01-01",
    end_date: str = "2024-12-31",
    dedupe: bool = True,
    filter_targets: bool = True,
    max_pages_per_query: int = MAX_PAGES,
) -> list[dict]:
    """
    Scrape EDGAR EFTS for HSR early termination 8-K filings.

    Parameters
    ----------
    start_date : str
        ISO date string, inclusive lower bound for filing date.
    end_date : str
        ISO date string, inclusive upper bound for filing date.
    dedupe : bool
        If True, deduplicate by (symbol, date) keeping the first occurrence.
    filter_targets : bool
        If True, apply heuristics to remove likely acquirers / funds.
    max_pages_per_query : int
        Safety cap on pagination per query (default 20 = 200 results).

    Returns
    -------
    list of dicts, each with keys:
        symbol, date, company, timing, query, display_name_raw, accession
    """
    all_events: list[dict] = []

    for query in HSR_QUERIES:
        logger.info(f"Querying EDGAR: {query!r} [{start_date} to {end_date}]")
        results = _scrape_single_query(query, start_date, end_date, max_pages_per_query)
        logger.info(f"  -> {len(results)} raw results")
        all_events.extend(results)
        time.sleep(RATE_LIMIT_SECS)

    # Deduplicate by (symbol, date) — same filing may match multiple queries
    if dedupe:
        seen: set[tuple] = set()
        deduped: list[dict] = []
        for ev in all_events:
            key = (ev["symbol"], ev["date"])
            if key not in seen:
                seen.add(key)
                deduped.append(ev)
        all_events = deduped

    # Filter to likely M&A targets
    if filter_targets:
        all_events = filter_likely_targets(all_events)

    # Sort by date ascending
    all_events.sort(key=lambda x: x["date"])

    return all_events


# ---------------------------------------------------------------------------
# Target filtering
# ---------------------------------------------------------------------------

def filter_likely_targets(events: list[dict]) -> list[dict]:
    """
    Heuristically remove filings that are probably NOT from an M&A target.

    Filters applied:
    1. Remove tickers matching known acquirer patterns (BRK, BX, KKR, etc.).
    2. Remove company names containing tokens that suggest an acquirer vehicle
       (e.g. "Merger Sub", "Acquisition LLC", "Partners LP").
    3. Remove single-letter or implausibly short tickers (< 2 chars) which are
       often CIK artifacts.

    This is intentionally conservative — false negatives (dropping real targets)
    are less harmful than false positives (polluting the backtest with acquirers),
    but when in doubt we keep the event and let the researcher decide.
    """
    filtered = []
    for ev in events:
        ticker = ev["symbol"]
        company_lower = ev["company"].lower()

        # Extract just the company name portion (before the first parenthetical)
        company_name_only = re.split(r'\s*\(', company_lower)[0].strip()

        # Skip implausibly short tickers
        if len(ticker) < 2:
            continue

        # Skip known acquirer tickers
        if any(re.match(p, ticker) for p in KNOWN_ACQUIRER_PATTERNS):
            continue

        # Skip company names with acquirer / SPAC tokens
        if any(tok in company_lower for tok in ACQUIRER_NAME_TOKENS):
            continue

        # Skip SPAC vehicles by name suffix
        if any(company_name_only.endswith(sfx) for sfx in SPAC_SUFFIXES):
            continue

        # Heuristic: roman numeral suffixes strongly suggest SPAC (e.g. "Holdings Corp. II")
        # company_name_only is already lowercased, so match lowercase roman numerals
        if re.search(r'\b(corp|inc|co|llc)[.\s]+[ivx]{1,4}$', company_name_only):
            continue

        filtered.append(ev)

    return filtered


# ---------------------------------------------------------------------------
# Utility: format for measure_event_impact()
# ---------------------------------------------------------------------------

def to_event_impact_format(events: list[dict]) -> list[dict]:
    """
    Convert scraper output to the list-of-dicts format expected by
    market_data.measure_event_impact().

    Returns list of dicts with keys: symbol, date, timing
    """
    return [
        {
            "symbol": ev["symbol"],
            "date": ev["date"],
            "timing": ev.get("timing", "market_hours"),
        }
        for ev in events
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(
        description="Scrape EDGAR EFTS for HSR early termination 8-K filings."
    )
    parser.add_argument("--start", default="2022-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   default="2022-06-30", help="End date YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=10,  help="Max results to display")
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Disable acquirer heuristic filter (show all tickers)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output full JSON instead of a table",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    print(f"Searching EDGAR for HSR early termination 8-K filings...")
    print(f"Date range: {args.start} to {args.end}")
    print(f"Acquirer filter: {'OFF' if args.no_filter else 'ON'}")
    print()

    events = scrape_hsr_terminations(
        start_date=args.start,
        end_date=args.end,
        filter_targets=not args.no_filter,
    )

    total = len(events)
    display = events[: args.limit]

    if args.json:
        print(json.dumps(display, indent=2))
    else:
        print(f"Found {total} events (showing first {len(display)}):")
        print()
        header = f"{'#':<4} {'Date':<12} {'Ticker':<8} {'Company'}"
        print(header)
        print("-" * 80)
        for i, ev in enumerate(display, 1):
            company_truncated = ev["company"][:55]
            print(f"{i:<4} {ev['date']:<12} {ev['symbol']:<8} {company_truncated}")

    print()
    print(f"Total events found: {total}")
    if total > 0:
        print(f"\nTo use with measure_event_impact():")
        print(f"  from tools.merger_catalyst_scraper import scrape_hsr_terminations, to_event_impact_format")
        print(f"  events = scrape_hsr_terminations(start_date='{args.start}', end_date='{args.end}')")
        print(f"  impact_events = to_event_impact_format(events)")


if __name__ == "__main__":
    _cli()
