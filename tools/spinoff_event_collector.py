"""
spinoff_event_collector.py — Collect corporate spin-off announcement events from SEC EDGAR.

Searches for 8-K filings containing specific narrow spin-off announcement phrases to identify
when large-cap companies announce they will spin off a division/subsidiary.

Academic background: Cusatis, Miles & Woolridge (1993) JFE find ~3% abnormal return
at spin-off announcements. Desai & Jain (1999) find ~4.45% for focus-increasing spin-offs.
Effect confirmed through 2024 (Nature 2021, Reexamining information asymmetry 2024).

Signal: LONG parent company stock at open after announcement day.

Usage:
    python tools/spinoff_event_collector.py --start 2019-01-01 --end 2024-12-31
    python tools/spinoff_event_collector.py --output data/spinoff_events.csv
"""

import json
import time
import argparse
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

EDGAR_EFTS = "https://efts.sec.gov/LATEST/search-index"
EDGAR_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"

# Browser-like headers (required - EDGAR blocks default User-Agent)
HEADERS = {
    "User-Agent": "financial-research-bot research@example.com",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

CACHE_DIR = Path(__file__).parent.parent / "data" / "edgar_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
TICKER_MAP_FILE = CACHE_DIR / "cik_to_ticker.json"

# Narrow announcement phrases that indicate a company is announcing a spin-off
# These avoid "complete the spin-off" (completion, not announcement) and
# overly broad "spin-off" alone (appears in any reference to past spin-offs)
SPINOFF_PHRASES = [
    '"plans to spin off"',
    '"intends to spin off"',
    '"intends to spin-off"',
    '"will spin off"',
    '"plan to spin off"',
    '"announced a spin-off"',
    '"announced the spin-off"',
    '"proposed spin-off"',
]


def load_cik_ticker_map(force_refresh: bool = False) -> dict:
    """Load CIK -> ticker mapping from EDGAR bulk JSON."""
    if not force_refresh and TICKER_MAP_FILE.exists():
        mtime = datetime.fromtimestamp(TICKER_MAP_FILE.stat().st_mtime)
        if (datetime.now() - mtime).days < 7:
            with open(TICKER_MAP_FILE) as f:
                return json.load(f)

    logger.info("Fetching EDGAR company tickers map...")
    try:
        resp = requests.get(EDGAR_COMPANY_TICKERS, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        cik_map = {}
        for item in raw.values():
            cik_str = str(item["cik_str"]).zfill(10)
            cik_map[cik_str] = {"ticker": item["ticker"], "name": item["title"]}
        with open(TICKER_MAP_FILE, "w") as f:
            json.dump(cik_map, f)
        logger.info(f"Loaded {len(cik_map)} CIK->ticker mappings")
        return cik_map
    except Exception as e:
        logger.error(f"Failed to load CIK->ticker map: {e}")
        return {}


def extract_ticker_from_display_names(display_names: list) -> str:
    """
    Extract ticker from EDGAR display_names field.
    Format: ["BORGWARNER INC  (BWA)  (CIK 0000908255)"]
    """
    for name_str in display_names:
        # Pattern: COMPANY NAME  (TICKER)  (CIK ...)
        matches = re.findall(r'\(([A-Z]{1,5})\)\s+\(CIK', name_str)
        if matches:
            return matches[0]
    return ""


def fetch_spinoff_filings(phrase: str, start_date: str, end_date: str, max_results: int = 500) -> list:
    """
    Fetch 8-K filings from EDGAR containing the given spin-off phrase.
    Returns list of {cik, ticker, company_name, file_date, accession_no, phrase}.
    """
    filings = []
    page_size = 100  # EDGAR EFTS supports up to 100 per page

    logger.info(f"Fetching phrase: {phrase} ({start_date} to {end_date})")

    for page_start in range(0, max_results, page_size):
        params = {
            "q": phrase,
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": str(page_start),
            "hits.hits.total.value": "true",
        }

        try:
            resp = requests.get(EDGAR_EFTS, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 403:
                logger.warning(f"EDGAR EFTS 403 for phrase {phrase}")
                break
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"EDGAR EFTS request failed: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {}).get("value", 0)

        if not hits:
            break

        if page_start == 0:
            logger.info(f"  Total hits: {total}")

        for hit in hits:
            src = hit.get("_source", {})
            file_date = src.get("file_date", "")
            display_names = src.get("display_names", [])

            # Try to get ticker from display_names
            ticker = extract_ticker_from_display_names(display_names)

            # Get CIK from entity_id or accession number
            entity_id = src.get("entity_id", "")
            accession = hit.get("_id", "")

            filings.append({
                "cik": entity_id,
                "ticker": ticker,
                "company_name": display_names[0] if display_names else "",
                "file_date": file_date,
                "accession_no": accession,
                "phrase": phrase.strip('"'),
            })

        if len(hits) < page_size or page_start + page_size >= min(total, max_results):
            break

        time.sleep(0.5)  # polite delay

    return filings


def collect_spinoff_events(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Main collection function. Searches all spin-off phrases, deduplicates,
    and resolves tickers via CIK mapping.

    Returns DataFrame with columns: ticker, company_name, date, phrase
    """
    cik_map = load_cik_ticker_map()

    all_filings = []
    for phrase in SPINOFF_PHRASES:
        filings = fetch_spinoff_filings(phrase, start_date, end_date)
        all_filings.extend(filings)
        time.sleep(1.0)

    if not all_filings:
        logger.warning("No filings found")
        return pd.DataFrame()

    df = pd.DataFrame(all_filings)
    df['date'] = pd.to_datetime(df['file_date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')

    # Try to resolve ticker via CIK map if not in display_names
    def resolve_ticker(row):
        if row['ticker']:
            return row['ticker']
        cik_padded = str(row.get('cik', '')).zfill(10)
        entry = cik_map.get(cik_padded, {})
        return entry.get('ticker', '')

    df['ticker'] = df.apply(resolve_ticker, axis=1)

    # Drop events without a ticker
    df_with_ticker = df[df['ticker'] != ''].copy()
    logger.info(f"Events with ticker: {len(df_with_ticker)} / {len(df)}")

    # Deduplicate: keep earliest filing per (ticker, 6-month window)
    # Sort by date ascending
    df_with_ticker = df_with_ticker.sort_values('date')

    deduped = []
    ticker_episodes = {}  # ticker -> last announcement date

    for _, row in df_with_ticker.iterrows():
        ticker = row['ticker']
        date = row['date']

        if ticker in ticker_episodes:
            last_date = ticker_episodes[ticker]
            # If same ticker filed again within 180 days, skip (same spin-off episode)
            if (date - last_date).days < 180:
                continue

        ticker_episodes[ticker] = date
        deduped.append(row)

    df_deduped = pd.DataFrame(deduped)
    logger.info(f"After deduplication: {len(df_deduped)} unique events")

    # Return sorted by date
    if not df_deduped.empty:
        df_deduped = df_deduped.sort_values('date')[['ticker', 'company_name', 'date_str', 'phrase', 'cik']].reset_index(drop=True)

    return df_deduped


def main():
    parser = argparse.ArgumentParser(description="Collect spin-off announcement events from EDGAR")
    parser.add_argument("--start", default="2019-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--output", default="data/spinoff_events.csv", help="Output CSV file")
    parser.add_argument("--min-market-cap", type=float, default=500, help="Min market cap in millions (default: 500)")
    args = parser.parse_args()

    events = collect_spinoff_events(args.start, args.end)

    if events.empty:
        logger.error("No events collected")
        return

    # Save raw events
    raw_path = Path(args.output).with_name(Path(args.output).stem + "_raw.csv")
    events.to_csv(raw_path, index=False)
    logger.info(f"Saved {len(events)} raw events to {raw_path}")

    # Filter to large-caps
    try:
        from tools.largecap_filter import filter_to_largecap
        min_cap_usd = args.min_market_cap * 1_000_000
        events_filtered = filter_to_largecap(
            [{"symbol": row.ticker, "date": row.date_str} for _, row in events.iterrows()],
            min_market_cap=min_cap_usd
        )
        tickers_kept = {e['symbol'] for e in events_filtered}
        events_lc = events[events['ticker'].isin(tickers_kept)].copy()
        logger.info(f"After >{args.min_market_cap}M market cap filter: {len(events_lc)} events")
    except Exception as e:
        logger.warning(f"Largecap filter failed: {e}. Saving unfiltered.")
        events_lc = events

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    events_lc.to_csv(output_path, index=False)
    logger.info(f"Saved {len(events_lc)} filtered events to {output_path}")

    print(f"\n{'='*60}")
    print(f"  Spin-Off Event Collection Summary")
    print(f"{'='*60}")
    print(f"  Period: {args.start} to {args.end}")
    print(f"  Raw events: {len(events)}")
    print(f"  After large-cap filter: {len(events_lc)}")
    print(f"\n  Sample events:")
    print(events_lc.head(20).to_string(index=False))

    return events_lc


if __name__ == "__main__":
    main()
