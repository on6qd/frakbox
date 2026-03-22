"""
SEO (Secondary Equity Offering) Event Collector.

Fetches 424B4 prospectus filings from SEC EDGAR to identify secondary offering
announcement dates and tickers for the SEO dilution short signal.

Strategy:
- 424B4 = final prospectus filed when offering PRICES (same day or next morning)
- The announcement drop happens the PRIOR EVENING (after hours) when the deal
  is announced to the market, or at open if priced pre-market
- So: use 424B4 filing date as Day 0 for "open" entry_price (next morning open)
  because the announcement is usually after-hours the evening before pricing.

EDGAR approach:
- Use the EDGAR full-text search REST API with proper headers
- Fall back to the daily EDGAR index files if EFTS is rate-limited
- Parse CIK -> ticker mapping via EDGAR company tickers JSON

Friction notes:
- EDGAR EFTS (efts.sec.gov) returns 403 to some clients — use requests + browser UA
- Ticker resolution requires CIK -> ticker lookup (EDGAR provides a bulk JSON)
- Many 424B4 filers are small-cap SPACs/micro-caps — must filter to >$1B market cap

Usage:
    python tools/seo_event_collector.py --start 2020-01-01 --end 2024-12-31 --output data/seo_events.csv
    python tools/seo_event_collector.py --year 2023 --min-market-cap 1000

Author: financial-researcher agent
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

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# EDGAR endpoints
EDGAR_BASE = "https://www.sec.gov"
EDGAR_EFTS = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"
EDGAR_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"

# Browser-like headers to avoid 403s (documented friction: EDGAR blocks default User-Agent)
HEADERS = {
    "User-Agent": "financial-research-bot research@example.com",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Cache directory
CACHE_DIR = Path(__file__).parent.parent / "data" / "edgar_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TICKER_MAP_FILE = CACHE_DIR / "cik_to_ticker.json"
TICKER_MAP_TTL_DAYS = 7  # Refresh the CIK->ticker map weekly


def load_cik_ticker_map(force_refresh: bool = False) -> dict:
    """
    Load CIK -> ticker mapping from EDGAR bulk JSON.
    Cached locally for TICKER_MAP_TTL_DAYS days.
    Returns dict: {cik_str: {"ticker": "AAPL", "name": "Apple Inc."}}
    """
    if not force_refresh and TICKER_MAP_FILE.exists():
        mtime = datetime.fromtimestamp(TICKER_MAP_FILE.stat().st_mtime)
        if (datetime.now() - mtime).days < TICKER_MAP_TTL_DAYS:
            with open(TICKER_MAP_FILE) as f:
                return json.load(f)

    logger.info("Fetching EDGAR company tickers map...")
    try:
        resp = requests.get(EDGAR_COMPANY_TICKERS, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.json()

        # Format: {index: {cik_str, ticker, title}}
        # Convert to {cik_str (zero-padded 10 digits): {ticker, name}}
        cik_map = {}
        for item in raw.values():
            cik_str = str(item["cik_str"]).zfill(10)
            cik_map[cik_str] = {
                "ticker": item["ticker"],
                "name": item["title"],
            }

        with open(TICKER_MAP_FILE, "w") as f:
            json.dump(cik_map, f)
        logger.info(f"Loaded {len(cik_map)} CIK->ticker mappings")
        return cik_map
    except Exception as e:
        logger.error(f"Failed to load CIK->ticker map: {e}")
        return {}


def fetch_424b4_via_efts(start_date: str, end_date: str, max_results: int = 200) -> list:
    """
    Fetch 424B4 filings from EDGAR full-text search.
    Returns list of dicts: {cik, company_name, file_date, accession_no}

    Note: EDGAR EFTS paginates at 10 results/page, max 10000 total.
    We use the search-index endpoint for structured JSON results.
    """
    filings = []
    page_size = 10

    # The EDGAR EFTS API
    url = "https://efts.sec.gov/LATEST/search-index"

    # Total pages needed
    max_pages = min(max_results // page_size + 1, 100)

    for page in range(max_pages):
        offset = page * page_size

        params = {
            "q": '"secondary offering"',
            "forms": "424B4",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "hits.hits.total.value": "true",
            "_source": "period_of_report,entity_name,file_date,file_num,period_of_report",
            "hits.hits._source": "entity_name,file_date,file_num",
            "from": str(offset),
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 403:
                logger.warning("EDGAR EFTS returned 403 — switching to alternative method")
                return filings
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"EDGAR EFTS request failed: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})
            filings.append({
                "cik": hit.get("_id", "").split(":")[0] if ":" in hit.get("_id", "") else "",
                "company_name": src.get("entity_name", ""),
                "file_date": src.get("file_date", ""),
                "accession_no": hit.get("_id", ""),
            })

        total = data.get("hits", {}).get("total", {}).get("value", 0)
        logger.info(f"EFTS page {page+1}: got {len(hits)} hits (total: {total})")

        if offset + page_size >= min(total, max_results):
            break

        time.sleep(0.5)  # Rate limit respect

    return filings


def fetch_424b4_via_full_index(year: int, quarter: int) -> list:
    """
    Alternative: fetch 424B4 filings from EDGAR quarterly full-index files.
    These are plain text files listing ALL filings for a quarter.
    More reliable than EFTS but less filtered (no text search).
    Returns list of dicts: {cik, company_name, file_date, accession_no}
    """
    # EDGAR quarterly index URL format
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx"

    cache_file = CACHE_DIR / f"full_index_{year}_Q{quarter}.txt"

    if cache_file.exists():
        logger.info(f"Loading cached index: {cache_file}")
        content = cache_file.read_text()
    else:
        logger.info(f"Fetching EDGAR index: {year} Q{quarter}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            content = resp.text
            cache_file.write_text(content)
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Failed to fetch index {year} Q{quarter}: {e}")
            return []

    # Parse the fixed-width company.idx file
    # Format: Company Name         Form Type   CIK          Date Filed  Filename
    filings = []
    lines = content.split("\n")

    # Find header line to determine column positions
    # company.idx has a header like:
    # Company Name                   Form Type   CIK         Date Filed  Filename
    # --------------- ...
    in_data = False
    for line in lines:
        if line.startswith("---"):
            in_data = True
            continue
        if not in_data or not line.strip():
            continue

        # Fixed-width: company name ends at col 62, form type at 74, CIK at 86, date at 98
        if len(line) < 50:
            continue

        # Split by multiple spaces — company.idx is space-separated
        # Format: "Company Name                   424B4       1234567    2023-01-15  edgar/data/..."
        parts = line.split()
        if len(parts) < 5:
            continue

        # Find form type - scan for 424B4
        form_type = None
        form_idx = None
        for i, p in enumerate(parts):
            if p == "424B4":
                form_type = p
                form_idx = i
                break

        if form_type != "424B4":
            continue

        # Company name is everything before form type index
        company_name = " ".join(parts[:form_idx])

        # After form type: CIK, date, filename
        remaining = parts[form_idx + 1:]
        if len(remaining) < 3:
            continue

        cik = remaining[0].zfill(10)
        file_date = remaining[1]
        filename = remaining[2] if len(remaining) > 2 else ""

        # Validate date format
        try:
            datetime.strptime(file_date, "%Y-%m-%d")
        except ValueError:
            continue

        # Convert filename to accession number
        accession = filename.replace("/", "-").replace(".txt", "").split("edgar-data-")[-1] if filename else ""

        filings.append({
            "cik": cik,
            "company_name": company_name,
            "file_date": file_date,
            "accession_no": accession,
            "form_type": "424B4",
        })

    return filings


def resolve_ticker_from_cik(cik: str, cik_map: dict) -> str | None:
    """
    Look up ticker for a CIK from the EDGAR CIK->ticker map.
    CIK is zero-padded to 10 digits.
    """
    cik_padded = str(cik).lstrip("0").zfill(10)
    entry = cik_map.get(cik_padded)
    if entry:
        return entry["ticker"]

    # Try without padding
    for k in [str(int(cik)) if cik.isdigit() else cik]:
        k_padded = k.zfill(10)
        if k_padded in cik_map:
            return cik_map[k_padded]["ticker"]

    return None


def get_market_cap_at_filing(ticker: str, file_date: str) -> float | None:
    """
    Get approximate market cap around the filing date.
    Uses yfinance - gets market cap from the period around the event.
    Returns market cap in millions USD, or None if unavailable.
    """
    try:
        import yfinance as yf
        # Check 30 days around the filing date for market cap proxy
        date = datetime.strptime(file_date, "%Y-%m-%d")
        start = (date - timedelta(days=5)).strftime("%Y-%m-%d")
        end = (date + timedelta(days=5)).strftime("%Y-%m-%d")

        hist = yf.Ticker(ticker).history(start=start, end=end)
        if hist.empty:
            return None

        # Use shares outstanding * close price as proxy
        info = yf.Ticker(ticker).info
        shares = info.get("sharesOutstanding")
        if shares and not hist.empty:
            close = hist["Close"].iloc[-1]
            return (shares * close) / 1_000_000

        # Fallback: just use current market cap
        market_cap = info.get("marketCap")
        if market_cap:
            return market_cap / 1_000_000

    except Exception:
        pass
    return None


def collect_seo_events(
    start_date: str = "2020-01-01",
    end_date: str = "2024-12-31",
    min_market_cap_m: float = 1000.0,
    max_events: int = 300,
    use_full_index: bool = True,
    use_efts: bool = True,
    output_file: str = None,
    check_market_cap: bool = False,  # Slow — only enable for final filtering
) -> pd.DataFrame:
    """
    Main function: collect SEO events from EDGAR 424B4 filings.

    Returns DataFrame with columns:
        ticker, company_name, file_date, cik, accession_no

    Args:
        start_date: Start of collection period (YYYY-MM-DD)
        end_date: End of collection period (YYYY-MM-DD)
        min_market_cap_m: Minimum market cap in millions (default $1B)
        max_events: Maximum events to collect
        use_full_index: Use EDGAR quarterly full-index files (reliable)
        use_efts: Try EDGAR full-text search first (filtered but may 403)
        output_file: If set, save CSV to this path
        check_market_cap: If True, filter by market cap via yfinance (slow)
    """
    logger.info(f"Collecting SEO events: {start_date} to {end_date}")

    # Load CIK -> ticker map
    cik_map = load_cik_ticker_map()
    if not cik_map:
        logger.error("Failed to load CIK->ticker map — cannot resolve tickers")
        return pd.DataFrame()

    all_filings = []

    # Method 1: Try EDGAR EFTS (filtered to "secondary offering" text)
    if use_efts:
        logger.info("Trying EDGAR EFTS full-text search...")
        efts_filings = fetch_424b4_via_efts(start_date, end_date, max_results=max_events)
        logger.info(f"EFTS returned {len(efts_filings)} filings")
        all_filings.extend(efts_filings)

    # Method 2: Full quarterly index (all 424B4s, not filtered to SEO text)
    # This is broader but captures MORE events. We filter later by market cap.
    if use_full_index and len(all_filings) < 50:
        logger.info("Using EDGAR quarterly full-index files...")

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Enumerate quarters in range
        quarters = []
        y, q = start_dt.year, (start_dt.month - 1) // 3 + 1
        while True:
            quarters.append((y, q))
            q += 1
            if q > 4:
                q = 1
                y += 1
            end_q = (end_dt.month - 1) // 3 + 1
            if y > end_dt.year or (y == end_dt.year and q > end_q):
                break

        logger.info(f"Fetching {len(quarters)} quarterly indexes...")
        for year, quarter in quarters:
            q_filings = fetch_424b4_via_full_index(year, quarter)
            # Filter to date range
            q_filtered = [
                f for f in q_filings
                if start_date <= f["file_date"] <= end_date
            ]
            all_filings.extend(q_filtered)
            logger.info(f"Q{quarter} {year}: {len(q_filtered)} 424B4 filings")
            time.sleep(0.3)

    if not all_filings:
        logger.error("No filings collected")
        return pd.DataFrame()

    logger.info(f"Total raw filings: {len(all_filings)}")

    # Resolve tickers
    rows = []
    seen_keys = set()  # Deduplicate by (ticker, file_date)

    for f in all_filings:
        cik = str(f.get("cik", "")).zfill(10)
        ticker = resolve_ticker_from_cik(cik, cik_map)

        if not ticker:
            continue

        file_date = f.get("file_date", "")
        key = (ticker, file_date)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        rows.append({
            "ticker": ticker,
            "company_name": f.get("company_name", ""),
            "file_date": file_date,
            "cik": cik,
            "accession_no": f.get("accession_no", ""),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("No tickers resolved from filings")
        return df

    # Sort by date
    df["file_date"] = pd.to_datetime(df["file_date"])
    df = df.sort_values("file_date").reset_index(drop=True)
    df["file_date"] = df["file_date"].dt.strftime("%Y-%m-%d")

    logger.info(f"Resolved {len(df)} unique (ticker, date) pairs")

    # Filter out known bad/irrelevant tickers
    # Exclude ETFs, funds, blank tickers
    bad_patterns = re.compile(r'^(ETF|FUND|\d+|[A-Z]{5,}$)', re.IGNORECASE)
    df = df[~df["ticker"].apply(lambda t: bool(bad_patterns.match(t)) if t else True)]

    # Filter out warrants and units (typically end in W, WS, U, R)
    df = df[~df["ticker"].str.match(r'^[A-Z]+[WUR]$', na=True)]

    logger.info(f"After ticker cleanup: {len(df)} events")

    # Optional: filter by market cap (slow — requires yfinance call per ticker)
    if check_market_cap and min_market_cap_m > 0:
        logger.info(f"Checking market caps (min ${min_market_cap_m:.0f}M)... this is slow")

        try:
            from tools.largecap_filter import filter_to_largecap
            df_temp = df.rename(columns={"ticker": "ticker_col"})
            df_temp["ticker"] = df_temp["ticker_col"]
            df = filter_to_largecap(df, min_market_cap_m=min_market_cap_m)
        except Exception as e:
            logger.error(f"Market cap filter failed: {e}")

    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} events to {output_path}")

    return df


def print_summary(df: pd.DataFrame):
    """Print a summary of collected SEO events."""
    if df.empty:
        print("No events collected.")
        return

    print(f"\n{'='*60}")
    print(f"SEO EVENT COLLECTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total events: {len(df)}")
    print(f"Unique tickers: {df['ticker'].nunique()}")
    print(f"Date range: {df['file_date'].min()} to {df['file_date'].max()}")

    # Events per year
    df_copy = df.copy()
    df_copy["year"] = pd.to_datetime(df_copy["file_date"]).dt.year
    yearly = df_copy.groupby("year").size()
    print(f"\nEvents by year:")
    for year, count in yearly.items():
        print(f"  {year}: {count}")

    print(f"\nSample events (first 10):")
    print(df[["ticker", "company_name", "file_date"]].head(10).to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect SEO events from EDGAR 424B4 filings")
    parser.add_argument("--start", default="2020-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31", help="End date YYYY-MM-DD")
    parser.add_argument("--year", type=int, help="Convenience: set both start and end to a single year")
    parser.add_argument("--min-market-cap", type=float, default=0,
                        help="Min market cap in millions (0 = no filter, slow if set)")
    parser.add_argument("--output", default="data/seo_events_raw.csv", help="Output CSV path")
    parser.add_argument("--no-efts", action="store_true", help="Skip EDGAR EFTS (full-text search)")
    parser.add_argument("--no-index", action="store_true", help="Skip quarterly index files")
    parser.add_argument("--check-market-cap", action="store_true",
                        help="Filter by market cap via yfinance (slow)")
    parser.add_argument("--max", type=int, default=300, help="Max events to collect")

    args = parser.parse_args()

    start = f"{args.year}-01-01" if args.year else args.start
    end = f"{args.year}-12-31" if args.year else args.end

    df = collect_seo_events(
        start_date=start,
        end_date=end,
        min_market_cap_m=args.min_market_cap,
        max_events=args.max,
        use_full_index=not args.no_index,
        use_efts=not args.no_efts,
        output_file=args.output,
        check_market_cap=args.check_market_cap,
    )

    print_summary(df)
