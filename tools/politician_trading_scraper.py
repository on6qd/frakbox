"""
politician_trading_scraper.py

Fetches US politician stock trading data from Capitol Trades (capitoltrades.com).

Data source: Capitol Trades embeds RSC (React Server Component) data directly in the
HTML of their server-rendered pages. Each page at /trades?pageSize=96&page=N contains
up to 96 trade records in a JSON payload inside a <script> tag.

Total dataset: ~34,000+ trades going back to 2012.

Usage:
    from tools.politician_trading_scraper import get_recent_trades, get_historical_trades

    recent = get_recent_trades()          # last 180 days
    historical = get_historical_trades()   # all available history (paginated, slow)
"""

import json
import re
import time
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
import requests

logger = logging.getLogger(__name__)

# --- Configuration ---
BASE_URL = "https://www.capitoltrades.com/trades"
CACHE_FILE = os.path.join(os.path.dirname(__file__), "politician_trades_cache.json")
PAGE_SIZE = 96          # max supported by Capitol Trades
SLEEP_BETWEEN_PAGES = 2  # seconds, be a good citizen
REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.capitoltrades.com/",
}

# Tickers that look like stock symbols but are actually funds/options/bonds
# (heuristic: skip if contains dot, slash, or looks like CUSIP/ISIN)
SKIP_PATTERNS = [
    r"^[0-9]",           # starts with a digit (CUSIP-like)
    r"\.",               # contains a dot (mutual fund tickers like FXAIX)
    r"/",                # options notation
    r"^[A-Z]{1,5}:[A-Z]{2}$",  # exchange-qualified (e.g. CVX:US - we strip the :US part)
]


# --- Normalisation helpers ---

def _clean_ticker(raw_ticker: Optional[str]) -> Optional[str]:
    """Strip exchange suffix (CVX:US -> CVX), validate ticker looks like a stock."""
    if not raw_ticker:
        return None
    ticker = raw_ticker.split(":")[0].strip().upper()
    if not ticker:
        return None
    # Must be 1-5 uppercase letters (NASDAQ/NYSE convention)
    if not re.match(r"^[A-Z]{1,5}$", ticker):
        return None
    return ticker


def _parse_amount_range(value: Optional[float]) -> tuple[Optional[int], Optional[int]]:
    """
    Capitol Trades provides a single 'value' field which is the midpoint of the
    STOCK Act disclosure range (e.g. $8,000 means the $1k-$15k range).
    We approximate min/max from the known STOCK Act brackets.
    """
    if value is None:
        return None, None
    v = float(value)
    # STOCK Act amount bands (approximate midpoints)
    bands = [
        (0,       1_000,     500),
        (1_001,   15_000,    8_000),
        (15_001,  50_000,    32_500),
        (50_001,  100_000,   75_000),
        (100_001, 250_000,   175_000),
        (250_001, 500_000,   375_000),
        (500_001, 1_000_000, 750_000),
        (1_000_001, 5_000_000, 3_000_000),
        (5_000_001, 25_000_000, 15_000_000),
        (25_000_001, 50_000_000, 37_500_000),
    ]
    for lo, hi, mid in bands:
        if abs(v - mid) < 0.01 * mid + 100:   # within 1% or $100 of midpoint
            return lo, hi
    # Fall back: use the value itself as both bounds
    return int(v), int(v)


def _normalise_trade(raw: dict) -> Optional[dict]:
    """
    Convert a raw Capitol Trades trade record into our standard format.
    Returns None if the record should be skipped (non-stock asset, foreign stock, etc.)
    """
    issuer = raw.get("issuer") or {}
    politician = raw.get("politician") or {}

    raw_ticker = issuer.get("issuerTicker", "")
    ticker = _clean_ticker(raw_ticker)
    if not ticker:
        return None

    # Skip non-US stocks (sector check + country check)
    country = issuer.get("country", "us")
    if country != "us":
        return None

    # Map transaction type
    tx_type = raw.get("txType", "").lower()
    if tx_type in ("buy", "purchase"):
        transaction_type = "Purchase"
    elif tx_type in ("sell", "sale"):
        transaction_type = "Sale"
    elif tx_type == "exchange":
        transaction_type = "Exchange"
    else:
        transaction_type = tx_type.capitalize() if tx_type else "Unknown"

    # Politician name + party
    first = politician.get("firstName", "")
    last = politician.get("lastName", "")
    full_name = f"{first} {last}".strip()
    party_raw = politician.get("party", "").lower()
    party_map = {"republican": "Republican", "democrat": "Democrat", "independent": "Independent"}
    party = party_map.get(party_raw, party_raw.capitalize())

    chamber_raw = raw.get("chamber", politician.get("chamber", "")).lower()
    chamber = "House" if chamber_raw == "house" else "Senate" if chamber_raw == "senate" else chamber_raw.capitalize()

    # Dates
    tx_date = raw.get("txDate", "")       # "2026-03-12"
    pub_date_raw = raw.get("pubDate", "")  # "2026-03-23T13:01:31Z"
    disclosure_date = pub_date_raw[:10] if pub_date_raw else ""

    amount_min, amount_max = _parse_amount_range(raw.get("value"))

    return {
        "politician":        full_name,
        "chamber":           chamber,
        "party":             party,
        "ticker":            ticker,
        "transaction_type":  transaction_type,
        "transaction_date":  tx_date,
        "disclosure_date":   disclosure_date,
        "amount_min":        amount_min,
        "amount_max":        amount_max,
        "asset_description": issuer.get("issuerName", ""),
        # --- extra fields useful for analysis ---
        "_tx_id":            raw.get("_txId"),
        "_politician_id":    raw.get("_politicianId"),
        "_issuer_id":        raw.get("_issuerId"),
        "price":             raw.get("price"),
        "reporting_gap_days": raw.get("reportingGap"),
        "sector":            issuer.get("sector", ""),
    }


# --- Page fetching ---

def _fetch_page(page: int, sort: str = "-pubDate") -> tuple[list[dict], int, int]:
    """
    Fetch one page of trades from Capitol Trades.
    Returns (raw_trades, total_count, total_pages).
    """
    url = f"{BASE_URL}?pageSize={PAGE_SIZE}&page={page}&sortBy={sort}"
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    html = resp.text
    return _parse_trades_from_html(html)


def _parse_trades_from_html(html: str) -> tuple[list[dict], int, int]:
    """
    Parse RSC (React Server Component) payload embedded in Capitol Trades HTML.

    Capitol Trades uses Next.js App Router which embeds server-rendered data as
    escaped JSON inside <script> tags of the form:
        self.__next_f.push([1, "...RSC payload..."])

    The RSC payload is a JSON-encoded string containing the component tree with
    trade data embedded as: "data":[{"_issuerId":...}, ...]
    """
    # Extract all RSC script payloads
    rsc_scripts = re.findall(
        r'self\.__next_f\.push\(\[1,(.*?)\]\)',
        html,
        re.DOTALL,
    )

    trades: list[dict] = []
    total_count = 0
    total_pages = 1

    for script_content in rsc_scripts:
        try:
            # Each script payload is a JSON-encoded string
            parsed = json.loads(script_content)
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(parsed, str):
            continue

        # Extract the trade data array
        if '"_issuerId"' in parsed:
            idx = parsed.find('"data":[{"_issuerId"')
            if idx >= 0:
                start = parsed.index('[', idx)
                depth = 0
                for i, c in enumerate(parsed[start:], start):
                    if c == '[':
                        depth += 1
                    elif c == ']':
                        depth -= 1
                        if depth == 0:
                            try:
                                trades = json.loads(parsed[start:i + 1])
                            except json.JSONDecodeError:
                                pass
                            break

        # Extract pagination metadata
        paging_match = re.search(r'"totalCount":(\d+),"totalPages":(\d+)', parsed)
        if paging_match:
            total_count = int(paging_match.group(1))
            total_pages = int(paging_match.group(2))

    return trades, total_count, total_pages


# --- Cache management ---

def _load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"trades": [], "last_updated": None, "total_count": 0}


def _save_cache(cache: dict) -> None:
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2, default=str)


def _cache_is_fresh(cache: dict, max_age_hours: float = 4.0) -> bool:
    last = cache.get("last_updated")
    if not last:
        return False
    try:
        updated = datetime.fromisoformat(last.replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - updated
        return age.total_seconds() < max_age_hours * 3600
    except (ValueError, AttributeError):
        return False


# --- Public API ---

def get_recent_trades(
    days: int = 180,
    use_cache: bool = True,
    refresh_if_older_than_hours: float = 4.0,
) -> list[dict]:
    """
    Return normalised trade records from the last `days` calendar days.

    Fetches pages sorted by publication date (newest first) and stops once
    all trades on a page are older than the cutoff.

    Args:
        days: Number of calendar days to look back (default 180).
        use_cache: Whether to use/update the cache file.
        refresh_if_older_than_hours: Re-fetch if cached data is older than this.

    Returns:
        List of normalised trade dicts (see module docstring for schema).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    if use_cache:
        cache = _load_cache()
        if _cache_is_fresh(cache, refresh_if_older_than_hours):
            # Filter cached trades to requested window
            trades = []
            for t in cache.get("trades", []):
                tx_date = t.get("transaction_date", "")
                if tx_date and tx_date >= cutoff.strftime("%Y-%m-%d"):
                    trades.append(t)
            logger.info(f"Returning {len(trades)} trades from cache (cutoff: {cutoff.date()})")
            return trades

    logger.info(f"Fetching recent trades (last {days} days) from Capitol Trades...")
    all_raw: list[dict] = []
    page = 1
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    total_count = 0
    total_pages = 9999

    while True:
        logger.info(f"  Fetching page {page}...")
        try:
            raw_trades, total_count, total_pages = _fetch_page(page, sort="-txDate")
        except requests.RequestException as e:
            logger.error(f"  Request failed on page {page}: {e}")
            break

        if not raw_trades:
            break

        all_raw.extend(raw_trades)

        # Stop when the oldest txDate on this page is before our cutoff
        oldest_on_page = min(
            (t.get("txDate") or "" for t in raw_trades),
            default=""
        )
        if oldest_on_page and oldest_on_page < cutoff_str:
            logger.info(f"  Reached cutoff at page {page} (oldest txDate: {oldest_on_page})")
            break

        if page >= total_pages:
            break

        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

    # Normalise and filter
    normalised = []
    seen_ids = set()
    for raw in all_raw:
        tx = _normalise_trade(raw)
        if tx is None:
            continue
        tx_id = tx["_tx_id"]
        if tx_id in seen_ids:
            continue
        seen_ids.add(tx_id)
        # Apply date filter
        tx_date = tx.get("transaction_date", "")
        if tx_date and tx_date >= cutoff.strftime("%Y-%m-%d"):
            normalised.append(tx)

    # Update cache with fresh recent data
    if use_cache and normalised:
        cache = _load_cache()
        # Merge: keep old records outside the window, replace within window
        old_trades = [
            t for t in cache.get("trades", [])
            if (t.get("transaction_date") or "") < cutoff_str
        ]
        existing_ids = {t["_tx_id"] for t in old_trades}
        merged = old_trades + [t for t in normalised if t["_tx_id"] not in existing_ids]
        cache["trades"] = merged
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        cache["total_count"] = total_count
        _save_cache(cache)

    logger.info(f"Returning {len(normalised)} normalised trades")
    return normalised


def get_historical_trades(
    max_pages: Optional[int] = None,
    start_date: Optional[str] = None,
    stop_date: Optional[str] = None,
    use_cache: bool = True,
    force_refresh: bool = False,
) -> list[dict]:
    """
    Return all available historical trade records from Capitol Trades.

    WARNING: The full dataset has ~34,000+ trades across ~362 pages.
    At 2 seconds/page, a full fetch takes ~12 minutes. Results are cached.

    Args:
        max_pages:    Stop after this many pages (None = all pages).
        start_date:   Only return trades on/after this date ("YYYY-MM-DD").
        stop_date:    Stop fetching when all trades on a page are before this date.
        use_cache:    Use cached data if available.
        force_refresh: Ignore cache and re-fetch everything.

    Returns:
        List of normalised trade dicts.
    """
    if use_cache and not force_refresh:
        cache = _load_cache()
        cached_trades = cache.get("trades", [])
        if cached_trades:
            logger.info(f"Returning {len(cached_trades)} trades from cache")
            if start_date:
                cached_trades = [t for t in cached_trades if t.get("transaction_date", "") >= start_date]
            return cached_trades

    logger.info("Fetching historical trades from Capitol Trades (this may take several minutes)...")

    # Get first page to find total
    try:
        first_raw, total_count, total_pages = _fetch_page(1, sort="-txDate")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch first page: {e}")
        return []

    logger.info(f"Capitol Trades has {total_count} total trades across {total_pages} pages")

    if max_pages:
        total_pages = min(total_pages, max_pages)

    all_raw: list[dict] = list(first_raw)

    for page in range(2, total_pages + 1):
        if page % 10 == 0:
            logger.info(f"  Fetched {page}/{total_pages} pages ({len(all_raw)} raw trades)...")
        try:
            raw_trades, _, _ = _fetch_page(page, sort="-txDate")
        except requests.RequestException as e:
            logger.warning(f"  Failed on page {page}: {e}, skipping")
            time.sleep(5)
            continue

        if not raw_trades:
            break

        all_raw.extend(raw_trades)

        # Early stop if all trades older than stop_date
        if stop_date:
            oldest = min(
                (t.get("txDate") or t.get("pubDate", "")[:10] for t in raw_trades),
                default=""
            )
            if oldest and oldest < stop_date:
                logger.info(f"  Reached stop_date {stop_date} at page {page}")
                break

        time.sleep(SLEEP_BETWEEN_PAGES)

    # Normalise
    normalised: list[dict] = []
    seen_ids: set = set()
    for raw in all_raw:
        tx = _normalise_trade(raw)
        if tx is None:
            continue
        tx_id = tx["_tx_id"]
        if tx_id in seen_ids:
            continue
        seen_ids.add(tx_id)
        if start_date and tx.get("transaction_date", "") < start_date:
            continue
        normalised.append(tx)

    logger.info(f"Fetched {len(normalised)} normalised trades")

    # Persist to cache
    if use_cache:
        cache = _load_cache()
        cache["trades"] = normalised
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        cache["total_count"] = total_count
        _save_cache(cache)
        logger.info(f"Saved {len(normalised)} trades to cache: {CACHE_FILE}")

    return normalised


def get_trades_for_ticker(
    ticker: str,
    days: Optional[int] = None,
) -> list[dict]:
    """
    Return all cached/fetched trades for a specific ticker symbol.

    Call get_historical_trades() first to populate the cache.
    """
    cache = _load_cache()
    trades = cache.get("trades", [])
    if not trades:
        logger.warning("Cache is empty. Run get_recent_trades() or get_historical_trades() first.")
        return []
    ticker = ticker.upper()
    result = [t for t in trades if t.get("ticker") == ticker]
    if days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        result = [t for t in result if t.get("transaction_date", "") >= cutoff]
    return result


def summarise_trades(trades: list[dict]) -> dict:
    """Print a brief summary of a trade list."""
    if not trades:
        return {"count": 0}
    tickers = {}
    politicians = {}
    for t in trades:
        tickers[t["ticker"]] = tickers.get(t["ticker"], 0) + 1
        politicians[t["politician"]] = politicians.get(t["politician"], 0) + 1

    buys = sum(1 for t in trades if t["transaction_type"] == "Purchase")
    sells = sum(1 for t in trades if t["transaction_type"] == "Sale")
    dates = sorted(t["transaction_date"] for t in trades if t["transaction_date"])

    return {
        "count":            len(trades),
        "purchases":        buys,
        "sales":            sells,
        "unique_tickers":   len(tickers),
        "top_tickers":      sorted(tickers.items(), key=lambda x: -x[1])[:10],
        "top_politicians":  sorted(politicians.items(), key=lambda x: -x[1])[:10],
        "date_range":       (dates[0], dates[-1]) if dates else ("", ""),
    }


# --- CLI convenience ---

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    mode = sys.argv[1] if len(sys.argv) > 1 else "recent"

    if mode == "recent":
        trades = get_recent_trades(days=180)
    elif mode == "historical":
        max_p = int(sys.argv[2]) if len(sys.argv) > 2 else None
        trades = get_historical_trades(max_pages=max_p)
    else:
        print("Usage: python politician_trading_scraper.py [recent|historical [max_pages]]")
        sys.exit(1)

    summary = summarise_trades(trades)
    print(json.dumps(summary, indent=2))
    if trades:
        print("\nFirst 3 trades:")
        for t in trades[:3]:
            print(json.dumps(t, indent=2))
