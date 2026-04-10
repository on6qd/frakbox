#!/usr/bin/env python3
"""Convertible Note Offering Event Collector + Backtest.

Collects convertible note offering announcements from SEC EDGAR 8-K filings,
filters to large-cap (>$1B), deduplicates, and runs backtest with IS/OOS split.

Strategy: When a company announces a convertible note offering, existing equity
holders face dilution risk. Academic literature (Dann & Mikkelson 1984, Eckbo 1986)
shows negative abnormal returns post-announcement for equity. The effect should be
weaker than straight equity offerings (SEO) because convertibles are less dilutive.

EDGAR approach:
- Use EFTS full-text search for 8-K filings containing "convertible" AND "offering"
- Filter for NEW offerings (not amendments/mentions of existing notes)
- Resolve CIK -> ticker via EDGAR bulk JSON
- Filter by market cap (>$1B)
- Deduplicate by ticker-month

Usage:
    python tools/convertible_note_collector.py --start 2021-01-01 --end 2025-12-31
    python tools/convertible_note_collector.py --backtest  # full collect + backtest

Author: financial-researcher agent
"""
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime
from collections import Counter

import requests
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "financial-researcher research@example.com"}
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR = DATA_DIR / "edgar_cache"
CACHE_DIR.mkdir(exist_ok=True)

# ── CIK -> Ticker Resolution ──────────────────────────────────────────────────

def load_cik_ticker_map() -> dict:
    """Load cached CIK -> ticker map from SEO collector's cache."""
    cache_file = CACHE_DIR / "cik_to_ticker.json"
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)

    logger.info("Fetching EDGAR company tickers map...")
    resp = requests.get("https://www.sec.gov/files/company_tickers.json",
                       headers=HEADERS, timeout=30)
    resp.raise_for_status()
    raw = resp.json()
    cik_map = {}
    for item in raw.values():
        cik_str = str(item["cik_str"]).zfill(10)
        cik_map[cik_str] = {"ticker": item["ticker"], "name": item["title"]}

    with open(cache_file, 'w') as f:
        json.dump(cik_map, f)
    logger.info(f"Loaded {len(cik_map)} CIK->ticker mappings")
    return cik_map


def resolve_ticker(cik: str, cik_map: dict) -> str | None:
    """Resolve CIK to ticker."""
    cik_padded = str(cik).lstrip("0").zfill(10)
    entry = cik_map.get(cik_padded)
    if entry:
        return entry["ticker"]
    return None


# ── EDGAR EFTS Search ──────────────────────────────────────────────────────────

def fetch_convertible_8k_efts(start_date: str, end_date: str, max_results: int = 500) -> list:
    """
    Search EDGAR EFTS for 8-K filings mentioning convertible note offerings.

    Uses the EFTS search-index endpoint with full-text query.
    Returns list of dicts: {cik, entity_name, file_date, accession_no}
    """
    url = "https://efts.sec.gov/LATEST/search-index"
    filings = []
    page_size = 10
    max_pages = min(max_results // page_size + 1, 100)

    # Search for 8-K filings with convertible offering language
    # "aggregate principal amount" is highly specific to new note offerings
    query = '"convertible" "offering" "aggregate principal amount"'

    for page in range(max_pages):
        offset = page * page_size
        params = {
            "q": query,
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": str(offset),
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 403:
                logger.warning("EDGAR EFTS returned 403 — will retry with alternate approach")
                break
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"EFTS page {page}: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})
            filing_id = hit.get("_id", "")
            cik = filing_id.split(":")[0] if ":" in filing_id else ""

            filings.append({
                "cik": cik,
                "entity_name": src.get("entity_name", ""),
                "display_names": src.get("display_names", []),
                "file_date": src.get("file_date", ""),
                "accession_no": filing_id,
            })

        total = data.get("hits", {}).get("total", {}).get("value", 0)
        logger.info(f"EFTS page {page+1}: {len(hits)} hits (total: {total})")

        if offset + page_size >= min(total, max_results):
            break

        time.sleep(0.15)

    return filings


def fetch_convertible_8k_alternate(start_date: str, end_date: str) -> list:
    """
    Alternate EDGAR search using the full-text search API (not search-index).
    This endpoint has different rate limits and may work when search-index fails.
    """
    url = "https://efts.sec.gov/LATEST/search-index"

    # Try a simpler query
    filings = []
    queries = [
        '"convertible notes" "offering"',
        '"convertible senior notes" "offering"',
    ]

    for q in queries:
        page_size = 10
        for page in range(50):
            offset = page * page_size
            params = {
                "q": q,
                "forms": "8-K",
                "dateRange": "custom",
                "startdt": start_date,
                "enddt": end_date,
                "from": str(offset),
            }

            try:
                resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
                if resp.status_code != 200:
                    break
                data = resp.json()
            except Exception:
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                src = hit.get("_source", {})
                filing_id = hit.get("_id", "")
                cik = filing_id.split(":")[0] if ":" in filing_id else ""
                filings.append({
                    "cik": cik,
                    "entity_name": src.get("entity_name", ""),
                    "display_names": src.get("display_names", []),
                    "file_date": src.get("file_date", ""),
                    "accession_no": filing_id,
                })

            total = data.get("hits", {}).get("total", {}).get("value", 0)
            if offset + page_size >= total:
                break
            time.sleep(0.15)

    return filings


# ── Market Cap Filter ──────────────────────────────────────────────────────────

def get_market_caps(tickers: list) -> dict:
    """Get market caps for a list of tickers. Returns {ticker: cap_usd}."""
    import yfinance as yf

    mc_cache_path = DATA_DIR / "convertible_mc_cache.json"
    if mc_cache_path.exists():
        with open(mc_cache_path) as f:
            cache = json.load(f)
    else:
        cache = {}

    new_tickers = [t for t in tickers if t not in cache]
    logger.info(f"Market cap lookups: {len(new_tickers)} new out of {len(tickers)} total")

    for i, tkr in enumerate(new_tickers):
        if i % 25 == 0 and i > 0:
            logger.info(f"  Progress: {i}/{len(new_tickers)}")
        try:
            info = yf.Ticker(tkr).info
            cache[tkr] = info.get('marketCap', 0) or 0
        except Exception:
            cache[tkr] = 0
        time.sleep(0.05)

    with open(mc_cache_path, 'w') as f:
        json.dump(cache, f)

    return cache


# ── Main Collection ────────────────────────────────────────────────────────────

def collect_convertible_events(
    start_date: str = "2021-01-01",
    end_date: str = "2025-12-31",
    min_market_cap: float = 1_000_000_000,
) -> list:
    """
    Collect convertible note offering events from EDGAR.
    Returns list of dicts: {ticker, file_date, entity_name, cik, market_cap}
    """
    logger.info(f"Collecting convertible note events: {start_date} to {end_date}")

    # Load CIK map
    cik_map = load_cik_ticker_map()

    # Fetch from EDGAR
    filings = fetch_convertible_8k_efts(start_date, end_date, max_results=500)
    logger.info(f"Primary search: {len(filings)} raw filings")

    if len(filings) < 20:
        logger.info("Few results, trying alternate queries...")
        alt = fetch_convertible_8k_alternate(start_date, end_date)
        logger.info(f"Alternate search: {len(alt)} additional filings")
        filings.extend(alt)

    # Resolve tickers
    resolved = []
    seen_accessions = set()
    for f in filings:
        acc = f.get("accession_no", "")
        if acc in seen_accessions:
            continue
        seen_accessions.add(acc)

        cik = str(f.get("cik", "")).lstrip("0").zfill(10)

        # Try display_names first (more reliable for entity tickers)
        ticker = None
        display_names = f.get("display_names", [])
        if display_names:
            for dn in display_names:
                # display_names format: "Company Name (TICKER) (CIK 0001234567)"
                import re
                match = re.search(r'\(([A-Z]{1,5})\)', dn)
                if match:
                    ticker = match.group(1)
                    break

        if not ticker:
            ticker = resolve_ticker(cik, cik_map)

        if not ticker:
            continue

        # Filter out warrants/units/preferred
        if any(ticker.endswith(s) for s in ['W', 'WS', 'U', 'R', '-P', '-PA', '-PB']):
            continue

        resolved.append({
            "ticker": ticker,
            "file_date": f.get("file_date", ""),
            "entity_name": f.get("entity_name", ""),
            "cik": cik,
        })

    logger.info(f"Resolved {len(resolved)} events with tickers")

    # Get market caps and filter
    unique_tickers = list({r["ticker"] for r in resolved})
    mc_cache = get_market_caps(unique_tickers)

    filtered = []
    for r in resolved:
        mc = mc_cache.get(r["ticker"], 0)
        if mc >= min_market_cap:
            r["market_cap"] = mc
            filtered.append(r)

    logger.info(f"Large-cap (>$1B): {len(filtered)} events")

    # Deduplicate by ticker-month (same company can't have 2 offerings in one month)
    seen = set()
    deduped = []
    for r in sorted(filtered, key=lambda x: x["file_date"]):
        key = (r["ticker"], r["file_date"][:7])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    logger.info(f"Deduplicated: {len(deduped)} events")

    # Year distribution
    year_dist = Counter(r["file_date"][:4] for r in deduped)
    logger.info(f"By year: {dict(sorted(year_dist.items()))}")

    return deduped


# ── Backtest ───────────────────────────────────────────────────────────────────

def run_backtest(events: list, label: str) -> dict:
    """Run measure_event_impact backtest on a list of events."""
    import market_data

    event_dates = [
        {"symbol": e["ticker"], "date": e["file_date"],
         "timing": "after_hours", "entry_price": "open"}
        for e in events
    ]

    if len(event_dates) > 250:
        event_dates = event_dates[:250]

    result = market_data.measure_event_impact(
        event_dates=event_dates,
        benchmark="SPY",
        sector_etf=None,
        estimate_costs=True,
        event_type="convertible_note_offering",
    )

    summary = {"label": label, "n": result["events_measured"]}
    for h in ["1d", "3d", "5d", "10d"]:
        avg = result.get(f"avg_abnormal_{h}")
        pos = result.get(f"positive_rate_abnormal_{h}")
        p = result.get(f"wilcoxon_p_abnormal_{h}")
        if avg is not None:
            summary[h] = {
                "avg": round(avg, 2),
                "neg_rate": round(100 - pos, 1),
                "p": round(p, 4),
            }

    summary["passes_mt"] = result.get("passes_multiple_testing", False)
    summary["sample_sufficient"] = result.get("sample_sufficient", False)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Convertible note offering collector + backtest")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--min-market-cap", type=float, default=1e9)
    parser.add_argument("--backtest", action="store_true", help="Run full backtest with IS/OOS split")
    parser.add_argument("--output", default="data/convertible_events.json")

    args = parser.parse_args()

    # Collect events
    events = collect_convertible_events(args.start, args.end, args.min_market_cap)

    # Save
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(events, f, indent=2)
    print(f"\nSaved {len(events)} events to {output_path}")

    # Year distribution
    year_dist = Counter(e["file_date"][:4] for e in events)
    print(f"By year: {dict(sorted(year_dist.items()))}")

    if not args.backtest:
        print("\nRun with --backtest for IS/OOS temporal validation")
        return

    if len(events) < 20:
        print(f"\nOnly {len(events)} events — too few for backtest. Need broader search.")
        return

    # ── Full set backtest ──
    print(f"\n{'='*60}")
    print("FULL SET BACKTEST")
    print(f"{'='*60}")
    full_summary = run_backtest(events, f"FULL {args.start[:4]}-{args.end[:4]}")
    print(json.dumps(full_summary, indent=2))

    # ── Temporal split: IS = 2021-2023, OOS = 2024-2025 ──
    is_events = [e for e in events if e["file_date"] < "2024-01-01"]
    oos_events = [e for e in events if e["file_date"] >= "2024-01-01"]

    print(f"\nIS (2021-2023): {len(is_events)} events")
    print(f"OOS (2024-2025): {len(oos_events)} events")

    results = {"full": full_summary}

    if len(is_events) >= 15 and len(oos_events) >= 10:
        print(f"\n{'='*60}")
        print(f"IN-SAMPLE 2021-2023 ({len(is_events)} events)")
        print(f"{'='*60}")
        is_summary = run_backtest(is_events, f"IS 2021-2023")
        print(json.dumps(is_summary, indent=2))
        results["is"] = is_summary

        print(f"\n{'='*60}")
        print(f"OUT-OF-SAMPLE 2024-2025 ({len(oos_events)} events)")
        print(f"{'='*60}")
        oos_summary = run_backtest(oos_events, f"OOS 2024-2025")
        print(json.dumps(oos_summary, indent=2))
        results["oos"] = oos_summary
    else:
        print("\nInsufficient data for temporal IS/OOS split")
        results["split_note"] = f"IS={len(is_events)}, OOS={len(oos_events)} — insufficient"

    # ── Winsorized (cap outliers at 5th/95th percentile) ──
    # Already handled by measure_event_impact's winsorized stats

    # ── Final summary ──
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    print(json.dumps(results, indent=2))

    # Save full results
    with open(DATA_DIR / "convertible_backtest_results.json", 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved results to {DATA_DIR / 'convertible_backtest_results.json'}")


if __name__ == "__main__":
    main()
