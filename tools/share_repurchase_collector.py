#!/usr/bin/env python3
"""Share Repurchase Announcement Collector + Backtest.

Collects open-market share repurchase program announcements from SEC EDGAR 8-K
filings, filters to large-cap (>$1B), deduplicates, and runs backtest.

Academic support:
- Ikenberry, Lakonishok & Vermaelen (1995): +3.5% abnormal 1-3 months
- Peyer & Vermaelen (2009): effect persists, stronger for value stocks
- Manconi, Peyer & Vermaelen (2019): still present internationally

Signal direction: LONG. Companies that announce buybacks signal undervaluation.
Stronger for: value stocks, smaller companies, larger buyback programs.

EDGAR approach:
- Use EFTS full-text search for 8-K filings containing repurchase program language
- Filter for NEW program announcements (not routine mentions)
- Resolve CIK -> ticker via EDGAR bulk JSON
- Filter by market cap (>$1B)
- Deduplicate by ticker-quarter (companies don't announce multiple programs in a quarter)

Usage:
    python tools/share_repurchase_collector.py --start 2021-01-01 --end 2025-12-31
    python tools/share_repurchase_collector.py --backtest

Author: financial-researcher agent
"""
import sys
import json
import time
import argparse
import logging
import re
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


def load_cik_ticker_map() -> dict:
    """Load cached CIK -> ticker map."""
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
    return cik_map


def resolve_ticker(cik: str, cik_map: dict) -> str | None:
    cik_padded = str(cik).lstrip("0").zfill(10)
    entry = cik_map.get(cik_padded)
    return entry["ticker"] if entry else None


def fetch_repurchase_8k_efts(start_date: str, end_date: str, max_results: int = 500) -> list:
    """
    Search EDGAR EFTS for 8-K filings announcing share repurchase programs.
    Uses multiple targeted queries to maximize recall while filtering noise.
    """
    url = "https://efts.sec.gov/LATEST/search-index"
    filings = []
    seen_ids = set()

    # Multiple queries to capture different announcement phrasings
    queries = [
        # Most specific: board authorization language
        '"share repurchase program" "authorized"',
        '"stock repurchase program" "authorized"',
        '"share repurchase program" "approved"',
        '"stock repurchase program" "approved"',
        # Catch dollar amount announcements
        '"share repurchase" "billion" "program"',
        '"buyback program" "authorized"',
    ]

    for q_idx, query in enumerate(queries):
        page_size = 10
        max_pages = min(max_results // page_size + 1, 50)

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
                if resp.status_code != 200:
                    logger.warning(f"EFTS returned {resp.status_code} for query {q_idx}")
                    break
                data = resp.json()
            except Exception as e:
                logger.error(f"EFTS error: {e}")
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                filing_id = hit.get("_id", "")
                if filing_id in seen_ids:
                    continue
                seen_ids.add(filing_id)

                src = hit.get("_source", {})
                cik = filing_id.split(":")[0] if ":" in filing_id else ""

                filings.append({
                    "cik": cik,
                    "entity_name": src.get("entity_name", ""),
                    "display_names": src.get("display_names", []),
                    "file_date": src.get("file_date", ""),
                    "accession_no": filing_id,
                    "query": query,
                })

            total = data.get("hits", {}).get("total", {}).get("value", 0)
            if offset + page_size >= min(total, max_results):
                break

            time.sleep(0.12)

        logger.info(f"Query {q_idx+1}/{len(queries)}: '{query[:40]}...' -> {len([f for f in filings if f['query']==query])} new")
        time.sleep(0.3)

    logger.info(f"Total unique filings across all queries: {len(filings)}")
    return filings


def get_market_caps(tickers: list) -> dict:
    """Get market caps for a list of tickers."""
    import yfinance as yf

    mc_cache_path = DATA_DIR / "repurchase_mc_cache.json"
    if mc_cache_path.exists():
        with open(mc_cache_path) as f:
            cache = json.load(f)
    else:
        cache = {}

    new_tickers = [t for t in tickers if t not in cache]
    if new_tickers:
        logger.info(f"Market cap lookups: {len(new_tickers)} new")
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


def collect_repurchase_events(
    start_date: str = "2021-01-01",
    end_date: str = "2025-12-31",
    min_market_cap: float = 1_000_000_000,
) -> list:
    """
    Collect share repurchase program announcements from EDGAR.
    Returns deduplicated list of events.
    """
    logger.info(f"Collecting repurchase announcements: {start_date} to {end_date}")

    cik_map = load_cik_ticker_map()
    filings = fetch_repurchase_8k_efts(start_date, end_date, max_results=500)

    # Resolve tickers
    resolved = []
    for f in filings:
        cik = str(f.get("cik", "")).lstrip("0").zfill(10)

        # Try display_names first
        ticker = None
        for dn in f.get("display_names", []):
            match = re.search(r'\(([A-Z]{1,5})\)', dn)
            if match:
                ticker = match.group(1)
                break
        if not ticker:
            ticker = resolve_ticker(cik, cik_map)
        if not ticker:
            continue

        # Filter out warrants/units/preferred/ETFs
        if any(ticker.endswith(s) for s in ['W', 'WS', 'U', 'R']) or len(ticker) > 5:
            continue

        resolved.append({
            "ticker": ticker,
            "file_date": f.get("file_date", ""),
            "entity_name": f.get("entity_name", ""),
            "cik": cik,
        })

    logger.info(f"Resolved {len(resolved)} events with tickers")

    # Market cap filter
    unique_tickers = list({r["ticker"] for r in resolved})
    mc_cache = get_market_caps(unique_tickers)

    filtered = [r for r in resolved if mc_cache.get(r["ticker"], 0) >= min_market_cap]
    for r in filtered:
        r["market_cap"] = mc_cache.get(r["ticker"], 0)
    logger.info(f"Large-cap (>$1B): {len(filtered)} events")

    # Deduplicate by ticker-quarter
    seen = set()
    deduped = []
    for r in sorted(filtered, key=lambda x: x["file_date"]):
        # Quarter key: YYYY-Q#
        try:
            dt = datetime.strptime(r["file_date"], "%Y-%m-%d")
            q_key = (r["ticker"], f"{dt.year}-Q{(dt.month-1)//3+1}")
        except ValueError:
            continue
        if q_key not in seen:
            seen.add(q_key)
            deduped.append(r)

    logger.info(f"Deduplicated (ticker-quarter): {len(deduped)} events")

    year_dist = Counter(r["file_date"][:4] for r in deduped)
    logger.info(f"By year: {dict(sorted(year_dist.items()))}")

    return deduped


def run_backtest(events: list, label: str) -> dict:
    """Run measure_event_impact backtest."""
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
        event_type="share_repurchase_announcement",
    )

    summary = {"label": label, "n": result["events_measured"]}
    for h in ["1d", "3d", "5d", "10d", "20d"]:
        avg = result.get(f"avg_abnormal_{h}")
        pos = result.get(f"positive_rate_abnormal_{h}")
        p = result.get(f"wilcoxon_p_abnormal_{h}")
        if avg is not None:
            summary[h] = {
                "avg": round(avg, 2),
                "pos_rate": round(pos, 1),
                "p": round(p, 4),
            }

    summary["passes_mt"] = result.get("passes_multiple_testing", False)
    summary["sample_sufficient"] = result.get("sample_sufficient", False)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Share repurchase announcement collector + backtest")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--min-market-cap", type=float, default=1e9)
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument("--output", default="data/repurchase_events.json")
    args = parser.parse_args()

    events = collect_repurchase_events(args.start, args.end, args.min_market_cap)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(events, f, indent=2)
    print(f"\nSaved {len(events)} events to {output_path}")

    year_dist = Counter(e["file_date"][:4] for e in events)
    print(f"By year: {dict(sorted(year_dist.items()))}")

    if not args.backtest:
        print("\nRun with --backtest for IS/OOS validation")
        return

    if len(events) < 20:
        print(f"\nOnly {len(events)} events — too few for backtest")
        return

    # Full set
    print(f"\n{'='*60}")
    print("FULL SET BACKTEST")
    print(f"{'='*60}")
    full_summary = run_backtest(events, f"FULL {args.start[:4]}-{args.end[:4]}")
    print(json.dumps(full_summary, indent=2))

    # IS/OOS split: 2021-2023 vs 2024-2025
    is_events = [e for e in events if e["file_date"] < "2024-01-01"]
    oos_events = [e for e in events if e["file_date"] >= "2024-01-01"]
    print(f"\nIS (2021-2023): {len(is_events)} events")
    print(f"OOS (2024-2025): {len(oos_events)} events")

    results = {"full": full_summary}

    if len(is_events) >= 15 and len(oos_events) >= 10:
        print(f"\n{'='*60}")
        print(f"IN-SAMPLE 2021-2023 ({len(is_events)} events)")
        print(f"{'='*60}")
        is_summary = run_backtest(is_events, "IS 2021-2023")
        print(json.dumps(is_summary, indent=2))
        results["is"] = is_summary

        print(f"\n{'='*60}")
        print(f"OUT-OF-SAMPLE 2024-2025 ({len(oos_events)} events)")
        print(f"{'='*60}")
        oos_summary = run_backtest(oos_events, "OOS 2024-2025")
        print(json.dumps(oos_summary, indent=2))
        results["oos"] = oos_summary

    # Large-cap (>$10B) subset — should be weaker per academic literature
    mega_events = [e for e in events if e.get("market_cap", 0) >= 10_000_000_000]
    if len(mega_events) >= 20:
        print(f"\n{'='*60}")
        print(f"MEGA-CAP (>$10B) SUBSET ({len(mega_events)} events)")
        print(f"{'='*60}")
        mega_summary = run_backtest(mega_events, "MEGA-CAP >$10B")
        print(json.dumps(mega_summary, indent=2))
        results["mega_cap"] = mega_summary

    # Small-large-cap ($1B-$10B) subset — should be stronger
    mid_events = [e for e in events if 1_000_000_000 <= e.get("market_cap", 0) < 10_000_000_000]
    if len(mid_events) >= 20:
        print(f"\n{'='*60}")
        print(f"MID-CAP ($1-10B) SUBSET ({len(mid_events)} events)")
        print(f"{'='*60}")
        mid_summary = run_backtest(mid_events, "MID-CAP $1-10B")
        print(json.dumps(mid_summary, indent=2))
        results["mid_cap"] = mid_summary

    # Save
    with open(DATA_DIR / "repurchase_backtest_results.json", 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n{'='*60}")
    print("FINAL RESULTS")
    print(f"{'='*60}")
    print(json.dumps(results, indent=2))
    print(f"\nSaved to {DATA_DIR / 'repurchase_backtest_results.json'}")


if __name__ == "__main__":
    main()
