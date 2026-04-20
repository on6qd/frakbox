#!/usr/bin/env python3
"""8-K Item 3.02 (Unregistered Sale of Equity Securities) scanner.

Item 3.02 is the PIPE / private placement disclosure — dilutive share issuance
outside of a registered offering. Canonical retest validated 2026-04-20:
pooled h=10 SPY-adj -3.48% p=4e-5 at price>$5 (n=644); recent 2023+ -5.21%
p=1e-5 (n=430); 2024+ -6.07% p=1e-4 (n=204). Effect is monotonic in price,
recency, horizon, and chronic-filer count. Chronic repeat filers have STRONGER
signal — do NOT exclude them.

Tradeable cell (knowledge: item_302_pipe_private_placement_short_validated_canonical_2026_04_20):
  8-K Item 3.02 + price > $5 + hold 10d -> short, expected -5.21% SPY-adj,
  65% negative rate, cluster buffer 30d.

Hypothesis id: 84f218f0.

Usage:
    # Recent monitoring (daily use)
    python tools/item_302_pipe_scanner.py --days 3 --json-events

    # Historical scan
    python tools/item_302_pipe_scanner.py --start 2024-01-01 --end 2026-04-20

    # Full backtest
    python tools/item_302_pipe_scanner.py --backtest --start 2023-01-01
"""
import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, '/Users/frakbox/Bots/financial_researcher')

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None

HEADERS = {"User-Agent": "financial-researcher research@example.com"}
SEC_DELAY = 0.15
MIN_MARKET_CAP = 500_000_000   # $500M default (daily scanner baseline)
MIN_PRICE = 5.0                 # Canonical liquidity floor
EFTS_PAGE_SIZE = 100


def search_item_302(start_date: str, end_date: str) -> list[dict]:
    """Search EDGAR EFTS for 8-K filings containing Item 3.02."""
    q = '%22Item+3.02%22'
    base_url = (
        f"https://efts.sec.gov/LATEST/search-index"
        f"?q={q}&forms=8-K"
        f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
    )

    all_hits = []
    url = base_url + f"&from=0&size={EFTS_PAGE_SIZE}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"EFTS error: {resp.status_code}", file=sys.stderr)
        return []

    data = resp.json()
    total = data.get("hits", {}).get("total", {}).get("value", 0)
    hits = data.get("hits", {}).get("hits", [])
    all_hits.extend(hits)
    print(f"  8-K Item 3.02: {total} total filings ({start_date} to {end_date})",
          file=sys.stderr)

    fetched = len(hits)
    max_to_fetch = min(total, 10000)
    while fetched < max_to_fetch:
        time.sleep(SEC_DELAY)
        url = base_url + f"&from={fetched}&size={EFTS_PAGE_SIZE}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  Pagination error at offset {fetched}: {resp.status_code}",
                  file=sys.stderr)
            break
        page_hits = resp.json().get("hits", {}).get("hits", [])
        if not page_hits:
            break
        all_hits.extend(page_hits)
        fetched += len(page_hits)

    results = []
    seen = set()
    for h in all_hits:
        src = h.get("_source", {})
        ciks = src.get("ciks", [])
        names = src.get("display_names", [])
        file_date = src.get("file_date", "")
        items = src.get("items", [])

        # Confirm Item 3.02 in items list
        has_302 = any("3.02" in str(it) for it in items)
        if items and not has_302:
            continue

        cik = ciks[0].lstrip("0") if ciks else ""
        dedup_key = (cik, file_date)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # Extract ticker from display_name
        ticker = None
        if names:
            m = re.search(r'\(([A-Z]{1,5})\)', names[0])
            if m:
                ticker = m.group(1)

        results.append({
            "cik": cik,
            "display_name": names[0] if names else "",
            "ticker": ticker,
            "file_date": file_date,
            "items": items,
            "accession": h.get("_id", ""),
        })

    return results


def filter_largecap_and_price(events: list[dict], min_mcap: float = MIN_MARKET_CAP,
                              min_price: float = MIN_PRICE) -> list[dict]:
    """Filter to large-cap (>$500M) and price > $5 (canonical liquidity floor)."""
    if yf is None:
        print("yfinance not available, skipping cap/price filter", file=sys.stderr)
        return [e for e in events if e.get("ticker")]

    filtered = []
    tickers = list(set(e["ticker"] for e in events if e.get("ticker")))

    for i, tick in enumerate(tickers):
        try:
            info = yf.Ticker(tick).info
            mcap = info.get("marketCap", 0) or 0
            price = info.get("regularMarketPrice") or info.get("previousClose", 0) or 0

            if mcap < min_mcap:
                print(f"  Filter out {tick}: cap ${mcap/1e6:.0f}M < ${min_mcap/1e6:.0f}M",
                      file=sys.stderr)
            elif price < min_price:
                print(f"  Filter out {tick}: price ${price:.2f} < ${min_price:.2f} (liquidity floor)",
                      file=sys.stderr)
            else:
                for e in events:
                    if e.get("ticker") == tick:
                        e["market_cap"] = mcap
                        e["price_at_scan"] = price
                        filtered.append(e)
        except Exception as ex:
            print(f"  Error checking {tick}: {ex}", file=sys.stderr)

        if (i + 1) % 10 == 0:
            print(f"  Cap/price check: {i+1}/{len(tickers)}", file=sys.stderr)
        time.sleep(0.2)

    return filtered


def run_backtest(events: list[dict]) -> dict:
    """Run abnormal return backtest on Item 3.02 events."""
    import market_data
    import db

    db.init_db()

    event_dates = []
    for e in events:
        if e.get("ticker") and e.get("file_date"):
            event_dates.append({"symbol": e["ticker"], "date": e["file_date"]})

    if not event_dates:
        print("No events to backtest", file=sys.stderr)
        return {}

    print(f"\nMeasuring abnormal returns for {len(event_dates)} events...",
          file=sys.stderr)

    result = market_data.measure_event_impact(
        event_dates=event_dates,
        entry_price="open",
        benchmark="SPY",
    )

    print(f"\nEvents measured: {result.get('n_events', 0)}")
    print(f"\n--- ABNORMAL RETURN RESULTS ---")
    print(f"{'Horizon':<12} {'Avg Abn Return':>16} {'Neg% (short)':>14} {'p-value':>10}")
    print("-" * 52)
    for h_key in ['1d', '3d', '5d', '10d']:
        h_data = result.get(h_key, {})
        avg = h_data.get('abnormal_mean', 0)
        neg_rate = h_data.get('negative_rate', 0)
        p = h_data.get('p_value', 1.0)
        print(f"{h_key:<12} {avg:>+14.3f}% {neg_rate:>13.1f}% {p:>10.4f}")

    return {
        "n_events": result.get('n_events', 0),
        "h10_mean": result.get('10d', {}).get('abnormal_mean', 0),
        "h10_p": result.get('10d', {}).get('p_value', 1),
        "h10_neg_rate": result.get('10d', {}).get('negative_rate', 0),
        "full_result": result,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Scan EDGAR for 8-K Item 3.02 PIPE / private placement filings")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Look back N days from today")
    parser.add_argument("--json-events", action="store_true",
                        help="Emit final list as JSON events at end of stdout")
    parser.add_argument("--backtest", action="store_true",
                        help="Run full backtest with abnormal returns")
    parser.add_argument("--no-filter", action="store_true",
                        help="Skip market cap / price filter")
    parser.add_argument("--min-price", type=float, default=MIN_PRICE,
                        help=f"Min price filter (canonical floor ${MIN_PRICE:.2f})")
    parser.add_argument("--min-mcap-m", type=float, default=MIN_MARKET_CAP / 1e6,
                        help=f"Min market cap in millions (default {MIN_MARKET_CAP/1e6:.0f})")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    if args.days:
        start = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
        end = today
    elif args.start:
        start = args.start
        end = args.end or today
    else:
        start = "2024-01-01"
        end = today

    events = search_item_302(start, end)
    print(f"\nRaw events found: {len(events)}", file=sys.stderr)

    events = [e for e in events if e.get("ticker")]
    print(f"Events with tickers: {len(events)}", file=sys.stderr)

    if not args.no_filter and events:
        events = filter_largecap_and_price(
            events, min_mcap=args.min_mcap_m * 1e6, min_price=args.min_price)
        print(f"Passed cap>=${args.min_mcap_m:.0f}M & price>=${args.min_price:.2f}: {len(events)}",
              file=sys.stderr)

    # Dedup: keep only FIRST 3.02 filing per ticker within the scan window.
    # Chronic filer amplification is separate — handled at backtest/analysis time,
    # not at live-trigger time (we only want one position per ticker per window).
    events.sort(key=lambda e: e["file_date"])
    seen_tickers = set()
    deduped = []
    for e in events:
        if e["ticker"] not in seen_tickers:
            seen_tickers.add(e["ticker"])
            deduped.append(e)
    events = deduped

    print(f"\nFinal events (first per ticker): {len(events)}")
    for e in events:
        mcap_str = f" (${e.get('market_cap',0)/1e9:.1f}B)" if e.get('market_cap') else ""
        price_str = f" @ ${e.get('price_at_scan',0):.2f}" if e.get('price_at_scan') else ""
        print(f"  {e['ticker']} {e['file_date']}{mcap_str}{price_str}: "
              f"{e['display_name'][:60]}")

    if args.backtest and events:
        print("\n" + "=" * 70)
        print("RUNNING BACKTEST")
        print("=" * 70)
        run_backtest(events)

    if args.json_events:
        json_events = [
            {
                "symbol": e["ticker"],
                "date": e["file_date"],
                "market_cap": e.get("market_cap", 0),
                "price_at_scan": e.get("price_at_scan", 0),
                "accession": e.get("accession", ""),
            }
            for e in events
        ]
        print(json.dumps(json_events))

    return events


if __name__ == "__main__":
    main()
