#!/usr/bin/env python3
"""8-K Item 3.01 (Failure to Satisfy Listing Standards) scanner and backtester.

Companies listed on NYSE/Nasdaq must file an 8-K within 4 business days of
receiving a delisting notice or failing to satisfy listing standards. The
hypothesis is that forced institutional selling and retail panic drive
sustained negative abnormal returns after filing.

Hypothesis:
  Large-cap companies filing an 8-K with Item 3.01 produce negative abnormal
  returns of -5% or more over 5-10 days after the filing date.

Causal mechanism:
  1. Actors: Companies failing market cap, bid price, or financial thresholds
  2. Transmission: Index fund forced selling, institutional mandate violations,
     retail panic, potential OTC relisting uncertainty
  3. Useful note: Item 3.01 is a LEADING indicator — it precedes actual
     delisting by 30-180 days during which selling pressure persists.

Usage:
    # Historical scan
    python tools/delisting_8k_scanner.py --start 2023-01-01 --end 2025-12-31

    # Recent monitoring
    python tools/delisting_8k_scanner.py --days 30

    # Full backtest with abnormal return measurement
    python tools/delisting_8k_scanner.py --backtest --start 2023-01-01 --end 2025-12-31

    # JSON events for data_tasks.py
    python tools/delisting_8k_scanner.py --start 2023-01-01 --end 2025-12-31 --json-events
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

HEADERS = {"User-Agent": "financial-researcher research@frakbox.io"}
SEC_DELAY = 0.15
MIN_MARKET_CAP = 500_000_000  # $500M
EFTS_PAGE_SIZE = 100

# Text patterns that indicate a VOLUNTARY exchange transfer (neutral/positive signal)
VOLUNTARY_PATTERNS = [
    "acting pursuant to authorization from its board",
    "intention to voluntarily withdraw",
    "voluntarily withdraw its listing",
    "notified the new york stock exchange of its intention",
    "notified nasdaq of its intention",
    "notified the nyse of its intention",
    "voluntarily transfer",
]

# Text patterns that indicate a FORCED delisting warning (negative signal — our target)
FORCED_PATTERNS = [
    "received a notice",
    "received a letter",
    "notice of deficiency",
    "notice of non-compliance",
    "notice of noncompliance",
    "noncompliance with",
    "non-compliance with",
    "failure to satisfy",
    "failure to meet",
    "does not comply",
    "did not comply",
    "failed to comply",
    "deficiency notice",
    "listing rule violation",
    "listing rules related to",
    "reprimand",
    "panel determination",
    "delisting determination",
    "delist",
]


def classify_filing_content(text: str) -> str:
    """
    Classify Item 3.01 filing as 'forced', 'voluntary', or 'unknown'
    by inspecting the text around the Item 3.01 section.

    Returns: 'forced' | 'voluntary' | 'unknown'
    """
    import re
    idx = text.lower().find("3.01")
    if idx < 0:
        return "unknown"

    # Extract ~1500 chars after the section header
    snippet = text[idx: idx + 1500]
    snippet_clean = re.sub(r"<[^>]+>", " ", snippet).lower()

    # Check voluntary first (many forced notices also mention "transfer" generically)
    for pat in VOLUNTARY_PATTERNS:
        if pat in snippet_clean:
            return "voluntary"

    # Then check forced
    for pat in FORCED_PATTERNS:
        if pat in snippet_clean:
            return "forced"

    return "unknown"


def fetch_filing_text(accession_id: str, cik: str) -> str | None:
    """Fetch the primary document of an 8-K filing from EDGAR."""
    parts = accession_id.split(":")
    if len(parts) < 2:
        return None
    raw_acc = parts[0].replace("-", "")
    file_name = parts[1]
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{raw_acc}/{file_name}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def search_item_301(start_date: str, end_date: str) -> list[dict]:
    """Search EDGAR EFTS for 8-K filings containing Item 3.01."""
    # Use quoted phrase for precision; Item 3.01 = Failure to Meet Listing Standards
    q = '%22Item+3.01%22'
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
    print(f"  8-K Item 3.01: {total} total filings found ({start_date} to {end_date})", file=sys.stderr)

    fetched = len(hits)
    max_to_fetch = min(total, 10000)
    while fetched < max_to_fetch:
        time.sleep(SEC_DELAY)
        url = base_url + f"&from={fetched}&size={EFTS_PAGE_SIZE}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  Pagination error at offset {fetched}: {resp.status_code}", file=sys.stderr)
            break
        page_hits = resp.json().get("hits", {}).get("hits", [])
        if not page_hits:
            break
        all_hits.extend(page_hits)
        fetched += len(page_hits)

    results = []
    seen = set()  # Dedup by (cik, file_date)
    for h in all_hits:
        src = h.get("_source", {})
        ciks = src.get("ciks", [])
        names = src.get("display_names", [])
        file_date = src.get("file_date", "")
        items = src.get("items", [])

        # Confirm Item 3.01 is present in items list (if items metadata is populated)
        has_301 = any("3.01" in str(it) for it in items)
        if items and not has_301:
            continue

        cik = ciks[0].lstrip("0") if ciks else ""
        dedup_key = (cik, file_date)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # Extract ticker from display_name e.g. "ACME Corp (ACME)"
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


def classify_events(events: list[dict], verbose: bool = True) -> list[dict]:
    """
    Fetch each filing and classify as 'forced' or 'voluntary' transfer.
    Adds 'filing_type' key to each event dict.
    Only fetches for events that don't already have a classification.
    """
    for i, e in enumerate(events):
        if e.get("filing_type"):
            continue
        acc = e.get("accession", "")
        cik = e.get("cik", "")
        if not acc or not cik:
            e["filing_type"] = "unknown"
            continue
        text = fetch_filing_text(acc, cik)
        if text:
            e["filing_type"] = classify_filing_content(text)
        else:
            e["filing_type"] = "unknown"
        if verbose:
            print(
                f"  [{i+1}/{len(events)}] {e.get('ticker','?')} {e.get('file_date','?')}: {e['filing_type']}",
                file=sys.stderr,
            )
        time.sleep(SEC_DELAY)
    return events


def filter_largecap(events: list[dict]) -> list[dict]:
    """Filter to large-cap stocks (>$500M market cap) using yfinance."""
    if yf is None:
        print("yfinance not available, skipping market cap filter", file=sys.stderr)
        return [e for e in events if e.get("ticker")]

    # Use largecap_filter module for caching and batch efficiency
    try:
        import pandas as pd
        from tools.largecap_filter import filter_to_largecap as _filter_lc

        df = pd.DataFrame(events)
        df_with_tickers = df[df["ticker"].notna()].copy()
        if df_with_tickers.empty:
            return []

        df_filtered = _filter_lc(df_with_tickers, min_market_cap_m=500, ticker_col="ticker")
        return df_filtered.to_dict("records")

    except Exception as ex:
        print(f"  largecap_filter failed ({ex}), falling back to inline check", file=sys.stderr)

    # Inline fallback
    filtered = []
    tickers = list(set(e["ticker"] for e in events if e.get("ticker")))

    for i, tick in enumerate(tickers):
        try:
            info = yf.Ticker(tick).info
            mcap = info.get("marketCap", 0) or 0
            if mcap >= MIN_MARKET_CAP:
                for e in events:
                    if e.get("ticker") == tick:
                        e["market_cap"] = mcap
                        filtered.append(e)
            else:
                print(f"  Filtered out {tick}: market cap ${mcap/1e6:.0f}M < $500M", file=sys.stderr)
        except Exception as ex:
            print(f"  Error checking {tick}: {ex}", file=sys.stderr)

        if (i + 1) % 10 == 0:
            print(f"  Market cap check: {i+1}/{len(tickers)}", file=sys.stderr)
        time.sleep(0.2)

    return filtered


def run_backtest(events: list[dict]) -> dict:
    """Run abnormal return backtest on delisting notice events."""
    import market_data
    import db
    from research import record_known_effect, record_dead_end

    db.init_db()

    # Build event list for measure_event_impact
    event_dates = []
    for e in events:
        if e.get("ticker") and e.get("file_date"):
            event_dates.append({
                "symbol": e["ticker"],
                "date": e["file_date"]
            })

    if not event_dates:
        print("No events to backtest", file=sys.stderr)
        return {}

    print(f"\nMeasuring abnormal returns for {len(event_dates)} events...", file=sys.stderr)

    result = market_data.measure_event_impact(
        event_dates=event_dates,
        entry_price="open",  # after-hours filings, enter at next open
        benchmark="SPY",
    )

    # Print results
    print(f"\nEvents measured: {result.get('n_events', 0)}")
    print(f"\n--- ABNORMAL RETURN RESULTS (SHORT HYPOTHESIS) ---")
    print(f"{'Horizon':<12} {'Avg Abn Return':>16} {'Dir% (short)':>14} {'p-value':>10}")
    print("-" * 56)

    for h_key in ['1d', '3d', '5d', '10d']:
        h_data = result.get(h_key, {})
        avg = h_data.get('abnormal_mean', 0)
        neg_rate = h_data.get('negative_rate', 0)
        p = h_data.get('p_value', 1.0)
        print(f"{h_key:<12} {avg:>+14.3f}% {neg_rate:>13.1f}% {p:>10.4f}")

    # OOS split: discovery = first 60%, validation = last 40%
    sorted_events = sorted(event_dates, key=lambda e: e["date"])
    split_idx = int(len(sorted_events) * 0.6)
    discovery = sorted_events[:split_idx]
    validation = sorted_events[split_idx:]

    print(f"\n--- OUT-OF-SAMPLE ANALYSIS ---")
    if len(discovery) >= 5:
        disc_result = market_data.measure_event_impact(
            event_dates=discovery, entry_price="open",
            benchmark="SPY",
        )
        for h_key in ['1d', '3d', '5d', '10d']:
            h_data = disc_result.get(h_key, {})
            avg = h_data.get('abnormal_mean', 0)
            neg_rate = h_data.get('negative_rate', 0)
            print(f"  DISCOVERY n={disc_result.get('n_events',0)} | {h_key}: avg={avg:+.2f}% neg_rate={neg_rate:.1f}%")

    if len(validation) >= 3:
        val_result = market_data.measure_event_impact(
            event_dates=validation, entry_price="open",
            benchmark="SPY",
        )
        for h_key in ['1d', '3d', '5d', '10d']:
            h_data = val_result.get(h_key, {})
            avg = h_data.get('abnormal_mean', 0)
            neg_rate = h_data.get('negative_rate', 0)
            print(f"  VALIDATION n={val_result.get('n_events',0)} | {h_key}: avg={avg:+.2f}% neg_rate={neg_rate:.1f}%")

    passes_mt = result.get('passes_multiple_testing', False)
    print(f"\nPasses multiple testing correction: {passes_mt}")

    # Find best horizon
    best_horizon = None
    best_abs = 0
    for h_key in ['1d', '3d', '5d', '10d']:
        h_data = result.get(h_key, {})
        avg = abs(h_data.get('abnormal_mean', 0))
        if avg > best_abs:
            best_abs = avg
            best_horizon = h_key

    h_data = result.get(best_horizon, {}) if best_horizon else {}

    assessment = {
        "status": "",
        "hypothesis_class": "event",
        "expected_direction": "short",
        "universe": (
            f"EDGAR 8-K Item 3.01, large-cap >$500M, "
            f"{events[0]['file_date']} to {events[-1]['file_date']}"
        ),
        "n_events": result.get('n_events', 0),
        "best_horizon": best_horizon,
        "avg_abnormal": h_data.get('abnormal_mean', 0),
        "p_value": h_data.get('p_value', 1.0),
        "neg_rate": h_data.get('negative_rate', 0),
        "passes_mt": passes_mt,
        "discovery_n": len(discovery),
        "validation_n": len(validation),
        "sample_events": [f"{e['symbol']} {e['date']}" for e in event_dates[:10]],
        "full_result": result,
    }

    # Evaluation criteria
    neg_rate = h_data.get('negative_rate', 0)
    p = h_data.get('p_value', 1.0)
    avg_abn = h_data.get('abnormal_mean', 0)
    n = result.get('n_events', 0)

    checks = {
        "n_sufficient": n >= 10,
        "passes_mt": passes_mt,
        "direction_correct": neg_rate > 50,
        "abnormal_above_threshold": abs(avg_abn) > 0.5,
        "return_after_costs": abs(avg_abn) > 0.416,
    }

    failed = [k for k, v in checks.items() if not v]

    if not failed:
        assessment["status"] = "VALIDATED"
    elif len(failed) <= 2 and checks.get("direction_correct") and checks.get("n_sufficient"):
        assessment["status"] = "PRELIMINARY_NEEDS_MORE_DATA"
    else:
        assessment["status"] = "DEAD_END"

    # Record in knowledge base
    if "DEAD_END" in assessment["status"]:
        record_dead_end(
            "delisting_8k_item_301_short",
            f"Signal failed check(s): {', '.join(failed)}. n={n}, "
            f"best_horizon={best_horizon}, avg_abnormal={avg_abn:.3f}%, "
            f"p={p:.4f}, neg_rate={neg_rate:.1f}%, passes_mt={passes_mt}."
        )

    record_known_effect("delisting_8k_item_301_short", assessment)

    return assessment


def main():
    parser = argparse.ArgumentParser(
        description="Scan EDGAR for 8-K Item 3.01 delisting/listing failure notices"
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, help="Look back N days from today")
    parser.add_argument("--json-events", action="store_true", help="Output as JSON events")
    parser.add_argument("--backtest", action="store_true", help="Run full backtest with abnormal returns")
    parser.add_argument("--no-filter", action="store_true", help="Skip market cap filter")
    parser.add_argument("--no-dedup", action="store_true", help="Keep all filings per ticker (not just first)")
    parser.add_argument(
        "--classify", action="store_true",
        help="Fetch each filing to classify as forced delisting vs. voluntary transfer",
    )
    parser.add_argument(
        "--forced-only", action="store_true",
        help="After classification, keep only forced delisting events (implies --classify)",
    )
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    if args.days:
        start = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")
        end = today
    elif args.start:
        start = args.start
        end = args.end or today
    else:
        start = "2023-01-01"
        end = today

    # Search EDGAR EFTS
    print(f"Scanning EDGAR for 8-K Item 3.01 filings from {start} to {end}...", file=sys.stderr)
    events = search_item_301(start, end)
    print(f"\nRaw events found: {len(events)}", file=sys.stderr)

    # Filter to those with extractable tickers
    events = [e for e in events if e.get("ticker")]
    print(f"Events with tickers: {len(events)}", file=sys.stderr)

    # Filter to large-cap
    if not args.no_filter and events:
        events = filter_largecap(events)
        print(f"Large-cap events (>$500M): {len(events)}", file=sys.stderr)

    # Dedup: keep only FIRST filing per ticker (initial notice is the signal)
    if not args.no_dedup:
        events.sort(key=lambda e: e["file_date"])
        seen_tickers = set()
        deduped = []
        followups = []
        for e in events:
            if e["ticker"] not in seen_tickers:
                seen_tickers.add(e["ticker"])
                deduped.append(e)
            else:
                followups.append(e)
        if followups:
            print(
                f"  Removed {len(followups)} follow-up filings (keeping first per ticker)",
                file=sys.stderr
            )
        events = deduped

    # Content classification (fetch each filing to distinguish forced vs voluntary)
    if args.classify or args.forced_only:
        print(f"\nClassifying {len(events)} events (fetching filing content)...", file=sys.stderr)
        events = classify_events(events)
        forced = [e for e in events if e.get("filing_type") == "forced"]
        voluntary = [e for e in events if e.get("filing_type") == "voluntary"]
        unknown = [e for e in events if e.get("filing_type") == "unknown"]
        print(
            f"Classification: {len(forced)} forced, {len(voluntary)} voluntary, {len(unknown)} unknown",
            file=sys.stderr,
        )
        if args.forced_only:
            events = forced
            print(f"Keeping only forced delistings: {len(events)} events", file=sys.stderr)

    print(f"\nFinal events: {len(events)}")
    for e in events:
        mcap = e.get('market_cap')
        mcap_str = f" (${mcap/1e9:.1f}B)" if mcap and mcap > 1e9 else (f" (${mcap/1e6:.0f}M)" if mcap else "")
        ft = e.get('filing_type', '')
        ft_str = f" [{ft}]" if ft else ""
        print(f"  {e['ticker']} {e['file_date']}{mcap_str}{ft_str}: {e['display_name'][:55]}")

    if args.json_events:
        json_events = [{"symbol": e["ticker"], "date": e["file_date"]} for e in events]
        print(json.dumps(json_events))

    if args.backtest and events:
        if len(events) < 10:
            print(f"\nWARNING: Only {len(events)} events — below 10 minimum for robust backtest. Proceeding anyway.", file=sys.stderr)
        print("\n" + "=" * 70)
        print("RUNNING BACKTEST")
        print("=" * 70)
        result = run_backtest(events)
        print(f"\nStatus: {result.get('status', 'N/A')}")
        print(f"Assessment: {json.dumps({k: v for k, v in result.items() if k != 'full_result'}, indent=2)}")

    return events


if __name__ == "__main__":
    main()
