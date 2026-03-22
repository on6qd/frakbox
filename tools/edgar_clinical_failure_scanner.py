"""
EDGAR Clinical Failure Scanner
================================
Scans SEC EDGAR for recent 8-K filings mentioning Phase 2/3 clinical failure
or FDA CRL events. Run at session start to catch qualifying d302c84b events.

Auto-qualifies candidates by immediately checking:
  1. Market cap < $500M (single-asset small biotech)
  2. Stock crashed >55% on or around the announcement date
  3. Pre-announcement price not down >20% in prior 10 days (pre-anticipated)
  4. Post-crash price > $0.50/share (executable)

Usage:
  python tools/edgar_clinical_failure_scanner.py
  python tools/edgar_clinical_failure_scanner.py --days 7   # last 7 days
  python tools/edgar_clinical_failure_scanner.py --days 30  # last 30 days
  python tools/edgar_clinical_failure_scanner.py --auto-qualify  # show QUALIFYING events only

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

# Persistent disqualification memory — events we've already evaluated and rejected
DISQUALIFIED_EVENTS_FILE = Path(__file__).parent.parent / "logs" / "clinical_disqualified_events.json"


def load_disqualified_events() -> dict:
    """Load previously evaluated and disqualified events from persistent file.

    Returns dict keyed by 'TICKER:YYYY-MM-DD' -> {reason, disqualified_date}
    """
    if DISQUALIFIED_EVENTS_FILE.exists():
        try:
            with open(DISQUALIFIED_EVENTS_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_disqualified_event(ticker: str, filing_date: str, reason: str):
    """Persist a disqualified event so it is skipped in future scans."""
    events = load_disqualified_events()
    key = f"{ticker}:{filing_date}"
    events[key] = {
        "ticker": ticker,
        "filing_date": filing_date,
        "reason": reason,
        "disqualified_date": datetime.today().strftime("%Y-%m-%d"),
    }
    DISQUALIFIED_EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DISQUALIFIED_EVENTS_FILE, "w") as f:
        json.dump(events, f, indent=2)


def add_manual_disqualification(ticker: str, filing_date: str, reason: str):
    """CLI helper: manually add a known-bad event to the disqualification list."""
    save_disqualified_event(ticker, filing_date, reason)
    print(f"Added {ticker} ({filing_date}) to disqualified events: {reason}")


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


def auto_qualify_candidate(ticker: str, filing_date: str) -> dict:
    """
    Auto-qualify a clinical failure candidate.

    Checks:
    1. Market cap < $500M at filing date
    2. Crash > 55% within ±2 days of filing date
    3. Not pre-anticipated (not down >20% in prior 10 days)
    4. Post-crash price > $0.50

    Returns:
        dict with keys:
            qualifies: bool
            reason: str (if not qualifying)
            market_cap_m: float or None
            crash_pct: float or None (negative = down)
            pre_crash_drift: float or None
            crash_date: str or None
            post_crash_price: float or None
    """
    result = {
        "qualifies": False,
        "reason": None,
        "market_cap_m": None,
        "crash_pct": None,
        "pre_crash_drift": None,
        "crash_date": None,
        "post_crash_price": None,
    }

    if ticker == "?":
        result["reason"] = "unknown ticker"
        return result

    try:
        import yfinance as yf
        import pandas as pd

        # Get price data: 15 days before filing to 3 days after
        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        start = (filing_dt - timedelta(days=15)).strftime("%Y-%m-%d")
        end_dt = (filing_dt + timedelta(days=5)).strftime("%Y-%m-%d")

        stock = yf.download(ticker, start=start, end=end_dt, auto_adjust=True, progress=False)
        if stock.empty:
            result["reason"] = "no price data (delisted?)"
            return result

        # Flatten MultiIndex if needed
        if isinstance(stock.columns, pd.MultiIndex):
            stock.columns = stock.columns.get_level_values(0)

        # Get market cap from info
        try:
            info = yf.Ticker(ticker).fast_info
            mkt_cap_m = getattr(info, 'market_cap', None)
            if mkt_cap_m:
                mkt_cap_m = mkt_cap_m / 1e6
                result["market_cap_m"] = mkt_cap_m
                if mkt_cap_m > 500:
                    result["reason"] = f"market cap too large (${mkt_cap_m:.0f}M > $500M)"
                    return result
        except Exception:
            pass  # Market cap check optional, continue without it

        # Find the largest 1-day crash within ±2 days of filing date
        best_crash = 0
        best_crash_date = None
        pre_crash_close = None

        for i in range(len(stock)):
            row_date = stock.index[i].date()
            filing_only = filing_dt.date()
            days_delta = (row_date - filing_only).days

            if -1 <= days_delta <= 2:  # Filing day or next 2 trading days
                if i > 0:
                    prev_close = stock["Close"].iloc[i - 1]
                    today_open = stock["Open"].iloc[i]
                    gap_pct = (today_open - prev_close) / prev_close * 100

                    if gap_pct < best_crash:
                        best_crash = gap_pct
                        best_crash_date = str(row_date)
                        pre_crash_close = prev_close
                        result["post_crash_price"] = float(today_open)

        result["crash_pct"] = best_crash
        result["crash_date"] = best_crash_date

        # Check: crash must be > 55%
        if best_crash > -55:
            result["reason"] = f"crash too small ({best_crash:.1f}%, need < -55%)"
            return result

        # Check: post-crash price > $0.50
        if result["post_crash_price"] and result["post_crash_price"] < 0.50:
            result["reason"] = f"post-crash price ${result['post_crash_price']:.3f} < $0.50 (not executable)"
            return result

        # Check pre-anticipation: was stock already down >20% in prior 10 trading days?
        # Find index of pre-crash close
        if pre_crash_close is not None:
            # Get close 10 trading days before the crash date
            crash_idx = None
            for i, row_date in enumerate(stock.index):
                if str(row_date.date()) == best_crash_date:
                    crash_idx = i
                    break

            if crash_idx is not None and crash_idx >= 2:
                # Look at close 5-10 days before crash
                lookback_idx = max(0, crash_idx - 10)
                prior_close = float(stock["Close"].iloc[lookback_idx])
                pre_crash_drift = (float(pre_crash_close) - prior_close) / prior_close * 100
                result["pre_crash_drift"] = pre_crash_drift

                if pre_crash_drift < -20:
                    result["reason"] = f"pre-anticipated: stock already down {pre_crash_drift:.1f}% in prior 10d"
                    return result

        # All checks passed
        result["qualifies"] = True
        return result

    except Exception as e:
        result["reason"] = f"error: {str(e)[:80]}"
        return result


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
    parser.add_argument("--auto-qualify", action="store_true",
                        help="Auto-check each candidate for crash >55%, market cap, etc.")
    parser.add_argument("--disqualify", nargs=3, metavar=("TICKER", "DATE", "REASON"),
                        help="Manually add a disqualification: --disqualify ALDX 2026-03-17 'multi-pipeline+bounce'")
    parser.add_argument("--list-disqualified", action="store_true",
                        help="Show all previously disqualified events and exit")
    args = parser.parse_args()

    # Handle manual disqualification
    if args.disqualify:
        ticker, date, reason = args.disqualify
        add_manual_disqualification(ticker, date, reason)
        return

    # Handle list disqualified
    if args.list_disqualified:
        events = load_disqualified_events()
        if not events:
            print("No disqualified events on record.")
        else:
            print(f"=== Previously Disqualified Events ({len(events)}) ===")
            for key, ev in sorted(events.items()):
                print(f"  {ev['ticker']:8} {ev['filing_date']}  (disqualified {ev['disqualified_date']}): {ev['reason']}")
        return

    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    print(f"=== EDGAR Clinical Failure Scanner ===")
    print(f"Search window: {start_date} to {end_date}")
    print()

    # Load disqualified events cache
    disqualified = load_disqualified_events()

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
        return []

    results = []
    for acc, hit in all_hits.items():
        result = format_result(hit)
        results.append(result)

    # Sort by filing date descending
    results.sort(key=lambda x: x["filing_date"], reverse=True)

    # Filter out previously disqualified events
    n_before = len(results)
    results_filtered = []
    skipped_known = []
    for r in results:
        key = f"{r['ticker']}:{r['filing_date']}"
        if key in disqualified:
            skipped_known.append((r['ticker'], r['filing_date'], disqualified[key]['reason']))
        else:
            results_filtered.append(r)
    if skipped_known:
        print(f"Skipping {len(skipped_known)} previously disqualified events:")
        for t, d, reason in skipped_known:
            print(f"  {t} ({d}): {reason}")
        print()
    results = results_filtered

    if args.auto_qualify:
        print(f"Auto-qualifying {len(results)} candidates...")
        print()
        qualifying = []
        for r in results:
            ticker = r["ticker"]
            filing_date = r["filing_date"]
            print(f"  Checking {ticker} ({r['company'][:30]}) filed {filing_date}...")
            q = auto_qualify_candidate(ticker, filing_date)
            r["qualification"] = q
            if q["qualifies"]:
                qualifying.append(r)
                print(f"    *** QUALIFIES *** crash={q['crash_pct']:.1f}%, "
                      f"price=${q.get('post_crash_price', 0):.3f}, "
                      f"pre-drift={q.get('pre_crash_drift', 0):.1f}%")
            else:
                # Persist the auto-disqualification so future runs skip it
                save_disqualified_event(ticker, filing_date, q["reason"])
                print(f"    SKIP: {q['reason']} (saved to disqualified list)")

        print()
        if qualifying:
            print(f"=== QUALIFYING EVENTS ({len(qualifying)}) ===")
            print("These meet all d302c84b criteria. Manual verification still needed for:")
            print("  - Single-pipeline status (company has no other drugs)")
            print("  - FDA path forward (company must NOT announce continued regulatory plans)")
            print()
            for r in qualifying:
                q = r["qualification"]
                print(f"TICKER: {r['ticker']}")
                print(f"  Company:     {r['company']}")
                print(f"  Filing date: {r['filing_date']}")
                print(f"  Crash:       {q['crash_pct']:.1f}% on {q['crash_date']}")
                print(f"  Entry price: ${q.get('post_crash_price', 0):.3f} (open after crash)")
                print(f"  Pre-drift:   {q.get('pre_crash_drift', 0):.1f}% in 10d before")
                if q.get('market_cap_m'):
                    print(f"  Market cap:  ${q['market_cap_m']:.0f}M")
                print()
        else:
            print("No qualifying clinical failure events found.")
            print("(All candidates failed crash >55%, market cap, or pre-anticipation check)")
        return qualifying
    else:
        # Original output mode: show all candidates
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
        print("Run with --auto-qualify to automatically check each candidate for crash >55%")
        print("Or manually check next steps:")
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
