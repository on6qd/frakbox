"""
CEO Departure Daily Scanner

Runs daily via launchd to detect large-cap CEO performance failure departures.
Fires EDGAR 8-K scan for last 2 days, filters for performance failures,
checks stock direction, and logs qualifying events.

Usage:
    python tools/ceo_departure_daily_scan.py
    python tools/ceo_departure_daily_scan.py --days 7  # look back 7 days

Hypothesis: 5dbcfb37 (CEO performance failure departure short)
Signal: SHORT at next open after announcement, 1d hold
Expected return: -3.25% abnormal, p=0.0034, N=13, CI=[-5.16, -1.52]

NOTE: Signal fires in OVERNIGHT GAP. After-hours order needed.
Use trigger="after_hours_immediate" in trade loop.
"""

import sys
import json
import argparse
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.edgar_ceo_departure_scanner import scan_ceo_departures

LOG_PATH = Path(__file__).parent.parent / "logs" / "ceo_departure_scan.log"
import db as _db

# Known relief-rally exclusions from hypothesis 5dbcfb37
# These companies show POSITIVE stock reaction to CEO departure -- do NOT short
RELIEF_RALLY_EXCLUSIONS = [
    "PTON", "VSCO", "DIS", "LYFT", "SBUX",
    "LULU",  # March 2026 - rose +3.8% on CEO departure
    "DLTR",  # March 2026 - rose on CEO departure
    "ATR",   # March 2026 - planned retirement, not performance failure
]

def main():
    parser = argparse.ArgumentParser(description="CEO departure daily scanner")
    parser.add_argument("--days", type=int, default=2,
                        help="Number of days to look back (default: 2)")
    parser.add_argument("--min-cap", type=float, default=2000,
                        help="Minimum market cap in $M (default: 2000)")
    args = parser.parse_args()

    end = datetime.date.today().strftime("%Y-%m-%d")
    start = (datetime.date.today() - datetime.timedelta(days=args.days)).strftime("%Y-%m-%d")

    timestamp = datetime.datetime.now().isoformat()
    print(f"=== CEO Departure Daily Scanner ===")
    print(f"Date: {timestamp[:19]}")
    print(f"Scanning: {start} to {end}")
    print(f"Min market cap: ${args.min_cap:.0f}M")
    print()

    # Run scan
    results = scan_ceo_departures(
        start_date=start,
        end_date=end,
        min_cap_m=args.min_cap,
        check_direction=True,
        filter_departure_type=True
    )

    # Filter for qualifying signals
    today = datetime.date.today()
    qualifying = []
    for r in results:
        ticker = r.get("ticker", "")

        # Check trading window: event must be within last 2 calendar days to be actionable
        # (CEO departure signal requires entry at next-day open — stale events can't be traded)
        filing_date_str = r.get("filing_date", "")[:10]
        try:
            filing_date = datetime.date.fromisoformat(filing_date_str)
            days_old = (today - filing_date).days
            if days_old > 2:
                print(f"SKIP {ticker}: event {filing_date_str} is {days_old} days old (window closed)")
                continue
        except ValueError:
            pass  # If date parse fails, continue checking

        if not r.get("stock_fell", False):
            continue
        if ticker in RELIEF_RALLY_EXCLUSIONS:
            print(f"EXCLUDED {ticker} (known relief-rally pattern)")
            continue
        if r.get("departure_type") in ("planned", "not_ceo"):
            continue

        overnight = r.get("overnight_return_pct", 0)
        # Minimum 2% drop to qualify
        if overnight > -2.0:
            print(f"SKIP {ticker}: overnight drop only {overnight:.1f}% (need < -2.0%)")
            continue

        # Welcome departure qualifier: if stock was RECOVERING before announcement,
        # market likely celebrates the CEO removal — not a short signal.
        # LULU March 2024: 20d prior abnormal = +5.7%, stock rose +3.8% on departure.
        prior_abn = r.get("prior_20d_abnormal_return_pct")
        if prior_abn is not None and prior_abn > 0.0:
            print(f"SKIP {ticker}: welcome departure — 20d prior abnormal = +{prior_abn:.1f}% (market relieved)")
            continue

        qualifying.append(r)

    # Log to file
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(f"\n=== {timestamp[:19]} | Scan {start} to {end} ===\n")
        f.write(f"Events checked: {len(results)}\n")
        f.write(f"Qualifying performance failures: {len(qualifying)}\n")
        for r in qualifying:
            ticker = r.get("ticker", "?")
            company = r.get("company", "?")[:35]
            overnight = r.get("overnight_return_pct", 0)
            dep_type = r.get("departure_type", "?")
            f.write(f"  QUALIFYING: {ticker} | {company} | overnight={overnight:.1f}% | type={dep_type}\n")

    # Alert if qualifying events found
    if qualifying:
        print(f"\n{'='*60}")
        print(f"ACTION REQUIRED: {len(qualifying)} qualifying CEO departure(s) found!")
        print(f"{'='*60}")
        for r in qualifying:
            ticker = r.get("ticker", "?")
            overnight = r.get("overnight_return_pct", 0)
            filing_date = r.get("filing_date", "?")[:10]
            print(f"\n  TICKER: {ticker}")
            print(f"  Filing date: {filing_date}")
            print(f"  Overnight drop: {overnight:.1f}%")
            print(f"  Departure type: {r.get('departure_type', '?')}")
            print(f"  Market cap: ${r.get('market_cap_m', 0):.0f}M")
            print()
            print(f"  ACTION: Set trigger on hypothesis 5dbcfb37:")
            print(f"    1. Update hypothesis expected_symbol to '{ticker}'")
            print(f"    2. Set trigger='after_hours_immediate' (for same-night entry)")
            print(f"       OR trigger='next_market_open' (if announcement already priced in)")
            print(f"    3. SHORT ${5000} at open, 1d hold")
            print()
            print(f"  VERIFY BEFORE TRADING:")
            print(f"    [] Is this clearly a performance-failure departure (not planned)?")
            print(f"    [] Was the stock up at recent 52-week high (not already beaten down)?")
            print(f"    [] No concurrent events (earnings, FDA approval, etc.)?")
            print(f"    [] Stock price > $5 (to avoid penny stock slippage)?")

            # Write alert to JSONL
            alert = {
                "timestamp": timestamp,
                "ticker": ticker,
                "company": r.get("company", ""),
                "filing_date": filing_date,
                "overnight_return_pct": overnight,
                "departure_type": r.get("departure_type", "unknown"),
                "market_cap_m": r.get("market_cap_m", 0),
                "prior_20d_abnormal_return_pct": r.get("prior_20d_abnormal_return_pct"),
                "is_welcome_departure": r.get("is_welcome_departure", False),
                "action_required": True,
                "hypothesis_id": "5dbcfb37",
                "instruction": "SHORT at next-day open, 1d hold, $5000 position"
            }
            _db.init_db()
            _db.append_scanner_signal('ceo_departure', alert)

        print(f"\nAlerts written to scanner_signals table")
    else:
        print(f"\nNo qualifying CEO departure events found in {start} to {end}.")

    print(f"\nScan complete. Log written to {LOG_PATH}")


if __name__ == "__main__":
    main()
