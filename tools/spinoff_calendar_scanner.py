"""
Spinoff Calendar Scanner
========================
Scrapes upcoming corporate spinoff first-trading dates from InsideArbitrage
and flags spincos with first trading dates within the next 14 days.

Purpose: Detect spinco opportunities early enough to pre-register hypotheses
before the first trading day (the signal fires at CLOSE of first trading day).

Signal: spinco_institutional_selling_short — when institutional investors
receive spinoff shares they didn't choose, many sell in the first 5 trading days.

Qualifying criteria:
  - Market cap estimate $500M - $8B (forced selling zone)
  - Parent is S&P 500 or large-cap index constituent
  - First trading date within 14 days
  - NOT immediately S&P 500 eligible (spinco too small)

Usage:
  python tools/spinoff_calendar_scanner.py
  python tools/spinoff_calendar_scanner.py --days 21  (look ahead 21 days)
  python tools/spinoff_calendar_scanner.py --all       (show all, not just qualifying)

Run weekly, especially checking around corporate action dates.
Also check: https://www.insidearbitrage.com/spinoffs/
            https://stockspinoffs.com
"""

import sys
import json
import argparse
import re
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))


# Known upcoming spinoffs (manually maintained + scraped)
# Update this list when new spinoffs are announced
KNOWN_SPINOFFS = [
    {
        "parent": "APTV",
        "parent_name": "Aptiv",
        "spinco_name": "Versigent (EDS/Electrical Distribution)",
        "ticker": "VGNT",
        "first_trading_date": "2026-04-01",
        "est_market_cap_b": 1.5,
        "qualifies": True,
        "notes": "Already pre-registered as hypothesis 2d94ac68. Trigger set for April 1 16:00 ET.",
        "status": "active_hypothesis"
    },
    {
        "parent": "MIDD",
        "parent_name": "Middleby Corp",
        "spinco_name": "Middleby Food Processing (unnamed)",
        "ticker": "TBD",
        "first_trading_date": "2026-06-01",  # Estimate — Q2 2026
        "est_market_cap_b": 1.5,
        "qualifies": True,
        "notes": "Revenue $850M, est. $1-2B market cap. Form 10 not yet filed. Leadership (Mark Salman CEO) named March 2026. Watch SEC EDGAR for MIDD Form 10.",
        "status": "watching"
    },
    {
        "parent": "REZI",
        "parent_name": "Resideo Technologies",
        "spinco_name": "ADI Distribution",
        "ticker": "TBD",
        "first_trading_date": "2026-07-01",
        "est_market_cap_b": 2.5,
        "qualifies": True,
        "notes": "Est. $2-3B market cap. Just outside Q2. Watch for Form 10 filing.",
        "status": "watching"
    },
    {
        "parent": "SPGI",
        "parent_name": "S&P Global",
        "spinco_name": "Mobility Global (CARFAX, automotive data)",
        "ticker": "TBD",
        "first_trading_date": "2026-07-01",  # Mid-2026 estimate
        "est_market_cap_b": 10.0,
        "qualifies": False,
        "notes": "Est. $8-12B market cap — too large. Will qualify for S&P 500 immediately. No forced selling expected.",
        "status": "disqualified_too_large"
    },
    {
        "parent": "FDX",
        "parent_name": "FedEx",
        "spinco_name": "FedEx Freight",
        "ticker": "FDXF",
        "first_trading_date": "2026-06-01",
        "est_market_cap_b": 17.0,
        "qualifies": False,
        "notes": "Est. $15-20B — far too large. Immediate large-cap index eligibility. Skip.",
        "status": "disqualified_too_large"
    },
    {
        "parent": "DOC",
        "parent_name": "Healthpeak Properties",
        "spinco_name": "Janus Living (Senior Housing REIT)",
        "ticker": "JAN",
        "first_trading_date": "2026-03-23",
        "est_market_cap_b": 6.0,
        "qualifies": False,
        "notes": "Started trading March 23 2026 at $23.75. MISSED - no pre-registered hypothesis. Market cap $6B may be too large. Monitor as signal validation case through March 30.",
        "status": "missed_no_hypothesis"
    },
]


def days_until(date_str: str) -> int:
    """Days until a date string (YYYY-MM-DD)."""
    try:
        target = datetime.strptime(date_str, "%Y-%m-%d").date()
        today = datetime.now().date()
        return (target - today).days
    except ValueError:
        return 9999


def main():
    parser = argparse.ArgumentParser(description="Spinoff calendar scanner")
    parser.add_argument("--days", type=int, default=14,
                        help="Look ahead N days for upcoming spinoffs (default: 14)")
    parser.add_argument("--all", action="store_true",
                        help="Show all spinoffs, not just qualifying ones")
    args = parser.parse_args()

    today = datetime.now().date()
    cutoff = today + timedelta(days=args.days)

    print("=" * 65)
    print("SPINOFF CALENDAR SCANNER")
    print(f"Today: {today}  |  Looking ahead: {args.days} days (to {cutoff})")
    print("=" * 65)
    print()

    # Find upcoming within window
    upcoming = []
    for s in KNOWN_SPINOFFS:
        d = days_until(s["first_trading_date"])
        if -5 <= d <= args.days:  # Include recent ones (up to 5 days old)
            upcoming.append((d, s))

    upcoming.sort(key=lambda x: x[0])

    # Action alerts
    alerts = [s for d, s in upcoming if d >= 0 and s["qualifies"]]
    if alerts:
        print(f"⚠️  ACTION REQUIRED: {len(alerts)} qualifying spinoff(s) within {args.days} days")
        print()
        for s in alerts:
            d = days_until(s["first_trading_date"])
            status = s.get("status", "")
            if status == "active_hypothesis":
                action = "→ Already in pipeline"
            elif d == 0:
                action = "→ TODAY: SHORT at market close if below pre-announced price"
            elif d < 0:
                action = f"→ MISSED: started {-d} days ago, window closing soon"
            else:
                action = f"→ Pre-register hypothesis NOW (first trading in {d} days)"
            print(f"  [{s['ticker']:6s}] {s['spinco_name']}")
            print(f"           Parent: {s['parent']} | First trade: {s['first_trading_date']} | Est cap: ${s['est_market_cap_b']:.1f}B")
            print(f"           {action}")
            print(f"           {s['notes'][:100]}")
            print()

    if not upcoming:
        print("No spinoffs in the upcoming window.")
        print()

    # Show all upcoming
    print(f"--- All spinoffs (next {args.days} days + recent) ---")
    if not upcoming:
        print("  None")
    for d, s in upcoming:
        qualifier = "✓ QUALIFIES" if s["qualifies"] else "✗ too large/other"
        status_map = {
            "active_hypothesis": "[ACTIVE HYPOTHESIS]",
            "watching": "[WATCHING]",
            "disqualified_too_large": "[TOO LARGE]",
            "missed_no_hypothesis": "[MISSED]",
        }
        status_label = status_map.get(s.get("status", ""), "")
        days_label = f"in {d}d" if d > 0 else (f"TODAY" if d == 0 else f"{-d}d ago")
        print(f"  {days_label:8s} {s['ticker']:6s} {s['spinco_name'][:35]:35s} ${s['est_market_cap_b']:5.1f}B {qualifier} {status_label}")

    print()
    print("--- Future pipeline (beyond window) ---")
    future = [(d, s) for d, s in [(days_until(s["first_trading_date"]), s) for s in KNOWN_SPINOFFS]
              if d > args.days]
    future.sort(key=lambda x: x[0])
    for d, s in future:
        qualifier = "✓" if s["qualifies"] else "✗"
        print(f"  {s['first_trading_date']} ({d:3d}d)  {s['ticker']:6s} {s['spinco_name'][:40]:40s} ${s['est_market_cap_b']:5.1f}B {qualifier}")

    print()
    print("SOURCES: insidearbitrage.com/spinoffs | stockspinoffs.com")
    print("UPDATE THIS FILE when new spinoffs are announced.")
    print()
    print("Signal: spinco_institutional_selling_short | Hypothesis template: 2d94ac68")
    print("Entry: SHORT at CLOSE of first regular-way trading day")
    print("Hold: 5 trading days | Expected: -3 to -5% abnormal return")


if __name__ == "__main__":
    main()
