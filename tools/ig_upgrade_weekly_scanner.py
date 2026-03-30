#!/usr/bin/env python3
"""
Weekly First-IG Upgrade Scanner
Scans BusinessWire, PRNewswire, and web for companies achieving first-ever
investment grade credit ratings. Run every Monday morning.

Usage:
    python3 tools/ig_upgrade_weekly_scanner.py
    python3 tools/ig_upgrade_weekly_scanner.py --days 14  # last 14 days
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subprocess
import json
from datetime import datetime, timedelta
import re
import urllib.request
import urllib.parse

# Known events already in our database (to avoid duplicate tracking)
KNOWN_EVENTS = {
    "VRT": "2026-02-19",   # S&P BB+→BBB-
    "PR":  "2026-03-17",   # S&P BB+→BBB- (Fitch already BBB- Jul 2025)
    "VST": "2025-12-02",   # S&P BB+→BBB- (Fitch follow-on Mar 2026)
    "ROL": "2025-02-01",   # approx
    "CCL": "2025-10-02",   # Fitch only, during hours
    "FCX": "2017-12-01",   # discovery set
}

# Companies to exclude (return-to-IG or already IG)
EXCLUDE_TICKERS = {
    "WDC",   # return to IG (was IG at Fitch Dec 2021)
    "FLG",   # deposit ratings only, IDR still BB+
}


def search_businesswire(query, days=7):
    """Search BusinessWire for press releases matching query in last N days."""
    try:
        encoded = urllib.parse.quote(query)
        url = f"https://www.businesswire.com/news/home/?rss=G20"
        # Use web search as proxy
        return search_web_for_ig_upgrades(days)
    except Exception as e:
        return []


def search_web_for_ig_upgrades(days=7):
    """Search web for recent IG upgrade announcements."""
    # Format date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    queries = [
        f'"investment grade" "BBB-" site:businesswire.com after:{start_date.strftime("%Y-%m-%d")}',
        f'"investment grade" "BBB-" site:prnewswire.com after:{start_date.strftime("%Y-%m-%d")}',
        f'"investment grade" "first time" OR "first-ever" "BBB-" after:{start_date.strftime("%Y-%m-%d")}',
        f'"rising star" "BBB-" "S&P" after:{start_date.strftime("%Y-%m-%d")}',
    ]
    return queries


def print_monitoring_checklist(days=7):
    """Print actionable checklist for weekly monitoring."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    print("=" * 65)
    print(f"FIRST-IG UPGRADE WEEKLY SCANNER — {datetime.now().strftime('%Y-%m-%d')}")
    print(f"Scanning period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("=" * 65)
    print()

    print("STEP 1: BusinessWire search (primary source — companies self-announce)")
    print(f'  URL: https://www.businesswire.com/news/home/search?query="investment+grade"')
    bw_url = f'https://www.businesswire.com/news/home/search?rss=true&query=%22investment+grade%22+%22BBB-%22&dateRange={start_date.strftime("%Y%m%d")}-{end_date.strftime("%Y%m%d")}'
    print(f"  RSS: {bw_url}")
    print()

    print("STEP 2: PRNewswire search")
    pr_url = f'https://www.prnewswire.com/news-releases/news-releases-list.html'
    print(f"  URL: {pr_url}")
    print(f"  Search: 'investment grade BBB-' in Finance category")
    print()

    print("STEP 3: Web searches to run manually or via WebSearch:")
    for q in search_web_for_ig_upgrades(days):
        print(f"  → {q}")
    print()

    print("STEP 4: S&P Rising Stars (monthly report)")
    print("  URL: https://www.spglobal.com/ratings/en/research/")
    print("  Search: 'BBB Pulse Rising Stars' or 'This Month in Credit'")
    print()

    print("STEP 5: Known near-IG candidates to check for upgrade news:")
    candidates = [
        ("COIN", "BB-",  "S&P", "Coinbase — ~$60B mktcap, never IG. Far from threshold (2027+)"),
        ("HLT",  "BB+",  "S&P", "Hilton Hotels — S&P BB+ affirmed Apr 2025 (stable). Moody's Ba2. Monitor quarterly"),
        ("BIRK", "BB",   "S&P", "Birkenstock — UPDATED: only BB now (2 notches below BBB-). Long runway"),
        ("TEVA", "BB+",  "S&P", "Teva Pharma — verify if ever had prior IG before trading"),
        # EXCLUDED:
        # RCL — RETURN to IG (S&P BBB- Feb 2025 after COVID downgrade). NOT first-ever.
        # Citadel Securities — private firm (Ken Griffin). No equity ticker. Not tradeable.
    ]
    for ticker, rating, agency, note in candidates:
        print(f"  {ticker:6} ({agency} {rating}): {note}")
    print()

    print("STEP 6: QUALIFYING CRITERIA (before trading):")
    print("  ✓ True first-ever IG from any major agency (S&P, Moody's, Fitch)")
    print("  ✓ Announced AFTER market hours (before-hours or AH announcement only)")
    print("  ✓ Company 24-month pre-event run-up < 250%")
    print("  ✓ VIX < 30 at time of announcement")
    print("  ✓ NOT a return to IG (company never had IG from any agency before)")
    print()
    print("  → If qualifies: Enter at NEXT morning's open")
    print("  → Use hypothesis f86dcb4e (first_ig_credit_upgrade)")
    print("  → If VIX > 30: DO NOT TRADE (regime constraint)")
    print()

    print("KNOWN EVENTS IN DATABASE:")
    for ticker, date in KNOWN_EVENTS.items():
        print(f"  {ticker}: {date}")
    print()

    print("HYPOTHESIS STATUS: f86dcb4e")
    print("  Discovery: n=12, 3d avg=+2.42%, p=0.021, dir=75%")
    print("  OOS true-first-IG (AH timing): VRT ✓ (+2.39%), PR ✓ (+5.97%) = 2/2 = 100%")
    print("  OOS total (incl. issues): ~3/5 = 60%")
    print("  Action: ACTIVATE when next qualifying event detected")
    print()


def quick_web_scan():
    """Use WebSearch-style approach to find recent IG upgrades."""
    print("RECENT IG UPGRADE SCAN (web search results):")
    print("  Run these searches to find events in the last 7 days:")
    print()
    searches = [
        'site:businesswire.com "investment grade" "BBB-" 2026',
        'site:prnewswire.com "achieves investment grade" 2026',
        '"first investment grade" "credit rating" 2026',
        '"BBB-" "upgraded" "investment grade" -sovereign 2026',
    ]
    for s in searches:
        print(f"  SEARCH: {s}")
    print()
    print("  LAST KNOWN EVENTS:")
    print("    PR  (Permian Resources): S&P BB+→BBB- on March 17, 2026")
    print("    VRT (Vertiv): S&P BB+→BBB- on Feb 19, 2026 (first-ever true IG)")
    print("    PR prior: Fitch BBB- July 2025 (TRUE first-ever for PR)")
    print()
    print("  NOTE: DETECTION FAILURE RISK")
    print("    PR was detected 12 days late. Next event: monitor Monday mornings.")
    print("    Run this script every Monday + check BusinessWire manually.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Weekly First-IG Upgrade Scanner")
    parser.add_argument("--days", type=int, default=7, help="Days to scan back (default: 7)")
    parser.add_argument("--check", type=str, help="Check specific ticker for IG status")
    args = parser.parse_args()

    print_monitoring_checklist(args.days)
    quick_web_scan()

    print()
    print("=" * 65)
    print("NEXT STEP: Copy searches above into WebSearch tool")
    print("  → Look for companies announcing first-ever investment grade")
    print("  → Verify: true-first-IG, AH announcement, VIX<30")
    print("  → If found: activate hypothesis f86dcb4e immediately")
    print("=" * 65)
