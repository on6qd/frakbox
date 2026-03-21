"""
cluster_auto_scanner.py - Daily automated scanner for insider cluster buying opportunities.

Runs at market close to find fresh clusters filed in the last 24 hours.
When a qualifying cluster is found, creates a research queue entry and
optionally sets a trigger on an existing hypothesis for next-day execution.

This prevents missing opportunities that go stale between LLM sessions.

Usage:
    python tools/cluster_auto_scanner.py [--dry-run] [--hours N]

Schedule via launchd to run daily at 4:15 PM ET after market close.

Qualifications (matching hypothesis 1cb6140f):
    - 3+ insiders buying within 30 days
    - Total purchase value >= $500K
    - Market cap >= $500M (prevents delistment failures)
    - Filed within last 24 hours (48 if run twice per day)
    - Not already in active/pending experiments
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

import yfinance as yf

ET = ZoneInfo("America/New_York")

# Qualification thresholds (matching hypothesis 1cb6140f)
MIN_INSIDERS = 3
MIN_TOTAL_VALUE_K = 500    # $500K minimum
MIN_MARKET_CAP_M = 500     # $500M minimum
MAX_STALE_HOURS = 48       # Don't trade if cluster is older than 48 hours

# Hypothesis IDs to check for existing pending triggers
CLUSTER_HYPOTHESIS_IDS = ["1cb6140f", "76678219"]  # 3d and 5d cluster hypotheses


def get_current_market_cap_m(ticker: str) -> float:
    """Get current market cap in millions. Returns 0 if unavailable."""
    try:
        info = yf.Ticker(ticker).info
        mktcap = info.get("marketCap", 0) or 0
        return mktcap / 1_000_000
    except Exception:
        return 0.0


def find_fresh_clusters(hours: int = 48) -> list[dict]:
    """
    Fetch fresh insider clusters from OpenInsider filed within the last N hours.

    Returns list of qualifying clusters with details.
    """
    from tools.openinsider_scraper import get_cluster_buys

    cutoff = datetime.now(ET) - timedelta(hours=hours)

    clusters = get_cluster_buys(min_insiders=MIN_INSIDERS, days=7, min_value_k=100)

    fresh = []
    for c in clusters:
        # Check recency
        try:
            filing_date = datetime.strptime(c['filing_date'], '%Y-%m-%d')
            filing_date = filing_date.replace(tzinfo=ET)
            if filing_date < cutoff:
                continue
        except (ValueError, KeyError):
            continue

        # Check minimum thresholds
        n_insiders = c.get('n_insiders', 0)
        total_value_k = c.get('total_value_k', 0) or 0

        if n_insiders < MIN_INSIDERS:
            continue
        if total_value_k < MIN_TOTAL_VALUE_K:
            continue

        fresh.append(c)

    return fresh


def check_existing_positions(ticker: str) -> bool:
    """Return True if ticker already has an active experiment."""
    try:
        with open(BASE_DIR / "hypotheses.json") as f:
            hypotheses = json.load(f)
        for h in hypotheses:
            if (h.get("expected_symbol") == ticker and
                    h.get("status") in ["active", "pending"] and
                    h.get("trigger") is not None):
                return True
    except Exception:
        pass
    return False


def get_52w_position(ticker: str) -> dict:
    """Get price relative to 52-week high/low."""
    try:
        info = yf.Ticker(ticker).info
        price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
        high_52w = info.get("fiftyTwoWeekHigh", 0)
        low_52w = info.get("fiftyTwoWeekLow", 0)

        pct_from_high = (price - high_52w) / high_52w * 100 if high_52w > 0 else None
        pct_from_low = (price - low_52w) / low_52w * 100 if low_52w > 0 else None

        return {
            "price": price,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "pct_from_52w_high": pct_from_high,
            "pct_from_52w_low": pct_from_low,
        }
    except Exception:
        return {}


def set_cluster_trigger(ticker: str, hypothesis_id: str = "1cb6140f",
                         position_size: int = 5000, dry_run: bool = False) -> bool:
    """
    Set trigger on the cluster hypothesis for next market open.

    Creates a fresh hypothesis entry or sets trigger on existing one.
    Returns True if trigger was set.
    """
    hyp_path = BASE_DIR / "hypotheses.json"

    try:
        with open(hyp_path) as f:
            hypotheses = json.load(f)

        # Find the template hypothesis
        template = None
        for h in hypotheses:
            if h["id"] == hypothesis_id:
                template = h
                break

        if template is None:
            print(f"  ERROR: Hypothesis {hypothesis_id} not found")
            return False

        # Check if this ticker already has a pending trigger
        for h in hypotheses:
            if (h.get("expected_symbol") == ticker and
                    h.get("status") == "pending" and
                    h.get("trigger") is not None):
                print(f"  SKIP: {ticker} already has pending trigger")
                return False

        if dry_run:
            print(f"  DRY RUN: Would set trigger on {hypothesis_id} for {ticker}")
            return True

        # For auto-scanner, we log the opportunity but don't auto-create trades
        # The LLM agent should review and decide. We create a research queue entry instead.
        return True

    except Exception as e:
        print(f"  ERROR setting trigger: {e}")
        return False


def log_opportunity(cluster: dict, market_cap_m: float, position_52w: dict,
                    dry_run: bool = False):
    """Log a qualifying opportunity to the research queue for LLM review."""
    import research_queue

    ticker = cluster['ticker']
    company = cluster.get('company', ticker)
    n_insiders = cluster.get('n_insiders', 0)
    total_value_k = cluster.get('total_value_k', 0)
    filing_date = cluster.get('filing_date', '')
    price = cluster.get('price_per_share', 0)

    pct_from_high = position_52w.get('pct_from_52w_high', 0) or 0

    description = (
        f"AUTO-DETECTED insider cluster: {ticker} ({company}). "
        f"{n_insiders} insiders filed {filing_date}. "
        f"Total value: ${total_value_k/1000:.1f}M. "
        f"Price: ${price:.2f} ({pct_from_high:.1f}% from 52W high). "
        f"Market cap: ${market_cap_m:.0f}M. "
        f"QUALIFYING for hypothesis 1cb6140f (3d hold). "
        f"ACTION NEEDED: Review and set trigger if appropriate."
    )

    if dry_run:
        print(f"  DRY RUN: Would log to research queue: {description[:100]}...")
        return

    research_queue.add_research_task(
        category="insider_buying_cluster",
        question=description,
        priority=0,  # Highest priority
        reasoning=f"Auto-detected cluster with {n_insiders} insiders, ${total_value_k/1000:.1f}M total. "
                  f"Qualifying for 1cb6140f. Filed {filing_date}. Must act before 3d window expires."
    )
    print(f"  LOGGED to research queue: {ticker}")


def scan(hours: int = 48, dry_run: bool = False, verbose: bool = True) -> list[dict]:
    """
    Main scan function. Returns list of qualifying opportunities.

    Args:
        hours: How far back to look (default 48h)
        dry_run: If True, log findings but don't modify any files
        verbose: Print progress
    """
    if verbose:
        print(f"=== Insider Cluster Auto-Scanner ===")
        print(f"Scanning for clusters filed in last {hours} hours...")
        print(f"Thresholds: {MIN_INSIDERS}+ insiders, ${MIN_TOTAL_VALUE_K}K+ value, ${MIN_MARKET_CAP_M}M+ market cap")
        print()

    fresh_clusters = find_fresh_clusters(hours=hours)

    if verbose:
        print(f"Found {len(fresh_clusters)} clusters meeting insider/value thresholds")
        print()

    qualifying = []

    for cluster in fresh_clusters:
        ticker = cluster.get('ticker', '')
        if not ticker:
            continue

        if verbose:
            print(f"Checking {ticker} ({cluster.get('company', '')[:30]})...")

        # Check market cap
        mktcap_m = get_current_market_cap_m(ticker)
        if mktcap_m < MIN_MARKET_CAP_M:
            if verbose:
                print(f"  SKIP: Market cap ${mktcap_m:.0f}M < ${MIN_MARKET_CAP_M}M threshold")
            continue

        # Get 52W position
        pos_52w = get_52w_position(ticker)

        # Check if already trading
        if check_existing_positions(ticker):
            if verbose:
                print(f"  SKIP: {ticker} already has active/pending position")
            continue

        # Qualifying!
        cluster['market_cap_m'] = mktcap_m
        cluster['position_52w'] = pos_52w
        qualifying.append(cluster)

        if verbose:
            pct = pos_52w.get('pct_from_52w_high', 0) or 0
            print(f"  QUALIFYING: {cluster['n_insiders']} insiders, ${cluster.get('total_value_k', 0)/1000:.1f}M, "
                  f"${mktcap_m:.0f}M mktcap, {pct:.1f}% from 52W high")

        # Log to research queue
        log_opportunity(cluster, mktcap_m, pos_52w, dry_run=dry_run)

    if verbose:
        print(f"\n{'='*40}")
        print(f"Qualifying opportunities: {len(qualifying)}")
        for q in qualifying:
            pct = q.get('position_52w', {}).get('pct_from_52w_high', 0) or 0
            print(f"  {q['ticker']}: {q['n_insiders']} insiders, ${q.get('total_value_k', 0)/1000:.1f}M, {pct:.1f}% from 52W high")
        print()

        if not dry_run and qualifying:
            print("NEXT STEP: Set trigger on hypothesis 1cb6140f for qualifying tickers.")
            print("  1. Verify company fundamentals (not in financial distress)")
            print("  2. Verify no upcoming earnings that would confound the signal")
            print("  3. Set trigger: h['trigger'] = 'next_market_open'")

    return qualifying


def main():
    parser = argparse.ArgumentParser(description="Scan for fresh insider cluster opportunities")
    parser.add_argument("--dry-run", action="store_true", help="Log findings without modifying files")
    parser.add_argument("--hours", type=int, default=48, help="How far back to scan (default: 48h)")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = parser.parse_args()

    results = scan(hours=args.hours, dry_run=args.dry_run, verbose=not args.quiet)

    # Exit code: 0 if no qualifying opportunities, 1 if opportunities found
    sys.exit(0 if not results else 0)  # Always exit 0 for cron compatibility


if __name__ == "__main__":
    main()
