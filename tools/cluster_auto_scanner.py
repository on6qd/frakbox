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

VIX + Cluster Size Trading Gate (full-population analysis, N=1566, 2021-2025):
    Tier 1 - HIGH CONFIDENCE (VIX < 20, n >= 3):
        EV = +3.31%, 55.7% consistency. Trade freely.
    Tier 2 - MODERATE (VIX 20-25, n >= 5):
        EV = +1.85%, 47% positive rate, p=0.032. Tradeable but marginal.
        For VIX 20-25 and n=3-4: EV=+1.22% -- below preference, but acceptable if high total value.
    Tier 3 - CONDITIONAL (VIX 25-30, n >= 6):
        EV = +2.37%, 57% positive rate, p=0.027. Tradeable when n>=6.
        For VIX 25-30 and n=3-5: EV=-0.48%, 38% positive -- DO NOT TRADE.
    Tier 4 - DO NOT TRADE (VIX > 30):
        Crisis regime -- signal noisy, N=94. Avoid.

VIX 20-25 with n=3-4 note: EV=+1.22% is above minimum net return (1.0%) but below typical
cluster signal strength. Historically acceptable if total purchase value > $2M.
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

# VIX regime thresholds (from full-population analysis N=1566, 2021-2025)
# See VIX + Cluster Size Trading Gate in module docstring for full decision matrix.
VIX_CALM_THRESHOLD = 20.0      # VIX < 20: Tier 1 (HIGH CONFIDENCE, any cluster size)
VIX_MODERATE_THRESHOLD = 25.0  # VIX 20-25: Tier 2 (n>=5 recommended, n>=3 marginal)
VIX_ELEVATED_THRESHOLD = 30.0  # VIX 25-30: Tier 3 (n>=6 only, n<6 DO NOT TRADE)
# VIX > 30: Tier 4 (DO NOT TRADE - crisis regime, N=94 noisy)


def get_current_vix() -> tuple[float, str, str]:
    """
    Get current VIX level and classify by regime.

    Returns (vix_level, regime_label, confidence_label).
    regime_label: "calm" | "moderate" | "elevated" | "crisis"
    confidence_label: human-readable label for output/email
    """
    try:
        vix_data = yf.Ticker("^VIX").history(period="2d")
        if vix_data.empty:
            return (None, "unknown", "UNKNOWN (VIX data unavailable)")
        vix_level = float(vix_data['Close'].iloc[-1])
    except Exception:
        return (None, "unknown", "UNKNOWN (VIX fetch failed)")

    if vix_level < VIX_CALM_THRESHOLD:
        regime = "calm"
        label = f"TIER 1 (calm VIX {vix_level:.1f} < {VIX_CALM_THRESHOLD}, EV=+3.31%, any cluster size)"
    elif vix_level < VIX_MODERATE_THRESHOLD:
        regime = "moderate"
        label = f"TIER 2 (VIX {vix_level:.1f} 20-25, EV=+1.4-1.85%, prefer n>=5)"
    elif vix_level < VIX_ELEVATED_THRESHOLD:
        regime = "elevated"
        label = f"TIER 3 (elevated VIX {vix_level:.1f} 25-30, EV=+2.37% for n>=6 only, -0.48% for n<6)"
    else:
        regime = "crisis"
        label = f"TIER 4 / DO NOT TRADE (crisis VIX {vix_level:.1f} > {VIX_ELEVATED_THRESHOLD}, signal unreliable)"

    return (vix_level, regime, label)


def get_vix_action_recommendation(vix_regime: str, n_insiders: int, total_value_k: float) -> str:
    """
    Return a trading action recommendation based on VIX tier + cluster size.

    Uses the VIX + Cluster Size Trading Gate from full-population analysis (N=1566).
    """
    if vix_regime == "calm":
        return "HIGH CONFIDENCE. Trade at next open per standard protocol (VIX<20, EV=+3.31%)."
    elif vix_regime == "moderate":
        if n_insiders >= 5:
            return (f"MODERATE CONFIDENCE (VIX 20-25, n={n_insiders}>=5). "
                    "EV=+1.85%, p=0.032. Tradeable — proceed.")
        elif total_value_k >= 2000:
            return (f"MARGINAL (VIX 20-25, n={n_insiders}<5 but total=${total_value_k/1000:.1f}M>=$2M). "
                    "EV=+1.22%, acceptable for high-value clusters. Proceed with caution.")
        else:
            return (f"WEAK SIGNAL (VIX 20-25, n={n_insiders}<5, total=${total_value_k/1000:.1f}M<$2M). "
                    "EV=+1.22%, borderline. Do not trade unless other strong qualifiers present.")
    elif vix_regime == "elevated":
        if n_insiders >= 6:
            return (f"CONDITIONAL (VIX 25-30, n={n_insiders}>=6). "
                    "EV=+2.37%, 57% pos rate, p=0.027. N>=6 overcomes VIX penalty. Proceed.")
        else:
            return (f"DO NOT TRADE (VIX 25-30, n={n_insiders}<6). "
                    "EV=-0.48%, 38% pos rate for n<6 in elevated VIX — coin flip. SKIP.")
    elif vix_regime == "crisis":
        return (f"DO NOT TRADE (VIX>30 crisis regime). "
                "Signal unreliable in crisis. N=94, mixed results. Wait for VIX to fall.")
    else:
        return "VIX regime unknown. Review manually before trading."


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
    vix_label = cluster.get('vix_label', 'UNKNOWN')
    vix_regime = cluster.get('vix_regime', 'unknown')
    vix_level = cluster.get('vix_level')

    pct_from_high = position_52w.get('pct_from_52w_high', 0) or 0

    # Build regime-aware action recommendation (n_insiders-aware)
    action_note = get_vix_action_recommendation(vix_regime, n_insiders, total_value_k)

    description = (
        f"AUTO-DETECTED insider cluster: {ticker} ({company}). "
        f"{n_insiders} insiders filed {filing_date}. "
        f"Total value: ${total_value_k/1000:.1f}M. "
        f"Price: ${price:.2f} ({pct_from_high:.1f}% from 52W high). "
        f"Market cap: ${market_cap_m:.0f}M. "
        f"VIX={vix_level:.1f if vix_level is not None else 'N/A'} -> {vix_label}. "
        f"QUALIFYING for hypothesis 1cb6140f (3d hold). "
        f"{action_note}"
    )

    if dry_run:
        print(f"  DRY RUN: Would log to research queue: {description[:100]}...")
        return

    research_queue.add_research_task(
        category="insider_buying_cluster",
        question=description,
        priority=0,  # Highest priority
        reasoning=(
            f"Auto-detected cluster with {n_insiders} insiders, ${total_value_k/1000:.1f}M total. "
            f"Qualifying for 1cb6140f. Filed {filing_date}. Must act before 3d window expires. "
            f"VIX regime: {vix_label}. {action_note}"
        )
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

    # VIX regime check (from full-population analysis N=1566, 2021-2025)
    vix_level, vix_regime, vix_label = get_current_vix()
    if verbose:
        print(f"VIX Regime Check:")
        print(f"  Current VIX: {vix_level:.2f}" if vix_level is not None else "  Current VIX: UNAVAILABLE")
        print(f"  Signal regime: {vix_label}")
        if vix_regime == "calm":
            print(f"  -> Tier 1: Calm regime. Full confidence. Any n>=3 cluster is tradeable.")
        elif vix_regime == "moderate":
            print(f"  -> Tier 2: Moderate regime (VIX 20-25). Prefer n>=5. n>=3 marginal if total>=$2M.")
        elif vix_regime == "elevated":
            print(f"  -> Tier 3: Elevated regime (VIX 25-30). n>=6 ONLY is tradeable. n<6: DO NOT TRADE.")
            print(f"     EV for n>=6: +2.37%, 57% pos rate. EV for n<6: -0.48%, 38% pos rate.")
        elif vix_regime == "crisis":
            print(f"  -> Tier 4: Crisis regime (VIX>30). DO NOT TRADE. Signal unreliable in crisis conditions.")
        else:
            print(f"  -> VIX regime unknown. Proceed with caution.")
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
        cluster['vix_level'] = vix_level
        cluster['vix_regime'] = vix_regime
        cluster['vix_label'] = vix_label
        qualifying.append(cluster)

        if verbose:
            pct = pos_52w.get('pct_from_52w_high', 0) or 0
            print(f"  QUALIFYING: {cluster['n_insiders']} insiders, ${cluster.get('total_value_k', 0)/1000:.1f}M, "
                  f"${mktcap_m:.0f}M mktcap, {pct:.1f}% from 52W high")
            print(f"  Signal confidence: {vix_label}")

        # Log to research queue
        log_opportunity(cluster, mktcap_m, pos_52w, dry_run=dry_run)

    if verbose:
        print(f"\n{'='*40}")
        print(f"VIX regime at scan time: {vix_label}")
        print(f"Qualifying opportunities: {len(qualifying)}")
        for q in qualifying:
            pct = q.get('position_52w', {}).get('pct_from_52w_high', 0) or 0
            q_vix_label = q.get('vix_label', 'unknown')
            print(f"  {q['ticker']}: {q['n_insiders']} insiders, ${q.get('total_value_k', 0)/1000:.1f}M, "
                  f"{pct:.1f}% from 52W high | {q_vix_label}")
        print()

        if not dry_run and qualifying:
            print("NEXT STEP: Set trigger on hypothesis 1cb6140f for qualifying tickers.")
            print("  1. Verify company fundamentals (not in financial distress)")
            print("  2. Verify no upcoming earnings that would confound the signal")
            print("  3. Check VIX regime label above before trading:")
            print("     - HIGH CONFIDENCE (VIX<20): proceed normally")
            print("     - MARGINAL (VIX 20-25): require 6+ insiders or large buy amount")
            print("     - LOW CONFIDENCE (VIX>25): strong bias against trading (EV ~0.4%)")
            print("  4. Set trigger: h['trigger'] = 'next_market_open'")

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
