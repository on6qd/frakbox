"""
First-Ever IG Upgrade Scanner
==============================
Scans for companies that received their FIRST-EVER investment grade credit rating.
Publishes weekly S&P rising stars + company press releases via 8-K/press releases.

Key sources:
1. S&P Global weekly "Rising Stars" report (Monday)
2. Company 8-K filings mentioning rating upgrades
3. BusinessWire/PRNewswire press release search

Usage:
  python tools/scan_ig_upgrades.py              # Check for recent upgrades
  python tools/scan_ig_upgrades.py --days 14    # Look back 14 days
  python tools/scan_ig_upgrades.py --verify TICKER DATE  # Verify specific event

Disqualification criteria (from known_effects):
  - Returns-to-IG (had prior IG that lapsed): NEGATIVE effect
  - Stock up >25% in 90 days before upgrade: priced in, exclude
  - Small-cap (<$5B market cap): insufficient liquidity
"""

import sys
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from tools.yfinance_utils import safe_download
    import yfinance as yf
except ImportError:
    print("Warning: yfinance not available")

# Known first-ever IG upgrades (from knowledge base)
KNOWN_EVENTS = [
    {
        "symbol": "VRT",
        "name": "Vertiv Holdings",
        "date": "2026-02-19",
        "agency": "S&P",
        "from_rating": "BB+",
        "to_rating": "BBB-",
        "type": "true_first_ever",
        "1d_abn": 1.11,
        "3d_abn": 2.39,
        "market_cap_b": 40,
        "notes": "True first-ever IG. Confirmed via press release."
    },
    {
        "symbol": "PR",
        "name": "Permian Resources Corp",
        "date": "2026-03-17",
        "agency": "S&P",
        "from_rating": "BB+",
        "to_rating": "BBB-",
        "type": "true_first_ever",
        "1d_abn": 1.96,
        "3d_abn": 5.97,
        "market_cap_b": 12,
        "notes": "True first-ever IG. Detected 12d late. Strong signal."
    },
    {
        "symbol": "VST",
        "name": "Vistra Corp",
        "date": "2025-12-02",
        "agency": "S&P",
        "from_rating": "BB+",
        "to_rating": "BBB-",
        "type": "true_first_ever",
        "1d_abn": -0.93,
        "3d_abn": -3.79,
        "market_cap_b": 50,
        "notes": "True first-ever IG but stock UP 30%+ in 90d prior = priced in. EXCLUDED."
    },
]

# Companies currently monitored as IG candidates
MONITORING_LIST = [
    # CCL DISQUALIFIED: return-to-IG (had Baa3 before COVID March 2020 downgrade)
    # {
    #     "symbol": "CCL", "name": "Carnival Corp", "current_rating_sp": "BB-",
    #     "notes": "RETURN-TO-IG - had prior IG, NEGATIVE/NEUTRAL effect per exclusion criterion"
    # },
    # MSCI already upgraded to BBB- by S&P on Feb 27, 2023 - NOT a candidate
    {
        "symbol": "COIN",
        "name": "Coinbase Global",
        "current_rating_sp": "BB-",
        "outlook": "positive",
        "notes": "No prior IG ever. BB- from S&P. Possible upgrade to BBB- if crypto regulation improves.",
        "watch_since": "2026-03-29",
    },
]


def check_stock_run_up(symbol: str, upgrade_date: str, threshold: float = 0.25) -> bool:
    """Check if stock ran up >threshold in 90 days before upgrade (priced in)."""
    try:
        end = datetime.strptime(upgrade_date, "%Y-%m-%d")
        start = end - timedelta(days=90)
        prices = safe_download(symbol, start=start.strftime("%Y-%m-%d"),
                               end=upgrade_date)
        if prices is None or prices.empty:
            return False
        prior_90d_return = (prices['Close'].iloc[-1] / prices['Close'].iloc[0] - 1)
        return prior_90d_return > threshold
    except Exception as e:
        print(f"  Warning: could not check run-up for {symbol}: {e}")
        return False


def check_market_cap(symbol: str, min_cap_b: float = 5.0) -> float:
    """Get market cap in billions."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        cap = info.get('marketCap', 0)
        return cap / 1e9
    except Exception:
        return 0


def compute_abnormal_return(symbol: str, upgrade_date: str, horizon_days: int = 3) -> float:
    """Compute abnormal return vs SPY after upgrade."""
    try:
        end = datetime.strptime(upgrade_date, "%Y-%m-%d")
        start_fetch = end - timedelta(days=3)
        # Need to go forward horizon_days trading days
        end_fetch = end + timedelta(days=horizon_days * 2 + 5)  # buffer for non-trading days

        prices = safe_download(symbol, start=start_fetch.strftime("%Y-%m-%d"),
                               end=end_fetch.strftime("%Y-%m-%d"))
        spy = safe_download("SPY", start=start_fetch.strftime("%Y-%m-%d"),
                            end=end_fetch.strftime("%Y-%m-%d"))

        if prices is None or spy is None or prices.empty or spy.empty:
            return None

        # Find entry date (first trading day after upgrade)
        entry_candidates = prices.index[prices.index > end]
        spy_entry_candidates = spy.index[spy.index > end]

        if len(entry_candidates) == 0 or len(spy_entry_candidates) == 0:
            return None

        entry_date = entry_candidates[0]
        spy_entry = spy_entry_candidates[0]

        # Find exit (horizon trading days later)
        entry_pos = prices.index.get_loc(entry_date)
        spy_pos = spy.index.get_loc(spy_entry)

        if entry_pos + horizon_days >= len(prices) or spy_pos + horizon_days >= len(spy):
            return None

        exit_date = prices.index[entry_pos + horizon_days]
        spy_exit = spy.index[spy_pos + horizon_days]

        stock_return = (prices['Close'][exit_date] / prices['Close'][entry_date] - 1) * 100
        spy_return = (spy['Close'][spy_exit] / spy['Close'][spy_entry] - 1) * 100

        return stock_return - spy_return
    except Exception as e:
        print(f"  Warning: could not compute abnormal return for {symbol}: {e}")
        return None


def verify_event(symbol: str, upgrade_date: str):
    """Verify a specific first-ever IG upgrade event."""
    print(f"\n{'='*60}")
    print(f"Verifying: {symbol} upgrade on {upgrade_date}")
    print(f"{'='*60}")

    # Check market cap
    cap = check_market_cap(symbol)
    print(f"Market cap: ${cap:.1f}B {'✓' if cap >= 5 else '✗ BELOW $5B threshold'}")

    # Check run-up
    run_up = check_stock_run_up(symbol, upgrade_date)
    print(f"90d run-up >25%: {'✗ PRICED IN (exclude)' if run_up else '✓ Not priced in'}")

    # Compute abnormal returns
    for horizon in [1, 3, 5]:
        abn = compute_abnormal_return(symbol, upgrade_date, horizon)
        if abn is not None:
            direction = "+" if abn > 0 else ""
            print(f"{horizon}d abnormal return: {direction}{abn:.2f}%")

    print()
    qualifies = cap >= 5 and not run_up
    print(f"Qualifies for hypothesis f86dcb4e: {'YES ✓' if qualifies else 'NO ✗'}")


def print_monitoring_status():
    """Print current monitoring list and known events."""
    print("\n" + "="*60)
    print("FIRST-EVER IG UPGRADE SIGNAL — STATUS REPORT")
    print("="*60)

    print(f"\n{'KNOWN EVENTS (N=' + str(len([e for e in KNOWN_EVENTS if e['type'] == 'true_first_ever'])) + ' true positive)':}")
    print("-"*50)
    for e in KNOWN_EVENTS:
        status = "✓ SIGNAL" if e['type'] == 'true_first_ever' and e['3d_abn'] > 0 else "✗ PRICED_IN" if 'priced' in e['notes'].lower() else "~"
        print(f"  {e['symbol']:6} {e['date']} {e['agency']} BB+→BBB- {status} ({e['3d_abn']:+.2f}% 3d abn)")

    true_pos = [e for e in KNOWN_EVENTS if e['type'] == 'true_first_ever' and e['3d_abn'] > 0]
    avg_3d = sum(e['3d_abn'] for e in true_pos) / len(true_pos) if true_pos else 0
    print(f"\n  True positives: n={len(true_pos)}, avg 3d abn = {avg_3d:+.2f}%")

    print(f"\n{'MONITORING LIST':}")
    print("-"*50)
    for m in MONITORING_LIST:
        print(f"  {m['symbol']:6} {m['current_rating_sp']} — {m['notes'][:80]}")

    print(f"\n{'HOW TO DETECT UPGRADES':}")
    print("-"*50)
    print("  1. Monitor S&P weekly rising stars (published Mondays)")
    print("     URL: https://www.spglobal.com/ratings/en/research/articles/")
    print("     Search: 'BBB Pulse Rising Stars' or 'Rising Stars Fallen Angels'")
    print("  2. Watch company IR pages for 8-K filings mentioning ratings")
    print("  3. Search BusinessWire/PR Newswire: 'investment grade' + 'first time'")
    print("  4. SEC EDGAR: 8-K filings with 'investment grade' keyword (Item 7.01/8.01)")
    print()
    print("  KEY: Enter within 1-2 TRADING DAYS of upgrade. PR and VRT were missed (detected 12d late).")
    print()


def main():
    parser = argparse.ArgumentParser(description='First-ever IG upgrade scanner')
    parser.add_argument('--days', type=int, default=7, help='Look back N days')
    parser.add_argument('--verify', nargs=2, metavar=('TICKER', 'DATE'),
                        help='Verify specific event: --verify MSCI 2026-03-17')
    args = parser.parse_args()

    if args.verify:
        verify_event(args.verify[0], args.verify[1])
    else:
        print_monitoring_status()


if __name__ == '__main__':
    main()
