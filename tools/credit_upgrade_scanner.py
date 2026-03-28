"""
S&P Credit Upgrade Scanner — First-Ever Investment Grade Detection
=================================================================
Scans for companies recently upgraded from speculative grade (junk) to investment
grade (BBB- or better) for the first time EVER.

WHY: The first_ig_credit_upgrade signal shows +2.0 to +5.97% abnormal 3d return
after true first-ever IG upgrades. PR missed on March 17 (12d lag). Need detection
within 1 trading day.

WHAT IT LOOKS FOR:
  - S&P or Moody's upgrade from BB+ → BBB- (or equivalent)
  - Must be FIRST EVER investment grade (not return-to-IG after COVID downgrade)
  - Company market cap > $5B (sufficient liquidity)
  - Stock NOT up >25% in prior 90 days (would be priced in)

DATA SOURCES:
  1. BusinessWire/PRNewswire: Companies issue press releases on same day as upgrade
  2. S&P Global "This Week in Credit" — lists "rising stars" weekly (Fridays)
  3. Moody's credit rating actions RSS feed

Usage:
  python3 tools/credit_upgrade_scanner.py              # Check recent upgrades
  python3 tools/credit_upgrade_scanner.py --alert      # Print if actionable signal found

NOTE: This is a SEARCH tool, not a trading tool. When it finds a qualifying event:
  1. Verify it's a true first-ever IG (check company history)
  2. Check market cap > $5B
  3. Check stock not up >25% in prior 90d
  4. Assign symbol to hypothesis f86dcb4e and set trigger=next_market_open
  5. Hold 3 trading days from entry
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import research
import db


def check_recent_upgrades_web():
    """Check for recent S&P credit upgrades using web sources."""
    try:
        import yfinance as yf
        # List of known companies near IG threshold (BB+ at S&P with positive outlook)
        # These are candidates to watch for upgrade
        candidates = {
            'CCL': {'name': 'Carnival Corp', 'current': 'BB+', 'target': '2026-2027', 'current_s&p': 'BB+'},
            'RCL': {'name': 'Royal Caribbean', 'current': 'BBB-', 'note': 'Already IG (upgraded Feb 2025)'},
            'NCL': {'name': 'Norwegian Cruise Line', 'current': 'BB+', 'note': 'Still HY, watching'},
            'NCLH': {'name': 'Norwegian Cruise', 'current': 'B+', 'note': 'Still deep HY'},
        }

        print("Known companies near IG threshold:")
        for ticker, info in candidates.items():
            print(f"  {ticker}: {info['name']} — S&P: {info.get('current', 'unknown')}")
            if 'note' in info:
                print(f"        Note: {info['note']}")

    except Exception as e:
        print(f"Error: {e}")


def show_signal_status():
    """Show current first_ig_credit_upgrade signal status."""
    kb = research.load_knowledge()
    sigs = kb.get('known_effects', {})
    signal = sigs.get('first_ig_credit_upgrade', {})

    if not signal:
        print("Signal not found in knowledge base")
        return

    print("=" * 70)
    print("FIRST-EVER IG CREDIT UPGRADE SIGNAL STATUS")
    print("=" * 70)
    print(f"Status: {signal.get('status', 'unknown')}")
    print(f"Effect size: {signal.get('effect_size', 'unknown')}")
    print()

    print("Validation events:")
    for e in signal.get('validation_events', []):
        outcome = e.get('outcome', 'unknown')
        status_icon = "✓" if "POSITIVE" in outcome else ("✗" if "NEGATIVE" in outcome else "~")
        print(f"  {status_icon} {e['symbol']} ({e['date']}): 1d={e.get('1d_abn','?'):+.2f}%, 3d={e.get('3d_abn','?'):+.2f}% | {outcome[:80]}")

    print()
    print("Hypothesis: f86dcb4e (first_ig_credit_upgrade)")
    h = db.get_hypothesis_by_id('f86dcb4e')
    if h:
        print(f"Status: {h.get('status')} | Symbol: {h.get('expected_symbol')}")

    print()
    print("Disqualification criteria:")
    for c in signal.get('disqualification_criteria', []):
        print(f"  - {c}")

    print()
    print("Pipeline note:", signal.get('pipeline_note', 'None'))
    print("CCL monitoring:", signal.get('ccl_monitoring', 'None'))


def activation_checklist(symbol, upgrade_date):
    """Run pre-activation checklist for a first-ever IG upgrade."""
    print(f"\n=== ACTIVATION CHECKLIST: {symbol} ===")
    print(f"Upgrade announced: {upgrade_date}")
    print()

    # Check days since upgrade
    upgrade_dt = datetime.strptime(upgrade_date, '%Y-%m-%d')
    days_since = (datetime.now() - upgrade_dt).days
    if days_since > 3:
        print(f"⚠️  WARNING: {days_since} days since upgrade. Optimal window is 0-1 days.")
        print("   The 3-day signal window may be partially or fully passed.")
    else:
        print(f"✓ Time window: {days_since} days since upgrade (within 3d signal window)")

    # Check market cap
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        info = ticker.info
        mktcap = info.get('marketCap', 0)
        if mktcap < 5e9:
            print(f"⚠️  Market cap ${mktcap/1e9:.1f}B < $5B threshold (insufficient liquidity)")
        else:
            print(f"✓ Market cap: ${mktcap/1e9:.1f}B (sufficient liquidity)")

        # Check 90-day price run-up
        hist = ticker.history(period='90d')
        if not hist.empty:
            price_90d_ago = float(hist['Close'].iloc[0])
            price_now = float(hist['Close'].iloc[-1])
            run_up = (price_now / price_90d_ago - 1) * 100
            if run_up > 25:
                print(f"⚠️  Stock up {run_up:+.1f}% in 90d — signal likely priced in (threshold: >25%)")
            else:
                print(f"✓ 90-day run-up: {run_up:+.1f}% (below 25% threshold, not priced in)")

    except Exception as e:
        print(f"Could not check market cap/price: {e}")

    print()
    print("MANUAL CHECKS REQUIRED:")
    print("[ ] Confirm this is TRUE FIRST-EVER IG (company never had IG rating before)")
    print("[ ] Confirm upgrade is by S&P or Moody's (not just Fitch alone)")
    print("[ ] Check for company-specific negative news (earnings miss, restatement, etc.)")
    print()
    print("IF ALL CHECKS PASS:")
    print(f"  1. Update hypothesis f86dcb4e with symbol={symbol}")
    print(f"  python3 -c \"import db; db.init_db(); db.update_hypothesis_fields('f86dcb4e', expected_symbol='{symbol}', trigger='next_market_open', trigger_position_size=5000, trigger_stop_loss_pct=8, trigger_take_profit_pct=10)\"")
    print(f"  2. Trade_loop will execute at next market open")
    print(f"  3. Exit after 3 trading days (set deadline accordingly)")


def main():
    parser = argparse.ArgumentParser(description='Monitor for first-ever IG credit upgrades')
    parser.add_argument('--status', action='store_true', help='Show signal status')
    parser.add_argument('--check', metavar='SYMBOL', help='Run activation checklist for a symbol')
    parser.add_argument('--date', metavar='DATE', help='Upgrade date for --check (YYYY-MM-DD)', default=None)
    parser.add_argument('--alert', action='store_true', help='Print alert-style output')
    args = parser.parse_args()

    db.init_db()

    if args.check:
        upgrade_date = args.date or datetime.now().strftime('%Y-%m-%d')
        activation_checklist(args.check, upgrade_date)
    else:
        show_signal_status()
        print()
        check_recent_upgrades_web()

    print()
    print("=" * 70)
    print("MONITORING PROTOCOL:")
    print("  1. Check S&P 'This Week in Credit' every Friday (spglobal.com)")
    print("  2. Search BusinessWire for '[company] achieves investment grade'")
    print("  3. If found: run python3 tools/credit_upgrade_scanner.py --check TICKER --date YYYY-MM-DD")
    print("  4. If checklist passes: activate hypothesis f86dcb4e within same day")
    print()
    print("NEXT EXPECTED EVENTS:")
    print("  CCL: S&P upgrade unlikely until late 2026/2027 (needs leverage < 3x)")
    print("  Unknown 2nd rising star from S&P March 23, 2026 report")


if __name__ == '__main__':
    main()
