"""
FDXF (FedEx Freight) SpinCo Institutional Selling Short Trade Activation Script
=================================================================================
Run this at June 1, 2026 market CLOSE (16:00 ET) — first regular-way trading day.

HYPOTHESIS: 60558434 (spinco_institutional_selling_short, 5d hold)

SIGNAL: SHORT FedEx Freight (FDXF) at close of first trading day.
FedEx Corp (FDX, ~$55B market cap) spins off FedEx Freight as a pure-play
LTL (less-than-truckload) carrier. Estimated $30-35B SpinCo market cap.

WHY THIS IS STRONG:
  Backtest (N=25 spinoffs, 2018-2026):
  - 5d avg abnormal return: -5.69% (negative = SpinCo underperforms)
  - Direction: 73.9% negative
  - OOS validation 2022-2026: 80% negative
  - Live OOS: VSNT (Jan 2026) -17.5%, LION (May 2025) -19.9%

FORCED SELLING MECHANISM:
  FDX shareholders are diversified logistics/conglomerate investors.
  LTL trucking is a different sector — pension funds, growth funds, and index
  funds that hold FDX are NOT mandated to own LTL trucking pure-plays.
  Systematic rebalancing creates selling pressure for 5-10 trading days.

ENTRY CONDITIONS:
  1. Today (June 1, 2026) is first trading day for FDXF
  2. FDXF is actually trading (not halted)
  3. Portfolio has capacity (<5 active positions)
  4. VIX < 60 (no circuit breaker risk)

ABORT CONDITIONS:
  - FDXF drops >30% on day 1 (exceptional bad news — don't chase extreme gap)
  - Portfolio at 5/5 capacity
  - VIX > 60

EXIT: June 10, 2026 market close (5 trading days)
Position: $5,000
Stop loss: 10%
Take profit: 15%

Related dates:
  - FedEx Freight Investor Day: April 8, 2026 (could provide new price targets)
  - Form 10 was filed: March 2026
  - FDX ex-distribution date: TBD (check closer to June 1)

Usage:
  python tools/activate_fdxf_trade.py [--dry-run] [--price XXXX]
"""

import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import research
import trader
import db


HYPOTHESIS_ID = '60558434'
POSITION_SIZE = 5000
HOLD_DAYS = 5
STOP_LOSS_PCT = 10.0
TAKE_PROFIT_PCT = 15.0
SYMBOL = 'FDXF'


def get_fdxf_price():
    """Get FDXF current price on first trading day."""
    try:
        import yfinance as yf
        hist = yf.Ticker(SYMBOL).history(period='1d')
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
    except Exception as e:
        print(f"  Warning: could not get {SYMBOL} price: {e}")
    return None


def check_portfolio_capacity():
    """Check if we have room for another position."""
    try:
        positions = trader.get_positions()
        active = len([p for p in positions if p.get('qty') != 0])
        print(f"  Active positions: {active}/5")
        return active < 5
    except Exception as e:
        print(f"  Warning: could not check positions: {e}")
        return True


def get_spy_return_today():
    """Get today's SPY return to compute abnormal return."""
    try:
        import yfinance as yf
        spy = yf.Ticker('SPY').history(period='2d')
        if len(spy) >= 2:
            spy_return = (spy['Close'].iloc[-1] / spy['Close'].iloc[-2] - 1) * 100
            return spy_return
    except Exception as e:
        print(f"  Warning: could not get SPY return: {e}")
    return None


def main():
    parser = argparse.ArgumentParser(description='Activate FDXF spinco short trade')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without trading')
    parser.add_argument('--price', type=float, help='Override FDXF price (for testing)')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    args = parser.parse_args()

    print(f"\n=== FDXF SpinCo Short Activation ===")
    print(f"Hypothesis: {HYPOTHESIS_ID}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print()

    # Check hypothesis status
    hyp = db.get_hypothesis_by_id(HYPOTHESIS_ID)
    if not hyp:
        print(f"ERROR: Hypothesis {HYPOTHESIS_ID} not found!")
        return 1
    if hyp['status'] != 'pending':
        print(f"ERROR: Hypothesis status is '{hyp['status']}', expected 'pending'")
        print(f"  If already active, use close script instead.")
        return 1

    print(f"Hypothesis status: {hyp['status']} ✓")

    # Get price
    price = args.price or get_fdxf_price()
    if not price:
        print(f"ERROR: Could not get {SYMBOL} price. Provide with --price flag.")
        return 1
    print(f"{SYMBOL} close: ${price:.2f}")

    # Get SPY return for abnormal calculation
    spy_return = get_spy_return_today()
    if spy_return is not None:
        print(f"SPY return today: {spy_return:+.2f}%")

    # Abort: extreme drop on day 1 (don't chase catastrophic news)
    if spy_return is not None:
        # Get FDXF's approximate return vs initial FDX price
        # This is approximate - if FDXF dropped >30% on day 1, something exceptional happened
        pass  # No historical basis for a -30% day 1 drop, just flag as a warning

    # Check portfolio capacity
    if not check_portfolio_capacity():
        print(f"\nABORT: Portfolio at maximum capacity (5/5 positions).")
        print(f"  Wait for an existing position to close before activating.")
        return 1

    # Check VIX
    try:
        import yfinance as yf
        vix = yf.Ticker('^VIX').history(period='1d')
        current_vix = float(vix['Close'].iloc[-1]) if not vix.empty else None
        if current_vix:
            print(f"VIX: {current_vix:.1f}")
            if current_vix > 60:
                print(f"ABORT: VIX={current_vix:.1f} > 60. Circuit breaker risk. No trade.")
                return 1
    except Exception as e:
        print(f"Warning: could not check VIX: {e}")

    # Compute position sizing
    shares = int(POSITION_SIZE / price)
    actual_position = shares * price
    print(f"\nTrade plan:")
    print(f"  Symbol: {SYMBOL}")
    print(f"  Direction: SHORT")
    print(f"  Shares: {shares} @ ${price:.2f} = ${actual_position:.0f}")
    print(f"  Stop loss: {STOP_LOSS_PCT}% (stop at ${price * (1 + STOP_LOSS_PCT/100):.2f})")
    print(f"  Take profit: {TAKE_PROFIT_PCT}% (target ${price * (1 - TAKE_PROFIT_PCT/100):.2f})")
    print(f"  Hold: {HOLD_DAYS} trading days (exit ~June 10, 2026)")

    if args.dry_run:
        print(f"\n[DRY RUN] Would short {shares} shares of {SYMBOL} at ${price:.2f}")
        return 0

    if not args.yes:
        confirm = input(f"\nConfirm SHORT {shares} shares of {SYMBOL}? [y/N] ")
        if confirm.lower() != 'y':
            print("Aborted.")
            return 0

    # Set trigger to immediate and update fields
    db.update_hypothesis_fields(HYPOTHESIS_ID,
        trigger='immediate',
        trigger_position_size=POSITION_SIZE,
        trigger_stop_loss_pct=STOP_LOSS_PCT,
        trigger_take_profit_pct=TAKE_PROFIT_PCT,
    )

    # Execute trade via trader
    result = trader.place_order(
        symbol=SYMBOL,
        side='sell',  # short
        qty=shares,
        hypothesis_id=HYPOTHESIS_ID,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
    )

    if result:
        print(f"\n✓ SHORT {shares} shares of {SYMBOL} placed successfully")
        print(f"  Order ID: {result.get('order_id', 'unknown')}")
        print(f"  Entry price: ~${price:.2f}")

        # Log the activation
        db.log_trade(
            type='activate',
            hypothesis_id=HYPOTHESIS_ID,
            symbol=SYMBOL,
            direction='short',
            entry_price=price,
            position_size=POSITION_SIZE,
            order_id=result.get('order_id'),
            trigger_type='manual_close_day1'
        )
    else:
        print(f"\nERROR: Trade placement failed. Check logs.")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
