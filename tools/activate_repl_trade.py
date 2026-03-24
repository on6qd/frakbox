"""
REPL (Replimune) FDA PDUFA Trade Activation Script
====================================================
Run this the MORNING AFTER the FDA decision on April 10, 2026 (or as soon as
the market opens on the day of the rejection, if announced AH/pre-mkt).

HYPOTHESES:
  - 5f805860 (fda_clinical_rejection_short): If REPL drops 40-55%, enter SHORT
  - d302c84b (clinical_efficacy_failure_short): If REPL drops >55%, enter SHORT

SIGNAL: REPL BLA resubmission for RP1 (vusolimogene oderparepvec) + nivolumab
in advanced melanoma. PDUFA date: April 10, 2026.
Prior CRL (July 2025): EFFICACY reasons — FDA questioned heterogeneity and trial design.
Resubmission did NOT run a new confirmatory trial — added analysis to same data.

BACKTEST (N=6 fda_clinical_rejection_short events, 2019-2024):
  - 3d avg abnormal return: -24.1% (from -15.3% to -34.7%)
  - Direction: 100% negative
  - OOS validation: 100% negative (all 3 OOS events)

BACKTEST (N=14 clinical_efficacy_failure_short events, 2019-2026):
  - 3d avg abnormal return: -27.5%
  - Direction: 92.9% negative
  - OOS 2022-2026: 87.5% negative

ENTRY CONDITIONS:
  1. FDA issued CRL or rejection (NOT approval)
  2. REPL opened DOWN on crash day
  3. Crash is due to CLINICAL/EFFICACY reasons (not CMC/manufacturing)
  4. REPL dropped ≥40% from prior close
  5. Portfolio has capacity (<5 active positions)

DECISION TREE:
  If crash_pct > 55%: use hypothesis d302c84b (larger effect expected)
  If crash_pct 40-55%: use hypothesis 5f805860

ABORT CONDITIONS:
  - FDA APPROVED (we only short on rejection)
  - Crash < 40% (not a clean signal - could reverse)
  - Crash > 85% (panic levels, may reverse sharply Monday)
  - CRL was for CMC/manufacturing reasons only (not efficacy)
  - Pre-announced: REPL stock declined >20% in 10 days before April 10

DATES:
  - PDUFA: April 10, 2026 (Friday)
  - FDA typically acts on or before PDUFA date
  - If announced AH on April 9 or April 10: enter at April 11 open
  - If announced April 10 pre-mkt: enter at April 10 open

Usage:
  python tools/activate_repl_trade.py [--dry-run] [--price XXXX] [--prior-close XXXX]
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import research
import trader
import db


# Hypothesis IDs
HYP_CRL_SHORT = '5f805860'     # fda_clinical_rejection_short (40-55% crash)
HYP_EFFICACY_SHORT = 'd302c84b'  # clinical_efficacy_failure_short (>55% crash)

POSITION_SIZE = 5000
HOLD_DAYS = 3
STOP_LOSS_PCT = 15.0       # Wider stop — biotech can recover sharply
TAKE_PROFIT_PCT = 25.0     # Capture 25% gain early
SYMBOL = 'REPL'

PDUFA_DATE = '2026-04-10'


def get_repl_price():
    """Get REPL current price."""
    try:
        import yfinance as yf
        hist = yf.Ticker(SYMBOL).history(period='2d')
        if len(hist) >= 2:
            return float(hist['Close'].iloc[-1]), float(hist['Close'].iloc[-2])
        elif not hist.empty:
            return float(hist['Close'].iloc[-1]), None
    except Exception as e:
        print(f"  Warning: could not get {SYMBOL} price: {e}")
    return None, None


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


def classify_crash(crash_pct):
    """Classify crash and return appropriate hypothesis."""
    abs_crash = abs(crash_pct)
    if abs_crash >= 85:
        return None, "ABORT: crash >85% (panic zone, potential reversal)"
    elif abs_crash >= 55:
        return HYP_EFFICACY_SHORT, f"STRONG: crash={abs_crash:.1f}% (>55%) -> use clinical_efficacy_failure_short"
    elif abs_crash >= 40:
        return HYP_CRL_SHORT, f"OK: crash={abs_crash:.1f}% (40-55%) -> use fda_clinical_rejection_short"
    else:
        return None, f"ABORT: crash={abs_crash:.1f}% (<40%) — below threshold, signal too weak"


def main():
    parser = argparse.ArgumentParser(description='Activate REPL FDA rejection short trade')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without trading')
    parser.add_argument('--price', type=float, help='Current REPL price (crash day open/close)')
    parser.add_argument('--prior-close', type=float, help='REPL prior close (before FDA decision)')
    parser.add_argument('--crl-type', choices=['clinical', 'cmc', 'both'],
                        help='Type of CRL: clinical (bad), cmc (abort), both (use clinical)')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    args = parser.parse_args()

    print(f"\n=== REPL FDA Rejection Short Activation ===")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print(f"PDUFA: {PDUFA_DATE}")
    print()

    # Get prices
    current_price, prior_close = get_repl_price()

    if args.price:
        current_price = args.price
    if args.prior_close:
        prior_close = args.prior_close

    if not current_price:
        print("ERROR: Could not get REPL price. Provide with --price flag.")
        return 1

    print(f"REPL current price: ${current_price:.2f}")

    if prior_close:
        crash_pct = (current_price / prior_close - 1) * 100
        print(f"REPL prior close: ${prior_close:.2f}")
        print(f"Crash: {crash_pct:+.1f}%")
    else:
        print("WARNING: No prior close available. Cannot compute crash %. Provide with --prior-close.")
        crash_pct = None

    # Check CRL type
    if args.crl_type:
        crl_type = args.crl_type
        if crl_type == 'cmc':
            print("\nABORT: CRL was CMC/manufacturing only.")
            print("  This signal requires CLINICAL/EFFICACY rejection.")
            print("  Historical: CMC CRLs show reversal (like RCKT 2024: recovered from -20%)")
            return 1
        print(f"CRL type: {crl_type} → qualifies for short")

    # Classify crash and select hypothesis
    if crash_pct is not None:
        hyp_id, message = classify_crash(crash_pct)
        print(f"\nSignal classification: {message}")

        if not hyp_id:
            return 1

        # Verify hypothesis status
        hyp = db.get_hypothesis_by_id(hyp_id)
        if not hyp:
            print(f"ERROR: Hypothesis {hyp_id} not found!")
            return 1
        if hyp['status'] != 'pending':
            print(f"ERROR: Hypothesis {hyp_id} status is '{hyp['status']}', expected 'pending'")
            return 1
        print(f"Hypothesis: {hyp_id} ({hyp['event_type']}) status={hyp['status']} ✓")

        # For d302c84b (TBD symbol), we need to update to REPL first
        if hyp.get('expected_symbol') == 'TBD' or not hyp.get('expected_symbol'):
            print(f"  Updating hypothesis symbol from TBD to REPL")
            db.update_hypothesis_fields(hyp_id, expected_symbol=SYMBOL)
    else:
        print("\nWARNING: Cannot determine crash %. Defaulting to fda_clinical_rejection_short.")
        hyp_id = HYP_CRL_SHORT

    # Check portfolio capacity
    if not check_portfolio_capacity():
        print(f"\nABORT: Portfolio at maximum capacity (5/5 positions).")
        return 1

    # Check VIX
    try:
        import yfinance as yf
        vix = yf.Ticker('^VIX').history(period='1d')
        current_vix = float(vix['Close'].iloc[-1]) if not vix.empty else None
        if current_vix:
            print(f"VIX: {current_vix:.1f}")
            if current_vix > 60:
                print(f"ABORT: VIX={current_vix:.1f} > 60. Circuit breaker risk.")
                return 1
    except Exception as e:
        print(f"Warning: could not check VIX: {e}")

    # Calculate shares
    shares = int(POSITION_SIZE / current_price)
    actual_position = shares * current_price

    print(f"\nTrade plan:")
    print(f"  Symbol: {SYMBOL}")
    print(f"  Direction: SHORT")
    print(f"  Shares: {shares} @ ${current_price:.2f} = ${actual_position:.0f}")
    print(f"  Stop loss: {STOP_LOSS_PCT}% (stop at ${current_price * (1 + STOP_LOSS_PCT/100):.2f})")
    print(f"  Take profit: {TAKE_PROFIT_PCT}% (target ${current_price * (1 - TAKE_PROFIT_PCT/100):.2f})")
    print(f"  Hold: {HOLD_DAYS} trading days")

    if args.dry_run:
        print(f"\n[DRY RUN] Would short {shares} shares of {SYMBOL} at ${current_price:.2f}")
        return 0

    if not args.yes:
        confirm = input(f"\nConfirm SHORT {shares} shares of {SYMBOL}? [y/N] ")
        if confirm.lower() != 'y':
            print("Aborted.")
            return 0

    # Set trigger to immediate
    db.update_hypothesis_fields(hyp_id,
        trigger='immediate',
        trigger_position_size=POSITION_SIZE,
        trigger_stop_loss_pct=STOP_LOSS_PCT,
        trigger_take_profit_pct=TAKE_PROFIT_PCT,
        expected_symbol=SYMBOL,
    )

    # Execute trade
    result = trader.place_order(
        symbol=SYMBOL,
        side='sell',  # short
        qty=shares,
        hypothesis_id=hyp_id,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
    )

    if result:
        print(f"\n✓ SHORT {shares} shares of {SYMBOL} placed successfully")
        print(f"  Order ID: {result.get('order_id', 'unknown')}")

        db.log_trade(
            type='activate',
            hypothesis_id=hyp_id,
            symbol=SYMBOL,
            direction='short',
            entry_price=current_price,
            position_size=POSITION_SIZE,
            order_id=result.get('order_id'),
            trigger_type='manual_fda_rejection'
        )
    else:
        print(f"\nERROR: Trade placement failed.")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
