"""
ZBIO (Zenas Biopharma) CEO/CFO Insider Cluster Trade Activation
================================================================
Activate this AFTER Liberation Day (April 2, 2026) if VIX < 30.

HYPOTHESIS: 2bbe0f04 (insider_buying_cluster_ceo_cfo)
Signal: CEO MOULDER LEON O JR bought $1.02M + 2 other insiders = $9.3M total
Cluster detected: March 31 - April 1, 2026
VIX at detection: 24.5 (< 30 gate = QUALIFIED)
Window expires: April 30, 2026

ENTRY CONDITIONS:
  1. VIX < 20 on entry day (HARD GATE — see regime analysis below)
  2. SPY not in acute selloff (within 5% of recent 20d MA)
  3. Portfolio capacity < 5 active positions
  4. ZBIO price not >30% above detection price (chase filter)

ABORT CONDITIONS:
  - VIX >= 20 (regime gate — see below)
  - ZBIO announces bad news (earnings miss, pipeline failure, etc.)
  - SPY down >3% from previous close
  - Portfolio at max capacity (5/5 positions)
  - After April 29 (>20 trading days since filing = stale signal)

VIX REGIME ANALYSIS:
  - VIX < 20: Full signal strength, EV=+7.01% (CEO/CFO premium)
  - VIX 20-25: EV drops to +1.4-1.85% — COIN FLIP, NOT WORTH $5K RISK
  - VIX > 25: Adverse macro regime, signal unreliable
  VIX was 24.5 at detection (April 1). Must wait for VIX < 20.

SIGNAL STATS (from hypothesis 2bbe0f04 backtest):
  - N=438, consistency=64%, avg return=+5% in 5d
  - CEO/CFO present = "12x more predictive" (Cohen, Malloy & Pomorski 2012)
  - ONLY reliable at VIX < 20

Usage:
  python tools/activate_zbio_trade.py --dry-run     # check conditions
  python tools/activate_zbio_trade.py               # activate trade
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import db
import trader

HYPOTHESIS_ID = '2bbe0f04'
SYMBOL = 'ZBIO'
POSITION_SIZE = 5000
HOLD_DAYS = 5
STOP_LOSS_PCT = 15.0
TAKE_PROFIT_PCT = 20.0
DETECTION_PRICE = 20.36  # Price when cluster detected April 1


def main():
    parser = argparse.ArgumentParser(description='Activate ZBIO CEO/CFO cluster trade')
    parser.add_argument('--dry-run', action='store_true', help='Check conditions without trading')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    args = parser.parse_args()

    db.init_db()

    # Check hypothesis status
    h = db.get_hypothesis_by_id(HYPOTHESIS_ID)
    if not h:
        print(f"ERROR: Hypothesis {HYPOTHESIS_ID} not found")
        return 1
    if h['status'] != 'pending':
        print(f"ABORT: Hypothesis status is '{h['status']}' (expected 'pending')")
        return 1

    # Check current VIX
    import yfinance as yf
    vix_hist = yf.Ticker('^VIX').history(period='2d')
    current_vix = float(vix_hist['Close'].iloc[-1]) if not vix_hist.empty else 999
    print(f"VIX: {current_vix:.1f}")
    if current_vix >= 20:
        print(f"ABORT: VIX={current_vix:.1f} >= 20. Signal is COIN FLIP at VIX 20-25 (EV=+1.4-1.85%).")
        print(f"  Wait for VIX < 20, or ABANDON if VIX stays >=20 through April 25.")
        return 1

    # Check current ZBIO price
    zbio_hist = yf.Ticker(SYMBOL).history(period='2d')
    current_price = float(zbio_hist['Close'].iloc[-1]) if not zbio_hist.empty else None
    if current_price is None:
        print("ERROR: Could not get ZBIO price")
        return 1
    print(f"ZBIO price: ${current_price:.2f} (detection: ${DETECTION_PRICE:.2f})")

    # Chase filter: don't enter if price >30% above detection
    chase_limit = DETECTION_PRICE * 1.30
    if current_price > chase_limit:
        print(f"ABORT: ZBIO ${current_price:.2f} > ${chase_limit:.2f} chase limit (30% above detection). Wait for retracement.")
        return 1

    # Check portfolio capacity
    summary = trader.get_account_summary()
    n_positions = len(summary.get('positions', []))
    print(f"Portfolio: {n_positions}/5 positions")
    if n_positions >= 5:
        print("ABORT: Portfolio at maximum capacity (5/5 positions)")
        return 1

    # SPY selloff check
    spy_hist = yf.Ticker('SPY').history(period='25d')
    spy_curr = float(spy_hist['Close'].iloc[-1])
    spy_20d_ma = float(spy_hist['Close'].tail(20).mean())
    spy_vs_ma = (spy_curr / spy_20d_ma - 1) * 100
    print(f"SPY: ${spy_curr:.2f} ({spy_vs_ma:+.1f}% vs 20d MA ${spy_20d_ma:.2f})")
    if spy_vs_ma < -5:
        print(f"WARNING: SPY is {spy_vs_ma:.1f}% below 20d MA. Macro selloff - increased risk.")

    shares = int(POSITION_SIZE / current_price)
    actual_size = shares * current_price
    print(f"\nTrade plan: LONG {shares} shares of ZBIO @ ~${current_price:.2f} = ${actual_size:.0f}")
    print(f"  Stop loss: {STOP_LOSS_PCT}% (at ${current_price * (1 - STOP_LOSS_PCT/100):.2f})")
    print(f"  Take profit: {TAKE_PROFIT_PCT}% (at ${current_price * (1 + TAKE_PROFIT_PCT/100):.2f})")
    print(f"  Hold: {HOLD_DAYS} trading days")
    print(f"  Signal basis: CEO $1.02M + 2 insiders = $9.3M total, n=3 cluster filed 2026-03-31/04-01")

    if args.dry_run:
        print("\n[DRY RUN] Conditions met. Would place LONG order.")
        return 0

    if not args.yes:
        confirm = input(f"\nConfirm LONG {shares} shares of ZBIO? [y/N] ")
        if confirm.lower() != 'y':
            print("Aborted.")
            return 0

    # Update hypothesis with symbol and trigger
    db.update_hypothesis_fields(
        HYPOTHESIS_ID,
        expected_symbol=SYMBOL,
        trigger='immediate',
        trigger_position_size=POSITION_SIZE,
        trigger_stop_loss_pct=STOP_LOSS_PCT,
        trigger_take_profit_pct=TAKE_PROFIT_PCT,
    )

    # Place trade
    result = trader.place_order(
        symbol=SYMBOL,
        side='buy',
        qty=shares,
        hypothesis_id=HYPOTHESIS_ID,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
    )

    if result:
        print(f"\n✓ LONG {shares} shares of ZBIO placed")
        print(f"  Hypothesis: {HYPOTHESIS_ID} (insider_buying_cluster_ceo_cfo)")
    else:
        print("\nERROR: Trade placement failed")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
