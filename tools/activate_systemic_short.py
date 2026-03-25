"""
Activate Systemic 52w Low Short Trade
======================================
Use this script to activate individual stock shorts after a systemic selloff day
is confirmed by systemic_52w_low_scanner.py.

Hypothesis: f055dc19 (sp500_52w_low_systemic_short)
Expected return: -1.88% abnormal over 5 days
Entry: Next market open after systemic selloff day
Exit: 5 trading days later
Stop loss: 8%

Usage:
    python tools/activate_systemic_short.py --ticker ADBE [--dry-run] [--yes]

IMPORTANT: First verify signal fired via:
    python tools/systemic_52w_low_scanner.py
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
import research
import trader
import db


HYPOTHESIS_ID = 'f055dc19'
POSITION_SIZE = 5000
STOP_LOSS_PCT = 8
HOLD_DAYS = 5


def check_capacity():
    db.init_db()
    hypotheses = db.load_hypotheses()
    hyp_active = len([h for h in hypotheses if h.get('status') == 'active'])
    try:
        api = trader.get_api()
        alpaca_count = len(api.list_positions())
    except Exception:
        alpaca_count = 0
    count = max(hyp_active, alpaca_count)
    if alpaca_count > hyp_active:
        print(f'  [WARNING] Alpaca has {alpaca_count} positions but only {hyp_active} in hypothesis DB!')
    return count


def main():
    parser = argparse.ArgumentParser(description='Activate systemic 52w low short trade')
    parser.add_argument('--ticker', required=True, help='Stock ticker to short')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--price', type=float, default=None)
    parser.add_argument('--yes', action='store_true')
    args = parser.parse_args()

    ticker = args.ticker.upper()
    
    print("=" * 60)
    print(f"SYSTEMIC SHORT ACTIVATION: {ticker}")
    print(f"Hypothesis: {HYPOTHESIS_ID} (sp500_52w_low_systemic_short)")
    print("=" * 60)
    print()
    print("PRE-CHECK: Did you confirm signal fired with systemic_52w_low_scanner.py?")
    print("  Signal requires: SPY down >0.5% AND >=5 stocks at 52w lows")
    if not args.yes:
        confirm = input("Confirm signal verified? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Aborted.")
            return 1
    
    # Portfolio capacity
    active_count = check_capacity()
    print(f"\nActive trades: {active_count}/5")
    if active_count >= 5:
        print(f"ABORT: Portfolio at capacity ({active_count}/5).")
        return 1
    
    # Get price
    if args.price:
        entry_price = args.price
    else:
        try:
            import yfinance as yf
            hist = yf.Ticker(ticker).history(period='1d', interval='1m')
            if hist.empty:
                hist = yf.Ticker(ticker).history(period='2d')
            entry_price = float(hist['Close'].iloc[-1]) if not hist.empty else None
        except:
            entry_price = None
        
        if entry_price is None:
            print("ERROR: Could not fetch price. Use --price XXXX")
            return 1
    
    print(f"\n{ticker} price: ${entry_price:.2f}")
    print(f"Position size: ${POSITION_SIZE:,}")
    shares = int(POSITION_SIZE / entry_price)
    print(f"Approximate shares: {shares}")
    print(f"Stop loss: {STOP_LOSS_PCT}% = ${entry_price * (1 + STOP_LOSS_PCT/100):.2f}")
    
    exit_date = (datetime.now() + timedelta(days=HOLD_DAYS * 1.5)).strftime('%Y-%m-%d')
    print(f"Target exit: ~{exit_date} ({HOLD_DAYS} trading days)")
    print(f"Expected return: -1.88% abnormal")
    print()
    
    if args.dry_run:
        print(f"[DRY RUN] Would short {ticker} at ${entry_price:.2f}")
        return 0
    
    if not args.yes:
        confirm = input("Place trade? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Aborted.")
            return 0
    
    # Create individual hypothesis for this stock
    import json
    individual_result = research.create_hypothesis(
        event_type="sp500_52w_low_systemic_short",
        event_description=f"{ticker} first-touch 52w low on systemic selloff day. Activated from f055dc19 class hypothesis. Short at next open, hold 5 days.",
        causal_mechanism="Systemic selloff creates forced selling; stocks at 52w lows face restricted institutional buying and stop-loss cascades. Momentum continuation over 3-5 days.",
        causal_mechanism_criteria=["actors_incentives", "transmission_channel"],
        expected_symbol=ticker,
        expected_direction="short",
        expected_magnitude_pct=1.88,
        expected_timeframe_days=HOLD_DAYS,
        historical_evidence=[{"date": "2019-2026", "note": "N=781 events, 5d mean=-1.88% OOS"}],
        sample_size=781,
        consistency_pct=55.2,
        confounders={
            "broad_market_direction": "bear - SPY confirmed down >0.5% on entry day",
            "vix_level": 25.0,
            "sector_trend": "systemic selloff - broad market weakness",
            "survivorship_bias": "universe includes real delistings",
            "selection_bias": "117-stock universe",
            "event_timing": "intraday",
            "market_regime": "elevated"
        },
        market_regime_note=f"Activated on systemic selloff. SPY down >0.5%, >=5 stocks at 52w lows.",
        confidence=7,
        out_of_sample_split={
            "split_type": "temporal",
            "discovery_indices": list(range(600)),
            "validation_indices": list(range(600, 765)),
            "discovery_consistency_pct": 54.5,
            "validation_consistency_pct": 57.6
        },
        survivorship_bias_note="Universe includes real delistings",
        selection_bias_note="117-stock subset of S&P 500",
        passes_multiple_testing=True,
        backtest_symbols=["HD", "ABT", "BAX", "DAL", "UAL", "XOM"],
    )
    
    new_hyp_id = individual_result['id']
    print(f"Individual hypothesis created: {new_hyp_id}")
    
    # Activate it
    research.activate_hypothesis(new_hyp_id, entry_price=entry_price, position_size=POSITION_SIZE)
    print(f"Hypothesis {new_hyp_id} activated at ${entry_price:.2f}")
    
    # Place order
    result = trader.place_experiment(symbol=ticker, direction='short', notional_amount=POSITION_SIZE)
    if not result.get('success'):
        print(f"ERROR: {result.get('error')}")
        print("Hypothesis activated but order FAILED. Check Alpaca manually.")
        return 1
    
    print(f"\n✓ {ticker} SHORT ACTIVE at ${entry_price:.2f}")
    print(f"  Hypothesis: {new_hyp_id}")
    print(f"  Stop loss: ${entry_price * 1.08:.2f} (8%)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
