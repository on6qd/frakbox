"""
Liberation Day Monitoring Script
=================================
Run AFTER CLOSE on April 2, 2026 (or early morning April 3).

Checks:
1. SPY after-hours/next-open reaction
2. VIX level
3. Auto tariff stocks (TM, HMC, F, GM, STLA)
4. Validates if Liberation Day trigger conditions are met
5. Records in knowledge base

Usage:
    python tools/liberation_day_monitor.py
    python tools/liberation_day_monitor.py --tariff-rate 25 --universal
    python tools/liberation_day_monitor.py --help
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import db

# Baselines (March 31 2026 close - last day before Liberation Day)
BASELINES = {
    'SPY': 650.34, 'WFC': 79.61, 'AMD': 203.43, 'QCOM': 128.78,
    'GLD': 430.29, 'KRE': 65.15, 'AEP': 131.08, 'COST': 996.43,
    'STLD': 180.00, 'NUE': 169.10, 'SYK': 328.59,
    # Auto tariff stocks (March 31 2026 closes)
    'TM': 206.09, 'HMC': 24.31, 'F': 11.54, 'GM': 74.50, 'STLA': 7.09,
    # Auto parts
    'APTV': 69.44, 'VC': None,
}

# April 1 close targets (will be updated)
APRIL1_CLOSES = {}


def get_price(symbol):
    """Get latest close price."""
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period='3d')
        if not hist.empty:
            return float(hist['Close'].iloc[-1])
    except Exception as e:
        print(f"  Warning: {symbol} price error: {e}")
    return None


def compute_change(current, baseline):
    """Compute % change from baseline."""
    if current and baseline:
        return (current / baseline - 1) * 100
    return None


def assess_trigger_conditions(spy_change_pct, vix_level, tariff_rate, is_universal):
    """
    Assess whether Liberation Day conditions support the pre-registered trades.

    The trades were pre-registered unconditionally (fire April 6-7 regardless).
    This function assesses ex-post whether conditions support the hypothesis.
    """
    print("\n=== TRIGGER CONDITION ASSESSMENT ===")

    conditions = []

    # SPY reaction
    if spy_change_pct is not None:
        if spy_change_pct < -2:
            conditions.append(f"✓ SPY reaction strong ({spy_change_pct:+.1f}%) - signals significant tariff shock")
        elif spy_change_pct < 0:
            conditions.append(f"~ SPY reaction mild ({spy_change_pct:+.1f}%) - moderate tariff concern")
        else:
            conditions.append(f"✗ SPY UP ({spy_change_pct:+.1f}%) - market may be pricing in tariff optimism")

    # VIX level
    if vix_level is not None:
        if vix_level > 30:
            conditions.append(f"✓ VIX={vix_level:.1f} - fear elevated, shorts favored")
        elif vix_level > 25:
            conditions.append(f"~ VIX={vix_level:.1f} - moderate fear, mixed signal")
        else:
            conditions.append(f"✗ VIX={vix_level:.1f} - low fear, counter to short thesis")

    # Tariff severity
    if tariff_rate is not None:
        if tariff_rate >= 20:
            conditions.append(f"✓ Tariff rate {tariff_rate}% - substantial impact expected")
        elif tariff_rate >= 10:
            conditions.append(f"~ Tariff rate {tariff_rate}% - moderate impact")
        else:
            conditions.append(f"✗ Tariff rate {tariff_rate}% - below historical threshold (historical tests used >10%)")

    if is_universal:
        conditions.append("✓ Universal/broad tariff - affects all signal sectors (banks, semis, retail)")

    for c in conditions:
        print(f"  {c}")

    # Overall assessment
    supports = sum(1 for c in conditions if c.startswith('✓'))
    against = sum(1 for c in conditions if c.startswith('✗'))

    print(f"\nSupporting conditions: {supports}, Against: {against}")
    print("NOTE: Trades fire unconditionally (pre-registered). This is retrospective context only.")
    print("      Do NOT abort pre-registered trades based on this assessment.")


def main():
    parser = argparse.ArgumentParser(description='Monitor Liberation Day aftermath')
    parser.add_argument('--tariff-rate', type=float, help='Universal tariff rate announced (e.g. 20 for 20%)')
    parser.add_argument('--universal', action='store_true', help='Was it a universal/broad tariff?')
    parser.add_argument('--auto-tariff-pct', type=float, help='Auto tariff rate if different (e.g. 25)')
    parser.add_argument('--spy-ah', type=float, help='SPY after-hours price on April 2')
    args = parser.parse_args()

    print(f"\n=== LIBERATION DAY MONITORING ===")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M ET')}")
    print()

    # Get current prices
    print("Current prices (April 3 or post-Liberation Day):")
    prices = {}
    key_symbols = ['SPY', '^VIX', 'WFC', 'AMD', 'QCOM', 'GLD', 'KRE', 'AEP', 'COST', 'STLD', 'NUE',
                   'TM', 'HMC', 'F', 'GM', 'STLA']

    for sym in key_symbols:
        price = get_price(sym)
        if price:
            prices[sym] = price
            baseline = BASELINES.get(sym)
            if baseline:
                chg = compute_change(price, baseline)
                print(f"  {sym}: ${price:.2f} ({chg:+.1f}% from Mar31 baseline)")
            else:
                print(f"  {sym}: ${price:.2f}")

    print()

    # SPY change from baseline
    spy_price = prices.get('SPY')
    spy_baseline = BASELINES['SPY']
    spy_change = compute_change(spy_price, spy_baseline)

    if args.spy_ah:
        spy_ah_change = compute_change(args.spy_ah, spy_baseline)
        print(f"SPY after-hours (user provided): ${args.spy_ah:.2f} ({spy_ah_change:+.1f}% from baseline)")

    vix = prices.get('^VIX')

    # Assess trigger conditions
    assess_trigger_conditions(spy_change, vix, args.tariff_rate, args.universal)

    # Record in knowledge base
    print("\n=== RECORDING TO KNOWLEDGE BASE ===")
    notes = []
    notes.append(f"Liberation Day April 2 2026 monitoring. Run {datetime.now().strftime('%Y-%m-%d %H:%M')}.")

    if args.tariff_rate:
        notes.append(f"Tariff rate announced: {args.tariff_rate}%")
        if args.universal:
            notes.append("Universal/broad tariff confirmed")
    if args.auto_tariff_pct:
        notes.append(f"Auto tariff rate: {args.auto_tariff_pct}%")

    if spy_change:
        notes.append(f"SPY reaction: {spy_change:+.1f}% vs March 31 baseline")
    if vix:
        notes.append(f"VIX post-event: {vix:.1f}")

    # Compute abnormal returns for all hypothesis targets
    hyp_results = []
    for sym, hid, direction in [
        ('WFC', 'b73efac3', 'short'), ('AMD', '132e9128', 'short'), ('QCOM', '14de5527', 'short'),
        ('GLD', 'b768e8d8', 'long'), ('KRE', '6e732966', 'short'),
        ('AEP', '35b63a23', 'long'), ('COST', '8c2f8cbb', 'long')
    ]:
        price = prices.get(sym)
        baseline = BASELINES.get(sym)
        if price and baseline:
            chg = compute_change(price, baseline)
            spy_chg = spy_change if spy_change else 0
            abnormal = chg - spy_chg
            hyp_results.append(f"{sym}({direction}): {chg:+.1f}% raw, {abnormal:+.1f}% abnormal")

    if hyp_results:
        notes.append("Hypothesis targets: " + "; ".join(hyp_results))

    db.record_known_effect('liberation_day_2026_post_event_checklist', ' | '.join(notes))
    print("Recorded to knowledge base: liberation_day_2026_post_event_checklist")

    # Auto tariff check
    if args.auto_tariff_pct and args.auto_tariff_pct > 25:
        print(f"\n⚠️  AUTO TARIFFS > 25%: Check TM/HMC/F/GM/STLA prices")
        print("   This is a separate signal not in pre-registered hypotheses (auto_import_tariff_short)")
        for sym in ['TM', 'HMC', 'F', 'GM', 'STLA']:
            p = prices.get(sym)
            if p:
                b = BASELINES.get(sym)
                if b:
                    chg = compute_change(p, b)
                    print(f"   {sym}: {chg:+.1f}% from baseline")

    print("\nDONE. Key actions:")
    print("  1. Ensure WFC/AMD/QCOM triggers fire at April 6 09:30")
    print("  2. Ensure GLD/AEP/COST triggers fire at April 7 09:30")
    print("  3. SYK result: should have closed April 2 (deadline)")
    print("  4. Check KRE early close note (5d=April 17 based on 2025 OOS)")
    print("  5. Update stld_oos_april6_baseline knowledge entry")


if __name__ == '__main__':
    main()
