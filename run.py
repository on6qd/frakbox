#!/usr/bin/env python3
"""
Research runner — manages the hypothesis-test-learn loop.

Usage:
  python run.py --status          # show account, active experiments, research summary
  python run.py --review           # check active hypotheses against current prices
"""

import sys
import json
from datetime import datetime

from trader import get_account_summary, place_experiment, close_position
from research import (
    load_hypotheses,
    get_active_hypotheses,
    get_pending_hypotheses,
    get_research_summary,
    complete_hypothesis,
)


def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


def show_status():
    """Show account state, active experiments, and research progress."""
    print_header(f"RESEARCH STATUS — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Account
    summary = get_account_summary()
    print(f"\n  Account:")
    print(f"    Equity: ${summary['equity']:,.0f}")
    print(f"    Cash: ${summary['cash']:,.0f}")
    print(f"    Buying Power: ${summary['buying_power']:,.0f}")

    # Active experiments
    active = get_active_hypotheses()
    print(f"\n  Active Experiments: {len(active)}")
    for h in active:
        trade = h.get("trade", {})
        print(f"    #{h['id']} {h['expected_symbol']} ({h['expected_direction']}) — {h['event_type']}")
        print(f"       Entry: ${trade.get('entry_price', 0):.2f} | Deadline: {trade.get('deadline', 'n/a')[:10]}")
        print(f"       Thesis: {h['event_description'][:80]}")

    # Pending
    pending = get_pending_hypotheses()
    if pending:
        print(f"\n  Pending Hypotheses: {len(pending)}")
        for h in pending:
            print(f"    #{h['id']} {h['expected_symbol']} — {h['event_description'][:60]}")

    # Positions
    if summary["positions"]:
        print(f"\n  Open Positions:")
        for p in summary["positions"]:
            print(f"    {p['symbol']}: {p['qty']} shares "
                  f"(entry ${p['entry_price']:.2f}, now ${p['current_price']:.2f}, "
                  f"P&L: ${p['unrealized_pl']:+,.2f} / {p['unrealized_plpc']:+.1f}%)")

    # Research summary
    research = get_research_summary()
    print(f"\n  Research Progress:")
    print(f"    Total hypotheses: {research['total_hypotheses']}")
    print(f"    Direction accuracy: {research['direction_accuracy']}")
    if research['by_event_type']:
        print(f"    By type: {research['by_event_type']}")

    print()


def review_experiments():
    """Check active experiments — have they hit their target or deadline?"""
    print_header(f"EXPERIMENT REVIEW — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    active = get_active_hypotheses()
    if not active:
        print("\n  No active experiments to review.")
        return

    summary = get_account_summary()
    positions = {p["symbol"]: p for p in summary["positions"]}

    for h in active:
        symbol = h["expected_symbol"]
        trade = h.get("trade", {})
        entry_price = trade.get("entry_price", 0)
        deadline = trade.get("deadline", "")

        print(f"\n  Experiment #{h['id']}: {symbol} ({h['expected_direction']})")
        print(f"  Event: {h['event_description'][:80]}")
        print(f"  Expected: {h['expected_magnitude_pct']:+.1f}% within {h['expected_timeframe_days']} days")

        if symbol in positions:
            p = positions[symbol]
            print(f"  Current: {p['unrealized_plpc']:+.1f}% (${p['unrealized_pl']:+,.2f})")
            print(f"  Deadline: {deadline[:10]}")

            # Check if deadline passed
            if deadline and datetime.now().isoformat() > deadline:
                print(f"  >>> DEADLINE PASSED — should close and record result")
        else:
            print(f"  WARNING: No position found for {symbol}")

    print()


if __name__ == "__main__":
    if "--review" in sys.argv:
        review_experiments()
    else:
        show_status()
