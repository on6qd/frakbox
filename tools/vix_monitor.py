"""
vix_monitor.py - Monitor VIX for first-close-above-30 signal.

Checks whether VIX has closed above 30 and, if so, whether this is the FIRST
close above 30 in a 30-calendar-day window (no prior >30 close in last 30 days).
If the condition fires and hypothesis b63a0168 is still pending with no trigger set,
it activates the hypothesis by setting trigger='next_market_open'.

Usage:
    python tools/vix_monitor.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import db
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


VIX_THRESHOLD = 30.0
CLUSTER_WINDOW_DAYS = 30
HYPOTHESIS_ID = 'b63a0168'


def run():
    db.init_db()

    # Fetch 40 days of VIX history to evaluate the 30-day clustering condition
    vix_ticker = yf.Ticker('^VIX')
    hist = vix_ticker.history(period='40d')

    if hist.empty:
        print("ERROR: Could not fetch VIX data from yfinance.")
        sys.exit(1)

    # Flatten MultiIndex columns if present (guard for yfinance version changes)
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.get_level_values(0)

    # Ensure the index is timezone-naive for consistent date arithmetic
    hist.index = hist.index.tz_localize(None) if hist.index.tzinfo is not None else hist.index

    # Sort ascending (yfinance usually returns newest last, but be safe)
    hist = hist.sort_index()

    # Most recent close
    latest_date = hist.index[-1].date()
    latest_close = float(hist['Close'].iloc[-1])

    print(f"--- VIX Monitor ---")
    print(f"Most recent VIX close: {latest_close:.2f} (date: {latest_date})")
    print(f"Signal threshold: VIX > {VIX_THRESHOLD}")

    if latest_close <= VIX_THRESHOLD:
        print(f"Status: VIX below {VIX_THRESHOLD} — no signal. Currently watching.")
        hyp = db.get_hypothesis_by_id(HYPOTHESIS_ID)
        if hyp:
            print(f"Hypothesis {HYPOTHESIS_ID} ({hyp['expected_symbol']} {hyp['expected_direction']}): "
                  f"status={hyp['status']}, trigger={hyp['trigger']}")
        return

    # VIX is above 30 — check if this is the first close above 30 in a 30-day window
    print(f"VIX is ABOVE {VIX_THRESHOLD} — checking 30-day cluster window...")

    window_start = pd.Timestamp(latest_date) - timedelta(days=CLUSTER_WINDOW_DAYS)

    # All closes in the 30-day window EXCLUDING today/latest
    prior_window = hist[
        (hist.index >= window_start) & (hist.index < hist.index[-1])
    ]
    prior_above_30 = prior_window[prior_window['Close'] > VIX_THRESHOLD]

    if not prior_above_30.empty:
        earliest_prior = prior_above_30.index[0].date()
        print(f"Cluster condition NOT met: prior VIX>30 close found on {earliest_prior} "
              f"(within {CLUSTER_WINDOW_DAYS}-day window). This is NOT a first-touch event.")
        hyp = db.get_hypothesis_by_id(HYPOTHESIS_ID)
        if hyp:
            print(f"Hypothesis {HYPOTHESIS_ID}: status={hyp['status']}, trigger={hyp['trigger']}")
        return

    # This IS the first close above 30 in the 30-day window — potential activation
    print(f"FIRST close above {VIX_THRESHOLD} in {CLUSTER_WINDOW_DAYS}-day window confirmed.")
    print(f"Checking hypothesis {HYPOTHESIS_ID} for activation...")

    hyp = db.get_hypothesis_by_id(HYPOTHESIS_ID)
    if hyp is None:
        print(f"ERROR: Hypothesis {HYPOTHESIS_ID} not found in database.")
        return

    print(f"Hypothesis: {hyp['event_type']} | symbol={hyp['expected_symbol']} "
          f"| direction={hyp['expected_direction']} | status={hyp['status']} "
          f"| trigger={hyp['trigger']}")

    if hyp['status'] == 'pending' and hyp['trigger'] is None:
        db.update_hypothesis_fields(
            HYPOTHESIS_ID,
            trigger='next_market_open',
        )
        print()
        print("=" * 60)
        print("ACTIVATED: Hypothesis b63a0168 (VIX SPY long)")
        print(f"  VIX closed at {latest_close:.2f} on {latest_date}")
        print(f"  First close above {VIX_THRESHOLD} in {CLUSTER_WINDOW_DAYS} days.")
        print(f"  Trigger set to: next_market_open")
        print(f"  trade_loop.py will execute at next market open.")
        print("=" * 60)
    elif hyp['status'] == 'pending' and hyp['trigger'] is not None:
        print(f"Hypothesis already has trigger='{hyp['trigger']}' — no action needed.")
    else:
        print(f"Hypothesis status is '{hyp['status']}' — no activation required.")


if __name__ == '__main__':
    run()
