"""
politician_trade_monitor.py

Real-time monitor for politician stock disclosures.
Fetches recent disclosures and surfaces actionable large-cap purchase signals.

Usage:
    python tools/politician_trade_monitor.py         # print today's signals
    python tools/politician_trade_monitor.py --days 7  # last 7 days

Signal criteria (from backtest politician_signal_backtest.py):
    - Transaction type: Purchase
    - Amount: >= $50,000 (amount_min >= 50001)
    - Stock: Large-cap (>$1B market cap)
    - Action: Buy at next market open after disclosure date
"""

import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from tools.politician_trading_scraper import get_recent_trades, get_trades_for_ticker
from tools.largecap_filter import filter_to_largecap
import pandas as pd


LARGE_TRADE_THRESHOLD = 50_001   # amount_min for >$50K trades
MIN_MARKET_CAP_M = 1_000         # 1B market cap


def get_signals(days: int = 3) -> list[dict]:
    """
    Get politician purchase signals for the last N days.
    Returns sorted list of actionable signals (large purchases, large-cap stocks).
    """
    trades = get_recent_trades(days=max(days + 5, 30))   # buffer for scraping
    if not trades:
        print("Warning: No recent trades found.")
        return []

    # Filter to purchases within the window
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    purchases = [
        t for t in trades
        if t["transaction_type"] == "Purchase"
        and t.get("disclosure_date", "") >= cutoff
        and t.get("ticker")
        and (t.get("amount_min") or 0) >= LARGE_TRADE_THRESHOLD
    ]

    if not purchases:
        return []

    # Filter to large-cap
    unique_tickers = list(set(t["ticker"] for t in purchases))
    df = pd.DataFrame({"ticker": unique_tickers})
    df_lc = filter_to_largecap(df, min_market_cap_m=MIN_MARKET_CAP_M, verbose=False)
    largecap = set(df_lc["ticker"].tolist())

    signals = [t for t in purchases if t["ticker"] in largecap]

    # Sort by disclosure date (most recent first), then by amount (largest first)
    signals.sort(key=lambda t: (t["disclosure_date"], -(t.get("amount_min") or 0)), reverse=True)
    return signals


def format_signal(t: dict) -> str:
    """Format a signal for display."""
    amount_str = f"${t.get('amount_min', 0):,} - ${t.get('amount_max', 0):,}"
    gap = t.get("reporting_gap_days")
    gap_str = f" ({gap}d lag)" if gap else ""
    return (
        f"  {t['ticker']:6s}  {t['disclosure_date']}  "
        f"{t['politician']} ({t['chamber']}/{t['party']})  "
        f"${amount_str}{gap_str}"
    )


def main():
    parser = argparse.ArgumentParser(description="Politician trade monitor")
    parser.add_argument("--days", type=int, default=3, help="Days to look back")
    args = parser.parse_args()

    print(f"=== Politician Trade Signals (last {args.days} days) ===")
    print(f"Criteria: Purchase, >$50K, Large-cap (>$1B)")
    print()

    signals = get_signals(days=args.days)

    if not signals:
        print("No qualifying signals found.")
        return

    print(f"Found {len(signals)} signal(s):\n")
    for s in signals:
        print(format_signal(s))

    print(f"\n--- Backtest expectation ---")
    print(f"  5d abnormal return: +0.74% (N=247, p=0.002)")
    print(f"  Direction >0.5% ABN: 49% (marginal — awaiting full OOS validation)")
    print(f"  Status: PRELIMINARY — do not activate until full 34K dataset validated")


if __name__ == "__main__":
    main()
