"""
tariff_rollback_analysis.py - Backtest of tariff rollback/pause events on sector ETFs.

Tests whether beaten-down sectors (SOXX, XLF, XLI) outperform SPY following
official tariff rollbacks or pauses, while GLD underperforms (risk-on reversal).

Events tested (US-China trade war + Liberation Day):
  1. 2018-12-01: G20 90-day truce
  2. 2019-01-19: US-China trade talks restart / partial truce confirmed
  3. 2019-05-15: Huawei 90-day temporary reprieve
  4. 2019-10-11: Phase 1 deal framework announced
  5. 2019-12-13: Phase 1 deal signed
  6. 2025-04-09: Liberation Day 90-day rollback

Usage:
    python3 tools/tariff_rollback_analysis.py
"""

from __future__ import annotations

import sys
import os

# Make sure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from tools.yfinance_utils import get_close_prices

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROLLBACK_EVENTS = [
    {
        "date": "2018-12-01",
        "label": "G20 90-day truce",
        "notes": "US-China agreed to pause new tariffs at G20 Buenos Aires",
    },
    {
        "date": "2019-01-19",
        "label": "US-China talks resume / partial truce",
        "notes": "Reports of progress in trade talks after December agreement; markets rallied",
    },
    {
        "date": "2019-05-15",
        "label": "Huawei 90-day reprieve",
        "notes": "Commerce Dept granted Huawei temporary general license amid May 10 escalation",
    },
    {
        "date": "2019-10-11",
        "label": "Phase 1 framework announced",
        "notes": "Trump announced Phase 1 deal framework; paused Oct 15 tariff increase",
    },
    {
        "date": "2019-12-13",
        "label": "Phase 1 deal signed",
        "notes": "US-China Phase 1 trade deal signed; reduced tariffs on $360B of goods",
    },
    {
        "date": "2025-04-09",
        "label": "Liberation Day 90-day rollback",
        "notes": "Trump paused reciprocal tariffs for 90 days; kept 10% baseline + China 125%",
    },
]

TICKERS = ["SPY", "SOXX", "XLF", "XLI", "GLD", "KRE"]
HORIZONS = [1, 5, 10]  # trading days

FETCH_START = "2018-11-01"
FETCH_END   = "2025-05-01"


# ---------------------------------------------------------------------------
# Data fetch
# ---------------------------------------------------------------------------

def fetch_data() -> pd.DataFrame:
    print(f"Fetching price data for: {', '.join(TICKERS)}")
    print(f"  Period: {FETCH_START} to {FETCH_END}")
    closes = get_close_prices(TICKERS, start=FETCH_START, end=FETCH_END)
    print(f"  Downloaded {len(closes)} trading days")
    return closes


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def get_trading_day(closes: pd.DataFrame, event_date: str, offset: int = 0) -> pd.Timestamp | None:
    """Return the Nth trading day on or after event_date (offset=0 means the event day itself)."""
    dates = closes.index
    event_ts = pd.Timestamp(event_date)
    # Find first trading day >= event_date
    future = dates[dates >= event_ts]
    if len(future) == 0:
        return None
    base_idx = dates.get_loc(future[0])
    target_idx = base_idx + offset
    if target_idx >= len(dates):
        return None
    return dates[target_idx]


def calc_return(closes: pd.DataFrame, ticker: str, t0: pd.Timestamp, t1: pd.Timestamp) -> float | None:
    """Return from close on t0 to close on t1."""
    if t0 not in closes.index or t1 not in closes.index:
        return None
    p0 = closes.loc[t0, ticker]
    p1 = closes.loc[t1, ticker]
    if pd.isna(p0) or pd.isna(p1) or p0 == 0:
        return None
    return (p1 / p0 - 1) * 100  # in percent


def calc_pre_event_spy(closes: pd.DataFrame, event_date: str, lookback: int = 5) -> float | None:
    """SPY return over lookback days BEFORE the event (to confirm prior selloff)."""
    t0 = get_trading_day(closes, event_date, offset=0)
    if t0 is None:
        return None
    idx = closes.index.get_loc(t0)
    if idx < lookback:
        return None
    t_start = closes.index[idx - lookback]
    return calc_return(closes, "SPY", t_start, t0)


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def analyze() -> None:
    closes = fetch_data()

    print("\n" + "=" * 80)
    print("TARIFF ROLLBACK ANALYSIS — SECTOR ABNORMAL RETURNS")
    print("=" * 80)

    results_by_horizon: dict[int, dict[str, list]] = {h: {t: [] for t in TICKERS if t != "SPY"} for h in HORIZONS}
    all_rows = []

    for event in ROLLBACK_EVENTS:
        edate = event["date"]
        label = event["label"]

        t0 = get_trading_day(closes, edate, offset=0)
        if t0 is None:
            print(f"\n[SKIP] {edate} — no trading day found")
            continue

        pre_spy_5d = calc_pre_event_spy(closes, edate, lookback=5)
        pre_spy_3d = calc_pre_event_spy(closes, edate, lookback=3)
        spy_event_day = calc_return(closes, "SPY", closes.index[closes.index.get_loc(t0) - 1], t0) if closes.index.get_loc(t0) > 0 else None

        print(f"\n{'=' * 70}")
        print(f"EVENT: {edate} — {label}")
        print(f"  Notes: {event['notes']}")
        print(f"  Pre-event SPY:  3d = {pre_spy_3d:+.1f}%  |  5d = {pre_spy_5d:+.1f}%" if pre_spy_5d is not None and pre_spy_3d is not None else "  Pre-event: N/A")
        print(f"  Event-day SPY:  {spy_event_day:+.1f}%" if spy_event_day is not None else "  Event-day SPY: N/A")
        print()
        print(f"  {'Ticker':<8} {'1d raw':>8} {'1d abnl':>9} {'5d raw':>8} {'5d abnl':>9} {'10d raw':>9} {'10d abnl':>10}")
        print(f"  {'-' * 72}")

        row = {"date": edate, "label": label, "pre_spy_5d": pre_spy_5d, "spy_event_day": spy_event_day}

        for ticker in TICKERS:
            raw_returns = {}
            abnl_returns = {}
            spy_returns = {}

            for h in HORIZONS:
                t1 = get_trading_day(closes, edate, offset=h)
                if t1 is None:
                    raw_returns[h] = None
                    abnl_returns[h] = None
                    spy_returns[h] = None
                    continue
                raw = calc_return(closes, ticker, t0, t1)
                spy_r = calc_return(closes, "SPY", t0, t1)
                abnl = (raw - spy_r) if (raw is not None and spy_r is not None) else None
                raw_returns[h] = raw
                abnl_returns[h] = abnl
                spy_returns[h] = spy_r

            def fmt(v):
                return f"{v:+.1f}%" if v is not None else "  N/A"

            print(f"  {ticker:<8} {fmt(raw_returns[1]):>8} {fmt(abnl_returns[1]):>9} "
                  f"{fmt(raw_returns[5]):>8} {fmt(abnl_returns[5]):>9} "
                  f"{fmt(raw_returns[10]):>9} {fmt(abnl_returns[10]):>10}")

            for h in HORIZONS:
                if ticker != "SPY" and abnl_returns[h] is not None:
                    results_by_horizon[h][ticker].append(abnl_returns[h])

            row[f"{ticker}_1d_raw"] = raw_returns.get(1)
            row[f"{ticker}_1d_abnl"] = abnl_returns.get(1)
            row[f"{ticker}_5d_raw"] = raw_returns.get(5)
            row[f"{ticker}_5d_abnl"] = abnl_returns.get(5)
            row[f"{ticker}_10d_raw"] = raw_returns.get(10)
            row[f"{ticker}_10d_abnl"] = abnl_returns.get(10)

        all_rows.append(row)

    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("SUMMARY: AVERAGE ABNORMAL RETURNS ACROSS ALL ROLLBACK EVENTS")
    print("=" * 80)
    print(f"\n{'Ticker':<8} {'N':>4} {'Avg 1d':>9} {'Dir 1d':>8} {'Avg 5d':>9} {'Dir 5d':>8} {'Avg 10d':>10} {'Dir 10d':>9}")
    print("-" * 75)

    SIGNAL_TICKERS = ["SOXX", "XLF", "XLI", "GLD", "KRE"]
    for ticker in SIGNAL_TICKERS:
        n = len(results_by_horizon[5][ticker])
        if n == 0:
            continue

        stats_rows = []
        for h in HORIZONS:
            vals = results_by_horizon[h][ticker]
            if not vals:
                stats_rows.append(("N/A", "N/A"))
                continue
            avg = np.mean(vals)
            # Directional: SOXX/XLF/XLI/KRE should be positive; GLD should be negative
            if ticker == "GLD":
                direction = sum(1 for v in vals if v < -0.5) / len(vals)
                dir_label = f"{direction:.0%} neg"
            else:
                direction = sum(1 for v in vals if v > 0.5) / len(vals)
                dir_label = f"{direction:.0%} pos"
            stats_rows.append((f"{avg:+.2f}%", dir_label))

        print(f"  {ticker:<6} {n:>4}  {stats_rows[0][0]:>8} {stats_rows[0][1]:>8}  "
              f"{stats_rows[1][0]:>8} {stats_rows[1][1]:>8}  "
              f"{stats_rows[2][0]:>9} {stats_rows[2][1]:>9}")

    # ------------------------------------------------------------------
    # Hypothesis validity check
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("HYPOTHESIS VALIDITY CHECK")
    print("=" * 80)
    print("\nHypothesis: After tariff rollback, beaten-down sectors outperform SPY")
    print("by 3%+ over 5-10 trading days. GLD should reverse (underperform SPY).")
    print()

    LONG_TICKERS = ["SOXX", "XLF", "XLI", "KRE"]
    for h in [5, 10]:
        print(f"  --- {h}-day horizon ---")
        for ticker in LONG_TICKERS:
            vals = results_by_horizon[h][ticker]
            if not vals:
                continue
            avg = np.mean(vals)
            direction = sum(1 for v in vals if v > 0.5) / len(vals)
            meets_return = avg >= 3.0
            meets_direction = direction >= 0.60
            status = "PASS" if (meets_return and meets_direction) else "FAIL"
            print(f"  {ticker:<6} avg={avg:+.2f}%  dir={direction:.0%}  3%+ threshold: {'YES' if meets_return else 'NO'}  60%+ dir: {'YES' if meets_direction else 'NO'}  -> {status}")

        gld_vals = results_by_horizon[h]["GLD"]
        if gld_vals:
            avg = np.mean(gld_vals)
            direction_neg = sum(1 for v in gld_vals if v < -0.5) / len(gld_vals)
            print(f"  GLD    avg={avg:+.2f}%  neg_dir={direction_neg:.0%}  (should be negative after rollback)")
        print()

    # ------------------------------------------------------------------
    # Implications for active Liberation Day positions
    # ------------------------------------------------------------------
    print("=" * 80)
    print("IMPLICATIONS FOR ACTIVE POSITIONS (GLD/WFC/KRE/COST)")
    print("=" * 80)
    print()

    # Focus on the Liberation Day event for this
    ld_event = next((r for r in all_rows if r["date"] == "2025-04-09"), None)
    if ld_event:
        print("Liberation Day 2025-04-09 — actual outcome (OOS for current strategy):")
        for ticker in ["SOXX", "XLF", "XLI", "GLD", "KRE"]:
            r5 = ld_event.get(f"{ticker}_5d_abnl")
            r10 = ld_event.get(f"{ticker}_10d_abnl")
            raw5 = ld_event.get(f"{ticker}_5d_raw")
            raw10 = ld_event.get(f"{ticker}_10d_raw")
            print(f"  {ticker:<6}  5d abnl={r5:+.1f}%  (raw {raw5:+.1f}%)  |  10d abnl={r10:+.1f}%  (raw {raw10:+.1f}%)"
                  if all(v is not None for v in [r5, r10, raw5, raw10])
                  else f"  {ticker:<6}  data unavailable")

    print()
    print("Active positions to monitor for rollback exit:")
    print("  GLD long   -> SELL if rollback announced (GLD should drop on risk-on)")
    print("  KRE short  -> CLOSE short if rollback announced (KRE should spike)")
    print("  WFC short  -> CLOSE short if rollback announced (financials recover)")
    print("  COST long  -> May hold (defensive stocks lag on rollback - risk-on rotation)")
    print()
    print("Exit trigger criteria (if 2026 Liberation Day tariffs ARE rolled back):")
    print("  1. Official announcement day: SPY +2%+ intraday")
    print("  2. SOXX / KRE up 4%+ on announcement day")
    print("  3. GLD down 1%+ on announcement day")
    print("  -> If 2/3 criteria met: close GLD long, close KRE short, close WFC short")


if __name__ == "__main__":
    analyze()
