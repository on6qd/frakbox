"""
Backtest runner for VWAP deviation (disposition effect) signal.

Tests whether stocks trading > 15% above their 252-day rolling VWAP
subsequently underperform the market (short hypothesis).

Run:
    python tools/run_vwap_backtest.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import market_data
from tools.vwap_deviation_signal import find_vwap_deviation_events
from tools.largecap_filter import filter_to_largecap

import pandas as pd

# ── Universe ────────────────────────────────────────────────────────────────
UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "JPM", "JNJ", "XOM", "PG", "HD",
    "BA", "CAT", "GS", "WMT", "CVX", "MCD", "UNH", "AMGN",
    "HON", "IBM", "MMM", "GE",
]

FULL_START  = "2018-01-01"
FULL_END    = "2024-12-31"
IS_END      = "2023-12-31"   # in-sample cutoff (use 2024 for OOS later)
OOS_START   = "2024-01-01"


def print_results(label: str, result: dict):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

    n = result.get("events_measured", 0)
    print(f"  Events measured : {n}")
    print(f"  Entry mode      : {result.get('entry_price_mode', 'N/A')}")
    print(f"  Avg cost (rt)   : {result.get('avg_estimated_cost_pct', 'N/A')}")
    dqw = result.get("data_quality_warning")
    if dqw:
        print(f"  [WARNING] {dqw}")

    horizons = [1, 3, 5, 10, 20]
    print(f"\n  {'Horizon':>8} | {'Avg Abn Ret':>12} | {'Stdev':>8} | {'Pos Rate':>9} | {'p-value':>10} | {'Multi-test':>10}")
    print(f"  {'-'*8}-+-{'-'*12}-+-{'-'*8}-+-{'-'*9}-+-{'-'*10}-+-{'-'*10}")
    for h in horizons:
        avg   = result.get(f"avg_abnormal_{h}d",            "N/A")
        std   = result.get(f"stdev_abnormal_{h}d",          "N/A")
        pos   = result.get(f"positive_rate_abnormal_{h}d",  "N/A")
        pval  = result.get(f"p_value_abnormal_{h}d",        "N/A")
        multi = result.get("passes_multiple_testing",        "N/A")

        def fmt_f(v, decimals=3):
            if isinstance(v, float):
                return f"{v:>{8+decimals}.{decimals}f}"
            return f"{'N/A':>11}"

        avg_s  = f"{avg:>+12.3f}%" if isinstance(avg, float) else f"{'N/A':>12}"
        std_s  = f"{std:>8.3f}%"   if isinstance(std, float) else f"{'N/A':>8}"
        pos_s  = f"{pos:>8.1f}%"   if isinstance(pos, float) else f"{'N/A':>8}"
        pval_s = f"{pval:>10.4f}"  if isinstance(pval, float) else f"{'N/A':>10}"
        # pass/fail only shown for first horizon
        multi_s = f"{'YES' if multi else 'no':>10}" if h == 1 else f"{'':>10}"

        print(f"  {h:>7}d | {avg_s} | {std_s} | {pos_s} | {pval_s} | {multi_s}")

    # Bootstrap CIs
    print()
    for h in horizons:
        bci = result.get(f"bootstrap_ci_abnormal_{h}d")
        if bci and isinstance(bci, dict):
            excl = "CI EXCLUDES ZERO" if bci.get("ci_excludes_zero") else "CI includes zero"
            print(f"  Bootstrap {h}d: [{bci['ci_lower']:+.3f}%, {bci['ci_upper']:+.3f}%]  <- {excl}")

    # Wilcoxon
    print()
    for h in horizons:
        wp = result.get(f"wilcoxon_p_abnormal_{h}d")
        if wp is not None:
            print(f"  Wilcoxon {h}d p = {wp:.4f}")


def events_to_measure_format(events: list) -> list:
    """Convert our event dicts to market_data format."""
    return [{"symbol": e["symbol"], "date": e["date"]} for e in events]


def main():
    print("\n" + "="*60)
    print("  VWAP DEVIATION SIGNAL BACKTEST")
    print("  Disposition Effect: Price >> VWAP -> Short signal")
    print("="*60)

    # ── Pass 1: Base signal (no volume filter) ───────────────────────────────
    print(f"\n[Step 1] Scanning universe for VWAP>1.15 signals ({FULL_START} to {FULL_END})...")
    print(f"  Symbols: {UNIVERSE}")

    events_all = find_vwap_deviation_events(
        symbols=UNIVERSE,
        start_date=FULL_START,
        end_date=FULL_END,
        threshold=1.15,
        cooldown_days=20,
        require_low_volume=False,
        require_high_volume=False,
        verbose=True,
    )

    print(f"\n  All events (2018-2024): {len(events_all)}")

    # Split in-sample vs out-of-sample
    events_is  = [e for e in events_all if e["date"] <= IS_END]
    events_oos = [e for e in events_all if e["date"] >= OOS_START]
    print(f"  In-sample (2018-2023): {len(events_is)}")
    print(f"  Out-of-sample (2024):  {len(events_oos)}")

    # Large-cap filter (these are already large-caps but run anyway for consistency)
    print(f"\n[Step 2] Applying large-cap filter...")
    df_is = pd.DataFrame(events_is).rename(columns={"symbol": "ticker"})
    df_is_lc = filter_to_largecap(df_is, min_market_cap_m=500, ticker_col="ticker")
    events_is_lc = df_is_lc.rename(columns={"ticker": "symbol"}).to_dict("records")
    print(f"  After large-cap filter: {len(events_is_lc)} in-sample events")

    if len(events_is_lc) == 0:
        print("  ERROR: No events after filter. Aborting.")
        return

    # ── Backtest: Base signal (in-sample) ────────────────────────────────────
    print(f"\n[Step 3] Measuring event impact (base signal, in-sample)...")
    measure_events = events_to_measure_format(events_is_lc)
    result_base = market_data.measure_event_impact(
        event_dates=measure_events,
        benchmark="SPY",
        entry_price="close",
        estimate_costs=True,
        event_type="vwap_deviation",
    )
    print_results("BASE SIGNAL — No volume filter (In-sample 2018-2023)", result_base)

    # ── Pass 2: Low volume filter ────────────────────────────────────────────
    print(f"\n[Step 4] Scanning with require_low_volume=True...")
    events_lv_all = find_vwap_deviation_events(
        symbols=UNIVERSE,
        start_date=FULL_START,
        end_date=IS_END,
        threshold=1.15,
        cooldown_days=20,
        require_low_volume=True,
        verbose=True,
    )
    print(f"  Low-vol events (2018-2023): {len(events_lv_all)}")

    if len(events_lv_all) >= 5:
        df_lv = pd.DataFrame(events_lv_all).rename(columns={"symbol": "ticker"})
        df_lv_lc = filter_to_largecap(df_lv, min_market_cap_m=500, ticker_col="ticker")
        events_lv_lc = df_lv_lc.rename(columns={"ticker": "symbol"}).to_dict("records")
        print(f"  After large-cap filter: {len(events_lv_lc)} events")

        if len(events_lv_lc) >= 5:
            result_lv = market_data.measure_event_impact(
                event_dates=events_to_measure_format(events_lv_lc),
                benchmark="SPY",
                entry_price="close",
                estimate_costs=True,
                event_type="vwap_deviation",
            )
            print_results("LOW VOLUME filter — Rise on light volume (2018-2023)", result_lv)
        else:
            print("  Too few events after filter, skipping low-vol backtest.")
    else:
        print("  Too few low-vol events, skipping.")

    # ── Pass 3: High volume filter ───────────────────────────────────────────
    print(f"\n[Step 5] Scanning with require_high_volume=True...")
    events_hv_all = find_vwap_deviation_events(
        symbols=UNIVERSE,
        start_date=FULL_START,
        end_date=IS_END,
        threshold=1.15,
        cooldown_days=20,
        require_high_volume=True,
        verbose=True,
    )
    print(f"  High-vol events (2018-2023): {len(events_hv_all)}")

    if len(events_hv_all) >= 5:
        df_hv = pd.DataFrame(events_hv_all).rename(columns={"symbol": "ticker"})
        df_hv_lc = filter_to_largecap(df_hv, min_market_cap_m=500, ticker_col="ticker")
        events_hv_lc = df_hv_lc.rename(columns={"ticker": "symbol"}).to_dict("records")
        print(f"  After large-cap filter: {len(events_hv_lc)} events")

        if len(events_hv_lc) >= 5:
            result_hv = market_data.measure_event_impact(
                event_dates=events_to_measure_format(events_hv_lc),
                benchmark="SPY",
                entry_price="close",
                estimate_costs=True,
                event_type="vwap_deviation",
            )
            print_results("HIGH VOLUME filter — Rise on heavy volume (2018-2023)", result_hv)
        else:
            print("  Too few events after filter, skipping high-vol backtest.")
    else:
        print("  Too few high-vol events, skipping.")

    # ── Out-of-sample (2024) check on base signal ────────────────────────────
    print(f"\n[Step 6] Out-of-sample check (2024, base signal)...")
    df_oos = pd.DataFrame(events_oos).rename(columns={"symbol": "ticker"}) if events_oos else pd.DataFrame()
    if not df_oos.empty:
        df_oos_lc = filter_to_largecap(df_oos, min_market_cap_m=500, ticker_col="ticker")
        events_oos_lc = df_oos_lc.rename(columns={"ticker": "symbol"}).to_dict("records")
        print(f"  OOS events: {len(events_oos_lc)}")
        if len(events_oos_lc) >= 3:
            result_oos = market_data.measure_event_impact(
                event_dates=events_to_measure_format(events_oos_lc),
                benchmark="SPY",
                entry_price="close",
                estimate_costs=True,
                event_type="vwap_deviation",
            )
            print_results("OUT-OF-SAMPLE 2024 — Base signal", result_oos)
        else:
            print("  Too few OOS events for meaningful check.")
    else:
        print("  No OOS events found.")

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    print(f"  Universe: {len(UNIVERSE)} large-cap stocks")
    print(f"  Signal: Close / VWAP_252 >= 1.15 (15% above 1-year cost basis)")
    print(f"  Hypothesis: SHORT — stocks overextended vs VWAP underperform")
    print(f"  In-sample period: {FULL_START} to {IS_END}")
    print(f"  Out-of-sample: {OOS_START} to {FULL_END}")
    print()
    print("  NOTE: For a SHORT signal, we want:")
    print("    - NEGATIVE avg_abnormal returns")
    print("    - LOW positive_rate (< 50%)")
    print("    - p_value < 0.05")
    print("    - passes_multiple_testing = True")
    print()


if __name__ == "__main__":
    main()
