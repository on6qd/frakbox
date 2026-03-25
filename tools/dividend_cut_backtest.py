"""
dividend_cut_backtest.py

Backtest the "dividend cut short" signal for large-cap S&P 500 stocks (2015-2024).

Hypothesis:
    When a large-cap S&P 500 company announces a dividend cut (>25% reduction)
    or suspension, the stock continues to underperform for 5-10 days after
    announcement. Short at the open the day after announcement.

Methodology:
    1. Scan yfinance dividend history for S&P 500 large-caps (>$2B market cap)
    2. Identify cuts >25% or suspensions (dividend drops to 0)
    3. Measure 1d, 3d, 5d, 10d abnormal returns (vs SPY benchmark)
    4. Apply multiple testing correction
    5. OOS split: discovery 2015-2021, validation 2022-2024
    6. Record result in knowledge base

Usage:
    python3 tools/dividend_cut_backtest.py
"""

from __future__ import annotations

import sys
import os
import warnings
warnings.filterwarnings("ignore")

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats

import db
import market_data
from tools.yfinance_utils import safe_download, get_close_prices

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MIN_MARKET_CAP = 2e9          # $2B minimum
CUT_THRESHOLD = 0.25          # >25% reduction counts as a cut
START_DATE = "2015-01-01"
END_DATE   = "2024-12-31"
DISCOVERY_END = "2021-12-31"  # OOS split point
VALIDATION_START = "2022-01-01"

# Large-cap S&P 500 components — representative universe (stable large-caps
# that have existed throughout the period, avoiding survivorship bias by
# including companies that experienced distress/dividend cuts)
# Note: This includes both current and historically significant S&P 500 members
SP500_UNIVERSE = [
    # Financials
    "JPM", "BAC", "WFC", "C", "GS", "MS", "AXP", "USB", "PNC", "TFC",
    "COF", "BK", "STT", "FITB", "KEY", "CFG", "HBAN", "RF", "MTB", "ZION",
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "HAL",
    "BKR", "DVN", "FANG", "APA", "MRO", "HES", "KMI", "WMB", "OKE", "LNG",
    # Industrials
    "BA", "GE", "MMM", "HON", "UPS", "CAT", "DE", "RTX", "LMT", "NOC",
    "GD", "EMR", "ETN", "ITW", "ROK", "PH", "DOV", "FTV", "XYL", "TT",
    # Consumer Discretionary
    "AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "TGT", "LOW", "TJX", "BKNG",
    "GM", "F", "EBAY", "YUM", "DG", "DLTR", "RCL", "CCL", "MAR", "HLT",
    # Consumer Staples
    "PG", "KO", "PEP", "WMT", "COST", "PM", "MO", "CL", "KHC", "GIS",
    "K", "CPB", "MKC", "HRL", "SJM", "CAG", "TSN", "HSY", "MDLZ", "STZ",
    # Health Care
    "JNJ", "UNH", "PFE", "ABBV", "MRK", "ABT", "TMO", "DHR", "BMY", "LLY",
    "AMGN", "GILD", "CVS", "CI", "HUM", "MDT", "SYK", "BSX", "EW", "BAX",
    # Technology
    "AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CSCO", "IBM", "TXN", "QCOM", "INTC",
    "AMD", "MU", "HPQ", "HPE", "JNPR", "MSI", "CTSH", "CDW", "KEYS", "AMAT",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "XEL", "SRE", "PEG", "ED",
    "ETR", "FE", "PPL", "NI", "CNP", "AES", "CMS", "LNT", "WEC", "AWK",
    # REITs
    "AMT", "PLD", "CCI", "EQIX", "SPG", "O", "WY", "PSA", "EQR", "AVB",
    # Materials / Telecom / Misc
    "LIN", "APD", "ECL", "DD", "DOW", "NEM", "FCX", "T", "VZ", "TMUS",
    "DIS", "CMCSA", "NFLX", "PARA", "WBD", "OMC", "IPG", "FLS", "AIZ", "AIG",
    # Airlines / Hospitality (high-cut probability)
    "DAL", "UAL", "AAL", "LUV", "ALK", "NCLH",
    # Retail / Other
    "M", "KSS", "JWN", "L", "CL", "NWL", "LEG", "WHR", "MAS", "FHN",
]

SP500_UNIVERSE = list(set(SP500_UNIVERSE))  # deduplicate


# ---------------------------------------------------------------------------
# Step 1: Find dividend cuts
# ---------------------------------------------------------------------------

def get_dividend_cuts(tickers: list[str]) -> list[dict]:
    """
    For each ticker, fetch dividend history and identify cuts >25% or suspensions.
    Returns list of events with keys: symbol, date, old_div, new_div, cut_pct, suspended.
    """
    print(f"Scanning {len(tickers)} tickers for dividend cuts ({START_DATE} to {END_DATE})...")
    events = []
    errors = []

    for i, ticker in enumerate(tickers):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(tickers)} tickers scanned, {len(events)} cuts found...")

        try:
            t = yf.Ticker(ticker)
            divs = t.dividends

            if divs is None or len(divs) < 2:
                continue

            # Normalize index to UTC-naive dates
            divs.index = pd.to_datetime(divs.index).tz_localize(None)

            # Filter to our date range
            divs = divs[(divs.index >= START_DATE) & (divs.index <= END_DATE)]

            if len(divs) < 2:
                continue

            # Sort chronologically
            divs = divs.sort_index()

            # Look for cuts: compare each dividend to the prior one
            for j in range(1, len(divs)):
                prev_div = divs.iloc[j - 1]
                curr_div = divs.iloc[j]
                curr_date = divs.index[j]

                if prev_div <= 0:
                    continue

                # Suspension: dividend drops to 0
                if curr_div == 0:
                    events.append({
                        "symbol": ticker,
                        "date": curr_date.strftime("%Y-%m-%d"),
                        "old_div": float(prev_div),
                        "new_div": float(curr_div),
                        "cut_pct": 1.0,
                        "suspended": True,
                    })

                # Cut: >25% reduction
                elif prev_div > 0 and curr_div < prev_div:
                    cut_pct = (prev_div - curr_div) / prev_div
                    if cut_pct >= CUT_THRESHOLD:
                        events.append({
                            "symbol": ticker,
                            "date": curr_date.strftime("%Y-%m-%d"),
                            "old_div": float(prev_div),
                            "new_div": float(curr_div),
                            "cut_pct": float(cut_pct),
                            "suspended": False,
                        })

        except Exception as e:
            errors.append((ticker, str(e)))

    if errors:
        print(f"  Errors on {len(errors)} tickers: {errors[:5]}")

    print(f"  Found {len(events)} raw dividend cut events")
    return events


# ---------------------------------------------------------------------------
# Step 2: Filter to large-cap, deduplicate, verify tradeable
# ---------------------------------------------------------------------------

def filter_events(events: list[dict]) -> list[dict]:
    """
    Filter to large-cap stocks and deduplicate.
    Also verify market cap at time of event (rough: use current cap as proxy,
    noting this is a limitation — survivorship bias toward surviving companies).
    """
    print("Filtering events to large-cap (>$2B) stocks...")

    # Get current market caps
    cap_cache = {}
    filtered = []

    for ev in events:
        ticker = ev["symbol"]

        if ticker not in cap_cache:
            try:
                info = yf.Ticker(ticker).fast_info
                cap = getattr(info, "market_cap", None)
                if cap is None:
                    # Try slower info
                    full_info = yf.Ticker(ticker).info
                    cap = full_info.get("marketCap", 0)
                cap_cache[ticker] = cap if cap else 0
            except Exception:
                cap_cache[ticker] = 0

        if cap_cache[ticker] >= MIN_MARKET_CAP:
            filtered.append(ev)

    # Deduplicate: same ticker within 30 days (dividend restated or error)
    filtered.sort(key=lambda x: (x["symbol"], x["date"]))
    deduped = []
    prev = None
    for ev in filtered:
        if prev is None:
            deduped.append(ev)
            prev = ev
            continue
        same_ticker = ev["symbol"] == prev["symbol"]
        close_dates = abs((pd.Timestamp(ev["date"]) - pd.Timestamp(prev["date"])).days) < 30
        if same_ticker and close_dates:
            # Keep the larger cut
            if ev["cut_pct"] > prev["cut_pct"]:
                deduped[-1] = ev
                prev = ev
        else:
            deduped.append(ev)
            prev = ev

    print(f"  After large-cap filter + dedup: {len(deduped)} events")
    return deduped


# ---------------------------------------------------------------------------
# Step 3: Measure abnormal returns
# ---------------------------------------------------------------------------

def measure_abnormal_returns(events: list[dict]) -> dict:
    """
    Use market_data.measure_event_impact to compute abnormal returns.
    Entry: open of the day AFTER the dividend cut ex-date.
    """
    print(f"\nMeasuring abnormal returns for {len(events)} events...")

    # Format events for measure_event_impact
    # The dividend cut is announced on or before the ex-date.
    # We enter at the open the next trading day after the ex-date.
    event_list = []
    for ev in events:
        event_list.append({
            "symbol": ev["symbol"],
            "date": ev["date"],
            "timing": "after_hours",   # treat as after-hours so entry = next open
            "entry_price": "open",
            "cut_pct": ev["cut_pct"],
            "suspended": ev["suspended"],
        })

    result = market_data.measure_event_impact(
        event_dates=event_list,
        benchmark="SPY",
        entry_price="open",
        estimate_costs=True,
        event_type="dividend_cut_short",
    )

    return result


# ---------------------------------------------------------------------------
# Step 4: OOS Split Analysis
# ---------------------------------------------------------------------------

def split_oos(events: list[dict], result: dict) -> dict:
    """
    Split events into discovery (pre-2022) and validation (2022+) periods.
    """
    indiv = result.get("individual_impacts", [])

    discovery_events = []
    validation_events = []

    # Map events by symbol+date for lookup
    for imp in indiv:
        ev_date = imp.get("event_date", "")
        if not ev_date:
            continue
        if ev_date <= DISCOVERY_END:
            discovery_events.append(imp)
        else:
            validation_events.append(imp)

    def summarize(evs, label):
        if not evs:
            return {label: "no events"}
        n = len(evs)
        for horizon in ["1d", "3d", "5d", "10d"]:
            key = f"abnormal_{horizon}"
            # individual_impacts values are in percentage points (e.g. -1.5 = -1.5%)
            vals = [e.get(key, np.nan) for e in evs]
            vals = [v for v in vals if v is not None and not np.isnan(float(v))]
            vals = [float(v) for v in vals]
            if vals:
                avg_pct = np.mean(vals)         # in pct points
                # For short: negative pct_pts are wins; threshold -0.5 pct pts (=-0.5%)
                neg_rate = sum(1 for v in vals if v < -0.5) / len(vals)
                print(f"  {label} n={n} | {horizon}: avg={avg_pct:.2f}% neg_rate={neg_rate:.1%}")

        return {
            "n": n,
            "label": label,
            "impacts": evs,
        }

    disc = summarize(discovery_events, "DISCOVERY")
    val = summarize(validation_events, "VALIDATION")

    return {"discovery": disc, "validation": val}


# ---------------------------------------------------------------------------
# Step 5: Power analysis
# ---------------------------------------------------------------------------

def check_power(n: int) -> bool:
    """Simple power check: n >= 20 required."""
    return n >= 20


# ---------------------------------------------------------------------------
# Main backtest
# ---------------------------------------------------------------------------

def run_backtest():
    print("=" * 70)
    print("DIVIDEND CUT SHORT SIGNAL BACKTEST")
    print(f"Universe: {len(SP500_UNIVERSE)} S&P 500 large-caps | {START_DATE} to {END_DATE}")
    print("=" * 70)

    # Step 1: Find cuts
    raw_events = get_dividend_cuts(SP500_UNIVERSE)

    if not raw_events:
        print("ERROR: No dividend cuts found. Aborting.")
        return

    # Step 2: Filter
    events = filter_events(raw_events)

    if len(events) < 20:
        print(f"INSUFFICIENT SAMPLE: only {len(events)} events (need >=20). Recording dead end.")
        db.init_db()
        db.record_dead_end(
            "dividend_cut_short",
            f"Insufficient events after filtering to large-cap S&P500: n={len(events)} (need >=20). "
            f"Universe of {len(SP500_UNIVERSE)} tickers yielded only {len(raw_events)} raw cuts, "
            f"{len(events)} after large-cap filter."
        )
        return

    # Print event sample
    print(f"\nSample events:")
    for ev in events[:10]:
        print(f"  {ev['symbol']} {ev['date']}: cut {ev['cut_pct']:.1%} {'(SUSPENDED)' if ev['suspended'] else ''}")

    # Step 3: Measure impact
    result = measure_abnormal_returns(events)

    n_measured = result.get("events_measured", 0)
    print(f"\nEvents measured: {n_measured}")

    if n_measured < 20:
        print(f"INSUFFICIENT MEASURED EVENTS: {n_measured}. Recording dead end.")
        db.init_db()
        db.record_dead_end(
            "dividend_cut_short",
            f"Only {n_measured} events had price data available (need >=20)."
        )
        return

    # Collect key metrics
    # NOTE: measure_event_impact returns values in percentage points (e.g. 1.5 = 1.5%)
    # and positive_rate in 0-100 scale (e.g. 60.0 = 60%)
    metrics = {}
    for horizon in ["1d", "3d", "5d", "10d"]:
        avg_key = f"avg_abnormal_{horizon}"
        pos_key = f"positive_rate_abnormal_{horizon}"
        p_key = f"p_value_abnormal_{horizon}"           # correct key name
        stdev_key = f"stdev_abnormal_{horizon}"
        boot_key = f"bootstrap_ci_abnormal_{horizon}"

        avg_pct_pts = result.get(avg_key, np.nan)
        avg_decimal = avg_pct_pts / 100.0 if not np.isnan(avg_pct_pts) else np.nan

        pos_rate_0_100 = result.get(pos_key, 50.0)     # 0-100 scale
        pos_rate_decimal = pos_rate_0_100 / 100.0
        dir_rate_short = 1.0 - pos_rate_decimal        # for short: negative returns win

        metrics[horizon] = {
            "avg_abnormal": avg_decimal,               # in decimal (e.g. -0.015 = -1.5%)
            "avg_abnormal_pct_pts": avg_pct_pts,       # in pct pts (e.g. -1.5)
            "direction_rate_short": dir_rate_short,    # fraction (0-1), short wins = negative return
            "p_value": result.get(p_key, np.nan),
            "stdev": result.get(stdev_key, np.nan),
            "bootstrap_ci": result.get(boot_key, {}),
        }

    print("\n--- ABNORMAL RETURN RESULTS ---")
    print(f"{'Horizon':<10} {'Avg Abn Return':>15} {'Dir% (short)':>14} {'p-value':>10}")
    print("-" * 52)
    for h, m in metrics.items():
        avg = m["avg_abnormal"]
        dir_pct = m["direction_rate_short"]
        p = m["p_value"]
        p_str = f"{p:.4f}" if not np.isnan(p) else "nan"
        print(f"{h:<10} {avg:>+15.3%} {dir_pct:>13.1%} {p_str:>10}")

    # Multiple testing correction
    passes_mt = result.get("passes_multiple_testing", False)
    print(f"\nPasses multiple testing correction: {passes_mt}")
    print(f"Data quality warning: {result.get('data_quality_warning', 'None')}")

    # Estimated costs — API returns in pct points (e.g. 0.437 = 0.437%)
    avg_cost = result.get("avg_estimated_cost_pct", 0)  # in pct points
    print(f"Estimated round-trip cost: {avg_cost:.3f}%")

    # OOS split
    print("\n--- OUT-OF-SAMPLE ANALYSIS ---")
    oos = split_oos(events, result)

    disc_n = oos["discovery"].get("n", 0)
    val_n = oos["validation"].get("n", 0)
    print(f"Discovery period (2015-2021): n={disc_n}")
    print(f"Validation period (2022-2024): n={val_n}")

    # Validation-period abnormal returns (for the signal to pass, we need
    # >=3 validation events that confirm the direction)
    val_impacts = oos["validation"].get("impacts", [])
    # individual_impacts abnormal values are in percentage points (e.g. -1.5 = -1.5%)
    val_abnormal_5d_raw = [v.get("abnormal_5d", np.nan) for v in val_impacts]
    val_abnormal_5d = [float(v) for v in val_abnormal_5d_raw if v is not None and not np.isnan(float(v))]
    # For short: negative pct_pts wins, threshold -0.5 pct pts
    val_neg_rate = sum(1 for v in val_abnormal_5d if v < -0.5) / len(val_abnormal_5d) if val_abnormal_5d else 0
    val_avg_5d_pct = np.mean(val_abnormal_5d) if val_abnormal_5d else np.nan
    val_avg_5d = val_avg_5d_pct / 100.0 if not np.isnan(val_avg_5d_pct) else np.nan
    print(f"Validation 5d: avg={val_avg_5d_pct:.2f}%, neg_rate={val_neg_rate:.1%}, n={len(val_abnormal_5d)}")

    # ---------------------------------------------------------------------------
    # Signal pass/fail decision
    # ---------------------------------------------------------------------------
    print("\n--- SIGNAL EVALUATION ---")

    # Find best horizon (most negative avg abnormal return, in decimal form)
    best_h = min(metrics.keys(), key=lambda h: metrics[h]["avg_abnormal"])
    best_avg = metrics[best_h]["avg_abnormal"]           # decimal (e.g. -0.015)
    best_avg_pct = metrics[best_h]["avg_abnormal_pct_pts"]  # pct pts (e.g. -1.5)
    best_p = metrics[best_h]["p_value"]
    best_dir = metrics[best_h]["direction_rate_short"]

    best_p_str = f"{best_p:.4f}" if not np.isnan(best_p) else "nan"
    print(f"Best horizon: {best_h} | avg={best_avg:.3%} | p={best_p_str} | dir%={best_dir:.1%}")

    # avg_cost is in pct points from the API; convert to decimal for comparison
    avg_cost_decimal = avg_cost / 100.0

    # Check all criteria
    # best_avg is in decimal for threshold comparison
    checks = {
        "n>=20": n_measured >= 20,
        "passes_multiple_testing": passes_mt,
        "direction>50pct": best_dir > 0.50,
        "abnormal_return>0.5pct": abs(best_avg) > 0.005,    # >0.5% in decimal
        "return_after_costs>0": abs(best_avg) - avg_cost_decimal > 0,
        "oos_n>=3": len(val_abnormal_5d) >= 3,
        "oos_direction>50pct": val_neg_rate > 0.50 if val_abnormal_5d else False,
    }

    print("\nChecklist:")
    for check, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {check}")

    all_pass = all(checks.values())
    print(f"\nOverall signal assessment: {'PASS - record as known effect' if all_pass else 'FAIL - record as dead end'}")

    # ---------------------------------------------------------------------------
    # Record result
    # ---------------------------------------------------------------------------
    db.init_db()

    if all_pass:
        # Compute direction consistency across ALL horizons
        all_dir_pcts = [m["direction_rate_short"] for m in metrics.values()]
        avg_consistency = np.mean(all_dir_pcts) * 100

        effect = {
            "signal": "dividend_cut_short",
            "description": "Short large-cap S&P500 stocks after >25% dividend cut or suspension",
            "n_events": n_measured,
            "discovery_n": disc_n,
            "validation_n": val_n,
            "best_horizon": best_h,
            "avg_abnormal_returns": {h: f"{m['avg_abnormal']:.4f}" for h, m in metrics.items()},
            "direction_rates_short": {h: f"{m['direction_rate_short']:.3f}" for h, m in metrics.items()},
            "p_values": {h: f"{m['p_value']:.4f}" for h, m in metrics.items()},
            "passes_multiple_testing": passes_mt,
            "estimated_cost_pct": f"{avg_cost_decimal:.4f}",
            "oos_avg_5d": f"{val_avg_5d:.4f}",
            "oos_neg_rate": f"{val_neg_rate:.3f}",
            "entry": "next open after ex-date",
            "universe": "S&P500 large-cap >$2B market cap",
            "date_range": f"{START_DATE} to {END_DATE}",
            "causal_mechanism": (
                "Dividend cuts signal management's private information about deteriorating "
                "cash flows. Income-focused institutional investors (pension funds, dividend "
                "ETFs) are forced to sell due to mandate changes. Both selling pressure and "
                "fundamental re-rating persist for 5-10 days. Michaely et al. (1995) documented "
                "significant negative price drift post-cut."
            ),
            "survivorship_bias": (
                "Universe includes surviving companies at time of backtest. Companies that "
                "went bankrupt post-cut are excluded. This likely UNDERSTATES the true effect "
                "as the worst cuts may be absent."
            ),
            "selection_bias": (
                "Only S&P500 large-caps included. Smaller companies with more severe cuts "
                "are excluded. Effect may differ for mid/small-caps."
            ),
        }
        db.record_known_effect("dividend_cut_short", effect)
        print(f"\nRecorded as KNOWN EFFECT: dividend_cut_short")

    else:
        failed_checks = [k for k, v in checks.items() if not v]
        p_str = f"{best_p:.4f}" if not np.isnan(best_p) else "nan"
        reason = (
            f"Signal failed {len(failed_checks)} check(s): {', '.join(failed_checks)}. "
            f"n={n_measured}, best_horizon={best_h}, avg_abnormal={best_avg:.3%} ({best_avg_pct:.2f} pct pts), "
            f"p={p_str}, direction%={best_dir:.1%}, "
            f"passes_mt={passes_mt}, oos_n={len(val_abnormal_5d)}, oos_neg_rate={val_neg_rate:.1%}. "
            f"Universe: {len(SP500_UNIVERSE)} S&P500 large-caps >$2B, {START_DATE}-{END_DATE}."
        )
        db.record_dead_end("dividend_cut_short", reason)
        print(f"\nRecorded as DEAD END: dividend_cut_short")
        print(f"Reason: {reason}")

    # Print full summary
    print("\n" + "=" * 70)
    print("BACKTEST SUMMARY")
    print("=" * 70)
    print(f"Signal:          Dividend Cut Short (>25% cut or suspension)")
    print(f"Universe:        S&P 500 large-caps >$2B")
    print(f"Period:          {START_DATE} - {END_DATE}")
    print(f"Events found:    {n_measured}")
    print(f"OOS split:       {disc_n} discovery / {val_n} validation")
    print(f"Entry:           Open next day after ex-date")
    print()
    print(f"{'Horizon':<10} {'Avg Abn Return':>15} {'Dir% (short)':>14} {'p-value':>10}")
    print("-" * 52)
    for h, m in metrics.items():
        p_s = f"{m['p_value']:.4f}" if not np.isnan(m['p_value']) else "nan"
        print(f"{h:<10} {m['avg_abnormal']:>+15.3%} {m['direction_rate_short']:>13.1%} {p_s:>10}")
    print()
    print(f"Passes multiple testing: {passes_mt}")
    print(f"Round-trip cost:         {avg_cost_decimal:.3%}")
    val_avg_5d_pct_str = f"{val_avg_5d_pct:.2f}%" if not np.isnan(val_avg_5d_pct) else "nan"
    print(f"OOS validation (5d):     avg={val_avg_5d_pct_str}, neg_rate={val_neg_rate:.1%}, n={len(val_abnormal_5d)}")
    print(f"Result:                  {'PASS' if all_pass else 'FAIL'}")
    print("=" * 70)

    return {
        "n_events": n_measured,
        "events": events,
        "metrics": metrics,
        "passes": all_pass,
        "checks": checks,
        "oos": oos,
        "result": result,
    }


if __name__ == "__main__":
    run_backtest()
