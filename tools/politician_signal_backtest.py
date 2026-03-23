"""
politician_signal_backtest.py

Rigorous backtest of politician trading as a predictive signal.

Tests the TRADEABLE signal: politician discloses purchase of stock X.
Question: Does buying at next market open after disclosure generate abnormal returns?

Segmentation:
- All purchases vs sales
- Chamber (House vs Senate)
- Party
- Trade size
- Reporting gap (fast vs slow disclosure)
"""

import json
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy import stats
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from tools.yfinance_utils import safe_download
from tools.largecap_filter import filter_to_largecap
from tools.politician_trading_scraper import get_historical_trades, summarise_trades

HORIZONS = [1, 3, 5, 10, 20]
BENCHMARK = "SPY"
CUTOFF_DATE = "2025-01-29"   # our data start
DIRECTION_THRESHOLD = 0.5   # percent, per methodology.json


def check_mt(p_values: list) -> bool:
    """Manual multiple testing check matching research.py logic."""
    sig = [p for p in p_values if p < 0.05]
    if len(sig) >= 2:
        return True
    if len(sig) == 1 and min(p_values) < 0.01:
        return True
    return False


def get_prices(tickers: list, start: str, end: str) -> pd.DataFrame:
    """Batch download close prices. Returns DataFrame with ticker as column name."""
    all_t = list(set(tickers + [BENCHMARK]))
    raw = safe_download(all_t, start=start, end=end, auto_adjust=True)
    if raw is None or raw.empty:
        return pd.DataFrame()
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw["Close"]
    else:
        # Flat column names like "Close_NVDA" — extract Close_ columns
        close_cols = [c for c in raw.columns if c.startswith("Close_")]
        if close_cols:
            close = raw[close_cols].copy()
            close.columns = [c.replace("Close_", "") for c in close_cols]
        else:
            # Single ticker case
            close = raw[["Close"]].copy() if "Close" in raw.columns else raw
    return close


def compute_abnormal_returns(events: list, close: pd.DataFrame) -> dict:
    """
    For each event {ticker, date}, compute abnormal returns at each horizon.
    events: list of {'ticker': str, 'date': str, 'metadata': dict}
    Returns: dict of horizon -> list of abnormal returns
    """
    results = {h: [] for h in HORIZONS}

    for ev in events:
        ticker = ev["ticker"]
        date = ev["date"]
        if ticker not in close.columns:
            continue
        s = close[ticker].dropna()
        b = close[BENCHMARK].dropna()
        edt = pd.Timestamp(date)
        fut_s = s.index[s.index >= edt]
        fut_b = b.index[b.index >= edt]
        if len(fut_s) == 0 or len(fut_b) == 0:
            continue
        for h in HORIZONS:
            if len(fut_s) <= h or len(fut_b) <= h:
                continue
            try:
                sr = (s[fut_s[h]] / s[fut_s[0]] - 1) * 100
                br = (b[fut_b[h]] / b[fut_b[0]] - 1) * 100
                ar = sr - br
                if abs(ar) < 100:  # sanity filter
                    results[h].append(ar)
            except Exception:
                continue

    return results


def summarize_results(h_results: dict, label: str) -> dict:
    """Compute t-tests and statistics for each horizon."""
    out = {"label": label, "horizons": {}}
    p_values = []

    for h in HORIZONS:
        ars = np.array(h_results[h])
        n = len(ars)
        if n < 10:
            out["horizons"][h] = {"n": n, "error": "insufficient data"}
            continue
        mean = float(np.mean(ars))
        se = float(stats.sem(ars))
        t_stat, p = stats.ttest_1samp(ars, 0)
        direction = float(np.mean(ars > DIRECTION_THRESHOLD) * 100)
        out["horizons"][h] = {
            "n": n,
            "mean_pct": round(mean, 3),
            "median_pct": round(float(np.median(ars)), 3),
            "se": round(se, 3),
            "t_stat": round(float(t_stat), 3),
            "p_value": round(float(p), 4),
            "direction_pct": round(direction, 1),
            "ci_95_low": round(mean - 1.96 * se, 3),
            "ci_95_high": round(mean + 1.96 * se, 3),
        }
        p_values.append(float(p))

    out["passes_multiple_testing"] = check_mt(p_values)
    out["n_events"] = len(h_results.get(5, []))
    return out


def print_summary(r: dict):
    label = r.get("label", "?")
    n = r.get("n_events", 0)
    mt = "✓ MT PASSES" if r.get("passes_multiple_testing") else "✗ MT fails"
    print(f"\n{'='*65}")
    print(f"  {label}  N={n}  [{mt}]")
    print(f"{'='*65}")
    h_data = r.get("horizons", {})
    for h in HORIZONS:
        if h not in h_data:
            continue
        d = h_data[h]
        if "error" in d:
            print(f"  {h:>3}d: {d['error']}")
            continue
        sig = " **" if d["p_value"] < 0.05 else ("  *" if d["p_value"] < 0.10 else "   ")
        print(f"  {h:>3}d: mean={d['mean_pct']:+.3f}%  p={d['p_value']:.4f}{sig}  "
              f"dir={d['direction_pct']:.0f}%  n={d['n']}")


def run_segment(events: list, close: pd.DataFrame, label: str) -> dict:
    if not events:
        print(f"  [{label}] No events")
        return {"label": label, "n_events": 0}
    print(f"  Running [{label}] N={len(events)}...", end=" ", flush=True)
    h_results = compute_abnormal_returns(events, close)
    r = summarize_results(h_results, label)
    mt_str = "MT✓" if r["passes_multiple_testing"] else "MT✗"
    # Print quick summary
    h5 = r["horizons"].get(5, {})
    if "mean_pct" in h5:
        print(f"5d: {h5['mean_pct']:+.3f}% p={h5['p_value']:.3f} [{mt_str}]")
    else:
        print("insufficient data")
    return r


def main():
    print("=== Politician Trading Signal Backtest ===\n")

    # Load trades
    cache_file = os.path.join(os.path.dirname(__file__), "politician_trades_cache.json")
    if not os.path.exists(cache_file):
        print("ERROR: No cache. Run get_historical_trades() first.")
        return {}

    with open(cache_file) as f:
        cache = json.load(f)
    trades = cache.get("trades", [])
    print(f"Loaded {len(trades)} trades from cache")
    s = summarise_trades(trades)
    print(f"Date range: {s['date_range']}")
    print(f"Purchases: {s['purchases']}, Sales: {s['sales']}")

    # Filter to purchases with required fields
    purchases = [t for t in trades
                 if t["transaction_type"] == "Purchase"
                 and t.get("ticker")
                 and t.get("transaction_date")
                 and t.get("disclosure_date")
                 and t["disclosure_date"] >= CUTOFF_DATE]
    print(f"\nPurchases with disclosure dates: {len(purchases)}")

    # Large-cap filter
    unique_tickers = list(set(t["ticker"] for t in purchases))
    df = pd.DataFrame({"ticker": unique_tickers})
    df_lc = filter_to_largecap(df, min_market_cap_m=1000, verbose=False)
    largecap = set(df_lc["ticker"].tolist())
    purchases = [t for t in purchases if t["ticker"] in largecap]
    print(f"After large-cap filter (>1B): {len(purchases)}")

    # Note selection bias
    print("\n[BIAS NOTE] Selection bias: Capitol Trades scrapes STOCK Act disclosures.")
    print("  Survivorship: large-cap only. Reporting: may be incomplete pre-2022.")

    # Get price data for all tickers
    all_tickers = list(set(t["ticker"] for t in purchases))
    all_dates = [t["disclosure_date"] for t in purchases]
    start = min(all_dates)
    end_dt = datetime.strptime(max(all_dates), "%Y-%m-%d") + timedelta(days=45)
    end = end_dt.strftime("%Y-%m-%d")
    print(f"\nDownloading prices: {len(all_tickers)} tickers from {start} to {end}...")
    close = get_prices(all_tickers, start, end)
    print(f"Price data: {close.shape[0]} days x {close.shape[1]} tickers")

    # ---- DISCLOSURE DATE (TRADEABLE SIGNAL) ----
    events_disc = [{"ticker": t["ticker"], "date": t["disclosure_date"], "metadata": t}
                   for t in purchases]
    all_results = {}

    print("\n--- RUNNING BACKTESTS ---")
    r = run_segment(events_disc, close, "all_purchases_disclosure")
    print_summary(r)
    all_results["all_purchases_disclosure"] = r

    # ---- TRANSACTION DATE (informational, not tradeable) ----
    events_tx = [{"ticker": t["ticker"], "date": t["transaction_date"], "metadata": t}
                 for t in purchases]
    r_tx = run_segment(events_tx, close, "all_purchases_txdate")
    print_summary(r_tx)
    all_results["all_purchases_txdate"] = r_tx

    # ---- SALES (opposite direction) ----
    sales = [t for t in trades
             if t["transaction_type"] in ("Sale", "Sale (Full)", "Sale (Partial)")
             and t.get("ticker") and t.get("disclosure_date")
             and t["ticker"] in largecap
             and t["disclosure_date"] >= CUTOFF_DATE]
    if sales:
        events_sales = [{"ticker": t["ticker"], "date": t["disclosure_date"], "metadata": t}
                        for t in sales]
        # For sales, we expect negative abnormal returns (or positive if politicians sell before drop)
        r_sales = run_segment(events_sales, close, "all_sales_disclosure")
        print_summary(r_sales)
        all_results["all_sales_disclosure"] = r_sales

    # ---- CHAMBER SPLITS ----
    for chamber in ["House", "Senate"]:
        subset = [e for e in events_disc if e["metadata"].get("chamber") == chamber]
        r = run_segment(subset, close, f"purchases_{chamber}")
        all_results[f"purchases_{chamber}"] = r

    # ---- PARTY SPLITS ----
    for party in ["Republican", "Democrat"]:
        subset = [e for e in events_disc if e["metadata"].get("party") == party]
        r = run_segment(subset, close, f"purchases_{party}")
        all_results[f"purchases_{party}"] = r

    # ---- TRADE SIZE ----
    large_trades = [e for e in events_disc
                    if (e["metadata"].get("amount_min") or 0) >= 50_000]
    r = run_segment(large_trades, close, "purchases_large_50k_plus")
    all_results["purchases_large_50k_plus"] = r

    very_large = [e for e in events_disc
                  if (e["metadata"].get("amount_min") or 0) >= 250_000]
    r = run_segment(very_large, close, "purchases_very_large_250k_plus")
    all_results["purchases_very_large_250k_plus"] = r

    # ---- REPORTING GAP (fast reporters = fresher info) ----
    fast = [e for e in events_disc
            if (e["metadata"].get("reporting_gap_days") or 999) <= 10]
    r = run_segment(fast, close, "purchases_fast_report_10d")
    all_results["purchases_fast_report_10d"] = r

    slow = [e for e in events_disc
            if (e["metadata"].get("reporting_gap_days") or 0) >= 30]
    r = run_segment(slow, close, "purchases_slow_report_30d_plus")
    all_results["purchases_slow_report_30d_plus"] = r

    # ---- TOP POLITICIANS (most active traders) ----
    from collections import Counter
    pol_counts = Counter(e["metadata"]["politician"] for e in events_disc)
    top_pols = [p for p, c in pol_counts.most_common(5)]
    for pol in top_pols[:3]:
        subset = [e for e in events_disc if e["metadata"]["politician"] == pol]
        r = run_segment(subset, close, f"pol_{pol.replace(' ', '_')}")
        all_results[f"pol_{pol}"] = r

    # ---- PRINT FULL RESULTS ----
    print("\n\n=== FULL RESULTS SUMMARY ===")
    for key, r in all_results.items():
        print_summary(r)

    # Save
    out_file = os.path.join(os.path.dirname(__file__), "politician_backtest_results.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nResults saved to {out_file}")

    # Print MT passing signals
    print("\n=== MT-PASSING SIGNALS ===")
    for key, r in all_results.items():
        if r.get("passes_multiple_testing"):
            h5 = r.get("horizons", {}).get(5, {})
            if "mean_pct" in h5:
                print(f"  ✓ {key}: 5d={h5['mean_pct']:+.3f}% p={h5['p_value']:.4f} n={h5['n']}")

    return all_results


if __name__ == "__main__":
    main()
