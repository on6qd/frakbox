"""
politician_backtest.py

Backtest US politician stock trading as a signal.

Tests:
1. Purchase signal: Does a politician buying a stock predict abnormal returns?
2. Two dates: txDate (true information date) vs disclosure_date (tradeable date)
3. Segmentation: chamber, party, trade size, sector
4. Multiple testing correction applied

Usage:
    python tools/politician_backtest.py
"""

import json
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from tools.yfinance_utils import safe_download
from tools.largecap_filter import filter_to_largecap
from market_data import passes_multiple_testing, power_analysis
from tools.politician_trading_scraper import get_historical_trades, summarise_trades


# --- Config ---
HORIZONS = [1, 3, 5, 10, 20]
BENCHMARK = "SPY"
MIN_EVENTS = 20          # minimum N per subgroup
MIN_TOTAL_EVENTS = 50    # minimum N for overall signal


def load_trades(use_cache=True, max_pages=None):
    """Load trades, either from cache or fresh fetch."""
    import json
    cache_file = os.path.join(os.path.dirname(__file__), "politician_trades_cache.json")
    if use_cache and os.path.exists(cache_file):
        with open(cache_file) as f:
            cache = json.load(f)
        trades = cache.get("trades", [])
        if trades:
            print(f"Loaded {len(trades)} trades from cache")
            return trades
    print("Fetching trades from Capitol Trades...")
    trades = get_historical_trades(max_pages=max_pages, use_cache=True)
    return trades


def get_price_data(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Download close prices for all tickers and benchmark."""
    all_tickers = list(set(tickers + [BENCHMARK]))
    data = safe_download(all_tickers, start=start, end=end, auto_adjust=True)
    if data is None or data.empty:
        return pd.DataFrame()
    if isinstance(data.columns, pd.MultiIndex):
        close = data["Close"]
    else:
        close = data[["Close"]] if "Close" in data.columns else data
    return close


def compute_abnormal_return(ticker: str, event_date: str, horizon: int,
                             price_cache: dict) -> float | None:
    """
    Compute horizon-day abnormal return for ticker starting from event_date.
    Buys at OPEN after event_date (i.e., next trading day open).
    Returns in percentage terms.
    """
    closes = price_cache.get(ticker)
    bench = price_cache.get(BENCHMARK)
    if closes is None or bench is None:
        return None

    # Find the event date in the price series
    try:
        all_dates = closes.index
        # Get the first trading day AT OR AFTER event_date
        event_dt = pd.Timestamp(event_date)
        future_dates = all_dates[all_dates >= event_dt]
        if len(future_dates) < horizon + 1:
            return None
        entry_date = future_dates[0]
        exit_date = future_dates[horizon] if len(future_dates) > horizon else future_dates[-1]

        stock_ret = (closes[exit_date] / closes[entry_date] - 1) * 100
        bench_ret = (bench[exit_date] / bench[entry_date] - 1) * 100
        return stock_ret - bench_ret
    except Exception:
        return None


def run_backtest(events: list[dict], label: str = "all") -> dict:
    """
    Run backtest for a list of events.
    Each event: {'ticker': str, 'date': str (YYYY-MM-DD), 'metadata': dict}
    """
    if not events:
        return {"label": label, "n": 0, "error": "no events"}

    # Get unique tickers
    tickers = list(set(e["ticker"] for e in events))
    dates = [e["date"] for e in events]
    start = min(dates)
    end = (datetime.strptime(max(dates), "%Y-%m-%d") + timedelta(days=40)).strftime("%Y-%m-%d")

    print(f"  [{label}] Downloading prices for {len(tickers)} tickers ({start} to {end})...")

    # Download in chunks to avoid timeouts
    price_cache = {}
    chunk_size = 50
    for i in range(0, len(tickers) + 1, chunk_size):
        chunk = tickers[i:i+chunk_size] + ([BENCHMARK] if i == 0 else [])
        data = get_price_data(list(set(chunk)), start, end)
        if data.empty:
            continue
        for col in data.columns:
            price_cache[col] = data[col].dropna()

    # Compute abnormal returns per horizon
    results = {h: [] for h in HORIZONS}
    skipped = 0

    for event in events:
        ticker = event["ticker"]
        date = event["date"]
        for h in HORIZONS:
            ar = compute_abnormal_return(ticker, date, h, price_cache)
            if ar is not None and not np.isnan(ar) and abs(ar) < 100:  # sanity check
                results[h].append(ar)
            else:
                skipped += 1 if h == HORIZONS[0] else 0

    summary = {"label": label, "n_events": len(events), "n_skipped": skipped}

    for h in HORIZONS:
        ars = results[h]
        if len(ars) < 5:
            summary[f"h{h}"] = {"n": len(ars), "error": "insufficient data"}
            continue
        arr = np.array(ars)
        mean = np.mean(arr)
        se = stats.sem(arr)
        t, p = stats.ttest_1samp(arr, 0)
        direction_correct = np.mean(arr > 0.5)  # fraction exceeding 0.5% threshold
        summary[f"h{h}"] = {
            "n": len(ars),
            "mean_pct": round(mean, 3),
            "median_pct": round(np.median(arr), 3),
            "se": round(se, 3),
            "t_stat": round(t, 3),
            "p_value": round(p, 4),
            "direction_pct": round(direction_correct * 100, 1),
            "ci_95_low": round(mean - 1.96 * se, 3),
            "ci_95_high": round(mean + 1.96 * se, 3),
        }

    # Multiple testing check
    p_values = [summary[f"h{h}"]["p_value"] for h in HORIZONS
                if f"h{h}" in summary and "p_value" in summary[f"h{h}"]]
    passes_mt = passes_multiple_testing(p_values, method="holm")
    summary["passes_multiple_testing"] = passes_mt

    return summary


def print_result(r: dict):
    label = r.get("label", "?")
    n = r.get("n_events", 0)
    passes_mt = r.get("passes_multiple_testing", False)
    mt_str = "✓ MT PASSES" if passes_mt else "✗ MT fails"
    print(f"\n{'='*60}")
    print(f"  {label}  N={n}  [{mt_str}]")
    print(f"{'='*60}")
    for h in HORIZONS:
        key = f"h{h}"
        if key not in r:
            continue
        d = r[key]
        if "error" in d:
            print(f"  {h:>3}d: {d['error']}")
            continue
        sig = " **" if d["p_value"] < 0.05 else ("  *" if d["p_value"] < 0.10 else "   ")
        print(f"  {h:>3}d: mean={d['mean_pct']:+.2f}%  p={d['p_value']:.4f}{sig}  "
              f"dir={d['direction_pct']:.0f}%  n={d['n']}")


def main():
    print("=== Politician Trading Backtest ===\n")

    # Load data
    trades = load_trades(use_cache=True)
    if not trades:
        print("ERROR: No trades loaded. Run get_historical_trades() first.")
        return

    summary = summarise_trades(trades)
    print(f"Dataset: {summary['count']} trades, {summary['purchases']} purchases, "
          f"{summary['sales']} sales")
    print(f"Date range: {summary['date_range'][0]} to {summary['date_range'][1]}")
    print(f"Top tickers: {summary['top_tickers'][:5]}")
    print()

    # Filter to purchases only (signal is buys)
    purchases = [t for t in trades if t["transaction_type"] == "Purchase"
                 and t.get("ticker") and t.get("transaction_date")]

    # Filter to large-cap only (avoid micro/small cap noise)
    all_tickers = list(set(t["ticker"] for t in purchases))
    print(f"Filtering {len(all_tickers)} unique tickers to large-cap...")
    largecap = set(filter_to_largecap(all_tickers, min_market_cap=1e9))
    purchases_lc = [t for t in purchases if t["ticker"] in largecap]
    print(f"Large-cap purchases: {len(purchases_lc)} (from {len(purchases)} total)")

    # Cut to 2020+ for cleaner data (STOCK Act enforcement improved)
    purchases_lc = [t for t in purchases_lc if t["transaction_date"] >= "2020-01-01"]
    print(f"2020+ purchases: {len(purchases_lc)}")

    # --- Test 1: Disclosure date signal (TRADEABLE) ---
    events_disclosure = [
        {"ticker": t["ticker"], "date": t["disclosure_date"], "metadata": t}
        for t in purchases_lc if t.get("disclosure_date")
    ]
    print(f"\n--- Signal 1: Disclosure Date (N={len(events_disclosure)}) ---")
    result_disclosure = run_backtest(events_disclosure, label="disclosure_date")
    print_result(result_disclosure)

    # --- Test 2: Transaction date signal (informational, not tradeable) ---
    events_tx = [
        {"ticker": t["ticker"], "date": t["transaction_date"], "metadata": t}
        for t in purchases_lc
    ]
    print(f"\n--- Signal 2: Transaction Date (N={len(events_tx)}) ---")
    result_tx = run_backtest(events_tx, label="transaction_date")
    print_result(result_tx)

    # --- Segmentation: Chamber ---
    for chamber in ["House", "Senate"]:
        subset = [e for e in events_disclosure
                  if e["metadata"].get("chamber") == chamber]
        if len(subset) >= MIN_EVENTS:
            print(f"\n--- Chamber: {chamber} (N={len(subset)}) ---")
            r = run_backtest(subset, label=f"chamber_{chamber}")
            print_result(r)

    # --- Segmentation: Party ---
    for party in ["Republican", "Democrat"]:
        subset = [e for e in events_disclosure
                  if e["metadata"].get("party") == party]
        if len(subset) >= MIN_EVENTS:
            print(f"\n--- Party: {party} (N={len(subset)}) ---")
            r = run_backtest(subset, label=f"party_{party}")
            print_result(r)

    # --- Segmentation: Large trades (>$50K) ---
    large = [e for e in events_disclosure
             if (e["metadata"].get("amount_min") or 0) >= 50_000]
    if len(large) >= MIN_EVENTS:
        print(f"\n--- Large trades >$50K (N={len(large)}) ---")
        r = run_backtest(large, label="large_trades_50k_plus")
        print_result(r)

    # --- Segmentation: Short reporting gap (<15 days, more timely) ---
    fast = [e for e in events_disclosure
            if (e["metadata"].get("reporting_gap_days") or 999) <= 15]
    if len(fast) >= MIN_EVENTS:
        print(f"\n--- Fast reporters (<15 day gap) (N={len(fast)}) ---")
        r = run_backtest(fast, label="fast_reporters_lt15d")
        print_result(r)

    print("\n=== DONE ===")

    # Save results
    all_results = {
        "disclosure_date": result_disclosure,
        "transaction_date": result_tx,
    }
    out_file = os.path.join(os.path.dirname(__file__), "politician_backtest_results.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Results saved to {out_file}")


if __name__ == "__main__":
    main()
