"""
earnings_miss_backtest.py - Backtest post-earnings miss drift signal.

Collects earnings surprise data from yfinance, filters for misses of specified
severity, and measures abnormal returns at multiple horizons.

Usage:
    python3 tools/earnings_miss_backtest.py --universe sp100 --miss-threshold -5 --start 2020 --end 2025
    python3 tools/earnings_miss_backtest.py --universe sp100 --miss-threshold -10 --start 2020 --end 2025
"""

import argparse
import json
import sys
import time
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats

warnings.filterwarnings("ignore")

# Large-cap universe (~100 names, diversified sectors)
SP100_TICKERS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "BRK-B", "LLY", "AVGO", "JPM",
    "UNH", "V", "XOM", "MA", "COST", "HD", "PG", "JNJ", "ABBV", "WMT",
    "NFLX", "BAC", "CRM", "KO", "MRK", "CVX", "AMD", "PEP", "TMO", "ORCL",
    "ACN", "LIN", "MCD", "CSCO", "ADBE", "ABT", "WFC", "IBM", "PM", "GE",
    "QCOM", "TXN", "INTU", "ISRG", "CAT", "AMGN", "VZ", "BA", "NOW", "PFE",
    "GS", "BKNG", "BLK", "AXP", "T", "MS", "LOW", "RTX", "UNP", "SPGI",
    "DE", "NEE", "HON", "MDT", "SYK", "BMY", "SCHW", "LRCX", "ELV", "PLD",
    "GILD", "MMC", "CB", "CI", "SO", "REGN", "ADI", "ZTS", "DUK", "CL",
    "CME", "MDLZ", "BDX", "SHW", "FDX", "PYPL", "ITW", "APD", "NOC", "MCK",
    "TGT", "PGR", "MO", "HUM", "GM", "F", "AAL", "DAL", "UAL", "LUV",
]


def fetch_earnings_data(symbol, limit=40):
    """Fetch earnings surprise data for a symbol. Returns DataFrame or None."""
    try:
        t = yf.Ticker(symbol)
        ed = t.get_earnings_dates(limit=limit)
        if ed is None or len(ed) == 0:
            return None
        # Filter to reported results only
        reported = ed[ed["Reported EPS"].notna()].copy()
        if len(reported) == 0:
            return None
        reported["symbol"] = symbol
        reported.index.name = "earnings_date"
        reported = reported.reset_index()
        reported["earnings_date"] = pd.to_datetime(reported["earnings_date"]).dt.tz_localize(None)
        return reported
    except Exception:
        return None


def compute_abnormal_returns(events_df, benchmark="SPY", horizons=[1, 3, 5, 10]):
    """
    For each earnings event, compute abnormal return at multiple horizons.
    Entry = next trading day open after earnings date.
    """
    import os, sys
    sys.path.insert(0, os.path.dirname(__file__))
    from yfinance_utils import safe_download

    if events_df.empty:
        return events_df

    # Get unique symbols + benchmark
    symbols = list(events_df["symbol"].unique())
    all_syms = list(set(symbols + [benchmark]))

    # Determine date range
    min_date = events_df["earnings_date"].min() - timedelta(days=5)
    max_date = events_df["earnings_date"].max() + timedelta(days=max(horizons) + 5)
    today = datetime.now()
    if max_date > today:
        max_date = today

    # Download price data
    price_cache = {}
    for sym in all_syms:
        try:
            df = safe_download(sym, start=min_date.strftime("%Y-%m-%d"), end=max_date.strftime("%Y-%m-%d"))
            if df is not None and len(df) > 10:
                price_cache[sym] = df
        except Exception:
            continue

    if benchmark not in price_cache:
        print(f"ERROR: Cannot download benchmark {benchmark}")
        return events_df

    bench_df = price_cache[benchmark]

    results = []
    for _, row in events_df.iterrows():
        sym = row["symbol"]
        edate = row["earnings_date"]

        if sym not in price_cache:
            continue

        sym_df = price_cache[sym]

        # Find entry date = first trading day after earnings
        future_dates = sym_df.index[sym_df.index > pd.Timestamp(edate)]
        if len(future_dates) < max(horizons) + 1:
            continue

        entry_date = future_dates[0]
        entry_price = sym_df.loc[entry_date, "Open"]

        result = {
            "symbol": sym,
            "earnings_date": edate,
            "entry_date": entry_date,
            "entry_price": entry_price,
            "surprise_pct": row["Surprise(%)"],
            "eps_estimate": row["EPS Estimate"],
            "reported_eps": row["Reported EPS"],
        }

        for h in horizons:
            if len(future_dates) <= h:
                continue
            exit_date = future_dates[h]
            exit_price = sym_df.loc[exit_date, "Close"]

            # Stock return
            stock_ret = (exit_price - entry_price) / entry_price

            # Benchmark return over same period
            bench_future = bench_df.index[bench_df.index >= entry_date]
            if len(bench_future) <= h:
                continue
            bench_entry = bench_df.loc[bench_future[0], "Open"]
            bench_exit_date = bench_future[h]
            bench_exit = bench_df.loc[bench_exit_date, "Close"]
            bench_ret = (bench_exit - bench_entry) / bench_entry

            abnormal = stock_ret - bench_ret
            result[f"abnormal_{h}d"] = abnormal
            result[f"raw_{h}d"] = stock_ret

        results.append(result)

    return pd.DataFrame(results)


def analyze_results(results_df, horizons=[1, 3, 5, 10], direction="short"):
    """Compute statistics for the backtest results."""
    analysis = {}
    for h in horizons:
        col = f"abnormal_{h}d"
        if col not in results_df.columns:
            continue
        data = results_df[col].dropna()
        if len(data) < 5:
            continue

        mean_ret = data.mean()
        median_ret = data.median()
        std_ret = data.std()
        n = len(data)

        # T-test: is mean significantly different from 0?
        t_stat, p_val = stats.ttest_1samp(data, 0)

        # Direction accuracy (for short signal, negative abnormal = correct)
        if direction == "short":
            direction_correct = (data < -0.005).mean()  # >0.5% threshold
        else:
            direction_correct = (data > 0.005).mean()

        analysis[f"{h}d"] = {
            "n": n,
            "mean_abnormal": round(mean_ret * 100, 3),
            "median_abnormal": round(median_ret * 100, 3),
            "std_pct": round(std_ret * 100, 2),
            "t_stat": round(t_stat, 3),
            "p_value": round(p_val, 4),
            "direction_correct_pct": round(direction_correct * 100, 1),
            "significant_005": p_val < 0.05,
        }

    return analysis


def main():
    parser = argparse.ArgumentParser(description="Backtest post-earnings miss drift")
    parser.add_argument("--universe", default="sp100", help="Universe: sp100 or comma-separated tickers")
    parser.add_argument("--miss-threshold", type=float, default=-5.0, help="Surprise %% threshold (e.g., -5 = 5%% miss)")
    parser.add_argument("--start", type=int, default=2020, help="Start year for discovery period")
    parser.add_argument("--end", type=int, default=2025, help="End year for discovery period")
    parser.add_argument("--oos-start", type=int, default=None, help="OOS start year (default: end year)")
    parser.add_argument("--limit", type=int, default=40, help="Max quarters per ticker from yfinance")
    args = parser.parse_args()

    if args.universe == "sp100":
        tickers = SP100_TICKERS
    else:
        tickers = [t.strip() for t in args.universe.split(",")]

    oos_start = args.oos_start or args.end

    print(f"Scanning {len(tickers)} tickers for earnings misses <= {args.miss_threshold}%...")
    print(f"Discovery: {args.start}-{oos_start}, OOS: {oos_start}-present")

    # Phase 1: Collect earnings data
    all_earnings = []
    fetched = 0
    for i, sym in enumerate(tickers):
        ed = fetch_earnings_data(sym, limit=args.limit)
        if ed is not None:
            all_earnings.append(ed)
            fetched += 1
        # Rate limiting
        if (i + 1) % 20 == 0:
            print(f"  Fetched {i+1}/{len(tickers)} ({fetched} with data)...")
            time.sleep(1)  # Gentle rate limit

    if not all_earnings:
        print("ERROR: No earnings data collected")
        sys.exit(1)

    all_df = pd.concat(all_earnings, ignore_index=True)
    print(f"\nTotal earnings events: {len(all_df)}")

    # Phase 2: Filter for misses
    misses = all_df[all_df["Surprise(%)"] <= args.miss_threshold].copy()
    print(f"Events with surprise <= {args.miss_threshold}%: {len(misses)}")

    if len(misses) < 5:
        print("ERROR: Too few miss events. Try a less strict threshold.")
        sys.exit(1)

    # Split into discovery and OOS
    discovery = misses[misses["earnings_date"].dt.year.between(args.start, oos_start - 1)]
    oos = misses[misses["earnings_date"].dt.year >= oos_start]
    print(f"Discovery period ({args.start}-{oos_start-1}): {len(discovery)} events")
    print(f"OOS period ({oos_start}+): {len(oos)} events")

    # Phase 3: Compute abnormal returns
    horizons = [1, 3, 5, 10]

    print("\n=== DISCOVERY PERIOD ===")
    if len(discovery) >= 5:
        disc_results = compute_abnormal_returns(discovery, horizons=horizons)
        disc_analysis = analyze_results(disc_results, horizons=horizons, direction="short")
        for h, stats_dict in disc_analysis.items():
            sig = "***" if stats_dict["significant_005"] else ""
            print(f"  {h}: N={stats_dict['n']}, mean={stats_dict['mean_abnormal']:.2f}%, "
                  f"median={stats_dict['median_abnormal']:.2f}%, p={stats_dict['p_value']:.4f}{sig}, "
                  f"dir={stats_dict['direction_correct_pct']:.1f}%")

        # Show worst misses
        if "abnormal_5d" in disc_results.columns:
            print(f"\n  Top 5 worst misses (5d abnormal):")
            worst = disc_results.nlargest(5, "surprise_pct", keep="first")
            for _, r in worst.head(5).iterrows():
                abn = r.get("abnormal_5d", float("nan"))
                print(f"    {r['symbol']} {r['earnings_date'].date()} surprise={r['surprise_pct']:.1f}% "
                      f"abnormal_5d={abn*100:.2f}%")
    else:
        print("  Insufficient discovery events")
        disc_results = pd.DataFrame()
        disc_analysis = {}

    print("\n=== OOS PERIOD ===")
    if len(oos) >= 3:
        oos_results = compute_abnormal_returns(oos, horizons=horizons)
        oos_analysis = analyze_results(oos_results, horizons=horizons, direction="short")
        for h, stats_dict in oos_analysis.items():
            sig = "***" if stats_dict["significant_005"] else ""
            print(f"  {h}: N={stats_dict['n']}, mean={stats_dict['mean_abnormal']:.2f}%, "
                  f"median={stats_dict['median_abnormal']:.2f}%, p={stats_dict['p_value']:.4f}{sig}, "
                  f"dir={stats_dict['direction_correct_pct']:.1f}%")
    else:
        print("  Insufficient OOS events")
        oos_results = pd.DataFrame()
        oos_analysis = {}

    # Phase 4: Distribution analysis
    print("\n=== MISS SEVERITY DISTRIBUTION ===")
    bins = [(-100, -20), (-20, -10), (-10, -5), (-5, 0)]
    for lo, hi in bins:
        count = len(misses[(misses["Surprise(%)"] > lo) & (misses["Surprise(%)"] <= hi)])
        total = len(all_df[(all_df["Surprise(%)"] > lo) & (all_df["Surprise(%)"] <= hi)])
        print(f"  {lo}% to {hi}%: {total} events total ({count} in miss subset)")

    # Output JSON summary
    summary = {
        "universe_size": len(tickers),
        "total_earnings_events": len(all_df),
        "miss_threshold": args.miss_threshold,
        "total_misses": len(misses),
        "discovery_n": len(discovery),
        "oos_n": len(oos),
        "discovery_analysis": disc_analysis,
        "oos_analysis": oos_analysis,
    }

    print(f"\n=== JSON SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))

    # Save detailed results
    if len(disc_results) > 0:
        disc_results.to_csv("/tmp/earnings_miss_discovery.csv", index=False)
        print(f"\nDiscovery results saved to /tmp/earnings_miss_discovery.csv")
    if len(oos_results) > 0:
        oos_results.to_csv("/tmp/earnings_miss_oos.csv", index=False)
        print(f"OOS results saved to /tmp/earnings_miss_oos.csv")


if __name__ == "__main__":
    main()
