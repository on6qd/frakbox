"""
Comprehensive Tariff Sector Rotation Backtest
==============================================
Tests how major S&P 500 sector ETFs perform after major tariff escalation events.
Uses the same event set as the XLF and XLU tests.

Outputs abnormal returns (vs SPY) at 1d, 5d, 10d, 20d horizons.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.yfinance_utils import safe_download

# Major tariff escalation events (confirmed by market reaction, same set used in XLF/XLU tests)
TARIFF_EVENTS = [
    # 2018 US-China trade war
    ("2018-03-22", "Trump announces 25% steel / 10% aluminum tariffs (Section 232)"),
    ("2018-07-06", "$34B Chinese goods tariffs take effect"),
    ("2018-07-10", "USTR announces $200B additional tariff list"),
    ("2018-09-17", "Trump announces $200B at 10% tariffs on Chinese goods"),
    ("2019-05-05", "Trump tweets 10→25% tariff escalation on $200B Chinese goods"),
    ("2019-08-01", "Trump announces 10% tariffs on remaining $300B Chinese goods"),
    # 2025 tariff escalation
    ("2025-02-01", "Trump announces 25% tariffs on Canada/Mexico, 10% on China"),
    ("2025-04-02", "Liberation Day: 10% universal tariff + reciprocal tariffs announced"),
]

# Sector ETFs to test
SECTORS = {
    "XLK": "Technology",
    "XLY": "Consumer Discretionary",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLE": "Energy",
    "XLV": "Healthcare",
    "XLP": "Consumer Staples",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLC": "Communication Services",
    "SPY": "S&P 500 (benchmark)",
}

HORIZONS = [1, 5, 10, 20]


def get_price(ticker, date_str, offset=0):
    """Get close price at event date + offset trading days."""
    try:
        base = pd.Timestamp(date_str)
        start = base - timedelta(days=5)
        end = base + timedelta(days=35)
        data = safe_download(ticker, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        if data is None or len(data) < 2:
            return None
        # Get the trading days
        trading_days = data.index.tolist()
        # Find the event date or next trading day
        event_idx = None
        for i, day in enumerate(trading_days):
            if day >= base:
                event_idx = i
                break
        if event_idx is None:
            return None
        target_idx = event_idx + offset
        if target_idx >= len(trading_days):
            return None
        return float(data["Close"].iloc[target_idx])
    except Exception as e:
        return None


def compute_abnormal_return(sector_ticker, spy_prices, sector_prices, event_date, horizon):
    """Compute abnormal return = sector return - SPY return over horizon."""
    base_sector = sector_prices.get((event_date, 0))
    h_sector = sector_prices.get((event_date, horizon))
    base_spy = spy_prices.get((event_date, 0))
    h_spy = spy_prices.get((event_date, horizon))

    if any(x is None for x in [base_sector, h_sector, base_spy, h_spy]):
        return None
    if base_sector == 0 or base_spy == 0:
        return None

    sector_ret = (h_sector - base_sector) / base_sector * 100
    spy_ret = (h_spy - base_spy) / base_spy * 100
    return sector_ret - spy_ret


def main():
    print("=" * 70)
    print("TARIFF ESCALATION: SECTOR ROTATION ANALYSIS")
    print(f"Events: {len(TARIFF_EVENTS)} major tariff escalations (2018-2025)")
    print("=" * 70)

    # Download all price data
    print("\nDownloading price data...")

    # Cache prices: dict[(ticker, event_date, offset)] = price
    price_cache = {}

    all_tickers = list(SECTORS.keys())

    for event_date, event_name in TARIFF_EVENTS:
        for ticker in all_tickers:
            for offset in [0] + HORIZONS:
                key = (ticker, event_date, offset)
                price_cache[key] = get_price(ticker, event_date, offset)

    # Compute results
    print("\n" + "=" * 70)
    print(f"{'Sector':25} {'1d':>8} {'5d':>8} {'10d':>8} {'20d':>8}  {'Dir(5d)':>8} {'N':>4}")
    print("-" * 70)

    results = {}

    for ticker, sector_name in SECTORS.items():
        if ticker == "SPY":
            continue

        sector_results = {h: [] for h in HORIZONS}

        for event_date, _ in TARIFF_EVENTS:
            for horizon in HORIZONS:
                base_sector = price_cache.get((ticker, event_date, 0))
                h_sector = price_cache.get((ticker, event_date, horizon))
                base_spy = price_cache.get(("SPY", event_date, 0))
                h_spy = price_cache.get(("SPY", event_date, horizon))

                if any(x is None for x in [base_sector, h_sector, base_spy, h_spy]):
                    continue

                sector_ret = (h_sector / base_sector - 1) * 100
                spy_ret = (h_spy / base_spy - 1) * 100
                abnormal = sector_ret - spy_ret
                sector_results[horizon].append(abnormal)

        results[ticker] = sector_results

        # Format output
        def fmt(returns):
            if not returns:
                return "   N/A"
            mean = np.mean(returns)
            return f"{mean:+6.2f}%"

        n = len(sector_results[5]) if sector_results[5] else 0
        dir_5d = sum(1 for x in sector_results[5] if x < 0) / n * 100 if n > 0 else 0
        # For shorts, negative is good; for longs, positive is good
        label = f"{sector_name[:23]:23}"
        print(f"{ticker:4} {label:20} {fmt(sector_results[1]):>8} {fmt(sector_results[5]):>8} {fmt(sector_results[10]):>8} {fmt(sector_results[20]):>8}  {dir_5d:6.0f}%↓  {n:2}")

    print("\n" + "=" * 70)
    print("STATISTICAL SIGNIFICANCE (t-test vs zero, 2-tailed)")
    print("-" * 70)

    significant = []

    for ticker, sector_results in results.items():
        if ticker == "SPY":
            continue
        sig_horizons = []
        for h in HORIZONS:
            returns = sector_results[h]
            if len(returns) < 3:
                continue
            t, p = stats.ttest_1samp(returns, 0)
            if p < 0.10:
                sig_horizons.append(f"{h}d(p={p:.3f})")
        if sig_horizons:
            sector_name = SECTORS[ticker]
            print(f"  {ticker} ({sector_name}): {', '.join(sig_horizons)}")
            significant.append(ticker)

    if not significant:
        print("  No sectors significant at p<0.10")

    print("\n" + "=" * 70)
    print("PER-EVENT BREAKDOWN (5d abnormal returns)")
    print("-" * 70)

    # Show per-event for top sectors
    top_sectors = ["XLF", "XLK", "XLI", "XLB", "XLU", "XLP"]
    headers = [e[0] for e in TARIFF_EVENTS]

    print(f"{'Sector':8}", end="")
    for e in TARIFF_EVENTS:
        print(f" {e[0][5:]:>7}", end="")
    print()
    print("-" * (8 + 8 * len(TARIFF_EVENTS)))

    for ticker in top_sectors:
        if ticker not in results:
            continue
        print(f"{ticker:8}", end="")
        for event_date, _ in TARIFF_EVENTS:
            base_sector = price_cache.get((ticker, event_date, 0))
            h_sector = price_cache.get((ticker, event_date, 5))
            base_spy = price_cache.get(("SPY", event_date, 0))
            h_spy = price_cache.get(("SPY", event_date, 5))

            if any(x is None for x in [base_sector, h_sector, base_spy, h_spy]):
                print(f" {'N/A':>7}", end="")
                continue

            sector_ret = (h_sector / base_sector - 1) * 100
            spy_ret = (h_spy / base_spy - 1) * 100
            abnormal = sector_ret - spy_ret
            print(f" {abnormal:+7.2f}", end="")
        print()

    print("\nKey: Values are 5-day ABNORMAL returns (sector minus SPY)")
    print("Negative = underperforms SPY (short opportunity)")
    print("Positive = outperforms SPY (long opportunity)")


if __name__ == "__main__":
    main()
