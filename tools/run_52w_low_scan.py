#!/usr/bin/env python3
"""
52-Week Low First-Touch Scanner (Research Only)
Finds S&P 500 large-cap stocks approaching their first-ever 52-week low
with zero crossings in the past 2 years and current price within 3% of 52w low.

RESEARCH ONLY - does not modify any database or create hypotheses.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from tools.yfinance_utils import safe_download

# Full ticker universe as specified
TICKERS = list(dict.fromkeys([
    'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NVDA', 'TSLA', 'JPM', 'V', 'MA',
    'PG', 'JNJ', 'UNH', 'HD', 'CVX', 'MRK', 'ABBV', 'PEP', 'KO', 'COST',
    'WMT', 'DIS', 'MCD', 'NKE', 'INTC', 'IBM', 'GE', 'CAT', 'BA', 'HON',
    'MMM', 'EMR', 'ETN', 'LMT', 'RTX', 'NOC', 'GD', 'SPGI', 'BLK', 'SCHW',
    'MS', 'GS', 'AXP', 'USB', 'WFC', 'BAC', 'C', 'PFE', 'AMGN', 'GILD',
    'BMY', 'LLY', 'ABT', 'MDT', 'SYK', 'BSX', 'EW', 'BDX', 'ZBH', 'SHW',
    'PPG', 'APD', 'ECL', 'DD', 'LIN', 'FDX', 'UPS', 'CSX', 'NSC', 'CP',
    'CNI', 'UNP', 'JBHT', 'XOM', 'COP', 'SLB', 'HAL', 'BKR', 'NEE', 'DUK',
    'SO', 'D', 'EXC', 'AEP', 'PCG', 'MO', 'PM', 'MDLZ', 'KHC', 'CPB',
    'CAG', 'GIS', 'HSY', 'MKC', 'DPZ', 'YUM', 'CMG', 'SBUX', 'TJX', 'ROST',
    'GPS', 'M', 'JWN', 'ADBE', 'ORCL', 'SAP', 'CRM', 'NOW', 'INTU', 'WDAY',
    'ZM', 'DDOG', 'TDG', 'GWW', 'OTIS', 'CARR', 'ROK', 'PH', 'ITW', 'DOV',
    'IR', 'AME', 'VRSK', 'MCO', 'MSCI', 'IEX', 'IDEX', 'ROP', 'DG', 'DLTR',
    'WBA', 'CVS', 'HUM', 'CI', 'ELV', 'CNC', 'MOH', 'SBAC', 'AMT', 'CCI',
    'EQIX', 'PLD', 'SPG', 'AVB', 'EQR', 'MAA', 'ESS', 'ARE', 'BXP', 'WY',
    'LW', 'NTAP', 'STX', 'WDC', 'MU', 'LRCX', 'KLAC', 'AMAT', 'TXN', 'ADI',
    'NXPI', 'ON', 'AVGO', 'QCOM',
]))

START = str(date.today() - timedelta(days=760))   # ~2 years + buffer for rolling window
END   = str(date.today() + timedelta(days=1))

PCT_THRESHOLD = 3.0        # within 3% of 52w low
MKTCAP_MIN_B  = 5.0        # $5B minimum market cap
ROLLING_WINDOW = 252       # trading-day 52-week low window


def get_market_cap_b(ticker: str) -> float | None:
    """Fetch market cap in billions from yfinance Ticker.info."""
    try:
        info = yf.Ticker(ticker).info
        mc = info.get('marketCap') or info.get('market_cap')
        if mc:
            return round(mc / 1e9, 2)
    except Exception:
        pass
    return None


def analyze_ticker(ticker: str) -> dict | None:
    """
    Download 2 years of history, compute rolling 252-day min,
    count zero-crossing events, return candidate dict or None.
    """
    try:
        df = safe_download(ticker, start=START, end=END)
        if len(df) < ROLLING_WINDOW + 10:
            return None

        close = df['Close'].dropna()
        if len(close) < ROLLING_WINDOW + 10:
            return None

        # Rolling 252-day minimum (52-week low)
        rolling_min = close.rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).min()
        prev_rolling_min = rolling_min.shift(1)

        # Crossings: close drops BELOW the prior day's rolling minimum
        crossed = close < prev_rolling_min
        n_crossings = int(crossed.sum())

        current_close = float(close.iloc[-1])
        current_52w_low = float(rolling_min.dropna().iloc[-1])

        pct_above = (current_close / current_52w_low - 1) * 100

        # Filter: zero crossings AND within threshold of 52w low
        if n_crossings != 0 or pct_above > PCT_THRESHOLD:
            return None

        return {
            'symbol': ticker,
            'current_price': round(current_close, 2),
            '52w_low': round(current_52w_low, 2),
            'pct_above': round(pct_above, 2),
            'crossings': n_crossings,
        }

    except Exception:
        return None


def main():
    print(f"=== 52-WEEK LOW FIRST-TOUCH SCANNER ===")
    print(f"Universe : {len(TICKERS)} tickers")
    print(f"History  : {START} to {END}")
    print(f"Filters  : crossings=0, pct_above <= {PCT_THRESHOLD}%, market_cap > ${MKTCAP_MIN_B}B")
    print()

    candidates = []
    errors = []

    for i, ticker in enumerate(TICKERS):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(TICKERS)} scanned, {len(candidates)} candidates so far...")

        result = analyze_ticker(ticker)
        if result is not None:
            candidates.append(result)

        # Small sleep every 10 tickers to avoid rate limits
        if (i + 1) % 10 == 0:
            time.sleep(0.5)

    print(f"\n  Done scanning. Fetching market caps for {len(candidates)} candidates...\n")

    # Enrich with market cap and filter >$5B
    final = []
    for c in candidates:
        mc = get_market_cap_b(c['symbol'])
        c['market_cap_b'] = mc
        if mc is None or mc >= MKTCAP_MIN_B:
            # Include if we couldn't fetch (None) or it passes the filter
            # But only mark as "unknown" if None
            final.append(c)

    # Filter out confirmed small-caps (market cap known and < $5B)
    final = [c for c in final
             if c['market_cap_b'] is None or c['market_cap_b'] >= MKTCAP_MIN_B]

    # Sort by pct_above ascending (closest to 52w low first)
    final.sort(key=lambda x: x['pct_above'])

    print("=" * 75)
    print(f"{'SYMBOL':<8} {'PRICE':>8} {'52W_LOW':>8} {'PCT_ABOVE':>10} {'CROSSINGS':>10} {'MKTCAP_B':>10}")
    print("-" * 75)

    if not final:
        print("  No candidates found matching all criteria.")
    else:
        for c in final:
            mc_str = f"${c['market_cap_b']:.1f}B" if c['market_cap_b'] else "N/A"
            print(f"  {c['symbol']:<6} {c['current_price']:>9.2f} {c['52w_low']:>9.2f} "
                  f"{c['pct_above']:>9.2f}% {c['crossings']:>10} {mc_str:>10}")

    print("=" * 75)
    print(f"\nTotal candidates (crossings=0, within {PCT_THRESHOLD}% of 52w low, mktcap>${MKTCAP_MIN_B}B): {len(final)}")
    print("\nRESEARCH ONLY — no database changes made.")


if __name__ == '__main__':
    main()
