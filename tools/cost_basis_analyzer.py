"""
Cost Basis Analyzer - Volume Profile Approach

Tests the disposition effect hypothesis from steer.md:
- When stocks rise on low volume, more investors have unrealized gains → selling pressure
- When stocks rise on high volume, more shares changed hands at higher prices → less selling pressure

Key metric: "percent_in_profit" = what % of past-year volume was transacted below current price
This approximates "what fraction of current holders are sitting on gains"

If percent_in_profit is high (lots of gains) → predict selling pressure → bearish
If percent_in_profit is low (many underwater) → predict less selling → bullish

This is distinct from simple VWAP comparison because:
- VWAP measures average cost
- This measures the DISTRIBUTION and what % of shareholders are at a gain

Usage:
    python tools/cost_basis_analyzer.py
"""

import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def compute_percent_in_profit(prices_df, lookback_days=252):
    """
    For each trading day, compute what % of the last `lookback_days` volume
    was transacted at prices BELOW the current price.

    This approximates the fraction of current holders sitting on unrealized gains.

    Method: Each day's volume is treated as traded at that day's average price (H+L)/2.
    A more precise version would use intraday data, but daily OHLCV is the best we have.

    Returns: Series of percent_in_profit values (0-100)
    """
    avg_price = (prices_df['High'] + prices_df['Low']) / 2
    volume = prices_df['Volume']
    close = prices_df['Close']

    results = []

    for i in range(lookback_days, len(prices_df)):
        current_price = close.iloc[i]

        # Look at last lookback_days of trading
        window_prices = avg_price.iloc[i-lookback_days:i]
        window_vol = volume.iloc[i-lookback_days:i]

        total_vol = window_vol.sum()
        if total_vol == 0:
            results.append(np.nan)
            continue

        # Volume traded BELOW current price (those holders are in profit)
        below_mask = window_prices < current_price
        vol_in_profit = window_vol[below_mask].sum()

        percent_in_profit = (vol_in_profit / total_vol) * 100
        results.append(percent_in_profit)

    return pd.Series(results, index=prices_df.index[lookback_days:])


def run_backtest(symbols, start_date, end_date,
                 pip_threshold_high=75,  # >75% in profit = high gains
                 pip_threshold_low=35,   # <35% in profit = many underwater
                 forward_days=5,
                 benchmark="SPY"):
    """
    Test whether percent_in_profit predicts forward abnormal returns.

    High PIP (lots of gains) → expect selling pressure → lower forward returns?
    Low PIP (many underwater) → expect reduced selling → higher forward returns?
    """
    print(f"Loading data for {len(symbols)} symbols from {start_date} to {end_date}...")

    # Load benchmark
    spy = yf.download("SPY", start=start_date, end=end_date, progress=False)
    if spy.empty:
        print("ERROR: Could not load SPY benchmark")
        return None

    # Handle multi-level columns from yfinance
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.get_level_values(0)

    spy_returns = spy['Close'].pct_change()

    all_results = []

    for sym in symbols:
        try:
            data = yf.download(sym, start=start_date, end=end_date, progress=False)
            if data.empty or len(data) < 300:
                continue

            # Handle multi-level columns
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            pip = compute_percent_in_profit(data, lookback_days=252)

            # For each day with PIP signal, measure forward returns
            for dt_str in pip.index.strftime('%Y-%m-%d'):
                dt = pd.Timestamp(dt_str)
                if dt not in pip.index:
                    continue

                pip_val = pip[dt]
                if np.isnan(pip_val):
                    continue

                # Classify this signal
                if pip_val >= pip_threshold_high:
                    signal = 'high_pip'  # Many holders in profit
                elif pip_val <= pip_threshold_low:
                    signal = 'low_pip'   # Many holders underwater
                else:
                    continue  # Middle ground - not a strong signal

                # Find future return
                try:
                    close_series = data['Close']
                    dt_pos = close_series.index.get_loc(dt)

                    if dt_pos + forward_days >= len(close_series):
                        continue

                    entry_price = close_series.iloc[dt_pos + 1] if dt_pos + 1 < len(close_series) else np.nan
                    exit_price = close_series.iloc[dt_pos + forward_days]

                    if np.isnan(entry_price) or entry_price <= 0:
                        continue

                    raw_return = (exit_price - entry_price) / entry_price * 100

                    # Get benchmark return for same period
                    spy_dt_pos = spy_returns.index.get_loc(dt) if dt in spy_returns.index else None
                    if spy_dt_pos is None or spy_dt_pos + forward_days >= len(spy_returns):
                        abnormal_return = raw_return
                    else:
                        spy_fwd = spy['Close'].iloc[spy_dt_pos + forward_days] / spy['Close'].iloc[spy_dt_pos + 1] - 1
                        abnormal_return = raw_return - spy_fwd * 100

                    all_results.append({
                        'symbol': sym,
                        'date': dt_str,
                        'pip': pip_val,
                        'signal': signal,
                        'raw_return': raw_return,
                        'abnormal_return': abnormal_return,
                    })
                except (KeyError, IndexError):
                    continue
        except Exception as e:
            print(f"  Error on {sym}: {e}")
            continue

    return all_results


def analyze_results(results):
    """Compute statistics for high vs low PIP signals"""
    if not results:
        print("No results to analyze")
        return

    df = pd.DataFrame(results)
    print(f"\nTotal signal events: {len(df)}")
    print(f"Symbols covered: {df['symbol'].nunique()}")

    for signal_type, label in [('high_pip', 'HIGH PIP (>75%, lots of gains)'),
                                 ('low_pip', 'LOW PIP (<35%, many underwater)')]:
        subset = df[df['signal'] == signal_type]
        if len(subset) < 10:
            print(f"\n{label}: N={len(subset)} (too few)")
            continue

        returns = subset['abnormal_return'].dropna()

        # t-test
        t_stat, p_val = stats.ttest_1samp(returns, 0)

        print(f"\n{label}:")
        print(f"  N = {len(returns)}")
        print(f"  Mean abnormal return: {returns.mean():.2f}%")
        print(f"  Median: {returns.median():.2f}%")
        print(f"  Std dev: {returns.std():.2f}%")
        print(f"  % positive: {(returns > 0).mean()*100:.1f}%")
        print(f"  t-stat: {t_stat:.2f}, p-value: {p_val:.4f}")

        # Also test direction: high PIP should be NEGATIVE (selling pressure)
        if signal_type == 'high_pip':
            t_neg, p_neg = stats.ttest_1samp(returns, 0, alternative='less')
            print(f"  P(negative, one-sided): {p_neg:.4f}")

    # Is there a significant difference between high and low PIP?
    high_returns = df[df['signal'] == 'high_pip']['abnormal_return'].dropna()
    low_returns = df[df['signal'] == 'low_pip']['abnormal_return'].dropna()

    if len(high_returns) >= 10 and len(low_returns) >= 10:
        t_diff, p_diff = stats.ttest_ind(high_returns, low_returns)
        print(f"\nHigh PIP vs Low PIP difference:")
        print(f"  Mean diff: {high_returns.mean() - low_returns.mean():.2f}%")
        print(f"  t-stat: {t_diff:.2f}, p-value: {p_diff:.4f}")

    return df


if __name__ == "__main__":
    # Test universe: 100 large-cap stocks
    test_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'BRK-B', 'JPM', 'UNH',
        'JNJ', 'V', 'XOM', 'PG', 'MA', 'HD', 'CVX', 'MRK', 'ABBV', 'KO',
        'PEP', 'BAC', 'WMT', 'AVGO', 'TMO', 'COST', 'DIS', 'ABT', 'ACN', 'WFC',
        'CRM', 'DHR', 'AMD', 'LLY', 'TXN', 'NFLX', 'QCOM', 'PM', 'MDT', 'NEE',
        'BMY', 'AMGN', 'HON', 'SPGI', 'IBM', 'UPS', 'CAT', 'LOW', 'ORCL', 'GE',
        'RTX', 'ELV', 'SBUX', 'AXP', 'PLD', 'SYK', 'MDLZ', 'BLK', 'ADI', 'DE',
        'TJX', 'CI', 'GILD', 'CVS', 'MU', 'SO', 'REGN', 'ZTS', 'CB', 'NOW',
        'ISRG', 'MMC', 'LRCX', 'MO', 'DUK', 'SHW', 'APD', 'AON', 'ITW', 'BSX',
        'HCA', 'EMR', 'FCX', 'WM', 'PH', 'GM', 'MCK', 'HUM', 'PSX', 'KLAC',
        'ANET', 'F', 'NSC', 'AIG', 'ETN', 'MSI', 'CSX', 'D', 'AZO', 'OXY'
    ]

    print("=" * 60)
    print("COST BASIS / DISPOSITION EFFECT TEST")
    print("Metric: Percent of past-year volume transacted below current price")
    print("Hypothesis: High % in profit → selling pressure → lower forward returns")
    print("=" * 60)

    # Test on 2021-2023 (discovery period)
    print("\n--- DISCOVERY PERIOD (2021-2023) ---")
    results_disc = run_backtest(
        test_symbols[:50],  # 50 stocks for speed
        start_date="2020-01-01",  # need lookback period
        end_date="2024-01-01",
        pip_threshold_high=75,
        pip_threshold_low=35,
        forward_days=5,
    )

    if results_disc:
        df_disc = analyze_results(results_disc)

        print("\n--- PIP Distribution Statistics ---")
        df = pd.DataFrame(results_disc)
        print(f"PIP range: {df['pip'].min():.1f}% to {df['pip'].max():.1f}%")
        print(f"PIP median: {df['pip'].median():.1f}%")
        print(f"High PIP events (>75%): {(df['pip'] > 75).sum()}")
        print(f"Low PIP events (<35%): {(df['pip'] < 35).sum()}")

        # Test across multiple forward horizons
        print("\n--- MULTI-HORIZON TEST (High PIP only) ---")
        for fwd in [1, 3, 5, 10, 20]:
            results_h = run_backtest(
                test_symbols[:30],
                start_date="2020-01-01",
                end_date="2024-01-01",
                pip_threshold_high=75,
                pip_threshold_low=35,
                forward_days=fwd,
            )
            if results_h:
                df_h = pd.DataFrame(results_h)
                high = df_h[df_h['signal'] == 'high_pip']['abnormal_return'].dropna()
                if len(high) >= 10:
                    t, p = stats.ttest_1samp(high, 0)
                    print(f"  {fwd}d: N={len(high)}, mean={high.mean():.2f}%, p={p:.4f}, pos%={(high>0).mean()*100:.0f}%")
