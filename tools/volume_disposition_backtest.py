"""
Volume-Disposition Effect Backtest
===================================
Tests the hypothesis from steer.md:
  - When a stock rises on LOW cumulative volume → many investors hold large unrealized gains
    → selling pressure → bearish going forward
  - When a stock rises on HIGH cumulative volume → high turnover, fewer old holders
    → less selling pressure → bullish going forward

Methodology:
  1. Universe: S&P 500 component stocks (2015–2024)
  2. Lookback: 60 trading days (~3 months)
  3. On each monthly sample date:
     - Compute 60d price return
     - Compute volume turnover ratio = (60d total volume) / (252d avg daily volume × 60)
       * ratio > 1.0 means above-average volume during the rise
       * ratio < 1.0 means below-average volume during the rise
  4. Filter: stocks where 60d return > +10%  (price has risen meaningfully)
  5. Split into HIGH turnover (>1.2) and LOW turnover (<0.8)
  6. Measure forward 5d, 10d, 20d raw returns (we use SPY as benchmark)
  7. Compare groups for statistical significance
"""

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------
# Representative sample of S&P 500 stocks across sectors (avoiding survivorship
# bias by using mostly large-cap names that were in the index throughout 2015-2024)
UNIVERSE = [
    # Tech
    "AAPL", "MSFT", "GOOGL", "META", "NVDA", "AVGO", "ORCL", "ADBE", "CRM", "INTC",
    "AMD", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "SNPS", "CDNS",
    # Healthcare
    "JNJ", "UNH", "LLY", "PFE", "ABBV", "MRK", "TMO", "DHR", "ABT", "BMY",
    "AMGN", "GILD", "VRTX", "REGN", "SYK", "BSX", "MDT", "ISRG", "ZBH", "BAX",
    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "AXP", "CB", "MMC",
    "PGR", "TRV", "ALL", "AFL", "MET", "PRU", "AIG", "SFM", "FITB", "KEY",
    # Consumer
    "AMZN", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX", "COST", "TGT", "WMT",
    "PG", "KO", "PEP", "MDLZ", "GIS", "K", "CL", "EL", "CHD", "SJM",
    # Industrials
    "HON", "GE", "CAT", "DE", "MMM", "LMT", "RTX", "BA", "NOC", "GD",
    "UPS", "FDX", "CSX", "NSC", "UNP", "EMR", "ETN", "ROK", "PH", "DOV",
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "PXD", "OXY",
]


def get_price_data(symbols, start="2014-01-01", end="2025-01-01"):
    """Download OHLCV data for all symbols."""
    print(f"Downloading data for {len(symbols)} symbols...")
    data = {}
    batch_size = 20
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        try:
            raw = yf.download(batch, start=start, end=end, auto_adjust=True, progress=False)
            for sym in batch:
                try:
                    if len(batch) == 1:
                        sym_data = raw
                    else:
                        sym_data = raw.xs(sym, level=1, axis=1) if sym in raw.columns.get_level_values(1) else None
                    if sym_data is not None and not sym_data.empty:
                        # Ensure columns are 1D
                        sym_data = sym_data.copy()
                        for col in ['Close', 'Volume']:
                            if col in sym_data and isinstance(sym_data[col], pd.DataFrame):
                                sym_data[col] = sym_data[col].iloc[:, 0]
                        data[sym] = sym_data[['Close', 'Volume']].dropna()
                except Exception:
                    pass
        except Exception as e:
            print(f"  Batch error: {e}")
    print(f"  Got data for {len(data)} symbols")
    return data


def run_backtest(data, spy_returns,
                 lookback_days=60,     # window to measure price rise + volume
                 avg_vol_window=252,   # baseline for "normal" volume
                 min_return=0.10,      # stock must have risen at least 10%
                 high_turnover_thresh=1.20,  # turnover ratio threshold for "high volume"
                 low_turnover_thresh=0.80,   # turnover ratio threshold for "low volume"
                 forward_days=(5, 10, 20),   # horizons to measure
                 sample_freq=21):            # sample every ~1 month
    """
    For each stock, on each monthly sample date:
    - Compute 60d return and volume turnover ratio
    - If stock is up >10%, classify as high or low turnover
    - Measure subsequent forward returns (abnormal vs SPY)
    """
    records = []

    for sym, df in data.items():
        closes = df['Close']
        volumes = df['Volume']
        dates = closes.index

        if len(dates) < avg_vol_window + lookback_days + max(forward_days) + 10:
            continue

        # Monthly sample dates
        start_idx = avg_vol_window + lookback_days
        for i in range(start_idx, len(dates) - max(forward_days) - 5, sample_freq):
            sample_date = dates[i]

            # 1. 60d price return
            price_now = closes.iloc[i]
            price_60d_ago = closes.iloc[i - lookback_days]
            if price_60d_ago <= 0:
                continue
            ret_60d = (price_now / price_60d_ago) - 1

            # Only interested in stocks that have risen
            if ret_60d < min_return:
                continue

            # 2. Volume turnover ratio
            # Total volume over past 60d vs 252d average daily volume
            vol_60d = volumes.iloc[i - lookback_days:i].sum()
            avg_daily_vol_252d = volumes.iloc[i - avg_vol_window:i].mean()
            if avg_daily_vol_252d <= 0:
                continue
            # Expected volume if turnover was "normal"
            expected_vol = avg_daily_vol_252d * lookback_days
            turnover_ratio = vol_60d / expected_vol

            # Classify
            if turnover_ratio >= high_turnover_thresh:
                group = "high_turnover"
            elif turnover_ratio <= low_turnover_thresh:
                group = "low_turnover"
            else:
                group = "medium"

            # 3. Forward returns at each horizon
            fwd_returns = {}
            for fwd in forward_days:
                if i + fwd < len(dates):
                    fwd_price = closes.iloc[i + fwd]
                    stock_ret = (fwd_price / price_now) - 1

                    # Benchmark: SPY return over same period
                    fwd_date = dates[i + fwd]
                    try:
                        spy_ret_slice = spy_returns.loc[sample_date:fwd_date]
                        if len(spy_ret_slice) >= fwd - 2:
                            spy_ret = float((1 + spy_ret_slice).prod() - 1)
                        else:
                            spy_ret = 0.0
                    except Exception:
                        spy_ret = 0.0

                    fwd_returns[f'abnormal_{fwd}d'] = float(stock_ret) - spy_ret
                    fwd_returns[f'raw_{fwd}d'] = float(stock_ret)

            records.append({
                'symbol': sym,
                'date': sample_date,
                'ret_60d': ret_60d,
                'turnover_ratio': turnover_ratio,
                'group': group,
                **fwd_returns
            })

    return pd.DataFrame(records)


def analyze_results(df, forward_days=(5, 10, 20)):
    """Compare high vs low turnover groups and report statistics."""
    print("\n" + "=" * 70)
    print("VOLUME-DISPOSITION BACKTEST RESULTS")
    print("=" * 70)
    print(f"\nTotal observations (stocks up >10% in 60d): {len(df)}")

    high = df[df['group'] == 'high_turnover']
    low = df[df['group'] == 'low_turnover']
    med = df[df['group'] == 'medium']

    print(f"\nGroup sizes:")
    print(f"  High turnover (ratio >= 1.2): n={len(high)}")
    print(f"  Low turnover  (ratio <= 0.8): n={len(low)}")
    print(f"  Medium       (0.8–1.2):       n={len(med)}")

    results = {}
    print(f"\n{'Horizon':>10} | {'High Turn Avg':>14} | {'Low Turn Avg':>13} | {'Difference':>11} | {'p-value':>9} | {'Significant':>12}")
    print("-" * 85)

    for fwd in forward_days:
        col = f'abnormal_{fwd}d'
        if col not in df.columns:
            continue

        h_vals = high[col].dropna()
        l_vals = low[col].dropna()

        if len(h_vals) < 10 or len(l_vals) < 10:
            continue

        h_mean = h_vals.mean()
        l_mean = l_vals.mean()
        diff = h_mean - l_mean

        # Welch's t-test
        t_stat, p_val = stats.ttest_ind(h_vals, l_vals, equal_var=False)

        sig = "YES ✓" if p_val < 0.05 else "NO"
        print(f"  {fwd}d {'abn':>4} | {h_mean:>+13.2%} | {l_mean:>+12.2%} | {diff:>+10.2%} | {p_val:>9.4f} | {sig:>12}")

        results[fwd] = {
            'high_mean': h_mean,
            'low_mean': l_mean,
            'diff': diff,
            'p_value': p_val,
            'n_high': len(h_vals),
            'n_low': len(l_vals),
        }

    print(f"\n--- Per-group summary statistics ---")
    for group_name, gdf in [("High turnover", high), ("Low turnover", low)]:
        print(f"\n{group_name}:")
        for fwd in forward_days:
            col = f'abnormal_{fwd}d'
            if col in gdf.columns:
                vals = gdf[col].dropna()
                pos_rate = (vals > 0).mean()
                print(f"  {fwd}d: mean={vals.mean():+.2%}, median={vals.median():+.2%}, "
                      f"positive_rate={pos_rate:.1%}, n={len(vals)}")

    return results


def main():
    print("Volume-Disposition Effect Backtest")
    print("===================================")
    print("Hypothesis: stocks rising on LOW volume face more selling pressure")
    print("           (more investors holding large unrealized gains)")
    print()

    # Download data
    data = get_price_data(UNIVERSE)

    # SPY as benchmark
    spy_raw = yf.download("SPY", start="2014-01-01", end="2025-01-01",
                          auto_adjust=True, progress=False)['Close']
    # Ensure we have a 1D Series
    if isinstance(spy_raw, pd.DataFrame):
        spy_raw = spy_raw.iloc[:, 0]
    spy_returns = spy_raw.pct_change().dropna()

    # Run backtest
    print("\nRunning backtest...")
    df = run_backtest(data, spy_returns)

    if df.empty:
        print("No data returned from backtest!")
        return

    # Analyze
    results = analyze_results(df)

    # Additional: turnover ratio distribution
    print(f"\n--- Turnover ratio statistics ---")
    print(f"  Mean:   {df['turnover_ratio'].mean():.2f}")
    print(f"  Median: {df['turnover_ratio'].median():.2f}")
    print(f"  25th percentile: {df['turnover_ratio'].quantile(0.25):.2f}")
    print(f"  75th percentile: {df['turnover_ratio'].quantile(0.75):.2f}")

    # Distribution of 60d returns
    print(f"\n--- 60d return distribution (filtered >10%) ---")
    print(f"  Mean:   {df['ret_60d'].mean():.1%}")
    print(f"  Median: {df['ret_60d'].median():.1%}")
    print(f"  Max:    {df['ret_60d'].max():.1%}")

    # Save results
    summary = {
        "date_run": datetime.now().isoformat(),
        "description": "Volume-disposition effect: high vs low turnover during price rise",
        "universe_size": len(UNIVERSE),
        "symbols_with_data": len(data),
        "total_observations": len(df),
        "results_by_horizon": {
            str(k): {kk: round(float(vv), 6) for kk, vv in v.items()}
            for k, v in results.items()
        }
    }
    with open("logs/volume_disposition_results.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nResults saved to logs/volume_disposition_results.json")

    return df, results


if __name__ == "__main__":
    result = main()
