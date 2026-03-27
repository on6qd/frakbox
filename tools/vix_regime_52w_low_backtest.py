"""
VIX Regime Filter for 52w Low Momentum Short
==============================================
Tests whether the 52w low momentum short signal's performance varies by VIX regime.

Hypothesis: The short signal only works in moderate VIX (20-30). Fails in:
  - Low VIX (<20): bull market, stocks recover from 52w lows
  - High VIX (>30): capitulation, too volatile, potential for reversal

Dataset: S&P 500 large-cap stocks, first-ever 52w low crossings 2018-2024 (discovery)
Benchmark: SPY returns over same hold period
Output: Returns by VIX regime with stats
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from scipy import stats
import yfinance as yf
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

from tools.yfinance_utils import safe_download, get_close_prices


def get_sp500_tickers():
    """Get S&P 500 tickers from Wikipedia."""
    try:
        import requests
        from bs4 import BeautifulSoup
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        tickers = []
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if cells:
                ticker = cells[0].text.strip().replace('.', '-')
                tickers.append(ticker)
        return tickers[:200]  # Limit for speed
    except Exception as e:
        print(f"Wikipedia failed: {e}, using fallback list")
        # Fallback: large-cap S&P 500 names
        return [
            'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NVDA', 'BRK-B', 'JPM',
            'UNH', 'V', 'XOM', 'LLY', 'JNJ', 'MA', 'PG', 'AVGO', 'HD', 'CVX',
            'MRK', 'ABBV', 'KO', 'PEP', 'COST', 'WMT', 'MCD', 'AMD', 'CSCO',
            'ADBE', 'ACN', 'ABT', 'TXN', 'CRM', 'NEE', 'LIN', 'DHR', 'VZ',
            'NFLX', 'PM', 'TMO', 'ORCL', 'INTC', 'IBM', 'GE', 'RTX', 'CAT',
            'GS', 'UNP', 'HON', 'BA', 'LOW', 'SPGI', 'BLK', 'MS', 'AXP',
            'BKNG', 'AMGN', 'SYK', 'GILD', 'TJX', 'T', 'C', 'DE', 'ELV',
            'SBUX', 'MDT', 'ISRG', 'PLD', 'MU', 'AMAT', 'BMY', 'CI', 'USB',
            'NKE', 'LRCX', 'KLAC', 'REGN', 'PANW', 'AMT', 'EOG', 'TGT', 'CVS',
            'SLB', 'FCX', 'EMR', 'MMC', 'MCO', 'HUM', 'ADP', 'F', 'GM',
            'BAC', 'WFC', 'PNC', 'TFC', 'KEY', 'RF', 'CFG', 'FITB', 'HBAN',
            'D', 'DUK', 'SO', 'AEP', 'XEL', 'PCG', 'EXC',
            'WM', 'RSG', 'GWW', 'ITW', 'PH', 'ETN', 'ROK', 'EMN', 'FMC',
            'UPS', 'FDX', 'ODFL', 'JBHT', 'DAL', 'UAL', 'LUV', 'AAL',
            'MAR', 'HLT', 'MGM', 'WYNN', 'CCL', 'RCL', 'NCLH',
            'MO', 'MDLZ', 'KHC', 'CPB', 'K', 'SJM', 'HRL', 'MKC', 'CAG',
            'PFE', 'MRNA', 'BIIB', 'VRTX', 'ILMN', 'ZBH', 'BSX',
            'AIG', 'PRU', 'MET', 'AFL', 'ALL', 'TRV', 'HIG', 'CB', 'CINF',
        ]


def find_first_touch_52w_lows(prices, min_days_above=30):
    """Find first-ever 52w low crossings for a price series."""
    events = []
    n = len(prices)

    for i in range(252, n):  # Need at least 252 days for 52w low
        today_close = prices.iloc[i]
        window_52w = prices.iloc[i-252:i]
        low_52w = window_52w.min()

        if today_close < low_52w:
            # Check if this is a first-ever crossing (no crossing in prior 252 trading days)
            # "First ever" means no prior 52w low crossing in lookback
            prior_breach = False

            # Look back up to 252 days - was there a prior 52w low crossing?
            for j in range(max(0, i-252), i):
                if j < 252:
                    continue
                prev_close = prices.iloc[j]
                prev_window = prices.iloc[j-252:j]
                prev_52w_low = prev_window.min()
                if prev_close < prev_52w_low:
                    prior_breach = True
                    break

            if not prior_breach:
                events.append({
                    'date': prices.index[i],
                    'close': float(today_close),
                    'low_52w': float(low_52w)
                })
                # Skip forward to avoid consecutive-day duplicates
                i_skip = min(i + 10, n - 1)

    return events


def get_vix_data(start, end):
    """Get VIX daily closes."""
    try:
        vix = yf.download('^VIX', start=start, end=end + timedelta(days=5),
                         auto_adjust=True, progress=False)
        if vix.empty:
            return pd.Series(dtype=float)
        if isinstance(vix.columns, pd.MultiIndex):
            return vix['Close'].iloc[:, 0].dropna()
        return vix['Close'].dropna()
    except:
        return pd.Series(dtype=float)


def compute_5d_return(prices, signal_date, direction='short'):
    """Compute 5-day abnormal return (entry at next open proxy)."""
    try:
        idx = prices.index.get_loc(signal_date)
    except KeyError:
        # Try to find nearest date
        date_idx = prices.index.searchsorted(signal_date)
        if date_idx >= len(prices):
            return None
        idx = date_idx

    # Entry: day after signal (open proxy = next close)
    entry_idx = idx + 1
    exit_idx = idx + 6  # 5 trading days

    if entry_idx >= len(prices) or exit_idx >= len(prices):
        return None

    entry_price = float(prices.iloc[entry_idx])
    exit_price = float(prices.iloc[exit_idx])

    raw_return = (exit_price - entry_price) / entry_price

    if direction == 'short':
        return -raw_return  # Short profit = negative raw return
    return raw_return


def main():
    print("=" * 70)
    print("VIX REGIME FILTER: 52w Low Momentum Short Backtest")
    print("Discovery period: 2018-2024 (OOS: 2025+)")
    print("=" * 70)
    print()

    # Date range for discovery
    START = datetime(2018, 1, 1)
    END = datetime(2024, 12, 31)

    # Get VIX data
    print("Loading VIX data...")
    vix_data = get_vix_data(START, END)
    print(f"VIX data: {len(vix_data)} days from {vix_data.index[0].date()} to {vix_data.index[-1].date()}")
    print()

    # Get SPY data for benchmark
    print("Loading SPY benchmark...")
    spy_raw = safe_download('SPY', START, END + timedelta(days=5))
    if spy_raw is None or spy_raw.empty:
        print("ERROR: Cannot load SPY data")
        return
    spy_prices = spy_raw['Close'] if not isinstance(spy_raw.columns, pd.MultiIndex) else spy_raw['Close']['SPY']
    spy_prices = spy_prices.dropna()
    print(f"SPY: {len(spy_prices)} days")
    print()

    # Get tickers
    tickers = get_sp500_tickers()
    print(f"Testing {len(tickers)} tickers...")
    print()

    all_events = []
    n_tickers_processed = 0
    n_tickers_failed = 0

    for ticker in tickers:
        try:
            raw = safe_download(ticker, START, END + timedelta(days=10))
            if raw is None or raw.empty or len(raw) < 300:
                n_tickers_failed += 1
                continue

            if isinstance(raw.columns, pd.MultiIndex):
                prices = raw['Close'][ticker].dropna() if ticker in raw['Close'].columns else raw['Close'].iloc[:, 0].dropna()
            else:
                prices = raw['Close'].dropna()

            if len(prices) < 300:
                n_tickers_failed += 1
                continue

            # Find first-touch 52w low events
            events = find_first_touch_52w_lows(prices)

            for event in events:
                signal_date = event['date']

                # Get VIX on signal day
                vix_dates = vix_data.index
                closest_vix_idx = vix_dates.searchsorted(signal_date)
                if closest_vix_idx >= len(vix_dates):
                    continue
                vix_on_day = float(vix_data.iloc[closest_vix_idx])

                # Compute stock 5d return
                stock_return = compute_5d_return(prices, signal_date, direction='short')
                if stock_return is None:
                    continue

                # Compute SPY 5d return (for benchmark)
                spy_return = compute_5d_return(spy_prices, signal_date, direction='long')
                if spy_return is None:
                    continue

                # Abnormal return = stock short return - spy short return
                # Short profits when stock falls more than SPY
                abnormal = stock_return - (-spy_return)  # stock short - spy short = stock_down - spy_down
                # More precisely: abnormal short return = -(stock return) - -(spy return)
                # = spy_raw_return - stock_raw_return
                # But we already computed as short return = -raw_return
                # Abnormal = short_stock - short_spy = (-stock_raw) - (-spy_raw) = spy_raw - stock_raw
                # Let me redo this clearly:

                # stock raw return over 5d
                try:
                    idx = prices.index.get_loc(signal_date)
                    entry_idx, exit_idx = idx + 1, idx + 6
                    if exit_idx >= len(prices):
                        continue
                    stock_raw = (float(prices.iloc[exit_idx]) - float(prices.iloc[entry_idx])) / float(prices.iloc[entry_idx])

                    spy_idx = spy_prices.index.searchsorted(prices.index[entry_idx])
                    if spy_idx + 5 >= len(spy_prices):
                        continue
                    spy_raw = (float(spy_prices.iloc[spy_idx + 5]) - float(spy_prices.iloc[spy_idx])) / float(spy_prices.iloc[spy_idx])

                    # Short abnormal return = -(stock_raw - spy_raw) = spy_raw - stock_raw
                    # Positive means stock underperformed (good for short)
                    abnormal_short = spy_raw - stock_raw

                    all_events.append({
                        'ticker': ticker,
                        'date': signal_date,
                        'vix': vix_on_day,
                        'stock_raw': stock_raw,
                        'spy_raw': spy_raw,
                        'abnormal_short': abnormal_short,
                    })
                except (KeyError, IndexError):
                    continue

            n_tickers_processed += 1
            if n_tickers_processed % 20 == 0:
                print(f"  Processed {n_tickers_processed}/{len(tickers)} tickers, {len(all_events)} events found...")

        except Exception as e:
            n_tickers_failed += 1
            continue

    print(f"\nTotal events: {len(all_events)}")
    print(f"Tickers processed: {n_tickers_processed}, failed: {n_tickers_failed}")

    if not all_events:
        print("No events found!")
        return

    df = pd.DataFrame(all_events)
    df = df.sort_values('date')

    print(f"\nDate range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"VIX range: {df['vix'].min():.1f} to {df['vix'].max():.1f}")
    print()

    # ---- REGIME ANALYSIS ----
    def analyze_regime(subset, label):
        if len(subset) < 5:
            print(f"{label}: n={len(subset)} (too few)")
            return

        returns = subset['abnormal_short'].values
        mean_ret = np.mean(returns) * 100
        median_ret = np.median(returns) * 100
        direction = (returns > 0.005).mean() * 100  # >0.5% threshold

        t_stat, p_val = stats.ttest_1samp(returns, 0)

        print(f"{label}: n={len(subset):3d} | mean={mean_ret:+.2f}% | median={median_ret:+.2f}% | dir={direction:.0f}% | p={p_val:.4f} {'**' if p_val<0.05 else ''}")

    print("=" * 70)
    print("REGIME ANALYSIS (Abnormal Short Returns, 5-day hold)")
    print("=" * 70)

    # Overall
    analyze_regime(df, "ALL REGIMES   ")
    print()

    # VIX regimes
    print("By VIX level at signal:")
    analyze_regime(df[df['vix'] < 15], "VIX < 15     ")
    analyze_regime(df[(df['vix'] >= 15) & (df['vix'] < 20)], "VIX 15-20    ")
    analyze_regime(df[(df['vix'] >= 20) & (df['vix'] < 25)], "VIX 20-25    ")
    analyze_regime(df[(df['vix'] >= 25) & (df['vix'] < 30)], "VIX 25-30    ")
    analyze_regime(df[(df['vix'] >= 30) & (df['vix'] < 35)], "VIX 30-35    ")
    analyze_regime(df[df['vix'] >= 35], "VIX >= 35    ")
    print()

    # Key thresholds
    print("Key thresholds:")
    analyze_regime(df[df['vix'] < 20], "VIX < 20 (bull)")
    analyze_regime(df[(df['vix'] >= 20) & (df['vix'] < 30)], "VIX 20-30 (moderate)")
    analyze_regime(df[df['vix'] >= 20], "VIX >= 20    ")
    analyze_regime(df[df['vix'] >= 25], "VIX >= 25    ")
    analyze_regime(df[df['vix'] >= 30], "VIX >= 30    ")
    print()

    # Calendar year breakdown
    print("By year:")
    for year in sorted(df['date'].dt.year.unique()):
        subset = df[df['date'].dt.year == year]
        analyze_regime(subset, f"  {year}          ")
    print()

    # Monthly VIX distribution
    print("VIX distribution of all events:")
    bins = [0, 15, 20, 25, 30, 35, 40, 100]
    labels = ['<15', '15-20', '20-25', '25-30', '30-35', '35-40', '40+']
    df['vix_bin'] = pd.cut(df['vix'], bins=bins, labels=labels)
    print(df['vix_bin'].value_counts().sort_index().to_string())
    print()

    # Regime interaction: VIX>=25 AND SPY trend
    print("Interaction: VIX>=25 segmented by SPY trend:")
    high_vix = df[df['vix'] >= 25]
    analyze_regime(high_vix[high_vix['spy_raw'] < -0.01], "VIX>=25 + SPY<-1%")
    analyze_regime(high_vix[high_vix['spy_raw'] >= -0.01], "VIX>=25 + SPY>=-1%")

    print()
    print("=" * 70)
    print("CONCLUSION:")

    # Key regime comparison
    bull = df[df['vix'] < 20]['abnormal_short']
    moderate = df[(df['vix'] >= 20) & (df['vix'] < 30)]['abnormal_short']
    high = df[df['vix'] >= 30]['abnormal_short']

    print(f"Bull market (VIX<20): {bull.mean()*100:+.2f}% avg, {(bull>0.005).mean()*100:.0f}% directional, n={len(bull)}")
    print(f"Moderate (VIX 20-30): {moderate.mean()*100:+.2f}% avg, {(moderate>0.005).mean()*100:.0f}% directional, n={len(moderate)}")
    print(f"High fear (VIX>=30):  {high.mean()*100:+.2f}% avg, {(high>0.005).mean()*100:.0f}% directional, n={len(high)}")

    # Save results
    df.to_csv('/tmp/vix_regime_52w_low_results.csv', index=False)
    print(f"\nFull results saved to /tmp/vix_regime_52w_low_results.csv")


if __name__ == '__main__':
    main()
