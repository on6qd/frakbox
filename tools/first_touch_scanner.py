#!/usr/bin/env python3
"""
First-Touch 52-Week Low Scanner (Full S&P 500, Batch Download)

Scans S&P 500 stocks for those approaching or just crossing their 52-week low
for the FIRST TIME in 2 years. Uses yf.download() in batches for speed.

Logic:
  - For each trading day, check if close < prior day's rolling 252-day min
  - Debounce clusters within 45 days as a single event
  - Count distinct crossing events over the 2-year window
  - Filter: 0 or 1 crossings, within 5% of current 52w low, market cap > $500M

Usage:
    python3 tools/first_touch_scanner.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import yfinance as yf
from datetime import date, timedelta
import warnings
warnings.filterwarnings('ignore')


# ── Configuration ────────────────────────────────────────────────────────────
LOOKBACK_DAYS   = 730          # 2 years of price history
WINDOW          = 252          # trading days in 52w
DEBOUNCE_DAYS   = 45           # cluster nearby crossings as one event
MAX_PCT_ABOVE   = 5.0          # must be within 5% of 52w low
MIN_MARKET_CAP  = 500e6        # $500M
BATCH_SIZE      = 50           # tickers per yf.download() call
TOP_N           = 20           # rows to print


def get_sp500_tickers() -> pd.DataFrame:
    """
    Get S&P 500 tickers from multiple sources (with fallbacks):
      1. Wikipedia (with proper User-Agent header)
      2. Local cached universe (build_sp500_universe.py)
    Returns DataFrame with columns: ticker, name
    """
    # Try Wikipedia with a browser-like header
    try:
        import requests
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        import io
        tables = pd.read_html(io.StringIO(resp.text))
        df = tables[0][['Symbol', 'Security']].copy()
        df.columns = ['ticker', 'name']
        df['ticker'] = df['ticker'].str.replace('.', '-', regex=False)
        print(f"  Loaded {len(df)} tickers from Wikipedia.")
        return df
    except Exception as e:
        err_short = str(e)[:100]  # Truncate to avoid HTML dump in logs
        print(f"  Wikipedia unavailable ({err_short}), falling back to local cache...")

    # Fallback: local static cache (411 tickers, no names)
    from tools.build_sp500_universe import load_sp500_universe
    tickers = load_sp500_universe()
    df = pd.DataFrame({'ticker': tickers, 'name': tickers})  # use ticker as name placeholder
    print(f"  Loaded {len(df)} tickers from local cache.")
    return df


def download_batch(tickers: list, start: str, end: str) -> pd.DataFrame:
    """Download closing prices for a batch of tickers. Returns wide DataFrame."""
    data = yf.download(
        tickers,
        start=start,
        end=end,
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    if isinstance(data.columns, pd.MultiIndex):
        # Multi-ticker: columns are (field, ticker)
        if 'Close' in data.columns.get_level_values(0):
            return data['Close']
        return pd.DataFrame()
    else:
        # Single ticker
        if 'Close' in data.columns:
            return data[['Close']].rename(columns={'Close': tickers[0]})
        return pd.DataFrame()


def count_crossings(series: pd.Series, debounce_days: int = DEBOUNCE_DAYS) -> dict:
    """
    Count distinct 52w-low crossing events for a price series.

    A crossing is: close < prior day's rolling 252-day minimum.
    Adjacent crossings within debounce_days are clustered into one event.

    Returns dict with:
      crossing_count, first_crossing_date, last_crossing_date,
      current_52w_low, current_close, pct_above_52w_low
    """
    series = series.dropna()
    if len(series) < WINDOW + 2:
        return None

    rolling_min = series.rolling(WINDOW, min_periods=WINDOW).min()
    prev_min    = rolling_min.shift(1)

    # Crossing: today's close dips below yesterday's rolling 252d min
    crossed = series < prev_min
    cross_dates = series.index[crossed].tolist()

    # Debounce
    if cross_dates:
        clusters = []
        cluster_start = cross_dates[0]
        for i in range(1, len(cross_dates)):
            gap = (cross_dates[i] - cross_dates[i - 1]).days
            if gap > debounce_days:
                clusters.append(cluster_start)
                cluster_start = cross_dates[i]
        clusters.append(cluster_start)
        n_events = len(clusters)
        first_dt = clusters[0].date()
        last_dt  = clusters[-1].date()
    else:
        n_events = 0
        first_dt = None
        last_dt  = None

    current_close   = float(series.iloc[-1])
    current_52w_low = float(rolling_min.iloc[-1])

    if pd.isna(current_52w_low) or current_52w_low <= 0:
        return None

    pct_above = (current_close / current_52w_low - 1) * 100.0

    return {
        'crossing_count':      n_events,
        'first_crossing_date': str(first_dt) if first_dt else None,
        'last_crossing_date':  str(last_dt)  if last_dt  else None,
        'current_close':       round(current_close, 2),
        'current_52w_low':     round(current_52w_low, 2),
        'pct_above_52w_low':   round(pct_above, 2),
    }


def get_market_caps(tickers: list) -> dict:
    """Fetch market caps in bulk using individual Ticker.fast_info."""
    caps = {}
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).fast_info
            mc = getattr(info, 'market_cap', None)
            caps[ticker] = float(mc) if mc else None
        except Exception:
            caps[ticker] = None
    return caps


def run_scan() -> pd.DataFrame:
    print("Fetching S&P 500 constituents from Wikipedia...")
    sp500_df = get_sp500_tickers()
    tickers  = sp500_df['ticker'].tolist()
    name_map = dict(zip(sp500_df['ticker'], sp500_df['name']))
    print(f"  {len(tickers)} tickers loaded.")

    # Date range: 2 years + 60 extra days buffer for rolling window warmup
    end_dt   = date.today() + timedelta(days=1)
    start_dt = date.today() - timedelta(days=LOOKBACK_DAYS + 60)
    start_str = str(start_dt)
    end_str   = str(end_dt)

    # ── Batch price download ────────────────────────────────────────────────
    all_closes = {}
    n_batches  = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nDownloading price history in {n_batches} batches of {BATCH_SIZE}...")

    for i in range(0, len(tickers), BATCH_SIZE):
        batch  = tickers[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{n_batches}: {batch[0]} … {batch[-1]}", end='\r', flush=True)

        df = download_batch(batch, start_str, end_str)
        if df.empty:
            continue
        for col in df.columns:
            all_closes[col] = df[col]

    print(f"\n  Downloaded data for {len(all_closes)} tickers.")

    # ── Analyse each ticker ────────────────────────────────────────────────
    print("\nAnalysing crossings...")
    rows = []
    for ticker in tickers:
        if ticker not in all_closes:
            continue
        result = count_crossings(all_closes[ticker])
        if result is None:
            continue

        # Apply filters:
        #   - 0 or 1 distinct crossing events
        #   - within MAX_PCT_ABOVE of 52w low
        n = result['crossing_count']
        pct = result['pct_above_52w_low']

        if n > 1:
            continue
        if pct > MAX_PCT_ABOVE:
            continue

        rows.append({
            'ticker':              ticker,
            'name':                name_map.get(ticker, ''),
            'crossing_count':      n,
            'first_crossing_date': result['first_crossing_date'],
            'last_crossing_date':  result['last_crossing_date'],
            'current_close':       result['current_close'],
            'current_52w_low':     result['current_52w_low'],
            'pct_above_52w_low':   pct,
            'market_cap':          None,   # filled below
        })

    print(f"  {len(rows)} candidates before market cap filter.")

    if not rows:
        print("No candidates found.")
        return pd.DataFrame()

    # ── Market cap filter ──────────────────────────────────────────────────
    candidate_tickers = [r['ticker'] for r in rows]
    print(f"\nFetching market caps for {len(candidate_tickers)} candidates...")
    caps = get_market_caps(candidate_tickers)

    filtered = []
    for r in rows:
        mc = caps.get(r['ticker'])
        r['market_cap'] = mc
        if mc is None or mc < MIN_MARKET_CAP:
            continue
        filtered.append(r)

    # ── Build output DataFrame ─────────────────────────────────────────────
    result_df = pd.DataFrame(filtered)
    if result_df.empty:
        print("No candidates passed market cap filter.")
        return result_df

    result_df = result_df.sort_values('pct_above_52w_low').reset_index(drop=True)
    result_df['market_cap_b'] = (result_df['market_cap'] / 1e9).round(2)
    result_df['status'] = result_df['crossing_count'].map({
        0: 'approaching (never crossed)',
        1: 'first touch',
    })

    return result_df


def print_results(df: pd.DataFrame, top_n: int = TOP_N, show_all: bool = False):
    if df.empty:
        print("No results.")
        return

    print(f"\n{'='*110}")
    print(f"  FIRST-TOUCH 52W LOW SCANNER — top {min(top_n, len(df))} of {len(df)} candidates  "
          f"(filter: ≤1 crossing, within {MAX_PCT_ABOVE}%, mktcap >${MIN_MARKET_CAP/1e6:.0f}M)")
    print(f"{'='*110}")
    header = (
        f"  {'#':>3}  {'Ticker':<7}  {'Company':<35}  {'Price':>7}  "
        f"{'52wLow':>7}  {'%Above':>7}  {'Crossings':>9}  "
        f"{'Last Cross':>12}  {'MktCap$B':>9}  Status"
    )
    print(header)
    print(f"  {'-'*105}")

    for i, row in df.head(top_n).iterrows():
        last_x = row['last_crossing_date'] or '—'
        print(
            f"  {i+1:>3}  {row['ticker']:<7}  {row['name'][:35]:<35}  "
            f"{row['current_close']:>7.2f}  {row['current_52w_low']:>7.2f}  "
            f"{row['pct_above_52w_low']:>6.2f}%  {row['crossing_count']:>9}  "
            f"{last_x:>12}  {row['market_cap_b']:>9.1f}  {row['status']}"
        )

    print(f"{'='*110}")

    # Summary breakdown
    n0 = (df['crossing_count'] == 0).sum()
    n1 = (df['crossing_count'] == 1).sum()
    print(f"\n  Summary: {n0} approaching (never crossed in 2y) | {n1} first-touch events")
    print(f"  Scan date: {date.today()}\n")


if __name__ == '__main__':
    results = run_scan()
    print_results(results)
