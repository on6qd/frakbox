"""
Post-Earnings Insider Buying Backtest
======================================
Hypothesis: When CEO/CFO buys >$100K within 5 trading days after stock drops >5%,
the stock outperforms SPY by >2% over the next 20 trading days.

Two-pass approach:
1. Extract CEO/CFO purchases >$100K from EDGAR (no price needed)
2. For each purchase, check if stock had >5% drop in prior 5 days via Tiingo/yfinance
3. Calculate abnormal returns for qualifying events

Discovery: 2020-2022 | OOS: 2023-2024
"""

import os
import sys
import pickle
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "sec_form4_cache")
PRICE_CACHE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                 "data", "post_earnings_price_cache.pkl")

TIINGO_API_KEY = os.environ.get("TIINGO_API_KEY", "0ecf1cc45d3a45ad1a93df4ee23bee1a6e4e97d3")

CEO_CFO_TITLES = [
    'chief executive', 'ceo', 'c.e.o',
    'chief financial', 'cfo', 'c.f.o',
    'president and ceo', 'president & ceo',
    'co-ceo', 'co-chief executive',
    'president/ceo', 'president, ceo',
]

MIN_PURCHASE_VALUE = 100_000
MAX_PURCHASE_VALUE = 200_000_000  # cap at $200M to remove data errors
DROP_THRESHOLD = -0.05
LOOKBACK_DAYS = 5
FORWARD_HORIZONS = [5, 10, 20]


def load_quarters(years):
    """Load EDGAR Form 4 data for specified years."""
    all_subs, all_trans, all_owners = [], [], []

    for year in years:
        for q in range(1, 5):
            path = os.path.join(CACHE_DIR, f"{year}q{q}_form345.pkl")
            if not os.path.exists(path):
                continue
            with open(path, 'rb') as f:
                d = pickle.load(f)
            all_subs.append(d['submissions'])
            all_trans.append(d['nonderiv_trans'])
            all_owners.append(d['reporting_owners'])

    return (pd.concat(all_subs, ignore_index=True),
            pd.concat(all_trans, ignore_index=True),
            pd.concat(all_owners, ignore_index=True))


def is_ceo_cfo(s):
    if pd.isna(s):
        return False
    s = str(s).lower()
    return any(k in s for k in CEO_CFO_TITLES)


def extract_ceo_cfo_purchases(subs, trans, owners):
    """Extract CEO/CFO open-market purchases above threshold."""
    buys = trans[
        (trans['TRANS_CODE'] == 'P') &
        (trans['TRANS_ACQUIRED_DISP_CD'] == 'A')
    ].copy()

    buys['shares'] = pd.to_numeric(buys['TRANS_SHARES'], errors='coerce')
    buys['price'] = pd.to_numeric(buys['TRANS_PRICEPERSHARE'], errors='coerce')
    buys['value'] = buys['shares'] * buys['price']
    buys = buys[(buys['value'] >= MIN_PURCHASE_VALUE) &
                (buys['value'] <= MAX_PURCHASE_VALUE)].copy()

    # Join submissions
    subs_slim = subs[['ACCESSION_NUMBER', 'ISSUERTRADINGSYMBOL', 'FILING_DATE']].copy()
    buys = buys.merge(subs_slim, on='ACCESSION_NUMBER', how='left')
    buys = buys[buys['ISSUERTRADINGSYMBOL'].notna() & (buys['ISSUERTRADINGSYMBOL'] != '')]

    # Join owners for title
    o_slim = owners[['ACCESSION_NUMBER', 'RPTOWNERNAME', 'RPTOWNER_TITLE']].copy()
    buys = buys.merge(o_slim, on='ACCESSION_NUMBER', how='left')

    # CEO/CFO filter
    buys['is_exec'] = buys['RPTOWNER_TITLE'].apply(is_ceo_cfo)
    buys = buys[buys['is_exec']].copy()

    # Parse dates
    buys['trans_date'] = pd.to_datetime(buys['TRANS_DATE'], format='%d-%b-%Y', errors='coerce')
    buys = buys.dropna(subset=['trans_date'])

    buys = buys.rename(columns={
        'ISSUERTRADINGSYMBOL': 'ticker',
        'RPTOWNERNAME': 'exec_name',
        'RPTOWNER_TITLE': 'exec_title'
    })

    # Keep max-value purchase per ticker per day
    buys = buys.sort_values('value', ascending=False).drop_duplicates(
        subset=['ticker', 'trans_date']
    )

    return buys[['ticker', 'trans_date', 'value', 'exec_name', 'exec_title']].reset_index(drop=True)


def tiingo_get_prices(ticker, start_date, end_date, max_retries=2):
    """Download adjusted close prices from Tiingo for a single ticker."""
    url = (f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
           f"?startDate={start_date}&endDate={end_date}&token={TIINGO_API_KEY}")

    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, headers={'Content-Type': 'application/json'}, timeout=15)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            if not data:
                return None

            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            df = df.set_index('date').sort_index()
            return df['adjClose']
        except Exception:
            if attempt < max_retries:
                time.sleep(2)

    return None


def load_price_cache():
    """Load or create price cache."""
    if os.path.exists(PRICE_CACHE_PATH):
        with open(PRICE_CACHE_PATH, 'rb') as f:
            return pickle.load(f)
    return {}


def save_price_cache(cache):
    os.makedirs(os.path.dirname(PRICE_CACHE_PATH), exist_ok=True)
    with open(PRICE_CACHE_PATH, 'wb') as f:
        pickle.dump(cache, f)


def get_prices(ticker, price_cache, start='2019-01-01', end='2025-01-01'):
    """Get prices for ticker, using cache and Tiingo fallback."""
    if ticker in price_cache:
        return price_cache[ticker]

    prices = tiingo_get_prices(ticker, start, end)
    if prices is not None and len(prices) > 20:
        price_cache[ticker] = prices
        return prices

    # Fallback to yfinance
    try:
        import yfinance as yf
        df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
        if df is not None and not df.empty and len(df) > 20:
            s = df['Close'].squeeze().dropna()
            price_cache[ticker] = s
            return s
    except Exception:
        pass

    price_cache[ticker] = None  # Mark as failed
    return None


def compute_prior_drop(ticker, purchase_date, price_cache, lookback=LOOKBACK_DAYS):
    """Check if stock dropped >5% in the lookback days before purchase_date."""
    prices = get_prices(ticker, price_cache)
    if prices is None:
        return None, None

    mask = prices.index <= pd.Timestamp(purchase_date)
    recent = prices[mask]

    if len(recent) < lookback + 1:
        return None, None

    window = recent.iloc[-(lookback + 1):]
    start_p = window.iloc[0]
    end_p = window.iloc[-1]

    if start_p <= 0:
        return None, None

    drop = (end_p - start_p) / start_p
    return drop <= DROP_THRESHOLD, drop


def compute_abnormal_return(ticker, entry_date, horizon, price_cache, spy_prices):
    """Compute abnormal return = stock return - SPY return over horizon days from entry_date."""
    prices = get_prices(ticker, price_cache)
    if prices is None or spy_prices is None:
        return None

    # Entry = first available close after entry_date
    stock_after = prices[prices.index >= pd.Timestamp(entry_date)]
    spy_after = spy_prices[spy_prices.index >= pd.Timestamp(entry_date)]

    if len(stock_after) < horizon + 1 or len(spy_after) < horizon + 1:
        return None

    entry_stock = stock_after.iloc[0]
    exit_stock = stock_after.iloc[horizon]
    entry_spy = spy_after.iloc[0]
    exit_spy = spy_after.iloc[horizon]

    if entry_stock <= 0 or entry_spy <= 0:
        return None

    stock_ret = (exit_stock - entry_stock) / entry_stock
    spy_ret = (exit_spy - entry_spy) / entry_spy
    return float(stock_ret - spy_ret)


def analyze_results(events_df, label=""):
    """Run statistics on events results."""
    from scipy import stats

    print(f"\n{'='*60}")
    print(f"Results: {label}")
    print(f"{'='*60}")

    for h in FORWARD_HORIZONS:
        col = f'ar_{h}d'
        if col not in events_df.columns:
            continue
        sub = events_df[col].dropna()
        if len(sub) == 0:
            continue
        t_stat, p_val = stats.ttest_1samp(sub, 0)
        direction = (sub > 0.005).mean()
        print(f"  {h}d: n={len(sub):3d} | mean={sub.mean()*100:+.2f}% | "
              f"median={sub.median()*100:+.2f}% | "
              f"direction={direction:.1%} | p={p_val:.4f} | t={t_stat:.2f}")

    return events_df


def main(quick_test=False):
    print("=== POST-CRASH CEO/CFO INSIDER BUY BACKTEST ===")
    print(f"Signal: CEO/CFO buys >$100K within 5 days after stock drops >5%")
    print(f"Test: Abnormal return over 5/10/20 days vs SPY")
    print()

    # Load EDGAR data
    print("Loading EDGAR Form 4 data (2020-2024)...")
    years = [2020, 2021, 2022] if quick_test else [2020, 2021, 2022, 2023, 2024]
    subs, trans, owners = load_quarters(years)
    print(f"  Loaded: {len(subs):,} submissions, {len(trans):,} transactions")

    # Extract CEO/CFO purchases
    print("\nExtracting CEO/CFO purchases >= $100K...")
    purchases = extract_ceo_cfo_purchases(subs, trans, owners)
    print(f"  Found: {len(purchases):,} CEO/CFO purchases")
    print(f"  Unique tickers: {purchases['ticker'].nunique()}")

    # Large-cap filter (current market caps — proxy filter)
    # Rather than hitting rate limits, use a reasonable proxy:
    # Remove tickers with obvious micro-cap patterns (< 4 letters usually bigger)
    # This is imperfect but avoids rate limit issues
    print("\n  Note: Using all CEO/CFO tickers (large-cap filter deferred to price check)")

    if quick_test:
        # Test with smaller sample
        purchases = purchases.sample(min(200, len(purchases)), random_state=42)
        print(f"  Quick test: using {len(purchases)} samples")

    # Load price cache
    print("\nLoading price cache...")
    price_cache = load_price_cache()
    cached_tickers = sum(1 for v in price_cache.values() if v is not None)
    print(f"  Cached: {cached_tickers} tickers ({len(price_cache)} total incl. failed)")

    # Get SPY prices
    print("  Loading SPY...")
    spy_prices = get_prices('SPY', price_cache)
    if spy_prices is None:
        print("ERROR: Could not get SPY prices!")
        return

    # Process each event: check prior drop, compute forward returns
    print(f"\nProcessing {len(purchases)} purchases...")
    events = []

    for idx, (_, row) in enumerate(purchases.iterrows()):
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{len(purchases)} | Events found: {len(events)}")
            if idx % 500 == 0 and idx > 0:
                save_price_cache(price_cache)

        ticker = row['ticker']
        purchase_date = row['trans_date']

        # Check prior drop
        dropped, prior_drop = compute_prior_drop(ticker, purchase_date, price_cache)
        if dropped is None or not dropped:
            continue

        # Compute forward abnormal returns
        entry_date = purchase_date + timedelta(days=1)
        row_result = {
            'ticker': ticker,
            'purchase_date': purchase_date,
            'prior_5d_drop': prior_drop,
            'value': row['value'],
            'exec_name': row['exec_name'],
            'exec_title': row['exec_title'],
        }

        for h in FORWARD_HORIZONS:
            ar = compute_abnormal_return(ticker, entry_date, h, price_cache, spy_prices)
            row_result[f'ar_{h}d'] = ar

        events.append(row_result)
        time.sleep(0.05)  # Rate limit Tiingo: ~20 req/sec max

    # Save cache after processing
    save_price_cache(price_cache)
    print(f"\nFound {len(events)} post-drop insider buy events")

    if len(events) == 0:
        print("No events found! Check data.")
        return

    events_df = pd.DataFrame(events)
    print(f"Date range: {events_df['purchase_date'].min().date()} to {events_df['purchase_date'].max().date()}")
    print(f"Drop range: {events_df['prior_5d_drop'].min()*100:.1f}% to {events_df['prior_5d_drop'].max()*100:.1f}%")
    print(f"Purchase value range: ${events_df['value'].min()/1e3:.0f}K to ${events_df['value'].max()/1e6:.1f}M")

    # --- FULL SAMPLE ---
    print("\n\n=== FULL SAMPLE ANALYSIS ===")
    analyze_results(events_df, "All years")

    # --- DISCOVERY VS OOS ---
    disc = events_df[events_df['purchase_date'] < '2023-01-01'].copy()
    oos = events_df[events_df['purchase_date'] >= '2023-01-01'].copy()
    print(f"\nDiscovery (2020-2022): n={len(disc)} events")
    print(f"OOS (2023-2024): n={len(oos)} events")

    if len(disc) >= 5:
        analyze_results(disc, "Discovery 2020-2022")
    if len(oos) >= 5:
        analyze_results(oos, "OOS 2023-2024")

    # --- BREAKDOWN BY DROP MAGNITUDE ---
    print("\n\n=== BREAKDOWN BY DROP MAGNITUDE ===")
    for thresh in [-0.07, -0.10, -0.15]:
        sub = events_df[events_df['prior_5d_drop'] <= thresh]
        if len(sub) >= 10:
            analyze_results(sub, f"Drop <= {thresh*100:.0f}%  (n={len(sub)})")

    # --- TOP EVENTS ---
    print("\n\n=== TOP 15 EVENTS BY PRIOR DROP ===")
    cols = ['ticker', 'purchase_date', 'prior_5d_drop', 'value', 'ar_5d', 'ar_20d', 'exec_title']
    available = [c for c in cols if c in events_df.columns]
    print(events_df.nsmallest(15, 'prior_5d_drop')[available].to_string())

    # Save events
    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "data", "post_crash_insider_events.csv")
    events_df.to_csv(out_path, index=False)
    print(f"\nEvents saved to {out_path}")

    return events_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--quick', action='store_true', help='Quick test with 200 samples')
    args = parser.parse_args()
    main(quick_test=args.quick)
