"""
Systemic 52w Low Scanner
========================
Checks whether today is a "systemic selloff" day that triggers the
sp500_52w_low_systemic_short signal (hypothesis f055dc19).

Signal fires when:
1. SPY daily return < -0.5%
2. >= 5 S&P 500 large-cap stocks simultaneously touch new 52-week lows (first touch)

Run this daily after market close to detect trigger conditions.

Usage:
    python tools/systemic_52w_low_scanner.py [--date YYYY-MM-DD]
    python tools/systemic_52w_low_scanner.py --check-today
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent))


def _load_full_universe():
    """Load full S&P 500 universe from data/sp500_universe.json, with fallback."""
    cache_file = Path(__file__).parent.parent / 'data' / 'sp500_universe.json'
    try:
        with open(cache_file) as f:
            data = json.load(f)
        tickers = data.get('tickers', [])
        if len(tickers) >= 200:
            return tickers
    except Exception:
        pass
    return None  # Fall back to UNIVERSE constant below


# Fallback hardcoded universe (used only if JSON file unavailable)
_FALLBACK_UNIVERSE = [
    # Consumer Discretionary
    'HD', 'MCD', 'NKE', 'AMZN', 'TSLA', 'TGT', 'DPZ', 'CMG', 'YUM', 'HLT', 'MAR', 'LVS',
    # Consumer Staples
    'PG', 'KO', 'PEP', 'WMT', 'COST', 'MDLZ', 'KHC', 'MKC', 'CAG', 'GIS', 'CPB', 'CLX', 'CL',
    # Healthcare
    'JNJ', 'UNH', 'ABT', 'TMO', 'MDT', 'BMY', 'MRK', 'PFE', 'ABBV', 'AMGN', 'GILD', 'CVS', 'BSX', 'SYK', 'ISRG',
    # Technology
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'AVGO', 'ADBE', 'ORCL', 'CRM', 'INTC', 'AMD', 'QCOM', 'TXN',
    # Financials
    'JPM', 'BAC', 'GS', 'MS', 'USB', 'RF', 'CFG', 'ZION', 'KEY', 'BK', 'STT',
    # Industrials
    'RTX', 'LMT', 'GD', 'NOC', 'BA', 'CAT', 'DE', 'GE', 'HON', 'MMM', 'OTIS', 'CARR',
    # Energy
    'XOM', 'CVX', 'COP', 'EOG', 'SLB',
    # Materials
    'NEM', 'FCX', 'LIN', 'APD',
    # Real Estate
    'AMT', 'CCI', 'SPG', 'O', 'WELL', 'EQR', 'SBAC', 'VNO',
    # Utilities
    'NEE', 'DUK', 'SO',
    # Communication Services
    'T', 'VZ', 'DIS', 'NFLX', 'TMUS',
    # Airlines/Transport
    'DAL', 'UAL', 'AAL', 'LUV', 'UPS', 'FDX',
    # Consumer Discretionary (more)
    'BAX', 'KIM', 'CCL', 'RCL', 'NCLH',
]

# Remove duplicates
_FALLBACK_UNIVERSE = list(dict.fromkeys(_FALLBACK_UNIVERSE))
# Use full universe if available, otherwise fallback
UNIVERSE = _load_full_universe() or _FALLBACK_UNIVERSE


def get_spy_return(date_str=None):
    """Get SPY return for a given date (or most recent trading day)."""
    end = datetime.strptime(date_str, '%Y-%m-%d') if date_str else datetime.now()
    start = end - timedelta(days=10)
    
    spy = yf.download('SPY', start=start, end=end + timedelta(days=1), 
                      auto_adjust=True, progress=False)
    if spy.empty:
        return None, None
    
    if isinstance(spy.columns, pd.MultiIndex):
        closes = spy['Close']['SPY'].dropna()
    else:
        closes = spy['Close'].dropna()
    
    if len(closes) < 2:
        return None, None
    
    last_date = closes.index[-1]
    spy_ret = (closes.iloc[-1] / closes.iloc[-2] - 1)
    return float(spy_ret), last_date.strftime('%Y-%m-%d')


def find_52w_low_first_touches(check_date_str=None, universe=None):
    """
    Find S&P 500 stocks that are making first-touch 52-week lows today.
    Returns list of tickers.
    """
    if universe is None:
        universe = UNIVERSE
    
    end = datetime.strptime(check_date_str, '%Y-%m-%d') if check_date_str else datetime.now()
    start = end - timedelta(days=275)  # 252 trading days + buffer
    
    first_touch_tickers = []
    
    for ticker in universe:
        try:
            hist = yf.download(ticker, start=start, end=end + timedelta(days=1),
                               auto_adjust=True, progress=False)
            if hist.empty:
                continue
            
            if isinstance(hist.columns, pd.MultiIndex):
                closes = hist['Close'][ticker].dropna()
            else:
                closes = hist['Close'].dropna()
            
            if len(closes) < 130:  # Need 252d history
                continue
            
            recent_252 = closes.tail(252)
            current = float(closes.iloc[-1])
            low_252 = float(recent_252.min())
            
            # Is today at or within 0.1% of the 52w low?
            if current > low_252 * 1.001:
                continue
            
            # Is yesterday above the 52w low by at least 0.5%? (first touch filter)
            if len(closes) >= 2:
                yesterday = float(closes.iloc[-2])
                # Get yesterday's rolling 252d low
                if len(closes) >= 253:
                    yesterday_low = float(closes.iloc[-253:-1].min())
                else:
                    yesterday_low = float(closes.iloc[:-1].min())
                
                if yesterday > yesterday_low * 1.005:
                    first_touch_tickers.append(ticker)
            
        except Exception:
            pass
    
    return first_touch_tickers


def scan(date_str=None, verbose=True):
    """
    Run the systemic selloff scanner.
    Returns dict with signal status.
    """
    if verbose:
        print("=" * 60)
        print("SYSTEMIC 52W LOW SCANNER")
        print(f"Hypothesis: f055dc19 (sp500_52w_low_systemic_short)")
        print("=" * 60)
    
    # Check SPY return
    spy_ret, spy_date = get_spy_return(date_str)
    if spy_ret is None:
        print("ERROR: Could not fetch SPY data")
        return {"signal": False, "error": "SPY data unavailable"}
    
    spy_pct = spy_ret * 100
    spy_threshold = -0.5
    spy_ok = spy_ret < spy_threshold / 100
    
    if verbose:
        print(f"\nSPY return ({spy_date}): {spy_pct:.2f}%  (threshold: <{spy_threshold}%)")
        print(f"  SPY condition: {'✓ PASS' if spy_ok else '✗ FAIL'}")
    
    if not spy_ok:
        if verbose:
            print(f"\nSIGNAL: NOT TRIGGERED")
            print(f"  SPY only down {spy_pct:.2f}% (need <{spy_threshold}%)")
        return {
            "signal": False,
            "spy_return_pct": spy_pct,
            "spy_date": spy_date,
            "n_stocks_at_low": 0,
            "stocks_at_low": [],
            "reason": f"SPY down only {spy_pct:.2f}% (need <-0.5%)"
        }
    
    # Scan for 52w low first touches
    if verbose:
        print(f"\nScanning {len(UNIVERSE)} S&P 500 stocks for first-touch 52w lows...")
    
    first_touch = find_52w_low_first_touches(date_str, UNIVERSE)
    n_at_low = len(first_touch)
    n_threshold = 5
    stocks_ok = n_at_low >= n_threshold
    
    if verbose:
        print(f"  Stocks at 52w low (first touch): {n_at_low}  (threshold: >={n_threshold})")
        if first_touch:
            print(f"  Stocks: {', '.join(first_touch[:20])}")
        print(f"  Stock count condition: {'✓ PASS' if stocks_ok else '✗ FAIL'}")
    
    signal_fires = spy_ok and stocks_ok
    
    if verbose:
        print(f"\n{'='*60}")
        if signal_fires:
            print(f"🚨 SIGNAL FIRES: sp500_52w_low_systemic_short")
            print(f"  - SPY down {spy_pct:.2f}% ✓")
            print(f"  - {n_at_low} stocks at 52w lows ✓")
            print(f"\nACTION: Short these stocks at TOMORROW'S OPEN:")
            for t in first_touch:
                print(f"  python tools/activate_systemic_short.py --ticker {t} --yes")
            print(f"\nExpected 5d abnormal return: -1.88% each")
            print(f"NOTE: Check portfolio capacity (max 5 positions) before activating!")
        else:
            print(f"Signal NOT triggered today.")
            print(f"  SPY: {spy_pct:.2f}% {'✓' if spy_ok else '✗'}")
            print(f"  Stocks at 52w low: {n_at_low} {'✓' if stocks_ok else '✗'}")
        print("=" * 60)
    
    return {
        "signal": signal_fires,
        "spy_return_pct": spy_pct,
        "spy_date": spy_date,
        "n_stocks_at_low": n_at_low,
        "stocks_at_low": first_touch,
        "spy_condition_met": spy_ok,
        "stocks_condition_met": stocks_ok
    }


def main():
    parser = argparse.ArgumentParser(description='Scan for systemic 52w low signal')
    parser.add_argument('--date', type=str, default=None,
                        help='Date to check (YYYY-MM-DD, default: most recent trading day)')
    parser.add_argument('--check-today', action='store_true',
                        help='Check today (same as no args)')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress verbose output, just print signal status')
    args = parser.parse_args()
    
    result = scan(date_str=args.date, verbose=not args.quiet)
    
    if args.quiet:
        if result.get('signal'):
            print(f"SIGNAL FIRES: SPY={result['spy_return_pct']:.1f}%, n_lows={result['n_stocks_at_low']}, tickers={result['stocks_at_low']}")
        else:
            print(f"No signal: SPY={result.get('spy_return_pct', 'N/A'):.1f}%, n_lows={result.get('n_stocks_at_low', 0)}")
    
    return 0 if not result.get('error') else 1


if __name__ == '__main__':
    sys.exit(main())
