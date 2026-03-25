"""
Scan for S&P 500 stocks near 52-week lows that have earnings in April/May 2026.
These are candidates for sp500_52w_low_catalyst_short (hypothesis 99008c7a).

Usage: python tools/april_catalyst_scan.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.yfinance_utils import safe_download
from tools.build_sp500_universe import load_sp500_universe
import yfinance as yf
import json

# Already being traded/watched - skip
ACTIVE_OR_WATCHED = {
    'MKC', 'NKE', 'CAG', 'SYK', 'ABT', 'BAX', 'HD', 'GO', 'CTAS', 'KHC', 'V'
}

def scan_near_lows_with_earnings(pct_threshold=5.0, earnings_months=('2026-04', '2026-05')):
    tickers = load_sp500_universe()
    print(f"Scanning {len(tickers)} S&P 500 tickers for stocks near 52w low + April/May earnings...")

    near_low_stocks = []

    for ticker in tickers:
        if ticker in ACTIVE_OR_WATCHED:
            continue
        try:
            hist = yf.Ticker(ticker).history(period='1y')
            if hist is None or len(hist) < 200:
                continue
            close = hist['Close'].dropna()
            if len(close) < 200:
                continue
            current = float(close.iloc[-1])
            low_52w = float(close.min())
            pct_above_low = (current - low_52w) / low_52w * 100

            if pct_above_low <= pct_threshold:
                near_low_stocks.append({
                    'ticker': ticker,
                    'current': round(current, 2),
                    '52w_low': round(low_52w, 2),
                    'pct_above_low': round(pct_above_low, 2)
                })
        except Exception as e:
            pass  # Skip failures silently

    print(f"\nFound {len(near_low_stocks)} stocks within {pct_threshold}% of 52w low:")
    near_low_stocks.sort(key=lambda x: x['pct_above_low'])
    for s in near_low_stocks:
        print(f"  {s['ticker']}: ${s['current']} (+{s['pct_above_low']:.1f}% above 52w low ${s['52w_low']})")

    print(f"\nChecking earnings dates for {len(near_low_stocks)} stocks...")
    april_candidates = []
    for s in near_low_stocks:
        try:
            t = yf.Ticker(s['ticker'])
            info = t.info

            # Try earningsDate
            ed = info.get('earningsDate', None)
            if ed:
                import datetime
                if isinstance(ed, (int, float)):
                    ed_str = datetime.datetime.fromtimestamp(ed).strftime('%Y-%m-%d')
                elif isinstance(ed, list):
                    ed_str = str(ed[0]) if ed else ''
                else:
                    ed_str = str(ed)

                s['earnings_date'] = ed_str[:10]
                if any(m in ed_str for m in earnings_months):
                    april_candidates.append(s)
        except:
            pass

    print(f"\n=== APRIL/MAY 2026 CATALYST SHORT CANDIDATES ===")
    print(f"(Near 52w low AND earnings in April or May)")
    if april_candidates:
        for s in sorted(april_candidates, key=lambda x: x.get('earnings_date', '')):
            print(f"  {s['ticker']}: earnings={s.get('earnings_date','?')} | "
                  f"${s['current']} (+{s['pct_above_low']:.1f}% above 52w low ${s['52w_low']})")
    else:
        print("  None found (excluding already-watched stocks)")

    return april_candidates


if __name__ == '__main__':
    candidates = scan_near_lows_with_earnings()
