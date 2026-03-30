"""
Quick scan for first-ever 52w low crossings in the S&P 500 universe
during March 27-31, 2026 (tariff selloff period).
"""
import json
import warnings
from pathlib import Path
import sys
import yfinance as yf
import pandas as pd
from datetime import datetime

warnings.filterwarnings('ignore')
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    with open('data/sp500_universe.json') as f:
        data = json.load(f)
    tickers = data.get('tickers', [])
    print(f'Universe: {len(tickers)} tickers')

    end = '2026-04-01'
    start = '2024-04-01'

    print('Downloading 2yr price data...')
    prices = yf.download(tickers, start=start, end=end, progress=False)['Close']
    print(f'Got data for {len(prices.columns)} tickers, {len(prices)} days')

    cutoff = pd.Timestamp('2026-03-27')
    end_dt = pd.Timestamp('2026-03-31')

    first_ever_new = []
    borderline = []  # within 2% of 52w low but not yet crossed

    for ticker in prices.columns:
        s = prices[ticker].dropna()
        if len(s) < 200:
            continue

        # Get March 27-31 dates
        recent = s[(s.index >= cutoff) & (s.index <= end_dt)]
        if recent.empty:
            continue

        crossed = False
        for dt in recent.index:
            prior = s[s.index < dt].tail(252)
            if len(prior) < 100:
                continue
            prior_52w_low = prior.min()
            today_close = float(s[dt])

            if today_close < prior_52w_low:
                # Check if first ever (no prior crossing in 2yr data)
                # Simple check: was any previous day below its own rolling 52w min?
                is_first = True
                history = s[s.index < cutoff]
                if len(history) >= 252:
                    for i in range(252, len(history)):
                        window_min = float(history.iloc[i-252:i].min())
                        if float(history.iloc[i]) < window_min:
                            is_first = False
                            break

                if is_first:
                    first_ever_new.append({
                        'ticker': ticker,
                        'date': dt.strftime('%Y-%m-%d'),
                        'close': round(today_close, 2),
                        'prior_52w_low': round(float(prior_52w_low), 2),
                        'pct_below': round((today_close / prior_52w_low - 1) * 100, 2)
                    })
                    crossed = True
                    break

        if not crossed:
            # Check March 31 specifically for borderline
            last_dt = recent.index[-1]
            if last_dt.strftime('%Y-%m-%d') >= '2026-03-31':
                prior = s[s.index < last_dt].tail(252)
                if len(prior) >= 100:
                    prior_52w_low = prior.min()
                    today_close = float(s[last_dt])
                    pct_above = (today_close / prior_52w_low - 1) * 100
                    if 0 < pct_above < 3.0:
                        borderline.append({
                            'ticker': ticker,
                            'close': round(today_close, 2),
                            'barrier': round(float(prior_52w_low), 2),
                            'pct_above': round(pct_above, 2)
                        })

    print()
    print(f'=== FIRST-EVER 52W LOW CROSSINGS (March 27-31) ===')
    print(f'Found: {len(first_ever_new)}')
    for e in sorted(first_ever_new, key=lambda x: x['pct_below']):
        print(f'  {e["ticker"]:6s}  {e["date"]}  close=${e["close"]:8.2f}  52w_low=${e["prior_52w_low"]:.2f}  ({e["pct_below"]:+.1f}%)')

    print()
    print(f'=== BORDERLINE (0-3% above 52w low on March 31) ===')
    print(f'Found: {len(borderline)}')
    for e in sorted(borderline, key=lambda x: x['pct_above'])[:20]:
        print(f'  {e["ticker"]:6s}  close=${e["close"]:8.2f}  barrier=${e["barrier"]:.2f}  (+{e["pct_above"]:.1f}%)')


if __name__ == '__main__':
    main()
