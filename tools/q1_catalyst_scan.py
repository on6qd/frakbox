"""
Scan for large-cap S&P 500 stocks near 52w lows with upcoming Q1 2026 earnings
Candidates for sp500_52w_low_catalyst_short
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
from tools.yfinance_utils import safe_download

candidates = [
    'JPM', 'C', 'BAC', 'WFC', 'GS', 'MS',  # Banks April 11-15
    'JNJ', 'UNH', 'ABBV', 'MRK', 'BMY',    # Health/Pharma
    'XOM', 'CVX', 'COP', 'OXY', 'PSX',      # Energy
    'DE', 'CAT', 'MMM', 'HON', 'GE',        # Industrial
    'VZ', 'T', 'CMCSA',                       # Telecom
    'INTC', 'QCOM',                            # Semis
    'AAL', 'UAL', 'DAL',                      # Airlines
    'SYK', 'MDT', 'BSX', 'ABT',              # Medtech
    'PFE', 'BIIB', 'AMGN',                   # Biotech
    'D', 'SO', 'NEE', 'DUK', 'AES',         # Utilities
    'SBAC', 'AMT', 'CCI',                    # REITs
    'AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN', # Mega-cap tech
    'V', 'MA', 'BRK-B',                      # Financial/other
    'LMT', 'RTX', 'NOC', 'GD',              # Defense
    'WMT', 'COST', 'TGT', 'HD', 'LOW',      # Retail
    'F', 'GM', 'TSLA',                        # Auto
    'NEM', 'FCX',                             # Miners
    'KO', 'PEP', 'MCD', 'YUM', 'SBUX',     # Consumer
]

print("Scanning for 52w low proximity + upcoming earnings...")
print(f"{'Ticker':<6} {'Cap':>6} {'Curr':>8} {'%Above':>7} {'Xings':>6} {'Earnings':>15}")
print('-'*55)

first_touch_near = []
for sym in candidates:
    try:
        t = yf.Ticker(sym)
        info = t.info
        cap = info.get('marketCap', 0)
        if cap < 5e9:
            continue
        
        hist = t.history(period='2y')
        if len(hist) < 100:
            continue
        
        current = hist['Close'].iloc[-1]
        low_2y = hist['Close'].min()
        pct_above = (current / low_2y - 1) * 100
        
        # Only near lows (< 10% above)
        if pct_above > 10:
            continue
        
        # Count 52w low crossings
        hist['rolling_52w_min'] = hist['Close'].rolling(252).min()
        crossings = int((hist['Close'] < hist['rolling_52w_min'].shift(1)).sum())
        
        # Get earnings date
        earn_str = 'unknown'
        try:
            cal = t.calendar
            earn_dates = cal.get('Earnings Date', [])
            if isinstance(earn_dates, list) and earn_dates:
                earn_str = str(earn_dates[0])
        except:
            pass
        
        print(f"{sym:<6} {cap/1e9:>5.0f}B {current:>8.2f} {pct_above:>+6.1f}% {crossings:>6} {earn_str:>15}")
        
        if crossings <= 1:
            first_touch_near.append((sym, pct_above, earn_str, cap/1e9))
        
    except Exception as e:
        pass

print()
print("=== FIRST-TOUCH CANDIDATES (0-1 crossings, <10% above 52w low) ===")
first_touch_near.sort(key=lambda x: x[1])
for sym, pct, earn, cap in first_touch_near:
    print(f"  {sym}: {pct:+.1f}% above 52w low | earnings={earn} | cap=${cap:.0f}B")
