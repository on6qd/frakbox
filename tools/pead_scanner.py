"""
PEAD Scanner: Find upcoming S&P 500 earnings with high beat potential
========================================================================
Scans for stocks approaching earnings that have historically beaten by >=10%.
When an earnings beat fires, updates the PEAD hypothesis symbol and queues a trade.

Usage:
  python3 tools/pead_scanner.py          # Scan upcoming earnings
  python3 tools/pead_scanner.py --check  # Check recent beats (for trade activation)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd

PEAD_HYPOTHESIS_ID = '9e2a03ac'
MIN_SURPRISE_PCT = 10.0  # >=10% EPS surprise to qualify

SP500_TICKERS = [
    'AAPL','MSFT','AMZN','GOOGL','META','NVDA','BRK.B','JPM','V','UNH',
    'HD','PG','MA','AVGO','CVX','LLY','PFE','ABBV','KO','MRK','PEP','TMO',
    'COST','BAC','MCD','ACN','CSCO','NKE','WMT','IBM','XOM','VZ','T','GS',
    'MS','C','WFC','USB','AXP','BLK','SPGI','CME','ICE','COF','TRV','MMC',
    'AON','MET','PRU','AFL','ALL','CB','HIG','LNC','UNM','SYF','SYK',
    'ABT','DHR','MDT','BSX','EW','ISRG','BDX','ZBH','HOLX','BAX','RMD','IQV',
    'CTAS','NKE','MKC','CAG','GIS','CPB','SJM','HRL','MKC','KHC','TSN',
    'F','GM','TM','STLA','RIVN','LCID','DE','CAT','MMM','EMR','HON',
    'RTX','BA','LMT','NOC','GD','AMGN','GILD','BIIB','VRTX','REGN','BMY',
]

def check_recent_beats(days_back=5):
    """Check stocks that just reported earnings with >=10% beat."""
    print(f"=== PEAD Scanner: Checking Recent Earnings Beats (last {days_back} days) ===")
    
    # Get current VIX
    try:
        vix_ticker = yf.Ticker('^VIX')
        vix_hist = vix_ticker.history(period='5d')
        current_vix = float(vix_hist['Close'].iloc[-1])
        print(f"Current VIX: {current_vix:.1f}")
        if current_vix >= 20:
            print(f"WARNING: VIX={current_vix:.1f} >= 20. PEAD signal is weaker in elevated VIX.")
            print(f"  VIX 20-25: direction historically 45% (FAILS). VIX 25+: uncertain.")
    except:
        current_vix = None
        print("Could not get VIX")
    print()
    
    qualifying_beats = []
    
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    
    checked = 0
    for ticker in SP500_TICKERS[:80]:
        try:
            t = yf.Ticker(ticker)
            dates = t.earnings_dates
            if dates is None or len(dates) == 0:
                continue
            
            # Filter to recent past (last N days)
            dates = dates[dates.index.tz_localize(None) if dates.index.tz else dates.index]
            recent_dates = []
            for dt, row in dates.iterrows():
                dt_naive = dt.replace(tzinfo=None) if hasattr(dt, 'tzinfo') else dt
                date_str = dt_naive.strftime('%Y-%m-%d')
                if cutoff_date <= date_str <= today:
                    eps_est = row.get('EPS Estimate')
                    eps_act = row.get('Reported EPS')
                    surprise_pct = row.get('Surprise(%)')
                    
                    if pd.isna(eps_est) or pd.isna(eps_act) or pd.isna(surprise_pct):
                        continue
                    
                    if surprise_pct >= MIN_SURPRISE_PCT:
                        # Get market cap
                        info = t.info
                        mkt_cap = info.get('marketCap', 0)
                        
                        if mkt_cap < 500e6:  # Skip small caps
                            continue
                        
                        qualifying_beats.append({
                            'ticker': ticker,
                            'date': date_str,
                            'eps_estimate': eps_est,
                            'eps_actual': eps_act,
                            'surprise_pct': surprise_pct,
                            'market_cap_b': mkt_cap / 1e9,
                            'current_vix': current_vix,
                            'vix_regime': 'low' if current_vix and current_vix < 20 else 'elevated',
                        })
                        print(f"  BEAT: {ticker} on {date_str} | EPS surprise: +{surprise_pct:.0f}% | Cap: ${mkt_cap/1e9:.0f}B")
            
            checked += 1
        except Exception as e:
            continue
    
    print(f"\nChecked {checked} tickers.")
    
    if not qualifying_beats:
        print("No qualifying beats found in the last 5 days.")
        return []
    
    print(f"\n{len(qualifying_beats)} qualifying beats found!")
    for b in qualifying_beats:
        print(f"  {b['ticker']}: +{b['surprise_pct']:.0f}% surprise on {b['date']} | Cap: ${b['market_cap_b']:.0f}B | VIX: {b['current_vix']:.1f} ({b['vix_regime']})")
    
    return qualifying_beats


def scan_upcoming_earnings(days_ahead=7):
    """Scan for upcoming earnings to watch."""
    print(f"=== PEAD Scanner: Upcoming Earnings (next {days_ahead} days) ===")
    
    today = datetime.now()
    end = today + timedelta(days=days_ahead)
    
    upcoming = []
    for ticker in SP500_TICKERS[:50]:
        try:
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is None:
                continue
            
            earnings_dates = cal.get('Earnings Date', [])
            if not earnings_dates:
                continue
            
            for edate in earnings_dates:
                if isinstance(edate, str):
                    edate = datetime.strptime(edate, '%Y-%m-%d')
                
                if today <= edate <= end:
                    # Get historical beat rate
                    dates = t.earnings_dates
                    if dates is not None and len(dates) >= 4:
                        past = dates[dates.index.tz_localize(None) < pd.Timestamp(today)]
                        past = past.dropna(subset=['Surprise(%)'])
                        if len(past) >= 4:
                            recent4 = past.head(4)
                            avg_surprise = recent4['Surprise(%)'].mean()
                            beat_pct = (recent4['Surprise(%)'] >= 10).mean() * 100
                        else:
                            avg_surprise = 0
                            beat_pct = 0
                    else:
                        avg_surprise = 0
                        beat_pct = 0
                    
                    upcoming.append({
                        'ticker': ticker,
                        'earnings_date': edate.strftime('%Y-%m-%d'),
                        'avg_surprise_4q': avg_surprise,
                        'pct_big_beats_4q': beat_pct,
                    })
        except:
            continue
    
    if upcoming:
        upcoming.sort(key=lambda x: (x['earnings_date'], -x['avg_surprise_4q']))
        print("Upcoming earnings (sorted by date, then avg surprise):")
        for u in upcoming:
            star = " *** HIGH BEAT RATE" if u['pct_big_beats_4q'] >= 50 else ""
            print(f"  {u['ticker']} on {u['earnings_date']} | avg_surprise={u['avg_surprise_4q']:+.0f}% | big_beat_rate={u['pct_big_beats_4q']:.0f}%{star}")
    else:
        print("No upcoming earnings found.")
    
    return upcoming


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PEAD Event Scanner')
    parser.add_argument('--check', action='store_true', help='Check recent beats only')
    parser.add_argument('--days', type=int, default=5, help='Days to look back/ahead')
    args = parser.parse_args()
    
    if args.check:
        beats = check_recent_beats(days_back=args.days)
    else:
        beats = check_recent_beats(days_back=5)
        print()
        upcoming = scan_upcoming_earnings(days_ahead=7)
