"""
Daily Scanner: 52-Week Low First-Touch SHORT Signal

Runs against the S&P 500 large-cap universe to detect when any stock
first breaks below its 52-week low (after 30+ days above that level).

Hypothesis: sp500_52w_low_momentum_short (ID: 86d28864)
Expected: SHORT at next-day open, hold 5 trading days, expected -1.68% abnormal

Multi-signal behavior:
- ALL signals are logged to logs/52w_low_signals.jsonl regardless of whether they trade
- When multiple signals fire on the same day, only ONE trigger is set on the hypothesis
- Priority: larger market cap wins (better liquidity, less slippage)
- If hypothesis already has a pending trigger (from a prior run same day), the existing
  trigger is only replaced if the new signal has higher market cap

Usage:
    python tools/fiftytwo_week_low_scanner.py
    python tools/fiftytwo_week_low_scanner.py --dry-run    # show detections only
"""

import sys
import os
import argparse
import json
from datetime import datetime, timedelta
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import pandas as pd

# Universe of S&P 500 large-cap stocks
# Loaded from data/sp500_universe.json (built by tools/build_sp500_universe.py)
# Falls back to static 174-ticker list if cache is unavailable
def _load_universe():
    """Load SP500 universe from cache file, falling back to static list."""
    cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'sp500_universe.json')
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                data = json.load(f)
            tickers = data.get('tickers', [])
            if len(tickers) >= 200:  # sanity check
                return tickers
        except Exception:
            pass
    # Static fallback list (174 tickers, last updated 2026-03-23)
    return [
        # Technology
        'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META', 'INTC', 'AMD', 'QCOM', 'TXN', 'AVGO',
        'IBM', 'CSCO', 'ORCL', 'CRM', 'NOW', 'ADBE', 'MU', 'AMAT', 'LRCX', 'KLAC',
        'SNPS', 'CDNS', 'ANSS', 'ACN',
        # Financials
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'USB', 'PNC', 'TFC', 'AXP', 'V', 'MA', 'COF',
        'BLK', 'SCHW', 'ICE', 'CME', 'MCO', 'SPGI', 'CB', 'BK', 'STT', 'TROW', 'BRK-B',
        # Healthcare
        'JNJ', 'PFE', 'MRK', 'ABT', 'BMY', 'ABBV', 'LLY', 'MDT', 'UNH',
        'BAX', 'BDX', 'BSX', 'EW', 'ZBH', 'SYK', 'DHR', 'HCA', 'ISRG',
        'REGN', 'GILD', 'VRTX', 'MRNA', 'BIIB', 'AMGN',
        # Consumer Discretionary
        'AMZN', 'TSLA', 'HD', 'LOW', 'NKE', 'MCD', 'SBUX', 'TGT',
        'LULU', 'TJX', 'ROST', 'BBY', 'BKNG', 'MAR', 'HLT', 'RCL', 'CCL',
        # Consumer Staples
        'WMT', 'KO', 'PEP', 'COST', 'CL', 'PG', 'MO', 'PM', 'MDLZ', 'HSY', 'GIS', 'K', 'STZ',
        'KHC', 'CAG', 'SJM', 'CPB', 'HRL', 'MKC', 'KMB', 'CHD', 'CLX',
        # Energy
        'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PSX', 'VLO', 'MPC', 'OXY', 'BKR', 'HAL',
        # Industrials
        'BA', 'GE', 'MMM', 'CAT', 'HON', 'LMT', 'RTX', 'UPS', 'FDX',
        'DE', 'EMR', 'ETN', 'ITW', 'PH', 'ROK', 'DOV', 'PNR', 'XYL',
        'UNP', 'NSC', 'CSX', 'CP', 'CNI',
        # Materials
        'FCX', 'NUE', 'DD', 'APD', 'LIN', 'PPG', 'SHW', 'ECL', 'ALB',
        # Communication Services
        'VZ', 'T', 'CMCSA', 'DIS', 'NFLX', 'EA', 'TTWO',
        # REITs
        'PLD', 'AMT', 'CCI', 'EQIX', 'PSA', 'DLR', 'O', 'AVB', 'EQR', 'SPG',
        # Utilities
        'NEE', 'DUK', 'SO', 'AEP', 'XEL', 'ED', 'SRE', 'D', 'PCG',
    ]

UNIVERSE = _load_universe()

HYPOTHESIS_ID = '86d28864'
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs', '52w_low_scanner_state.json')
SIGNALS_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs', '52w_low_signals.jsonl')


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {'last_triggered': {}}  # ticker -> last trigger date


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def get_market_cap(ticker: str) -> float:
    """Return market cap in dollars, or 0.0 on failure."""
    try:
        info = yf.Ticker(ticker).info
        return float(info.get('marketCap', 0) or 0)
    except Exception:
        return 0.0


def check_upcoming_earnings(ticker: str, hold_days: int = 5) -> dict:
    """
    Check if there is an upcoming earnings announcement within the hold period.

    Returns dict with:
        has_earnings_soon: bool
        earnings_date: str or None
        days_until_earnings: int or None
    """
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        if cal is None:
            return {'has_earnings_soon': False, 'earnings_date': None, 'days_until_earnings': None}

        earnings_dates = cal.get('Earnings Date', [])
        if not isinstance(earnings_dates, list):
            earnings_dates = [earnings_dates]

        import datetime as dt_module
        today = dt_module.date.today()

        for ed in earnings_dates:
            if ed is None:
                continue
            if hasattr(ed, 'date'):
                ed = ed.date()
            elif isinstance(ed, str):
                ed = dt_module.date.fromisoformat(ed)

            days_until = (ed - today).days
            # Earnings within hold_days + 2 buffer days is a confounder
            if 0 <= days_until <= hold_days + 2:
                return {
                    'has_earnings_soon': True,
                    'earnings_date': ed.isoformat(),
                    'days_until_earnings': days_until,
                }

        return {'has_earnings_soon': False, 'earnings_date': None, 'days_until_earnings': None}
    except Exception:
        return {'has_earnings_soon': False, 'earnings_date': None, 'days_until_earnings': None}


def log_signal(detection: dict):
    """Append a signal record to the persistent signals log (all signals, not just triggered ones)."""
    os.makedirs(os.path.dirname(SIGNALS_LOG), exist_ok=True)
    record = dict(detection)
    record['logged_at'] = datetime.now().isoformat()
    with open(SIGNALS_LOG, 'a') as f:
        f.write(json.dumps(record) + '\n')


def check_stock(ticker: str, lookback_days: int = 400, debounce_days: int = 30) -> dict | None:
    """
    Check if ticker is at a new 52-week low (first touch after 30+ day gap).
    
    Returns detection dict or None if no signal.
    """
    try:
        end = datetime.now().strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        df = yf.download(ticker, start=start, end=end, progress=False)
        
        if df.empty or len(df) < 100:
            return None
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # Rolling 252-day low (exclude today)
        df['52w_low'] = df['Close'].shift(1).rolling(252, min_periods=100).min()
        df['at_52w_low'] = df['Close'] <= df['52w_low']
        
        # Days since last 52-week low in prior debounce_days
        df['days_since_last_low'] = (
            df['at_52w_low'].shift(1)
            .rolling(debounce_days, min_periods=1)
            .sum()
        )
        df['first_touch'] = (
            df['at_52w_low'] &
            (df['days_since_last_low'] == 0) &
            df['52w_low'].notna()
        )
        
        # Check the most recent trading day
        if len(df) == 0:
            return None
        
        last_row = df.iloc[-1]
        last_date = df.index[-1].strftime('%Y-%m-%d')
        
        if not last_row['first_touch']:
            return None
        
        earnings_info = check_upcoming_earnings(ticker, hold_days=5)
        return {
            'ticker': ticker,
            'date': last_date,
            'close': float(last_row['Close']),
            '52w_low': float(last_row['52w_low']),
            'pct_below': float((last_row['Close'] / last_row['52w_low'] - 1) * 100),
            'hypothesis_id': HYPOTHESIS_ID,
            'action': 'SHORT at next market open',
            'hold_days': 5,
            'earnings_soon': earnings_info['has_earnings_soon'],
            'earnings_date': earnings_info['earnings_date'],
            'days_until_earnings': earnings_info['days_until_earnings'],
        }
    except Exception as e:
        print(f"  ERROR {ticker}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='52-Week Low First-Touch SHORT Scanner')
    parser.add_argument('--dry-run', action='store_true', help='Show detections without triggering trades')
    args = parser.parse_args()
    
    print(f"=== 52-Week Low Scanner ({datetime.now().strftime('%Y-%m-%d %H:%M')}) ===")
    print(f"Universe: {len(UNIVERSE)} stocks")
    print(f"Hypothesis: {HYPOTHESIS_ID} (sp500_52w_low_momentum_short)")
    print()
    
    state = load_state()
    detections = []
    
    for ticker in UNIVERSE:
        result = check_stock(ticker)
        if result:
            # Check debounce: don't re-trigger same stock within 30 days
            last_trig = state['last_triggered'].get(ticker)
            if last_trig:
                days_since = (datetime.now() - datetime.strptime(last_trig, '%Y-%m-%d')).days
                if days_since < 30:
                    print(f"  {ticker}: SKIP (already triggered {days_since}d ago)")
                    continue
            
            if result.get('earnings_soon'):
                print(f"  SKIP (earnings confounder): {ticker} — earnings {result['earnings_date']} "
                      f"({result['days_until_earnings']}d away, within hold window)")
                # Still log for OOS tracking, but mark as disqualified
                result['disqualified_reason'] = f"earnings {result['earnings_date']} within hold window"
                log_signal(result)
                continue

            detections.append(result)
            print(f"  SIGNAL: {ticker} on {result['date']}")
            print(f"    Close: {result['close']:.2f}, 52W Low: {result['52w_low']:.2f}")
            print(f"    Breach: {result['pct_below']:.1f}%")
            print(f"    Action: {result['action']}")
        time.sleep(0.05)
    
    if not detections:
        print("No signals detected.")
        save_state(state)
        return

    print(f"\n{len(detections)} signal(s) detected.")

    # Log ALL signals to the persistent signal log (for OOS tracking even if not traded)
    for det in detections:
        log_signal(det)
        print(f"  Logged signal: {det['ticker']} ({det['date']})")

    if args.dry_run:
        print("DRY RUN: No triggers set.")
        return

    # Fetch market caps and rank signals — larger cap = better liquidity = priority
    print("\nFetching market caps to prioritize signals...")
    for det in detections:
        det['market_cap'] = get_market_cap(det['ticker'])
        print(f"  {det['ticker']}: ${det['market_cap']/1e9:.1f}B market cap")

    detections_ranked = sorted(detections, key=lambda d: d['market_cap'], reverse=True)
    top_signal = detections_ranked[0]
    skipped = detections_ranked[1:]

    print(f"\nTop signal (largest cap): {top_signal['ticker']} (${top_signal['market_cap']/1e9:.1f}B)")
    if skipped:
        print(f"Skipped signals (lower cap, logged only): {[s['ticker'] for s in skipped]}")

    import research
    hypotheses = research.load_hypotheses()

    # Find the hypothesis record
    hyp = next((h for h in hypotheses if h['id'] == HYPOTHESIS_ID and h['status'] == 'pending'), None)
    if hyp is None:
        print(f"WARNING: Could not find pending hypothesis {HYPOTHESIS_ID}. Hypothesis may be active or retired.")
        save_state(state)
        return

    # Check if a trigger is already set (e.g. from a prior run today or from another signal)
    existing_trigger = hyp.get('trigger')
    existing_symbol = hyp.get('expected_symbol')

    if existing_trigger == 'next_market_open' and existing_symbol and existing_symbol != 'TBD':
        # Hypothesis already has a pending trigger — compare market caps
        existing_cap = get_market_cap(existing_symbol)
        print(f"\nHypothesis already has pending trigger: {existing_symbol} (${existing_cap/1e9:.1f}B)")

        if top_signal['market_cap'] > existing_cap:
            print(f"  New signal {top_signal['ticker']} has higher cap — replacing trigger.")
        else:
            print(f"  Existing signal {existing_symbol} has equal/higher cap — keeping existing trigger.")
            print(f"  New signal {top_signal['ticker']} logged for OOS tracking only.")
            # Still record in state so debounce works correctly
            for det in detections:
                state['last_triggered'][det['ticker']] = det['date']
            save_state(state)
            return

    # Set trigger for the top-ranked signal
    hyp['expected_symbol'] = top_signal['ticker']
    hyp['trigger'] = 'next_market_open'
    hyp['trigger_position_size'] = 5000
    hyp['trigger_stop_loss_pct'] = 8  # Stop loss at 8% (tighter for short)
    hyp['trigger_take_profit_pct'] = 10
    hyp['trigger_notes'] = (
        f"52-week low first touch on {top_signal['date']}, breach={top_signal['pct_below']:.1f}%. "
        f"Market cap: ${top_signal['market_cap']/1e9:.1f}B. "
        + (f"Also detected (not traded): {[s['ticker'] for s in skipped]}" if skipped else "")
    )
    print(f"\nTrigger set: {top_signal['ticker']} SHORT at next market open")

    # Update debounce state for ALL detected signals (not just the one traded)
    for det in detections:
        state['last_triggered'][det['ticker']] = det['date']

    research.save_hypotheses(hypotheses)
    save_state(state)
    print("\nDone. trade_loop.py will execute at next market open.")


if __name__ == '__main__':
    main()
