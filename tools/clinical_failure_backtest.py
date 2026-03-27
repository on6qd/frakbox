"""
Clinical Efficacy Failure Post-Crash Drift Backtest
====================================================
Hypothesis d302c84b: After a >55% single-day crash from clinical trial failure
or FDA CRL (clinical/efficacy), short at next-day open, hold 3 trading days.

Expected: +27% abnormal short return over 3 days.

Tests the provided symbols known to have had clinical failures.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


# Known clinical failure events (VERIFIED via web search 2026-03-27)
KNOWN_EVENTS = {
    # ticker: [(crash_date, event_description)]
    'ARDX': [('2021-07-20', 'FDA CRL tenapanor CKD phosphorus - efficacy deemed small/unclear')],
    'NERV': [('2020-05-29', 'Phase 3 roluperidone schizophrenia - missed all endpoints')],
    'GRTX': [('2023-08-10', 'FDA CRL avasopasem radiotherapy mucositis - not persuasive efficacy')],
    'IMMP': [('2026-03-13', 'Phase 3 TACTI-004 halted IDMC futility efti+pembro 1L NSCLC')],
    'GOSS': [('2026-02-23', 'Phase 3 PROSERA failure seralutinib PAH - missed by margin')],
    'REPL': [('2025-07-22', 'FDA CRL RP1/vusolimogene melanoma - IGNYTE trial inadequate')],
}

# Additional verified events
ADDITIONAL_EVENTS = {
    'RLMD': [('2022-10-13', 'Phase 3 REL-1017 depression - missed primary vs placebo')],
    'ATHX': [('2022-05-20', 'Phase 3 TREASURE MultiStem stroke - missed primary endpoint')],
    'VRCA': [('2022-09-19', 'Phase 2b IMC-1 fibromyalgia - missed pain endpoint p=0.302')],
    'SAVA': [('2024-11-25', 'Phase 3 simufilam Alzheimer - missed all co-primary endpoints')],
    'FULC': [('2024-09-12', 'Phase 3 REACH losmapimod FSHD - missed primary endpoint')],
    'OTLK': [('2023-08-29', 'FDA CRL ONS-5010 CMC+efficacy issues - second CRL 2025-08-29')],
}


def find_crash_date(ticker, expected_date_str=None, threshold=-0.50):
    """Find the largest single-day crash for a ticker, optionally near expected date."""
    try:
        if expected_date_str:
            expected = pd.Timestamp(expected_date_str)
            start = expected - timedelta(days=30)
            end = expected + timedelta(days=30)
        else:
            start = datetime(2018, 1, 1)
            end = datetime.now()

        data = yf.download(ticker, start=start, end=end,
                          auto_adjust=True, progress=False)
        if data.empty:
            return None, None

        if isinstance(data.columns, pd.MultiIndex):
            closes = data['Close'].iloc[:, 0].dropna()
        else:
            closes = data['Close'].dropna()

        if len(closes) < 3:
            return None, None

        daily_returns = closes.pct_change()

        if expected_date_str:
            # Find crash near expected date
            window = daily_returns[
                (daily_returns.index >= expected - timedelta(days=10)) &
                (daily_returns.index <= expected + timedelta(days=10))
            ]
            big_crashes = window[window <= threshold]
        else:
            big_crashes = daily_returns[daily_returns <= threshold]

        if big_crashes.empty:
            return None, None

        # Get the largest crash
        crash_date = big_crashes.idxmin()
        crash_return = float(big_crashes.min())

        return crash_date, crash_return

    except Exception as e:
        return None, None


def compute_post_crash_return(ticker, crash_date, hold_days=3):
    """
    Compute return for a short position entered at open of day AFTER crash.
    Entry: open of day+1 (proxied by close of day+1)
    Exit: close of day+1+hold_days
    """
    try:
        start = crash_date - timedelta(days=5)
        end = crash_date + timedelta(days=hold_days + 10)

        data = yf.download(ticker, start=start, end=end,
                          auto_adjust=True, progress=False)
        if data.empty:
            return None

        if isinstance(data.columns, pd.MultiIndex):
            closes = data['Close'].iloc[:, 0].dropna()
            opens = data['Open'].iloc[:, 0].dropna() if 'Open' in data.columns.get_level_values(0) else None
        else:
            closes = data['Close'].dropna()
            opens = data['Open'].dropna() if 'Open' in data.columns else None

        # Find crash date in index
        try:
            crash_idx = closes.index.get_loc(crash_date)
        except KeyError:
            # Try to find nearest
            crash_idx = closes.index.searchsorted(crash_date)
            if crash_idx >= len(closes):
                return None

        entry_idx = crash_idx + 1  # Day after crash
        exit_idx = entry_idx + hold_days

        if exit_idx >= len(closes):
            return None

        # Entry: use open if available, else close of entry day
        if opens is not None and len(opens) > entry_idx:
            entry_price = float(opens.iloc[entry_idx])
        else:
            entry_price = float(closes.iloc[entry_idx])

        exit_price = float(closes.iloc[exit_idx])
        crash_close = float(closes.iloc[crash_idx])

        # Short return = -(exit - entry) / entry
        short_return = -(exit_price - entry_price) / entry_price

        return {
            'crash_close': crash_close,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'short_return': short_return,
            'raw_return': (exit_price - entry_price) / entry_price,
        }

    except Exception as e:
        return None


def main():
    print("=" * 70)
    print("CLINICAL FAILURE POST-CRASH DRIFT BACKTEST")
    print("Hypothesis d302c84b: Short next-day open after >55% crash")
    print("Hold: 3 trading days | Benchmark: SPY over same period")
    print("=" * 70)
    print()

    # Get SPY data for benchmark
    spy_data = yf.download('SPY', start='2018-01-01', end='2026-04-01',
                           auto_adjust=True, progress=False)
    if isinstance(spy_data.columns, pd.MultiIndex):
        spy_closes = spy_data['Close'].iloc[:, 0].dropna()
    else:
        spy_closes = spy_data['Close'].dropna()

    all_results = []

    # Combine event sets
    all_events = {**KNOWN_EVENTS, **ADDITIONAL_EVENTS}

    for ticker, events in all_events.items():
        for expected_date_str, description in events:
            print(f"Processing {ticker} (~{expected_date_str}): {description}")

            # Find actual crash date
            crash_date, crash_return = find_crash_date(ticker, expected_date_str, threshold=-0.40)

            if crash_date is None:
                print(f"  ✗ No crash found near {expected_date_str}")
                continue

            if crash_return > -0.40:
                print(f"  ✗ Crash only {crash_return*100:.1f}%, below threshold")
                continue

            print(f"  Crash: {crash_date.date()} ({crash_return*100:.1f}%)")

            # Compute post-crash 3d return
            result = compute_post_crash_return(ticker, crash_date, hold_days=3)
            if result is None:
                print(f"  ✗ Cannot compute forward return")
                continue

            # Compute SPY benchmark return
            try:
                spy_idx = spy_closes.index.searchsorted(crash_date)
                entry_spy_idx = spy_idx + 1
                exit_spy_idx = entry_spy_idx + 3

                if exit_spy_idx < len(spy_closes):
                    spy_entry = float(spy_closes.iloc[entry_spy_idx])
                    spy_exit = float(spy_closes.iloc[exit_spy_idx])
                    spy_return = (spy_exit - spy_entry) / spy_entry
                    # Benchmark short: negative SPY return is good (means market fell)
                    # Abnormal short = stock_short - spy_short
                    # = -stock_raw - (-spy_raw) = spy_raw - stock_raw
                    abnormal = spy_return - result['raw_return']
                else:
                    abnormal = result['short_return']
                    spy_return = 0
            except:
                abnormal = result['short_return']
                spy_return = 0

            print(f"  Entry: {result['entry_price']:.3f} | Exit: {result['exit_price']:.3f}")
            print(f"  Short 3d return: {result['short_return']*100:+.1f}%")
            print(f"  Abnormal (vs SPY): {abnormal*100:+.1f}%")

            all_results.append({
                'ticker': ticker,
                'crash_date': crash_date,
                'crash_return': crash_return,
                'short_return': result['short_return'],
                'spy_return': spy_return,
                'abnormal': abnormal,
                'description': description,
            })

    print()
    print("=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)

    if not all_results:
        print("No results!")
        return

    df = pd.DataFrame(all_results)
    df = df.sort_values('crash_date')

    print(f"\nN events: {len(df)}")
    print(f"\nIndividual results:")
    for _, r in df.iterrows():
        sign = "✓" if r['short_return'] > 0.005 else ("✗" if r['short_return'] < -0.005 else "~")
        print(f"  {sign} {r['ticker']:8s} {r['crash_date'].date()} crash={r['crash_return']*100:.0f}% | "
              f"short3d={r['short_return']*100:+.1f}% | abnormal={r['abnormal']*100:+.1f}%")

    print()
    short_returns = df['short_return'].values
    abnormal_returns = df['abnormal'].values

    mean_short = np.mean(short_returns) * 100
    mean_abnormal = np.mean(abnormal_returns) * 100
    direction = (short_returns > 0.005).mean() * 100

    if len(short_returns) >= 3:
        t_short, p_short = stats.ttest_1samp(short_returns, 0)
        t_abn, p_abn = stats.ttest_1samp(abnormal_returns, 0)
    else:
        p_short = p_abn = 1.0

    print(f"AGGREGATE:")
    print(f"  Mean short 3d return: {mean_short:+.2f}%")
    print(f"  Mean abnormal return: {mean_abnormal:+.2f}%")
    print(f"  Direction (>0.5%): {direction:.0f}%")
    print(f"  p-value (short return): {p_short:.4f}")
    print(f"  p-value (abnormal): {p_abn:.4f}")

    print()
    # Subsegment by crash severity
    severe = df[df['crash_return'] <= -0.55]
    moderate = df[(df['crash_return'] > -0.55) & (df['crash_return'] <= -0.40)]
    print(f"By crash severity:")
    if len(severe) > 0:
        s_ret = severe['short_return'].values
        print(f"  >55% crash (n={len(severe)}): mean={np.mean(s_ret)*100:+.2f}%, dir={(s_ret>0.005).mean()*100:.0f}%")
    if len(moderate) > 0:
        m_ret = moderate['short_return'].values
        print(f"  40-55% crash (n={len(moderate)}): mean={np.mean(m_ret)*100:+.2f}%, dir={(m_ret>0.005).mean()*100:.0f}%")

    print()
    print("CONCLUSION:")
    if p_short < 0.05 and mean_short > 0.5 and direction > 55:
        print(f"✓ VALID: mean={mean_short:+.2f}%, dir={direction:.0f}%, p={p_short:.4f}")
    elif mean_short < 0:
        print(f"✗ INVERTED: mean={mean_short:+.2f}% (stocks RECOVER after clinical failure crash)")
    else:
        print(f"? WEAK/UNCERTAIN: mean={mean_short:+.2f}%, dir={direction:.0f}%, p={p_short:.4f}")


if __name__ == '__main__':
    main()
