"""
PEAD Q1 2026 Earnings Season Candidate Scanner
================================================
Scans large-cap ($10B+) S&P 500 stocks in Healthcare, Financials, Tech, and
Consumer Staples that report Q1 2026 earnings between April 14 - May 16, 2026.

For each candidate:
  - Checks historical EPS beat rate and avg surprise (last 4-8 quarters)
  - Flags stocks with 75%+ beat rate at >=10% surprise as PEAD candidates
  - Checks current price vs 52-week low (avoid stocks at 52w low)
  - Prints recommended action for when VIX drops below 20

Usage:
  python3 tools/pead_q1_2026_scan.py
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

import yfinance as yf
import pandas as pd

# Target window
WINDOW_START = datetime(2026, 4, 14)
WINDOW_END   = datetime(2026, 5, 16)

# PEAD hypothesis to activate
PEAD_HYPOTHESIS_ID = '9e2a03ac'

# Minimum market cap (in USD)
MIN_MARKET_CAP = 10e9

# Min beat rate (fraction of last 4+ quarters with >=10% EPS surprise)
MIN_BEAT_RATE = 0.50   # 50%+ quarters must be >=10% beat to flag
HIGH_BEAT_RATE = 0.75  # 75%+ = strong candidate

# Sectors: Healthcare, Financials, Tech, Consumer Staples
# Curated list of large-cap S&P 500 names
# Sorted by sector for readability
CANDIDATES = {
    'Technology': [
        'AAPL', 'MSFT', 'NVDA', 'AVGO', 'ORCL', 'ACN', 'IBM', 'TXN',
        'QCOM', 'AMAT', 'ADI', 'MU', 'KLAC', 'LRCX', 'INTC', 'HPQ',
        'CSCO', 'CRM', 'NOW', 'INTU', 'ADBE', 'SNPS', 'CDNS',
        'FTNT', 'PANW', 'ANET', 'GOOG', 'GOOGL', 'META', 'AMZN',
    ],
    'Healthcare': [
        'UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'PFE', 'ABT', 'TMO',
        'DHR', 'BMY', 'AMGN', 'GILD', 'CVS', 'CI', 'HUM', 'ELV',
        'MDT', 'BSX', 'ISRG', 'BDX', 'HOLX', 'IQV', 'VRTX', 'REGN',
        'BIIB', 'A', 'ZBH', 'SYK', 'EW', 'RMD', 'BAX', 'MCK',
        'CAH', 'CNC', 'MOH',
    ],
    'Financials': [
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SPGI',
        'V', 'MA', 'AXP', 'COF', 'DFS', 'SYF', 'USB', 'PNC',
        'TFC', 'KEY', 'FITB', 'RF', 'HBAN', 'CME', 'ICE', 'NDAQ',
        'CB', 'MMC', 'AON', 'TRV', 'MET', 'PRU', 'AFL', 'ALL', 'AIG',
        'HIG', 'L', 'CINF', 'TROW', 'BEN', 'IVZ',
    ],
    'Consumer Staples': [
        'PG', 'KO', 'PEP', 'COST', 'WMT', 'MCD', 'SBUX', 'YUM',
        'KHC', 'GIS', 'CAG', 'CPB', 'MKC', 'SJM', 'HRL', 'TSN',
        'KR', 'SYY', 'BJ', 'DLTR', 'DG', 'MDLZ', 'CL', 'CHD',
        'CLX', 'EL', 'KMB', 'MO', 'PM', 'STZ', 'TAP', 'SAM',
    ],
}

ALL_TICKERS = []
TICKER_SECTOR = {}
for sector, tickers in CANDIDATES.items():
    for t in tickers:
        if t not in TICKER_SECTOR:
            ALL_TICKERS.append(t)
            TICKER_SECTOR[t] = sector


def get_beat_stats(ticker_obj, today_ts):
    """
    Returns (beat_rate_4q, avg_surprise_4q, beat_rate_8q, avg_surprise_8q, quarters_checked)
    beat_rate = fraction of quarters with Surprise >= 10%
    Returns None if insufficient data.
    """
    try:
        dates = ticker_obj.earnings_dates
        if dates is None or len(dates) < 2:
            return None

        # earnings_dates index may be tz-aware
        idx = dates.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx = idx.tz_convert(None)
        dates = dates.copy()
        dates.index = idx

        # Only past quarters
        past = dates[dates.index < today_ts]
        past = past.dropna(subset=['Surprise(%)'])
        if len(past) < 2:
            return None

        recent4 = past.head(4)
        recent8 = past.head(8)

        beat4  = (recent4['Surprise(%)'] >= 10).mean()
        avg4   = recent4['Surprise(%)'].mean()
        beat8  = (recent8['Surprise(%)'] >= 10).mean() if len(recent8) >= 4 else beat4
        avg8   = recent8['Surprise(%)'].mean() if len(recent8) >= 4 else avg4

        return beat4, avg4, beat8, avg8, len(recent4)
    except Exception:
        return None


def get_52w_pct_above_low(ticker_obj):
    """Returns current price % above 52-week low. None if unavailable."""
    try:
        info = ticker_obj.info
        low52 = info.get('fiftyTwoWeekLow')
        curr  = info.get('currentPrice') or info.get('regularMarketPrice')
        if low52 and curr and low52 > 0:
            return (curr - low52) / low52 * 100
        return None
    except Exception:
        return None


def get_market_cap(ticker_obj):
    try:
        info = ticker_obj.info
        return info.get('marketCap') or 0
    except Exception:
        return 0


def get_next_earnings(ticker_obj, window_start, window_end):
    """
    Returns the earnings date if it falls within [window_start, window_end].
    Checks ticker.calendar first, then ticker.earnings_dates for near-future entries.
    """
    today = datetime.now()
    found_dates = []

    # Method 1: calendar
    try:
        cal = ticker_obj.calendar
        if cal is not None:
            # calendar can be a dict or DataFrame
            if isinstance(cal, dict):
                edates = cal.get('Earnings Date', [])
            else:
                # DataFrame: row labels are fields
                if 'Earnings Date' in cal.index:
                    edates = cal.loc['Earnings Date'].tolist()
                elif 'Earnings Date' in cal.columns:
                    edates = cal['Earnings Date'].tolist()
                else:
                    edates = []

            if edates:
                if not isinstance(edates, (list, tuple)):
                    edates = [edates]
                for ed in edates:
                    if ed is None:
                        continue
                    if isinstance(ed, str):
                        ed = datetime.strptime(ed[:10], '%Y-%m-%d')
                    elif hasattr(ed, 'to_pydatetime'):
                        ed = ed.to_pydatetime().replace(tzinfo=None)
                    if window_start <= ed <= window_end:
                        found_dates.append(ed)
    except Exception:
        pass

    # Method 2: earnings_dates — look for future entries in window
    try:
        dates = ticker_obj.earnings_dates
        if dates is not None and len(dates) > 0:
            idx = dates.index
            if hasattr(idx, 'tz') and idx.tz is not None:
                idx = idx.tz_convert(None)
            for ts in idx:
                dt = ts.to_pydatetime().replace(tzinfo=None)
                if window_start <= dt <= window_end:
                    found_dates.append(dt)
    except Exception:
        pass

    if not found_dates:
        return None
    # Return the earliest date in the window
    return sorted(found_dates)[0]


def main():
    today = datetime.now()
    today_ts = pd.Timestamp(today)

    print("=" * 72)
    print("PEAD Q1 2026 EARNINGS SEASON CANDIDATE SCAN")
    print(f"Window: {WINDOW_START.date()} to {WINDOW_END.date()}")
    print(f"As of: {today.strftime('%Y-%m-%d')}")
    print(f"Hypothesis ID: {PEAD_HYPOTHESIS_ID}")
    print("=" * 72)
    print()

    # Get current VIX
    try:
        vix_ticker = yf.Ticker('^VIX')
        vix_hist = vix_ticker.history(period='5d')
        current_vix = float(vix_hist['Close'].iloc[-1])
        print(f"Current VIX: {current_vix:.1f}  (target: < 20 for PEAD activation)")
    except Exception:
        current_vix = None
        print("Current VIX: unavailable")
    print()

    results = []
    errors = []
    total = len(ALL_TICKERS)

    print(f"Scanning {total} tickers across 4 sectors...")
    print("(This may take 3-5 minutes)")
    print()

    for i, ticker in enumerate(ALL_TICKERS):
        sector = TICKER_SECTOR[ticker]
        try:
            t = yf.Ticker(ticker)

            # Check market cap first (fast)
            mkt_cap = get_market_cap(t)
            if mkt_cap < MIN_MARKET_CAP:
                continue

            # Check earnings date
            earnings_dt = get_next_earnings(t, WINDOW_START, WINDOW_END)
            if earnings_dt is None:
                continue

            # Get beat stats
            stats = get_beat_stats(t, today_ts)
            if stats is None:
                beat4, avg4, beat8, avg8, nq = 0, 0, 0, 0, 0
            else:
                beat4, avg4, beat8, avg8, nq = stats

            # 52w low proximity
            pct_above_low = get_52w_pct_above_low(t)

            results.append({
                'ticker': ticker,
                'sector': sector,
                'earnings_date': earnings_dt,
                'mkt_cap_b': mkt_cap / 1e9,
                'beat_rate_4q': beat4,
                'avg_surprise_4q': avg4,
                'beat_rate_8q': beat8,
                'avg_surprise_8q': avg8,
                'n_quarters': nq,
                'pct_above_52w_low': pct_above_low,
            })

            # Progress indicator every 20 tickers
            if (i + 1) % 20 == 0:
                print(f"  ... {i+1}/{total} scanned, {len(results)} in window so far")

        except Exception as e:
            errors.append((ticker, str(e)))
            continue

        time.sleep(0.1)  # polite rate limiting

    print(f"\nScan complete. {len(results)} stocks report in window. {len(errors)} errors.\n")

    if not results:
        print("No results found. Check yfinance availability.")
        return

    # Sort by earnings date, then beat rate
    results.sort(key=lambda x: (x['earnings_date'], -x['beat_rate_4q']))

    # ---- Print full sorted list ----
    print("=" * 72)
    print("ALL REPORTERS IN WINDOW (April 14 - May 16, 2026)")
    print("=" * 72)
    print(f"{'Ticker':<8} {'Date':<12} {'Sector':<18} {'Cap $B':<8} {'Beat4q':<8} {'AvgSurp':<10} {'52w+%':<8}")
    print("-" * 72)
    for r in results:
        low_flag = ""
        if r['pct_above_52w_low'] is not None and r['pct_above_52w_low'] < 5:
            low_flag = " [NEAR 52wLOW]"
        beat_flag = ""
        if r['beat_rate_4q'] >= HIGH_BEAT_RATE:
            beat_flag = " ***"
        elif r['beat_rate_4q'] >= MIN_BEAT_RATE:
            beat_flag = " *"
        pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
        print(
            f"{r['ticker']:<8} {r['earnings_date'].strftime('%Y-%m-%d'):<12} "
            f"{r['sector']:<18} {r['mkt_cap_b']:<8.0f} "
            f"{r['beat_rate_4q']*100:<8.0f} "
            f"{r['avg_surprise_4q']:+.1f}%      "
            f"{pct_low_str:<8}"
            f"{beat_flag}{low_flag}"
        )

    # ---- High-confidence PEAD candidates ----
    strong = [r for r in results
              if r['beat_rate_4q'] >= HIGH_BEAT_RATE
              and r['avg_surprise_4q'] >= 10
              and (r['pct_above_52w_low'] is None or r['pct_above_52w_low'] >= 5)]

    moderate = [r for r in results
                if MIN_BEAT_RATE <= r['beat_rate_4q'] < HIGH_BEAT_RATE
                and r['avg_surprise_4q'] >= 10
                and (r['pct_above_52w_low'] is None or r['pct_above_52w_low'] >= 5)
                and r not in strong]

    near_low = [r for r in results
                if r['pct_above_52w_low'] is not None
                and r['pct_above_52w_low'] < 5]

    print()
    print("=" * 72)
    print(f"STRONG PEAD CANDIDATES (beat_rate >= 75%, avg_surprise >= 10%)")
    print("(Also NOT at 52-week low — avoids confound)")
    print("=" * 72)
    if strong:
        for r in strong:
            action_date = r['earnings_date'] + timedelta(days=1)
            # Skip weekends for action date
            while action_date.weekday() >= 5:
                action_date += timedelta(days=1)
            pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
            print(f"\n  {r['ticker']} ({r['sector']}) | Earnings: {r['earnings_date'].strftime('%Y-%m-%d')}")
            print(f"    Market Cap: ${r['mkt_cap_b']:.0f}B")
            print(f"    Beat Rate (4q): {r['beat_rate_4q']*100:.0f}% | Avg Surprise (4q): {r['avg_surprise_4q']:+.1f}%")
            print(f"    Beat Rate (8q): {r['beat_rate_8q']*100:.0f}% | Avg Surprise (8q): {r['avg_surprise_8q']:+.1f}%")
            print(f"    52w Low Distance: {pct_low_str} above low (safe from 52w confound)")
            print(f"    RECOMMENDED ACTION:")
            print(f"      When VIX drops below 20, activate PEAD long hypothesis {PEAD_HYPOTHESIS_ID}")
            print(f"      for {r['ticker']} on {action_date.strftime('%Y-%m-%d')} open")
            print(f"      (earnings night: {r['earnings_date'].strftime('%Y-%m-%d')}, entry next-day open)")
    else:
        print("  None found with current data.")

    print()
    print("=" * 72)
    print(f"MODERATE PEAD CANDIDATES (beat_rate 50-74%, avg_surprise >= 10%)")
    print("=" * 72)
    if moderate:
        for r in moderate:
            action_date = r['earnings_date'] + timedelta(days=1)
            while action_date.weekday() >= 5:
                action_date += timedelta(days=1)
            pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
            print(f"  {r['ticker']} ({r['sector']}) | {r['earnings_date'].strftime('%Y-%m-%d')} | "
                  f"Beat {r['beat_rate_4q']*100:.0f}% | AvgSurp {r['avg_surprise_4q']:+.1f}% | 52w+: {pct_low_str}")
    else:
        print("  None found.")

    print()
    print("=" * 72)
    print("NEAR 52-WEEK LOW (avoid — confound with mean reversion signal)")
    print("=" * 72)
    if near_low:
        for r in near_low:
            pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
            print(f"  {r['ticker']} ({r['sector']}) | {r['earnings_date'].strftime('%Y-%m-%d')} | "
                  f"Only {pct_low_str} above 52w low — SKIP for PEAD")
    else:
        print("  None flagged.")

    print()
    print("=" * 72)
    print("VIX REGIME STATUS & ACTIVATION TRIGGER")
    print("=" * 72)
    if current_vix:
        if current_vix < 20:
            print(f"  VIX = {current_vix:.1f} — BELOW 20. PEAD signal is in optimal regime NOW.")
            print(f"  Activate hypothesis {PEAD_HYPOTHESIS_ID} for any qualifying beat immediately.")
        elif current_vix < 25:
            print(f"  VIX = {current_vix:.1f} — Elevated. Signal weaker (dir~45%). WAIT for VIX < 20.")
        else:
            print(f"  VIX = {current_vix:.1f} — HIGH. PEAD signal NOT valid above VIX=20.")
            print(f"  April 2 Liberation Day tariff shock may push VIX higher near-term.")
            print(f"  Watch for VIX mean reversion toward 20 before mid-April earnings start.")
    print()
    print("  NOTE: Liberation Day (April 2) tariff announcements could spike VIX further.")
    print("  Monitor VIX daily in first week of April.")
    print("  If VIX < 20 by April 14, all STRONG candidates above are actionable.")
    print()

    # ---- Errors ----
    if errors:
        print(f"\nErrors ({len(errors)} tickers):")
        for ticker, err in errors[:10]:
            print(f"  {ticker}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors)-10} more")

    # ---- Write text output ----
    output_path = Path(__file__).parent / 'pead_q1_2026_candidates.txt'
    write_candidates_file(
        output_path, results, strong, moderate, near_low,
        current_vix, today, PEAD_HYPOTHESIS_ID
    )
    print(f"\nFull output saved to: {output_path}")


def write_candidates_file(path, results, strong, moderate, near_low,
                          current_vix, today, hyp_id):
    lines = []
    lines.append("PEAD Q1 2026 EARNINGS SEASON CANDIDATE LIST")
    lines.append("=" * 72)
    lines.append(f"Generated: {today.strftime('%Y-%m-%d')}")
    lines.append(f"Hypothesis: {hyp_id} (PEAD Long — EPS Beat >=10%, VIX<20)")
    lines.append(f"Signal: After >=10% EPS surprise, large-cap stocks drift +1.96% over 5 days when VIX<20")
    lines.append(f"Window: April 14 - May 16, 2026  (Q1 2026 earnings season)")
    vix_str = f"{current_vix:.1f}" if current_vix else "N/A"
    lines.append(f"Current VIX: {vix_str}")
    vix_status = "NOT in regime (VIX >= 20)" if current_vix and current_vix >= 20 else "IN regime (VIX < 20)"
    lines.append(f"VIX Regime: {vix_status}")
    lines.append("")

    lines.append("-" * 72)
    lines.append("SECTION 1: ALL REPORTERS IN WINDOW (by date)")
    lines.append("-" * 72)
    lines.append(f"{'Ticker':<8} {'Date':<12} {'Sector':<18} {'Cap $B':<8} {'Beat4q%':<9} {'AvgSurp4q':<12} {'52w+%'}")
    for r in results:
        pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
        lines.append(
            f"{r['ticker']:<8} {r['earnings_date'].strftime('%Y-%m-%d'):<12} "
            f"{r['sector']:<18} {r['mkt_cap_b']:<8.0f} "
            f"{r['beat_rate_4q']*100:<9.0f} "
            f"{r['avg_surprise_4q']:+.1f}%         "
            f"{pct_low_str}"
        )

    lines.append("")
    lines.append("-" * 72)
    lines.append("SECTION 2: STRONG PEAD CANDIDATES (beat_rate >= 75%, avg_surprise >= 10%)")
    lines.append("Entry: Next-day open after earnings. Exit: 5 trading days later. Size: $5,000.")
    lines.append("-" * 72)
    if strong:
        for r in strong:
            action_date = r['earnings_date'] + timedelta(days=1)
            while action_date.weekday() >= 5:
                action_date += timedelta(days=1)
            pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
            lines.append(f"")
            lines.append(f"  {r['ticker']} | {r['sector']} | Earnings: {r['earnings_date'].strftime('%Y-%m-%d')}")
            lines.append(f"    Market Cap: ${r['mkt_cap_b']:.0f}B")
            lines.append(f"    Beat Rate (4q): {r['beat_rate_4q']*100:.0f}% | Avg Surprise (4q): {r['avg_surprise_4q']:+.1f}%")
            lines.append(f"    Beat Rate (8q): {r['beat_rate_8q']*100:.0f}% | Avg Surprise (8q): {r['avg_surprise_8q']:+.1f}%")
            lines.append(f"    52w Low Distance: {pct_low_str}")
            lines.append(f"    RECOMMENDED ACTION:")
            lines.append(f"      When VIX drops below 20, activate PEAD long hypothesis {hyp_id}")
            lines.append(f"      for {r['ticker']} on {action_date.strftime('%Y-%m-%d')} open")
            lines.append(f"      (i.e. if {r['ticker']} beats EPS by >=10% on {r['earnings_date'].strftime('%Y-%m-%d')})")
    else:
        lines.append("  No strong candidates found with current data.")

    lines.append("")
    lines.append("-" * 72)
    lines.append("SECTION 3: MODERATE PEAD CANDIDATES (beat_rate 50-74%, avg_surprise >= 10%)")
    lines.append("-" * 72)
    if moderate:
        for r in moderate:
            action_date = r['earnings_date'] + timedelta(days=1)
            while action_date.weekday() >= 5:
                action_date += timedelta(days=1)
            pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
            lines.append(
                f"  {r['ticker']} ({r['sector']}) | {r['earnings_date'].strftime('%Y-%m-%d')} | "
                f"Beat {r['beat_rate_4q']*100:.0f}% | AvgSurp {r['avg_surprise_4q']:+.1f}% | 52w+: {pct_low_str}"
            )
            lines.append(
                f"    ACTION: If {r['ticker']} beats >=10% and VIX<20, activate {hyp_id} for "
                f"{action_date.strftime('%Y-%m-%d')} open"
            )
    else:
        lines.append("  None found.")

    lines.append("")
    lines.append("-" * 72)
    lines.append("SECTION 4: NEAR 52-WEEK LOW — AVOID (confound with mean reversion signal)")
    lines.append("-" * 72)
    if near_low:
        for r in near_low:
            pct_low_str = f"{r['pct_above_52w_low']:.1f}%" if r['pct_above_52w_low'] is not None else "N/A"
            lines.append(
                f"  {r['ticker']} ({r['sector']}) | {r['earnings_date'].strftime('%Y-%m-%d')} | "
                f"Only {pct_low_str} above 52w low — SKIP for PEAD, may fire 52w-low signal instead"
            )
    else:
        lines.append("  None flagged in this scan.")

    lines.append("")
    lines.append("-" * 72)
    lines.append("SECTION 5: VIX REGIME MONITORING & ACTIVATION PROTOCOL")
    lines.append("-" * 72)
    vix_str2 = f"{current_vix:.1f}" if current_vix else "N/A"
    lines.append(f"  Current VIX: {vix_str2}")
    lines.append(f"  PEAD signal validated for VIX<20 regime:")
    lines.append(f"    - VIX<20: avg5d=+1.96%, p=0.024, n=63, direction=68% (VALIDATED)")
    lines.append(f"    - VIX 20-25: direction~45% (FAILS threshold)")
    lines.append(f"    - VIX>25: uncertain, avoid")
    lines.append(f"")
    lines.append(f"  Liberation Day April 2 tariff shock risk:")
    lines.append(f"    - If VIX spikes above 30 on April 2-3, signal is NOT valid for early earners")
    lines.append(f"    - Monitor VIX daily April 7-14 for return toward 20")
    lines.append(f"    - If VIX still >= 20 by April 14, defer all PEAD entries until regime clears")
    lines.append(f"")
    lines.append(f"  ACTIVATION TRIGGER:")
    lines.append(f"    IF VIX < 20 AND stock beats EPS by >=10%:")
    lines.append(f"      python3 db.py update_hypothesis {hyp_id} trigger=next_market_open \\")
    lines.append(f"        trigger_position_size=5000 trigger_stop_loss_pct=10")
    lines.append(f"    Then trade_loop.py picks up at next_market_open.")

    lines.append("")
    lines.append("-" * 72)
    lines.append("METHODOLOGY NOTES")
    lines.append("-" * 72)
    lines.append("  - Beat history sourced from yfinance earnings_dates (past 4-8 quarters)")
    lines.append("  - Surprise% = (actual - estimate) / abs(estimate) * 100")
    lines.append("  - 52w low distance = (current_price - 52w_low) / 52w_low * 100")
    lines.append("  - Stocks near 52w low excluded to avoid confound with mean-reversion trades")
    lines.append("  - Entry: next-day open after earnings release (consistent with PEAD backtest)")
    lines.append("  - Exit: 5 trading days after entry (hypothesis tested at 5d horizon)")
    lines.append("  - Position size: $5,000 (standard experiment size)")
    lines.append("  - Stop loss: 10%")
    lines.append("  - Hypothesis 9e2a03ac: PEAD Long earnings beat >=10% VIX<20")

    path.write_text("\n".join(lines))


if __name__ == '__main__':
    main()
