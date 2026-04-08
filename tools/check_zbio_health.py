"""
ZBIO Position Health Check (hypothesis 2bbe0f04)
=================================================
Run at end of each trading day. Computes raw + abnormal return vs SPY/XBI
and applies pre-registered day-by-day decision rules.

PRE-REGISTERED DECISION RULES (locked 2026-04-09 00:45 ET):

Day-1 (Apr 8 close) — REFERENCE — raw -6.87%, abnormal vs SPY -9.82%.
  Already borderline. Rules begin at day-2.

Day-2 (Apr 9 close) — FIRST HARD GATE:
  IF raw ≤ -10%  OR  abnormal vs SPY ≤ -10%  → CLOSE at Apr 10 open
  IF raw ≥ -4% (recovery)                     → HOLD to day-3
  ELSE                                        → HOLD to day-3, tighten stop to 12% raw

Day-3 (Apr 10 close — REPL PDUFA day, watch volatility):
  IF raw ≤ -10%  OR  abnormal vs SPY ≤ -12%  → CLOSE at Apr 13 open
  IF raw ≥  0%                               → HOLD to day-5
  ELSE                                        → HOLD to day-5

Day-5 (Apr 14 close): MANDATORY EXIT per pre-reg (5-day horizon)

Existing standing stops (unchanged):
  - 15% raw stop loss (= $19.04)
  - 20% take profit (= $26.88)

Usage:
  python tools/check_zbio_health.py            # check w/ latest daily close
  python tools/check_zbio_health.py --day-n 2  # explicit day-N context
"""

import sys
import argparse
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import db

HYPOTHESIS_ID = '2bbe0f04'
SYMBOL = 'ZBIO'
ENTRY_PRICE = 22.40          # Alpaca fill price
ENTRY_DATE = '2026-04-07'    # Apr 7 open
RAW_STOP_PCT = 15.0
# Pre-registered thresholds from the file header
DAY2_ABORT_RAW = -10.0
DAY2_ABORT_ABN = -10.0
DAY2_HOLD_RAW = -4.0
DAY3_ABORT_RAW = -10.0
DAY3_ABORT_ABN = -12.0
DAY3_HOLD_RAW = 0.0


def _trading_day_count(entry: str, current: str) -> int:
    """Count trading days between entry and current (inclusive of current, exclusive of entry)."""
    import pandas as pd
    # Business days between — crude proxy (no holiday logic for this quick tool)
    bdays = pd.bdate_range(entry, current)
    return max(0, len(bdays) - 1)


def _fetch_close(symbol: str, ref_date: str, lookback: int = 6) -> tuple[float | None, str | None]:
    """Return (close, actual_date_str) for the last trading day on/before ref_date."""
    from tools.yfinance_utils import get_close_prices
    import datetime as dt
    start = (dt.date.fromisoformat(ref_date) - dt.timedelta(days=lookback)).isoformat()
    end = (dt.date.fromisoformat(ref_date) + dt.timedelta(days=2)).isoformat()
    series = get_close_prices(symbol, start=start, end=end)
    if series is None or len(series) == 0:
        return None, None
    series = series[series.index <= ref_date]
    if len(series) == 0:
        return None, None
    last_date = series.index[-1]
    last_val = series.iloc[-1]
    # get_close_prices may return a DataFrame (MultiIndex) — flatten
    if hasattr(last_val, 'iloc'):
        last_val = float(last_val.iloc[0])
    else:
        last_val = float(last_val)
    return last_val, str(last_date.date() if hasattr(last_date, 'date') else last_date)[:10]


def _fetch_open(symbol: str, ref_date: str) -> float | None:
    """Return open price on ref_date (or next trading day)."""
    import yfinance as yf
    import datetime as dt
    start = ref_date
    end = (dt.date.fromisoformat(ref_date) + dt.timedelta(days=4)).isoformat()
    df = yf.download(symbol, start=start, end=end, auto_adjust=False, progress=False)
    if df is None or len(df) == 0:
        return None
    # Handle multi-index columns (single-symbol download still returns MultiIndex)
    open_col = df['Open']
    if hasattr(open_col, 'iloc') and hasattr(open_col, 'columns'):
        return float(open_col.iloc[0, 0])
    return float(open_col.iloc[0])


def main() -> int:
    parser = argparse.ArgumentParser(description='ZBIO position health + decision recommendation')
    parser.add_argument('--day-n', type=int, default=None,
                        help='Override day number (default: computed from trading days since entry)')
    parser.add_argument('--ref-date', default=date.today().isoformat(),
                        help='Reference close date (default: today)')
    args = parser.parse_args()

    db.init_db()
    h = db.get_hypothesis_by_id(HYPOTHESIS_ID)
    if not h:
        print(f'ERROR: hypothesis {HYPOTHESIS_ID} not found')
        return 1
    if h['status'] != 'active':
        print(f"ABORT: hypothesis status is '{h['status']}', not active. Nothing to monitor.")
        return 0

    print('=' * 60)
    print(f'ZBIO HEALTH CHECK  ref={args.ref_date}')
    print('=' * 60)
    print(f'Entry: {ENTRY_DATE} open @ ${ENTRY_PRICE:.2f}')

    # Benchmarks: SPY open on entry day as reference
    spy_entry = _fetch_open('SPY', ENTRY_DATE)
    xbi_entry = _fetch_open('XBI', ENTRY_DATE)
    if spy_entry is None or xbi_entry is None:
        print('ERROR: could not fetch benchmark opens')
        return 1

    zbio_c, zbio_d = _fetch_close(SYMBOL, args.ref_date)
    spy_c, _ = _fetch_close('SPY', args.ref_date)
    xbi_c, _ = _fetch_close('XBI', args.ref_date)
    if zbio_c is None:
        print('ERROR: could not fetch ZBIO close')
        return 1

    raw = (zbio_c / ENTRY_PRICE - 1) * 100
    spy_ret = (spy_c / spy_entry - 1) * 100
    xbi_ret = (xbi_c / xbi_entry - 1) * 100
    abn_spy = raw - spy_ret
    abn_xbi = raw - xbi_ret

    day_n = args.day_n or _trading_day_count(ENTRY_DATE, zbio_d)
    print(f'Day-{day_n}  close-date: {zbio_d}')
    print(f'  ZBIO ${zbio_c:.2f}  raw {raw:+.2f}%')
    print(f'  SPY  benchmark {spy_ret:+.2f}%  -> abnormal vs SPY {abn_spy:+.2f}%')
    print(f'  XBI  benchmark {xbi_ret:+.2f}%  -> abnormal vs XBI {abn_xbi:+.2f}%')
    print()

    # Apply rules
    stop_trigger = -RAW_STOP_PCT
    if raw <= stop_trigger:
        print(f'*** STANDING 15% STOP HIT (raw {raw:+.2f} <= {stop_trigger:+.2f}) — CLOSE NOW ***')
        return 0

    recommend = 'HOLD'
    reason = ''
    if day_n <= 1:
        recommend = 'HOLD'
        reason = 'day-1 — reference only, day-2 gate tomorrow'
    elif day_n == 2:
        if raw <= DAY2_ABORT_RAW or abn_spy <= DAY2_ABORT_ABN:
            recommend = 'CLOSE AT NEXT OPEN'
            reason = f'day-2 gate: raw {raw:+.2f} or abn {abn_spy:+.2f} breached pre-reg thresholds'
        elif raw >= DAY2_HOLD_RAW:
            recommend = 'HOLD to day-3'
            reason = f'day-2 recovery (raw {raw:+.2f} >= {DAY2_HOLD_RAW})'
        else:
            recommend = 'HOLD to day-3, TIGHTEN STOP to 12% raw'
            reason = f'day-2 middle zone (raw {raw:+.2f})'
    elif day_n == 3:
        if raw <= DAY3_ABORT_RAW or abn_spy <= DAY3_ABORT_ABN:
            recommend = 'CLOSE AT NEXT OPEN'
            reason = f'day-3 gate: raw {raw:+.2f} or abn {abn_spy:+.2f} breached pre-reg thresholds'
        elif raw >= DAY3_HOLD_RAW:
            recommend = 'HOLD to day-5'
            reason = f'day-3 recovery'
        else:
            recommend = 'HOLD to day-5'
            reason = 'day-3 middle zone — tolerate to final exit'
    elif day_n >= 5:
        recommend = 'CLOSE NOW (5-day horizon mandatory exit)'
        reason = 'pre-registered 5-day hold expired'

    print(f'Recommendation: {recommend}')
    print(f'  Reason: {reason}')
    print()
    print('Pre-registered thresholds (reference):')
    print(f'  Day-2 abort: raw <= {DAY2_ABORT_RAW}% OR abn(SPY) <= {DAY2_ABORT_ABN}%')
    print(f'  Day-3 abort: raw <= {DAY3_ABORT_RAW}% OR abn(SPY) <= {DAY3_ABORT_ABN}%')
    print(f'  Standing 15% stop at ${ENTRY_PRICE * 0.85:.2f}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
