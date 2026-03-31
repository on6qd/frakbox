"""
Liberation Day 2026 Outcome Checker
=====================================
Measures actual hypothesis outcomes for all Liberation Day pre-registered trades.
Run this AFTER each hypothesis reaches its target hold period to get completion data.

Liberation Day hypothesis IDs and target dates:
  SPY long  (b63a0168): entry 2026-03-31, 20d = 2026-04-28
  VGNT short (2d94ac68): entry 2026-04-01, 2d = 2026-04-03
  WFC short (b73efac3): entry 2026-04-06 (orig trigger 04-03, Good Friday), 5d = 2026-04-13
  AMD short (132e9128): entry 2026-04-06 (orig trigger 04-03, Good Friday), 5d = 2026-04-13, 10d = 2026-04-18
  QCOM short (14de5527): entry 2026-04-06 (orig trigger 04-03, Good Friday), 5d = 2026-04-13, 10d = 2026-04-18
  GLD long (b768e8d8): entry 2026-04-07, 20d = 2026-05-05
  KRE short (6e732966): entry 2026-04-10, 5d = 2026-04-17 (MANUAL CLOSE), 10d = 2026-04-24
  COST long (8c2f8cbb): entry 2026-04-11, 10d = 2026-04-25
  AEP long (35b63a23): entry 2026-04-14, 10d = 2026-04-28

Usage:
    python3 tools/liberation_day_outcome_checker.py                    # check all
    python3 tools/liberation_day_outcome_checker.py --date 2026-04-10  # as of specific date
    python3 tools/liberation_day_outcome_checker.py --hyp b73efac3     # single hypothesis
    python3 tools/liberation_day_outcome_checker.py --generate-postmortem  # generate completion text
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
import db
from tools.yfinance_utils import safe_download

# ─────────────────────────────────────────────────────────────────────────────
# Liberation Day trade registry
# ─────────────────────────────────────────────────────────────────────────────
TRADES = [
    {
        'hyp_id': 'b63a0168',
        'symbol': 'SPY',
        'direction': 'long',
        'signal': 'vix_spike_above_30_spy_long',
        'entry_date': '2026-03-30',
        'entry_price_override': 639.87,
        'target_days': 20,
        'target_exit': '2026-04-28',
        'expected_return_pct': 1.69,
        'notes': 'VIX crossed 30 on March 27. Entry March 30 open (Alpaca fill $639.87). 20d target.'
    },
    {
        'hyp_id': '2d94ac68',
        'symbol': 'VGNT',
        'direction': 'short',
        'signal': 'spinco_institutional_selling_short',
        'entry_date': '2026-04-01',
        'target_days': 2,
        'target_exit': '2026-04-03',
        'expected_return_pct': 5.69,
        'notes': 'Versigent (Aptiv spinco) first trading day. Short at 15:55 April 1. Target 2d.'
    },
    {
        'hyp_id': 'b73efac3',
        'symbol': 'WFC',
        'direction': 'short',
        'signal': 'tariff_escalation_bank_short',
        'entry_date': '2026-04-06',
        'target_days': 5,
        'target_exit': '2026-04-13',
        'expected_return_pct': 2.39,
        'notes': 'Liberation Day tariff escalation. Bank short. 5d target. Trigger was 04-03 but Good Friday -> fires 04-06.'
    },
    {
        'hyp_id': '132e9128',
        'symbol': 'AMD',
        'direction': 'short',
        'signal': 'tariff_escalation_semiconductor_short',
        'entry_date': '2026-04-06',
        'target_days': 5,
        'target_exit': '2026-04-13',
        'expected_return_pct': 3.05,
        'alt_target_days': 10,
        'alt_target_exit': '2026-04-18',
        'notes': 'Semiconductor tariff short. 5d or 10d target. Trigger was 04-03 but Good Friday -> fires 04-06.'
    },
    {
        'hyp_id': '14de5527',
        'symbol': 'QCOM',
        'direction': 'short',
        'signal': 'tariff_escalation_semiconductor_short',
        'entry_date': '2026-04-06',
        'target_days': 5,
        'target_exit': '2026-04-13',
        'expected_return_pct': 3.05,
        'alt_target_days': 10,
        'alt_target_exit': '2026-04-18',
        'notes': 'Semiconductor tariff short. 5d or 10d target. Trigger was 04-03 but Good Friday -> fires 04-06.'
    },
    {
        'hyp_id': 'b768e8d8',
        'symbol': 'GLD',
        'direction': 'long',
        'signal': 'tariff_escalation_gld_long',
        'entry_date': '2026-04-07',
        'target_days': 20,
        'target_exit': '2026-05-05',
        'expected_return_pct': 3.87,
        'notes': 'Liberation Day gold safe-haven. 20d target.'
    },
    {
        'hyp_id': '6e732966',
        'symbol': 'KRE',
        'direction': 'short',
        'signal': 'tariff_escalation_kre_bank_short',
        'entry_date': '2026-04-13',  # Updated: was 04-10, moved to 04-13 after WFC/AMD/QCOM close (~Apr 11)
        'target_days': 10,
        'target_exit': '2026-04-25',
        'expected_return_pct': 3.08,
        'early_exit': '2026-04-18',  # 5d manual close if profitable
        'notes': 'Regional bank ETF short. 10d target but close at 5d if profitable (rollback risk). Entry delayed to Apr 13 for capacity.'
    },
    {
        'hyp_id': '8c2f8cbb',
        'symbol': 'COST',
        'direction': 'long',
        'signal': 'tariff_defensive_retail_long',
        'entry_date': '2026-04-14',  # Updated: was 04-11, moved to 04-14 for capacity management
        'target_days': 10,
        'target_exit': '2026-04-28',
        'expected_return_pct': 3.57,
        'notes': 'Costco defensive retail long. 10d target. Entry delayed to Apr 14 for capacity.'
    },
    {
        'hyp_id': '35b63a23',
        'symbol': 'AEP',
        'direction': 'long',
        'signal': 'tariff_escalation_utility_long',
        'entry_date': '2026-04-14',
        'target_days': 10,
        'target_exit': '2026-04-28',
        'expected_return_pct': 3.79,
        'notes': 'AEP utility long. 10d target. April 7 entry (Good Friday, market closed April 3-4).'
    },
]


def get_prices(symbol: str, start: str, end: str) -> pd.Series:
    """Download close prices for a symbol."""
    data = safe_download(symbol, start=start, end=end)
    if data is None or data.empty:
        return pd.Series(dtype=float)
    close = data['Close']
    if isinstance(close, pd.DataFrame):
        close = close.squeeze()
    if not isinstance(close, pd.Series):
        return pd.Series(dtype=float)
    close.index = pd.to_datetime(close.index)
    return close


def compute_return(prices: pd.Series, entry_date: str, target_days: int, direction: str) -> dict:
    """Compute actual return from entry date over target_days trading days."""
    entry_dt = pd.Timestamp(entry_date)
    future = prices[prices.index >= entry_dt]

    if len(future) == 0:
        return {'status': 'no_data', 'days_available': 0}

    entry_price = float(future.iloc[0])
    days_available = len(future) - 1  # trading days after entry

    result = {
        'entry_price': entry_price,
        'entry_actual_date': future.index[0].date(),
        'days_available': days_available,
        'returns': {}
    }

    for d in [2, 5, 10, 20]:
        if len(future) > d:
            exit_price = float(future.iloc[d])
            raw_ret = (exit_price / entry_price - 1) * 100
            if direction == 'short':
                raw_ret = -raw_ret  # Short: profit if price falls
            result['returns'][f'{d}d_raw'] = round(raw_ret, 2)
            result['returns'][f'{d}d_date'] = future.index[d].date()

    return result


def compute_abnormal_return(symbol_prices: pd.Series, spy_prices: pd.Series,
                            entry_date: str, target_days: int, direction: str) -> dict:
    """Compute abnormal return (symbol - SPY benchmark)."""
    entry_dt = pd.Timestamp(entry_date)

    sym_future = symbol_prices[symbol_prices.index >= entry_dt]
    spy_future = spy_prices[spy_prices.index >= entry_dt]

    if len(sym_future) == 0 or len(spy_future) == 0:
        return {'status': 'no_data'}

    entry_sym = float(sym_future.iloc[0])
    entry_spy = float(spy_future.iloc[0])

    days_available = min(len(sym_future), len(spy_future)) - 1

    result = {
        'entry_price': entry_sym,
        'entry_actual': sym_future.index[0].date(),
        'days_available': days_available,
        'returns': {}
    }

    for d in [2, 5, 10, 20]:
        if len(sym_future) > d and len(spy_future) > d:
            exit_sym = float(sym_future.iloc[d])
            exit_spy = float(spy_future.iloc[d])

            sym_ret = (exit_sym / entry_sym - 1) * 100
            spy_ret = (exit_spy / entry_spy - 1) * 100

            if direction == 'long':
                abnormal = sym_ret - spy_ret
            else:  # short
                # Short profits when sym falls; abnormal = -(sym_ret - spy_ret)
                abnormal = -(sym_ret - spy_ret)

            result['returns'][f'{d}d_abnormal'] = round(abnormal, 2)
            result['returns'][f'{d}d_raw'] = round(sym_ret * (1 if direction == 'long' else -1), 2)
            result['returns'][f'{d}d_date'] = sym_future.index[d].date()

    return result


def check_trades(as_of_date: str = None, hyp_id_filter: str = None):
    """Check all Liberation Day trade outcomes."""
    today = datetime.now().strftime('%Y-%m-%d') if not as_of_date else as_of_date

    # Download SPY for benchmark
    spy_start = '2026-03-28'
    spy_end = (pd.Timestamp(today) + timedelta(days=60)).strftime('%Y-%m-%d')
    spy_prices = get_prices('SPY', spy_start, spy_end)

    print(f"\n{'='*80}")
    print(f"LIBERATION DAY 2026 OUTCOME CHECKER — As of {today}")
    print(f"{'='*80}\n")

    trades_to_check = [t for t in TRADES if hyp_id_filter is None or t['hyp_id'] == hyp_id_filter]

    all_results = []

    for trade in trades_to_check:
        sym = trade['symbol']
        entry = trade['entry_date']
        direction = trade['direction']
        target_d = trade['target_days']
        expected = trade['expected_return_pct']
        hyp_id = trade['hyp_id']

        # Check hypothesis status
        h = db.get_hypothesis_by_id(hyp_id)
        status = h['status'] if h else 'NOT_FOUND'

        # Download symbol prices
        sym_start = (pd.Timestamp(entry) - timedelta(days=5)).strftime('%Y-%m-%d')
        sym_end = spy_end
        try:
            sym_prices = get_prices(sym, sym_start, sym_end)
        except Exception as e:
            print(f"⚠️  {sym} ({hyp_id[:8]}) — PRICE ERROR: {e}")
            continue

        if len(sym_prices) == 0:
            print(f"⚠️  {sym} ({hyp_id[:8]}) — NO PRICE DATA")
            continue

        # Compute returns
        res = compute_abnormal_return(sym_prices, spy_prices, entry, target_d, direction)

        print(f"{'─'*70}")
        dir_arrow = '↑' if direction == 'long' else '↓'
        print(f"{dir_arrow} {sym} [{hyp_id[:8]}] {trade['signal']}")
        print(f"  Status: {status} | Entry: {entry} | Target: {target_d}d | Expected: +{expected}% abnormal")

        if res.get('status') == 'no_data':
            print(f"  ⚠️  No price data available yet")
        else:
            print(f"  Entry price: ${res['entry_price']:.2f} on {res['entry_actual']}")
            print(f"  Trading days available: {res['days_available']}")

            returns = res.get('returns', {})
            if returns:
                header = f"  {'Days':>6}  {'Abnormal':>10}  {'Raw':>10}  {'Exit Date':>12}  {'Status':>10}"
                print(header)
                print(f"  {'-'*60}")

                for d in [2, 5, 10, 20]:
                    if f'{d}d_abnormal' in returns:
                        abn = returns[f'{d}d_abnormal']
                        raw = returns[f'{d}d_raw']
                        dt = returns[f'{d}d_date']

                        # Is this the target horizon?
                        is_target = (d == target_d)
                        marker = '← TARGET' if is_target else ''

                        if is_target:
                            hit = abn >= expected * 0.5  # passes if at least 50% of expected
                            status_icon = '✅ PASS' if abn > 0 and abn >= 0.5 else ('🔴 FAIL' if abn < -0.5 else '⚠️ WEAK')
                        else:
                            status_icon = ''

                        print(f"  {d:>6}d  {abn:>+9.2f}%  {raw:>+9.2f}%  {str(dt):>12}  {status_icon} {marker}")

            # Early exit note
            if 'early_exit' in trade:
                print(f"  ⚡ EARLY EXIT REMINDER: Close manually at 5d ({trade['early_exit']}) if profitable")

        print(f"  Notes: {trade['notes']}")
        all_results.append({'trade': trade, 'result': res})

    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")

    ready_to_close = []
    still_running = []

    for r in all_results:
        trade = r['trade']
        result = r['result']
        target_d = trade['target_days']
        days_avail = result.get('days_available', 0)

        if days_avail >= target_d:
            ret_key = f"{target_d}d_abnormal"
            abn = result.get('returns', {}).get(ret_key, None)
            if abn is not None:
                direction_correct = abn > 0
                ready_to_close.append({
                    'sym': trade['symbol'],
                    'hyp_id': trade['hyp_id'],
                    'abnormal': abn,
                    'expected': trade['expected_return_pct'],
                    'passed': direction_correct and abn >= 0.5
                })
        else:
            remaining = target_d - days_avail
            still_running.append(f"{trade['symbol']} ({remaining} more trading days)")

    if ready_to_close:
        print("\n🏁 Ready to complete:")
        for r in ready_to_close:
            icon = '✅' if r['passed'] else '❌'
            print(f"  {icon} {r['sym']} [{r['hyp_id'][:8]}]: {r['abnormal']:+.2f}% abnormal (expected +{r['expected']:.1f}%)")

    if still_running:
        print("\n⏳ Still running:")
        for s in still_running:
            print(f"  • {s}")

    print()
    return all_results


def generate_postmortem(hyp_id: str, result: dict) -> str:
    """Generate completion text for a hypothesis."""
    trade = next((t for t in TRADES if t['hyp_id'] == hyp_id), None)
    if not trade:
        return f"No trade found for {hyp_id}"

    target_d = trade['target_days']
    returns = result.get('returns', {})
    abn = returns.get(f'{target_d}d_abnormal', None)
    raw = returns.get(f'{target_d}d_raw', None)

    if abn is None:
        return f"Insufficient data for {trade['symbol']} (need {target_d} trading days)"

    direction_correct = abn > 0.5

    text = f"""Post-mortem for {trade['symbol']} ({hyp_id[:8]}) — {trade['signal']}
Entry: {trade['entry_date']} | Target: {target_d}d | Expected: +{trade['expected_return_pct']:.1f}% abnormal
Actual {target_d}d abnormal vs SPY: {abn:+.2f}%  (raw: {raw:+.2f}%)
Direction correct: {'YES ✅' if direction_correct else 'NO ❌'}
Signal validated: {'YES' if direction_correct and abn >= 0.5 else 'NO'}
Notes: {trade.get('notes', '')}
Liberation Day context: April 2 2026 tariff announcement. Pre-event: SPY -8.6% from 60d peak.
"""
    return text


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Liberation Day 2026 Outcome Checker')
    parser.add_argument('--date', help='Check as of date (YYYY-MM-DD)', default=None)
    parser.add_argument('--hyp', help='Filter to single hypothesis ID', default=None)
    parser.add_argument('--generate-postmortem', action='store_true', help='Generate completion text')
    args = parser.parse_args()

    db.init_db()

    results = check_trades(as_of_date=args.date, hyp_id_filter=args.hyp)

    if args.generate_postmortem:
        print("\n" + "="*80)
        print("GENERATED POST-MORTEMS")
        print("="*80)
        for r in results:
            if r['result'].get('days_available', 0) >= r['trade']['target_days']:
                pm = generate_postmortem(r['trade']['hyp_id'], r['result'])
                print(pm)
