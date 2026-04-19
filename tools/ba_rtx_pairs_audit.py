"""BA/RTX cointegration pairs trade audit.

Pre-registered hypothesis 5c30f121. Tests fragility of the scan hit by:
  1. Multiple IS/OOS windows (scan hit's 2020-2024 / 2024-2026 vs longer)
  2. Hedge-ratio stability
  3. Pairs trade simulation vs single-asset confound (BA-alone z-score)
  4. Structural break: pre-2022 vs post-2022
"""
import sys, os, json, numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.yfinance_utils import get_close_prices
from statsmodels.tsa.stattools import adfuller, coint
from statsmodels.regression.linear_model import OLS
import statsmodels.api as sm


def fetch_log(symbol, start, end):
    s = get_close_prices(symbol, start=start, end=end)
    s = s.dropna()
    return np.log(s)


def engle_granger(la, lb):
    """Return hedge_ratio, spread, adf_p, half_life."""
    X = sm.add_constant(lb)
    m = OLS(la, X).fit()
    beta = m.params.iloc[1]
    alpha = m.params.iloc[0]
    spread = la - (alpha + beta * lb)
    adf_stat, adf_p, *_ = adfuller(spread.dropna(), maxlag=1)
    # half-life from AR(1) on diff
    s = spread.dropna()
    ds = s.diff().dropna()
    sl = s.shift(1).dropna().loc[ds.index]
    mm = OLS(ds, sm.add_constant(sl)).fit()
    phi = mm.params.iloc[1]
    hl = -np.log(2) / np.log(1 + phi) if (1 + phi) > 0 and phi < 0 else np.nan
    return {'beta': beta, 'alpha': alpha, 'spread': spread, 'adf_p': adf_p, 'half_life': hl}


def pairs_trade_simulate(spread, z_entry=2.0, z_exit=0.5, z_stop=3.0, win=60, cost_bp=20):
    """Simulate a z-score entry/exit strategy on a spread series.
    cost_bp applied round-trip (enter+exit). Returns trade log and cumulative return."""
    z = (spread - spread.rolling(win).mean()) / spread.rolling(win).std()
    z = z.dropna()
    spread_r = spread.loc[z.index].diff().fillna(0)
    pos = 0  # +1 long spread, -1 short spread
    trades = []
    entry_idx = None
    entry_z = None
    equity = [0.0]
    pnl_series = pd.Series(0.0, index=z.index)
    last_pos = 0
    for dt, zv in z.items():
        # close on exit or stop
        if pos != 0:
            if (pos > 0 and zv >= -z_exit) or (pos < 0 and zv <= z_exit) or abs(zv) > z_stop:
                # close
                end_spread = spread.loc[dt]
                pnl = pos * (end_spread - spread.loc[entry_idx])
                # apply cost
                pnl -= cost_bp / 10000.0 * 2  # round-trip
                trades.append({'entry': entry_idx, 'exit': dt, 'entry_z': entry_z, 'exit_z': zv, 'pos': pos, 'pnl': pnl})
                pos = 0
                entry_idx = None
        if pos == 0:
            if zv <= -z_entry:
                pos = 1; entry_idx = dt; entry_z = zv
            elif zv >= z_entry:
                pos = -1; entry_idx = dt; entry_z = zv
        # pnl over period
        if last_pos != 0:
            pnl_series.loc[dt] = last_pos * spread_r.loc[dt]
        last_pos = pos
    # close final open trade at end
    if pos != 0 and entry_idx is not None:
        end_spread = spread.iloc[-1]
        pnl = pos * (end_spread - spread.loc[entry_idx])
        pnl -= cost_bp / 10000.0 * 2
        trades.append({'entry': entry_idx, 'exit': spread.index[-1], 'entry_z': entry_z, 'exit_z': z.iloc[-1], 'pos': pos, 'pnl': pnl, 'open': True})
    total = sum(t['pnl'] for t in trades)
    rets = pnl_series
    sharpe = (rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else 0.0
    return {'n_trades': len(trades), 'total_log_pnl': total, 'sharpe': sharpe, 'trades': trades, 'rets': rets}


def single_asset_zscore(ls, z_entry=2.0, z_exit=0.5, z_stop=3.0, win=60, cost_bp=20):
    """Run identical strategy on single log-price series (BA alone)."""
    # use price deviation from rolling mean as the "spread"
    return pairs_trade_simulate(ls, z_entry=z_entry, z_exit=z_exit, z_stop=z_stop, win=win, cost_bp=cost_bp)


def main():
    results = {}
    ba_full = fetch_log('BA', '2014-01-01', '2026-04-19')
    rtx_full = fetch_log('RTX', '2014-01-01', '2026-04-19')
    aligned = pd.concat([ba_full, rtx_full], axis=1).dropna()
    aligned.columns = ['BA', 'RTX']
    print(f"Loaded: BA {len(ba_full)} days, RTX {len(rtx_full)} days, aligned {len(aligned)}")

    windows = [
        ('scan_hit_window', '2020-01-01', '2024-01-01', '2024-01-01', '2026-04-19'),
        ('prereg_long_oos', '2020-01-01', '2023-01-01', '2023-01-01', '2026-04-19'),
        ('full_2015_window', '2015-01-01', '2023-01-01', '2023-01-01', '2026-04-19'),
        ('pre_covid', '2015-01-01', '2020-01-01', '2020-01-01', '2026-04-19'),
        ('post_covid_only', '2022-01-01', '2024-01-01', '2024-01-01', '2026-04-19'),
    ]

    for wname, is_s, is_e, oos_s, oos_e in windows:
        is_data = aligned.loc[is_s:is_e]
        oos_data = aligned.loc[oos_s:oos_e]
        if len(is_data) < 100 or len(oos_data) < 60:
            continue
        is_res = engle_granger(is_data['BA'], is_data['RTX'])
        # OOS: apply IS hedge ratio to OOS prices
        oos_spread = oos_data['BA'] - (is_res['alpha'] + is_res['beta'] * oos_data['RTX'])
        oos_adf_stat, oos_adf_p, *_ = adfuller(oos_spread.dropna(), maxlag=1)
        # OOS half-life
        oos_s_series = oos_spread.dropna()
        ds = oos_s_series.diff().dropna()
        sl = oos_s_series.shift(1).dropna().loc[ds.index]
        mm = OLS(ds, sm.add_constant(sl)).fit()
        phi = mm.params.iloc[1]
        oos_hl = -np.log(2) / np.log(1 + phi) if (1 + phi) > 0 and phi < 0 else np.nan

        # Re-estimate OOS hedge ratio to check drift
        oos_refit = engle_granger(oos_data['BA'], oos_data['RTX'])
        hedge_drift = abs(oos_refit['beta'] - is_res['beta']) / abs(is_res['beta']) if is_res['beta'] != 0 else np.nan

        # Pairs trade on OOS spread (using IS hedge ratio, realistic OOS trading)
        pairs_res = pairs_trade_simulate(oos_spread)
        # Single-asset confound: BA alone log-price z-score mean-reversion
        ba_alone = oos_data['BA']
        single_res = single_asset_zscore(ba_alone)

        results[wname] = {
            'is_period': f"{is_s} -> {is_e}",
            'oos_period': f"{oos_s} -> {oos_e}",
            'is_n': len(is_data),
            'oos_n': len(oos_data),
            'is_hedge_ratio': float(is_res['beta']),
            'oos_refit_hedge_ratio': float(oos_refit['beta']),
            'hedge_drift_pct': float(hedge_drift * 100) if not np.isnan(hedge_drift) else None,
            'is_adf_p': float(is_res['adf_p']),
            'oos_adf_p': float(oos_adf_p),
            'is_halflife': float(is_res['half_life']) if not np.isnan(is_res['half_life']) else None,
            'oos_halflife': float(oos_hl) if not np.isnan(oos_hl) else None,
            'pairs_sharpe': float(pairs_res['sharpe']),
            'pairs_trades': int(pairs_res['n_trades']),
            'pairs_total_log_pnl': float(pairs_res['total_log_pnl']),
            'ba_alone_sharpe': float(single_res['sharpe']),
            'ba_alone_trades': int(single_res['n_trades']),
            'ba_alone_total_log_pnl': float(single_res['total_log_pnl']),
            'pairs_minus_single_sharpe': float(pairs_res['sharpe'] - single_res['sharpe']),
        }

    print("\n" + "=" * 80)
    print(json.dumps(results, indent=2, default=str))

    # Summary judgment
    print("\n" + "=" * 80)
    print("PRE-REGISTERED CRITERIA CHECK (prereg_long_oos):")
    r = results.get('prereg_long_oos', {})
    if r:
        checks = {
            'OOS ADF p<0.05': r['oos_adf_p'] < 0.05,
            'Hedge drift <30%': (r['hedge_drift_pct'] is not None) and r['hedge_drift_pct'] < 30,
            'OOS half-life <=90d': (r['oos_halflife'] is not None) and 0 < r['oos_halflife'] <= 90,
            'OOS pairs Sharpe >1.0': r['pairs_sharpe'] > 1.0,
            'Pairs - BA-alone Sharpe >=0.5': r['pairs_minus_single_sharpe'] >= 0.5,
            '>=5 OOS round-trips': r['pairs_trades'] >= 5,
        }
        for k, v in checks.items():
            print(f"  {'PASS' if v else 'FAIL'}: {k}")
        passed = sum(checks.values())
        total = len(checks)
        print(f"\n  OVERALL: {passed}/{total} criteria passed")


if __name__ == '__main__':
    main()
