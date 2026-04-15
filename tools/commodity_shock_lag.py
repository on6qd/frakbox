"""Test: do large commodity daily moves predict next-day sector returns?

Usage: python3 tools/commodity_shock_lag.py --factor CL=F --target XLE --threshold 2.0 --break-date 2022-03-16
"""
import argparse
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.timeseries import get_aligned_returns
import numpy as np
from scipy import stats
from datetime import datetime

def run(factor, target, threshold_pct, break_date, benchmark='SPY'):
    # Fetch aligned daily returns
    ids = [factor, target, benchmark]
    returns = get_aligned_returns(ids, '2018-01-01', '2026-12-31')

    if returns is None or len(returns) < 100:
        return {"error": "Insufficient data"}
    
    # Filter to post-break period
    if break_date:
        returns = returns[returns.index >= break_date]
    
    factor_col = [c for c in returns.columns if factor.replace(':', '_').replace('=', '_').replace('^', '_') in c.replace(':', '_').replace('=', '_').replace('^', '_') or factor in c]
    target_col = [c for c in returns.columns if target in c]
    bench_col = [c for c in returns.columns if benchmark in c]
    
    if not factor_col or not target_col or not bench_col:
        return {"error": f"Column matching failed. Available: {list(returns.columns)}", "factor": factor_col, "target": target_col, "bench": bench_col}
    
    factor_col = factor_col[0]
    target_col = target_col[0]
    bench_col = bench_col[0]
    
    # Compute abnormal returns
    returns['target_abnormal'] = returns[target_col] - returns[bench_col]
    
    # Factor returns today
    factor_returns = returns[factor_col]
    
    # Next-day target abnormal returns
    returns['next_day_abnormal'] = returns['target_abnormal'].shift(-1)
    
    # Remove NaN
    valid = returns.dropna(subset=[factor_col, 'next_day_abnormal'])
    
    threshold = threshold_pct  # returns are already in percentage form
    
    # Big up days
    big_up = valid[valid[factor_col] > threshold]
    big_down = valid[valid[factor_col] < -threshold]
    normal = valid[(valid[factor_col].abs() <= threshold)]
    
    results = {
        'factor': factor,
        'target': target,
        'benchmark': benchmark,
        'threshold_pct': threshold_pct,
        'break_date': break_date,
        'period': f"{valid.index[0].strftime('%Y-%m-%d')} to {valid.index[-1].strftime('%Y-%m-%d')}",
        'total_days': len(valid),
    }
    
    # Split IS / OOS
    oos_start = '2025-01-01'
    is_data = valid[valid.index < oos_start]
    oos_data = valid[valid.index >= oos_start]
    
    for label, subset in [('is', is_data), ('oos', oos_data), ('full', valid)]:
        big_up_s = subset[subset[factor_col] > threshold]
        big_down_s = subset[subset[factor_col] < -threshold]
        
        section = {}
        
        # Big up days -> next day target
        if len(big_up_s) >= 5:
            up_next = big_up_s['next_day_abnormal']
            t_stat, p_val = stats.ttest_1samp(up_next, 0)
            section['big_up'] = {
                'n': len(big_up_s),
                'next_day_abnormal_mean': round(float(up_next.mean()) , 3),
                'next_day_abnormal_median': round(float(up_next.median()) , 3),
                'positive_rate': round(float((up_next > 0).mean()) * 100, 1),
                't_stat': round(float(t_stat), 3),
                'p_value': round(float(p_val), 4),
            }
        else:
            section['big_up'] = {'n': len(big_up_s), 'note': 'too few'}
            
        # Big down days -> next day target
        if len(big_down_s) >= 5:
            down_next = big_down_s['next_day_abnormal']
            t_stat, p_val = stats.ttest_1samp(down_next, 0)
            section['big_down'] = {
                'n': len(big_down_s),
                'next_day_abnormal_mean': round(float(down_next.mean()) , 3),
                'next_day_abnormal_median': round(float(down_next.median()) , 3),
                'positive_rate': round(float((down_next > 0).mean()) * 100, 1),
                't_stat': round(float(t_stat), 3),
                'p_value': round(float(p_val), 4),
            }
        else:
            section['big_down'] = {'n': len(big_down_s), 'note': 'too few'}
            
        # Combined: sign-aligned? (up days: positive = continuation, down days: negative = continuation)
        if len(big_up_s) >= 5 and len(big_down_s) >= 5:
            # For "continuation", we want factor_sign == target_sign the next day
            all_big = subset[subset[factor_col].abs() > threshold].copy()
            all_big['factor_sign'] = np.sign(all_big[factor_col])
            all_big['continuation'] = all_big['factor_sign'] * all_big['next_day_abnormal']
            t_stat, p_val = stats.ttest_1samp(all_big['continuation'], 0)
            section['combined_continuation'] = {
                'n': len(all_big),
                'mean_signed_abnormal': round(float(all_big['continuation'].mean()) , 3),
                'positive_rate': round(float((all_big['continuation'] > 0).mean()) * 100, 1),
                't_stat': round(float(t_stat), 3),
                'p_value': round(float(p_val), 4),
            }
        
        results[label] = section
    
    return results

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--factor', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--threshold', type=float, default=2.0)
    parser.add_argument('--break-date', default='2022-03-16')
    parser.add_argument('--benchmark', default='SPY')
    args = parser.parse_args()
    
    result = run(args.factor, args.target, args.threshold, args.break_date, args.benchmark)
    print(json.dumps(result, indent=2))
