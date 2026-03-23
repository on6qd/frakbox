"""
results_formatter.py — Utility for safely formatting measure_event_impact() results.

Addresses recurring friction: measure_event_impact() returns '?' string for horizons
with insufficient data, causing ValueError when formatting with :.2f%.

Usage:
    from tools.results_formatter import fmt, get_stat, print_impact_summary

CORRECT KEY NAMES for measure_event_impact() results:
    result['avg_abnormal_{h}']          # e.g. avg_abnormal_1d, avg_abnormal_5d
    result['median_abnormal_{h}']
    result['positive_rate_abnormal_{h}']  # % positive
    result['stdev_abnormal_{h}']
    result['skewness_abnormal_{h}']
    result['wilcoxon_p_abnormal_{h}']   # non-parametric p-value
    result['p_value_abnormal_{h}']      # t-test p-value (NOTE: NOT p_abnormal_{h})
    result['passes_multiple_testing']
    result['events_measured']
    result['bootstrap_ci_abnormal_{h}'] # dict with ci_lower, ci_upper, ci_excludes_zero
    result['avg_raw_{h}']               # raw return (not benchmark-adjusted)
    result['avg_sector_adj_{h}']        # sector-adjusted return

Horizons: 1d, 3d, 5d, 10d, 20d
"""


def fmt(value, decimals=2, suffix="%", na="?"):
    """
    Safely format a value that might be '?' string or float.

    Args:
        value: float or '?' string from measure_event_impact()
        decimals: decimal places (default 2)
        suffix: suffix to append (default '%')
        na: what to return for missing values (default '?')

    Returns:
        Formatted string like '2.34%' or '?' if value is missing
    """
    if value is None or value == '?' or value == 'N/A':
        return na
    try:
        return f"{float(value):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return na


def fmt_p(value, na="?"):
    """Format a p-value with 4 decimal places, no suffix."""
    if value is None or value == '?' or value == 'N/A':
        return na
    try:
        p = float(value)
        if p < 0.0001:
            return "<0.0001"
        return f"{p:.4f}"
    except (TypeError, ValueError):
        return na


def get_stat(result, horizon, stat_type='avg_abnormal', default='?'):
    """
    Safely retrieve a stat from measure_event_impact() result.

    Args:
        result: dict from measure_event_impact()
        horizon: '1d', '3d', '5d', '10d', or '20d'
        stat_type: prefix like 'avg_abnormal', 'p_value_abnormal', 'stdev_abnormal'
        default: value to return if key missing or value is '?'

    Returns:
        Float value or default
    """
    key = f"{stat_type}_{horizon}"
    val = result.get(key, default)
    if val == '?' or val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def print_impact_summary(result, title="Impact Summary"):
    """
    Print a formatted summary of measure_event_impact() results.
    Handles '?' values gracefully.
    """
    n = result.get('events_measured', '?')
    passes_mt = result.get('passes_multiple_testing', False)

    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    print(f"  N={n} events | Multiple testing: {'✓ PASS' if passes_mt else '✗ FAIL'}")

    if result.get('regime_filter'):
        excluded = result.get('regime_filtered_count', '?')
        print(f"  Regime filter: {result['regime_filter']} ({excluded} events excluded)")

    if result.get('data_quality_warning'):
        print(f"  ⚠ Warning: {result['data_quality_warning']}")

    print(f"\n  {'Horizon':<8} {'Abnormal':<12} {'StDev':<10} {'% Pos':<10} {'p-val':<10} {'CI excl 0?'}")
    print(f"  {'-'*70}")

    for h in ['1d', '3d', '5d', '10d', '20d']:
        avg = fmt(get_stat(result, h, 'avg_abnormal'))
        sd = fmt(get_stat(result, h, 'stdev_abnormal'))
        pos = fmt(get_stat(result, h, 'positive_rate_abnormal'))
        pval = fmt_p(result.get(f'p_value_abnormal_{h}', '?'))

        # Bootstrap CI
        ci = result.get(f'bootstrap_ci_abnormal_{h}', {})
        if isinstance(ci, dict):
            excl_zero = '✓' if ci.get('ci_excludes_zero', False) else '✗'
            ci_str = f"{fmt(ci.get('ci_lower'))} to {fmt(ci.get('ci_upper'))}"
            ci_info = f"{excl_zero} [{ci_str}]"
        else:
            ci_info = '?'

        print(f"  {h:<8} {avg:<12} {sd:<10} {pos:<10} {pval:<10} {ci_info}")

    print(f"\n  Entry mode: {result.get('entry_price_mode', '?')}")

    if result.get('avg_estimated_cost_pct'):
        print(f"  Est. round-trip cost: {fmt(result.get('avg_estimated_cost_pct'))}")


def check_passes_gates(result, min_magnitude=1.5, min_sample=10):
    """
    Check if a backtest result passes the key research gates.
    Returns dict with gate results and recommendations.
    """
    n = result.get('events_measured', 0)
    passes_mt = result.get('passes_multiple_testing', False)

    # Find best horizon
    best_horizon = None
    best_mag = 0
    for h in ['1d', '3d', '5d', '10d', '20d']:
        mag = abs(get_stat(result, h, 'avg_abnormal', 0))
        pval = get_stat(result, h, 'p_value_abnormal', 1.0)
        if mag > best_mag and pval < 0.05:
            best_mag = mag
            best_horizon = h

    gates = {
        'sample_sufficient': n >= min_sample,
        'passes_multiple_testing': passes_mt,
        'magnitude_sufficient': best_mag >= min_magnitude,
        'best_horizon': best_horizon,
        'best_magnitude': best_mag,
        'n': n,
    }
    gates['all_pass'] = all([
        gates['sample_sufficient'],
        gates['passes_multiple_testing'],
        gates['magnitude_sufficient'],
    ])

    return gates


if __name__ == '__main__':
    # Demo
    print("results_formatter.py — Usage examples:")
    print()
    print("  from tools.results_formatter import fmt, get_stat, print_impact_summary")
    print()
    print("  # Safe formatting:")
    print("  fmt(result.get('avg_abnormal_5d'))  # returns '2.34%' or '?'")
    print("  fmt_p(result.get('p_value_abnormal_5d'))  # returns '0.0123' or '?'")
    print()
    print("  # Safe stat retrieval:")
    print("  val = get_stat(result, '5d', 'avg_abnormal', default=0.0)")
    print()
    print("  # Full summary:")
    print("  print_impact_summary(result, title='My Signal')")
    print()
    print("CORRECT key names for p-values: p_value_abnormal_1d (NOT p_abnormal_1d)")
