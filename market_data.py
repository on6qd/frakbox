"""
Market data utilities — fetch historical prices and measure event impacts.

Uses yfinance for historical data (goes back to IPO date for most stocks).

IMPORTANT: All impact measurements compute ABNORMAL returns — the stock's return
minus what the benchmark (SPY) did over the same period. This isolates the event
effect from broad market moves.
"""

import math
import yfinance as yf
from datetime import datetime, timedelta
from scipy.stats import ttest_1samp, wilcoxon, norm, skew as scipy_skew


# Approximate weights of large constituents in sector ETFs.
# Used to warn about circular reference in sector-adjusted returns.
# Updated periodically — does not need to be exact.
SECTOR_ETF_MAJOR_CONSTITUENTS = {
    "XLK": {"AAPL": 0.22, "MSFT": 0.21, "NVDA": 0.06},
    "XLV": {"LLY": 0.12, "UNH": 0.10, "JNJ": 0.07, "ABBV": 0.07},
    "XLF": {"BRK-B": 0.14, "JPM": 0.10, "V": 0.08, "MA": 0.07},
    "XLE": {"XOM": 0.23, "CVX": 0.17},
    "XLY": {"AMZN": 0.22, "TSLA": 0.15, "HD": 0.09},
    "XLC": {"META": 0.22, "GOOGL": 0.12, "GOOG": 0.10},
    "XLI": {"GE": 0.05, "CAT": 0.05, "RTX": 0.05},
    "XLP": {"PG": 0.15, "COST": 0.11, "WMT": 0.10, "KO": 0.10},
    "XLU": {"NEE": 0.15, "SO": 0.08, "DUK": 0.07},
    "XLRE": {"PLD": 0.13, "AMT": 0.10, "EQIX": 0.08},
    "XLB": {"LIN": 0.18, "SHW": 0.08, "FCX": 0.07},
}


def get_price_history(symbol, days=90):
    """
    Fetch daily OHLCV data. Most recent last.
    Can go back decades — not limited to 2 years.
    """
    end = datetime.now()
    start = end - timedelta(days=days)

    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), interval="1d")

    if df.empty:
        return []

    prices = []
    for date, row in df.iterrows():
        prices.append({
            "date": date.strftime("%Y-%m-%d"),
            "open": round(row["Open"], 2),
            "high": round(row["High"], 2),
            "low": round(row["Low"], 2),
            "close": round(row["Close"], 2),
            "volume": int(row["Volume"]),
        })

    return prices


def get_price_around_date(symbol, event_date, days_before=5, days_after=20,
                          benchmark="SPY", event_timing="unknown"):
    """
    Fetch prices around a specific event date and compute abnormal returns.

    Returns raw returns, benchmark returns, and abnormal returns (raw - benchmark)
    at 1d, 3d, 5d, 10d, 20d horizons.

    Args:
        event_timing: "pre_market", "intraday", "after_hours", or "unknown"
            - pre_market/intraday/unknown: reference price = close of day BEFORE event
            - after_hours: reference price = close of event day (before the event moved it)
              Post-event returns start from next trading day.
    """
    event_dt = datetime.strptime(event_date, "%Y-%m-%d")
    start = event_dt - timedelta(days=days_before + 10)
    end = event_dt + timedelta(days=days_after + 10)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    # Fetch both stock and benchmark
    stock_df = yf.Ticker(symbol).history(start=start_str, end=end_str, interval="1d")
    if stock_df.empty:
        return {"error": f"No data for {symbol} around {event_date}"}

    bench_df = yf.Ticker(benchmark).history(start=start_str, end=end_str, interval="1d") if symbol != benchmark else stock_df

    # Build date-indexed lookups
    stock_by_date = {d.strftime("%Y-%m-%d"): round(row["Close"], 2) for d, row in stock_df.iterrows()}
    bench_by_date = {d.strftime("%Y-%m-%d"): round(row["Close"], 2) for d, row in bench_df.iterrows()}

    # Split into pre and post based on event timing
    stock_dates = sorted(stock_by_date.keys())

    if event_timing == "after_hours":
        # After-hours: event day close is the reference (before the event)
        # Post-event starts from the NEXT trading day
        pre_dates = [d for d in stock_dates if d <= event_date]
        post_dates = [d for d in stock_dates if d > event_date]
    else:
        # Pre-market/intraday/unknown: day before event is reference
        # Event day itself captures the reaction
        pre_dates = [d for d in stock_dates if d < event_date]
        post_dates = [d for d in stock_dates if d >= event_date]

    if not pre_dates or not post_dates:
        return {"error": "Not enough data around event date"}

    pre_event_price = stock_by_date[pre_dates[-1]]
    pre_event_bench = bench_by_date.get(pre_dates[-1])

    impact = {
        "symbol": symbol,
        "benchmark": benchmark,
        "event_date": event_date,
        "event_timing": event_timing,
        "pre_event_price": pre_event_price,
    }

    for horizon_label, horizon_idx in [("1d", 0), ("3d", 2), ("5d", 4), ("10d", 9), ("20d", 19)]:
        if len(post_dates) > horizon_idx:
            target_date = post_dates[horizon_idx]

            # Raw return
            post_price = stock_by_date[target_date]
            raw_return = ((post_price - pre_event_price) / pre_event_price) * 100

            # Benchmark return
            bench_return = 0
            if pre_event_bench and target_date in bench_by_date:
                bench_post = bench_by_date[target_date]
                bench_return = ((bench_post - pre_event_bench) / pre_event_bench) * 100

            # Abnormal return = raw - benchmark
            abnormal_return = raw_return - bench_return

            impact[f"raw_{horizon_label}"] = round(raw_return, 2)
            impact[f"bench_{horizon_label}"] = round(bench_return, 2)
            impact[f"abnormal_{horizon_label}"] = round(abnormal_return, 2)

    impact["pre_prices"] = [{"date": d, "close": stock_by_date[d]} for d in pre_dates[-days_before:]]
    impact["post_prices"] = [{"date": d, "close": stock_by_date[d]} for d in post_dates[:days_after]]

    return impact


def measure_event_impact(symbol=None, event_dates=None, benchmark="SPY", sector_etf=None,
                         event_timing="unknown", known_events=None, regime_filter=None):
    """
    Measure abnormal price impact across multiple instances of the same event type.

    Supports two calling conventions:
    1. Single-symbol: measure_event_impact("AAPL", ["2024-01-15", "2024-04-20"])
    2. Multi-symbol:  measure_event_impact(event_dates=[
           {"symbol": "AAPL", "date": "2024-01-15"},
           {"symbol": "MSFT", "date": "2024-04-20", "timing": "after_hours"},
       ])

    For each event, computes:
    - Raw return (what the stock did)
    - Benchmark return (what SPY did — controls for market-wide moves)
    - Abnormal return (raw - benchmark — the event-specific effect)
    - Optionally: sector-adjusted return (raw - sector ETF, corrected for large constituents)

    Args:
        symbol: Ticker to measure (None for multi-symbol mode)
        event_dates: List of date strings, or list of dicts with symbol/date/timing keys
        benchmark: Market benchmark (default SPY)
        sector_etf: Optional sector ETF (e.g., XLV for healthcare, XLF for financials)
        event_timing: Default timing if event_dates are strings (not dicts)
        known_events: Optional list of {"symbol", "date"} dicts for contamination checking.
                      If provided, flags events where another known event falls within
                      the measurement window.
        regime_filter: Optional VIX regime filter — "calm" (VIX<20), "elevated" (20-30),
                      or "crisis" (VIX>30). Only events matching the specified regime are
                      included in the analysis. Uses ^VIX close on event date.
    """
    if event_dates is None:
        return {"error": "event_dates is required"}

    impacts = []
    errors = []
    symbols_seen = set()
    regime_filtered_count = 0

    # Pre-fetch VIX data if regime filtering is requested
    vix_by_date = {}
    if regime_filter:
        regime_thresholds = {"calm": (0, 20), "elevated": (20, 30), "crisis": (30, 999)}
        if regime_filter not in regime_thresholds:
            return {"error": f"Invalid regime_filter '{regime_filter}'. Use 'calm', 'elevated', or 'crisis'."}
        # Determine date range for VIX fetch
        all_dates = []
        for de in event_dates:
            d = de["date"] if isinstance(de, dict) else de
            all_dates.append(d)
        if all_dates:
            min_date = min(all_dates)
            max_date = max(all_dates)
            vix_start = (datetime.strptime(min_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
            vix_end = (datetime.strptime(max_date, "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")
            vix_df = yf.Ticker("^VIX").history(start=vix_start, end=vix_end, interval="1d")
            if not vix_df.empty:
                vix_by_date = {d.strftime("%Y-%m-%d"): round(row["Close"], 2) for d, row in vix_df.iterrows()}

    for date_entry in event_dates:
        # Resolve symbol and date from the entry
        if isinstance(date_entry, dict):
            event_symbol = date_entry.get("symbol", symbol)
            date = date_entry["date"]
            timing = date_entry.get("timing", event_timing)
        elif isinstance(date_entry, (list, tuple)) and len(date_entry) == 2:
            event_symbol, date = date_entry
            timing = event_timing
        else:
            event_symbol = symbol
            date = date_entry
            timing = event_timing

        if event_symbol is None:
            errors.append({"date": date, "error": "No symbol specified"})
            continue

        # Regime filter: skip events outside the specified VIX regime
        if regime_filter and vix_by_date:
            vix_val = vix_by_date.get(date)
            # If exact date not found, try nearest prior date
            if vix_val is None:
                prior_dates = [d for d in sorted(vix_by_date.keys()) if d <= date]
                if prior_dates:
                    vix_val = vix_by_date[prior_dates[-1]]
            if vix_val is not None:
                lo, hi = regime_thresholds[regime_filter]
                if not (lo <= vix_val < hi):
                    regime_filtered_count += 1
                    continue

        symbols_seen.add(event_symbol)

        try:
            impact = get_price_around_date(event_symbol, date, benchmark=benchmark,
                                           event_timing=timing)
            if "error" not in impact:
                # Sector-adjusted returns with circular reference correction
                if sector_etf and sector_etf != event_symbol:
                    sector_impact = get_price_around_date(sector_etf, date,
                                                         benchmark=benchmark,
                                                         event_timing=timing)
                    if "error" not in sector_impact:
                        # Check for circular reference: is this stock a major constituent?
                        weight = SECTOR_ETF_MAJOR_CONSTITUENTS.get(sector_etf, {}).get(event_symbol, 0)
                        for h in ["1d", "3d", "5d", "10d", "20d"]:
                            raw_key = f"raw_{h}"
                            if raw_key in impact and raw_key in sector_impact:
                                sector_return = sector_impact[raw_key]
                                if weight > 0.05:
                                    # Correct for circular reference:
                                    # sector_return includes the stock's own move
                                    # Remove the stock's contribution to get a clean sector return
                                    stock_return = impact[raw_key]
                                    adjusted_sector = (sector_return - weight * stock_return) / (1 - weight)
                                    impact[f"sector_adj_{h}"] = round(
                                        impact[raw_key] - adjusted_sector, 2
                                    )
                                else:
                                    impact[f"sector_adj_{h}"] = round(
                                        impact[raw_key] - sector_return, 2
                                    )
                        if weight > 0.05:
                            impact["sector_adjustment_note"] = (
                                f"{event_symbol} is ~{weight:.0%} of {sector_etf}. "
                                f"Sector return adjusted to remove {event_symbol}'s contribution."
                            )
                impacts.append(impact)
            else:
                errors.append({"date": date, "symbol": event_symbol, "error": impact["error"]})
        except Exception as e:
            errors.append({"date": date, "symbol": event_symbol, "error": str(e)})
            continue

    if not impacts:
        return {"error": "Could not measure any events", "attempted": len(event_dates),
                "errors": errors}

    # Data quality check (exclude intentionally regime-filtered events from drop rate)
    eligible_events = len(event_dates) - regime_filtered_count
    drop_rate = (eligible_events - len(impacts)) / eligible_events * 100 if eligible_events > 0 else 0
    data_quality_warning = None
    if drop_rate > 30:
        data_quality_warning = (
            f"WARNING: {drop_rate:.0f}% of eligible events failed to produce data "
            f"({len(impacts)}/{eligible_events} succeeded"
            f"{f', {regime_filtered_count} excluded by regime filter' if regime_filtered_count else ''}). "
            f"Results may be unreliable — investigate data quality before forming hypotheses."
        )

    stats = {
        "symbol": symbol if symbol else None,
        "symbols": sorted(symbols_seen),
        "multi_symbol": len(symbols_seen) > 1,
        "benchmark": benchmark,
        "sector_etf": sector_etf,
        "event_timing": event_timing,
        "regime_filter": regime_filter,
        "regime_filtered_count": regime_filtered_count if regime_filter else None,
        "events_measured": len(impacts),
        "events_attempted": len(event_dates),
        "events_failed": len(errors),
        "drop_rate_pct": round(drop_rate, 1),
        "data_quality_warning": data_quality_warning,
        "errors": errors if errors else None,
        "individual_impacts": impacts,
    }

    # Aggregate both raw and abnormal returns
    for return_type in ["raw", "abnormal", "sector_adj"]:
        for horizon in ["1d", "3d", "5d", "10d", "20d"]:
            key = f"{return_type}_{horizon}"
            returns = [i[key] for i in impacts if key in i]
            if returns:
                positive = sum(1 for r in returns if r > 0)
                stats[f"avg_{key}"] = round(sum(returns) / len(returns), 2)
                sorted_returns = sorted(returns)
                mid = len(sorted_returns) // 2
                if len(sorted_returns) % 2 == 0:
                    stats[f"median_{key}"] = round((sorted_returns[mid - 1] + sorted_returns[mid]) / 2, 2)
                else:
                    stats[f"median_{key}"] = round(sorted_returns[mid], 2)
                stats[f"positive_rate_{key}"] = round(positive / len(returns) * 100, 1)
                stats[f"min_{key}"] = round(min(returns), 2)
                stats[f"max_{key}"] = round(max(returns), 2)
                stats[f"stdev_{key}"] = round(_stdev(returns), 2)

    # Statistical significance for abnormal returns using scipy
    significant_horizons = []
    for horizon in ["1d", "3d", "5d", "10d", "20d"]:
        key = f"abnormal_{horizon}"
        returns = [i[key] for i in impacts if key in i]
        if len(returns) >= 3:
            t_stat, p_value = ttest_1samp(returns, 0)
            stats[f"t_stat_{key}"] = round(float(t_stat), 3)
            stats[f"p_value_{key}"] = round(float(p_value), 4)
            stats[f"significant_{key}"] = p_value < 0.05
            if p_value < 0.05:
                significant_horizons.append(horizon)

            # Skewness warning — t-test unreliable on highly skewed small samples
            skewness = float(scipy_skew(returns))
            stats[f"skewness_{key}"] = round(skewness, 2)
            if abs(skewness) > 1.0:
                stats[f"skewness_warning_{key}"] = (
                    f"High skewness ({skewness:.2f}) — t-test may be unreliable. "
                    f"Check Wilcoxon p-value for robustness."
                )

            # Wilcoxon signed-rank test (non-parametric robustness check)
            nonzero_returns = [r for r in returns if r != 0]
            if len(nonzero_returns) >= 6:
                try:
                    _, wilcoxon_p = wilcoxon(nonzero_returns)
                    stats[f"wilcoxon_p_{key}"] = round(float(wilcoxon_p), 4)
                    # Flag divergence between t-test and Wilcoxon
                    t_sig = p_value < 0.05
                    w_sig = wilcoxon_p < 0.05
                    if t_sig and not w_sig:
                        stats[f"robustness_warning_{key}"] = (
                            f"t-test significant (p={p_value:.4f}) but Wilcoxon not "
                            f"(p={wilcoxon_p:.4f}). Significance may be driven by outliers."
                        )
                except ValueError:
                    pass  # Wilcoxon can fail with identical values

    # Multiple testing correction summary
    stats["significant_horizons"] = significant_horizons
    stats["num_significant_horizons"] = len(significant_horizons)

    if len(significant_horizons) >= 2:
        stats["passes_multiple_testing"] = True
        stats["multiple_testing_note"] = (
            f"{len(significant_horizons)} horizons significant at p<0.05 — passes multi-horizon check."
        )
    elif len(significant_horizons) == 1:
        h = significant_horizons[0]
        p = stats.get(f"p_value_abnormal_{h}", 1.0)
        if p < 0.01:
            stats["passes_multiple_testing"] = True
            stats["multiple_testing_note"] = (
                f"1 horizon ({h}) significant at p<0.01 — passes Bonferroni-adjusted threshold."
            )
        else:
            stats["passes_multiple_testing"] = False
            stats["multiple_testing_note"] = (
                f"Only 1 horizon ({h}) significant at p={p:.4f}, which does not survive "
                f"multiple testing correction (need p<0.01 for single-horizon or 2+ horizons at p<0.05). "
                f"This may be a false positive."
            )
    else:
        stats["passes_multiple_testing"] = False
        stats["multiple_testing_note"] = "No horizons reached significance at p<0.05."

    # Power analysis: given the observed effect and variance, how many samples do we need?
    for horizon in ["1d", "3d", "5d", "10d", "20d"]:
        key = f"abnormal_{horizon}"
        avg_key = f"avg_{key}"
        stdev_key = f"stdev_{key}"
        if avg_key in stats and stdev_key in stats and stats[stdev_key] > 0:
            recommended_n = compute_required_sample_size(
                abs(stats[avg_key]), stats[stdev_key]
            )
            stats[f"recommended_n_{key}"] = recommended_n
            stats[f"sample_sufficient_{key}"] = len(impacts) >= recommended_n

    # Cross-event contamination check
    if known_events:
        contamination = check_event_contamination(
            [{"symbol": i["symbol"], "date": i["event_date"]} for i in impacts],
            known_events=known_events,
        )
        if contamination:
            stats["contamination_warnings"] = contamination

    return stats


def check_event_contamination(events, known_events=None, window_days=20):
    """
    Check for overlapping events that could contaminate measurement windows.

    Args:
        events: List of {"symbol": str, "date": str} dicts being measured
        known_events: Additional known events to check against. If None, checks
                     only within the events list itself.
        window_days: Size of measurement window in calendar days

    Returns:
        List of warning dicts describing contaminated event pairs
    """
    all_events = list(events)
    if known_events:
        all_events.extend(known_events)

    warnings = []
    for i, ev in enumerate(events):
        ev_date = datetime.strptime(ev["date"], "%Y-%m-%d")
        for j, other in enumerate(all_events):
            if ev["symbol"] != other["symbol"]:
                continue
            # Skip self-comparison (same index in the original events list)
            if j < len(events) and j == i:
                continue
            other_date = datetime.strptime(other["date"], "%Y-%m-%d")
            gap = abs((ev_date - other_date).days)
            if 0 < gap <= window_days:
                warnings.append({
                    "event": ev,
                    "conflicting_event": other,
                    "gap_days": gap,
                    "warning": (
                        f"{ev['symbol']} has events on {ev['date']} and {other['date']} "
                        f"({gap} days apart). Measurement windows overlap — "
                        f"price impact may be contaminated."
                    ),
                })
    return warnings


def compute_required_sample_size(effect_size, stdev, alpha=0.05, power=0.8):
    """
    Compute required sample size for a one-sample t-test.

    Given the observed effect size and standard deviation, how many samples
    do we need to detect this effect with the specified power?

    Args:
        effect_size: Expected mean abnormal return (absolute value)
        stdev: Standard deviation of abnormal returns
        alpha: Significance level (default 0.05)
        power: Desired statistical power (default 0.80)

    Returns:
        Required sample size (integer, minimum 3)
    """
    if effect_size <= 0 or stdev <= 0:
        return 999  # Cannot compute — need positive values

    z_alpha = norm.ppf(1 - alpha / 2)
    z_beta = norm.ppf(power)
    n = math.ceil(((z_alpha + z_beta) * stdev / effect_size) ** 2)
    return max(3, n)


def apply_cross_category_fdr(category_p_values, alpha=0.05):
    """
    Apply Benjamini-Hochberg FDR correction across multiple event categories.

    When testing N categories, some will be significant by chance. This adjusts
    p-values to control the false discovery rate.

    Args:
        category_p_values: Dict of {category_name: min_p_value_across_horizons}
        alpha: Desired FDR level (default 0.05)

    Returns:
        Dict of {category_name: {"raw_p": float, "adjusted_p": float, "significant": bool}}
    """
    if not category_p_values:
        return {}

    # Sort by p-value
    sorted_cats = sorted(category_p_values.items(), key=lambda x: x[1])
    m = len(sorted_cats)
    results = {}

    for rank, (cat, raw_p) in enumerate(sorted_cats, 1):
        # BH adjusted p-value: p * m / rank
        adjusted_p = min(1.0, raw_p * m / rank)
        results[cat] = {
            "raw_p": round(raw_p, 4),
            "adjusted_p": round(adjusted_p, 4),
            "bh_rank": rank,
            "significant": adjusted_p < alpha,
        }

    return results


def _stdev(values):
    """Standard deviation (sample)."""
    if len(values) < 2:
        return 0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return variance ** 0.5
