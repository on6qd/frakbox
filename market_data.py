"""
Market data utilities — fetch historical prices and measure event impacts.

Uses yfinance for historical data (goes back to IPO date for most stocks).

IMPORTANT: All impact measurements compute ABNORMAL returns — the stock's return
minus what the benchmark (SPY) did over the same period. This isolates the event
effect from broad market moves.
"""

import yfinance as yf
from datetime import datetime, timedelta


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

    for horizon_label, horizon_idx in [("1d", 1), ("3d", 3), ("5d", 5), ("10d", 10), ("20d", 20)]:
        if len(post_dates) > horizon_idx - 1:
            target_date = post_dates[min(horizon_idx, len(post_dates) - 1)]

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


def measure_event_impact(symbol, event_dates, benchmark="SPY", sector_etf=None,
                         event_timing="unknown"):
    """
    Measure abnormal price impact across multiple instances of the same event type.

    For each event, computes:
    - Raw return (what the stock did)
    - Benchmark return (what SPY did — controls for market-wide moves)
    - Abnormal return (raw - benchmark — the event-specific effect)
    - Optionally: sector-adjusted return (raw - sector ETF)

    Args:
        symbol: Ticker to measure
        event_dates: List of "YYYY-MM-DD" strings, or list of dicts with
                     {"date": "YYYY-MM-DD", "timing": "pre_market"|"after_hours"|...}
        benchmark: Market benchmark (default SPY)
        sector_etf: Optional sector ETF (e.g., XLV for healthcare, XLF for financials)
        event_timing: Default timing if event_dates are strings (not dicts)
    """
    impacts = []
    errors = []
    for date_entry in event_dates:
        # Support both plain date strings and dicts with timing info
        if isinstance(date_entry, dict):
            date = date_entry["date"]
            timing = date_entry.get("timing", event_timing)
        else:
            date = date_entry
            timing = event_timing

        try:
            impact = get_price_around_date(symbol, date, benchmark=benchmark,
                                           event_timing=timing)
            if "error" not in impact:
                # Optionally add sector-adjusted returns
                if sector_etf and sector_etf != symbol:
                    sector_impact = get_price_around_date(sector_etf, date,
                                                         benchmark=benchmark,
                                                         event_timing=timing)
                    if "error" not in sector_impact:
                        for h in ["1d", "3d", "5d", "10d", "20d"]:
                            raw_key = f"raw_{h}"
                            if raw_key in impact and raw_key in sector_impact:
                                impact[f"sector_adj_{h}"] = round(
                                    impact[raw_key] - sector_impact[raw_key], 2
                                )
                impacts.append(impact)
            else:
                errors.append({"date": date, "error": impact["error"]})
        except Exception as e:
            errors.append({"date": date, "error": str(e)})
            continue

    if not impacts:
        return {"error": "Could not measure any events", "attempted": len(event_dates),
                "errors": errors}

    # Data quality check
    drop_rate = (len(event_dates) - len(impacts)) / len(event_dates) * 100
    data_quality_warning = None
    if drop_rate > 30:
        data_quality_warning = (
            f"WARNING: {drop_rate:.0f}% of events failed to produce data "
            f"({len(impacts)}/{len(event_dates)} succeeded). "
            f"Results may be unreliable — investigate data quality before forming hypotheses."
        )

    stats = {
        "symbol": symbol,
        "benchmark": benchmark,
        "sector_etf": sector_etf,
        "event_timing": event_timing,
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

    # Statistical significance for abnormal returns
    significant_horizons = []
    for horizon in ["1d", "3d", "5d", "10d", "20d"]:
        key = f"abnormal_{horizon}"
        returns = [i[key] for i in impacts if key in i]
        if len(returns) >= 3:
            avg = sum(returns) / len(returns)
            sd = _stdev(returns)
            n = len(returns)
            if sd > 0:
                t_stat = avg / (sd / (n ** 0.5))
                p_value = _t_test_p_value(t_stat, n - 1)
                stats[f"t_stat_{key}"] = round(t_stat, 3)
                stats[f"p_value_{key}"] = round(p_value, 4)
                stats[f"significant_{key}"] = p_value < 0.05
                if p_value < 0.05:
                    significant_horizons.append(horizon)

    # Multiple testing correction summary
    # With 5 horizons tested, a single p<0.05 hit has ~23% chance of being spurious.
    # Require 2+ horizons significant at 0.05, OR 1 horizon significant at 0.01.
    stats["significant_horizons"] = significant_horizons
    stats["num_significant_horizons"] = len(significant_horizons)

    if len(significant_horizons) >= 2:
        stats["passes_multiple_testing"] = True
        stats["multiple_testing_note"] = (
            f"{len(significant_horizons)} horizons significant at p<0.05 — passes multi-horizon check."
        )
    elif len(significant_horizons) == 1:
        # Check if the single significant horizon passes the stricter Bonferroni threshold
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

    return stats


def _stdev(values):
    """Standard deviation (sample)."""
    if len(values) < 2:
        return 0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return variance ** 0.5


def _t_test_p_value(t_stat, df):
    """
    Approximate two-tailed p-value for a t-statistic.
    Uses the normal approximation for df >= 30, otherwise a rough
    approximation via the regularized incomplete beta function.
    """
    import math
    t = abs(t_stat)
    if df >= 30:
        # Normal approximation
        p = 2 * (1 - _normal_cdf(t))
    else:
        # Approximation using the beta distribution relation:
        # p = I(df/(df+t^2), df/2, 1/2)  where I is the regularized incomplete beta
        x = df / (df + t * t)
        p = _regularized_beta(x, df / 2.0, 0.5)
    return min(p, 1.0)


def _normal_cdf(x):
    """Approximation of the standard normal CDF."""
    import math
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _regularized_beta(x, a, b, n_terms=200):
    """Regularized incomplete beta function via continued fraction (Lentz)."""
    import math
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    ln_prefix = a * math.log(x) + b * math.log(1 - x) - math.log(a) - _log_beta(a, b)
    prefix = math.exp(ln_prefix)
    # Continued fraction
    cf = 1.0
    tiny = 1e-30
    f = tiny
    c = tiny
    d = 0.0
    for m in range(1, n_terms + 1):
        # even step
        if m == 1:
            aa = 1.0
        else:
            k = m - 1
            aa = (k * (b - k) * x) / ((a + 2*k - 1) * (a + 2*k))
        d = 1.0 + aa * d
        if abs(d) < tiny: d = tiny
        c = 1.0 + aa / c
        if abs(c) < tiny: c = tiny
        d = 1.0 / d
        f *= c * d
        # odd step
        aa = -((a + m - 1 + (m - 1)) * (a + b + m - 1 + (m - 1)) * x) / ((a + 2*(m-1) + 1) * (a + 2*(m-1) + 2))
        # Simplified: use standard CF formula for beta
    # Simpler: use scipy-style approximation
    # Fall back to a cruder but reliable approximation
    return _beta_cf(x, a, b)


def _beta_cf(x, a, b):
    """Beta continued fraction — Lentz's method."""
    import math
    ln_prefix = a * math.log(x) + b * math.log(1 - x) - _log_beta(a, b)
    prefix = math.exp(ln_prefix)

    tiny = 1e-30
    f = 1.0
    c = 1.0
    d = 1.0 - (a + b) * x / (a + 1)
    if abs(d) < tiny: d = tiny
    d = 1.0 / d
    f = d

    for m in range(1, 201):
        # d_{2m}
        num = m * (b - m) * x / ((a + 2*m - 1) * (a + 2*m))
        d = 1.0 + num * d
        if abs(d) < tiny: d = tiny
        c = 1.0 + num / c
        if abs(c) < tiny: c = tiny
        d = 1.0 / d
        f *= c * d

        # d_{2m+1}
        num = -(a + m) * (a + b + m) * x / ((a + 2*m) * (a + 2*m + 1))
        d = 1.0 + num * d
        if abs(d) < tiny: d = tiny
        c = 1.0 + num / c
        if abs(c) < tiny: c = tiny
        d = 1.0 / d
        delta = c * d
        f *= delta

        if abs(delta - 1.0) < 1e-10:
            break

    return prefix * f / a


def _log_beta(a, b):
    """Log of the beta function."""
    import math
    return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
