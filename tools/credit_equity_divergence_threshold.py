"""
Test credit-equity divergence as a threshold regime signal.

Setup:
  - HYG: high-yield corporate bond ETF (credit market, risk-on/off proxy)
  - SPY: equity market
  - Divergence = 20d_return(HYG) - 20d_return(SPY)
      Positive -> bonds outperforming equities (credit warning not confirmed)
      Negative -> equities outperforming bonds (warning of "risk-on blowoff"
      where credit lags)

Conventional wisdom: when HYG underperforms SPY heavily (bonds stressed while
equities rally), it's a forward-looking bearish divergence for SPY.

Tests:
  - divergence < -3% (bonds lagging by 3% over 20d -> bearish SPY signal)
  - divergence < -5% (stronger)
  - divergence > +3% (bonds outperforming -> bullish SPY bounce?)
  - divergence > +5%
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from tools.timeseries import get_aligned_series


def first_close_events(series, threshold, direction, cluster_days=30):
    events = []
    last = None
    for dt, v in series.dropna().items():
        trips = (direction == "above" and v > threshold) or (direction == "below" and v < threshold)
        if not trips:
            continue
        if last is None or (dt - last).days > cluster_days:
            events.append(dt)
            last = dt
        else:
            last = dt
    return events


def measure_forward(spy, events, horizons):
    ret = spy.pct_change().fillna(0) * 100
    out = {}
    for h in horizons:
        rr = []
        for dt in events:
            if dt not in spy.index:
                continue
            loc = spy.index.get_loc(dt)
            e = min(loc + h, len(spy) - 1)
            if e > loc:
                rr.append(ret.iloc[loc + 1 : e + 1].sum())
        if len(rr) >= 3:
            a = np.array(rr)
            t, p = scipy_stats.ttest_1samp(a, 0)
            out[f"{h}d"] = {"n": len(a), "mean": float(a.mean()), "median": float(np.median(a)),
                            "std": float(a.std()), "t_stat": float(t), "p_value": float(p),
                            "positive_rate": float(np.mean(a > 0))}
    return out


def best_h(res, p_t=0.05, m_t=1.0):
    b = None
    for h, r in res.items():
        if r["p_value"] < p_t and abs(r["mean"]) >= m_t:
            if b is None or r["p_value"] < b[1]["p_value"]:
                b = (h, r)
    return b


def fmt(res):
    out = []
    for h, r in res.items():
        m = "*" if r["p_value"] < 0.05 else " "
        out.append(f"{h}: n={r['n']} mean={r['mean']:+.2f}% p={r['p_value']:.4f}{m} pos={r['positive_rate']:.2f}")
    return "\n    ".join(out)


def run(trigger, spy, thr, direction, label):
    horizons = [1, 3, 5, 10, 20]
    print(f"\n=== {label}: threshold={thr:+.2f} direction={direction} ===")
    evs = first_close_events(trigger, thr, direction, cluster_days=30)
    recent = [d for d in evs if d >= pd.Timestamp("2020-01-01")]
    print(f"Pooled n={len(evs)}, recent n={len(recent)}")
    if len(evs) < 3:
        return {"label": label, "overall": "INSUFFICIENT_N"}
    pooled_res = measure_forward(spy, evs, horizons)
    print("  Pooled:\n    " + fmt(pooled_res))
    pb = best_h(pooled_res)
    if pb:
        print(f"    POOLED: pass at {pb[0]} mean={pb[1]['mean']:+.2f}% p={pb[1]['p_value']:.4f}")
    else:
        print("    POOLED: FAIL canonical gate")
    recent_res = measure_forward(spy, recent, horizons) if len(recent) >= 3 else {}
    overall = "FAIL_NO_RECENT"
    rb = None
    if recent_res:
        print("  Recent:\n    " + fmt(recent_res))
        rb = best_h(recent_res)
        if rb:
            print(f"    RECENT: pass at {rb[0]} mean={rb[1]['mean']:+.2f}% p={rb[1]['p_value']:.4f}")
        else:
            print("    RECENT: FAIL canonical gate")
        if pb and rb:
            sign_ok = (pb[1]["mean"] >= 0) == (rb[1]["mean"] >= 0)
            overall = "CANONICAL_PASS" if sign_ok else "FAIL_SIGN_FLIP"
        else:
            overall = "FAIL_CANONICAL_GATE"
    print(f"  OVERALL: {overall}")
    return {"label": label, "threshold": thr, "direction": direction, "overall": overall,
            "pooled_best": pb, "recent_best": rb}


def main():
    # HYG only started 2007; use 2010+ for consistency
    df = get_aligned_series(["HYG", "SPY"], "2010-01-01", "2026-04-20")
    hyg_20d = df["HYG"].pct_change(20) * 100
    spy_20d = df["SPY"].pct_change(20) * 100
    divergence = hyg_20d - spy_20d  # percentage points

    print(f"Aligned n={len(df)}, span {df.index.min().date()} -> {df.index.max().date()}")
    dmeta = divergence.dropna().describe(percentiles=[0.05, 0.10, 0.25, 0.75, 0.90, 0.95]).round(3)
    print("Divergence (HYG20d - SPY20d) distribution (pp):")
    print(dmeta)
    print(f"Current divergence: {divergence.iloc[-1]:+.2f} pp")

    results = []
    # Bearish signal: bonds lagging equities
    results.append(run(divergence, df["SPY"], -3.0, "below", "DIV_BELOW_-3pp_bondslag_bearishspy"))
    results.append(run(divergence, df["SPY"], -5.0, "below", "DIV_BELOW_-5pp_bondslag_strong"))
    results.append(run(divergence, df["SPY"], -7.0, "below", "DIV_BELOW_-7pp_extreme_bondslag"))
    # Bullish signal: bonds outperforming (usually crisis recovery)
    results.append(run(divergence, df["SPY"], +3.0, "above", "DIV_ABOVE_+3pp_bondslead_bullishspy"))
    results.append(run(divergence, df["SPY"], +5.0, "above", "DIV_ABOVE_+5pp_bondslead_strong"))
    results.append(run(divergence, df["SPY"], +7.0, "above", "DIV_ABOVE_+7pp_extreme_bondslead"))

    print("\n\n=== SUMMARY ===")
    for r in results:
        print(f"  {r['label']}: {r['overall']}")


if __name__ == "__main__":
    main()
