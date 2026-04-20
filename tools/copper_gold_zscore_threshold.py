"""
Test Copper/Gold ratio z-score and delta-based triggers on SPY.

The raw ratio has secular drift so absolute thresholds are regime-dependent.
Two detrended formulations to try:
  (A) 252-day rolling z-score of HG/GC ratio — stationary around 0.
      Threshold: z > +1.5 (risk-on extreme), z < -1.5 (risk-off extreme).
  (B) 60-day percent change in HG/GC ratio.
      Threshold: pct_change > +15% (strong risk-on shift), < -15% (strong risk-off shift).

Both cluster-buffered (30d). Canonical retest: pooled 2010+ AND recent 2020+,
p<0.05, |mean|>=1%, sign consistency.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from tools.timeseries import get_aligned_series


def first_close_events_on_series(series, threshold, direction, cluster_days=30):
    events = []
    last_event = None
    for dt, v in series.dropna().items():
        trips = (direction == "above" and v > threshold) or (
            direction == "below" and v < threshold
        )
        if not trips:
            continue
        if last_event is None or (dt - last_event).days > cluster_days:
            events.append(dt)
            last_event = dt
        else:
            last_event = dt
    return events


def measure_forward_returns(spy, events, horizons):
    spy_ret = spy.pct_change().fillna(0) * 100
    results = {}
    for h in horizons:
        rr = []
        for dt in events:
            if dt not in spy.index:
                continue
            loc = spy.index.get_loc(dt)
            end_loc = min(loc + h, len(spy) - 1)
            if end_loc > loc:
                rr.append(spy_ret.iloc[loc + 1 : end_loc + 1].sum())
        if len(rr) >= 3:
            arr = np.array(rr)
            t, p = scipy_stats.ttest_1samp(arr, 0)
            results[f"{h}d"] = {
                "n": len(arr), "mean": float(arr.mean()),
                "median": float(np.median(arr)), "std": float(arr.std()),
                "t_stat": float(t), "p_value": float(p),
                "positive_rate": float(np.mean(arr > 0)),
            }
    return results


def best_horizon(res, p_t=0.05, m_t=1.0):
    best = None
    for h, r in res.items():
        if r["p_value"] < p_t and abs(r["mean"]) >= m_t:
            if best is None or r["p_value"] < best[1]["p_value"]:
                best = (h, r)
    return best


def fmt_res(res):
    out = []
    for h, r in res.items():
        mark = "*" if r["p_value"] < 0.05 else " "
        out.append(f"{h}: n={r['n']} mean={r['mean']:+.2f}% p={r['p_value']:.4f}{mark} pos={r['positive_rate']:.2f}")
    return "\n    ".join(out)


def run_test(trigger, spy, threshold, direction, label):
    horizons = [1, 3, 5, 10, 20]
    print(f"\n=== {label}: threshold={threshold:+.3f} direction={direction} ===")
    events = first_close_events_on_series(trigger, threshold, direction, cluster_days=30)
    recent = [d for d in events if d >= pd.Timestamp("2020-01-01")]
    print(f"Pooled events n={len(events)}, recent events n={len(recent)}")
    if len(events) < 3:
        print("SKIP: insufficient pooled events")
        return {"label": label, "overall": "INSUFFICIENT_N"}
    pooled_res = measure_forward_returns(spy, events, horizons)
    print("  Pooled:\n    " + fmt_res(pooled_res))
    p_best = best_horizon(pooled_res)
    if p_best:
        print(f"    POOLED passes at {p_best[0]} (mean={p_best[1]['mean']:+.2f}%, p={p_best[1]['p_value']:.4f})")
    else:
        print("    POOLED fails canonical gate")
    recent_res = measure_forward_returns(spy, recent, horizons) if len(recent) >= 3 else {}
    if recent_res:
        print("  Recent:\n    " + fmt_res(recent_res))
        r_best = best_horizon(recent_res)
        if r_best:
            print(f"    RECENT passes at {r_best[0]} (mean={r_best[1]['mean']:+.2f}%, p={r_best[1]['p_value']:.4f})")
        else:
            print("    RECENT fails canonical gate")
        if p_best and r_best:
            sign_ok = (p_best[1]["mean"] >= 0) == (r_best[1]["mean"] >= 0)
            overall = "CANONICAL_PASS" if sign_ok else "FAIL_SIGN_FLIP"
        else:
            overall = "FAIL_CANONICAL_GATE"
    else:
        overall = "FAIL_NO_RECENT"
    print(f"  OVERALL: {overall}")
    return {"label": label, "threshold": threshold, "direction": direction, "overall": overall,
            "pooled": p_best, "recent": (r_best if recent_res else None)}


def main():
    df = get_aligned_series(["HG=F", "GC=F", "SPY"], "2010-01-01", "2026-04-20")
    df["ratio"] = df["HG=F"] / df["GC=F"]

    # Z-score trigger (252d rolling)
    df["ratio_z252"] = (df["ratio"] - df["ratio"].rolling(252).mean()) / df["ratio"].rolling(252).std()
    # 60d pct change trigger
    df["ratio_chg60"] = df["ratio"].pct_change(60) * 100  # in percent

    zmeta = df["ratio_z252"].describe(percentiles=[0.05, 0.10, 0.90, 0.95]).round(3)
    chgmeta = df["ratio_chg60"].describe(percentiles=[0.05, 0.10, 0.90, 0.95]).round(2)
    print("ratio_z252 distribution:")
    print(zmeta)
    print("\nratio_chg60 distribution (%):")
    print(chgmeta)
    print(f"\nCurrent z={df['ratio_z252'].iloc[-1]:+.3f}, chg60={df['ratio_chg60'].iloc[-1]:+.2f}%")

    spy = df["SPY"]
    results = []

    # A: Z-score thresholds
    results.append(run_test(df["ratio_z252"], spy, +1.5, "above", "Z252_ABOVE_+1.5_riskon"))
    results.append(run_test(df["ratio_z252"], spy, +2.0, "above", "Z252_ABOVE_+2.0_riskon"))
    results.append(run_test(df["ratio_z252"], spy, -1.5, "below", "Z252_BELOW_-1.5_riskoff"))
    results.append(run_test(df["ratio_z252"], spy, -2.0, "below", "Z252_BELOW_-2.0_riskoff"))

    # B: 60d pct change thresholds
    results.append(run_test(df["ratio_chg60"], spy, +15.0, "above", "CHG60_ABOVE_+15pct_riskon_shift"))
    results.append(run_test(df["ratio_chg60"], spy, -15.0, "below", "CHG60_BELOW_-15pct_riskoff_shift"))
    results.append(run_test(df["ratio_chg60"], spy, +25.0, "above", "CHG60_ABOVE_+25pct_strong_riskon"))
    results.append(run_test(df["ratio_chg60"], spy, -25.0, "below", "CHG60_BELOW_-25pct_strong_riskoff"))

    print("\n\n=== SUMMARY ===")
    for r in results:
        print(f"  {r['label']}: {r['overall']}")


if __name__ == "__main__":
    main()
