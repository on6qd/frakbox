"""
Test Copper/Gold ratio (HG=F / GC=F) as a threshold regime signal for SPY.

Approach:
  1. Fetch HG=F, GC=F, SPY from 2010 to present.
  2. Compute daily ratio = HG/GC.
  3. For each direction (above/below) and threshold candidate, identify
     first-close cluster-buffered events (30d buffer).
  4. Measure SPY-benchmarked... wait, SPY IS the target so use raw SPY
     returns vs no signal. Absolute threshold test is fine — compare return
     AFTER threshold cross to unconditional return.
  5. Require canonical rules: pooled (2010+) AND recent (2020+) both
     p<0.05 AND |mean|>=1% AND same sign.

Notes:
  * Ratio drifts secularly; canonical recency check will catch regime dependence.
  * We normalize by percentiles of the FULL sample so the test is self-contained.
  * Because target IS SPY, we use raw SPY cumulative returns — not benchmarked.
    The control is "unconditional forward return" (mean of all 10d returns).
    We t-test against 0, so a significant mean absolute is what matters.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from tools.timeseries import get_aligned_series
from causal_tests import identify_first_close_events


def first_close_events_on_series(
    series: pd.Series, threshold: float, direction: str, cluster_days: int = 30
) -> list[pd.Timestamp]:
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


def measure_forward_returns(
    spy: pd.Series, event_dates: list[pd.Timestamp], horizons: list[int]
) -> dict:
    """Compute cumulative SPY returns at forward horizons from each event date (entry = next-day open proxy = next-day close)."""
    spy_ret = spy.pct_change().fillna(0) * 100  # daily %
    results = {}
    for h in horizons:
        horizon_returns = []
        for dt in event_dates:
            if dt not in spy.index:
                continue
            loc = spy.index.get_loc(dt)
            end_loc = min(loc + h, len(spy) - 1)
            if end_loc > loc:
                cum = spy_ret.iloc[loc + 1 : end_loc + 1].sum()
                horizon_returns.append(cum)
        if len(horizon_returns) >= 3:
            arr = np.array(horizon_returns)
            t, p = scipy_stats.ttest_1samp(arr, 0)
            results[f"{h}d"] = {
                "n": len(arr),
                "mean": float(arr.mean()),
                "median": float(np.median(arr)),
                "std": float(arr.std()),
                "t_stat": float(t),
                "p_value": float(p),
                "positive_rate": float(np.mean(arr > 0)),
            }
    return results


def eval_canonical(horizon_results: dict, p_thresh: float = 0.05, mean_thresh: float = 1.0):
    """Pick best horizon meeting canonical gate; return dict."""
    best = None
    for h, r in horizon_results.items():
        if r["p_value"] < p_thresh and abs(r["mean"]) >= mean_thresh:
            if best is None or r["p_value"] < best[1]["p_value"]:
                best = (h, r)
    return best


def run_test(ratio: pd.Series, spy: pd.Series, threshold: float, direction: str, label: str):
    horizons = [1, 3, 5, 10, 20]
    print(f"\n=== {label}: threshold={threshold:.5f} direction={direction} ===")
    pooled_events = first_close_events_on_series(ratio, threshold, direction, cluster_days=30)
    recent_events = [d for d in pooled_events if d >= pd.Timestamp("2020-01-01")]
    print(f"Pooled events n={len(pooled_events)}, recent events n={len(recent_events)}")
    if len(pooled_events) < 3:
        print("SKIP: insufficient pooled events")
        return None

    pooled_res = measure_forward_returns(spy, pooled_events, horizons)
    recent_res = measure_forward_returns(spy, recent_events, horizons) if len(recent_events) >= 3 else {}

    def fmt(res):
        out = []
        for h, r in res.items():
            sig = "*" if r["p_value"] < 0.05 else " "
            out.append(f"{h}: n={r['n']} mean={r['mean']:+.2f}% p={r['p_value']:.4f}{sig} pos={r['positive_rate']:.2f}")
        return "\n    ".join(out)

    print("  Pooled:")
    print("    " + fmt(pooled_res))
    pooled_best = eval_canonical(pooled_res)
    if pooled_best:
        print(f"    POOLED PASSES canonical at {pooled_best[0]} (mean={pooled_best[1]['mean']:+.2f}%, p={pooled_best[1]['p_value']:.4f})")
    else:
        print("    POOLED fails canonical gate (no horizon with p<0.05 AND |mean|>=1%)")

    if recent_res:
        print("  Recent (2020+):")
        print("    " + fmt(recent_res))
        recent_best = eval_canonical(recent_res)
        if recent_best:
            print(f"    RECENT PASSES canonical at {recent_best[0]} (mean={recent_best[1]['mean']:+.2f}%, p={recent_best[1]['p_value']:.4f})")
        else:
            print("    RECENT fails canonical gate")

        # Sign consistency
        if pooled_best and recent_best:
            sign_ok = (pooled_best[1]["mean"] >= 0) == (recent_best[1]["mean"] >= 0)
            overall = "PASS" if sign_ok else "FAIL_SIGN_FLIP"
            print(f"  CANONICAL OVERALL: {overall}")
            return {"threshold": threshold, "direction": direction, "label": label,
                    "pooled": pooled_best, "recent": recent_best, "overall": overall}

    return {"threshold": threshold, "direction": direction, "label": label,
            "pooled": pooled_best, "recent": None, "overall": "FAIL_NO_RECENT"}


def main():
    df = get_aligned_series(["HG=F", "GC=F", "SPY"], "2010-01-01", "2026-04-20")
    df["ratio"] = df["HG=F"] / df["GC=F"]

    ratio = df["ratio"]
    spy = df["SPY"]

    # Profile
    q = ratio.quantile([0.05, 0.10, 0.25, 0.75, 0.90, 0.95])
    print(f"Ratio span {ratio.index.min().date()} -> {ratio.index.max().date()} n={len(ratio)}")
    print(f"Percentiles: 5%={q.iloc[0]:.5f}, 10%={q.iloc[1]:.5f}, 25%={q.iloc[2]:.5f}, "
          f"75%={q.iloc[3]:.5f}, 90%={q.iloc[4]:.5f}, 95%={q.iloc[5]:.5f}")
    print(f"Current level: {ratio.iloc[-1]:.5f}")

    # Test extreme-low (risk-off): ratio below 10th/5th percentile
    # Hypothesis: risk-off = SPY negative forward return
    results = []
    results.append(run_test(ratio, spy, q.iloc[1], "below", "BELOW_10th_pct_riskoff"))
    results.append(run_test(ratio, spy, q.iloc[0], "below", "BELOW_5th_pct_riskoff"))
    # Test extreme-high (risk-on): ratio above 90th/95th percentile
    results.append(run_test(ratio, spy, q.iloc[4], "above", "ABOVE_90th_pct_riskon"))
    results.append(run_test(ratio, spy, q.iloc[5], "above", "ABOVE_95th_pct_riskon"))

    # Summary
    print("\n=== SUMMARY ===")
    for r in results:
        if r is None:
            continue
        overall = r["overall"]
        print(f"  {r['label']} ({r['direction']} {r['threshold']:.5f}): {overall}")


if __name__ == "__main__":
    main()
