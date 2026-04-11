"""
Test whether a factor LEVEL regime (today) predicts target FORWARD returns (over
the next N days).

Unlike data_tasks.py regression --test-type regime, this version:
- Uses forward returns (no contemporaneous contamination)
- Supports multiple horizons
- Uses terciles of the factor LEVEL (not returns) as regime
- Reports per-regime mean forward return, Sharpe, n
- Runs OOS validation

Usage:
    python3 tools/regime_forward_returns.py \
        --target SPY --factor "^VIX" \
        --start 2010-01-01 --end 2026-04-10 \
        --oos-start 2022-01-01 \
        --horizons 5,10,20
"""
import argparse
import json
import sys
import os
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.timeseries import get_series


def test_regime_forward(target: str, factor: str, start: str, end: str,
                         oos_start: str, horizons: list[int]) -> dict:
    # Fetch both series
    tgt = get_series(target, start, end)
    fac = get_series(factor, start, end)

    # Align
    df = pd.DataFrame({"target_price": tgt, "factor_level": fac}).dropna()
    if len(df) < 252:
        return {"error": f"Not enough data: {len(df)} days"}

    # Compute target log returns
    df["target_ret"] = np.log(df["target_price"] / df["target_price"].shift(1))

    # Forward N-day returns
    for h in horizons:
        df[f"fwd_{h}d"] = (
            np.log(df["target_price"].shift(-h) / df["target_price"])
        )

    # Split IS/OOS
    is_df = df[df.index < oos_start].copy()
    oos_df = df[df.index >= oos_start].copy()

    if len(is_df) < 252 or len(oos_df) < 60:
        return {"error": f"Not enough data IS={len(is_df)} OOS={len(oos_df)}"}

    # Build terciles from IS factor levels only
    is_factor = is_df["factor_level"].dropna()
    q33, q66 = is_factor.quantile([0.333, 0.667]).values

    def label(x):
        if pd.isna(x):
            return None
        if x <= q33:
            return "low"
        elif x <= q66:
            return "mid"
        else:
            return "high"

    is_df["regime"] = is_df["factor_level"].apply(label)
    oos_df["regime"] = oos_df["factor_level"].apply(label)

    results = {
        "target": target,
        "factor": factor,
        "oos_start": oos_start,
        "q33": float(q33),
        "q66": float(q66),
        "horizons": {},
        "n_is": len(is_df),
        "n_oos": len(oos_df),
    }

    for h in horizons:
        col = f"fwd_{h}d"
        is_sub = is_df[["regime", col]].dropna()
        oos_sub = oos_df[["regime", col]].dropna()

        # IS per-regime stats
        is_groups = is_sub.groupby("regime")[col]
        is_stats = {}
        for r, g in is_groups:
            if len(g) >= 10:
                is_stats[r] = {
                    "mean": float(g.mean()),
                    "std": float(g.std()),
                    "n": int(len(g)),
                    "sharpe_ann": float(g.mean() / g.std() * np.sqrt(252 / h)) if g.std() > 0 else 0,
                    "t_stat": float(g.mean() / (g.std() / np.sqrt(len(g)))) if g.std() > 0 else 0,
                }

        # Kruskal-Wallis on IS
        is_groups_list = [g.values for _, g in is_groups if len(g) >= 10]
        is_kw_h, is_kw_p = (None, None)
        if len(is_groups_list) >= 2:
            is_kw_h, is_kw_p = scipy_stats.kruskal(*is_groups_list)

        # OOS per-regime stats
        oos_groups = oos_sub.groupby("regime")[col]
        oos_stats = {}
        for r, g in oos_groups:
            if len(g) >= 5:
                oos_stats[r] = {
                    "mean": float(g.mean()),
                    "std": float(g.std()),
                    "n": int(len(g)),
                    "sharpe_ann": float(g.mean() / g.std() * np.sqrt(252 / h)) if g.std() > 0 else 0,
                }

        # Sign preservation: for each regime in is_stats, does OOS have same sign?
        sign_match = {}
        for r in is_stats:
            if r in oos_stats:
                is_sign = np.sign(is_stats[r]["mean"])
                oos_sign = np.sign(oos_stats[r]["mean"])
                sign_match[r] = bool(is_sign == oos_sign)

        # High-low spread
        is_spread = None
        if "high" in is_stats and "low" in is_stats:
            is_spread = is_stats["high"]["mean"] - is_stats["low"]["mean"]
        oos_spread = None
        if "high" in oos_stats and "low" in oos_stats:
            oos_spread = oos_stats["high"]["mean"] - oos_stats["low"]["mean"]
        spread_sign_match = (
            is_spread is not None and oos_spread is not None and
            np.sign(is_spread) == np.sign(oos_spread) and abs(oos_spread) > 0.001
        )

        results["horizons"][h] = {
            "is_stats": is_stats,
            "oos_stats": oos_stats,
            "is_kw_h": float(is_kw_h) if is_kw_h is not None else None,
            "is_kw_p": float(is_kw_p) if is_kw_p is not None else None,
            "is_high_low_spread": float(is_spread) if is_spread is not None else None,
            "oos_high_low_spread": float(oos_spread) if oos_spread is not None else None,
            "sign_match_by_regime": sign_match,
            "spread_sign_match": spread_sign_match,
        }

    return results


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target", required=True)
    p.add_argument("--factor", required=True)
    p.add_argument("--start", default="2010-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--oos-start", required=True)
    p.add_argument("--horizons", default="5,10,20")
    args = p.parse_args()

    horizons = [int(x) for x in args.horizons.split(",")]
    result = test_regime_forward(
        args.target, args.factor, args.start, args.end or "2026-04-10",
        args.oos_start, horizons
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
