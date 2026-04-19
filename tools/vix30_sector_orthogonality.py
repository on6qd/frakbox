"""Orthogonality test: per-event canonical abnormal returns for candidate vs existing VIX>30 hypotheses."""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import market_data

EVENTS = [
    "2015-08-24", "2018-02-05", "2018-12-21", "2020-02-27", "2020-09-03",
    "2020-10-26", "2021-01-27", "2021-12-01", "2022-01-25", "2022-04-26",
    "2022-09-26", "2024-08-05", "2025-04-03", "2026-03-27",
]

# (symbol, best_horizon)
UNIVERSE = {
    "XLP": 1, "XLV": 1, "XLI": 1,  # candidates
    "XLB": 20, "EEM": 10, "EFA": 20, "HYG": 1, "XME": 1, "SMH": 5,  # existing pending
}


def get_per_event(symbol: str, horizon: int) -> dict:
    evd = [{"symbol": symbol, "date": d} for d in EVENTS]
    r = market_data.measure_event_impact(
        event_dates=evd, benchmark="SPY", entry_price="open",
        check_factors=False, check_seasonal=False,
    )
    out = {}
    key = f"abnormal_{horizon}d"
    for imp in r.get("individual_impacts", []):
        if key in imp and imp[key] is not None:
            out[imp["event_date"]] = imp[key]
    return out


def main():
    series = {}
    for sym, hz in UNIVERSE.items():
        s = get_per_event(sym, hz)
        series[f"{sym}_{hz}d"] = s
        vals = list(s.values())
        print(f"{sym} {hz}d: n={len(s)}, mean={np.mean(vals):.2f}%, pos={np.mean([1 if v>0 else 0 for v in vals])*100:.1f}%")

    df = pd.DataFrame(series).sort_index()
    print("\nPer-event abnormal returns (%):")
    print(df.round(2).to_string())

    candidates = ["XLP_1d", "XLV_1d", "XLI_1d"]
    existing = ["XLB_20d", "EEM_10d", "EFA_20d", "HYG_1d", "XME_1d", "SMH_5d"]
    corr = df.corr()

    print("\n=== Correlation (candidate x existing) ===")
    print(corr.loc[candidates, existing].round(2).to_string())

    print("\n=== Correlation among candidates ===")
    print(corr.loc[candidates, candidates].round(2).to_string())

    print("\n=== Max |corr| candidate -> any existing (threshold 0.60) ===")
    for c in candidates:
        m = corr.loc[c, existing].abs().max()
        argmax = corr.loc[c, existing].abs().idxmax()
        verdict = "ORTHOGONAL" if m < 0.60 else "FAILS ORTHOGONALITY"
        print(f"  {c}: max |corr| = {m:.2f} with {argmax} (signed {corr.loc[c, argmax]:.2f}) -> {verdict}")


if __name__ == "__main__":
    main()
