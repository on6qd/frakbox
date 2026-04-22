"""Falsify remaining Liberation Day 2025-04-02 structural break scan hits.

Scan hits:
  0d3002ec (p=7): Energy-oil amplification: XLE/XOM/CVX oil beta 6x higher post 2025-04-02
  fa7d29b3 (p=7): Copper sector decrease: XLB/XLI/XME/EEM copper beta lower post 2025-04-02

Rule per macro_factor_structural_break_scan_artifact_rule_v2_2026_04_21:
Target F at 2025-04-02 must be >= 3x max(alt F) where alt dates span 2021-2025.
If fails, record as DEAD_END_SECULAR_DRIFT.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from tools.yfinance_utils import safe_download

def load_returns(tickers, start="2020-01-01", end="2026-04-20"):
    df = safe_download(tickers, start=start, end=end)
    close_cols = [c for c in df.columns if c.startswith("Close_")]
    close = df[close_cols].copy()
    close.columns = [c.replace("Close_", "") for c in close_cols]
    close = close.dropna(how="all")
    return close.pct_change().dropna()

def chow_test(y, X1, X2, break_idx):
    X = np.column_stack([np.ones(len(X1)), X1, X2])
    beta_full, *_ = np.linalg.lstsq(X, y, rcond=None)
    rss_full = float(np.sum((y - X @ beta_full)**2))
    y1, X1s, X2s = y[:break_idx], X1[:break_idx], X2[:break_idx]
    y2, X1b, X2b = y[break_idx:], X1[break_idx:], X2[break_idx:]
    if len(y1) < 20 or len(y2) < 20:
        return None
    Xa = np.column_stack([np.ones(len(y1)), X1s, X2s])
    Xb = np.column_stack([np.ones(len(y2)), X1b, X2b])
    ba, *_ = np.linalg.lstsq(Xa, y1, rcond=None)
    bb, *_ = np.linalg.lstsq(Xb, y2, rcond=None)
    rss_split = float(np.sum((y1 - Xa @ ba)**2) + np.sum((y2 - Xb @ bb)**2))
    k = X.shape[1]
    n = len(y)
    F = ((rss_full - rss_split) / k) / (rss_split / (n - 2*k))
    return F, ba, bb

def test_break(target, factor, control, rets, target_date, alt_dates):
    sub = rets[[target, factor, control]].dropna()
    y = sub[target].values
    X1 = sub[factor].values
    X2 = sub[control].values
    row = {}
    for dt in [target_date] + alt_dates:
        ts = pd.Timestamp(dt)
        idx_arr = np.where(sub.index >= ts)[0]
        if len(idx_arr) == 0:
            row[dt] = None
            continue
        bidx = int(idx_arr[0])
        out = chow_test(y, X1, X2, bidx)
        if out is None:
            row[dt] = None
        else:
            F, ba, bb = out
            row[dt] = {"F": round(F, 2), "pre_beta": round(ba[1], 4), "post_beta": round(bb[1], 4)}
    return row

def run(scan_hit_name, targets, factor, control, target_date, alt_dates):
    print(f"\n{'='*60}")
    print(f"SCAN HIT: {scan_hit_name}")
    print(f"Factor: {factor}, Control: {control}, Target date: {target_date}")
    print(f"{'='*60}")
    rets = load_returns(targets + [factor, control])
    verdicts = {}
    for tgt in targets:
        if tgt not in rets.columns:
            print(f"\n{tgt}: MISSING DATA")
            continue
        row = test_break(tgt, factor, control, rets, target_date, alt_dates)
        print(f"\n{tgt}:")
        for dt in [target_date] + alt_dates:
            r = row[dt]
            marker = "TARGET" if dt == target_date else "alt   "
            if r is None:
                print(f"  {marker} {dt}: insufficient data")
            else:
                print(f"  {marker} {dt}: F={r['F']:>7.2f}  pre_beta={r['pre_beta']:+.4f}  post_beta={r['post_beta']:+.4f}")
        target_F = row[target_date]["F"] if row[target_date] else None
        alt_Fs = [row[d]["F"] for d in alt_dates if row[d]]
        if target_F and alt_Fs:
            max_alt_F = max(alt_Fs)
            ratio = target_F / max_alt_F if max_alt_F > 0 else float('inf')
            verdict = "PASS" if ratio >= 3.0 else "FAIL"
            print(f"  -> target_F/max_alt_F = {ratio:.2f}x  [{verdict}]")
            verdicts[tgt] = (ratio, verdict)
    return verdicts

if __name__ == "__main__":
    alt_dates = ["2022-01-03", "2023-01-03", "2024-01-02", "2025-01-02", "2026-01-02"]

    # HIT 1: Energy-oil amplification
    v1 = run("ENERGY-OIL AMPLIFICATION (0d3002ec)",
             targets=["XLE", "XOM", "CVX"], factor="CL=F", control="SPY",
             target_date="2025-04-02", alt_dates=alt_dates)

    # HIT 2: Copper sector decrease
    v2 = run("COPPER-SECTOR DECREASE (fa7d29b3)",
             targets=["XLB", "XLI", "XME", "EEM"], factor="HG=F", control="SPY",
             target_date="2025-04-02", alt_dates=alt_dates)

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("\nENERGY-OIL:")
    for tgt, (ratio, verdict) in v1.items():
        print(f"  {tgt}: {ratio:.2f}x [{verdict}]")
    print("\nCOPPER-SECTOR:")
    for tgt, (ratio, verdict) in v2.items():
        print(f"  {tgt}: {ratio:.2f}x [{verdict}]")
