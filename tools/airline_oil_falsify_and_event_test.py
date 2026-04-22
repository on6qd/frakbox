"""Falsify airline-oil structural break at 2025-04-02 AND test cumulative oil move event spec.

Two-part test:
(A) Chow-test falsification: run Chow break at 2025-04-02 on AAL/DAL/UAL/LUV ~ CL=F,
    compare F-statistic to alt dates {2022-01-03, 2023-01-03, 2024-01-02, 2025-01-02 (inaug), 2026-01-02}.
    Rule: target F must be >= 3x max(alt F) to count as real regime break.

(B) Event-spec test: instead of daily oil moves (prior dead end), test cumulative 5-day
    oil return as event trigger. If 5d CL=F move > +X% (or < -X%), measure airline
    abnormal returns over next N days.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from scipy import stats
from tools.yfinance_utils import safe_download, get_close_prices

AIRLINES = ["AAL", "DAL", "UAL", "LUV"]
OIL = "CL=F"
SPY = "SPY"

def load_returns(start="2018-01-01", end="2026-04-20"):
    tickers = AIRLINES + [OIL, SPY]
    df = safe_download(tickers, start=start, end=end)
    # Columns look like "Close_AAL". Extract close cols and rename.
    close_cols = [c for c in df.columns if c.startswith("Close_")]
    close = df[close_cols].copy()
    close.columns = [c.replace("Close_", "") for c in close_cols]
    close = close.dropna(how="all")
    rets = close.pct_change().dropna()
    return rets

def chow_test(y, X1, X2, break_idx):
    """Return F-statistic for parameter-shift Chow test at break_idx."""
    # combined
    X = np.column_stack([np.ones(len(X1)), X1, X2])
    beta_full, res_full, *_ = np.linalg.lstsq(X, y, rcond=None)
    rss_full = float(np.sum((y - X @ beta_full)**2))
    # split
    y1, X1s, X2s = y[:break_idx], X1[:break_idx], X2[:break_idx]
    y2, X1b, X2b = y[break_idx:], X1[break_idx:], X2[break_idx:]
    if len(y1) < 20 or len(y2) < 20:
        return None
    Xa = np.column_stack([np.ones(len(y1)), X1s, X2s])
    Xb = np.column_stack([np.ones(len(y2)), X1b, X2b])
    ba, _, *_ = np.linalg.lstsq(Xa, y1, rcond=None)
    bb, _, *_ = np.linalg.lstsq(Xb, y2, rcond=None)
    rss_split = float(np.sum((y1 - Xa @ ba)**2) + np.sum((y2 - Xb @ bb)**2))
    k = X.shape[1]
    n = len(y)
    F = ((rss_full - rss_split) / k) / (rss_split / (n - 2*k))
    return F, ba, bb

def run_falsification():
    print("=" * 60)
    print("PART A: Falsification — Chow test at 2025-04-02 vs alt dates")
    print("=" * 60)
    rets = load_returns(start="2020-01-01", end="2026-04-20")
    alt_dates = ["2022-01-03", "2023-01-03", "2024-01-02", "2025-01-02", "2025-01-20", "2026-01-02"]
    target_date = "2025-04-02"
    all_dates = [target_date] + alt_dates

    results = {}
    for airline in AIRLINES:
        cols_needed = [airline, OIL, SPY]
        sub = rets[cols_needed].dropna()
        y = sub[airline].values
        X1 = sub[OIL].values
        X2 = sub[SPY].values
        row = {}
        for dt in all_dates:
            ts = pd.Timestamp(dt)
            # find break index
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
                row[dt] = {"F": round(F, 2), "pre_oil_beta": round(ba[1], 4), "post_oil_beta": round(bb[1], 4)}
        results[airline] = row
        print(f"\n{airline}:")
        target_F = row[target_date]["F"] if row[target_date] else None
        max_alt_F = max([row[d]["F"] for d in alt_dates if row[d]]) if any(row[d] for d in alt_dates) else None
        for dt in all_dates:
            marker = "TARGET" if dt == target_date else "alt   "
            r = row[dt]
            if r is None:
                print(f"  {marker} {dt}: insufficient data")
            else:
                print(f"  {marker} {dt}: F={r['F']:>7.2f}  pre_beta={r['pre_oil_beta']:+.4f}  post_beta={r['post_oil_beta']:+.4f}")
        if target_F and max_alt_F:
            ratio = target_F / max_alt_F
            verdict = "PASS (real break)" if ratio >= 3.0 else "FAIL (secular drift / scan artifact)"
            print(f"  -> target_F/max_alt_F = {ratio:.2f}x  {verdict}")
    return results

def run_event_test():
    print("\n" + "=" * 60)
    print("PART B: Event spec — cumulative 5-day oil move -> airline abnormal returns")
    print("=" * 60)
    rets = load_returns(start="2023-01-01", end="2026-04-20")
    # compute 5d rolling oil return
    oil_ret = rets[OIL]
    cum5 = (1 + oil_ret).rolling(5).apply(np.prod) - 1
    # Trigger dates: |5d oil return| > threshold, with cluster buffer so events are ≥30 trading days apart
    thresholds = [0.05, 0.08, 0.10]  # 5%, 8%, 10%
    horizons = [1, 3, 5, 10]
    # For abnormal returns: use SPY-subtracted returns for each airline
    for thr in thresholds:
        up_triggers = cum5.index[cum5 > thr]
        dn_triggers = cum5.index[cum5 < -thr]
        # cluster buffer 30d
        def buffer(idx):
            out = []
            last = None
            for t in idx:
                if last is None or (t - last).days >= 30:
                    out.append(t)
                    last = t
            return out
        up_triggers = buffer(up_triggers)
        dn_triggers = buffer(dn_triggers)
        print(f"\n--- 5d-oil-move threshold = ±{thr*100:.0f}%: N_up={len(up_triggers)}, N_dn={len(dn_triggers)} ---")
        for direction, triggers in [("UP (long oil)", up_triggers), ("DOWN (short oil)", dn_triggers)]:
            if not triggers:
                continue
            for h in horizons:
                row_results = []
                for airline in AIRLINES:
                    abnormal = []
                    for t in triggers:
                        # find t+1 onward for h days
                        idx_arr = np.where(rets.index > t)[0]
                        if len(idx_arr) < h:
                            continue
                        fwd = rets.iloc[idx_arr[0]:idx_arr[0]+h]
                        if len(fwd) < h:
                            continue
                        airline_ret = (1 + fwd[airline]).prod() - 1
                        spy_ret = (1 + fwd[SPY]).prod() - 1
                        abnormal.append(airline_ret - spy_ret)
                    if len(abnormal) < 3:
                        continue
                    arr = np.array(abnormal)
                    mean_ab = arr.mean()
                    t_stat, p = stats.ttest_1samp(arr, 0)
                    dir_rate = (arr < 0).mean() if "UP" in direction else (arr > 0).mean()
                    row_results.append((airline, len(arr), mean_ab, p, dir_rate))
                if row_results:
                    print(f"  {direction}, h={h}d:")
                    for airline, n, mean_ab, p, dir_rate in row_results:
                        exp_sign = "negative" if "UP" in direction else "positive"
                        mark = "***" if p < 0.05 and abs(mean_ab) > 0.005 else ""
                        print(f"    {airline}: n={n:>3}  mean_abnormal={mean_ab:+.3%}  p={p:.3f}  hit_rate({exp_sign})={dir_rate:.0%} {mark}")

if __name__ == "__main__":
    run_falsification()
    run_event_test()
