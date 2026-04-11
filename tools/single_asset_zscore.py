#!/usr/bin/env python3
"""Confound check: test z-score mean-reversion on a single asset's own rolling mean.
If single-asset mean-reversion works as well as the paired spread, the paired version adds nothing."""
import argparse
import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.yfinance_utils import get_close_prices


def run_single(sym, start, end, oos_start, lookback, entry_z, exit_z, max_hold):
    df = get_close_prices(sym, start=start, end=end)
    if df is None or len(df) < lookback + 50:
        return {"status": "error", "error": f"insufficient data"}
    s = df.iloc[:, 0] if hasattr(df, "columns") else df
    df = pd.DataFrame({"P": s}).dropna()
    # z-score of price vs rolling mean
    df["mean"] = df["P"].rolling(lookback).mean()
    df["std"] = df["P"].rolling(lookback).std()
    df["z"] = (df["P"] - df["mean"]) / df["std"]
    df["ret"] = df["P"].pct_change()
    df = df.dropna()

    trades = []
    position = 0
    entry_idx = None
    entry_date = None

    idxs = df.index.tolist()
    for i, dt in enumerate(idxs):
        z = df.at[dt, "z"]
        if pd.isna(z):
            continue
        if position == 0:
            if z > entry_z:
                position = -1
                entry_idx = i
                entry_date = dt
            elif z < -entry_z:
                position = 1
                entry_idx = i
                entry_date = dt
        else:
            held = i - entry_idx
            if abs(z) < exit_z or held >= max_hold:
                rets = df["ret"].iloc[entry_idx+1:i+1]
                trade_ret = position * rets.sum()
                trades.append({
                    "entry_date": str(entry_date.date()),
                    "exit_date": str(dt.date()),
                    "direction": "long" if position > 0 else "short",
                    "return": float(trade_ret),
                    "held_days": held,
                })
                position = 0
                entry_idx = None

    oos_dt = pd.Timestamp(oos_start)
    is_t = [t for t in trades if pd.Timestamp(t["entry_date"]) < oos_dt]
    oos_t = [t for t in trades if pd.Timestamp(t["entry_date"]) >= oos_dt]

    def summ(ts):
        if not ts:
            return {"n": 0}
        r = np.array([t["return"] for t in ts])
        return {
            "n": len(ts),
            "mean_return": float(r.mean()),
            "win_rate": float((r > 0).mean()),
            "sum_return": float(r.sum()),
            "sharpe_approx": float(r.mean() / r.std() * np.sqrt(252 / np.mean([t["held_days"] for t in ts]))) if r.std() > 0 else 0,
            "max_win": float(r.max()),
            "max_loss": float(r.min()),
        }

    return {
        "status": "ok",
        "symbol": sym,
        "total": len(trades),
        "IS": summ(is_t),
        "OOS": summ(oos_t),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start", default="2022-06-01")
    p.add_argument("--end", default="2026-01-01")
    p.add_argument("--oos-start", default="2024-01-01")
    p.add_argument("--lookback", type=int, default=90)
    p.add_argument("--entry-z", type=float, default=2.0)
    p.add_argument("--exit-z", type=float, default=0.5)
    p.add_argument("--max-hold", type=int, default=10)
    args = p.parse_args()
    r = run_single(args.symbol, args.start, args.end, args.oos_start,
                   args.lookback, args.entry_z, args.exit_z, args.max_hold)
    print(json.dumps(r, indent=2, default=str))


if __name__ == "__main__":
    main()
