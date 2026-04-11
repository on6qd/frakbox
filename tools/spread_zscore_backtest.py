#!/usr/bin/env python3
"""
Z-score mean-reversion backtest on hedge-adjusted spreads.

Tests: does the residual of A - β*B (from rolling OLS) exhibit tradeable
mean-reversion when z-score crosses ±threshold?

Critical question: even if A and B are not formally cointegrated,
does the spread produce a tradeable signal on extreme deviations?

Usage:
    python3 tools/spread_zscore_backtest.py --a XLU --b TLT \
        --start 2022-01-01 --end 2026-01-01 --oos-start 2024-01-01 \
        --lookback 60 --entry-z 2.0 --exit-z 0.5 --max-hold 10
"""
import argparse
import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools.yfinance_utils import get_close_prices


def run_spread_backtest(a, b, start, end, oos_start, lookback, entry_z, exit_z, max_hold):
    # Fetch both series — get_close_prices returns a DataFrame with single column
    a_df = get_close_prices(a, start=start, end=end)
    b_df = get_close_prices(b, start=start, end=end)
    if a_df is None or b_df is None or len(a_df) < 100 or len(b_df) < 100:
        return {"status": "error", "error": f"insufficient data: {a}={len(a_df) if a_df is not None else 0}, {b}={len(b_df) if b_df is not None else 0}"}
    a_s = a_df.iloc[:, 0] if hasattr(a_df, "iloc") and hasattr(a_df, "columns") else a_df
    b_s = b_df.iloc[:, 0] if hasattr(b_df, "iloc") and hasattr(b_df, "columns") else b_df
    df = pd.DataFrame({"A": a_s, "B": b_s}).dropna()
    if len(df) < lookback + 50:
        return {"status": "error", "error": f"insufficient overlap: {len(df)}"}

    # Rolling hedge ratio via rolling OLS: A = α + β*B
    # Use rolling mean/cov for speed
    roll_cov = df["A"].rolling(lookback).cov(df["B"])
    roll_var = df["B"].rolling(lookback).var()
    beta = roll_cov / roll_var
    alpha = df["A"].rolling(lookback).mean() - beta * df["B"].rolling(lookback).mean()
    spread = df["A"] - (alpha + beta * df["B"])
    # Rolling mean/std of spread for z-score
    sp_mean = spread.rolling(lookback).mean()
    sp_std = spread.rolling(lookback).std()
    zscore = (spread - sp_mean) / sp_std
    df["beta"] = beta
    df["spread"] = spread
    df["z"] = zscore
    df = df.dropna()

    # Returns
    df["A_ret"] = df["A"].pct_change()
    df["B_ret"] = df["B"].pct_change()
    # Hedged spread daily return: long A, short β*B (dollar-neutral approximation)
    df["spread_ret"] = df["A_ret"] - df["beta"].shift(1) * df["B_ret"]

    # Trading logic: when z > +entry_z, short spread (expect revert down).
    #                when z < -entry_z, long spread.
    #                exit when |z| < exit_z OR max_hold days reached.
    trades = []
    position = 0  # 0, +1 (long spread), -1 (short spread)
    entry_idx = None
    entry_date = None
    entry_z = float(entry_z)
    exit_z = float(exit_z)

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
            exit_signal = abs(z) < exit_z or held >= max_hold
            if exit_signal:
                # compute trade return
                spread_rets = df["spread_ret"].iloc[entry_idx+1:i+1]
                trade_ret = position * spread_rets.sum()
                trades.append({
                    "entry_date": str(entry_date.date()),
                    "exit_date": str(dt.date()),
                    "direction": "long" if position > 0 else "short",
                    "held_days": held,
                    "entry_z": float(df.at[entry_date, "z"]),
                    "exit_z": float(z),
                    "return": float(trade_ret),
                })
                position = 0
                entry_idx = None
                entry_date = None

    if not trades:
        return {"status": "ok", "n_trades": 0, "message": "no entries triggered"}

    # Split IS / OOS
    oos_dt = pd.Timestamp(oos_start)
    is_trades = [t for t in trades if pd.Timestamp(t["entry_date"]) < oos_dt]
    oos_trades = [t for t in trades if pd.Timestamp(t["entry_date"]) >= oos_dt]

    def summarize(tlist):
        if not tlist:
            return {"n": 0}
        rets = np.array([t["return"] for t in tlist])
        wins = rets[rets > 0]
        return {
            "n": len(tlist),
            "mean_return": float(rets.mean()),
            "median_return": float(np.median(rets)),
            "win_rate": float(len(wins) / len(tlist)),
            "sum_return": float(rets.sum()),
            "std_return": float(rets.std()),
            "sharpe_approx": float(rets.mean() / rets.std() * np.sqrt(252 / np.mean([t["held_days"] for t in tlist]))) if rets.std() > 0 else 0,
            "max_win": float(rets.max()),
            "max_loss": float(rets.min()),
            "avg_hold_days": float(np.mean([t["held_days"] for t in tlist])),
        }

    return {
        "status": "ok",
        "pair": f"{a}/{b}",
        "params": {
            "lookback": lookback, "entry_z": entry_z, "exit_z": exit_z,
            "max_hold": max_hold, "start": start, "end": end, "oos_start": oos_start,
        },
        "total_trades": len(trades),
        "in_sample": summarize(is_trades),
        "out_of_sample": summarize(oos_trades),
        "first_5_trades": trades[:5],
        "last_5_trades": trades[-5:],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--a", required=True)
    p.add_argument("--b", required=True)
    p.add_argument("--start", default="2022-01-01")
    p.add_argument("--end", default="2026-01-01")
    p.add_argument("--oos-start", default="2024-01-01")
    p.add_argument("--lookback", type=int, default=60)
    p.add_argument("--entry-z", type=float, default=2.0)
    p.add_argument("--exit-z", type=float, default=0.5)
    p.add_argument("--max-hold", type=int, default=10)
    args = p.parse_args()

    result = run_spread_backtest(
        args.a, args.b, args.start, args.end, args.oos_start,
        args.lookback, args.entry_z, args.exit_z, args.max_hold,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
