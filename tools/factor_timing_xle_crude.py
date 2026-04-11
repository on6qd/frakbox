"""Factor-timing pair-trade backtest: XLE vs SPY, overlaid with CL=F 20d momentum.

Follow-through on exposure_to_event_conversion_meta_finding (2026-04-11):
  "The alpha lives in CONTINUOUS factor exposure (pair trading, factor timing)
   not discrete events. Next step: design pair-trading methodology for validated
   exposure pairs."

Strategy (pre-registered 2026-04-12 in hypothesis e3456392):
  signal[t]   = sign( log(CL=F[t]) - log(CL=F[t-20]) )   # 20-day crude momentum
  position[t] = signal[t-1]                              # lagged 1d, no lookahead
  pnl[t]      = position[t] * (XLE_ret[t] - SPY_ret[t])  # dollar-neutral pair
             - tc_cost[t] if position flipped

Transaction cost: 10 bps per leg per flip (20 bps round trip).
IS:  2022-07-01 to 2024-07-01
OOS: 2024-07-01 to 2026-04-11
"""
import sys
import numpy as np
import pandas as pd
from tools.yfinance_utils import safe_download


def close_series(sym, start, end):
    df = safe_download(sym, start, end)
    if df is None or df.empty:
        raise RuntimeError(f"no data for {sym}")
    # single-symbol download -> Close column
    if "Close" in df.columns:
        s = df["Close"]
    else:
        # multi-index case
        s = df[("Close", sym)] if ("Close", sym) in df.columns else df.iloc[:, 0]
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    s.name = sym
    return s.dropna()


def annualized_sharpe(daily_returns):
    if len(daily_returns) < 2 or daily_returns.std() == 0:
        return float("nan")
    return float(np.sqrt(252) * daily_returns.mean() / daily_returns.std())


def max_drawdown(cum_returns):
    peak = cum_returns.cummax()
    dd = (cum_returns - peak) / peak
    return float(dd.min())


def run_backtest(start="2022-01-01", end="2026-04-12",
                 is_start="2022-07-01", is_end="2024-07-01",
                 oos_start="2024-07-01", oos_end="2026-04-11",
                 lookback=20, tc_bp_per_leg=10):
    xle = close_series("XLE", start, end)
    spy = close_series("SPY", start, end)
    oil = close_series("CL=F", start, end)

    # Align on business days
    df = pd.concat([xle, spy, oil], axis=1).dropna()
    df.columns = ["xle", "spy", "oil"]

    # Returns
    df["xle_ret"] = df["xle"].pct_change()
    df["spy_ret"] = df["spy"].pct_change()
    df["pair_ret"] = df["xle_ret"] - df["spy_ret"]

    # CL=F 20d momentum (log return)
    df["oil_log"] = np.log(df["oil"])
    df["oil_mom20"] = df["oil_log"] - df["oil_log"].shift(lookback)
    df["raw_signal"] = np.sign(df["oil_mom20"])
    # Lag 1 day to ensure no lookahead (signal observed at t-1 close, entered at t close)
    df["position"] = df["raw_signal"].shift(1)

    # Transaction cost only on flips
    df["flip"] = (df["position"] != df["position"].shift(1)).astype(int)
    # On flip: 2 legs closed + 2 legs opened = 4 legs × tc_bp each
    # But dollar-neutral flip is essentially "reverse both legs" = 4 legs round trip
    # Practically: flip cost = 4 * tc_bp_per_leg
    df["tc"] = df["flip"] * (4 * tc_bp_per_leg / 10000.0)

    df["strategy_ret"] = df["position"] * df["pair_ret"] - df["tc"]

    # Drop the warmup window before first valid signal
    df = df.dropna(subset=["strategy_ret"])

    # Slice IS/OOS
    is_df = df.loc[is_start:is_end].copy()
    oos_df = df.loc[oos_start:oos_end].copy()

    def stats(sub_df, label):
        if len(sub_df) < 5:
            return {"label": label, "n": len(sub_df), "error": "insufficient_data"}
        strat = sub_df["strategy_ret"]
        spy = sub_df["spy_ret"]
        strat_cum = (1 + strat).cumprod()
        spy_cum = (1 + spy).cumprod()
        years = len(sub_df) / 252
        out = {
            "label": label,
            "n": int(len(sub_df)),
            "years": round(years, 2),
            "strategy_total_return_pct": round(100 * (strat_cum.iloc[-1] - 1), 2),
            "strategy_annualized_pct": round(100 * (strat_cum.iloc[-1] ** (1 / max(years, 0.01)) - 1), 2),
            "strategy_sharpe": round(annualized_sharpe(strat), 3),
            "strategy_max_dd_pct": round(100 * max_drawdown(strat_cum), 2),
            "spy_total_return_pct": round(100 * (spy_cum.iloc[-1] - 1), 2),
            "spy_annualized_pct": round(100 * (spy_cum.iloc[-1] ** (1 / max(years, 0.01)) - 1), 2),
            "spy_sharpe": round(annualized_sharpe(spy), 3),
            "spy_max_dd_pct": round(100 * max_drawdown(spy_cum), 2),
            "num_flips": int(sub_df["flip"].sum()),
            "tc_drag_pct": round(100 * sub_df["tc"].sum(), 2),
            "pct_long_xle_days": round(100 * (sub_df["position"] > 0).mean(), 1),
        }
        # Direction of strategy mean (for sign consistency check)
        out["strategy_mean_daily_bp"] = round(10000 * strat.mean(), 2)
        return out

    import json
    result = {
        "is": stats(is_df, "IS 2022-07 .. 2024-07"),
        "oos": stats(oos_df, "OOS 2024-07 .. 2026-04"),
        "lookback": lookback,
        "tc_bp_per_leg": tc_bp_per_leg,
    }
    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    run_backtest()
