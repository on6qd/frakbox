"""
Volatility-adjusted position sizing and stop calibration.

Motivation (2026-04-09, from ZBIO insider cluster trade):
  ZBIO's daily_std is 8.80% — ~9x SPY volatility. The fund's default
  $5,000 position size and 15% raw stop were calibrated on large-cap
  validation data where daily_std ~2%. On ZBIO the 15% stop is only
  ~1.7x daily std — inside the noise envelope — so normal daily price
  action triggers false exits. Meanwhile the fixed $5K notional gives
  the position ~9x the dollar variance of a comparable MSFT trade.

  Journal #255 observation: ZBIO day-1 abnormal return of -9.82% looks
  catastrophic but is only 1.12 standard deviations — statistically
  normal, not signal-disconfirming.

Rule proposal:
  1) Compute daily_std from the last 65 trading days of returns.
  2) Horizon volatility σ_H = daily_std * sqrt(horizon_days).
  3) Stop width = k * σ_H where k=2 (approximate 95% one-tail barrier
     under normality; actual Brownian-barrier touch probability is
     higher, so this is a lower bound on stop looseness).
  4) Risk budget R = max acceptable loss per experiment
     (default: 10% of fund default position = $500 on $5K nominal).
  5) Position size = min(max_size, max(min_size, R / stop_width)).
  6) Stop-loss percentage for the activator = stop_width (capped).

  This is independent from — and layered ON TOP of — signal-level
  stop rules (e.g. pre-registered abnormal-return gates like the
  ZBIO day-2 / day-3 pre-registered thresholds). Use whichever is
  tighter.

Usage:
  python3 tools/volatility_sizing.py ZBIO --horizon 5
  python3 tools/volatility_sizing.py MSFT --horizon 5 --risk 500
"""
from __future__ import annotations

import sys
from pathlib import Path

# When invoked as `python3 tools/volatility_sizing.py`, sys.path[0] is the
# tools/ directory, which breaks `from tools.yfinance_utils import ...`.
# Push the project root onto sys.path so the import works either way.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from math import sqrt
from dataclasses import dataclass
from datetime import date, timedelta


DEFAULT_LOOKBACK_DAYS = 95   # calendar days, yields ~65 trading days
DEFAULT_HORIZON_DAYS = 5
DEFAULT_K = 2.0              # stop = k * horizon_std
DEFAULT_RISK_BUDGET = 500.0  # $ per experiment (10% of $5K nominal)
DEFAULT_MAX_SIZE = 5000.0    # fund policy cap
DEFAULT_MIN_SIZE = 1000.0    # below this, don't bother
FALLBACK_DAILY_STD = 0.02    # 2% fallback if history unavailable


@dataclass
class VolSizingResult:
    symbol: str
    daily_std: float
    n_samples: int
    horizon_days: int
    k: float
    horizon_std: float
    stop_pct: float
    risk_budget: float
    raw_size: float
    recommended_size: float
    default_size: float
    ratio_to_default: float
    notes: list


def compute_daily_std(symbol: str, lookback_days: int = DEFAULT_LOOKBACK_DAYS):
    """Return (daily_std, n) or (None, 0) on failure."""
    try:
        from tools.yfinance_utils import safe_download
        import numpy as np
    except Exception:
        return None, 0
    try:
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=lookback_days)
        df = safe_download(symbol, start=start.isoformat(), end=end.isoformat())
        if df is None or len(df) < 10:
            return None, 0
        closes = df['Close'].dropna().values.flatten()
        if len(closes) < 10:
            return None, 0
        rets = np.diff(closes) / closes[:-1]
        return float(np.std(rets)), int(len(rets))
    except Exception:
        return None, 0


def recommend(
    symbol: str,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    k: float = DEFAULT_K,
    risk_budget: float = DEFAULT_RISK_BUDGET,
    max_size: float = DEFAULT_MAX_SIZE,
    min_size: float = DEFAULT_MIN_SIZE,
    daily_std_override: float | None = None,
) -> VolSizingResult:
    notes = []
    if daily_std_override is not None:
        ds, n = float(daily_std_override), 0
        notes.append(f"using override daily_std={ds*100:.2f}%")
    else:
        ds, n = compute_daily_std(symbol)
        if ds is None:
            ds, n = FALLBACK_DAILY_STD, 0
            notes.append(f"history unavailable — fell back to {ds*100:.1f}% daily_std")
    horizon_std = ds * sqrt(horizon_days)
    stop_pct = k * horizon_std
    # Never use a stop narrower than the single-day 1-sigma; that's inside noise.
    single_day_sigma = ds
    if stop_pct < 1.5 * single_day_sigma:
        stop_pct = 1.5 * single_day_sigma
        notes.append("stop widened to 1.5x daily_std floor")
    raw = risk_budget / stop_pct if stop_pct > 0 else max_size
    rec = max(min_size, min(max_size, raw))
    if rec == max_size and raw > max_size:
        notes.append(f"raw size ${raw:,.0f} capped at max ${max_size:,.0f}")
    if rec == min_size and raw < min_size:
        notes.append(f"raw size ${raw:,.0f} floored at min ${min_size:,.0f}")
    return VolSizingResult(
        symbol=symbol,
        daily_std=ds,
        n_samples=n,
        horizon_days=horizon_days,
        k=k,
        horizon_std=horizon_std,
        stop_pct=stop_pct,
        risk_budget=risk_budget,
        raw_size=raw,
        recommended_size=rec,
        default_size=max_size,
        ratio_to_default=rec / max_size,
        notes=notes,
    )


def print_result(r: VolSizingResult):
    print("=" * 60)
    print(f"VOLATILITY-ADJUSTED SIZING  {r.symbol}")
    print("=" * 60)
    print(f"daily_std:        {r.daily_std*100:.2f}%  (n={r.n_samples})")
    print(f"horizon:          {r.horizon_days}d   σ_H={r.horizon_std*100:.2f}%   k={r.k}")
    print(f"stop-loss (pct):  {r.stop_pct*100:.2f}%")
    print(f"risk budget:      ${r.risk_budget:,.0f}")
    print(f"raw size:         ${r.raw_size:,.0f}")
    print(f"recommended size: ${r.recommended_size:,.0f}   "
          f"({r.ratio_to_default*100:.0f}% of ${r.default_size:,.0f} default)")
    if r.notes:
        print("notes:")
        for nt in r.notes:
            print(f"  - {nt}")
    print()
    print("To apply this recommendation on hypothesis activation:")
    print(f"  trigger_position_size = {int(r.recommended_size)}")
    print(f"  trigger_stop_loss_pct = {r.stop_pct*100:.1f}")
    print("=" * 60)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Volatility-adjusted position sizing")
    p.add_argument("symbol")
    p.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_DAYS)
    p.add_argument("--k", type=float, default=DEFAULT_K)
    p.add_argument("--risk", type=float, default=DEFAULT_RISK_BUDGET)
    p.add_argument("--max-size", type=float, default=DEFAULT_MAX_SIZE)
    p.add_argument("--min-size", type=float, default=DEFAULT_MIN_SIZE)
    p.add_argument("--daily-std", type=float, default=None,
                   help="override computed daily_std (decimal, e.g. 0.088 for 8.8%)")
    args = p.parse_args()
    r = recommend(
        symbol=args.symbol,
        horizon_days=args.horizon,
        k=args.k,
        risk_budget=args.risk,
        max_size=args.max_size,
        min_size=args.min_size,
        daily_std_override=args.daily_std,
    )
    print_result(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
