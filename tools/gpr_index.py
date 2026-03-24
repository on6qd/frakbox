"""
Geopolitical Risk (GPR) Index loader — Caldara & Iacoviello (2022).

Daily index from 1985 to present, updated daily.
Source: https://www.matteoiacoviello.com/gpr.htm

The GPR index is constructed by counting newspaper articles related to
geopolitical tensions, threats, and acts. Baseline = 100 (1985-2019 average).

Levels:
    < 75   = low geopolitical risk
    75-125 = normal
    125-200 = elevated (e.g., major tensions, regional conflicts)
    > 200  = crisis (e.g., Gulf War, 9/11, Ukraine invasion)

Cache lives at ~/.gpr_cache/gpr_daily.pkl — TTL 1 day (updated daily).

Usage:
    from tools.gpr_index import get_gpr_level, get_gpr_regime, get_gpr_context
    level = get_gpr_level("2024-01-15")      # 109.3
    regime = get_gpr_regime("2024-01-15")     # "normal"
    ctx = get_gpr_context("2024-01-15")       # full dict with level, regime, trend
"""

import sys
import pickle
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
from io import BytesIO

CACHE_DIR = Path.home() / ".gpr_cache"
CACHE_FILE = CACHE_DIR / "gpr_daily.pkl"
TTL_DAYS = 1  # refresh daily — the index updates daily

GPR_URL = "https://www.matteoiacoviello.com/gpr_files/data_gpr_daily_recent.xls"

# GPR regime thresholds (based on historical distribution)
REGIMES = {
    "low": (0, 75),
    "normal": (75, 125),
    "elevated": (125, 200),
    "crisis": (200, 9999),
}


def _is_fresh():
    if not CACHE_FILE.exists():
        return False
    mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=TTL_DAYS)


def _load_cache():
    try:
        with open(CACHE_FILE, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def _save_cache(df):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(df, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print(f"[gpr_cache] WARNING: could not write cache: {e}", file=sys.stderr)


def _fetch_gpr():
    """Download and parse GPR daily index. Returns DataFrame with DatetimeIndex."""
    if _is_fresh():
        cached = _load_cache()
        if cached is not None:
            print("[gpr_cache] HIT gpr_daily", file=sys.stderr)
            return cached

    print("[gpr_cache] MISS gpr_daily - downloading", file=sys.stderr)

    try:
        resp = requests.get(GPR_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[gpr] Error downloading GPR data: {e}", file=sys.stderr)
        # Try cache even if stale
        cached = _load_cache()
        if cached is not None:
            print("[gpr] Using stale cache as fallback", file=sys.stderr)
            return cached
        return pd.DataFrame()

    try:
        raw = pd.read_excel(BytesIO(resp.content))
    except Exception as e:
        print(f"[gpr] Error parsing GPR Excel: {e}", file=sys.stderr)
        return pd.DataFrame()

    # Parse: DAY column is YYYYMMDD integer, GPRD is the index
    df = raw[["DAY", "GPRD", "GPRD_ACT", "GPRD_THREAT"]].copy()
    df = df.dropna(subset=["GPRD"])
    df["date"] = pd.to_datetime(df["DAY"].astype(int).astype(str), format="%Y%m%d")
    df = df.set_index("date").drop(columns=["DAY"])
    df = df.rename(columns={
        "GPRD": "gpr",
        "GPRD_ACT": "gpr_acts",
        "GPRD_THREAT": "gpr_threats",
    })

    # Add moving averages
    df["gpr_ma7"] = df["gpr"].rolling(7, min_periods=1).mean()
    df["gpr_ma30"] = df["gpr"].rolling(30, min_periods=1).mean()

    print(f"[gpr] Loaded {len(df)} daily observations ({df.index[0].date()} to {df.index[-1].date()})", file=sys.stderr)
    _save_cache(df)
    return df


def get_gpr_level(date):
    """Return the GPR index value for a given date (or nearest prior)."""
    df = _fetch_gpr()
    if df.empty:
        return None
    ts = pd.Timestamp(date)
    val = df["gpr"].asof(ts)
    return round(float(val), 1) if not pd.isna(val) else None


def get_gpr_regime(date):
    """Classify geopolitical risk as 'low', 'normal', 'elevated', or 'crisis'."""
    level = get_gpr_level(date)
    if level is None:
        return "unknown"
    for regime, (lo, hi) in REGIMES.items():
        if lo <= level < hi:
            return regime
    return "unknown"


def get_gpr_context(date):
    """Return full geopolitical risk context for a date.

    Returns dict with current level, regime, 7d/30d averages, trend,
    and whether current level is above the 30-day average (escalating).
    """
    df = _fetch_gpr()
    if df.empty:
        return {"error": "GPR data unavailable"}

    ts = pd.Timestamp(date)
    row = df.loc[:ts].iloc[-1] if ts >= df.index[0] else None
    if row is None:
        return {"error": f"No GPR data for {date}"}

    level = round(float(row["gpr"]), 1)
    ma7 = round(float(row["gpr_ma7"]), 1)
    ma30 = round(float(row["gpr_ma30"]), 1)

    # Determine regime
    regime = "unknown"
    for r, (lo, hi) in REGIMES.items():
        if lo <= level < hi:
            regime = r
            break

    # Trend: compare 7d MA to 30d MA
    if ma7 > ma30 * 1.1:
        trend = "escalating"
    elif ma7 < ma30 * 0.9:
        trend = "de-escalating"
    else:
        trend = "stable"

    # Percentile in historical distribution
    all_vals = df["gpr"].dropna()
    percentile = round((all_vals < level).mean() * 100, 1)

    return {
        "date": date,
        "gpr_level": level,
        "gpr_regime": regime,
        "gpr_ma7": ma7,
        "gpr_ma30": ma30,
        "gpr_trend": trend,
        "gpr_percentile": percentile,
        "gpr_acts": round(float(row["gpr_acts"]), 1) if not pd.isna(row["gpr_acts"]) else None,
        "gpr_threats": round(float(row["gpr_threats"]), 1) if not pd.isna(row["gpr_threats"]) else None,
    }


if __name__ == "__main__":
    print("=== GPR Index Test ===\n")

    ctx = get_gpr_context("2024-01-15")
    print("GPR context 2024-01-15:")
    for k, v in ctx.items():
        print(f"  {k}: {v}")

    print()
    ctx_now = get_gpr_context(datetime.now().strftime("%Y-%m-%d"))
    print(f"GPR context today ({ctx_now.get('date')}):")
    for k, v in ctx_now.items():
        print(f"  {k}: {v}")
