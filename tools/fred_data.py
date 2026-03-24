"""
FRED (Federal Reserve Economic Data) fetcher with disk caching.

Cache lives at ~/.fred_cache/{series_id}_{start}_{end}.pkl
TTL: 30 days for non-empty results, 7 days for empty results.

Key series:
    FEDFUNDS  — Federal Funds Effective Rate
    DGS10     — 10-Year Treasury Constant Maturity Rate
    DGS2      — 2-Year Treasury Constant Maturity Rate
    CPIAUCSL  — Consumer Price Index (All Urban, Seasonally Adjusted)
    M2SL      — M2 Money Stock

Usage:
    from tools.fred_data import get_fred_series, get_macro_snapshot
    spread = get_yield_curve_spread("2020-01-01", "2024-01-01")
    snap = get_macro_snapshot("2024-01-15")
"""

import sys
import pickle
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

try:
    from config import FRED_API_KEY
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import FRED_API_KEY

CACHE_DIR = Path.home() / ".fred_cache"
TTL_NONEMPTY_DAYS = 30
TTL_EMPTY_DAYS = 7
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def _sanitize(s):
    return re.sub(r"[^A-Za-z0-9_\-]", "_", s)


def _cache_path(series_id, start_str, end_str):
    key = f"{_sanitize(series_id)}_{_sanitize(start_str)}_{_sanitize(end_str)}.pkl"
    return CACHE_DIR / key


def _is_fresh(path, empty):
    ttl_days = TTL_EMPTY_DAYS if empty else TTL_NONEMPTY_DAYS
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=ttl_days)


def _load_cache(path):
    try:
        with open(path, "rb") as f:
            payload = pickle.load(f)
        return payload["series"], payload["empty_result"]
    except Exception:
        return None


def _save_cache(path, series):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"series": series, "empty_result": len(series) == 0}
    try:
        with open(path, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print(f"[fred_cache] WARNING: could not write cache {path}: {e}", file=sys.stderr)


def get_fred_series(series_id, start, end):
    """Fetch a FRED series. Returns pd.Series with DatetimeIndex and float values."""
    if not FRED_API_KEY:
        print("[fred] FRED_API_KEY not set — returning empty", file=sys.stderr)
        return pd.Series(dtype=float)

    path = _cache_path(series_id, start, end)

    if path.exists():
        cached = _load_cache(path)
        if cached is not None:
            series, empty_result = cached
            if _is_fresh(path, empty=empty_result):
                print(f"[fred_cache] HIT {series_id}", file=sys.stderr)
                return series

    print(f"[fred_cache] MISS {series_id} - fetching", file=sys.stderr)

    try:
        resp = requests.get(FRED_BASE_URL, params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start,
            "observation_end": end,
        }, timeout=15)

        if resp.status_code == 429:
            print(f"[fred] 429 rate limit for {series_id}", file=sys.stderr)
            return pd.Series(dtype=float)

        resp.raise_for_status()
        data = resp.json()

        observations = data.get("observations", [])
        dates = []
        values = []
        for obs in observations:
            if obs["value"] == ".":  # FRED's missing data marker
                continue
            dates.append(pd.Timestamp(obs["date"]))
            values.append(float(obs["value"]))

        series = pd.Series(values, index=pd.DatetimeIndex(dates), dtype=float)
        series.name = series_id
        print(f"[fred] Fetched {len(series)} observations for {series_id}", file=sys.stderr)
        _save_cache(path, series)
        return series

    except Exception as e:
        print(f"[fred] Error fetching {series_id}: {e}", file=sys.stderr)
        return pd.Series(dtype=float)


def get_yield_curve_spread(start, end):
    """Return 10Y-2Y Treasury spread as pd.Series. Negative = inverted."""
    dgs10 = get_fred_series("DGS10", start, end)
    dgs2 = get_fred_series("DGS2", start, end)
    if dgs10.empty or dgs2.empty:
        return pd.Series(dtype=float, name="T10Y2Y_spread")
    # Align on common dates
    combined = pd.concat([dgs10, dgs2], axis=1, keys=["DGS10", "DGS2"]).dropna()
    spread = combined["DGS10"] - combined["DGS2"]
    spread.name = "T10Y2Y_spread"
    return spread


def get_rate_regime(date):
    """Classify Fed rate regime as 'hiking', 'cutting', or 'holding'.

    Compares FEDFUNDS rate at `date` vs 3 months prior.
    Threshold: >=0.25pp change.
    """
    end_dt = datetime.strptime(date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=200)  # fetch ~6 months
    ff = get_fred_series("FEDFUNDS", start_dt.strftime("%Y-%m-%d"), date)
    if len(ff) < 2:
        return "unknown"

    current = ff.iloc[-1]
    # Find rate ~3 months ago
    target_dt = end_dt - timedelta(days=90)
    prior = ff.asof(pd.Timestamp(target_dt))
    if pd.isna(prior):
        prior = ff.iloc[0]

    delta = current - prior
    if delta >= 0.25:
        return "hiking"
    elif delta <= -0.25:
        return "cutting"
    else:
        return "holding"


def get_macro_snapshot(date):
    """Return a dict of key macro indicators for a given date.

    For monthly series (CPI, M2), uses the most recent observation on or before
    the date. Year-over-year changes are computed from 13 months of data.
    """
    dt = datetime.strptime(date, "%Y-%m-%d")
    # Fetch enough history for YoY calculations
    start_13m = (dt - timedelta(days=400)).strftime("%Y-%m-%d")
    start_6m = (dt - timedelta(days=200)).strftime("%Y-%m-%d")

    # Daily series — use asof for nearest prior value
    ff = get_fred_series("FEDFUNDS", start_6m, date)
    dgs10 = get_fred_series("DGS10", start_6m, date)
    dgs2 = get_fred_series("DGS2", start_6m, date)

    # Monthly series
    cpi = get_fred_series("CPIAUCSL", start_13m, date)
    m2 = get_fred_series("M2SL", start_13m, date)

    ts = pd.Timestamp(date)

    def _asof(s):
        if s.empty:
            return None
        val = s.asof(ts)
        return round(float(val), 4) if not pd.isna(val) else None

    fed_funds = _asof(ff)
    y10 = _asof(dgs10)
    y2 = _asof(dgs2)

    spread = round(y10 - y2, 4) if y10 is not None and y2 is not None else None

    if spread is not None:
        if spread > 0.5:
            yc_status = "normal"
        elif spread >= -0.5:
            yc_status = "flat"
        else:
            yc_status = "inverted"
    else:
        yc_status = None

    # CPI year-over-year
    cpi_yoy = None
    if len(cpi) >= 2:
        latest_cpi = cpi.iloc[-1]
        target_12m = ts - timedelta(days=365)
        prior_cpi = cpi.asof(pd.Timestamp(target_12m))
        if not pd.isna(prior_cpi) and prior_cpi > 0:
            cpi_yoy = round((latest_cpi - prior_cpi) / prior_cpi * 100, 2)

    # M2 year-over-year
    m2_yoy = None
    if len(m2) >= 2:
        latest_m2 = m2.iloc[-1]
        target_12m = ts - timedelta(days=365)
        prior_m2 = m2.asof(pd.Timestamp(target_12m))
        if not pd.isna(prior_m2) and prior_m2 > 0:
            m2_yoy = round((latest_m2 - prior_m2) / prior_m2 * 100, 2)

    return {
        "date": date,
        "fed_funds_rate": fed_funds,
        "yield_10y": y10,
        "yield_2y": y2,
        "yield_curve_spread": spread,
        "yield_curve_status": yc_status,
        "rate_regime": get_rate_regime(date),
        "cpi_yoy_pct": cpi_yoy,
        "m2_yoy_pct": m2_yoy,
    }


if __name__ == "__main__":
    print("=== FRED Data Tool Test ===")
    ff = get_fred_series("FEDFUNDS", "2023-01-01", "2024-01-01")
    print(f"\nFEDFUNDS 2023: {len(ff)} observations")
    if not ff.empty:
        print(f"  Range: {ff.min():.2f}% - {ff.max():.2f}%")

    spread = get_yield_curve_spread("2023-01-01", "2024-01-01")
    print(f"\nYield curve spread 2023: {len(spread)} observations")
    if not spread.empty:
        print(f"  Range: {spread.min():.2f} - {spread.max():.2f}")

    snap = get_macro_snapshot("2024-01-15")
    print(f"\nMacro snapshot 2024-01-15:")
    for k, v in snap.items():
        print(f"  {k}: {v}")
