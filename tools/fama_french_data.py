"""
Fama-French factor data loader with disk caching.

Downloads daily factor returns from the Ken French data library (free, no API key).
Cache lives at ~/.ff_cache/ — full dataset cached as a single pickle, TTL 30 days.

Factors:
    Mkt-RF  — Market excess return (market minus risk-free)
    SMB     — Small Minus Big (size factor)
    HML     — High Minus Low (value factor)
    RF      — Risk-free rate
    Mom     — Momentum (from separate file)

Usage:
    from tools.fama_french_data import get_ff_factors, compute_factor_exposure
    factors = get_ff_factors("2020-01-01", "2024-01-01")
    exposure = compute_factor_exposure("AAPL", "2023-01-01", "2024-01-01")
"""

import io
import sys
import pickle
import zipfile
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

try:
    from tools.yfinance_utils import safe_download
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tools.yfinance_utils import safe_download

CACHE_DIR = Path.home() / ".ff_cache"
TTL_DAYS = 30

FF3_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
MOM_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"


def _is_fresh(path):
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=TTL_DAYS)


def _load_cache(path):
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def _save_cache(path, df):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "wb") as f:
            pickle.dump(df, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print(f"[ff_cache] WARNING: could not write cache {path}: {e}", file=sys.stderr)


def _parse_ff_csv(csv_text, expected_cols):
    """Parse Ken French CSV format: variable headers, YYYYMMDD dates, values in percent."""
    lines = csv_text.strip().split("\n")

    # Find data start: first line where first token is an 8-digit integer
    data_start = None
    for i, line in enumerate(lines):
        tokens = line.strip().split(",")
        if tokens and tokens[0].strip().isdigit() and len(tokens[0].strip()) == 8:
            data_start = i
            break

    if data_start is None:
        print("[ff] Could not find data start in CSV", file=sys.stderr)
        return pd.DataFrame()

    # Find data end: first blank line or non-numeric first column after data start
    data_end = len(lines)
    for i in range(data_start + 1, len(lines)):
        tokens = lines[i].strip().split(",")
        first = tokens[0].strip() if tokens else ""
        if not first or not first.isdigit() or len(first) != 8:
            data_end = i
            break

    # Parse data rows
    dates = []
    rows = []
    for i in range(data_start, data_end):
        tokens = [t.strip() for t in lines[i].split(",")]
        if len(tokens) < expected_cols + 1:
            continue
        try:
            dt = pd.Timestamp(datetime.strptime(tokens[0], "%Y%m%d"))
            vals = [float(t) / 100.0 for t in tokens[1:expected_cols + 1]]
            dates.append(dt)
            rows.append(vals)
        except (ValueError, IndexError):
            continue

    if not rows:
        return pd.DataFrame()

    return dates, rows


def _fetch_ff3():
    """Download and parse Fama-French 3 factors + RF."""
    cache_path = CACHE_DIR / "ff_factors_daily.pkl"

    if _is_fresh(cache_path):
        cached = _load_cache(cache_path)
        if cached is not None:
            print("[ff_cache] HIT ff_factors_daily", file=sys.stderr)
            return cached

    print("[ff_cache] MISS ff_factors_daily - downloading", file=sys.stderr)

    try:
        resp = requests.get(FF3_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ff] Error downloading FF3 factors: {e}", file=sys.stderr)
        return pd.DataFrame()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = [n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")][0]
        csv_text = zf.read(csv_name).decode("utf-8")

    result = _parse_ff_csv(csv_text, expected_cols=4)
    if not result:
        return pd.DataFrame()

    dates, rows = result
    df = pd.DataFrame(rows, index=pd.DatetimeIndex(dates),
                      columns=["Mkt-RF", "SMB", "HML", "RF"])
    print(f"[ff] Loaded {len(df)} daily factor observations", file=sys.stderr)
    _save_cache(cache_path, df)
    return df


def _fetch_momentum():
    """Download and parse Fama-French momentum factor."""
    cache_path = CACHE_DIR / "ff_momentum_daily.pkl"

    if _is_fresh(cache_path):
        cached = _load_cache(cache_path)
        if cached is not None:
            print("[ff_cache] HIT ff_momentum_daily", file=sys.stderr)
            return cached

    print("[ff_cache] MISS ff_momentum_daily - downloading", file=sys.stderr)

    try:
        resp = requests.get(MOM_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ff] Error downloading momentum factor: {e}", file=sys.stderr)
        return pd.Series(dtype=float, name="Mom")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = [n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")][0]
        csv_text = zf.read(csv_name).decode("utf-8")

    result = _parse_ff_csv(csv_text, expected_cols=1)
    if not result:
        return pd.Series(dtype=float, name="Mom")

    dates, rows = result
    series = pd.Series([r[0] for r in rows], index=pd.DatetimeIndex(dates),
                       dtype=float, name="Mom")
    print(f"[ff] Loaded {len(series)} daily momentum observations", file=sys.stderr)
    _save_cache(cache_path, series)
    return series


def get_ff_factors(start, end):
    """Return DataFrame with Mkt-RF, SMB, HML, RF columns for date range.

    Values are decimal returns (e.g., 0.003 = 0.3%).
    """
    df = _fetch_ff3()
    if df.empty:
        return df
    mask = (df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))
    return df.loc[mask].copy()


def get_momentum_factor(start, end):
    """Return momentum factor Series for date range. Values are decimal returns."""
    series = _fetch_momentum()
    if series.empty:
        return series
    mask = (series.index >= pd.Timestamp(start)) & (series.index <= pd.Timestamp(end))
    return series.loc[mask].copy()


def compute_factor_exposure(symbol, start, end):
    """Regress stock excess returns on Fama-French factors + momentum.

    Returns dict with factor betas, alpha, R-squared, and interpretation.
    Uses numpy.linalg.lstsq (no statsmodels dependency).
    """
    # Get stock returns
    df_price = safe_download(symbol, start=start, end=end)
    if df_price.empty or len(df_price) < 30:
        return {"error": f"Insufficient price data for {symbol} ({len(df_price)} days)"}

    stock_returns = df_price["Close"].pct_change().dropna()

    # Get factors
    factors = get_ff_factors(start, end)
    momentum = get_momentum_factor(start, end)

    if factors.empty:
        return {"error": "Could not fetch Fama-French factors"}

    # Merge all data on common dates
    combined = pd.DataFrame({"stock_ret": stock_returns})
    combined = combined.join(factors, how="inner")
    if not momentum.empty:
        combined = combined.join(momentum, how="inner")

    if len(combined) < 30:
        return {"error": f"Only {len(combined)} overlapping trading days (need 30+)"}

    # Excess returns = stock return - risk-free rate
    combined["excess_ret"] = combined["stock_ret"] - combined["RF"]

    # Build regression: excess_ret = alpha + b1*MktRF + b2*SMB + b3*HML [+ b4*Mom]
    has_mom = "Mom" in combined.columns
    if has_mom:
        X_cols = ["Mkt-RF", "SMB", "HML", "Mom"]
    else:
        X_cols = ["Mkt-RF", "SMB", "HML"]

    y = combined["excess_ret"].values
    X = combined[X_cols].values
    # Add intercept column
    X_with_const = np.column_stack([np.ones(len(X)), X])

    # OLS via lstsq
    coeffs, residuals, rank, sv = np.linalg.lstsq(X_with_const, y, rcond=None)

    alpha = coeffs[0]
    betas = coeffs[1:]

    # R-squared
    y_hat = X_with_const @ coeffs
    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Annualize alpha (252 trading days)
    alpha_annual = alpha * 252

    # Build interpretation
    parts = []
    smb_beta = betas[1]
    hml_beta = betas[2]
    mom_beta = betas[3] if has_mom else 0

    if smb_beta > 0.2:
        parts.append("small-cap tilt")
    elif smb_beta < -0.2:
        parts.append("large-cap tilt")

    if hml_beta > 0.2:
        parts.append("value tilt")
    elif hml_beta < -0.2:
        parts.append("growth tilt")

    if has_mom and mom_beta > 0.2:
        parts.append("positive momentum")
    elif has_mom and mom_beta < -0.2:
        parts.append("negative momentum")

    interpretation = ", ".join(parts) if parts else "neutral factor exposure"

    result = {
        "symbol": symbol,
        "period": f"{start} to {end}",
        "n_days": len(combined),
        "alpha_daily": round(alpha, 6),
        "alpha_annual_pct": round(alpha_annual * 100, 2),
        "market_beta": round(betas[0], 3),
        "smb_beta": round(betas[1], 3),
        "hml_beta": round(betas[2], 3),
        "mom_beta": round(betas[3], 3) if has_mom else None,
        "r_squared": round(r_squared, 4),
        "interpretation": interpretation,
    }
    return result


if __name__ == "__main__":
    print("=== Fama-French Factor Data Test ===")

    factors = get_ff_factors("2023-01-01", "2024-01-01")
    print(f"\nFF3 factors 2023: {len(factors)} trading days")
    if not factors.empty:
        print(factors.describe().round(4))

    mom = get_momentum_factor("2023-01-01", "2024-01-01")
    print(f"\nMomentum 2023: {len(mom)} trading days")

    print("\n--- AAPL Factor Exposure 2023 ---")
    exp = compute_factor_exposure("AAPL", "2023-01-01", "2024-01-01")
    for k, v in exp.items():
        print(f"  {k}: {v}")
