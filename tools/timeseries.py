"""
Unified time series fetcher — resolves arbitrary series identifiers to price data.

Series identifier format:
    "AAPL"              -> yfinance equity
    "CL=F"              -> yfinance commodity future (oil)
    "GC=F"              -> yfinance gold future
    "EURUSD=X"          -> yfinance FX pair
    "^VIX"              -> yfinance index
    "BTC-USD"           -> yfinance crypto
    "FRED:DGS10"        -> FRED series via tools/fred_data.py
    "FRED:FEDFUNDS"     -> FRED series
    "FF:Mkt-RF"         -> Fama-French factor via tools/fama_french_data.py

Usage:
    from tools.timeseries import get_series, get_returns, get_aligned_returns

    oil = get_series("CL=F", "2020-01-01", "2025-01-01")
    aal_returns = get_returns("AAL", "2020-01-01", "2025-01-01")
    aligned = get_aligned_returns(["CL=F", "AAL", "SPY"], "2020-01-01", "2025-01-01")
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Ensure tools/ and parent are importable
_tools_dir = Path(__file__).parent
_project_dir = _tools_dir.parent
if str(_tools_dir) not in sys.path:
    sys.path.insert(0, str(_tools_dir))
if str(_project_dir) not in sys.path:
    sys.path.insert(0, str(_project_dir))


def get_series(identifier: str, start: str, end: str, field: str = "close") -> pd.Series:
    """
    Fetch a time series by identifier.

    Args:
        identifier: Series identifier (see module docstring for format).
        start: Start date "YYYY-MM-DD".
        end: End date "YYYY-MM-DD".
        field: Price field for yfinance tickers ("close", "open", "high", "low", "volume").
               Ignored for FRED/FF sources.

    Returns:
        pd.Series with DatetimeIndex, named after the identifier.
    """
    if identifier.startswith("FRED:"):
        series_id = identifier.split(":", 1)[1]
        from tools.fred_data import get_fred_series
        s = get_fred_series(series_id, start, end)
        if s is None or (hasattr(s, "empty") and s.empty):
            raise ValueError(f"No data for FRED series {series_id}")
        s.name = identifier
        return s

    if identifier.startswith("FF:"):
        factor_name = identifier.split(":", 1)[1]
        from tools.fama_french_data import get_ff_factors, get_momentum_factor
        if factor_name == "Mom":
            s = get_momentum_factor(start, end)
        else:
            factors = get_ff_factors(start, end)
            if factor_name not in factors.columns:
                raise ValueError(
                    f"Unknown FF factor: {factor_name}. "
                    f"Available: {list(factors.columns)}"
                )
            s = factors[factor_name]
        if s is None or s.empty:
            raise ValueError(f"No data for FF factor {factor_name}")
        s.name = identifier
        return s

    # yfinance path — handles equities, futures, FX, crypto, indices
    from tools.yfinance_utils import safe_download
    df = safe_download(identifier, start=start, end=end)
    col = field.capitalize()
    if col not in df.columns:
        raise ValueError(
            f"Column '{col}' not found for {identifier}. "
            f"Available: {list(df.columns)}"
        )
    s = df[col].dropna()
    s.name = identifier
    return s


def get_returns(identifier: str, start: str, end: str, frequency: str = "daily") -> pd.Series:
    """
    Fetch a series and compute percentage returns.

    Args:
        identifier: Series identifier.
        start: Start date.
        end: End date.
        frequency: "daily" (default) or "weekly".

    Returns:
        pd.Series of percentage returns (e.g., 1.5 means +1.5%).
    """
    s = get_series(identifier, start, end)

    # FRED/FF series may already be in return or rate form — detect
    if identifier.startswith("FF:"):
        # Fama-French factors are already daily returns (decimal, e.g., 0.003 = 0.3%)
        returns = s * 100  # convert to percentage
    elif identifier.startswith("FRED:"):
        # FRED series are levels (rates, indices) — compute changes
        returns = s.pct_change().dropna() * 100
    else:
        # Price series — compute pct change
        returns = s.pct_change().dropna() * 100

    if frequency == "weekly":
        returns = returns.resample("W-FRI").sum()

    returns.name = identifier
    return returns


def get_aligned_series(
    identifiers: list[str], start: str, end: str, field: str = "close"
) -> pd.DataFrame:
    """
    Fetch multiple series, aligned on common trading dates (inner join).

    Returns:
        pd.DataFrame with one column per identifier, DatetimeIndex.
    """
    series_list = {}
    for ident in identifiers:
        try:
            series_list[ident] = get_series(ident, start, end, field=field)
        except (ValueError, Exception) as e:
            raise ValueError(f"Failed to fetch {ident}: {e}") from e

    df = pd.DataFrame(series_list)
    df = df.dropna()
    if df.empty:
        raise ValueError(
            f"No overlapping data for identifiers {identifiers} "
            f"between {start} and {end}"
        )
    return df


def get_aligned_returns(
    identifiers: list[str], start: str, end: str, frequency: str = "daily"
) -> pd.DataFrame:
    """
    Fetch multiple series, align, compute returns.

    Returns:
        pd.DataFrame of percentage returns, one column per identifier.
    """
    returns_list = {}
    for ident in identifiers:
        try:
            returns_list[ident] = get_returns(ident, start, end, frequency=frequency)
        except (ValueError, Exception) as e:
            raise ValueError(f"Failed to fetch returns for {ident}: {e}") from e

    df = pd.DataFrame(returns_list)
    df = df.dropna()
    if df.empty:
        raise ValueError(
            f"No overlapping return data for {identifiers} "
            f"between {start} and {end}"
        )
    return df


if __name__ == "__main__":
    print("Testing timeseries.py...")

    # Test yfinance equity
    spy = get_series("SPY", "2024-01-01", "2024-03-01")
    print(f"  SPY: {len(spy)} days, last={spy.iloc[-1]:.2f}")

    # Test yfinance commodity future
    oil = get_series("CL=F", "2024-01-01", "2024-03-01")
    print(f"  CL=F: {len(oil)} days, last={oil.iloc[-1]:.2f}")

    # Test returns
    aal_ret = get_returns("AAL", "2024-01-01", "2024-03-01")
    print(f"  AAL returns: {len(aal_ret)} days, mean={aal_ret.mean():.3f}%")

    # Test aligned returns
    aligned = get_aligned_returns(["CL=F", "AAL", "SPY"], "2024-01-01", "2024-03-01")
    print(f"  Aligned returns: {aligned.shape}, columns={list(aligned.columns)}")

    print("\nAll tests passed.")
