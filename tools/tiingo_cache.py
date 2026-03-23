"""
Tiingo disk cache — prevents 429 rate limit errors when backtesting delisted tickers.

Cache lives at ~/.tiingo_cache/{symbol}_{start}_{end}.pkl
TTL: 30 days for non-empty results, 7 days for empty results (missing/delisted tickers).

Usage:
    from tools.tiingo_cache import get_tiingo_cached
    df = get_tiingo_cached("ENRN", "2001-01-01", "2002-01-01")
"""

import os
import sys
import pickle
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Import TIINGO_API_KEY from config — works whether called from repo root or tools/
try:
    from config import TIINGO_API_KEY
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import TIINGO_API_KEY

CACHE_DIR = Path.home() / ".tiingo_cache"
TTL_NONEMPTY_DAYS = 30
TTL_EMPTY_DAYS = 7   # shorter TTL for "no data" results — ticker may come back


def _sanitize(s: str) -> str:
    """Replace characters that are not safe in filenames."""
    return re.sub(r"[^A-Za-z0-9_\-]", "_", s)


def _cache_path(symbol: str, start_str: str, end_str: str) -> Path:
    key = f"{_sanitize(symbol)}_{_sanitize(start_str)}_{_sanitize(end_str)}.pkl"
    return CACHE_DIR / key


def _is_fresh(path: Path, empty: bool) -> bool:
    """Return True if the cached file is within its TTL."""
    ttl_days = TTL_EMPTY_DAYS if empty else TTL_NONEMPTY_DAYS
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=ttl_days)


def _load_cache(path: Path):
    """Load and return (df, is_empty_result) from a pickle file, or None on error."""
    try:
        with open(path, "rb") as f:
            payload = pickle.load(f)
        # payload is {"df": DataFrame, "empty_result": bool}
        return payload["df"], payload["empty_result"]
    except Exception:
        return None


def _save_cache(path: Path, df: pd.DataFrame):
    """Persist df to cache. Marks empty_result=True when df is empty."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"df": df, "empty_result": df.empty}
    try:
        with open(path, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print(f"[tiingo_cache] WARNING: could not write cache {path}: {e}", file=sys.stderr)


def get_tiingo_cached(symbol: str, start_str: str, end_str: str) -> pd.DataFrame:
    """
    Fetch Tiingo daily OHLCV for symbol between start_str and end_str.

    Returns a DataFrame with DatetimeIndex and Open/High/Low/Close/Volume columns,
    matching the yfinance output format used throughout market_data.py.
    Returns an empty DataFrame when the ticker is unknown or TIINGO_API_KEY is unset.

    Results are cached to ~/.tiingo_cache/ as pickle files:
    - Non-empty results: 30-day TTL
    - Empty results (missing ticker): 7-day TTL
    """
    if not TIINGO_API_KEY:
        return pd.DataFrame()

    path = _cache_path(symbol, start_str, end_str)

    # --- Cache check ---
    if path.exists():
        cached = _load_cache(path)
        if cached is not None:
            df, empty_result = cached
            if _is_fresh(path, empty=empty_result):
                print(f"[tiingo_cache] HIT {symbol}", file=sys.stderr)
                return df
            # Stale — fall through to fetch

    # --- Cache miss: fetch from Tiingo ---
    print(f"[tiingo_cache] MISS {symbol} - fetching", file=sys.stderr)

    tiingo_symbol = symbol.upper().replace(".", "-")

    try:
        url = f"https://api.tiingo.com/tiingo/daily/{tiingo_symbol}/prices"
        resp = requests.get(url, params={
            "startDate": start_str,
            "endDate": end_str,
            "token": TIINGO_API_KEY,
        }, timeout=15)

        if resp.status_code == 404:
            _save_cache(path, pd.DataFrame())
            return pd.DataFrame()

        if resp.status_code == 429:
            print(f"[tiingo_cache] 429 rate limit for {symbol} — returning empty (not caching)", file=sys.stderr)
            return pd.DataFrame()

        resp.raise_for_status()

        data = resp.json()
        if not data:
            _save_cache(path, pd.DataFrame())
            return pd.DataFrame()

        # Build DataFrame matching yfinance structure
        rows = []
        for d in data:
            dt = pd.Timestamp(d["date"]).tz_localize(None)
            rows.append({
                "Date": dt,
                "Open": d.get("adjOpen", d.get("open", 0)),
                "High": d.get("adjHigh", d.get("high", 0)),
                "Low": d.get("adjLow", d.get("low", 0)),
                "Close": d.get("adjClose", d.get("close", 0)),
                "Volume": d.get("adjVolume", d.get("volume", 0)),
            })

        df = pd.DataFrame(rows).set_index("Date")
        print(f"[tiingo_cache] Fetched {len(df)} days for {symbol}", file=sys.stderr)
        _save_cache(path, df)
        return df

    except Exception as e:
        print(f"[tiingo_cache] Error fetching {symbol}: {e}", file=sys.stderr)
        return pd.DataFrame()
