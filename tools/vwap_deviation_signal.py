"""
VWAP Deviation Signal Generator

Tests the disposition effect hypothesis: when a stock trades significantly
above its 252-day rolling VWAP (volume-weighted average cost basis), most
investors are sitting on unrealized gains and are more likely to sell,
creating downward price pressure.

Usage:
    from tools.vwap_deviation_signal import find_vwap_deviation_events

    events = find_vwap_deviation_events(
        symbols=["AAPL", "MSFT", "GOOGL"],
        start_date="2018-01-01",
        end_date="2024-12-31",
        threshold=1.15,             # close must be >= 15% above VWAP_252
        cooldown_days=20,           # suppress consecutive signals within N trading days
        require_low_volume=False,   # if True: only signals where recent vol < avg vol
        require_high_volume=False,  # if True: only signals where recent vol > avg vol
        vwap_window=252,            # rolling window for VWAP computation
        volume_short_window=20,     # short-term volume avg window
        volume_long_window=100,     # long-term volume avg window
        verbose=True,
    )
    # Returns: [{"symbol": "AAPL", "date": "2022-03-15", "vwap_ratio": 1.21}, ...]
"""

import sys
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

# Allow importing from project root
sys.path.insert(0, str(Path(__file__).parent.parent))


def _download_ohlcv(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download daily OHLCV data from yfinance.
    Returns DataFrame with columns: Open, High, Low, Close, Volume.
    Index is DatetimeIndex (tz-naive).
    Returns empty DataFrame on failure.
    """
    try:
        # Download with a 300-day buffer before start so VWAP_252 is warm at start_date
        start_dt = pd.Timestamp(start_date) - pd.DateOffset(days=365)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = yf.download(
                symbol,
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_date,
                auto_adjust=True,
                progress=False,
            )
        if df.empty:
            return pd.DataFrame()

        # Flatten MultiIndex columns if present (yfinance >= 0.2.x)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        # Ensure tz-naive index
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # Keep only OHLCV columns
        needed = ["Open", "High", "Low", "Close", "Volume"]
        missing = [c for c in needed if c not in df.columns]
        if missing:
            return pd.DataFrame()

        df = df[needed].copy()
        df = df[df["Volume"] > 0].copy()  # drop zero-volume rows (holidays/gaps)
        df = df.dropna(subset=["Close", "Volume"])
        return df

    except Exception as e:
        print(f"  [vwap_signal] yfinance error for {symbol}: {e}", file=sys.stderr)
        return pd.DataFrame()


def _compute_rolling_vwap(df: pd.DataFrame, window: int) -> pd.Series:
    """
    Compute rolling VWAP = sum(close * volume) / sum(volume) over `window` trading days.
    Returns a Series aligned to df.index. Values are NaN until the window is warm.
    """
    pv = df["Close"] * df["Volume"]
    rolling_pv = pv.rolling(window=window, min_periods=window)
    rolling_vol = df["Volume"].rolling(window=window, min_periods=window)
    vwap = rolling_pv.sum() / rolling_vol.sum()
    return vwap


def find_vwap_deviation_events(
    symbols: list,
    start_date: str,
    end_date: str,
    threshold: float = 1.15,
    cooldown_days: int = 20,
    require_low_volume: bool = False,
    require_high_volume: bool = False,
    vwap_window: int = 252,
    volume_short_window: int = 20,
    volume_long_window: int = 100,
    verbose: bool = True,
) -> list:
    """
    Find dates where each symbol trades >= `threshold` * VWAP_252.

    Args:
        symbols: List of ticker symbols to scan.
        start_date: Earliest event date to include (YYYY-MM-DD).
        end_date: Latest event date to include (YYYY-MM-DD).
        threshold: Minimum ratio of Close / VWAP_252 to trigger a signal (default 1.15).
        cooldown_days: Minimum number of trading days between signals for the same symbol.
            Prevents event clustering from dominating the backtest.
        require_low_volume: If True, only keep signals where the 20-day avg volume
            is BELOW the 100-day avg volume (rise on light volume — stronger
            disposition effect as fewer new buyers exist to absorb sellers).
        require_high_volume: If True, only keep signals where the 20-day avg volume
            is ABOVE the 100-day avg volume (rise on heavy volume — more participants
            sitting on gains). Mutually exclusive with require_low_volume.
        vwap_window: Rolling window for VWAP computation in trading days (default 252).
        volume_short_window: Short-term volume average window in trading days.
        volume_long_window: Long-term volume average window in trading days.
        verbose: Print progress and per-symbol summary.

    Returns:
        List of dicts: [{"symbol": "AAPL", "date": "2022-03-15",
                         "vwap_ratio": 1.21, "vol_ratio": 0.87}, ...]
        Sorted by date ascending.
    """
    if require_low_volume and require_high_volume:
        raise ValueError("require_low_volume and require_high_volume are mutually exclusive.")

    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date)

    all_events = []

    for symbol in symbols:
        if verbose:
            print(f"  Processing {symbol}...")

        df = _download_ohlcv(symbol, start_date, end_date)
        if df.empty:
            if verbose:
                print(f"    -> No data, skipping.")
            continue

        # Compute rolling VWAP
        df["vwap_252"] = _compute_rolling_vwap(df, vwap_window)

        # Compute volume ratio (short vs long moving average)
        df["vol_ma_short"] = df["Volume"].rolling(window=volume_short_window,
                                                    min_periods=volume_short_window).mean()
        df["vol_ma_long"] = df["Volume"].rolling(window=volume_long_window,
                                                   min_periods=volume_long_window).mean()
        df["vol_ratio"] = df["vol_ma_short"] / df["vol_ma_long"]

        # Compute VWAP deviation ratio
        df["vwap_ratio"] = df["Close"] / df["vwap_252"]

        # Trim to the requested date range (VWAP is warm by now due to buffer)
        df_range = df[(df.index >= start_dt) & (df.index <= end_dt)].copy()
        if df_range.empty:
            continue

        # Drop rows where VWAP isn't warmed up yet
        df_range = df_range.dropna(subset=["vwap_252", "vol_ma_short", "vol_ma_long"])

        # Apply threshold filter
        candidates = df_range[df_range["vwap_ratio"] >= threshold].copy()

        # Apply volume quality filter
        if require_low_volume:
            candidates = candidates[candidates["vol_ratio"] < 1.0].copy()
        elif require_high_volume:
            candidates = candidates[candidates["vol_ratio"] >= 1.0].copy()

        if candidates.empty:
            if verbose:
                print(f"    -> 0 events after filters.")
            continue

        # De-duplicate: enforce cooldown_days between consecutive signals
        # Sort by date and greedily pick signals separated by >= cooldown_days
        deduped = []
        last_signal_date = None

        for idx, row in candidates.iterrows():
            if last_signal_date is None:
                deduped.append(idx)
                last_signal_date = idx
            else:
                # Count trading days since last signal
                trading_days_since = (
                    df_range.index.get_indexer([idx], method="nearest")[0] -
                    df_range.index.get_indexer([last_signal_date], method="nearest")[0]
                )
                if trading_days_since >= cooldown_days:
                    deduped.append(idx)
                    last_signal_date = idx

        n_before = len(candidates)
        n_after = len(deduped)

        for idx in deduped:
            row = candidates.loc[idx]
            all_events.append({
                "symbol": symbol,
                "date": idx.strftime("%Y-%m-%d"),
                "vwap_ratio": round(float(row["vwap_ratio"]), 4),
                "vol_ratio": round(float(row["vol_ratio"]), 4) if not np.isnan(row["vol_ratio"]) else None,
                "close": round(float(row["Close"]), 2),
                "vwap_252": round(float(row["vwap_252"]), 2),
            })

        if verbose:
            print(f"    -> {n_before} raw signals, {n_after} after {cooldown_days}-day cooldown")

    # Sort all events by date
    all_events.sort(key=lambda x: x["date"])

    if verbose:
        print(f"\nTotal events found: {len(all_events)}")

    return all_events


if __name__ == "__main__":
    # Quick sanity check on a single symbol
    test_events = find_vwap_deviation_events(
        symbols=["AAPL"],
        start_date="2020-01-01",
        end_date="2023-12-31",
        threshold=1.15,
        verbose=True,
    )
    for e in test_events[:5]:
        print(e)
