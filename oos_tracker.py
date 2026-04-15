"""
oos_tracker.py — Automated OOS (Out-of-Sample) observation tracking.

Replaces ad-hoc knowledge base entries with proper relational tracking.
Each signal event (NT 10-K filing, SEO, delisting, etc.) gets registered
as an OOS observation with entry prices. The update function automatically
fetches daily prices and computes abnormal returns through the hold period.

Usage via data_tasks.py:
    python3 data_tasks.py oos register --signal-type nt_10k_late_filing_short \
        --symbol AAPL --entry-date 2026-04-15 --hold-days 5 --direction short \
        --threshold -2.5
    python3 data_tasks.py oos update
    python3 data_tasks.py oos status
    python3 data_tasks.py oos close --id OOS-abc12345 --result validated
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta

import pandas as pd

import db


def _generate_id():
    return "OOS-" + uuid.uuid4().hex[:8]


def _fetch_close_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Fetch close prices for a list of tickers. Returns DataFrame with ticker columns."""
    from tools.yfinance_utils import get_close_prices
    try:
        return get_close_prices(tickers, start=start, end=end)
    except (ValueError, Exception) as e:
        print(f"[oos_tracker] get_close_prices failed: {e}", file=sys.stderr)
        # Fallback: fetch one by one via Tiingo
        import os
        import requests
        tiingo_key = os.environ.get("TIINGO_API_KEY", "")
        if not tiingo_key:
            return pd.DataFrame()
        frames = {}
        for ticker in tickers:
            try:
                headers = {"Content-Type": "application/json",
                           "Authorization": f"Token {tiingo_key}"}
                url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
                r = requests.get(url, params={"startDate": start, "endDate": end},
                                 headers=headers, timeout=15)
                if r.status_code == 200 and r.json():
                    data = r.json()
                    dates = [pd.Timestamp(d["date"]).tz_localize(None) for d in data]
                    closes = [d.get("adjClose", d.get("close", 0)) for d in data]
                    frames[ticker] = pd.Series(closes, index=dates, name=ticker)
            except Exception as ex:
                print(f"[oos_tracker] Tiingo fallback failed for {ticker}: {ex}",
                      file=sys.stderr)
        if frames:
            return pd.DataFrame(frames)
        return pd.DataFrame()


def register_observation(
    signal_type: str,
    symbol: str,
    entry_date: str,
    hold_days: int,
    direction: str,
    threshold: float | None = None,
    benchmark: str = "SPY",
    hypothesis_id: str | None = None,
    notes: str | None = None,
) -> dict:
    """
    Register a new OOS observation. Fetches entry prices automatically.

    Returns dict with observation details and result_id.
    """
    db.init_db()

    # Fetch entry prices
    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d")
    start_str = (entry_dt - timedelta(days=5)).strftime("%Y-%m-%d")
    end_str = (entry_dt + timedelta(days=3)).strftime("%Y-%m-%d")

    tickers = list(set([symbol, benchmark]))
    prices = _fetch_close_prices(tickers, start_str, end_str)

    if prices.empty:
        return {"status": "error", "error": f"Cannot fetch prices for {tickers}"}

    # Find entry prices: closest trading day on or before entry_date
    entry_ts = pd.Timestamp(entry_date)
    valid_dates = prices.index[prices.index <= entry_ts]
    if valid_dates.empty:
        # Try the first available date after entry
        valid_dates = prices.index
    if valid_dates.empty:
        return {"status": "error", "error": f"No price data around {entry_date}"}

    entry_row_date = valid_dates[-1]

    # Get prices for both symbol and benchmark
    if symbol not in prices.columns or benchmark not in prices.columns:
        missing = [t for t in [symbol, benchmark] if t not in prices.columns]
        return {"status": "error", "error": f"Missing price data for: {missing}"}

    entry_price = float(prices.loc[entry_row_date, symbol])
    entry_benchmark_price = float(prices.loc[entry_row_date, benchmark])

    if pd.isna(entry_price) or pd.isna(entry_benchmark_price):
        return {"status": "error", "error": f"NaN prices on {entry_row_date}"}

    obs_id = _generate_id()
    actual_entry_date = entry_row_date.strftime("%Y-%m-%d")

    obs = db.create_oos_observation(
        obs_id=obs_id,
        signal_type=signal_type,
        symbol=symbol,
        benchmark=benchmark,
        direction=direction,
        entry_date=actual_entry_date,
        entry_price=entry_price,
        entry_benchmark_price=entry_benchmark_price,
        hold_days=hold_days,
        success_threshold_pct=threshold,
        hypothesis_id=hypothesis_id,
        notes=notes,
    )

    return {
        "status": "ok",
        "id": obs_id,
        "signal_type": signal_type,
        "symbol": symbol,
        "benchmark": benchmark,
        "direction": direction,
        "entry_date": actual_entry_date,
        "entry_price": entry_price,
        "entry_benchmark_price": entry_benchmark_price,
        "hold_days": hold_days,
        "success_threshold_pct": threshold,
    }


def update_all_active() -> dict:
    """
    Update all active OOS observations with latest prices.

    Fetches prices in batch, computes daily returns, marks expired observations.
    Returns summary of updates.
    """
    db.init_db()
    active = db.get_active_oos_observations()

    if not active:
        return {"status": "ok", "updated": 0, "message": "No active OOS observations"}

    # Gather all unique tickers and date range needed
    all_tickers = set()
    earliest_date = None
    for obs in active:
        all_tickers.add(obs["symbol"])
        all_tickers.add(obs["benchmark"])
        d = datetime.strptime(obs["entry_date"], "%Y-%m-%d")
        if earliest_date is None or d < earliest_date:
            earliest_date = d

    start_str = earliest_date.strftime("%Y-%m-%d")
    end_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    # Batch fetch all prices
    all_tickers = sorted(all_tickers)
    prices = _fetch_close_prices(all_tickers, start_str, end_str)

    if prices.empty:
        return {"status": "error", "error": "Failed to fetch any prices"}

    results = []
    expired_count = 0
    updated_count = 0

    for obs in active:
        sym = obs["symbol"]
        bm = obs["benchmark"]
        entry_date = obs["entry_date"]
        entry_price = obs["entry_price"]
        entry_bm_price = obs["entry_benchmark_price"]
        hold_days = obs["hold_days"]
        obs_id = obs["id"]

        # Check if both columns exist
        if sym not in prices.columns or bm not in prices.columns:
            results.append({
                "id": obs_id, "symbol": sym,
                "error": f"Missing price data for {sym} or {bm}",
            })
            continue

        # Get trading days after entry
        entry_ts = pd.Timestamp(entry_date)
        post_entry = prices.index[prices.index > entry_ts]

        if post_entry.empty:
            results.append({"id": obs_id, "symbol": sym, "days": 0, "note": "No post-entry data yet"})
            continue

        # Get existing daily prices to avoid recomputing
        existing = db.get_oos_daily_prices(obs_id)
        existing_days = {d["day_number"] for d in existing}

        new_days = 0
        latest_abnormal = None
        for i, trade_date in enumerate(post_entry):
            day_num = i + 1
            if day_num > hold_days + 2:  # Allow a little buffer beyond hold_days
                break

            sym_close = prices.loc[trade_date, sym]
            bm_close = prices.loc[trade_date, bm]

            if pd.isna(sym_close) or pd.isna(bm_close):
                continue

            sym_close = float(sym_close)
            bm_close = float(bm_close)

            raw_return = (sym_close / entry_price - 1) * 100
            bm_return = (bm_close / entry_bm_price - 1) * 100
            abnormal = raw_return - bm_return

            db.upsert_oos_daily_price(
                observation_id=obs_id,
                day_number=day_num,
                trade_date=trade_date.strftime("%Y-%m-%d"),
                symbol_close=sym_close,
                benchmark_close=bm_close,
                raw_return_pct=round(raw_return, 4),
                benchmark_return_pct=round(bm_return, 4),
                abnormal_return_pct=round(abnormal, 4),
            )

            if day_num not in existing_days:
                new_days += 1
            latest_abnormal = abnormal

        # Check if hold period has been reached
        all_prices = db.get_oos_daily_prices(obs_id)
        max_day = max(d["day_number"] for d in all_prices) if all_prices else 0

        obs_result = {
            "id": obs_id,
            "symbol": sym,
            "direction": obs["direction"],
            "days_tracked": max_day,
            "hold_days": hold_days,
            "latest_abnormal_pct": round(latest_abnormal, 2) if latest_abnormal is not None else None,
            "new_days_added": new_days,
        }

        if max_day >= hold_days:
            # Hold period reached — mark as expired (needs human review)
            db.update_oos_status(obs_id, "expired")
            obs_result["status_change"] = "expired (hold period reached)"
            expired_count += 1

            # Get the final day's abnormal return
            final_day = [d for d in all_prices if d["day_number"] == hold_days]
            if final_day:
                obs_result["final_abnormal_pct"] = round(final_day[0]["abnormal_return_pct"], 2)

        results.append(obs_result)
        if new_days > 0:
            updated_count += 1

    return {
        "status": "ok",
        "active_checked": len(active),
        "updated": updated_count,
        "expired": expired_count,
        "results": results,
    }


def get_status_summary(signal_type: str | None = None, include_completed: bool = False) -> dict:
    """
    Get a summary of all OOS observations.

    Returns structured dict with current tracking status.
    """
    db.init_db()

    if include_completed:
        observations = db.get_oos_observations(signal_type=signal_type)
    else:
        observations = db.get_oos_observations(status="tracking", signal_type=signal_type)
        # Also include expired (awaiting review)
        expired = db.get_oos_observations(status="expired", signal_type=signal_type)
        observations = observations + expired

    summary_items = []
    for obs in observations:
        daily = db.get_oos_daily_prices(obs["id"])
        max_day = max(d["day_number"] for d in daily) if daily else 0
        latest = daily[-1] if daily else None

        direction = obs["direction"]
        abnormal = latest["abnormal_return_pct"] if latest else 0
        # Direction correct: for shorts, negative abnormal = correct
        # For longs, positive abnormal = correct
        if direction == "short":
            direction_correct = abnormal < -0.5
        else:
            direction_correct = abnormal > 0.5

        summary_items.append({
            "id": obs["id"],
            "signal_type": obs["signal_type"],
            "symbol": obs["symbol"],
            "direction": direction,
            "day": max_day,
            "hold_days": obs["hold_days"],
            "days_remaining": max(0, obs["hold_days"] - max_day),
            "current_abnormal_pct": round(abnormal, 2) if latest else None,
            "direction_correct": direction_correct,
            "threshold": obs["success_threshold_pct"],
            "status": obs["status"],
            "entry_date": obs["entry_date"],
            "hypothesis_id": obs["hypothesis_id"],
        })

    return {
        "status": "ok",
        "active_count": len([s for s in summary_items if s["status"] == "tracking"]),
        "expired_count": len([s for s in summary_items if s["status"] == "expired"]),
        "observations": summary_items,
    }


def close_observation(obs_id: str, result: str) -> dict:
    """
    Close an OOS observation with a result (validated or failed).

    Args:
        obs_id: The observation ID.
        result: "validated" or "failed".

    Returns:
        Confirmation dict with final statistics.
    """
    db.init_db()

    if result not in ("validated", "failed"):
        return {"status": "error", "error": f"Invalid result: {result}. Must be 'validated' or 'failed'."}

    obs = db.get_oos_observation(obs_id)
    if not obs:
        return {"status": "error", "error": f"Observation {obs_id} not found."}

    daily = db.get_oos_daily_prices(obs_id)
    hold_day = obs["hold_days"]
    final = [d for d in daily if d["day_number"] == hold_day]

    db.update_oos_status(obs_id, result)

    return {
        "status": "ok",
        "id": obs_id,
        "result": result,
        "signal_type": obs["signal_type"],
        "symbol": obs["symbol"],
        "direction": obs["direction"],
        "final_abnormal_pct": round(final[0]["abnormal_return_pct"], 2) if final else None,
        "hold_days": hold_day,
        "total_days_tracked": max(d["day_number"] for d in daily) if daily else 0,
    }
