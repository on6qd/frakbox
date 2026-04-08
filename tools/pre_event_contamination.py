"""
Pre-event contamination check — shared helper for all event-driven short
activators (FDA decisions, earnings misses, clinical trial failures).

Rule (known_effect pre_event_contamination_rule_30d_drawdown, 2026-04-08):
    If a stock is already >20% below its 30-day peak going into the event,
    the rejection/miss is already priced in. Expected post-event abnormal
    return is much smaller than backtest, while short-squeeze risk remains.
    This is asymmetric and unfavorable. ABORT.

Motivation: GRCE dropped -35% from $5.13 to $3.33 on 11x volume April 6-7,
16 days pre-PDUFA. Without this check, the activator would have entered a
contaminated short.

Usage:
    from tools.pre_event_contamination import check_pre_event_contamination
    ok, drawdown_pct, msg = check_pre_event_contamination("GRCE", crash_pct=45)
    if not ok:
        print(f"ABORT: {msg}")
        return 1
"""
from __future__ import annotations

DEFAULT_MAX_DRAWDOWN_PCT = 20.0
DEFAULT_PEAK_WINDOW = 30  # trading days


def check_pre_event_contamination(
    symbol: str,
    crash_pct: float | None = None,
    prior_close: float | None = None,
    max_drawdown_pct: float = DEFAULT_MAX_DRAWDOWN_PCT,
    peak_window: int = DEFAULT_PEAK_WINDOW,
    event_date: str | None = None,
):
    """
    Returns (ok: bool, drawdown_pct: float | None, message: str).

    - symbol: ticker to check
    - crash_pct: if the event has already happened today, pass the observed
      crash percentage so the crash bar is excluded from the pre-event window.
    - prior_close: optional override for the pre-event anchor price.
    - max_drawdown_pct: abort threshold (positive number, e.g. 20.0 for 20%).
    - peak_window: lookback in trading days for the peak.
    - event_date: ISO date of the catalyst (e.g. PDUFA day). When provided
      and today < event_date, runs in PRE-EVENT mode: never excludes the most
      recent bar (it's pre-event price action, possibly leak — must count toward
      contamination), and ignores prior_close (it would be yesterday-vs-day-
      before-yesterday, meaningless before the event). Without event_date, the
      legacy post-event heuristic applies. Bug found 2026-04-09: REPL pre-event
      leak was being silently excluded as if it were the post-event crash bar.
    """
    try:
        from tools.yfinance_utils import safe_download
    except ImportError:
        return True, None, "cannot import yfinance_utils — allowing"
    try:
        from datetime import date, timedelta
        end = date.today() + timedelta(days=1)
        start = end - timedelta(days=max(peak_window * 2, 60))
        df = safe_download(symbol, start=start.isoformat(), end=end.isoformat())
        if df is None or len(df) < 12:
            return True, None, f"insufficient history for {symbol} (<12 bars)"
        closes = df['Close'].dropna().values.flatten()
        if len(closes) < 12:
            return True, None, f"insufficient lookback for {symbol}"
        # Determine pre vs post-event mode
        is_post_event = True
        if event_date:
            try:
                from datetime import date as _date
                is_post_event = _date.today() >= _date.fromisoformat(event_date)
            except Exception:
                pass
        if is_post_event and crash_pct is not None and abs(crash_pct) > 10 and len(closes) >= 2:
            pre_crash = closes[:-1]
        else:
            pre_crash = closes
        if not is_post_event:
            anchor = float(pre_crash[-1])
        else:
            anchor = float(prior_close) if prior_close is not None else float(pre_crash[-1])
        peak = float(max(pre_crash[-peak_window:]))
        drawdown_pct = (anchor / peak - 1.0) * 100.0
        mode = "post-event" if is_post_event else "pre-event"
        if drawdown_pct < -max_drawdown_pct:
            return False, drawdown_pct, (
                f"PRE-EVENT CONTAMINATION ({mode} check): {symbol} anchor "
                f"${anchor:.2f} is {drawdown_pct:+.1f}% below its {peak_window}d "
                f"peak (${peak:.2f}). Signal is already priced in — expected "
                f"post-event abnormal drop will be much smaller than backtest, "
                f"while short-squeeze risk remains. Do NOT short a pre-leaked event."
            )
        return True, drawdown_pct, (
            f"{peak_window}d drawdown from peak ({mode}): {drawdown_pct:+.1f}% "
            f"(peak ${peak:.2f} -> anchor ${anchor:.2f}) — clean"
        )
    except Exception as e:
        return True, None, f"pre-event contamination check error: {e} — allowing"


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 -m tools.pre_event_contamination SYMBOL [crash_pct]")
        sys.exit(2)
    sym = sys.argv[1]
    cp = float(sys.argv[2]) if len(sys.argv) > 2 else None
    ok, dd, msg = check_pre_event_contamination(sym, crash_pct=cp)
    print(f"{sym}: ok={ok} drawdown_pct={dd} — {msg}")
