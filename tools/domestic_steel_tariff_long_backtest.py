"""
domestic_steel_tariff_long_backtest.py

Backtest: Do STLD and NUE show positive abnormal returns (vs SPY) in the
10-30 days AFTER major US tariff announcements?

Hypothesis: Domestic steel producers (STLD, NUE) benefit from import tariffs
because tariffs reduce foreign competition, allowing domestic producers to
raise prices and volumes. After a tariff announcement, the market should
price in improved earnings, leading to positive abnormal returns.

Key dates tested:
  2018-03-01  Trump steel/aluminum tariff ANNOUNCEMENT
  2018-03-08  Tariff signed into law (Section 232)
  2018-07-06  $34B China tariffs take effect (broader trade war escalation)
  2019-05-05  Trump tweet raises China tariffs 10%->25%
  2019-08-01  Trump announces 10% tariff on remaining $300B China goods
  2025-04-02  Liberation Day — sweeping universal tariffs

NOTE: Small sample (n=6 events). Statistical conclusions must be treated
with extreme caution. p-values are approximate and assume independence
(which is violated — 2018 events are correlated). This is signal discovery,
not confirmation.
"""

import sys
import os
import numpy as np
import pandas as pd
from scipy import stats

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.yfinance_utils import get_close_prices

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TICKERS = ["STLD", "NUE", "SPY"]

# All prices pulled from this range (covers all events + hold windows)
DATA_START = "2017-09-01"
DATA_END   = "2025-05-15"   # covers Liberation Day + 30 trading days

# Tariff announcement event dates
EVENTS = [
    ("2018-03-01", "Steel/Alum tariff announcement"),
    ("2018-03-08", "Section 232 signed into law"),
    ("2018-07-06", "China $34B tariffs take effect"),
    ("2019-05-05", "Trump raises China tariffs 10->25%"),
    ("2019-08-01", "Trump announces $300B China tariffs"),
    ("2025-04-02", "Liberation Day universal tariffs"),
]

HORIZONS = [5, 10, 20, 30]   # calendar trading days forward

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_next_trading_day(prices: pd.DataFrame, date_str: str) -> pd.Timestamp | None:
    """Return the first available index date >= date_str."""
    target = pd.Timestamp(date_str)
    idx = prices.index[prices.index >= target]
    return idx[0] if len(idx) > 0 else None


def calc_return(prices: pd.Series, t0: pd.Timestamp, n_days: int) -> float | None:
    """
    Calculate the n_days forward return starting from t0.
    Entry = close price ON t0. Exit = close price n_days later (trading days).
    Returns None if not enough data.
    """
    idx_pos = prices.index.get_loc(t0)
    exit_pos = idx_pos + n_days
    if exit_pos >= len(prices):
        return None
    entry = prices.iloc[idx_pos]
    exit_ = prices.iloc[exit_pos]
    return (exit_ - entry) / entry


# ---------------------------------------------------------------------------
# Main backtest
# ---------------------------------------------------------------------------

def run_backtest():
    print("=" * 65)
    print("DOMESTIC STEEL TARIFF LONG BACKTEST")
    print("Hypothesis: STLD/NUE outperform SPY after US tariff announcements")
    print("=" * 65)

    # Download prices
    print(f"\nDownloading prices {DATA_START} to {DATA_END}...")
    prices = get_close_prices(TICKERS, start=DATA_START, end=DATA_END)
    print(f"Got {len(prices)} trading days, columns: {list(prices.columns)}")
    print(f"Date range: {prices.index[0].date()} to {prices.index[-1].date()}")

    # Build per-event results table
    rows = []
    for date_str, label in EVENTS:
        t0 = get_next_trading_day(prices, date_str)
        if t0 is None:
            print(f"\nWARNING: No trading day found for {date_str} — skipping")
            continue

        actual_date = t0.strftime("%Y-%m-%d")
        note = f" (next trading day: {actual_date})" if actual_date != date_str else ""

        for horizon in HORIZONS:
            stld_ret = calc_return(prices["STLD"], t0, horizon)
            nue_ret  = calc_return(prices["NUE"],  t0, horizon)
            spy_ret  = calc_return(prices["SPY"],  t0, horizon)

            if stld_ret is None or spy_ret is None:
                continue

            stld_abnormal = stld_ret - spy_ret
            nue_abnormal  = nue_ret - spy_ret if nue_ret is not None else None

            rows.append({
                "event_date":   date_str,
                "actual_t0":    actual_date,
                "label":        label,
                "horizon":      horizon,
                "STLD_raw":     stld_ret,
                "NUE_raw":      nue_ret,
                "SPY_raw":      spy_ret,
                "STLD_abnormal": stld_abnormal,
                "NUE_abnormal":  nue_abnormal,
            })

    df = pd.DataFrame(rows)

    # ---------------------------------------------------------------------------
    # Print event-by-event detail
    # ---------------------------------------------------------------------------
    print("\n" + "-" * 65)
    print("EVENT-BY-EVENT DETAIL")
    print("-" * 65)

    for date_str, label in EVENTS:
        subset = df[df["event_date"] == date_str]
        if subset.empty:
            print(f"\n{date_str} {label}: NO DATA")
            continue
        print(f"\n{date_str} — {label}")
        for _, row in subset.iterrows():
            h = int(row["horizon"])
            stld_a = row["STLD_abnormal"] * 100
            nue_a  = row["NUE_abnormal"]  * 100 if row["NUE_abnormal"] is not None else float("nan")
            spy_r  = row["SPY_raw"] * 100
            stld_r = row["STLD_raw"] * 100
            nue_r  = row["NUE_raw"]  * 100 if row["NUE_raw"] is not None else float("nan")
            print(
                f"  {h:>2}d: "
                f"STLD raw={stld_r:+6.2f}%  NUE raw={nue_r:+6.2f}%  SPY raw={spy_r:+6.2f}%  |  "
                f"STLD abn={stld_a:+6.2f}%  NUE abn={nue_a:+6.2f}%"
            )

    # ---------------------------------------------------------------------------
    # Aggregate by horizon
    # ---------------------------------------------------------------------------
    print("\n" + "-" * 65)
    print("AGGREGATE RESULTS BY HORIZON (all events)")
    print("-" * 65)
    print(f"{'Horizon':>8}  {'n':>3}  {'STLD avg abn':>13}  {'STLD dir%':>10}  {'STLD p-val':>11}  "
          f"{'NUE avg abn':>12}  {'NUE dir%':>9}  {'NUE p-val':>10}")
    print("-" * 95)

    summary_rows = []
    for h in HORIZONS:
        sub = df[df["horizon"] == h].dropna(subset=["STLD_abnormal"])
        n = len(sub)
        if n == 0:
            continue

        stld_vals = sub["STLD_abnormal"].values
        nue_vals  = sub["NUE_abnormal"].dropna().values

        stld_mean = np.mean(stld_vals) * 100
        stld_dir  = np.mean(stld_vals > 0.005) * 100   # >0.5% threshold
        nue_mean  = np.mean(nue_vals) * 100 if len(nue_vals) > 0 else float("nan")
        nue_dir   = np.mean(nue_vals  > 0.005) * 100 if len(nue_vals) > 0 else float("nan")

        # One-sample t-test: H0 = abnormal return == 0
        stld_t, stld_p = stats.ttest_1samp(stld_vals, 0) if n >= 3 else (float("nan"), float("nan"))
        nue_t,  nue_p  = stats.ttest_1samp(nue_vals,  0) if len(nue_vals) >= 3 else (float("nan"), float("nan"))

        print(
            f"{h:>7}d  {n:>3}  {stld_mean:>+12.2f}%  {stld_dir:>9.0f}%  "
            f"{stld_p:>11.4f}  "
            f"{nue_mean:>+11.2f}%  {nue_dir:>8.0f}%  {nue_p:>10.4f}"
        )

        summary_rows.append({
            "horizon": h, "n": n,
            "STLD_mean_abn_pct": stld_mean, "STLD_dir_pct": stld_dir, "STLD_p": stld_p,
            "NUE_mean_abn_pct":  nue_mean,  "NUE_dir_pct":  nue_dir,  "NUE_p":  nue_p,
        })

    # ---------------------------------------------------------------------------
    # STLD vs NUE correlation check
    # ---------------------------------------------------------------------------
    print("\n" + "-" * 65)
    print("STLD vs NUE CORRELATION (abnormal returns, all events/horizons)")
    print("-" * 65)
    merged = df.dropna(subset=["STLD_abnormal", "NUE_abnormal"])
    if len(merged) >= 4:
        corr, corr_p = stats.pearsonr(merged["STLD_abnormal"], merged["NUE_abnormal"])
        print(f"  Pearson r = {corr:.3f}  (p={corr_p:.4f}, n={len(merged)})")
        print(f"  Interpretation: {'High correlation — moves together' if abs(corr) > 0.7 else 'Moderate/low correlation'}")
    else:
        print("  Insufficient data for correlation.")

    # ---------------------------------------------------------------------------
    # Bias notes and caveats
    # ---------------------------------------------------------------------------
    print("\n" + "-" * 65)
    print("BIAS NOTES AND CAVEATS")
    print("-" * 65)
    print("""
  1. SMALL SAMPLE: n=6 events. Any p-value is unreliable. You need at least
     20-30 independent events for a reliable t-test. Treat direction% and
     magnitude as exploratory, not confirmatory.

  2. INDEPENDENCE VIOLATION: The 2018-03-01 and 2018-03-08 events are 5 trading
     days apart. They share the same price history. The 5d window for the first
     event overlaps the second event entirely. These are NOT independent observations.
     Effective independent events: ~4 (not 6).

  3. SELECTION BIAS: These 6 dates were selected because they are 'major'
     tariff events. Minor tariff news (extensions, exemptions) not included.
     We may be cherry-picking dates where tariffs had maximal impact.

  4. LIBERATION DAY (2025-04-02): Only one full data point available. This is
     a single event with potentially regime-changing magnitude — it may not
     generalize to future tariff shocks of normal scale.

  5. SURVIVORSHIP BIAS: STLD and NUE are surviving steel companies. Other
     domestic steel producers may have performed differently or been acquired.

  6. BENCHMARK: SPY is a broad-market benchmark. Steel stocks have higher beta
     than SPY. A portion of outperformance is structural (higher beta) not
     tariff-specific. Ideally benchmark vs XME (metals ETF) or sector peers.

  7. 2025 DATA TRUNCATION: Data ends {DATA_END}. The 30-day window for
     Liberation Day (2025-04-02) may be incomplete depending on actual
     trading-day availability.
    """)

    # ---------------------------------------------------------------------------
    # Verdict
    # ---------------------------------------------------------------------------
    print("-" * 65)
    print("PRELIMINARY VERDICT")
    print("-" * 65)
    s = {r["horizon"]: r for r in summary_rows}
    if summary_rows:
        h20 = s.get(20)
        h10 = s.get(10)
        if h20:
            mean_str = f"{h20['STLD_mean_abn_pct']:+.2f}%"
            dir_str  = f"{h20['STLD_dir_pct']:.0f}%"
            p_str    = f"{h20['STLD_p']:.4f}"
            print(f"\n  STLD at 20d: avg abnormal={mean_str}, direction={dir_str}, p={p_str}")
        if h10:
            mean_str = f"{h10['STLD_mean_abn_pct']:+.2f}%"
            dir_str  = f"{h10['STLD_dir_pct']:.0f}%"
            p_str    = f"{h10['STLD_p']:.4f}"
            print(f"  STLD at 10d: avg abnormal={mean_str}, direction={dir_str}, p={p_str}")

    print("""
  SIGNAL VALIDITY: Cannot be statistically confirmed at n=6.
  DIRECTION: Check the numbers above. If avg abnormal > +2% and direction
             >= 67% at 20d, worth registering as hypothesis for deeper testing.
  NEXT STEP: Expand to ALL Section 232 / Section 301 tariff events 2018-2025
             to reach n>=15 before drawing firm conclusions.
    """)

    return df


if __name__ == "__main__":
    result_df = run_backtest()
    print("\nRaw data saved to result_df variable.")
    print("Done.")
