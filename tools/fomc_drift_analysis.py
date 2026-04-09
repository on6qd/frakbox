"""
FOMC Pre-Announcement Drift Analysis
=====================================
Tests the Lucca & Moench (2015) hypothesis: SPY shows positive abnormal
returns on FOMC decision days (and the day before).

Hypothesis class: calendar
Signal: fomc_pre_announcement_drift

Design:
  - Discovery: 2010-2019 (80 meetings)
  - Validation: 2020-2024 (42 meetings)
  - Holdout: 2025-2026 (live)

Measurements:
  1. Day-0 return: prior close → decision-day close (captures pre-announcement drift + reaction)
  2. Day-(-1) return: 2-days-before close → prior-day close (pure anticipation)
  3. 2-day window: 2-days-before close → decision-day close

Success criteria (set BEFORE testing):
  - Mean abnormal return > 0.15% per event (day-0 or 2-day window)
  - p < 0.05 (two-sided t-test)
  - Direction > 55% positive
  - Same sign and direction > 50% in OOS period
"""

import sys, json
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.yfinance_utils import safe_download
import numpy as np
from scipy import stats

# FOMC decision dates (statement day, typically 2pm ET announcement)
# Sources: federalreserve.gov/monetarypolicy/fomccalendars.htm
FOMC_DATES = {
    2010: ["2010-01-27", "2010-03-16", "2010-04-28", "2010-06-23", "2010-08-10", "2010-09-21", "2010-11-03", "2010-12-14"],
    2011: ["2011-01-26", "2011-03-15", "2011-04-27", "2011-06-22", "2011-08-09", "2011-09-21", "2011-11-02", "2011-12-13"],
    2012: ["2012-01-25", "2012-03-13", "2012-04-25", "2012-06-20", "2012-08-01", "2012-09-13", "2012-10-24", "2012-12-12"],
    2013: ["2013-01-30", "2013-03-20", "2013-05-01", "2013-06-19", "2013-07-31", "2013-09-18", "2013-10-30", "2013-12-18"],
    2014: ["2014-01-29", "2014-03-19", "2014-04-30", "2014-06-18", "2014-07-30", "2014-09-17", "2014-10-29", "2014-12-17"],
    2015: ["2015-01-28", "2015-03-18", "2015-04-29", "2015-06-17", "2015-07-29", "2015-09-17", "2015-10-28", "2015-12-16"],
    2016: ["2016-01-27", "2016-03-16", "2016-04-27", "2016-06-15", "2016-07-27", "2016-09-21", "2016-11-02", "2016-12-14"],
    2017: ["2017-02-01", "2017-03-15", "2017-05-03", "2017-06-14", "2017-07-26", "2017-09-20", "2017-11-01", "2017-12-13"],
    2018: ["2018-01-31", "2018-03-21", "2018-05-02", "2018-06-13", "2018-08-01", "2018-09-26", "2018-11-08", "2018-12-19"],
    2019: ["2019-01-30", "2019-03-20", "2019-05-01", "2019-06-19", "2019-07-31", "2019-09-18", "2019-10-30", "2019-12-11"],
    2020: ["2020-01-29", "2020-03-03", "2020-03-15", "2020-04-29", "2020-06-10", "2020-07-29", "2020-09-16", "2020-11-05", "2020-12-16"],
    2021: ["2021-01-27", "2021-03-17", "2021-04-28", "2021-06-16", "2021-07-28", "2021-09-22", "2021-11-03", "2021-12-15"],
    2022: ["2022-01-26", "2022-03-16", "2022-05-04", "2022-06-15", "2022-07-27", "2022-09-21", "2022-11-02", "2022-12-14"],
    2023: ["2023-02-01", "2023-03-22", "2023-05-03", "2023-06-14", "2023-07-26", "2023-09-20", "2023-11-01", "2023-12-13"],
    2024: ["2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12", "2024-07-31", "2024-09-18", "2024-11-07", "2024-12-18"],
    2025: ["2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18", "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10"],
    2026: ["2026-01-28", "2026-03-18"],  # Only past meetings
}

# Emergency meetings to exclude (unusual, not representative of normal FOMC cycle)
EMERGENCY_DATES = {"2020-03-03", "2020-03-15"}


def get_trading_day_before(date_str, prices_df):
    """Get the last trading day before the given date."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    for i in range(1, 10):
        prev = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
        if prev in prices_df.index.strftime("%Y-%m-%d").tolist():
            return prev
    return None


def get_trading_day_n_before(date_str, prices_df, n=2):
    """Get the nth trading day before the given date."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    count = 0
    for i in range(1, 20):
        prev = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
        if prev in prices_df.index.strftime("%Y-%m-%d").tolist():
            count += 1
            if count == n:
                return prev
    return None


def run_analysis(discovery_end=2019, oos_start=2020, oos_end=2024):
    """Run FOMC drift analysis with temporal split."""

    # Compile all non-emergency FOMC dates
    all_dates = []
    for year, dates in sorted(FOMC_DATES.items()):
        for d in dates:
            if d not in EMERGENCY_DATES:
                all_dates.append(d)

    # Split into discovery and OOS
    discovery_dates = [d for d in all_dates if int(d[:4]) <= discovery_end]
    oos_dates = [d for d in all_dates if oos_start <= int(d[:4]) <= oos_end]
    holdout_dates = [d for d in all_dates if int(d[:4]) > oos_end]

    print(f"FOMC dates: {len(all_dates)} total, {len(discovery_dates)} discovery (2010-{discovery_end}), "
          f"{len(oos_dates)} OOS ({oos_start}-{oos_end}), {len(holdout_dates)} holdout")

    # Fetch SPY data
    print("\nFetching SPY data...")
    spy = safe_download("SPY", start="2009-12-01", end="2026-04-09")
    if spy is None or len(spy) == 0:
        print("ERROR: Could not fetch SPY data")
        return None

    # Ensure index is datetime
    spy.index = spy.index.tz_localize(None) if spy.index.tz else spy.index
    date_strs = spy.index.strftime("%Y-%m-%d").tolist()

    # Compute daily returns
    spy["daily_ret"] = spy["Close"].pct_change()
    avg_daily_ret = spy["daily_ret"].mean()

    print(f"SPY data: {len(spy)} trading days, avg daily return: {avg_daily_ret*100:.4f}%")

    results = {"discovery": [], "oos": [], "holdout": []}

    for date_set, label in [(discovery_dates, "discovery"), (oos_dates, "oos"), (holdout_dates, "holdout")]:
        for fomc_date in date_set:
            if fomc_date not in date_strs:
                # FOMC date might be a holiday/weekend - skip
                continue

            day_before = get_trading_day_before(fomc_date, spy)
            two_days_before = get_trading_day_n_before(fomc_date, spy, 2)

            if not day_before or not two_days_before:
                continue

            close_fomc = spy.loc[fomc_date, "Close"] if fomc_date in date_strs else None
            close_day_before = spy.loc[day_before, "Close"] if day_before in date_strs else None
            close_2d_before = spy.loc[two_days_before, "Close"] if two_days_before in date_strs else None

            if close_fomc is None or close_day_before is None or close_2d_before is None:
                continue

            # Extract scalar values
            close_fomc = float(close_fomc.iloc[0]) if hasattr(close_fomc, 'iloc') else float(close_fomc)
            close_day_before = float(close_day_before.iloc[0]) if hasattr(close_day_before, 'iloc') else float(close_day_before)
            close_2d_before = float(close_2d_before.iloc[0]) if hasattr(close_2d_before, 'iloc') else float(close_2d_before)

            # Returns
            day0_ret = (close_fomc / close_day_before) - 1  # decision day return
            day_minus1_ret = (close_day_before / close_2d_before) - 1  # day before decision
            two_day_ret = (close_fomc / close_2d_before) - 1  # 2-day window

            # Abnormal returns (subtract average daily return)
            day0_abnormal = day0_ret - avg_daily_ret
            day_minus1_abnormal = day_minus1_ret - avg_daily_ret
            two_day_abnormal = two_day_ret - (2 * avg_daily_ret)  # 2 days of drift

            results[label].append({
                "date": fomc_date,
                "day0_ret": day0_ret,
                "day0_abnormal": day0_abnormal,
                "day_minus1_ret": day_minus1_ret,
                "day_minus1_abnormal": day_minus1_abnormal,
                "two_day_ret": two_day_ret,
                "two_day_abnormal": two_day_abnormal,
            })

    # Print results
    print("\n" + "="*70)
    print("FOMC PRE-ANNOUNCEMENT DRIFT ANALYSIS")
    print("="*70)

    for label in ["discovery", "oos", "holdout"]:
        data = results[label]
        if not data:
            continue

        n = len(data)

        # Day-0 (decision day)
        day0_rets = [d["day0_ret"] for d in data]
        day0_abn = [d["day0_abnormal"] for d in data]
        day0_mean = np.mean(day0_abn) * 100
        day0_dir = sum(1 for r in day0_rets if r > 0) / n * 100
        day0_t, day0_p = stats.ttest_1samp(day0_abn, 0)

        # Day -1 (day before decision)
        dm1_rets = [d["day_minus1_ret"] for d in data]
        dm1_abn = [d["day_minus1_abnormal"] for d in data]
        dm1_mean = np.mean(dm1_abn) * 100
        dm1_dir = sum(1 for r in dm1_rets if r > 0) / n * 100
        dm1_t, dm1_p = stats.ttest_1samp(dm1_abn, 0)

        # 2-day window
        td_rets = [d["two_day_ret"] for d in data]
        td_abn = [d["two_day_abnormal"] for d in data]
        td_mean = np.mean(td_abn) * 100
        td_dir = sum(1 for r in td_rets if r > 0) / n * 100
        td_t, td_p = stats.ttest_1samp(td_abn, 0)

        period = f"{'2010-2019' if label == 'discovery' else '2020-2024' if label == 'oos' else '2025-2026'}"
        print(f"\n--- {label.upper()} ({period}, N={n}) ---")
        print(f"  Day-0 (decision day):  mean_abn={day0_mean:+.3f}%  dir={day0_dir:.0f}%  t={day0_t:.2f}  p={day0_p:.4f}")
        print(f"  Day-(-1) (day before): mean_abn={dm1_mean:+.3f}%  dir={dm1_dir:.0f}%  t={dm1_t:.2f}  p={dm1_p:.4f}")
        print(f"  2-day window:          mean_abn={td_mean:+.3f}%  dir={td_dir:.0f}%  t={td_t:.2f}  p={td_p:.4f}")

    # Summary assessment
    disc = results["discovery"]
    oos = results["oos"]

    if disc:
        disc_day0_abn = [d["day0_abnormal"] for d in disc]
        disc_td_abn = [d["two_day_abnormal"] for d in disc]
        disc_day0_mean = np.mean(disc_day0_abn) * 100
        disc_td_mean = np.mean(disc_td_abn) * 100
        _, disc_day0_p = stats.ttest_1samp(disc_day0_abn, 0)
        _, disc_td_p = stats.ttest_1samp(disc_td_abn, 0)

        disc_day0_dir = sum(1 for d in disc if d["day0_ret"] > 0) / len(disc) * 100
        disc_td_dir = sum(1 for d in disc if d["two_day_ret"] > 0) / len(disc) * 100

    if oos:
        oos_day0_abn = [d["day0_abnormal"] for d in oos]
        oos_td_abn = [d["two_day_abnormal"] for d in oos]
        oos_day0_mean = np.mean(oos_day0_abn) * 100
        oos_td_mean = np.mean(oos_td_abn) * 100
        _, oos_day0_p = stats.ttest_1samp(oos_day0_abn, 0)
        _, oos_td_p = stats.ttest_1samp(oos_td_abn, 0)

        oos_day0_dir = sum(1 for d in oos if d["day0_ret"] > 0) / len(oos) * 100
        oos_td_dir = sum(1 for d in oos if d["two_day_ret"] > 0) / len(oos) * 100

    print("\n" + "="*70)
    print("ASSESSMENT vs PRE-REGISTERED CRITERIA")
    print("="*70)

    criteria = {
        "mean_abnormal_gt_0.15pct": None,
        "p_lt_0.05": None,
        "direction_gt_55pct": None,
        "oos_same_sign": None,
        "oos_direction_gt_50pct": None,
    }

    if disc:
        # Check best window (day-0 or 2-day)
        best_disc = "day0" if abs(disc_day0_mean) > abs(disc_td_mean) else "2day"
        best_mean = disc_day0_mean if best_disc == "day0" else disc_td_mean
        best_p = disc_day0_p if best_disc == "day0" else disc_td_p
        best_dir = disc_day0_dir if best_disc == "day0" else disc_td_dir

        criteria["mean_abnormal_gt_0.15pct"] = best_mean > 0.15
        criteria["p_lt_0.05"] = best_p < 0.05
        criteria["direction_gt_55pct"] = best_dir > 55

        if oos:
            oos_mean = oos_day0_mean if best_disc == "day0" else oos_td_mean
            oos_dir = oos_day0_dir if best_disc == "day0" else oos_td_dir
            criteria["oos_same_sign"] = (best_mean > 0 and oos_mean > 0) or (best_mean < 0 and oos_mean < 0)
            criteria["oos_direction_gt_50pct"] = oos_dir > 50

        print(f"\nBest discovery window: {best_disc}")
        for k, v in criteria.items():
            status = "PASS" if v else "FAIL" if v is not None else "N/A"
            print(f"  {k}: {status}")

        all_pass = all(v for v in criteria.values() if v is not None)
        print(f"\nOVERALL: {'VALIDATED' if all_pass else 'FAILED or INSUFFICIENT'}")

    return {
        "results": {k: len(v) for k, v in results.items()},
        "criteria": criteria,
        "discovery_stats": {
            "day0_mean_abn_pct": round(disc_day0_mean, 4) if disc else None,
            "day0_p": round(disc_day0_p, 4) if disc else None,
            "day0_dir_pct": round(disc_day0_dir, 1) if disc else None,
            "two_day_mean_abn_pct": round(disc_td_mean, 4) if disc else None,
            "two_day_p": round(disc_td_p, 4) if disc else None,
            "two_day_dir_pct": round(disc_td_dir, 1) if disc else None,
        },
        "oos_stats": {
            "day0_mean_abn_pct": round(oos_day0_mean, 4) if oos else None,
            "day0_p": round(oos_day0_p, 4) if oos else None,
            "day0_dir_pct": round(oos_day0_dir, 1) if oos else None,
            "two_day_mean_abn_pct": round(oos_td_mean, 4) if oos else None,
            "two_day_p": round(oos_td_p, 4) if oos else None,
            "two_day_dir_pct": round(oos_td_dir, 1) if oos else None,
        } if oos else None,
    }


if __name__ == "__main__":
    result = run_analysis()
    if result:
        print(f"\n--- JSON SUMMARY ---")
        print(json.dumps(result, indent=2))
