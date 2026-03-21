"""
SEC Form 4 Insider Buying Cluster Detector

Downloads SEC quarterly bulk data and identifies events where 2+ corporate
insiders made open-market purchases (>$50K each) within 30 days of each other.

Data source: https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/
"""

import io
import os
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

USER_AGENT = os.environ.get("SEC_USER_AGENT", "Financial Research Bot contact@example.com")
BASE_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sec_form4_cache")


def download_quarter(year: int, quarter: int, cache: bool = True) -> dict:
    """Download and parse one quarter of Form 4 data from SEC.

    Returns dict with DataFrames: submissions, nonderiv_trans, reporting_owners
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"{year}q{quarter}_form345.pkl")

    if cache and os.path.exists(cache_path):
        import pickle
        with open(cache_path, "rb") as f:
            return pickle.load(f)

    url = f"{BASE_URL}/{year}q{quarter}_form345.zip"
    print(f"  Downloading {year}Q{quarter}...")

    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=60)

    if resp.status_code == 404:
        print(f"  {year}Q{quarter} not found (404) — skipping")
        return None

    resp.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(resp.content))

    # Show available files for debugging
    available = zf.namelist()
    print(f"  ZIP contents: {available}")

    # Read the three key files — try multiple naming conventions
    result = {}

    # SUBMISSION file
    sub_file = None
    for name in ["SUBMISSION.tsv", "submission.tsv", "SUBMISSIONS.tsv"]:
        if name in available:
            sub_file = name
            break
    if sub_file is None:
        # Try partial match
        for name in available:
            if "SUBMISSION" in name.upper() and name.endswith(".tsv"):
                sub_file = name
                break

    # NONDERIV_TRANS file
    nd_file = None
    for name in ["NONDERIV_TRANS.tsv", "nonderiv_trans.tsv", "NONDERIVTRANS.tsv"]:
        if name in available:
            nd_file = name
            break
    if nd_file is None:
        for name in available:
            if "NONDERIV" in name.upper() and name.endswith(".tsv"):
                nd_file = name
                break

    # REPORTINGOWNER file
    ro_file = None
    for name in ["REPORTINGOWNER.tsv", "reportingowner.tsv", "REPORTING_OWNER.tsv"]:
        if name in available:
            ro_file = name
            break
    if ro_file is None:
        for name in available:
            if "OWNER" in name.upper() and name.endswith(".tsv"):
                ro_file = name
                break

    if not sub_file:
        print(f"  ERROR: No submission file found. Available: {available}")
        return None
    if not nd_file:
        print(f"  ERROR: No nonderiv_trans file found. Available: {available}")
        return None

    try:
        with zf.open(sub_file) as f:
            result["submissions"] = pd.read_csv(f, sep="\t", low_memory=False,
                                                 dtype=str, on_bad_lines="skip")
        print(f"  submissions columns: {list(result['submissions'].columns)}")

        with zf.open(nd_file) as f:
            result["nonderiv_trans"] = pd.read_csv(f, sep="\t", low_memory=False,
                                                    dtype=str, on_bad_lines="skip")
        print(f"  nonderiv_trans columns: {list(result['nonderiv_trans'].columns)}")

        if ro_file:
            with zf.open(ro_file) as f:
                result["reporting_owners"] = pd.read_csv(f, sep="\t", low_memory=False,
                                                          dtype=str, on_bad_lines="skip")
            print(f"  reporting_owners columns: {list(result['reporting_owners'].columns)}")
        else:
            print(f"  WARNING: No reporting owners file found")
            result["reporting_owners"] = pd.DataFrame()

    except Exception as e:
        print(f"  ERROR reading ZIP: {e}")
        return None

    if cache:
        import pickle
        with open(cache_path, "wb") as f:
            pickle.dump(result, f)

    time.sleep(0.15)  # SEC rate limit: max 10 req/sec
    return result


def identify_cluster_events(
    year_start: int = 2018,
    year_end: int = 2024,
    min_purchase_value: float = 50_000,
    cluster_window_days: int = 30,
    min_insiders_in_cluster: int = 2,
) -> pd.DataFrame:
    """
    Identify all insider buying cluster events in the given date range.

    A cluster event is when min_insiders_in_cluster different insiders each
    made open-market purchases >= min_purchase_value within cluster_window_days
    of each other.

    Returns DataFrame with columns:
        ticker, cluster_date (date 2nd+ insider filed),
        n_insiders, total_value, window_start, window_end
    """
    all_purchases = []

    for year in range(year_start, year_end + 1):
        for quarter in range(1, 5):
            # Skip future quarters
            quarter_end_month = quarter * 3
            if year == datetime.now().year and quarter_end_month > datetime.now().month:
                continue

            data = download_quarter(year, quarter)
            if data is None:
                continue

            submissions = data["submissions"]
            nonderiv = data["nonderiv_trans"]
            owners = data.get("reporting_owners", pd.DataFrame())

            # Standardize column names (lowercase, strip whitespace)
            submissions.columns = submissions.columns.str.lower().str.strip()
            nonderiv.columns = nonderiv.columns.str.lower().str.strip()
            if not owners.empty:
                owners.columns = owners.columns.str.lower().str.strip()

            print(f"\n  {year}Q{quarter} — raw sizes: sub={len(submissions)}, nd={len(nonderiv)}, owners={len(owners)}")

            # --- Find transaction code column ---
            purchase_col = None
            for col in ["trans_code", "transactioncode", "transaction_code", "transcode"]:
                if col in nonderiv.columns:
                    purchase_col = col
                    break

            if purchase_col is None:
                print(f"  WARNING: No transaction code column found. Columns: {list(nonderiv.columns[:20])}")
                continue

            # Filter to open-market purchases (transaction code P)
            purchases = nonderiv[nonderiv[purchase_col] == "P"].copy()
            print(f"  Open-market purchases (code=P): {len(purchases)}")

            if len(purchases) == 0:
                print(f"  Unique trans codes: {nonderiv[purchase_col].value_counts().head(10).to_dict()}")
                continue

            # --- Find shares column ---
            shares_col = None
            for col in ["trans_shares", "transactionshares", "sharesowned", "shares"]:
                if col in purchases.columns:
                    shares_col = col
                    break
            if shares_col is None:
                for col in purchases.columns:
                    if "share" in col.lower() and "trans" in col.lower():
                        shares_col = col
                        break

            # --- Find price column ---
            price_col = None
            for col in ["trans_pricepershare", "transactionpricepershare", "pricepershare", "price"]:
                if col in purchases.columns:
                    price_col = col
                    break
            if price_col is None:
                for col in purchases.columns:
                    if "price" in col.lower():
                        price_col = col
                        break

            if shares_col is None or price_col is None:
                print(f"  WARNING: Cannot find shares ({shares_col}) or price ({price_col}) columns.")
                print(f"  Available cols: {list(purchases.columns)}")
                continue

            purchases["_shares"] = pd.to_numeric(purchases[shares_col], errors="coerce")
            purchases["_price"] = pd.to_numeric(purchases[price_col], errors="coerce")
            purchases["_value"] = purchases["_shares"] * purchases["_price"]

            # Filter by minimum purchase value
            before_filter = len(purchases)
            purchases = purchases[purchases["_value"] >= min_purchase_value].copy()
            print(f"  After value filter (>=${min_purchase_value:,.0f}): {len(purchases)} (dropped {before_filter - len(purchases)})")

            if len(purchases) == 0:
                continue

            # --- Find accession number column ---
            acc_col = None
            for col in ["accession_number", "accession_num", "accessionnumber"]:
                if col in purchases.columns:
                    acc_col = col
                    break

            if acc_col is None:
                print(f"  WARNING: No accession number column in nonderiv. Cols: {list(purchases.columns)}")
                continue

            # --- Find accession number in submissions ---
            sub_acc_col = None
            for col in ["accession_number", "accession_num", "accessionnumber"]:
                if col in submissions.columns:
                    sub_acc_col = col
                    break

            # --- Find ticker column in submissions ---
            ticker_col = None
            for col in ["issuertradingsymbol", "ticker", "issuer_ticker", "issuerticker"]:
                if col in submissions.columns:
                    ticker_col = col
                    break

            # --- Find filing date column ---
            date_col = None
            for col in ["periodofreport", "period_of_report", "filingdate", "filing_date", "datefiled"]:
                if col in submissions.columns:
                    date_col = col
                    break

            if sub_acc_col is None or ticker_col is None or date_col is None:
                print(f"  WARNING: Missing key columns in submissions. acc={sub_acc_col}, ticker={ticker_col}, date={date_col}")
                print(f"  Submission cols: {list(submissions.columns)}")
                continue

            sub_merge = submissions[[sub_acc_col, ticker_col, date_col]].copy()
            sub_merge.columns = ["accession_num", "ticker", "filing_date"]

            purchases = purchases.rename(columns={acc_col: "accession_num"})
            merged = purchases.merge(sub_merge, on="accession_num", how="left")
            print(f"  After merge with submissions: {len(merged)} rows, {merged['ticker'].notna().sum()} with ticker")

            # --- Join with reporting owners for reporter ID ---
            if not owners.empty:
                # Find accession in owners
                own_acc_col = None
                for col in ["accession_number", "accession_num", "accessionnumber"]:
                    if col in owners.columns:
                        own_acc_col = col
                        break

                # Find reporter identifier (CIK preferred, name as fallback)
                reporter_col = None
                for col in ["rptownercik", "reportingownercik", "owner_cik", "ownercik"]:
                    if col in owners.columns:
                        reporter_col = col
                        break
                if reporter_col is None:
                    for col in ["rptownername", "reportingownername", "owner_name"]:
                        if col in owners.columns:
                            reporter_col = col
                            break

                if own_acc_col and reporter_col:
                    if own_acc_col != "accession_num":
                        owners = owners.rename(columns={own_acc_col: "accession_num"})
                    owner_sub = owners[["accession_num", reporter_col]].copy()
                    owner_sub.columns = ["accession_num", "reporter_id"]
                    merged = merged.merge(owner_sub, on="accession_num", how="left")
                else:
                    print(f"  WARNING: Could not find reporter ID in owners. Using accession_num as proxy.")
                    merged["reporter_id"] = merged["accession_num"]
            else:
                merged["reporter_id"] = merged["accession_num"]

            # --- Parse filing date and filter ---
            merged["filing_date"] = pd.to_datetime(merged["filing_date"], errors="coerce")
            merged = merged.dropna(subset=["filing_date"])

            # Filter to valid tickers (1-5 chars, non-null)
            merged = merged.dropna(subset=["ticker"])
            merged["ticker"] = merged["ticker"].str.strip().str.upper()
            merged = merged[merged["ticker"].str.len().between(1, 5)]
            merged = merged[~merged["ticker"].str.contains(r'[^A-Z\.\-]', na=True)]

            print(f"  Final qualifying purchases: {len(merged)}")

            if len(merged) > 0:
                all_purchases.append(
                    merged[["ticker", "filing_date", "reporter_id", "_value"]].copy()
                )

    if not all_purchases:
        print("\nNo purchase data collected!")
        return pd.DataFrame()

    all_df = pd.concat(all_purchases, ignore_index=True)
    all_df = all_df.sort_values(["ticker", "filing_date"])

    print(f"\n{'='*60}")
    print(f"Total qualifying purchases: {len(all_df)}")
    print(f"Unique tickers: {all_df['ticker'].nunique()}")
    print(f"Date range: {all_df['filing_date'].min().date()} to {all_df['filing_date'].max().date()}")

    # --- Identify cluster events ---
    cluster_events = []

    for ticker, group in all_df.groupby("ticker"):
        group = group.sort_values("filing_date").reset_index(drop=True)
        dates = group["filing_date"].tolist()
        reporters = group["reporter_id"].fillna("unknown").tolist()
        values = group["_value"].tolist()

        # Get unique sorted dates to avoid processing the same date multiple times
        unique_dates = sorted(set(dates))
        seen_cluster_dates = set()  # Track dates where a cluster was already recorded

        for trigger_date in unique_dates:
            window_start = trigger_date - timedelta(days=cluster_window_days)
            window_end = trigger_date

            # All purchases in the window [window_start, window_end]
            window_reporters = set()
            window_total = 0.0
            window_dates = []

            for j in range(len(dates)):
                if window_start <= dates[j] <= window_end:
                    window_reporters.add(reporters[j])
                    window_total += values[j] if not pd.isna(values[j]) else 0
                    window_dates.append(dates[j])

            if len(window_reporters) < min_insiders_in_cluster:
                continue

            # Only record when the cluster threshold was FIRST crossed.
            # Check if threshold was already met strictly before trigger_date.
            prev_reporters = set()
            for j in range(len(dates)):
                if window_start <= dates[j] < trigger_date:
                    prev_reporters.add(reporters[j])
            if len(prev_reporters) >= min_insiders_in_cluster:
                # Cluster was already active before this date — skip
                continue

            # Guard against duplicate entries for the same (ticker, date)
            if trigger_date.date() in seen_cluster_dates:
                continue
            seen_cluster_dates.add(trigger_date.date())

            cluster_events.append({
                "ticker": ticker,
                "cluster_date": trigger_date.date(),
                "n_insiders": len(window_reporters),
                "total_value": round(window_total, 2),
                "window_start": min(window_dates).date(),
                "window_end": max(window_dates).date(),
            })

    cluster_df = pd.DataFrame(cluster_events)

    if len(cluster_df) > 0:
        cluster_df = cluster_df.sort_values("cluster_date").reset_index(drop=True)
        print(f"\nCluster events identified: {len(cluster_df)}")
        print(f"Unique tickers with clusters: {cluster_df['ticker'].nunique()}")
        print(f"Cluster date range: {cluster_df['cluster_date'].min()} to {cluster_df['cluster_date'].max()}")
        print(f"\nSample events (first 10):")
        print(cluster_df.head(10).to_string())
    else:
        print("\nNo cluster events found.")

    return cluster_df


if __name__ == "__main__":
    print("=== Insider Cluster Detector ===")
    print("Scanning 2020-2022 for discovery dataset...\n")
    events = identify_cluster_events(year_start=2020, year_end=2022)

    if len(events) > 0:
        output_path = os.path.join(os.path.dirname(__file__), "..", "data", "insider_cluster_events.csv")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        events.to_csv(output_path, index=False)
        print(f"\nSaved {len(events)} events to {output_path}")
