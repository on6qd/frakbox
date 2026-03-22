"""
build_roles_from_bulk.py - Re-run CEO/CFO role classification on cluster_with_returns.csv
using the EDGAR bulk quarterly data.

Problem: clusters_with_roles.csv (18.5% join coverage) was built from a different
event detection run and doesn't match cluster_with_returns.csv events.

Solution: For each event in cluster_with_returns.csv (ticker + cluster_date),
look up Form 4 filings for that ticker in a ±30-day window from the quarterly
bulk data, identify buyers, and classify CEO/CFO presence.

Output: data/clusters_with_roles_full.csv - the full merged dataset.
"""

import sys
import os
import pickle
import glob
import re
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CACHE_DIR = "data/sec_form4_cache"
RETURNS_CSV = "data/cluster_with_returns.csv"
OUTPUT_CSV = "data/clusters_with_roles_full.csv"
WINDOW_DAYS = 45  # days around cluster_date to search for filings

# CEO/CFO detection patterns (case-insensitive)
CEO_PATTERNS = [
    r"chief executive",
    r"\bceo\b",
    r"president and ceo",
    r"president & ceo",
    r"ceo and president",
    r"chairman and ceo",
    r"chairman & ceo",
    r"executive chairman",
]

CFO_PATTERNS = [
    r"chief financial",
    r"\bcfo\b",
    r"evp.{0,5}cfo",
    r"svp.{0,5}cfo",
    r"vp.{0,5}cfo",
]

COO_PATTERNS = [
    r"chief operating",
    r"\bcoo\b",
]


def _is_ceo(title: str) -> bool:
    if not title or pd.isna(title):
        return False
    title_lower = title.lower()
    return any(re.search(p, title_lower) for p in CEO_PATTERNS)


def _is_cfo(title: str) -> bool:
    if not title or pd.isna(title):
        return False
    title_lower = title.lower()
    return any(re.search(p, title_lower) for p in CFO_PATTERNS)


def _is_c_suite(title: str) -> bool:
    if not title or pd.isna(title):
        return False
    title_lower = title.lower()
    return (
        _is_ceo(title)
        or _is_cfo(title)
        or any(re.search(p, title_lower) for p in COO_PATTERNS)
        or "chief" in title_lower
    )


def get_quarter(date_str: str) -> tuple[int, int]:
    """Convert date string to (year, quarter)."""
    dt = pd.Timestamp(date_str)
    return (dt.year, (dt.month - 1) // 3 + 1)


def load_quarter_data(year: int, quarter: int) -> dict | None:
    """Load a quarterly Form 4 cache file."""
    path = os.path.join(CACHE_DIR, f"{year}q{quarter}_form345.pkl")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def build_purchase_index_for_quarter(year: int, quarter: int) -> pd.DataFrame | None:
    """
    Build a flat DataFrame of open-market purchases with reporter titles
    for a given quarter.

    Returns DataFrame with columns:
        [ticker, filing_date, rptowner_name, rptowner_title, trans_shares,
         trans_price, is_ceo, is_cfo, is_c_suite]
    or None if data unavailable.
    """
    data = load_quarter_data(year, quarter)
    if data is None:
        return None

    submissions = data["submissions"]
    nonderiv = data["nonderiv_trans"]
    owners = data["reporting_owners"]

    if submissions.empty or nonderiv.empty or owners.empty:
        return None

    # Filter to purchase transactions (TRANS_CODE == 'P')
    purchases = nonderiv[nonderiv["TRANS_CODE"] == "P"].copy()
    if purchases.empty:
        return None

    # Join submissions → purchases
    sub_cols = ["ACCESSION_NUMBER", "FILING_DATE", "ISSUERTRADINGSYMBOL"]
    merged = purchases.merge(
        submissions[sub_cols],
        on="ACCESSION_NUMBER",
        how="inner",
    )

    # Join → reporting owners
    owner_cols = ["ACCESSION_NUMBER", "RPTOWNERNAME", "RPTOWNER_TITLE"]
    merged = merged.merge(
        owners[owner_cols],
        on="ACCESSION_NUMBER",
        how="left",
    )

    merged["filing_date"] = pd.to_datetime(merged["FILING_DATE"], errors="coerce")
    merged["ticker"] = merged["ISSUERTRADINGSYMBOL"].str.upper().str.strip()
    merged["rptowner_name"] = merged["RPTOWNERNAME"]
    merged["rptowner_title"] = merged["RPTOWNER_TITLE"]

    merged["is_ceo"] = merged["rptowner_title"].apply(_is_ceo)
    merged["is_cfo"] = merged["rptowner_title"].apply(_is_cfo)
    merged["is_c_suite"] = merged["rptowner_title"].apply(_is_c_suite)

    out_cols = [
        "ticker", "filing_date", "ACCESSION_NUMBER",
        "rptowner_name", "rptowner_title",
        "is_ceo", "is_cfo", "is_c_suite",
    ]
    return merged[out_cols].dropna(subset=["ticker", "filing_date"])


def classify_event(
    ticker: str,
    cluster_date: str,
    purchase_index: pd.DataFrame,
) -> dict:
    """
    Given a cluster event (ticker, cluster_date), look up Form 4 purchase
    filings in the purchase_index and classify CEO/CFO presence.

    Returns dict with: has_ceo, has_cfo, has_c_suite, n_title_matched,
                       titles_found, names_found
    """
    event_dt = pd.Timestamp(cluster_date)
    window_start = event_dt - timedelta(days=WINDOW_DAYS)
    window_end = event_dt + timedelta(days=7)  # allow slight lag in filing

    mask = (
        (purchase_index["ticker"] == ticker.upper())
        & (purchase_index["filing_date"] >= window_start)
        & (purchase_index["filing_date"] <= window_end)
    )
    subset = purchase_index[mask]

    if subset.empty:
        return {
            "has_ceo": False,
            "has_cfo": False,
            "has_c_suite": False,
            "n_title_matched": 0,
            "n_with_title": 0,
            "titles_found": None,
            "names_found": None,
        }

    has_ceo = subset["is_ceo"].any()
    has_cfo = subset["is_cfo"].any()
    has_c_suite = subset["is_c_suite"].any()
    n_with_title = subset["rptowner_title"].notna().sum()

    titles = "|".join(subset["rptowner_title"].dropna().unique())
    names = "|".join(subset["rptowner_name"].dropna().unique())

    return {
        "has_ceo": bool(has_ceo),
        "has_cfo": bool(has_cfo),
        "has_c_suite": bool(has_c_suite),
        "n_title_matched": len(subset),
        "n_with_title": int(n_with_title),
        "titles_found": titles if titles else None,
        "names_found": names if names else None,
    }


def main():
    print("=" * 70)
    print("BUILD ROLES FROM BULK DATA")
    print(f"Matching events from {RETURNS_CSV}")
    print(f"Using EDGAR bulk Form 4 data from {CACHE_DIR}")
    print("=" * 70)

    # Load target events
    returns_df = pd.read_csv(RETURNS_CSV)
    returns_df["cluster_date"] = pd.to_datetime(returns_df["cluster_date"])
    print(f"\nTarget events: {len(returns_df)}")
    print(f"Date range: {returns_df['cluster_date'].min()} to {returns_df['cluster_date'].max()}")

    # Determine which quarters to load
    quarters_needed = set()
    for _, row in returns_df.iterrows():
        dt = row["cluster_date"]
        # Add the relevant quarter + adjacent quarters for the window
        for offset_months in [-1, 0, 1, 2]:
            shifted = dt + pd.DateOffset(months=offset_months)
            q = (shifted.year, (shifted.month - 1) // 3 + 1)
            if 2020 <= q[0] <= 2025:
                quarters_needed.add(q)

    print(f"Quarters needed: {len(quarters_needed)}")

    # Load all needed quarters into a single purchase index
    all_purchases = []
    for (year, quarter) in sorted(quarters_needed):
        path = os.path.join(CACHE_DIR, f"{year}q{quarter}_form345.pkl")
        if not os.path.exists(path):
            print(f"  Missing: {year}Q{quarter}")
            continue
        print(f"  Loading {year}Q{quarter}...", end=" ", flush=True)
        df = build_purchase_index_for_quarter(year, quarter)
        if df is not None:
            all_purchases.append(df)
            print(f"{len(df):,} purchases")
        else:
            print("empty")

    if not all_purchases:
        print("ERROR: No purchase data loaded")
        return

    purchase_index = pd.concat(all_purchases, ignore_index=True)
    purchase_index = purchase_index.drop_duplicates(subset=["ticker", "filing_date", "ACCESSION_NUMBER"])
    print(f"\nTotal purchase records: {len(purchase_index):,}")
    print(f"Title coverage: {purchase_index['rptowner_title'].notna().mean():.1%}")
    print(f"CEO found: {purchase_index['is_ceo'].sum():,}")
    print(f"CFO found: {purchase_index['is_cfo'].sum():,}")

    # Classify each event
    print(f"\nClassifying {len(returns_df)} events...")
    results = []
    for i, row in returns_df.iterrows():
        if i % 200 == 0:
            print(f"  {i}/{len(returns_df)}...", flush=True)

        role_info = classify_event(
            ticker=row["ticker"],
            cluster_date=row["cluster_date"].strftime("%Y-%m-%d"),
            purchase_index=purchase_index,
        )
        results.append(role_info)

    role_df = pd.DataFrame(results)
    merged = pd.concat([returns_df.reset_index(drop=True), role_df], axis=1)

    # Save
    merged.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved to {OUTPUT_CSV}")

    # Summary statistics
    matched = merged["n_title_matched"] > 0
    has_ceo_or_cfo = merged["has_ceo"] | merged["has_cfo"]
    print(f"\n=== COVERAGE SUMMARY ===")
    print(f"Events with any Form 4 match: {matched.sum()} ({matched.mean():.1%})")
    print(f"Events with title data: {(merged['n_with_title'] > 0).sum()} ({(merged['n_with_title'] > 0).mean():.1%})")
    print(f"CEO present: {merged['has_ceo'].sum()} ({merged['has_ceo'].mean():.1%})")
    print(f"CFO present: {merged['has_cfo'].sum()} ({merged['has_cfo'].mean():.1%})")
    print(f"CEO or CFO present: {has_ceo_or_cfo.sum()} ({has_ceo_or_cfo.mean():.1%})")
    print(f"C-suite present: {merged['has_c_suite'].sum()} ({merged['has_c_suite'].mean():.1%})")

    # Performance comparison
    print(f"\n=== PERFORMANCE BY CEO/CFO PRESENCE ===")
    for label, mask in [
        ("With CEO or CFO", has_ceo_or_cfo),
        ("Without CEO or CFO", ~has_ceo_or_cfo),
    ]:
        sub = merged[mask]["abnormal_5d"]
        if len(sub) >= 10:
            from scipy import stats
            t, p = stats.ttest_1samp(sub.dropna(), 0)
            pos_rate = (sub > 0).mean()
            print(f"{label} (n={len(sub)}):")
            print(f"  avg 5d abnormal: {sub.mean():+.2f}%  median={sub.median():+.2f}%")
            print(f"  pos_rate={pos_rate:.1%}  p={p:.4f}")


if __name__ == "__main__":
    main()
