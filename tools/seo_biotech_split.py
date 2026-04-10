"""Split SEO bought-deal events into biotech vs non-biotech and run separate backtests.

Reviewer R5 (April 10): "Split biotech vs non-biotech SEO events. May improve
consistency from 67.9% to 72%+."

Classification: yfinance sector == "Healthcare" AND industry contains biotech/pharma keywords.
Falls back to company name keyword match if yfinance info unavailable.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.yfinance_utils import get_close_prices, safe_download
import market_data
import yfinance as yf

EVENTS_FILE = "data/seo_bought_deals_combined_2020_2025.json"

BIOTECH_NAME_KEYWORDS = [
    "biosciences", "biotherapeutics", "therapeutics", "pharma", "biopharma",
    "biotech", "genomics", "oncology", "biologic", "genetics", "medicines",
    "bioscience", "biologics", "medi-", "immunotherapy", "molecular",
    "cellular", "gene", "vaccines", "protein", "rna", "crispr"
]


def classify_biotech(ticker: str, company_name: str) -> tuple[bool, str]:
    """Returns (is_biotech, reason)."""
    # Try yfinance sector/industry first
    try:
        info = yf.Ticker(ticker).info
        sector = (info.get("sector") or "").lower()
        industry = (info.get("industry") or "").lower()
        if sector == "healthcare":
            if any(k in industry for k in ["biotech", "drug", "pharma", "medical"]):
                return True, f"yf sector={sector} industry={industry}"
    except Exception:
        pass

    # Fall back to company name keywords
    name_lower = company_name.lower()
    for kw in BIOTECH_NAME_KEYWORDS:
        if kw in name_lower:
            return True, f"name_kw={kw}"

    return False, "not healthcare"


def main():
    with open(EVENTS_FILE) as f:
        events = json.load(f)

    events = [e for e in events if e.get("ticker")]
    print(f"Total events with tickers: {len(events)}")

    # Classify each event
    biotech_events = []
    nonbio_events = []

    for e in events:
        ticker = e["ticker"]
        name = e.get("company_name", "")
        is_bio, reason = classify_biotech(ticker, name)
        entry = {
            "symbol": ticker,
            "date": e["announcement_date"],
            "company": name,
            "reason": reason,
        }
        if is_bio:
            biotech_events.append(entry)
        else:
            nonbio_events.append(entry)

    print(f"\nBiotech: {len(biotech_events)}")
    print(f"Non-biotech: {len(nonbio_events)}")

    # Print biotech sample
    print("\nSample biotech events:")
    for e in biotech_events[:5]:
        print(f"  {e['symbol']:6s} {e['date']} {e['company'][:40]} ({e['reason']})")
    print("\nSample non-biotech events:")
    for e in nonbio_events[:5]:
        print(f"  {e['symbol']:6s} {e['date']} {e['company'][:40]} ({e['reason']})")

    # Run backtests on both subsets
    print("\n" + "="*60)
    print("BIOTECH SEO BACKTEST")
    print("="*60)
    def aggregate(impacts, label):
        """Aggregate individual_impacts list into per-horizon stats."""
        import statistics
        from scipy import stats as spstats
        n_valid = len(impacts)
        out = {"label": label, "n": n_valid}
        for h in [1, 3, 5, 10]:
            key = f"abnormal_{h}d"
            vals = [imp.get(key) for imp in impacts if imp.get(key) is not None]
            if not vals:
                continue
            med = statistics.median(vals)
            mean = statistics.mean(vals)
            neg_rate = sum(1 for v in vals if v < 0) / len(vals)
            # Wilcoxon signed-rank (nonparametric)
            try:
                w_stat, w_p = spstats.wilcoxon(vals)
            except Exception:
                w_p = None
            # T-test
            try:
                t_stat, t_p = spstats.ttest_1samp(vals, 0)
            except Exception:
                t_p = None
            out[f"{h}d"] = {
                "n": len(vals),
                "mean": round(mean, 2),
                "median": round(med, 2),
                "neg_rate": round(neg_rate * 100, 1),
                "wilcoxon_p": round(w_p, 4) if w_p is not None else None,
                "t_p": round(t_p, 4) if t_p is not None else None,
            }
        return out

    bio_events_for_backtest = [{"symbol": e["symbol"], "date": e["date"]} for e in biotech_events]
    bio_result = market_data.measure_event_impact(
        event_dates=bio_events_for_backtest,
        benchmark="SPY",
        entry_price="open",
    )
    bio_stats = aggregate(bio_result.get("individual_impacts", []), "BIOTECH")
    print(f"N measured: {bio_stats['n']}")
    for h in [1, 3, 5, 10]:
        if f"{h}d" in bio_stats:
            s = bio_stats[f"{h}d"]
            print(f"  {h}d: avg={s['mean']}% median={s['median']}% neg_rate={s['neg_rate']}% wilcoxon_p={s['wilcoxon_p']} t_p={s['t_p']} (n={s['n']})")

    print("\n" + "="*60)
    print("NON-BIOTECH SEO BACKTEST")
    print("="*60)
    nonbio_events_for_backtest = [{"symbol": e["symbol"], "date": e["date"]} for e in nonbio_events]
    nonbio_result = market_data.measure_event_impact(
        event_dates=nonbio_events_for_backtest,
        benchmark="SPY",
        entry_price="open",
    )
    nonbio_stats = aggregate(nonbio_result.get("individual_impacts", []), "NON-BIOTECH")
    print(f"N measured: {nonbio_stats['n']}")
    for h in [1, 3, 5, 10]:
        if f"{h}d" in nonbio_stats:
            s = nonbio_stats[f"{h}d"]
            print(f"  {h}d: avg={s['mean']}% median={s['median']}% neg_rate={s['neg_rate']}% wilcoxon_p={s['wilcoxon_p']} t_p={s['t_p']} (n={s['n']})")

    # Save results
    summary = {
        "biotech": {
            "n_attempted": len(biotech_events),
            "stats": bio_stats,
        },
        "non_biotech": {
            "n_attempted": len(nonbio_events),
            "stats": nonbio_stats,
        },
    }
    out_path = "data/seo_biotech_split_results.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
