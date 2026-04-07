#!/usr/bin/env python3
"""
Insider Cluster Canonical Real-Time Benchmark
=============================================
Reanalyzes the cached timing_drift rows applying the new scanner gates
(filing_lag_days <= 1, CEO/CFO present, n in [3,5]) to derive the
canonical expected real-time return for the post-2026-04-07 scanner.

The trans_date-based feature analysis (n=5364) reported +2% large-cap 5d.
That number assumes a backtest entry impossible to achieve in real time.

This script answers: what does the scanner ACTUALLY expect to earn at
filing_date+1 entry, after the new lag<=1bd hard block is in place?

Usage: python3 tools/insider_cluster_canonical_benchmark.py
"""
import json
import statistics as stats
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROWS_PATH = ROOT / "data" / "cache" / "insider_cluster_timing_drift_rows.json"


def summarize(vals, label, threshold=0.5):
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"label": label, "n": 0}
    return {
        "label": label,
        "n": len(vals),
        "mean": round(stats.mean(vals), 2),
        "median": round(stats.median(vals), 2),
        "stdev": round(stats.stdev(vals), 2) if len(vals) > 1 else None,
        "pos_rate": round(sum(1 for v in vals if v > threshold) / len(vals) * 100, 1),
    }


def filter_rows(rows, ceo_only=False, lag_max=None, n_range=None):
    out = rows
    if ceo_only:
        out = [r for r in out if r.get("has_ceo_cfo")]
    if lag_max is not None:
        out = [r for r in out if r.get("lag_days") is not None and r["lag_days"] <= lag_max]
    if n_range is not None:
        lo, hi = n_range
        out = [r for r in out if lo <= r.get("n_insiders", 0) <= hi]
    return out


def main():
    rows = json.load(open(ROWS_PATH))
    print(f"[info] loaded {len(rows)} cached cluster rows", file=sys.stderr)

    out = {
        "source": str(ROWS_PATH),
        "n_total_rows": len(rows),
        "scope_note": (
            "Cached rows are 2024-2025 SEC EDGAR Form 4 clusters (n=626). "
            "Applying the new post-2026-04-07 scanner gates here gives the "
            "canonical real-time-tradeable expected return."
        ),
    }

    # Baseline: all clusters
    out["baseline_all"] = {
        "n": len(rows),
        "post_trans_5d": summarize([r["r_trans_5d"] for r in rows], "post_trans_5d"),
        "post_filing_5d": summarize([r["r_filing_5d"] for r in rows], "post_filing_5d"),
        "post_filing_plus1_5d": summarize([r["r_filing_plus1_5d"] for r in rows], "post_filing_plus1_5d"),
    }

    # Gate 1: CEO/CFO only
    g1 = filter_rows(rows, ceo_only=True)
    out["gate_ceo_cfo"] = {
        "n": len(g1),
        "post_trans_5d": summarize([r["r_trans_5d"] for r in g1], "post_trans_5d"),
        "post_filing_5d": summarize([r["r_filing_5d"] for r in g1], "post_filing_5d"),
        "post_filing_plus1_5d": summarize([r["r_filing_plus1_5d"] for r in g1], "post_filing_plus1_5d"),
    }

    # Gate 2: CEO/CFO + lag<=1d (the new scanner hard block)
    g2 = filter_rows(rows, ceo_only=True, lag_max=1)
    out["gate_ceo_cfo_lag1"] = {
        "n": len(g2),
        "post_trans_5d": summarize([r["r_trans_5d"] for r in g2], "post_trans_5d"),
        "post_filing_5d": summarize([r["r_filing_5d"] for r in g2], "post_filing_5d"),
        "post_filing_plus1_5d": summarize([r["r_filing_plus1_5d"] for r in g2], "post_filing_plus1_5d"),
    }

    # Gate 3: CEO/CFO + lag<=1d + n in [3,5] (full evaluator gate)
    g3 = filter_rows(rows, ceo_only=True, lag_max=1, n_range=(3, 5))
    out["gate_ceo_cfo_lag1_n35"] = {
        "n": len(g3),
        "post_trans_5d": summarize([r["r_trans_5d"] for r in g3], "post_trans_5d"),
        "post_filing_5d": summarize([r["r_filing_5d"] for r in g3], "post_filing_5d"),
        "post_filing_plus1_5d": summarize([r["r_filing_plus1_5d"] for r in g3], "post_filing_plus1_5d"),
    }

    # Sanity: lag=0 only (filed same day as transaction — fastest possible)
    g4 = filter_rows(rows, ceo_only=True, lag_max=0)
    out["gate_ceo_cfo_lag0"] = {
        "n": len(g4),
        "post_trans_5d": summarize([r["r_trans_5d"] for r in g4], "post_trans_5d"),
        "post_filing_5d": summarize([r["r_filing_5d"] for r in g4], "post_filing_5d"),
        "post_filing_plus1_5d": summarize([r["r_filing_plus1_5d"] for r in g4], "post_filing_plus1_5d"),
    }

    # Decision summary
    g3_filing = [r["r_filing_5d"] for r in g3 if r["r_filing_5d"] is not None]
    g3_filing_p1 = [r["r_filing_plus1_5d"] for r in g3 if r["r_filing_plus1_5d"] is not None]
    out["canonical_benchmark"] = {
        "gate": "CEO/CFO present + filing_lag<=1bd + n_insiders in [3,5]",
        "n": len(g3),
        "real_time_entry_filing_date": {
            "n": len(g3_filing),
            "mean_5d_abn": round(stats.mean(g3_filing), 2) if g3_filing else None,
            "pos_rate": round(sum(1 for v in g3_filing if v > 0.5) / len(g3_filing) * 100, 1) if g3_filing else None,
        },
        "scanner_t_plus_1_entry": {
            "n": len(g3_filing_p1),
            "mean_5d_abn": round(stats.mean(g3_filing_p1), 2) if g3_filing_p1 else None,
            "pos_rate": round(sum(1 for v in g3_filing_p1 if v > 0.5) / len(g3_filing_p1) * 100, 1) if g3_filing_p1 else None,
        },
        "interpretation": (
            "When the scanner activates a trade on the morning after a Form 4 lands "
            "(real-world cadence), this is the expected 5d abnormal return distribution. "
            "The historical trans_date-based +2% number is unattainable in practice; "
            "this is the gate-conditioned filing-date benchmark."
        ),
    }

    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
