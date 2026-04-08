#!/usr/bin/env python3
"""
Intraday Poller Historical Replay
=================================

Purpose
-------
Validate the tools/intraday_insider_poller.py infrastructure by answering:
"If we had been running the poller every day during 2024-2025, what fraction
of canonical-positive insider clusters would we have caught at a tradeable
trigger time, and would the catchable subset still carry the alpha?"

This is the true OOS test of the intraday pivot established on 2026-04-08
(see known_effect: insider_cluster_canonical_benchmark_2026_04_08).

Methodology
-----------
1. Load cached canonical cluster rows (2024-2025, n=626) from
   data/cache/insider_cluster_timing_drift_rows.json.

2. Apply canonical gate: has_ceo_cfo AND lag_days<=1 AND n_insiders in [3,5].
   This is the subset the NEW scanner would surface post-2026-04-07 (n~221).

3. For each canonical cluster, resolve ticker -> CIK via SEC company_tickers.json
   and fetch the issuer's Form 4 submissions from data.sec.gov/submissions/.
   Find the max acceptanceDateTime for filings whose filingDate matches the
   cluster's filing_date. That is the cluster's "trigger time" — the earliest
   moment a real-time poller could have detected the cluster.

4. Classify each cluster's trigger time relative to a simulated poll at
   14:00 ET on the filing_date:
     - pre_open_same_day      (00:00-09:30)  -> catchable at open
     - intraday_same_day      (09:30-16:00)  -> catchable intraday
     - after_close_same_day   (16:00-23:59)  -> catchable next open
     - previous_session       (-1 day)       -> MISSED (t+1 retired cadence)
     - stale                  (> -1 day)     -> MISSED
     - unknown                (no match)     -> NOT_FOUND

5. Catchability is defined as trigger_class in
   {pre_open_same_day, intraday_same_day, after_close_same_day}.

6. Segment canonical alpha (r_trans_5d, r_filing_5d) by catchability and
   by entry cadence. Report the expected alpha a real-time poller subscriber
   would have captured vs. the headline canonical benchmark.

Output
------
Prints a JSON summary of the replay to stdout. Writes a detailed per-cluster
CSV to data/cache/intraday_poller_historical_replay.csv.

Usage
-----
    python3 tools/intraday_poller_historical_replay.py
    python3 tools/intraday_poller_historical_replay.py --limit 50
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics as stats
import sys
import time
from pathlib import Path

import requests

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from tools.edgar_insider_scanner_v2 import sec_get  # noqa: E402
from tools.intraday_insider_poller import (  # noqa: E402
    _normalize_accept_time,
    classify_trigger,
    _parse_et_datetime,
)

ROWS_PATH = Path(_ROOT) / "data" / "cache" / "insider_cluster_timing_drift_rows.json"
CSV_OUT = Path(_ROOT) / "data" / "cache" / "intraday_poller_historical_replay.csv"
JSON_OUT = Path(_ROOT) / "data" / "cache" / "intraday_poller_historical_replay.json"
TICKER_CIK_CACHE = Path(_ROOT) / "data" / "cache" / "sec_company_tickers.json"

USER_AGENT = os.environ.get("SEC_USER_AGENT", "Financial Research Bot contact@example.com")
HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}


# ---------------------------------------------------------------------------
# Ticker -> CIK mapping
# ---------------------------------------------------------------------------

def load_ticker_to_cik() -> dict[str, str]:
    """Download and cache SEC company_tickers.json. Returns {TICKER: CIK10}."""
    if TICKER_CIK_CACHE.exists():
        try:
            with open(TICKER_CIK_CACHE) as f:
                data = json.load(f)
            if data:
                return data
        except Exception:
            pass

    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    out: dict[str, str] = {}
    for _, entry in raw.items():
        ticker = str(entry.get("ticker", "")).upper()
        cik = entry.get("cik_str")
        if not ticker or cik is None:
            continue
        out[ticker] = str(int(cik)).zfill(10)

    TICKER_CIK_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(TICKER_CIK_CACHE, "w") as f:
        json.dump(out, f)
    return out


# ---------------------------------------------------------------------------
# Per-issuer Form 4 fetcher (with disk cache)
# ---------------------------------------------------------------------------

SUBS_CACHE_PATH = Path(_ROOT) / "data" / "cache" / "intraday_replay_submissions_cache.json"


def _load_subs_cache() -> dict:
    if SUBS_CACHE_PATH.exists():
        try:
            return json.load(open(SUBS_CACHE_PATH))
        except Exception:
            return {}
    return {}


def _save_subs_cache(cache: dict):
    SUBS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(SUBS_CACHE_PATH) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, SUBS_CACHE_PATH)


def fetch_issuer_form4s(cik10: str, cache: dict) -> list[dict]:
    """Return recent Form 4 filings for a CIK. Each entry: {filing_date, accept_time}."""
    if cik10 in cache:
        return cache[cik10]

    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    try:
        resp = sec_get(url, timeout=20)
        if resp is None or resp.status_code != 200:
            cache[cik10] = []
            return []
        data = resp.json()
    except Exception:
        cache[cik10] = []
        return []

    all_f4s: list[dict] = []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", []) or []
    filing_dates = recent.get("filingDate", []) or []
    accept_dates = recent.get("acceptanceDateTime", []) or []

    for i, form in enumerate(forms):
        if form != "4":
            continue
        all_f4s.append({
            "filing_date": filing_dates[i] if i < len(filing_dates) else "",
            "accept_time": _normalize_accept_time(
                accept_dates[i] if i < len(accept_dates) else ""
            ),
        })

    # Also walk the older "files" pagination to catch 2024 filings (submissions API
    # typically keeps ~1000 recent; older ones live in EDGAR pagination files).
    for older_file in data.get("filings", {}).get("files", []) or []:
        name = older_file.get("name")
        if not name:
            continue
        url2 = f"https://data.sec.gov/submissions/{name}"
        try:
            resp2 = sec_get(url2, timeout=20)
            if resp2 is None or resp2.status_code != 200:
                continue
            d2 = resp2.json()
        except Exception:
            continue
        forms2 = d2.get("form", []) or []
        filing_dates2 = d2.get("filingDate", []) or []
        accept_dates2 = d2.get("acceptanceDateTime", []) or []
        for i, form in enumerate(forms2):
            if form != "4":
                continue
            all_f4s.append({
                "filing_date": filing_dates2[i] if i < len(filing_dates2) else "",
                "accept_time": _normalize_accept_time(
                    accept_dates2[i] if i < len(accept_dates2) else ""
                ),
            })

    cache[cik10] = all_f4s
    return all_f4s


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def get_trigger_time_for_cluster(
    ticker: str,
    filing_date: str,
    t2c: dict[str, str],
    subs_cache: dict,
) -> tuple[str, str]:
    """Return (trigger_time_str, reason). Empty trigger_time_str => NOT_FOUND."""
    ticker_up = ticker.upper()
    cik = t2c.get(ticker_up)
    if not cik:
        return "", "no_cik"
    f4s = fetch_issuer_form4s(cik, subs_cache)
    if not f4s:
        return "", "no_form4s"
    same_day = [f for f in f4s if f.get("filing_date") == filing_date and f.get("accept_time")]
    if not same_day:
        return "", "no_same_day_filing"
    accept_times = [f["accept_time"] for f in same_day]
    return max(accept_times), "ok"


def summarize(vals: list[float], threshold: float = 0.5) -> dict:
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"n": 0}
    return {
        "n": len(vals),
        "mean": round(stats.mean(vals), 2),
        "median": round(stats.median(vals), 2),
        "stdev": round(stats.stdev(vals), 2) if len(vals) > 1 else None,
        "pos_rate": round(sum(1 for v in vals if v > threshold) / len(vals) * 100, 1),
    }


def run_replay(limit: int | None = None) -> dict:
    rows_all = json.load(open(ROWS_PATH))
    gated = [
        r for r in rows_all
        if r.get("has_ceo_cfo")
        and r.get("lag_days") is not None
        and r["lag_days"] <= 1
        and 3 <= r.get("n_insiders", 0) <= 5
    ]
    print(f"[info] canonical gated rows: {len(gated)}", file=sys.stderr)
    if limit:
        gated = gated[:limit]

    t2c = load_ticker_to_cik()
    print(f"[info] ticker->cik map size: {len(t2c)}", file=sys.stderr)

    subs_cache = _load_subs_cache()

    enriched: list[dict] = []
    t0 = time.time()
    for i, r in enumerate(gated, 1):
        if i % 25 == 0:
            print(f"[progress] {i}/{len(gated)} ({time.time()-t0:.0f}s)", file=sys.stderr)
            _save_subs_cache(subs_cache)
        accept_time, reason = get_trigger_time_for_cluster(
            r["ticker"], r["filing_date"], t2c, subs_cache
        )

        # Simulate a poll run at 14:00 ET on the filing_date (so we can detect
        # intraday filings; an after_close filing on the same day would be
        # caught by the next 14:00 poll the following day but classified relative
        # to its own filing_date).
        sim_now = _parse_et_datetime(f"{r['filing_date']} 14:00:00")
        trigger_class = (
            classify_trigger(accept_time, sim_now) if accept_time else "not_found"
        )
        if reason != "ok":
            trigger_class = f"not_found_{reason}"

        enriched.append({
            **r,
            "accept_time": accept_time,
            "trigger_class": trigger_class,
        })
    _save_subs_cache(subs_cache)
    print(f"[info] done in {time.time()-t0:.0f}s", file=sys.stderr)

    # Count trigger classes
    from collections import Counter
    class_counts = Counter(e["trigger_class"] for e in enriched)

    # Catchable = real-time-tradeable this session or next open
    CATCHABLE = {"intraday_same_day", "after_close_same_day", "pre_open_same_day"}
    catchable = [e for e in enriched if e["trigger_class"] in CATCHABLE]
    not_catchable = [e for e in enriched if e["trigger_class"] not in CATCHABLE]
    catch_rate = round(len(catchable) / len(enriched) * 100, 1) if enriched else 0

    # Alpha by segment
    def alpha_for(subset, field):
        return summarize([e.get(field) for e in subset])

    result = {
        "n_canonical_gated": len(enriched),
        "n_catchable": len(catchable),
        "catch_rate_pct": catch_rate,
        "trigger_class_distribution": dict(class_counts),
        "catchable_alpha": {
            "r_trans_5d": alpha_for(catchable, "r_trans_5d"),
            "r_filing_5d": alpha_for(catchable, "r_filing_5d"),
            "r_filing_plus1_5d": alpha_for(catchable, "r_filing_plus1_5d"),
        },
        "not_catchable_alpha": {
            "r_trans_5d": alpha_for(not_catchable, "r_trans_5d"),
            "r_filing_5d": alpha_for(not_catchable, "r_filing_5d"),
            "r_filing_plus1_5d": alpha_for(not_catchable, "r_filing_plus1_5d"),
        },
        "by_trigger_class": {},
    }

    for klass in sorted(class_counts.keys()):
        subset = [e for e in enriched if e["trigger_class"] == klass]
        result["by_trigger_class"][klass] = {
            "n": len(subset),
            "r_trans_5d": alpha_for(subset, "r_trans_5d"),
            "r_filing_5d": alpha_for(subset, "r_filing_5d"),
            "r_filing_plus1_5d": alpha_for(subset, "r_filing_plus1_5d"),
        }

    # Dump detail CSV
    CSV_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "ticker", "trans_date", "filing_date", "lag_days", "n_insiders",
            "accept_time", "trigger_class",
            "r_trans_5d", "r_filing_5d", "r_filing_plus1_5d",
        ])
        for e in enriched:
            w.writerow([
                e["ticker"], e["trans_date"], e["filing_date"], e["lag_days"],
                e["n_insiders"], e["accept_time"], e["trigger_class"],
                e.get("r_trans_5d"), e.get("r_filing_5d"), e.get("r_filing_plus1_5d"),
            ])

    with open(JSON_OUT, "w") as f:
        json.dump(result, f, indent=2, default=str)

    return result


def main():
    p = argparse.ArgumentParser(description="Intraday poller historical replay")
    p.add_argument("--limit", type=int, default=None)
    args = p.parse_args()
    result = run_replay(limit=args.limit)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
