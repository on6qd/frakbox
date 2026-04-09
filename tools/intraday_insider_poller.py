#!/usr/bin/env python3
"""
Intraday Insider Cluster Poller
================================

Detects insider buying clusters that became qualifying TODAY so that we can
enter at filing_day_intraday cadence (the only cadence with positive expected
return per insider_cluster_t_plus_1_retirement_2026_04_08: +3.28% / 58.8% pos
rate vs. t+1 which was +1.26% / 42.5% pos rate).

Strategy
--------
1. Run the existing scanner_v2 with a rolling lookback window (default 14d)
   to catch clusters whose 3rd+ insider filed today but whose earlier members
   filed up to 30 days earlier.

2. For each returned cluster, enrich it with the filing acceptance time
   of its most-recent Form 4 (from EDGAR `index.json` `last-modified`).
   This is the cluster's "trigger time".

3. Classify each cluster:
     - `new_cluster_intraday`  -> trigger_time today during 09:30-16:00 ET
                                  (tradeable same-day at next price print)
     - `new_cluster_after_close` -> trigger_time today 16:00-23:59 ET
                                   (next-open entry, borderline — still high lag)
     - `new_cluster_yesterday`  -> trigger yesterday (miss window — t+1 retired)
     - `known_cluster`          -> already seen in a previous poll run
     - `stale_cluster`          -> trigger_time >2 sessions old

4. Persist state across runs in `data/intraday_poller_state.json`:
     { "last_run_ts": ..., "seen_accessions": [...], "seen_clusters": {ticker: first_seen_ts} }

5. Append each poll run to `data/intraday_poller_alerts.jsonl` for audit.

Usage
-----
    python3 tools/intraday_insider_poller.py            # standard run
    python3 tools/intraday_insider_poller.py --days 7   # shorter lookback
    python3 tools/intraday_insider_poller.py --dry-run  # don't update state
    python3 tools/intraday_insider_poller.py --since 2026-04-08T14:00
                                                        # override "today start"

Output
------
JSON to stdout with fields:
    run_time, window_days, clusters_total,
    new_clusters (list), known_clusters (list), stale_clusters (list)

Each cluster dict includes:
    ticker, issuer_name, n_insiders, total_value, has_ceo, has_cfo,
    latest_accept_time, trigger_classification, evaluator_decision
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

# Ensure repo root on path
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from tools.edgar_insider_scanner_v2 import (  # noqa: E402
    scan_insider_clusters,
    sec_get,
)

STATE_PATH = os.path.join(_ROOT, "data", "intraday_poller_state.json")
ALERTS_PATH = os.path.join(_ROOT, "data", "intraday_poller_alerts.jsonl")
ACCEPT_CACHE_PATH = os.path.join(_ROOT, "data", "intraday_poller_accept_cache.json")

USER_AGENT = os.environ.get(
    "SEC_USER_AGENT", "Financial Research Bot contact@example.com"
)
SEC_HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}

# Market hours in ET (naive — good enough for intraday classification)
MARKET_OPEN_HHMM = (9, 30)
MARKET_CLOSE_HHMM = (16, 0)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "last_run_ts": None,
        "seen_accessions": [],
        "seen_clusters": {},  # ticker -> {first_seen_ts, latest_accept_time, n_insiders}
    }


def _save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_PATH)


def _load_accept_cache() -> dict:
    if os.path.exists(ACCEPT_CACHE_PATH):
        try:
            with open(ACCEPT_CACHE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_accept_cache(cache: dict):
    os.makedirs(os.path.dirname(ACCEPT_CACHE_PATH), exist_ok=True)
    tmp = ACCEPT_CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, ACCEPT_CACHE_PATH)


# ---------------------------------------------------------------------------
# Acceptance time fetcher (via EDGAR submissions API)
# ---------------------------------------------------------------------------

def _normalize_accept_time(ts: str) -> str:
    """Normalize EDGAR acceptance timestamp to 'YYYY-MM-DD HH:MM:SS' (naive ET).

    EDGAR stores acceptanceDateTime in the submissions API as ISO with a 'Z'
    suffix, but the time is actually already in Eastern Time — see
    https://www.sec.gov/edgar/searchedgar/acceptance.htm. We strip the 'Z' and
    milliseconds and treat the result as naive ET.
    """
    if not ts:
        return ""
    t = ts.replace("Z", "").split(".")[0]
    # Expected form: 2026-04-08T15:52:16
    return t.replace("T", " ")


def get_issuer_form4_filings(issuer_cik: str, cache: Optional[dict] = None) -> list[dict]:
    """Return list of this issuer's recent Form 4 filings with acceptance times.

    Each entry: {accession, filing_date, accept_time (naive ET)}.
    Cached per-CIK with a short TTL for the duration of a single poll run.
    """
    if not issuer_cik:
        return []
    cik_padded = str(int(issuer_cik)).zfill(10)

    if cache is not None and cik_padded in cache:
        return cache[cik_padded]

    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    try:
        resp = sec_get(url, timeout=15)
        if resp is None or resp.status_code != 200:
            if cache is not None:
                cache[cik_padded] = []
            return []
        data = resp.json()
    except Exception:
        if cache is not None:
            cache[cik_padded] = []
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", []) or []
    accessions = recent.get("accessionNumber", []) or []
    filing_dates = recent.get("filingDate", []) or []
    accept_dates = recent.get("acceptanceDateTime", []) or []

    form4s = []
    for i, form in enumerate(forms):
        if form != "4":
            continue
        form4s.append({
            "accession": accessions[i] if i < len(accessions) else "",
            "filing_date": filing_dates[i] if i < len(filing_dates) else "",
            "accept_time": _normalize_accept_time(
                accept_dates[i] if i < len(accept_dates) else ""
            ),
        })

    if cache is not None:
        cache[cik_padded] = form4s
    return form4s


# ---------------------------------------------------------------------------
# Cluster enrichment
# ---------------------------------------------------------------------------

def enrich_cluster_with_accept_time(cluster: dict, cache: dict) -> dict:
    """Add `latest_accept_time` to cluster using the EDGAR submissions API.

    For the issuer's recent Form 4s, pick the one whose filing_date matches
    the cluster's latest_filing_date and return its acceptance time. If multiple
    Form 4s land on that day, use the max acceptance time.
    """
    issuer_cik = cluster.get("issuer_cik", "")
    latest_filing_date = cluster.get("latest_filing_date", "")
    if not issuer_cik or not latest_filing_date:
        cluster["latest_accept_time"] = ""
        return cluster

    form4s = get_issuer_form4_filings(issuer_cik, cache=cache)
    same_day = [f for f in form4s if f.get("filing_date") == latest_filing_date]
    if same_day:
        times = [f["accept_time"] for f in same_day if f.get("accept_time")]
        cluster["latest_accept_time"] = max(times) if times else ""
    else:
        cluster["latest_accept_time"] = ""
    return cluster


# ---------------------------------------------------------------------------
# Trigger classification
# ---------------------------------------------------------------------------

def _parse_et_datetime(s: str) -> Optional[datetime]:
    """Parse 'YYYY-MM-DD HH:MM:SS' as a naive ET datetime."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def classify_trigger(latest_accept_time: str, now_et: datetime) -> str:
    """Classify a cluster's trigger based on when its latest filing was accepted.

    Returns one of:
        "intraday_same_day"       -> today, 09:30-16:00 ET (fully tradeable)
        "after_close_same_day"    -> today, 16:00-23:59 ET (next open entry)
        "pre_open_same_day"       -> today, 00:00-09:30 ET (today's open entry)
        "previous_session"        -> yesterday (t+1 — retired cadence)
        "stale"                   -> >2 sessions old
        "unknown"                 -> parse error
    """
    dt = _parse_et_datetime(latest_accept_time)
    if dt is None:
        return "unknown"

    today = now_et.date()
    delta_days = (today - dt.date()).days

    if delta_days < 0:
        return "unknown"  # future timestamp, shouldn't happen
    if delta_days > 2:
        return "stale"
    if delta_days == 1:
        return "previous_session"
    if delta_days == 2:
        return "stale"

    # Same calendar day
    h, m = dt.hour, dt.minute
    if (h, m) < MARKET_OPEN_HHMM:
        return "pre_open_same_day"
    if (h, m) < MARKET_CLOSE_HHMM:
        return "intraday_same_day"
    return "after_close_same_day"


# ---------------------------------------------------------------------------
# Main poller
# ---------------------------------------------------------------------------

def run_poller(
    days: int = 14,
    min_insiders: int = 3,
    min_value: int = 50_000,
    dry_run: bool = False,
    now_et: Optional[datetime] = None,
    quiet: bool = True,
) -> dict:
    """Run one poll cycle. Returns result dict and writes alert + state."""
    from tools.insider_cluster_evaluator import evaluate_cluster

    if now_et is None:
        try:
            import pytz
            now_et = datetime.now(pytz.timezone("US/Eastern")).replace(tzinfo=None)
        except ImportError:
            now_et = datetime.now()  # fallback if pytz not available

    run_ts = now_et.strftime("%Y-%m-%dT%H:%M:%S")
    state = _load_state()
    accept_cache = _load_accept_cache()

    # Step 1: Run scanner
    t0 = time.time()
    clusters = scan_insider_clusters(
        days=days,
        min_insiders=min_insiders,
        min_value_per_insider=min_value,
        quiet=quiet,
    )
    scan_elapsed = time.time() - t0

    # Step 2: Enrich each cluster with latest acceptance time
    for c in clusters:
        enrich_cluster_with_accept_time(c, accept_cache)

    # Step 3: Classify and decide
    seen_clusters = state.get("seen_clusters", {})
    new_clusters: list[dict] = []
    known_clusters: list[dict] = []
    stale_clusters: list[dict] = []

    for c in clusters:
        ticker = c.get("ticker", "")
        # Clean exchange prefix
        if ":" in ticker:
            ticker = ticker.split(":")[-1]
        if not ticker or ticker.upper() in ("NONE", "N/A", ""):
            continue

        trigger_class = classify_trigger(c.get("latest_accept_time", ""), now_et)

        # CEO/CFO detection
        has_ceo = False
        has_cfo = False
        for ins in c.get("insiders", []):
            title = (ins.get("title") or "").upper()
            if "CEO" in title or "CHIEF EXECUTIVE" in title:
                has_ceo = True
            if "CFO" in title or "CHIEF FINANCIAL" in title:
                has_cfo = True

        # Run GO/NO-GO evaluator
        try:
            decision = evaluate_cluster(
                ticker=ticker,
                n_insiders=c["n_insiders"],
                total_value_usd=c["total_value"],
                has_ceo=has_ceo,
                has_cfo=has_cfo,
                days_since_latest_filing=c.get("days_since_latest_filing"),
                max_trans_to_filing_lag=c.get("max_trans_to_filing_lag"),
            )
        except Exception as e:
            decision = {"decision": "ERROR", "score": 0, "blockers": [str(e)], "warnings": []}

        # Trigger-class quality tier (from insider_cluster_intraday_replay_2026_04_09):
        #   STRONG: pre_open_same_day | intraday_same_day  -> realistic +2.08% / 62% pos (n=55, 2024-25)
        #   WEAK:   after_close_same_day                    -> realistic +0.67% / 39.7% pos (functionally t+1)
        #   MISS:   previous_session | stale | unknown
        if trigger_class in ("intraday_same_day", "pre_open_same_day"):
            quality_tier = "STRONG"
            quality_note = "same-session entry possible"
        elif trigger_class == "after_close_same_day":
            quality_tier = "WEAK"
            quality_note = (
                "after-close filing -> next-open entry only; expected 5d abnormal "
                "+0.67% / 39.7% pos (functionally retired t+1 cadence)"
            )
        else:
            quality_tier = "MISS"
            quality_note = "trigger time too stale to act"

        summary = {
            "ticker": ticker,
            "issuer_name": c.get("issuer_name", "")[:60],
            "n_insiders": c["n_insiders"],
            "total_value": c["total_value"],
            "has_ceo": has_ceo,
            "has_cfo": has_cfo,
            "latest_filing_date": c.get("latest_filing_date"),
            "latest_accept_time": c.get("latest_accept_time"),
            "days_since_latest_filing": c.get("days_since_latest_filing"),
            "max_trans_to_filing_lag": c.get("max_trans_to_filing_lag"),
            "trigger_classification": trigger_class,
            "trigger_quality_tier": quality_tier,
            "trigger_quality_note": quality_note,
            "decision": decision.get("decision"),
            "score": decision.get("score"),
            "blockers": decision.get("blockers", []),
            "warnings": decision.get("warnings", []),
        }

        prev = seen_clusters.get(ticker)
        if prev and prev.get("latest_accept_time") == c.get("latest_accept_time"):
            known_clusters.append(summary)
            continue

        # Route by trigger class
        if trigger_class in ("stale", "unknown"):
            stale_clusters.append(summary)
        elif trigger_class in ("intraday_same_day", "after_close_same_day", "pre_open_same_day"):
            new_clusters.append(summary)
            seen_clusters[ticker] = {
                "first_seen_ts": run_ts,
                "latest_accept_time": c.get("latest_accept_time"),
                "n_insiders": c["n_insiders"],
            }
        else:  # previous_session — already too late per retirement decision
            stale_clusters.append(summary)
            # still record so we don't re-flag
            seen_clusters[ticker] = {
                "first_seen_ts": run_ts,
                "latest_accept_time": c.get("latest_accept_time"),
                "n_insiders": c["n_insiders"],
            }

    result = {
        "run_time": run_ts,
        "window_days": days,
        "scan_elapsed_sec": round(scan_elapsed, 1),
        "clusters_total": len(clusters),
        "new_clusters": new_clusters,
        "known_clusters": known_clusters,
        "stale_clusters": stale_clusters,
    }

    # Step 4: Persist state + alert log
    if not dry_run:
        state["last_run_ts"] = run_ts
        state["seen_clusters"] = seen_clusters
        _save_state(state)
        _save_accept_cache(accept_cache)

        os.makedirs(os.path.dirname(ALERTS_PATH), exist_ok=True)
        with open(ALERTS_PATH, "a") as f:
            f.write(json.dumps(result, default=str) + "\n")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Intraday insider cluster poller")
    p.add_argument("--days", type=int, default=14, help="Scanner lookback window (default 14)")
    p.add_argument("--min-insiders", type=int, default=3)
    p.add_argument("--min-value", type=int, default=50000)
    p.add_argument("--dry-run", action="store_true", help="Do not update state/alerts")
    p.add_argument("--now", type=str, default=None,
                   help="Override current ET datetime (YYYY-MM-DDTHH:MM) for testing")
    p.add_argument("--verbose", action="store_true", help="Non-quiet scanner output")
    args = p.parse_args()

    now_et = None
    if args.now:
        try:
            now_et = datetime.strptime(args.now, "%Y-%m-%dT%H:%M")
        except ValueError:
            print(f"Bad --now format: {args.now}", file=sys.stderr)
            sys.exit(2)

    result = run_poller(
        days=args.days,
        min_insiders=args.min_insiders,
        min_value=args.min_value,
        dry_run=args.dry_run,
        now_et=now_et,
        quiet=not args.verbose,
    )

    # Compact summary
    print(json.dumps({
        "run_time": result["run_time"],
        "clusters_total": result["clusters_total"],
        "new_count": len(result["new_clusters"]),
        "known_count": len(result["known_clusters"]),
        "stale_count": len(result["stale_clusters"]),
        "scan_elapsed_sec": result["scan_elapsed_sec"],
    }, indent=2))

    if result["new_clusters"]:
        # Sort new clusters: STRONG first, then WEAK
        strong = [c for c in result["new_clusters"] if c.get("trigger_quality_tier") == "STRONG"]
        weak = [c for c in result["new_clusters"] if c.get("trigger_quality_tier") == "WEAK"]
        if strong:
            print("\nNEW CLUSTERS [STRONG — same-session entry]:")
            for c in strong:
                print(f"  {c['ticker']:<6} n={c['n_insiders']} ${c['total_value']:>12,.0f} "
                      f"accept={c['latest_accept_time']} trig={c['trigger_classification']} "
                      f"dec={c['decision']} tier=STRONG")
        if weak:
            print("\nNEW CLUSTERS [WEAK — after-close, next-open only, ~t+1 cadence]:")
            for c in weak:
                print(f"  {c['ticker']:<6} n={c['n_insiders']} ${c['total_value']:>12,.0f} "
                      f"accept={c['latest_accept_time']} trig={c['trigger_classification']} "
                      f"dec={c['decision']} tier=WEAK")
    if result["stale_clusters"]:
        print("\nSTALE/MISSED CLUSTERS:")
        for c in result["stale_clusters"]:
            print(f"  {c['ticker']:<6} n={c['n_insiders']} ${c['total_value']:>12,.0f} "
                  f"accept={c['latest_accept_time']} trig={c['trigger_classification']} "
                  f"dec={c['decision']}")


if __name__ == "__main__":
    main()
