#!/usr/bin/env python3
"""Unified daily scanner — runs all daily scanners in one command.

Runs each scanner as a subprocess, collects JSON output, and produces a
single merged JSON summary on stdout plus a human-readable line on stderr.

Scanners run sequentially so EDGAR rate limits are respected.

Usage:
    python tools/daily_scanner.py              # defaults: --days 3 (7 for insiders)
    python tools/daily_scanner.py --days 5    # override days (insiders use 2x: 10)
    python tools/daily_scanner.py --days 7    # insiders use 14
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Repo root so we can find data_tasks.py and tool scripts
REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], label: str) -> tuple[bool, str]:
    """Run a command, return (success, stdout)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=300,
        )
        if result.returncode != 0:
            print(
                f"[daily_scanner] {label} exited with code {result.returncode}. "
                f"stderr: {result.stderr[-500:]}",
                file=sys.stderr,
            )
            return False, result.stdout
        return True, result.stdout
    except subprocess.TimeoutExpired:
        print(f"[daily_scanner] {label} timed out after 300s", file=sys.stderr)
        return False, ""
    except Exception as exc:
        print(f"[daily_scanner] {label} failed: {exc}", file=sys.stderr)
        return False, ""


def _extract_json(stdout: str) -> dict | list | None:
    """Extract the last valid JSON object or array from stdout.

    Some scanners print human-readable text first, then JSON at the end.
    We walk backwards through lines looking for a valid JSON start.
    """
    lines = stdout.strip().splitlines()
    # Try largest suffix first (full output), then shrink
    for start in range(len(lines)):
        candidate = "\n".join(lines[start:])
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
    # No valid JSON found
    return None


def _count_from_text(stdout: str, pattern: str) -> int:
    """Extract an integer after `pattern` from stdout text.

    e.g. pattern="Final events: " -> looks for "Final events: 3" -> 3
    """
    for line in stdout.splitlines():
        if pattern in line:
            after = line.split(pattern)[-1].strip().split()[0]
            try:
                return int(after)
            except ValueError:
                pass
    return 0


# ---------------------------------------------------------------------------
# Individual scanner runners
# ---------------------------------------------------------------------------

def run_intraday_insider_poller() -> dict:
    """Intraday insider cluster poller — checks for same-day tradeable clusters.

    Must run BEFORE the regular insider scanner. Catches clusters where
    Form 4 acceptance_datetime is during today's market hours, enabling
    filing_day_intraday entry (the only validated cadence: +3.28% / 58.8% pos rate).
    """
    label = "Intraday Insider Poller"
    poller_path = REPO_ROOT / "tools" / "intraday_insider_poller.py"
    if not poller_path.exists():
        return {"scanner": label, "status": "skip", "note": "poller not found", "new_clusters": []}

    cmd = [sys.executable, str(poller_path)]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "new_clusters": []}

    data = _extract_json(stdout)
    if not isinstance(data, dict):
        data = {}

    new_clusters = data.get("new_clusters", [])
    # Flag intraday-tradeable clusters
    intraday_go = [
        c for c in new_clusters
        if c.get("trigger_classification") == "new_cluster_intraday"
        and c.get("evaluator_decision", "").startswith("GO")
    ]

    return {
        "scanner": label,
        "status": "ok",
        "new_clusters_found": len(new_clusters),
        "intraday_go": len(intraday_go),
        "clusters": new_clusters,
        "intraday_go_clusters": intraday_go,
    }


def run_nt_10k(days: int) -> dict:
    """NT 10-K/10-Q scanner. Uses --json-events to get JSON list of events."""
    label = "NT 10-K"
    cmd = [
        sys.executable, "tools/nt_filing_scanner.py",
        "--days", str(days),
        "--tag-first-time",
        "--json-events",
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "events_found": 0, "events": []}

    events = _extract_json(stdout)
    if not isinstance(events, list):
        events = []

    # Note: when --json-events is combined with --tag-first-time, the JSON list only
    # carries {symbol, date} — first-time tag info is in stderr diagnostic text.
    # The event count is still accurate.
    return {
        "scanner": label,
        "status": "ok",
        "events_found": len(events),
        "events": events,
    }


def run_activist_13d(days: int) -> dict:
    """Activist 13D scanner. Always emits JSON summary at end of stdout."""
    label = "Activist 13D"
    cmd = [
        sys.executable, "tools/activist_13d_scanner.py",
        "--days", str(days),
        "--evaluate",
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "total_found": 0, "go_count": 0, "candidates": []}

    data = _extract_json(stdout)
    if not isinstance(data, dict):
        data = {}

    return {
        "scanner": label,
        "status": "ok",
        "total_found": data.get("total_found", 0),
        "go_count": data.get("go_count", 0),
        "nogo_count": data.get("nogo_count", 0),
        "candidates": data.get("candidates", []),
    }


def run_insider_evaluate(days_insiders: int) -> dict:
    """Insider cluster scanner with GO/NO-GO evaluation. Always emits JSON summary."""
    label = "Insider Clusters"
    cmd = [
        sys.executable, "data_tasks.py",
        "scan-insiders-evaluate",
        "--days", str(days_insiders),
        "--min-insiders", "3",
        "--min-value", "50000",
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {
            "scanner": label,
            "status": "error",
            "clusters_found": 0,
            "go_count": 0,
            "weak_go_count": 0,
            "no_go_count": 0,
            "evaluated": [],
        }

    data = _extract_json(stdout)
    if not isinstance(data, dict):
        data = {}

    return {
        "scanner": label,
        "status": data.get("status", "ok"),
        "clusters_found": data.get("clusters_found", 0),
        "go_count": data.get("go_count", 0),
        "weak_go_count": data.get("weak_go_count", 0),
        "no_go_count": data.get("no_go_count", 0),
        "result_id": data.get("result_id", ""),
        "evaluated": data.get("evaluated", []),
    }


def run_cybersecurity_8k(days: int) -> dict:
    """Cybersecurity 8-K Item 1.05 scanner. Uses --json-events to get JSON list."""
    label = "Cybersecurity 8-K (1.05)"
    cmd = [
        sys.executable, "tools/cybersecurity_8k_scanner.py",
        "--days", str(days),
        "--json-events",
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "events_found": 0, "events": []}

    events = _extract_json(stdout)
    if not isinstance(events, list):
        # Fallback: parse "Final events: N" from stdout text
        n = _count_from_text(stdout, "Final events: ")
        events = []

    return {
        "scanner": label,
        "status": "ok",
        "events_found": len(events),
        "events": events,
    }


def run_delisting_8k(days: int) -> dict:
    """Delisting 8-K Item 3.01 scanner. Uses --json-events to get JSON list."""
    label = "Delisting 8-K (3.01)"
    cmd = [
        sys.executable, "tools/delisting_8k_scanner.py",
        "--days", str(days),
        "--json-events",
        # NOTE: --forced-only REMOVED 2026-04-15. Original backtest (n=97) was on ALL
        # 3.01 filings (classifier bug labeled all as forced). Signal validated on combined
        # set. Forced-only restricts to ~2/year (untradeable). All filings: ~28/year.
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "events_found": 0, "events": []}

    events = _extract_json(stdout)
    if not isinstance(events, list):
        events = []

    return {
        "scanner": label,
        "status": "ok",
        "events_found": len(events),
        "events": events,
    }


def run_seo_scanner(days: int) -> dict:
    """SEO bought-deal scanner. Always emits JSON summary at end of stdout."""
    label = "SEO Bought Deal"
    cmd = [
        sys.executable, "tools/seo_realtime_scanner.py",
        "--days", str(days),
        "--evaluate",
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "total_found": 0, "go_count": 0, "candidates": []}

    data = _extract_json(stdout)
    if not isinstance(data, dict):
        data = {}

    return {
        "scanner": label,
        "status": "ok",
        "total_found": data.get("total_found", 0),
        "go_count": data.get("go_count", 0),
        "candidates": data.get("candidates", []),
    }


def run_sp500_changes(days: int) -> dict:
    """S&P 500 index change scanner. Checks EDGAR + Wikipedia for additions/deletions.

    Runs with --check-now to force check regardless of announcement window.
    Off-cycle additions happen anytime — must check daily.
    """
    label = "S&P 500 Changes"
    cmd = [
        sys.executable, "tools/sp500_change_scanner.py",
        "--check-now",
        "--days", str(days),
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "new_changes": 0, "changes": []}

    # Scanner prints log lines, not JSON — parse new announcement count
    new_count = 0
    changes = []
    for line in stdout.splitlines():
        if "NEW potential S&P 500 change" in line:
            try:
                new_count = int(line.split("ALERT:")[1].split("NEW")[0].strip())
            except (ValueError, IndexError):
                new_count = 1
        # Parse individual change lines: "  April 9, 2026: +CASY / -HOLX"
        if line.strip().startswith("+") or (": +" in line and "/ -" in line):
            changes.append(line.strip())

    return {
        "scanner": label,
        "status": "ok",
        "new_changes": new_count,
        "changes": changes,
    }


# ---------------------------------------------------------------------------
# Actionable event detection
# ---------------------------------------------------------------------------

def find_actionable_events(results: dict) -> list[dict]:
    """Collect events that may trigger an active hypothesis.

    Returns a list of dicts with {scanner, ticker, date, decision, note}.
    """
    actionable = []

    # Intraday insider poller — highest priority, same-day entry
    poller = results.get("intraday_poller", {})
    for c in poller.get("intraday_go_clusters", []):
        actionable.append({
            "scanner": "Intraday Insider Poller",
            "ticker": c.get("ticker", "?"),
            "date": c.get("latest_accept_time", ""),
            "decision": "INTRADAY_GO",
            "note": f"Same-day filing, validated intraday cadence. {c.get('n_insiders', '?')} insiders, ${c.get('total_value', 0):,.0f}",
        })

    # Insider GO/WEAK_GO clusters
    insider = results.get("insider_clusters", {})
    for ev in insider.get("evaluated", []):
        decision = ev.get("decision", "")
        if decision in ("GO", "WEAK_GO"):
            actionable.append({
                "scanner": "Insider Clusters",
                "ticker": ev.get("ticker", ev.get("symbol", "?")),
                "date": ev.get("last_date", ev.get("date", "")),
                "decision": decision,
                "note": ev.get("reason", ""),
            })

    # Activist 13D GO candidates
    activist = results.get("activist_13d", {})
    for c in activist.get("candidates", []):
        if c.get("decision") == "GO":
            actionable.append({
                "scanner": "Activist 13D",
                "ticker": c.get("target", "?"),
                "date": c.get("file_date", ""),
                "decision": "GO",
                "note": c.get("reason", f"Tier {c.get('tier','?')} activist: {c.get('activist','')}"),
            })

    # SEO GO candidates
    seo = results.get("seo_bought_deal", {})
    for c in seo.get("candidates", []):
        if (c.get("decision") or "").startswith("GO"):
            actionable.append({
                "scanner": "SEO Bought Deal",
                "ticker": c.get("ticker", "?"),
                "date": c.get("filing_date_424b4", ""),
                "decision": c.get("decision", "GO"),
                "note": f"${c.get('market_cap', 0)/1e6:.0f}M cap, short candidate",
            })

    # NT 10-K events (always negative signal — worth flagging)
    nt = results.get("nt_10k", {})
    for ev in nt.get("events", []):
        actionable.append({
            "scanner": "NT 10-K",
            "ticker": ev.get("symbol", "?"),
            "date": ev.get("date", ""),
            "decision": "SIGNAL",
            "note": "NT filing (late 10-K/10-Q) — short signal",
        })

    # Cybersecurity 8-K events
    cyber = results.get("cybersecurity_8k", {})
    for ev in cyber.get("events", []):
        actionable.append({
            "scanner": "Cybersecurity 8-K",
            "ticker": ev.get("symbol", "?"),
            "date": ev.get("date", ""),
            "decision": "SIGNAL",
            "note": "Item 1.05 material cybersecurity incident — short signal",
        })

    # Delisting 8-K events
    delisting = results.get("delisting_8k", {})
    for ev in delisting.get("events", []):
        actionable.append({
            "scanner": "Delisting 8-K",
            "ticker": ev.get("symbol", "?"),
            "date": ev.get("date", ""),
            "decision": "SIGNAL",
            "note": "Item 3.01 delisting notice — short signal",
        })

    # S&P 500 index changes (additions are long signals)
    sp500 = results.get("sp500_changes", {})
    if sp500.get("new_changes", 0) > 0:
        for change_line in sp500.get("changes", []):
            actionable.append({
                "scanner": "S&P 500 Changes",
                "ticker": change_line,
                "date": "",
                "decision": "SIGNAL",
                "note": "S&P 500 index change — long signal for additions",
            })

    return actionable


# ---------------------------------------------------------------------------
# Summary line builder
# ---------------------------------------------------------------------------

def build_status_line(results: dict) -> str:
    """Build brief status line: 'NT 10-K: N | 13D: N (N GO) | ...'"""
    # Intraday poller
    poller = results.get("intraday_poller", {})
    poller_go = poller.get("intraday_go", 0)
    poller_total = poller.get("new_clusters_found", 0)

    nt_n = results.get("nt_10k", {}).get("events_found", "ERR")
    act_n = results.get("activist_13d", {}).get("total_found", "ERR")
    act_go = results.get("activist_13d", {}).get("go_count", "")
    ins_n = results.get("insider_clusters", {}).get("clusters_found", "ERR")
    ins_go = results.get("insider_clusters", {}).get("go_count", 0)
    ins_wgo = results.get("insider_clusters", {}).get("weak_go_count", 0)
    cyber_n = results.get("cybersecurity_8k", {}).get("events_found", "ERR")
    delist_n = results.get("delisting_8k", {}).get("events_found", "ERR")
    seo_n = results.get("seo_bought_deal", {}).get("total_found", "ERR")
    seo_go = results.get("seo_bought_deal", {}).get("go_count", "")
    sp500_n = results.get("sp500_changes", {}).get("new_changes", 0)

    act_part = f"{act_n}" if act_go == "" else f"{act_n} ({act_go} GO)"
    ins_part = f"{ins_n} clusters ({ins_go} GO, {ins_wgo} WEAK_GO)"
    seo_part = f"{seo_n}" if seo_go == "" else f"{seo_n} ({seo_go} GO)"
    sp500_part = f"{sp500_n} new" if sp500_n else "0"

    parts = []
    # Only show intraday poller if it found something
    if poller_go > 0:
        parts.append(f"⚡ INTRADAY GO: {poller_go}")
    elif poller_total > 0:
        parts.append(f"Intraday: {poller_total} new (0 GO)")

    parts.extend([
        f"NT 10-K: {nt_n} events",
        f"13D: {act_part} filings",
        f"Insider: {ins_part}",
        f"8-K 1.05: {cyber_n} events",
        f"8-K 3.01: {delist_n} events",
        f"SEO: {seo_part} deals",
        f"S&P 500: {sp500_part}",
    ])

    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Unified daily scanner — all scanners in one command")
    parser.add_argument(
        "--days", type=int, default=3,
        help="Days to look back for most scanners (default 3; insiders use 2x)",
    )
    parser.add_argument(
        "--insider-days", type=int, default=None,
        help="Override days for insider scanner specifically (default: 2x --days, min 7)",
    )
    args = parser.parse_args()

    days = args.days
    if args.insider_days is not None:
        insider_days = args.insider_days
    else:
        insider_days = max(7, days * 2)

    scan_time = datetime.now().isoformat()
    print(f"[daily_scanner] Starting at {scan_time} | days={days}, insider_days={insider_days}", file=sys.stderr)

    # Run each scanner — order matters for rate-limiting; failures are non-fatal
    results: dict = {}

    # Intraday poller runs FIRST — catches same-day tradeable insider clusters
    print("[daily_scanner] Running Intraday Insider Poller...", file=sys.stderr)
    results["intraday_poller"] = run_intraday_insider_poller()

    print("[daily_scanner] Running NT 10-K scanner...", file=sys.stderr)
    results["nt_10k"] = run_nt_10k(days)

    print("[daily_scanner] Running Activist 13D scanner...", file=sys.stderr)
    results["activist_13d"] = run_activist_13d(days)

    print("[daily_scanner] Running Insider cluster scanner...", file=sys.stderr)
    results["insider_clusters"] = run_insider_evaluate(insider_days)

    print("[daily_scanner] Running Cybersecurity 8-K scanner...", file=sys.stderr)
    results["cybersecurity_8k"] = run_cybersecurity_8k(days)

    print("[daily_scanner] Running Delisting 8-K scanner...", file=sys.stderr)
    results["delisting_8k"] = run_delisting_8k(days)

    print("[daily_scanner] Running SEO bought-deal scanner...", file=sys.stderr)
    results["seo_bought_deal"] = run_seo_scanner(days)

    print("[daily_scanner] Running S&P 500 changes scanner...", file=sys.stderr)
    results["sp500_changes"] = run_sp500_changes(days)

    # Aggregate
    total_events = (
        results["nt_10k"].get("events_found", 0)
        + results["activist_13d"].get("total_found", 0)
        + results["insider_clusters"].get("clusters_found", 0)
        + results["cybersecurity_8k"].get("events_found", 0)
        + results["delisting_8k"].get("events_found", 0)
        + results["seo_bought_deal"].get("total_found", 0)
        + results["sp500_changes"].get("new_changes", 0)
    )

    actionable = find_actionable_events(results)
    status_line = build_status_line(results)

    # Final output
    output = {
        "scan_time": scan_time,
        "days": days,
        "insider_days": insider_days,
        "total_events": total_events,
        "total_actionable": len(actionable),
        "status_line": status_line,
        "actionable_events": actionable,
        "scanners": results,
    }

    # Machine-readable JSON to stdout
    print(json.dumps(output, indent=2, default=str))

    # Human-readable summary to stderr
    print(f"\n[daily_scanner] DONE: {status_line}", file=sys.stderr)
    if actionable:
        print(f"[daily_scanner] {len(actionable)} actionable event(s):", file=sys.stderr)
        for ev in actionable:
            print(f"  {ev['decision']:10s} | {ev['ticker']:8s} | {ev['scanner']} | {ev['note']}", file=sys.stderr)


if __name__ == "__main__":
    main()
