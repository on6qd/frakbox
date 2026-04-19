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
        return {"scanner": label, "status": "error", "events_found": 0, "events": [],
                "go_count": 0, "evaluated": []}

    events = _extract_json(stdout)
    if not isinstance(events, list):
        events = []

    # JSON events now include is_first_time_filer flag when --tag-first-time is used.
    # Filter: only first-time filers are tradeable (repeat filers show no signal).
    first_time = [e for e in events if e.get("is_first_time_filer", True)]
    repeat = [e for e in events if not e.get("is_first_time_filer", True)]

    # Evaluate GO/NO_GO for each first-time event and auto-queue GO events
    evaluated = evaluate_nt10k_events(first_time)
    go_events = [e for e in evaluated if e.get("decision") == "GO"]

    return {
        "scanner": label,
        "status": "ok",
        "events_found": len(first_time),
        "events": first_time,
        "repeat_filers_skipped": len(repeat),
        "go_count": len(go_events),
        "evaluated": evaluated,
    }


def evaluate_nt10k_events(events: list[dict]) -> list[dict]:
    """Evaluate NT 10-K first-time filings for GO/NO_GO.

    GO criteria:
    1. First-time filer (already filtered upstream)
    2. Large-cap >$500M (already filtered upstream)
    3. NT 10-K only (not NT 10-Q)
    4. Filing within 2 business days (alpha decays quickly)
    5. No prior >30% drawdown from 60-day peak (pre-event contamination)
    6. Not already queued/tracked in research_queue

    GO events are auto-queued into research_queue as scan_hits (priority 10).
    """
    if not events:
        return []

    from datetime import timedelta
    import numpy as np

    evaluated = []
    for ev in events:
        ticker = ev.get("symbol", "")
        file_date = ev.get("date", "")
        form_type = ev.get("form_type", "NT 10-K")

        result = {
            "ticker": ticker,
            "date": file_date,
            "form_type": form_type,
            "decision": "GO",
            "blockers": [],
            "warnings": [],
        }

        # Check: NT 10-K only (10-Q shows no signal)
        if "10-Q" in form_type:
            result["decision"] = "NO_GO"
            result["blockers"].append("NT 10-Q — no validated signal for quarterly filings")
            evaluated.append(result)
            continue

        # Check: filing recency (within 2 business days)
        try:
            from datetime import datetime as dt
            filed = dt.strptime(file_date, "%Y-%m-%d")
            now = dt.now()
            # Count business days between filing and now
            bdays = int(np.busday_count(filed.date(), now.date()))
            result["filing_age_bdays"] = bdays
            if bdays > 2:
                result["decision"] = "NO_GO"
                result["blockers"].append(
                    f"Filing is {bdays} business days old (>2bd — alpha decayed)"
                )
        except Exception:
            result["warnings"].append("Could not parse filing date for recency check")

        # Check: pre-event contamination (>30% drawdown from 60-day peak)
        # AND pre-event 30-day raw return gate: only trade when -20% to -5%
        # (per knowledge entry nt_10k_pre_event_drawdown_refinement_2026_04_16)
        if result["decision"] == "GO" and ticker:
            try:
                sys.path.insert(0, str(REPO_ROOT))
                from tools.yfinance_utils import safe_download
                end_dt = file_date
                from datetime import datetime as dt
                start_dt = (dt.strptime(file_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
                df = safe_download(ticker, start_dt, end_dt)
                if df is not None and len(df) > 10:
                    close = df["Close"].values if "Close" in df.columns else None
                    if close is not None and len(close) > 0:
                        peak = float(np.max(close[-60:] if len(close) >= 60 else close))
                        last = float(close[-1])
                        if peak > 0:
                            drawdown_pct = (last - peak) / peak * 100
                            result["drawdown_from_peak_pct"] = round(drawdown_pct, 1)
                            if drawdown_pct < -30:
                                result["decision"] = "NO_GO"
                                result["blockers"].append(
                                    f"Pre-event drawdown {drawdown_pct:.1f}% from 60d peak (>30% contamination rule)"
                                )

                        # Pre-event 30-day raw return gate (refinement 2026-04-16)
                        # Knowledge: nt_10k_pre_event_drawdown_refinement_2026_04_16
                        # - moderate (-20% to -5%): STRONGEST signal, TRADE
                        # - deep (<-20%): mean-reverts, SKIP
                        # - flat (-5% to +5%): weak, SKIP
                        # - up (>+5%): reverses OOS, SKIP
                        # Need ~21 trading days of data to look back 30 calendar days
                        if result["decision"] == "GO" and len(close) >= 22:
                            # t-21 trading days ~= 30 calendar days prior
                            price_30d_ago = float(close[-22])
                            ret_30d_pct = (last - price_30d_ago) / price_30d_ago * 100
                            result["pre_event_30d_return_pct"] = round(ret_30d_pct, 2)
                            if ret_30d_pct < -20:
                                result["decision"] = "NO_GO"
                                result["blockers"].append(
                                    f"Pre-event 30d return {ret_30d_pct:.1f}% (<-20% — deep drawdown mean-reverts OOS, see nt_10k_pre_event_drawdown_refinement_2026_04_16)"
                                )
                            elif ret_30d_pct > -5:
                                result["decision"] = "NO_GO"
                                result["blockers"].append(
                                    f"Pre-event 30d return {ret_30d_pct:.1f}% (>-5% — signal only tradeable in moderate bucket -20% to -5%)"
                                )
                            else:
                                result["pre_event_bucket"] = "moderate"
            except Exception as exc:
                result["warnings"].append(f"Drawdown/pre-event check failed: {exc}")

        # Check: not already queued in research_queue
        if result["decision"] == "GO" and ticker:
            try:
                sys.path.insert(0, str(REPO_ROOT))
                import db
                db.init_db()
                existing = db.get_db().execute(
                    "SELECT id FROM research_queue WHERE question LIKE ? AND status='pending'",
                    (f"%{ticker}%NT%10%K%",),
                ).fetchone()
                if existing:
                    result["decision"] = "ALREADY_QUEUED"
                    result["blockers"].append(f"Already queued in research_queue: {existing[0][:12]}")
            except Exception:
                pass

        # Auto-queue GO events into research_queue
        if result["decision"] == "GO":
            try:
                sys.path.insert(0, str(REPO_ROOT))
                import db
                db.init_db()
                ret_note = ""
                if "pre_event_30d_return_pct" in result:
                    ret_note = f" Pre-event 30d return: {result['pre_event_30d_return_pct']:.1f}% (moderate bucket)."
                question = (
                    f"NT 10-K AUTO-DETECTED: {ticker} filed first-time NT 10-K on {file_date}.{ret_note} "
                    f"VALIDATED SIGNAL (moderate-predraw subgroup): short for 10d hold, expected -6% abnormal return. "
                    f"Action: Clone hypothesis 3db5eb00, set trigger='next_market_open', "
                    f"expected_symbol='{ticker}', position_size=$5000, stop_loss=10%."
                )
                db.add_research_task(
                    category="scan_hit",
                    question=question,
                    priority=10,
                    reasoning=f"NT 10-K first-time filer auto-detected, passed moderate pre-event drawdown gate (-20% to -5%). Discovery 10d=-6.48% p=0.0043, OOS 10d=-5.04% (n=6).",
                )
                result["queued"] = True
                print(f"[daily_scanner] NT 10-K GO: {ticker} ({file_date}) auto-queued as P0 scan hit", file=sys.stderr)
            except Exception as exc:
                result["warnings"].append(f"Auto-queue failed: {exc}")

        evaluated.append(result)

    return evaluated


def evaluate_8k_signal_events(events: list[dict], signal_name: str,
                               hypothesis_id: str, expected_return: str,
                               hold_days: str,
                               min_market_cap_b: float | None = None) -> list[dict]:
    """Generic evaluator for 8-K signal events (cybersecurity, delisting, etc.).

    GO criteria:
    1. Large-cap (already filtered upstream to >$500M)
    2. Optional: min_market_cap_b in $B for signal-specific tier refinements
       (e.g. cybersec 8-K tightened to >$10B per cybersec_8k_megacap_only_2026_04_16)
    3. Filing within 2 business days (alpha decays)
    4. No prior >30% drawdown from 60-day peak (contamination)
    5. Not already queued in research_queue

    GO events are auto-queued into research_queue as scan_hits (priority 10).
    """
    if not events:
        return []

    from datetime import timedelta
    import numpy as np

    signal_label = signal_name.replace("_", " ").upper()
    evaluated = []

    for ev in events:
        ticker = ev.get("symbol", ev.get("ticker", ""))
        file_date = ev.get("date", ev.get("file_date", ""))

        result = {
            "ticker": ticker,
            "date": file_date,
            "decision": "GO",
            "blockers": [],
            "warnings": [],
        }

        # Check: signal-specific market cap floor (refinement gate)
        if min_market_cap_b is not None:
            mcap = ev.get("market_cap") or ev.get("market_cap_m", 0)
            # Normalize: market_cap is raw dollars, market_cap_m is in millions
            if "market_cap_m" in ev and "market_cap" not in ev:
                mcap_b = float(mcap) / 1_000.0
            else:
                mcap_b = float(mcap) / 1_000_000_000.0
            result["market_cap_b"] = round(mcap_b, 2)
            if mcap_b > 0 and mcap_b < min_market_cap_b:
                result["decision"] = "NO_GO"
                result["blockers"].append(
                    f"Market cap ${mcap_b:.1f}B < ${min_market_cap_b:.0f}B tier floor "
                    f"(signal-specific refinement — see knowledge base)"
                )
            elif mcap_b == 0:
                result["warnings"].append(
                    "Market cap unknown — cannot verify tier floor, proceeding with caution"
                )

        # Check: filing recency (within 2 business days)
        try:
            from datetime import datetime as dt
            filed = dt.strptime(file_date, "%Y-%m-%d")
            now = dt.now()
            bdays = int(np.busday_count(filed.date(), now.date()))
            result["filing_age_bdays"] = bdays
            if bdays > 2:
                result["decision"] = "NO_GO"
                result["blockers"].append(
                    f"Filing is {bdays} business days old (>2bd — alpha decayed)"
                )
        except Exception:
            result["warnings"].append("Could not parse filing date for recency check")

        # Check: pre-event contamination (>30% drawdown from 60-day peak)
        if result["decision"] == "GO" and ticker:
            try:
                sys.path.insert(0, str(REPO_ROOT))
                from tools.yfinance_utils import safe_download
                from datetime import datetime as dt
                start_dt = (dt.strptime(file_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
                df = safe_download(ticker, start_dt, file_date)
                if df is not None and len(df) > 10:
                    close = df["Close"].values if "Close" in df.columns else None
                    if close is not None and len(close) > 0:
                        peak = float(np.max(close[-60:] if len(close) >= 60 else close))
                        last = float(close[-1])
                        if peak > 0:
                            drawdown_pct = (last - peak) / peak * 100
                            result["drawdown_from_peak_pct"] = round(drawdown_pct, 1)
                            if drawdown_pct < -30:
                                result["decision"] = "NO_GO"
                                result["blockers"].append(
                                    f"Pre-event drawdown {drawdown_pct:.1f}% from 60d peak (>30% contamination rule)"
                                )
            except Exception as exc:
                result["warnings"].append(f"Drawdown check failed: {exc}")

        # Check: not already queued
        if result["decision"] == "GO" and ticker:
            try:
                sys.path.insert(0, str(REPO_ROOT))
                import db
                db.init_db()
                existing = db.get_db().execute(
                    "SELECT id FROM research_queue WHERE question LIKE ? AND status='pending'",
                    (f"%{ticker}%{signal_name.split('_')[0]}%",),
                ).fetchone()
                if existing:
                    result["decision"] = "ALREADY_QUEUED"
                    result["blockers"].append(f"Already queued: {existing[0][:12]}")
            except Exception:
                pass

        # Auto-queue GO events
        if result["decision"] == "GO":
            try:
                sys.path.insert(0, str(REPO_ROOT))
                import db
                db.init_db()
                question = (
                    f"{signal_label} AUTO-DETECTED: {ticker} on {file_date}. "
                    f"VALIDATED SIGNAL: short for {hold_days} hold, expected {expected_return} abnormal return. "
                    f"Action: Clone hypothesis {hypothesis_id}, set trigger='next_market_open', "
                    f"expected_symbol='{ticker}', position_size=$5000, stop_loss=10%."
                )
                db.add_research_task(
                    category="scan_hit",
                    question=question,
                    priority=10,
                    reasoning=f"{signal_label} auto-detected by daily scanner.",
                )
                result["queued"] = True
                print(f"[daily_scanner] {signal_label} GO: {ticker} ({file_date}) auto-queued as P0 scan hit", file=sys.stderr)
            except Exception as exc:
                result["warnings"].append(f"Auto-queue failed: {exc}")

        evaluated.append(result)

    return evaluated


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
        return {"scanner": label, "status": "error", "events_found": 0, "events": [],
                "go_count": 0, "evaluated": []}

    events = _extract_json(stdout)
    if not isinstance(events, list):
        # Fallback: parse "Final events: N" from stdout text
        n = _count_from_text(stdout, "Final events: ")
        events = []

    # Auto-evaluate and queue GO events
    # REFINEMENT (cybersec_8k_megacap_only_2026_04_16): tighten from >$500M to >$10B.
    # Mid-cap ($500M-$10B) contributes noise (mid_high tier OOS directionally wrong).
    # Mega-cap only: OOS 5d=-4.92% (86% neg), OOS 10d=-7.73% (83% neg).
    evaluated = evaluate_8k_signal_events(
        events,
        signal_name="cybersecurity_8k_item_105_short",
        hypothesis_id="8844b439",
        expected_return="-4.9%",
        hold_days="5d",
        min_market_cap_b=10.0,
    )
    go_events = [e for e in evaluated if e.get("decision") == "GO"]

    return {
        "scanner": label,
        "status": "ok",
        "events_found": len(events),
        "events": events,
        "go_count": len(go_events),
        "evaluated": evaluated,
    }


def run_delisting_8k(days: int) -> dict:
    """Delisting 8-K Item 3.01 scanner. Uses --json-events + --classify."""
    label = "Delisting 8-K (3.01)"
    cmd = [
        sys.executable, "tools/delisting_8k_scanner.py",
        "--days", str(days),
        "--json-events",
        "--classify",  # Classify filing type to filter going-private (untradeable)
    ]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "events_found": 0, "events": [],
                "go_count": 0, "evaluated": []}

    events = _extract_json(stdout)
    if not isinstance(events, list):
        events = []

    # Filter out going_private events (stock ceases trading, untradeable)
    tradeable = [e for e in events if e.get("filing_type") != "going_private"]
    going_private = [e for e in events if e.get("filing_type") == "going_private"]

    # Auto-evaluate and queue GO events
    evaluated = evaluate_8k_signal_events(
        tradeable,
        signal_name="delisting_notice_8k_301_short",
        hypothesis_id="995a7465",
        expected_return="-3.92%",
        hold_days="10d",
    )
    go_events = [e for e in evaluated if e.get("decision") == "GO"]

    return {
        "scanner": label,
        "status": "ok",
        "events_found": len(tradeable),
        "events": tradeable,
        "going_private_filtered": len(going_private),
        "go_count": len(go_events),
        "evaluated": evaluated,
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

    # Auto-queue GO candidates into research_queue
    candidates = data.get("candidates", [])
    for c in candidates:
        if (c.get("decision") or "").startswith("GO"):
            ticker = c.get("ticker", "?")
            file_date = c.get("filing_date_424b4", "")
            try:
                sys.path.insert(0, str(REPO_ROOT))
                import db
                db.init_db()
                question = (
                    f"SEO BOUGHT DEAL AUTO-DETECTED: {ticker} on {file_date}. "
                    f"VALIDATED SIGNAL: short for 3-5d hold, expected -2.5% abnormal return. "
                    f"Action: Clone hypothesis 673aaa32, set trigger='next_market_open', "
                    f"expected_symbol='{ticker}', position_size=$5000, stop_loss=10%."
                )
                # Check not already queued
                existing = db.get_db().execute(
                    "SELECT id FROM research_queue WHERE question LIKE ? AND status='pending'",
                    (f"%{ticker}%SEO%BOUGHT%DEAL%",),
                ).fetchone()
                if not existing:
                    db.add_research_task(
                        category="scan_hit",
                        question=question,
                        priority=10,
                        reasoning=f"SEO bought deal auto-detected. Signal validated: MNR OOS -3.54% (threshold -2.5%).",
                    )
                    c["queued"] = True
                    print(f"[daily_scanner] SEO GO: {ticker} ({file_date}) auto-queued as P0 scan hit", file=sys.stderr)
            except Exception as exc:
                print(f"[daily_scanner] SEO auto-queue failed: {exc}", file=sys.stderr)

    return {
        "scanner": label,
        "status": "ok",
        "total_found": data.get("total_found", 0),
        "go_count": data.get("go_count", 0),
        "candidates": candidates,
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


def run_vix_monitor() -> dict:
    """VIX first-close-above-30 monitor.

    When VIX closes above 30 (first close in a 30-day cluster window) it sets
    trigger=next_market_open on:
      - SPY parent hypothesis (b63a0168)
      - All pending vix30_* family hypotheses (sector basket)
    trade_loop.py then enforces the $10K family budget cap.
    """
    label = "VIX Monitor"
    cmd = [sys.executable, "tools/vix_monitor.py"]
    ok, stdout = _run(cmd, label)
    if not ok:
        return {"scanner": label, "status": "error", "fired": False}
    fired = "ACTIVATED:" in stdout or "activated" in stdout
    return {
        "scanner": label,
        "status": "ok",
        "fired": fired,
        "summary": stdout.splitlines()[-5:] if stdout else [],
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

    # NT 10-K events — use evaluated GO/NO_GO decisions
    nt = results.get("nt_10k", {})
    for ev in nt.get("evaluated", []):
        if ev.get("decision") == "GO":
            actionable.append({
                "scanner": "NT 10-K",
                "ticker": ev.get("ticker", "?"),
                "date": ev.get("date", ""),
                "decision": "GO",
                "note": "NT 10-K first-time filer — short signal (auto-queued)",
            })
        elif ev.get("decision") == "NO_GO":
            # Still flag as informational for the summary
            actionable.append({
                "scanner": "NT 10-K",
                "ticker": ev.get("ticker", "?"),
                "date": ev.get("date", ""),
                "decision": "NO_GO",
                "note": f"NT 10-K — {'; '.join(ev.get('blockers', ['no detail']))}",
            })

    # Cybersecurity 8-K events — use evaluated GO/NO_GO when available
    cyber = results.get("cybersecurity_8k", {})
    for ev in cyber.get("evaluated", cyber.get("events", [])):
        decision = ev.get("decision", "SIGNAL")
        if decision in ("GO", "NO_GO", "SIGNAL"):
            actionable.append({
                "scanner": "Cybersecurity 8-K",
                "ticker": ev.get("ticker", ev.get("symbol", "?")),
                "date": ev.get("date", ""),
                "decision": decision,
                "note": f"Item 1.05 — {'; '.join(ev.get('blockers', ['short signal']))}" if decision == "NO_GO"
                        else "Item 1.05 material cybersecurity incident — short signal (auto-queued)" if decision == "GO"
                        else "Item 1.05 material cybersecurity incident — short signal",
            })

    # Delisting 8-K events — use evaluated GO/NO_GO when available
    delisting = results.get("delisting_8k", {})
    for ev in delisting.get("evaluated", delisting.get("events", [])):
        decision = ev.get("decision", "SIGNAL")
        if decision in ("GO", "NO_GO", "SIGNAL"):
            actionable.append({
                "scanner": "Delisting 8-K",
                "ticker": ev.get("ticker", ev.get("symbol", "?")),
                "date": ev.get("date", ""),
                "decision": decision,
                "note": f"Item 3.01 — {'; '.join(ev.get('blockers', ['short signal']))}" if decision == "NO_GO"
                        else "Item 3.01 delisting notice — short signal (auto-queued)" if decision == "GO"
                        else "Item 3.01 delisting notice — short signal",
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
    nt_go = results.get("nt_10k", {}).get("go_count", 0)
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
        f"NT 10-K: {nt_n} events ({nt_go} GO)" if nt_go else f"NT 10-K: {nt_n} events",
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

    print("[daily_scanner] Running VIX monitor...", file=sys.stderr)
    results["vix_monitor"] = run_vix_monitor()

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

    # Update OOS observations
    oos_result = None
    try:
        print("[daily_scanner] Updating OOS observations...", file=sys.stderr)
        sys.path.insert(0, str(REPO_ROOT))
        import oos_tracker
        oos_result = oos_tracker.update_all_active()
        if oos_result.get("updated", 0) > 0 or oos_result.get("expired", 0) > 0:
            print(f"[daily_scanner] OOS: {oos_result.get('updated', 0)} updated, "
                  f"{oos_result.get('expired', 0)} expired", file=sys.stderr)
        else:
            print("[daily_scanner] OOS: no active observations", file=sys.stderr)
    except Exception as e:
        print(f"[daily_scanner] OOS update failed: {e}", file=sys.stderr)
        oos_result = {"status": "error", "error": str(e)}

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
        "oos_tracker": oos_result,
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
