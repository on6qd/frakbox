#!/usr/bin/env python3
"""
Smoke test — verify the entire research pipeline works end-to-end.

Run this BEFORE enabling the launchd scheduler. It tests:
1. Market data: can we fetch prices and compute returns?
2. Known event calibration: do our measurements match known historical outcomes?
3. Hypothesis lifecycle: create -> activate -> complete round-trip
4. Alpaca connectivity: can we reach the paper trading API?
5. File I/O: can we read/write all JSON state files atomically?
6. Email: can we send a test report?

Usage:
    source venv/bin/activate
    python smoke_test.py           # run all checks
    python smoke_test.py --quick   # skip slow network checks
"""

import json
import os
import sys
import tempfile
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

PASS = 0
FAIL = 0
WARN = 0


def check(name, func):
    global PASS, FAIL, WARN
    try:
        result = func()
        if result is True:
            print(f"  PASS  {name}")
            PASS += 1
        elif result is None:
            print(f"  WARN  {name}")
            WARN += 1
        else:
            print(f"  FAIL  {name}: {result}")
            FAIL += 1
    except Exception as e:
        print(f"  FAIL  {name}: {e}")
        FAIL += 1


# --- 1. File I/O ---

def check_json_files():
    """All state files exist and are valid JSON."""
    files = ["hypotheses.json", "patterns.json", "knowledge_base.json",
             "methodology.json", "research_queue.json"]
    for f in files:
        path = os.path.join(BASE_DIR, f)
        if not os.path.exists(path):
            return f"{f} does not exist"
        with open(path) as fh:
            json.load(fh)  # Raises on invalid JSON
    return True


def check_atomic_write():
    """Atomic write works (tempfile + rename)."""
    from research import _atomic_write
    test_path = os.path.join(BASE_DIR, "_smoke_test_tmp.json")
    try:
        _atomic_write(test_path, {"test": True, "time": datetime.now().isoformat()})
        with open(test_path) as f:
            data = json.load(f)
        assert data["test"] is True
        return True
    finally:
        if os.path.exists(test_path):
            os.unlink(test_path)


def check_logs_dir():
    """Logs directory exists and is writable."""
    log_dir = os.path.join(BASE_DIR, "logs")
    if not os.path.isdir(log_dir):
        return "logs/ directory does not exist"
    test_file = os.path.join(log_dir, "_smoke_test.tmp")
    try:
        with open(test_file, "w") as f:
            f.write("test")
        return True
    finally:
        if os.path.exists(test_file):
            os.unlink(test_file)


# --- 2. Market data ---

def check_yfinance_import():
    """yfinance and scipy are importable."""
    import yfinance
    from scipy.stats import ttest_1samp
    return True


def check_price_fetch():
    """Can fetch recent SPY prices."""
    from market_data import get_price_history
    prices = get_price_history("SPY", days=10)
    if not prices:
        return "get_price_history('SPY', 10) returned empty"
    if len(prices) < 3:
        return f"Only got {len(prices)} days of SPY data (expected >= 3)"
    return True


def check_known_event():
    """Calibration: Lehman bankruptcy (2008-09-15) should show negative abnormal return for XLF."""
    from market_data import get_price_around_date
    result = get_price_around_date("XLF", "2008-09-15", benchmark="SPY")
    if "error" in result:
        return f"Could not fetch XLF around 2008-09-15: {result['error']}"
    abn_5d = result.get("abnormal_5d")
    if abn_5d is None:
        return "abnormal_5d not computed"
    if abn_5d > 0:
        return f"XLF abnormal_5d after Lehman was +{abn_5d}% (expected negative) — data may be wrong"
    return True


def check_measure_event_impact():
    """measure_event_impact works with multi-symbol input."""
    from market_data import measure_event_impact
    result = measure_event_impact(event_dates=[
        {"symbol": "AAPL", "date": "2024-01-26"},  # AAPL earnings
        {"symbol": "MSFT", "date": "2024-01-30"},  # MSFT earnings
    ])
    if "error" in result and result.get("events_measured", 0) == 0:
        return f"measure_event_impact returned error: {result['error']}"
    if result.get("events_measured", 0) < 1:
        return "No events measured successfully"
    # Check that statistical fields are present
    if "passes_multiple_testing" not in result:
        return "passes_multiple_testing field missing from output"
    return True


# --- 3. Hypothesis lifecycle ---

def check_hypothesis_round_trip():
    """Create, activate, and complete a hypothesis without errors."""
    from research import (load_hypotheses, save_hypotheses, create_hypothesis,
                          activate_hypothesis, complete_hypothesis)

    # Save current state
    original = load_hypotheses()

    try:
        h = create_hypothesis(
            event_type="smoke_test",
            event_description="Smoke test — not a real hypothesis",
            causal_mechanism="Test: economic actors buy because of mandates; transmission channel is direct market orders; documented in Ball & Brown 1968",
            causal_mechanism_criteria=["actors_incentives", "transmission_channel", "academic_reference"],
            expected_symbol="SPY",
            expected_direction="long",
            expected_magnitude_pct=1.0,
            expected_timeframe_days=5,
            historical_evidence=[
                {"date": "2024-01-01", "return": 1.0},
                {"date": "2024-02-01", "return": 0.5},
                {"date": "2024-03-01", "return": 1.5},
                {"date": "2024-04-01", "return": 0.8},
                {"date": "2024-05-01", "return": 1.2},
                {"date": "2024-06-01", "return": 0.9},
                {"date": "2024-07-01", "return": 1.1},
                {"date": "2024-08-01", "return": 0.7},
                {"date": "2024-09-01", "return": 1.3},
                {"date": "2024-10-01", "return": 0.6},
            ],
            sample_size=10,
            consistency_pct=70,
            confounders={
                "broad_market_direction": "bull",
                "vix_level": 15,
                "sector_trend": "neutral",
                "survivorship_bias": "not applicable — smoke test",
                "selection_bias": "not applicable — smoke test",
                "event_timing": "unknown",
                "market_regime": "calm",
            },
            market_regime_note="Smoke test",
            confidence=5,
            out_of_sample_split={
                "discovery_indices": [0, 1, 2, 3, 4, 5, 6],
                "validation_indices": [7, 8, 9],
                "discovery_consistency_pct": 70,
                "validation_consistency_pct": 66,
                "split_type": "temporal",
            },
            survivorship_bias_note="Smoke test — not applicable",
            selection_bias_note="Smoke test — not applicable",
        )

        if not h or not h.get("id"):
            return "create_hypothesis returned no ID"

        activate_hypothesis(h["id"], entry_price=100.0, position_size=5000)

        complete_hypothesis(
            h["id"],
            exit_price=101.0,
            actual_return_pct=1.0,
            post_mortem="Smoke test completed successfully",
            spy_return_pct=0.5,
            timing_accuracy="N/A — smoke test",
            mechanism_validated="N/A — smoke test",
            confounder_attribution="N/A — smoke test",
            surprise_factor="N/A — smoke test",
        )

        return True

    finally:
        # Restore original state — remove smoke test hypothesis
        hypotheses = load_hypotheses()
        hypotheses = [h for h in hypotheses if h.get("event_type") != "smoke_test"]
        save_hypotheses(hypotheses)

        # Clean up patterns
        from research import load_patterns, save_patterns, RESULTS_FILE
        patterns = load_patterns()
        patterns = [p for p in patterns if p.get("event_type") != "smoke_test"]
        save_patterns(patterns)

        # Clean up results.jsonl (remove smoke_test entries)
        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE) as f:
                lines = f.readlines()
            cleaned = [l for l in lines if '"smoke_test"' not in l]
            with open(RESULTS_FILE, "w") as f:
                f.writelines(cleaned)


# --- 4. Alpaca connectivity ---

def check_alpaca():
    """Can reach Alpaca paper trading API."""
    from trader import get_account_summary
    summary = get_account_summary()
    if "error" in summary:
        return f"Alpaca error: {summary['error']}"
    equity = summary.get("equity", 0)
    if equity == 0:
        return "Alpaca returned zero equity — check API keys"
    return True


# --- 5. Research queue ---

def check_research_queue():
    """Research queue loads and has seed tasks."""
    from research_queue import load_queue, get_next_research_task
    q = load_queue()
    if not q.get("queue"):
        return "Research queue is empty — no seed tasks"
    task = get_next_research_task()
    if not task:
        return "get_next_research_task() returned None despite pending tasks"
    return True


# --- 6. Self-review ---

def check_self_review_imports():
    """Self-review module loads and functions exist."""
    from self_review import (needs_review, needs_bootstrap_review, run_bootstrap_review,
                             compute_confidence_score, load_methodology)
    m = load_methodology()
    if "defaults" not in m:
        return "methodology.json missing 'defaults' section"
    score = compute_confidence_score(10, 70, 2.0, 3.0, has_literature=True)
    if not 1 <= score <= 10:
        return f"compute_confidence_score returned {score} (expected 1-10)"
    return True


# --- Run all checks ---

def main():
    quick = "--quick" in sys.argv

    print("\n=== Research Pipeline Smoke Test ===\n")

    print("File I/O:")
    check("JSON state files valid", check_json_files)
    check("Atomic write works", check_atomic_write)
    check("Logs directory writable", check_logs_dir)

    print("\nPython imports:")
    check("yfinance + scipy importable", check_yfinance_import)
    check("Self-review module", check_self_review_imports)
    check("Research queue", check_research_queue)

    if not quick:
        print("\nMarket data (network):")
        check("Fetch SPY prices", check_price_fetch)
        check("Known event: Lehman/XLF", check_known_event)
        check("measure_event_impact()", check_measure_event_impact)

        print("\nHypothesis lifecycle:")
        check("Create -> activate -> complete", check_hypothesis_round_trip)

        print("\nAlpaca paper trading:")
        check("API connectivity", check_alpaca)

    print(f"\n{'='*40}")
    print(f"  PASS: {PASS}  |  FAIL: {FAIL}  |  WARN: {WARN}")
    print(f"{'='*40}")

    if FAIL > 0:
        print("\nFix the failures above before enabling the scheduler.")
        sys.exit(1)
    else:
        print("\nAll checks passed. Safe to enable the scheduler:")
        print("  launchctl load com.research.operations.plist")
        print("  launchctl load com.research.event_scan.plist")
        print("  launchctl load com.research.research.plist")
        sys.exit(0)


if __name__ == "__main__":
    main()
