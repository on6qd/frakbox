"""
Research engine — manages the full hypothesis lifecycle with statistical rigor.

A hypothesis requires:
  - Clear causal mechanism (why should A cause B?)
  - Historical evidence (N instances, consistency, effect size)
  - Confounders identified
  - Out-of-sample validation plan
  - Minimum sample size before we call it a pattern
"""

import json
import os
import tempfile
import uuid
from datetime import datetime, timedelta

HYPOTHESES_FILE = os.path.join(os.path.dirname(__file__), "hypotheses.json")
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "results.jsonl")
PATTERNS_FILE = os.path.join(os.path.dirname(__file__), "patterns.json")
KNOWLEDGE_FILE = os.path.join(os.path.dirname(__file__), "knowledge_base.json")


def load_hypotheses():
    if not os.path.exists(HYPOTHESES_FILE):
        return []
    with open(HYPOTHESES_FILE) as f:
        return json.load(f)


def save_hypotheses(hypotheses):
    _atomic_write(HYPOTHESES_FILE, hypotheses)


def load_patterns():
    """Load validated patterns — the growing knowledge base."""
    if not os.path.exists(PATTERNS_FILE):
        return []
    with open(PATTERNS_FILE) as f:
        return json.load(f)


def save_patterns(patterns):
    _atomic_write(PATTERNS_FILE, patterns)


def create_hypothesis(
    event_type,
    event_description,
    causal_mechanism,
    expected_symbol,
    expected_direction,
    expected_magnitude_pct,
    expected_timeframe_days,
    historical_evidence,
    sample_size,
    consistency_pct,
    confounders,
    market_regime_note,
    confidence,
    literature_reference=None,
    out_of_sample_plan=None,
    survivorship_bias_note=None,
    selection_bias_note=None,
):
    """
    Create a new hypothesis with full research backing.

    Args:
        event_type: Category (e.g., "earnings_surprise", "fda_decision")
        event_description: The specific current event triggering this test
        causal_mechanism: WHY this should work — the explanatory chain
        expected_symbol: Stock/ETF to trade
        expected_direction: "long" or "short"
        expected_magnitude_pct: Expected move in percent
        expected_timeframe_days: Days for the move to play out
        historical_evidence: List of past instances with dates and outcomes
        sample_size: Number of historical instances studied
        consistency_pct: What % of historical instances showed the expected effect
        confounders: Known confounding variables as a dict:
            {
                "market_trend": "bull/bear/flat — SPY direction over past month",
                "vix_level": float — VIX at time of hypothesis,
                "sector_trend": "sector ETF direction over past month",
                "sector_etf": "XLV/XLF/etc — which ETF was used for sector adjustment",
                "other": ["list of other potential confounders identified"]
            }
        market_regime_note: Current market context that could affect the outcome
        confidence: 1-10 confidence score with justification
        literature_reference: Academic or established research supporting this
        out_of_sample_plan: How we validated this isn't just in-sample overfitting
    """
    hypotheses = load_hypotheses()

    hypothesis = {
        "id": uuid.uuid4().hex[:8],
        "created": datetime.now().isoformat(),
        "status": "pending",  # pending -> active -> completed | invalidated

        # The thesis
        "event_type": event_type,
        "event_description": event_description,
        "causal_mechanism": causal_mechanism,
        "expected_symbol": expected_symbol,
        "expected_direction": expected_direction,
        "expected_magnitude_pct": expected_magnitude_pct,
        "expected_timeframe_days": expected_timeframe_days,

        # Research backing
        "historical_evidence": historical_evidence,
        "sample_size": sample_size,
        "consistency_pct": consistency_pct,
        "confounders": confounders,
        "market_regime_note": market_regime_note,
        "confidence": confidence,
        "literature_reference": literature_reference,
        "out_of_sample_plan": out_of_sample_plan,
        "survivorship_bias_note": survivorship_bias_note,
        "selection_bias_note": selection_bias_note,

        # Filled when trade is placed
        "trade": None,
        # Filled when experiment concludes
        "result": None,
    }

    hypotheses.append(hypothesis)
    save_hypotheses(hypotheses)
    return hypothesis


def activate_hypothesis(hypothesis_id, entry_price, position_size, order_id=None,
                        spy_price=None, vix_level=None, sector_etf_price=None):
    """Mark a hypothesis as active (trade placed). Records market context at entry.
    Enforces max_concurrent_experiments from methodology.json."""
    from self_review import load_methodology
    m = load_methodology()
    max_concurrent = m["defaults"].get("max_concurrent_experiments", 5)

    hypotheses = load_hypotheses()
    active_count = sum(1 for h in hypotheses if h["status"] == "active")
    if active_count >= max_concurrent:
        raise ValueError(
            f"Cannot activate: {active_count} active experiments already "
            f"(max {max_concurrent}). Close or invalidate existing experiments first."
        )
    for h in hypotheses:
        if h["id"] == hypothesis_id:
            h["status"] = "active"
            h["trade"] = {
                "entry_price": entry_price,
                "position_size": position_size,
                "entry_time": datetime.now().isoformat(),
                "order_id": order_id,
                "deadline": (datetime.now() + timedelta(days=h["expected_timeframe_days"])).isoformat(),
                # Market context at entry — needed for computing abnormal returns at exit
                "spy_at_entry": spy_price,
                "vix_at_entry": vix_level,
                "sector_etf_at_entry": sector_etf_price,
            }
            break
    save_hypotheses(hypotheses)


def complete_hypothesis(hypothesis_id, exit_price, actual_return_pct, post_mortem,
                        spy_return_pct=None, sector_etf_return_pct=None):
    """
    Record the outcome of a hypothesis test.

    IMPORTANT: actual_return_pct is the RAW return. We compute abnormal return here
    by subtracting what SPY did over the same period. This is the only way to know
    if the event actually caused the price move vs the whole market moving.

    post_mortem should contain:
        - What actually happened vs what was expected
        - Were confounders present?
        - Did the causal mechanism hold, or did something else explain the move?
        - What did we learn?
        - Should we update the pattern or discard it?
    """
    hypotheses = load_hypotheses()
    for h in hypotheses:
        if h["id"] == hypothesis_id:
            h["status"] = "completed"
            expected_return = h["expected_magnitude_pct"] if h["expected_direction"] == "long" else -h["expected_magnitude_pct"]

            # Abnormal return = stock return - market return
            abnormal_return = actual_return_pct - (spy_return_pct or 0)
            sector_adj_return = actual_return_pct - (sector_etf_return_pct or 0) if sector_etf_return_pct is not None else None

            # Judge correctness on abnormal return, not raw
            direction_correct = (abnormal_return > 0) == (h["expected_direction"] == "long")

            h["result"] = {
                "exit_price": exit_price,
                "exit_time": datetime.now().isoformat(),
                "raw_return_pct": actual_return_pct,
                "spy_return_pct": spy_return_pct,
                "abnormal_return_pct": round(abnormal_return, 2),
                "sector_adj_return_pct": round(sector_adj_return, 2) if sector_adj_return is not None else None,
                "expected_return_pct": expected_return,
                "direction_correct": direction_correct,
                "magnitude_ratio": abs(abnormal_return / expected_return) if expected_return != 0 else None,
                "post_mortem": post_mortem,
            }
            log_result(h)
            _update_pattern(h)
            break
    save_hypotheses(hypotheses)


def invalidate_hypothesis(hypothesis_id, reason):
    """Mark a hypothesis as invalidated (conditions changed before testing)."""
    hypotheses = load_hypotheses()
    for h in hypotheses:
        if h["id"] == hypothesis_id:
            h["status"] = "invalidated"
            h["result"] = {"reason": reason, "time": datetime.now().isoformat()}
            break
    save_hypotheses(hypotheses)


def _update_pattern(completed_hypothesis):
    """Update the pattern library with results from a completed experiment."""
    patterns = load_patterns()
    h = completed_hypothesis
    event_type = h["event_type"]

    # Find or create pattern entry for this event type
    pattern = None
    for p in patterns:
        if p["event_type"] == event_type:
            pattern = p
            break

    if pattern is None:
        pattern = {
            "event_type": event_type,
            "experiments": [],
            "total_tests": 0,
            "direction_correct_count": 0,
            "avg_expected_magnitude": 0,
            "avg_actual_magnitude": 0,
            "reliability_score": None,
            "notes": "",
            "last_updated": datetime.now().isoformat(),
        }
        patterns.append(pattern)

    pattern["experiments"].append({
        "hypothesis_id": h["id"],
        "symbol": h["expected_symbol"],
        "direction_correct": h["result"]["direction_correct"],
        "expected_pct": h["expected_magnitude_pct"],
        "actual_pct": h["result"]["abnormal_return_pct"],
        "date": h["result"]["exit_time"],
    })
    pattern["total_tests"] += 1
    pattern["direction_correct_count"] += 1 if h["result"]["direction_correct"] else 0
    pattern["last_updated"] = datetime.now().isoformat()

    # Recalculate averages
    exps = pattern["experiments"]
    pattern["avg_expected_magnitude"] = sum(e["expected_pct"] for e in exps) / len(exps)
    pattern["avg_actual_magnitude"] = sum(abs(e["actual_pct"]) for e in exps) / len(exps)
    pattern["reliability_score"] = round(pattern["direction_correct_count"] / pattern["total_tests"], 2)

    save_patterns(patterns)


def get_active_hypotheses():
    return [h for h in load_hypotheses() if h["status"] == "active"]


def get_pending_hypotheses():
    return [h for h in load_hypotheses() if h["status"] == "pending"]


def get_completed_hypotheses():
    return [h for h in load_hypotheses() if h["status"] == "completed"]


def log_result(hypothesis):
    """Append a completed hypothesis to the results log."""
    r = hypothesis["result"]
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "timestamp": datetime.now().isoformat(),
            "id": hypothesis["id"],
            "event_type": hypothesis["event_type"],
            "symbol": hypothesis["expected_symbol"],
            "direction": hypothesis["expected_direction"],
            "expected_pct": hypothesis["expected_magnitude_pct"],
            "raw_return_pct": r.get("raw_return_pct"),
            "abnormal_return_pct": r.get("abnormal_return_pct"),
            "sector_adj_return_pct": r.get("sector_adj_return_pct"),
            "spy_return_pct": r.get("spy_return_pct"),
            "direction_correct": r["direction_correct"],
            "confidence": hypothesis["confidence"],
            "sample_size": hypothesis["sample_size"],
            "consistency_pct": hypothesis["consistency_pct"],
            "post_mortem": r["post_mortem"],
        }) + "\n")


def get_research_summary():
    """Summarize the state of all research."""
    hypotheses = load_hypotheses()
    completed = [h for h in hypotheses if h["status"] == "completed"]
    active = [h for h in hypotheses if h["status"] == "active"]
    pending = [h for h in hypotheses if h["status"] == "pending"]
    patterns = load_patterns()

    direction_correct = sum(1 for h in completed if h["result"]["direction_correct"]) if completed else 0

    # Find best and worst performing patterns
    reliable_patterns = [p for p in patterns if p["total_tests"] >= 3]
    reliable_patterns.sort(key=lambda p: p["reliability_score"], reverse=True)

    return {
        "total_hypotheses": len(hypotheses),
        "pending": len(pending),
        "active": len(active),
        "completed": len(completed),
        "invalidated": sum(1 for h in hypotheses if h["status"] == "invalidated"),
        "direction_accuracy": f"{direction_correct}/{len(completed)}" if completed else "n/a",
        "patterns_discovered": len(patterns),
        "reliable_patterns": [
            {"event_type": p["event_type"], "reliability": p["reliability_score"], "tests": p["total_tests"]}
            for p in reliable_patterns[:5]
        ],
        "by_event_type": _group_accuracy_by_type(completed),
    }


def _group_accuracy_by_type(completed):
    types = {}
    for h in completed:
        t = h["event_type"]
        if t not in types:
            types[t] = {"total": 0, "correct": 0, "avg_confidence": []}
        types[t]["total"] += 1
        types[t]["avg_confidence"].append(h["confidence"])
        if h["result"]["direction_correct"]:
            types[t]["correct"] += 1
    return {
        t: {
            "accuracy": f"{v['correct']}/{v['total']}",
            "avg_confidence": round(sum(v['avg_confidence']) / len(v['avg_confidence']), 1),
        }
        for t, v in types.items()
    }


# --- Knowledge Base ---

def load_knowledge():
    """Load the knowledge base."""
    if not os.path.exists(KNOWLEDGE_FILE):
        return {"literature": {}, "known_effects": {}, "dead_ends": []}
    with open(KNOWLEDGE_FILE) as f:
        return json.load(f)


def save_knowledge(kb):
    _atomic_write(KNOWLEDGE_FILE, kb)


def _atomic_write(filepath, data):
    """Write JSON atomically: write to temp file then rename."""
    dir_name = os.path.dirname(filepath)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, filepath)
    except Exception:
        os.unlink(tmp_path)
        raise


def record_literature(event_type, findings):
    """
    Store literature review findings for an event type.

    Args:
        event_type: e.g., "earnings_surprise", "fda_decision"
        findings: dict with keys:
            - summary: what academic/established research says
            - known_magnitude: established effect size if known
            - known_timeframe: established timeframe if known
            - sources: list of paper/article references
            - gaps: what isn't known yet (what we can research)
    """
    kb = load_knowledge()
    kb["literature"][event_type] = {
        **findings,
        "recorded": datetime.now().isoformat(),
    }
    save_knowledge(kb)


def record_known_effect(event_type, effect):
    """
    Record a validated causal effect (from our own experiments or strong literature).

    Args:
        effect: dict with keys:
            - description: plain English description
            - direction: "positive" or "negative"
            - avg_magnitude_pct: average effect size
            - timeframe_days: typical timeframe
            - sample_size: how many instances support this
            - reliability: fraction (0-1) of times the effect appeared
            - our_tests: number of our own experiments confirming this
            - status: "strong", "moderate", "weak", "disproven"
    """
    kb = load_knowledge()
    kb["known_effects"][event_type] = {
        **effect,
        "last_updated": datetime.now().isoformat(),
    }
    save_knowledge(kb)


def record_dead_end(event_type, reason):
    """Record a research direction that didn't pan out, so we don't revisit it."""
    kb = load_knowledge()
    kb["dead_ends"].append({
        "event_type": event_type,
        "reason": reason,
        "recorded": datetime.now().isoformat(),
    })
    save_knowledge(kb)
