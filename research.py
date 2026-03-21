"""
Research engine — manages the full hypothesis lifecycle with statistical rigor.

A hypothesis requires:
  - Clear causal mechanism (why should A cause B?)
  - Historical evidence (N instances, consistency, effect size)
  - Confounders identified
  - Out-of-sample validation plan
  - Minimum sample size before we call it a pattern
"""

import hashlib
import json
import os
import re
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


def validate_causal_mechanism(mechanism_text, criteria_met):
    """
    Check that a causal mechanism meets the rubric (at least 2 of 3 criteria).

    Args:
        mechanism_text: The causal mechanism description
        criteria_met: List of which criteria are satisfied, from:
            - "actors_incentives": identifies specific economic actors and their incentives
            - "transmission_channel": explains the transmission channel
            - "academic_reference": references an established principle or finding

    Returns:
        (valid, message) tuple
    """
    valid_criteria = {"actors_incentives", "transmission_channel", "academic_reference"}
    met = [c for c in criteria_met if c in valid_criteria]
    if len(met) >= 2:
        return True, f"Causal mechanism satisfies {len(met)}/3 criteria: {met}"
    return False, (
        f"Causal mechanism only satisfies {len(met)}/3 criteria: {met}. "
        f"Need at least 2 of: actors_incentives, transmission_channel, academic_reference. "
        f"'Stocks go up because they always do' is not a mechanism."
    )


def validate_out_of_sample(historical_evidence, discovery_cutoff_date=None,
                           discovery_indices=None, validation_indices=None):
    """
    Validate that historical evidence is properly split into discovery and validation sets.

    PREFERRED: Use temporal splits (discovery_cutoff_date) to avoid look-ahead bias.
    Train on older events, validate on newer events.

    Args:
        historical_evidence: List of dicts, each with at least a "date" field for temporal splits
        discovery_cutoff_date: "YYYY-MM-DD" — events before this = discovery, after = validation.
            If provided, discovery_indices and validation_indices are ignored.
        discovery_indices: (Legacy) Indices for pattern discovery
        validation_indices: (Legacy) Indices for validation

    Returns:
        (valid, message, split_info) tuple
    """
    total = len(historical_evidence)

    if discovery_cutoff_date:
        # Temporal split — preferred method
        discovery_idx = []
        validation_idx = []
        for i, ev in enumerate(historical_evidence):
            ev_date = ev.get("date", "")
            if not ev_date:
                return False, f"Event at index {i} has no 'date' field. Temporal splits require dates.", None
            if ev_date < discovery_cutoff_date:
                discovery_idx.append(i)
            else:
                validation_idx.append(i)
        discovery_indices = discovery_idx
        validation_indices = validation_idx
        split_type = "temporal"

    elif discovery_indices is None and validation_indices is None:
        # Auto-split: sort by date, first 70% = discovery, last 30% = validation
        dated = [(i, ev.get("date", "")) for i, ev in enumerate(historical_evidence)]
        if not all(d for _, d in dated):
            return False, "Cannot auto-split: not all events have 'date' fields.", None
        dated.sort(key=lambda x: x[1])
        cutoff = int(len(dated) * 0.7)
        discovery_indices = [i for i, _ in dated[:cutoff]]
        validation_indices = [i for i, _ in dated[cutoff:]]
        split_type = "temporal_auto"

    else:
        split_type = "index_based"

    if not validation_indices:
        return False, "No validation set provided. Must hold back at least 30% of instances.", None

    overlap = set(discovery_indices) & set(validation_indices)
    if overlap:
        return False, f"Discovery and validation sets overlap at indices {overlap}.", None

    val_pct = len(validation_indices) / total * 100

    if len(validation_indices) < 3:
        return False, (
            f"Validation set has only {len(validation_indices)} instances (need at least 3). "
            f"Total sample may be too small for proper out-of-sample testing."
        ), None

    split_info = {
        "split_type": split_type,
        "total_instances": total,
        "discovery_count": len(discovery_indices),
        "validation_count": len(validation_indices),
        "validation_pct": round(val_pct, 1),
        "discovery_indices": discovery_indices,
        "validation_indices": validation_indices,
    }

    if discovery_cutoff_date:
        split_info["cutoff_date"] = discovery_cutoff_date

    return True, (
        f"Out-of-sample split ({split_type}): {len(discovery_indices)} discovery, "
        f"{len(validation_indices)} validation ({val_pct:.0f}%)"
    ), split_info


def _compute_prediction_hash(event_type, expected_symbol, expected_direction,
                              expected_magnitude_pct, expected_timeframe_days):
    """
    Create a hash of the prediction fields for pre-registration.
    This prevents post-hoc adjustment of predictions after seeing results.
    """
    payload = json.dumps({
        "event_type": event_type,
        "expected_symbol": expected_symbol,
        "expected_direction": expected_direction,
        "expected_magnitude_pct": expected_magnitude_pct,
        "expected_timeframe_days": expected_timeframe_days,
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _compute_idempotency_key(event_type, expected_symbol, event_description):
    """
    Compute an idempotency key to prevent duplicate hypothesis creation on crash+rerun.
    """
    payload = json.dumps({
        "event_type": event_type,
        "expected_symbol": expected_symbol,
        "event_description": event_description,
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def check_related_dead_ends(category):
    """
    Check knowledge base for dead ends related to this category.

    Searches for dead ends whose event_type contains the category as a substring,
    shares a common prefix, or is semantically adjacent.

    Args:
        category: Event type being researched (e.g., "earnings_surprise")

    Returns:
        List of related dead end dicts, or empty list if none found.
    """
    kb = load_knowledge()
    dead_ends = kb.get("dead_ends", [])

    if not dead_ends:
        return []

    related = []
    # Extract the root category for prefix matching (e.g., "earnings" from "earnings_surprise")
    root = category.split("_")[0] if "_" in category else category

    for de in dead_ends:
        de_type = de.get("event_type", "")
        de_root = de_type.split("_")[0] if "_" in de_type else de_type
        # Match: same category, shared prefix, or substring match
        if (de_type == category
                or de_root == root
                or category in de_type
                or de_type in category):
            related.append(de)

    return related


def create_hypothesis(
    event_type,
    event_description,
    causal_mechanism,
    causal_mechanism_criteria,
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
    out_of_sample_split,
    survivorship_bias_note,
    selection_bias_note,
    literature_reference=None,
    event_timing="unknown",
    regime_note=None,
    passes_multiple_testing=None,
    backtest_symbols=None,
    backtest_events=None,
):
    """
    Create a new hypothesis with full research backing.

    Args:
        event_type: Category (e.g., "earnings_surprise", "fda_decision")
        event_description: The specific current event triggering this test
        causal_mechanism: WHY this should work — the explanatory chain
        causal_mechanism_criteria: List of criteria met from the rubric:
            ["actors_incentives", "transmission_channel", "academic_reference"]
            Must satisfy at least 2 of 3.
        expected_symbol: Stock/ETF to trade in the live test
        expected_direction: "long" or "short"
        expected_magnitude_pct: Expected move in percent
        expected_timeframe_days: Days for the move to play out
        historical_evidence: List of past instances with dates and outcomes
        sample_size: Number of historical instances studied
        consistency_pct: What % of historical instances showed the expected effect
        confounders: Known confounding variables as a dict. MUST include all keys
            from methodology.json confounders_tracked:
            {
                "broad_market_direction": "bull/bear/flat",
                "vix_level": float,
                "sector_trend": "description",
                "survivorship_bias": "how addressed",
                "selection_bias": "how addressed",
                "event_timing": "pre_market/intraday/after_hours/unknown",
                "market_regime": "calm/elevated/crisis",
            }
        market_regime_note: Current market context that could affect the outcome
        confidence: 1-10 confidence score from compute_confidence_score()
        out_of_sample_split: Dict with keys:
            {"discovery_indices": [...], "validation_indices": [...],
             "discovery_consistency_pct": float, "validation_consistency_pct": float,
             "split_type": "temporal"|"temporal_auto"|"index_based"}
            Pattern must hold in BOTH sets.
        survivorship_bias_note: REQUIRED. How survivorship bias was addressed.
        selection_bias_note: REQUIRED. How selection bias was addressed.
        literature_reference: Academic or established research supporting this
        event_timing: "pre_market", "intraday", "after_hours", or "unknown"
        regime_note: Whether effect is regime-dependent (from regime conditioning)
        passes_multiple_testing: Boolean from measure_event_impact() results
        backtest_symbols: List of symbols used in multi-symbol backtest
            (e.g., ["AAPL", "MSFT", "NVDA"]). Shows pattern is general, not stock-specific.
        backtest_events: List of {"symbol", "date"} dicts from the multi-symbol backtest.
            Legacy: may be an int (count) in older hypotheses. New hypotheses should use list format.
    """
    # Idempotency check FIRST: return existing hypothesis before running any validation.
    # This prevents validation errors when re-running after a crash+restart.
    idempotency_key = _compute_idempotency_key(event_type, expected_symbol, event_description)
    hypotheses = load_hypotheses()
    for existing in hypotheses:
        if existing.get("idempotency_key") == idempotency_key:
            return existing  # Already created — return existing

    # Validate symbol format (TBD is allowed for event-driven hypotheses)
    symbol_warnings = []
    if expected_symbol == "TBD":
        symbol_warnings.append(
            "Symbol is TBD — must be resolved to a real ticker before activation."
        )
    elif not expected_symbol or len(expected_symbol) > 5:
        raise ValueError(
            f"expected_symbol '{expected_symbol}' is invalid. "
            f"Use a 1-5 character ticker, or 'TBD' for event-driven hypotheses."
        )

    # Validate causal mechanism
    valid, msg = validate_causal_mechanism(causal_mechanism, causal_mechanism_criteria)
    if not valid:
        raise ValueError(f"Causal mechanism validation failed: {msg}")

    # Validate required bias notes
    if not survivorship_bias_note:
        raise ValueError("survivorship_bias_note is required. Explain how survivorship bias was addressed.")
    if not selection_bias_note:
        raise ValueError("selection_bias_note is required. Explain how selection bias was addressed.")

    # Validate out-of-sample split
    if not out_of_sample_split or not out_of_sample_split.get("validation_indices"):
        raise ValueError(
            "out_of_sample_split is required with discovery_indices and validation_indices. "
            "Split historical evidence 70/30 and verify pattern holds in both sets."
        )

    # Load methodology for validation checks
    from self_review import load_methodology
    m = load_methodology()

    # Validate expected magnitude exceeds noise floor
    min_magnitude = m.get("defaults", {}).get("min_abnormal_return_pct", 1.5)
    if expected_magnitude_pct < min_magnitude:
        raise ValueError(
            f"expected_magnitude_pct ({expected_magnitude_pct}%) is below "
            f"min_abnormal_return_pct ({min_magnitude}%). Effect too small to be actionable."
        )

    # Validate expected return covers transaction costs
    round_trip_cost = m.get("defaults", {}).get("estimated_round_trip_cost_pct", 0.1)
    min_net = m.get("defaults", {}).get("min_net_return_after_costs_pct", 1.0)
    net_return = expected_magnitude_pct - round_trip_cost
    if net_return < min_net:
        raise ValueError(
            f"Expected net return after costs ({net_return:.2f}%) is below "
            f"minimum ({min_net}%). Not economically viable."
        )

    # Validate historical evidence contains real price data (not placeholders)
    placeholder_count = 0
    measured_count = 0
    for ev in historical_evidence:
        if ev.get("note") and "placeholder" in ev["note"].lower():
            placeholder_count += 1
        elif str(ev.get("symbol", "")).startswith("LIT_REF"):
            placeholder_count += 1
        elif ev.get("abnormal_1d") is not None or ev.get("abnormal_5d") is not None:
            measured_count += 1
    min_measured = m.get("defaults", {}).get("min_sample_size_exploratory", 5)
    if placeholder_count > 0 and measured_count < min_measured:
        raise ValueError(
            f"historical_evidence has {placeholder_count} placeholder entries and only "
            f"{measured_count} measured entries (need >= {min_measured}). "
            f"Run measure_event_impact() to collect real price data before creating a hypothesis."
        )

    # Validate confounders contain all tracked fields (blocking)
    tracked = m.get("confounders_tracked", [])
    missing_confounders = [c for c in tracked if c not in confounders]
    if missing_confounders:
        raise ValueError(
            f"Missing confounders: {missing_confounders}. "
            f"All confounders from methodology.json must be recorded. "
            f"Use 'unknown' or 'not applicable' if you don't have data."
        )

    # Check for related dead ends
    dead_end_warnings = []
    related_dead_ends = check_related_dead_ends(event_type)
    if related_dead_ends:
        dead_end_warnings = [
            f"Related dead end: {de['event_type']} — {de['reason']}"
            for de in related_dead_ends
        ]

    # Warn if multiple testing correction failed
    multiple_testing_warning = None
    if passes_multiple_testing is False:
        multiple_testing_warning = (
            "WARNING: This hypothesis did not pass multiple testing correction. "
            "The statistical significance may be a false positive. Proceed with extra caution."
        )

    # Pre-registration: hash the prediction before any trade is placed
    prediction_hash = _compute_prediction_hash(
        event_type, expected_symbol, expected_direction,
        expected_magnitude_pct, expected_timeframe_days
    )

    hypothesis = {
        "id": uuid.uuid4().hex[:8],
        "created": datetime.now().isoformat(),
        "prediction_hash": prediction_hash,
        "idempotency_key": idempotency_key,
        "status": "pending",  # pending -> active -> completed | invalidated

        # The thesis
        "event_type": event_type,
        "event_description": event_description,
        "causal_mechanism": causal_mechanism,
        "causal_mechanism_criteria": causal_mechanism_criteria,
        "expected_symbol": expected_symbol,
        "expected_direction": expected_direction,
        "expected_magnitude_pct": expected_magnitude_pct,
        "expected_timeframe_days": expected_timeframe_days,
        "event_timing": event_timing,

        # Multi-symbol backtest evidence
        "backtest_symbols": backtest_symbols,
        "backtest_events": backtest_events,

        # Research backing
        "historical_evidence": historical_evidence,
        "sample_size": sample_size,
        "consistency_pct": consistency_pct,
        "out_of_sample_split": out_of_sample_split,
        "confounders": confounders,
        "market_regime_note": market_regime_note,
        "regime_note": regime_note,
        "confidence": confidence,
        "literature_reference": literature_reference,
        "survivorship_bias_note": survivorship_bias_note,
        "selection_bias_note": selection_bias_note,
        "passes_multiple_testing": passes_multiple_testing,
        "multiple_testing_warning": multiple_testing_warning,

        # Warnings from validation checks
        "confounder_warnings": None,  # Now blocking — missing confounders raise ValueError
        "symbol_warnings": symbol_warnings if symbol_warnings else None,
        "dead_end_warnings": dead_end_warnings if dead_end_warnings else None,

        # Filled when trade is placed
        "trade": None,
        # Filled when experiment concludes
        "result": None,
    }

    # Pre-registration: log the prediction to results.jsonl BEFORE any trade
    _log_pre_registration(hypothesis)

    hypotheses.append(hypothesis)
    save_hypotheses(hypotheses)
    return hypothesis


def _log_pre_registration(hypothesis):
    """Log prediction to results.jsonl at creation time for pre-registration."""
    # Idempotency: check if already logged
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if (entry.get("type") == "pre_registration"
                            and entry.get("id") == hypothesis["id"]):
                        return  # Already logged
                except (json.JSONDecodeError, KeyError):
                    continue

    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps({
            "type": "pre_registration",
            "timestamp": datetime.now().isoformat(),
            "id": hypothesis["id"],
            "prediction_hash": hypothesis["prediction_hash"],
            "event_type": hypothesis["event_type"],
            "symbol": hypothesis["expected_symbol"],
            "direction": hypothesis["expected_direction"],
            "magnitude_pct": hypothesis["expected_magnitude_pct"],
            "timeframe_days": hypothesis["expected_timeframe_days"],
            "backtest_symbols": hypothesis.get("backtest_symbols"),
        }) + "\n")


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
    found = False
    for h in hypotheses:
        if h["id"] == hypothesis_id:
            found = True
            if h["status"] != "pending":
                raise ValueError(
                    f"Cannot activate hypothesis {hypothesis_id}: status is '{h['status']}', "
                    f"expected 'pending'."
                )
            if h["expected_symbol"] == "TBD":
                raise ValueError(
                    f"Cannot activate hypothesis {hypothesis_id}: symbol is still 'TBD'. "
                    f"Update expected_symbol to a real ticker first."
                )
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
    if not found:
        raise ValueError(f"Hypothesis {hypothesis_id} not found.")
    save_hypotheses(hypotheses)


def complete_hypothesis(hypothesis_id, exit_price, actual_return_pct, post_mortem,
                        spy_return_pct=None, sector_etf_return_pct=None,
                        confounders_at_exit=None,
                        timing_accuracy=None, mechanism_validated=None,
                        confounder_attribution=None, surprise_factor=None):
    """
    Record the outcome of a hypothesis test.

    IMPORTANT: actual_return_pct is the RAW return. We compute abnormal return here
    by subtracting what SPY did over the same period. This is the only way to know
    if the event actually caused the price move vs the whole market moving.

    Args:
        confounders_at_exit: Dict of confounder values at exit time for comparison
            with entry confounders. E.g., {"vix_level": 22.5, "market_regime": "calm"}
        timing_accuracy: str — did the move happen in the expected window?
            e.g., "Move occurred in first 2 days of 5-day window" or
            "Move was delayed — didn't start until day 4"
        mechanism_validated: str — did the theorized causal channel actually operate?
            e.g., "Yes — index fund buying visible in volume data" or
            "No — move was driven by unrelated earnings revision"
        confounder_attribution: str — what % of the observed move can be attributed
            to the event vs. other factors?
            e.g., "~70% event-driven, ~30% sector momentum (XLV up 2% same period)"
        surprise_factor: str — what was the most unexpected aspect?
            e.g., "Reversal happened faster than historical average" or
            "Effect was 3x larger than backtest suggested — possible regime sensitivity"

    post_mortem should contain:
        - What actually happened vs what was expected
        - Were confounders present?
        - Did the causal mechanism hold, or did something else explain the move?
        - What did we learn?
        - Should we update the pattern or discard it?
    """
    from self_review import load_methodology
    m = load_methodology()
    min_direction_threshold = m.get("defaults", {}).get("min_direction_threshold_pct", 0.5)

    hypotheses = load_hypotheses()
    found = False
    for h in hypotheses:
        if h["id"] == hypothesis_id:
            found = True
            # Status check: only active hypotheses can be completed
            if h["status"] != "active":
                raise ValueError(
                    f"Cannot complete hypothesis {hypothesis_id}: status is '{h['status']}', "
                    f"expected 'active'. Activate the hypothesis first via activate_hypothesis()."
                )

            # Pre-registration tamper check: verify prediction hash
            expected_hash = _compute_prediction_hash(
                h["event_type"], h["expected_symbol"], h["expected_direction"],
                h["expected_magnitude_pct"], h["expected_timeframe_days"]
            )
            if h.get("prediction_hash") and expected_hash != h["prediction_hash"]:
                raise ValueError(
                    f"TAMPER DETECTED: Prediction hash mismatch for hypothesis {hypothesis_id}. "
                    f"Stored: {h['prediction_hash']}, Computed: {expected_hash}. "
                    f"Prediction fields may have been modified after pre-registration."
                )

            h["status"] = "completed"
            expected_return = h["expected_magnitude_pct"] if h["expected_direction"] == "long" else -h["expected_magnitude_pct"]

            # Abnormal return = stock return - market return
            abnormal_return = actual_return_pct - (spy_return_pct or 0)
            sector_adj_return = actual_return_pct - (sector_etf_return_pct or 0) if sector_etf_return_pct is not None else None

            # Judge correctness on abnormal return with minimum threshold
            # A +0.01% move on a +5% prediction is noise, not a correct call
            direction_matches = (abnormal_return > 0) == (h["expected_direction"] == "long")
            direction_correct = direction_matches and abs(abnormal_return) >= min_direction_threshold

            # Magnitude ratio: direction-aware (0 if wrong direction)
            if direction_correct and expected_return != 0:
                magnitude_ratio = abs(abnormal_return) / abs(expected_return)
            else:
                magnitude_ratio = 0.0

            h["result"] = {
                "exit_price": exit_price,
                "exit_time": datetime.now().isoformat(),
                "raw_return_pct": actual_return_pct,
                "spy_return_pct": spy_return_pct,
                "abnormal_return_pct": round(abnormal_return, 2),
                "sector_adj_return_pct": round(sector_adj_return, 2) if sector_adj_return is not None else None,
                "expected_return_pct": expected_return,
                "direction_correct": direction_correct,
                "direction_matches_but_below_threshold": direction_matches and not direction_correct,
                "magnitude_ratio": round(magnitude_ratio, 3) if magnitude_ratio is not None else None,
                "post_mortem": post_mortem,
                "confounders_at_exit": confounders_at_exit,
                # Structured post-mortem fields (all required for quality post-mortems)
                "timing_accuracy": timing_accuracy,
                "mechanism_validated": mechanism_validated,
                "confounder_attribution": confounder_attribution,
                "surprise_factor": surprise_factor,
            }
            log_result(h)
            _update_pattern(h)
            break
    if not found:
        raise ValueError(f"Hypothesis {hypothesis_id} not found.")
    save_hypotheses(hypotheses)


def invalidate_hypothesis(hypothesis_id, reason):
    """Mark a hypothesis as invalidated (conditions changed before testing)."""
    hypotheses = load_hypotheses()
    found = False
    for h in hypotheses:
        if h["id"] == hypothesis_id:
            found = True
            h["status"] = "invalidated"
            h["result"] = {"reason": reason, "time": datetime.now().isoformat()}
            break
    if not found:
        raise ValueError(f"Hypothesis {hypothesis_id} not found.")
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

    # Idempotency: check if already logged
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    if (entry.get("type") != "pre_registration"
                            and entry.get("id") == hypothesis["id"]):
                        return  # Already logged
                except (json.JSONDecodeError, KeyError):
                    continue

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
            "backtest_symbols": hypothesis.get("backtest_symbols"),
            "confounders_at_exit": r.get("confounders_at_exit"),
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
    # Deduplication: don't re-record the same dead end
    for existing in kb["dead_ends"]:
        if existing.get("event_type") == event_type:
            existing["reason"] = reason
            existing["updated"] = datetime.now().isoformat()
            save_knowledge(kb)
            return
    kb["dead_ends"].append({
        "event_type": event_type,
        "reason": reason,
        "recorded": datetime.now().isoformat(),
    })
    save_knowledge(kb)


def check_promotion_or_retirement(event_type):
    """
    Check if a pattern should be promoted to known_effects or retired as a dead end.

    Uses thresholds from methodology.json promotion_criteria.

    Returns:
        {"action": "promote"|"retire"|"none", "reason": str, "stats": dict}
    """
    from self_review import load_methodology
    m = load_methodology()
    criteria = m.get("promotion_criteria", {})

    min_tests = criteria.get("min_live_tests", 3)
    min_acc = criteria.get("min_live_accuracy", 0.6)
    min_mag = criteria.get("min_live_magnitude_ratio", 0.3)
    retire_tests = criteria.get("retirement_min_tests", 5)
    retire_acc = criteria.get("retirement_max_accuracy", 0.3)

    patterns = load_patterns()
    pattern = None
    for p in patterns:
        if p["event_type"] == event_type:
            pattern = p
            break

    if not pattern:
        return {"action": "none", "reason": "No pattern data yet.", "stats": None}

    total = pattern["total_tests"]
    accuracy = pattern["reliability_score"] or 0
    experiments = pattern.get("experiments", [])
    # Only count magnitude ratio for direction-correct experiments
    # For shorts, actual_pct is negative when correct — use abs() for both
    mag_ratios = []
    for e in experiments:
        if e.get("expected_pct", 0) != 0 and e.get("direction_correct", False):
            mag_ratios.append(abs(e.get("actual_pct", 0)) / abs(e["expected_pct"]))
    avg_mag_ratio = sum(mag_ratios) / len(mag_ratios) if mag_ratios else 0

    stats = {
        "total_tests": total,
        "accuracy": accuracy,
        "avg_magnitude_ratio": round(avg_mag_ratio, 2),
    }

    # Check promotion
    if total >= min_tests and accuracy >= min_acc and avg_mag_ratio >= min_mag:
        return {
            "action": "promote",
            "reason": (
                f"Pattern qualifies for promotion: {total} live tests, "
                f"{accuracy:.0%} accuracy, {avg_mag_ratio:.2f} avg magnitude ratio. "
                f"Thresholds: {min_tests} tests, {min_acc:.0%} accuracy, {min_mag} magnitude."
            ),
            "stats": stats,
        }

    # Check retirement
    if total >= retire_tests and accuracy <= retire_acc:
        return {
            "action": "retire",
            "reason": (
                f"Pattern should be retired: {total} live tests with only "
                f"{accuracy:.0%} accuracy (threshold: {retire_acc:.0%} over {retire_tests} tests)."
            ),
            "stats": stats,
        }

    return {
        "action": "none",
        "reason": f"Pattern has {total} tests, {accuracy:.0%} accuracy — needs more data.",
        "stats": stats,
    }


def verify_data_integrity():
    """
    Verify referential integrity across all data files.
    Run at session start to catch data loss (e.g., hypothesis created but not persisted).

    Returns:
        {"ok": bool, "issues": list of strings}
    """
    issues = []
    hypotheses = load_hypotheses()
    hyp_ids = {h["id"] for h in hypotheses}

    # Check research_queue.json references
    rq_path = os.path.join(os.path.dirname(__file__), "research_queue.json")
    if os.path.exists(rq_path):
        with open(rq_path) as f:
            rq = json.load(f)

        # Check session_handoff hypothesis_ids
        handoff = rq.get("session_handoff", {})
        for key, hid in handoff.get("hypothesis_ids", {}).items():
            if hid not in hyp_ids:
                issues.append(
                    f"MISSING HYPOTHESIS: research_queue.json session_handoff references "
                    f"'{hid}' ({key}) but it does not exist in hypotheses.json. "
                    f"The hypothesis was likely lost due to a session timeout. Re-create it."
                )

        # Check next_session_priorities for hypothesis ID references
        for p in rq.get("next_session_priorities", []):
            if isinstance(p, dict):
                task_raw = p.get("task", "")
                # task may be nested (a dict with 'task' key) or a plain string
                if isinstance(task_raw, dict):
                    task = task_raw.get("task", "") or str(task_raw)
                else:
                    task = str(task_raw)
            else:
                task = str(p)
            refs = re.findall(r'\b([0-9a-f]{8})\b', task)
            for ref in refs:
                if ref not in hyp_ids and ref not in {t.get("id") for t in rq.get("queue", [])}:
                    issues.append(
                        f"DANGLING REFERENCE: next_session_priorities mentions '{ref}' "
                        f"which is not a known hypothesis or task ID."
                    )

    # Check results.jsonl for orphaned pre-registrations
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    entry = json.loads(line.strip())
                    rid = entry.get("id")
                    if rid and entry.get("type") == "pre_registration" and rid not in hyp_ids:
                        issues.append(
                            f"ORPHANED PRE-REGISTRATION: results.jsonl line {line_num} "
                            f"has pre-registration for '{rid}' but hypothesis is missing."
                        )
                except json.JSONDecodeError:
                    issues.append(f"CORRUPT DATA: results.jsonl line {line_num} is not valid JSON")

    # Check active hypotheses have real symbols
    for h in hypotheses:
        if h["status"] == "active" and h["expected_symbol"] == "TBD":
            issues.append(
                f"INVALID STATE: Hypothesis {h['id']} is ACTIVE but symbol is 'TBD'. "
                f"Cannot have an active trade on an unresolved symbol."
            )

    # Check for stale active hypotheses past deadline
    for h in hypotheses:
        if h["status"] == "active":
            deadline = h.get("trade", {}).get("deadline")
            if deadline and deadline < datetime.now().isoformat():
                issues.append(
                    f"OVERDUE: Hypothesis {h['id']} ({h['event_type']}) is past deadline "
                    f"{deadline[:10]}. Complete or invalidate it."
                )

    return {"ok": len(issues) == 0, "issues": issues}
