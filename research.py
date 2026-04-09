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

import db as _db

def load_hypotheses():
    return _db.load_hypotheses()


def save_hypotheses(hypotheses):
    _db.save_hypotheses(hypotheses)


def load_patterns():
    """Load validated patterns from SQLite."""
    return _db.load_patterns()


def save_patterns(patterns):
    """Save patterns to SQLite."""
    _db.save_patterns(patterns)


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
                              expected_magnitude_pct, expected_timeframe_days,
                              hypothesis_class='event', spec_json=None):
    """
    Create a hash of the prediction fields for pre-registration.
    This prevents post-hoc adjustment of predictions after seeing results.

    For non-event classes, also hashes hypothesis_class and class-specific
    prediction fields from spec_json.
    """
    payload_dict = {
        "event_type": event_type,
        "expected_symbol": expected_symbol,
        "expected_direction": expected_direction,
        "expected_magnitude_pct": expected_magnitude_pct,
        "expected_timeframe_days": expected_timeframe_days,
    }
    if hypothesis_class != 'event' and spec_json:
        payload_dict["hypothesis_class"] = hypothesis_class
        prediction_keys = _SPEC_PREDICTION_KEYS.get(hypothesis_class, [])
        for k in prediction_keys:
            if k in spec_json:
                val = spec_json[k]
                # Normalize lists to sorted tuples for consistent hashing
                if isinstance(val, list):
                    val = sorted(str(v) for v in val)
                payload_dict[f"spec.{k}"] = val
    payload = json.dumps(payload_dict, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# Fields from spec_json that define the testable prediction (hashed for pre-registration).
# Window/config params are NOT hashed — they're tuning, not the prediction itself.
_SPEC_PREDICTION_KEYS = {
    "exposure": ["factor_series", "beta_direction"],
    "lead_lag": ["leader_series", "follower_series", "expected_direction"],
    "cointegration": ["series_a", "series_b", "entry_threshold_zscore"],
    "regime": ["target_symbol", "regime_indicator", "regimes"],
    "structural_break": ["relationship", "suspected_break_date"],
    "threshold": ["trigger_series", "threshold_value", "threshold_direction", "expected_target_direction"],
    "network": ["hub_symbol", "spoke_symbols", "expected_propagation_lag_days"],
    "calendar": ["pattern_type", "pattern_spec", "expected_direction"],
    "cross_section": ["universe", "sort_factor", "long_quintile", "short_quintile"],
}


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
    success_criteria,
    literature_reference=None,
    event_timing="unknown",
    regime_note=None,
    passes_multiple_testing=None,
    backtest_symbols=None,
    backtest_events=None,
    hypothesis_class='event',
    spec_json=None,
):
    """
    Create a new hypothesis with full research backing.

    Args:
        event_type: Category (e.g., "earnings_surprise", "fda_decision", "oil_airline_exposure")
        event_description: The specific current event triggering this test
        causal_mechanism: WHY this should work — the explanatory chain
        causal_mechanism_criteria: List of criteria met from the rubric:
            ["actors_incentives", "transmission_channel", "academic_reference"]
            Must satisfy at least 2 of 3.
        expected_symbol: Stock/ETF to trade in the live test
        expected_direction: "long" or "short"
        expected_magnitude_pct: Expected move in percent
        expected_timeframe_days: Days for the move to play out
        historical_evidence: List of past instances with dates and outcomes (for event class)
            or a list containing a task_result reference dict (for non-event classes)
        hypothesis_class: One of: event, exposure, lead_lag, cointegration, regime,
            structural_break, threshold, network, calendar, cross_section (default: 'event')
        spec_json: Class-specific specification dict (required for non-event classes).
            See CLAUDE.md for the schema per class.
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
        success_criteria: REQUIRED. Concrete thresholds that define "valid" BEFORE testing.
            e.g., "abnormal return > 2%, p < 0.05, consistent in 60%+ of instances,
            pattern holds in OOS validation set". Locked at creation — cannot be changed
            after seeing results.
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

    # --- Focus gate: limit scatter across too many signal types ---
    from config import MAX_ACTIVE_SIGNAL_TYPES
    active_types = set()
    for h in hypotheses:
        if h["status"] in ("pending", "active"):
            active_types.add(h.get("event_type"))
    if event_type not in active_types and len(active_types) >= MAX_ACTIVE_SIGNAL_TYPES:
        raise ValueError(
            f"Focus limit: {len(active_types)} signal types already under investigation "
            f"(max {MAX_ACTIVE_SIGNAL_TYPES}). Complete or retire existing signals before "
            f"starting '{event_type}'. Active types: {', '.join(sorted(active_types))}"
        )

    # --- Abandon threshold gate: prevent over-investigation of dead signals ---
    ABANDON_THRESHOLD = 3  # Retire signal type after 3 consecutive failures
    abandoned_in_type = [h for h in hypotheses
                         if h.get("event_type") == event_type
                         and h["status"] == "abandoned"]
    completed_correct_in_type = [h for h in hypotheses
                                 if h.get("event_type") == event_type
                                 and h["status"] == "completed"
                                 and isinstance(h.get("result"), dict)
                                 and h["result"].get("direction_correct")]
    if len(abandoned_in_type) >= ABANDON_THRESHOLD and not completed_correct_in_type:
        raise ValueError(
            f"Abandon threshold: {len(abandoned_in_type)} hypotheses already abandoned "
            f"for signal type '{event_type}' with 0 correct completions. "
            f"Signal type appears to be a dead end. Record it with record_dead_end() "
            f"and investigate a different signal type."
        )

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

    # Validate direction
    if expected_direction not in ("long", "short"):
        raise ValueError(
            f"expected_direction must be 'long' or 'short', got '{expected_direction}'."
        )

    # Validate timeframe
    if not isinstance(expected_timeframe_days, (int, float)) or expected_timeframe_days < 1:
        raise ValueError(
            f"expected_timeframe_days must be >= 1, got {expected_timeframe_days}."
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

    # Validate success criteria (Step 3 of investigation method)
    if not success_criteria:
        raise ValueError(
            "success_criteria is required. Define what 'valid' looks like BEFORE running the test. "
            "Use concrete thresholds (e.g., 'abnormal return > 2%, p < 0.05, consistent in 60%+ "
            "of instances, holds in OOS validation'). This prevents moving the goalposts."
        )

    # Validate out-of-sample split
    if hypothesis_class == 'event':
        # Event class: requires explicit validation_indices into historical_evidence list
        if not out_of_sample_split or not out_of_sample_split.get("validation_indices"):
            raise ValueError(
                "out_of_sample_split is required with discovery_indices and validation_indices. "
                "Split historical evidence 70/30 and verify pattern holds in both sets."
            )
        oos = out_of_sample_split or {}
        validation_indices = oos.get("validation_indices", [])
        if len(validation_indices) < 3:
            raise ValueError(
                f"out_of_sample_split.validation_indices must have >= 3 entries. "
                f"Got {len(validation_indices)}. Minimum 3 real named validation instances required."
            )
    else:
        # Non-event classes: OOS is handled by temporal split in the regression/test itself.
        # The out_of_sample_split dict should contain split_type and oos_start date.
        if not out_of_sample_split or not out_of_sample_split.get("split_type"):
            raise ValueError(
                "out_of_sample_split is required for non-event hypotheses. "
                "Provide {'split_type': 'temporal', 'oos_start': 'YYYY-MM-DD'}."
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

    # Validate historical evidence contains real data (not placeholders)
    if hypothesis_class == 'event':
        # Event class: evidence must contain measured event impacts
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
    else:
        # Non-event classes: evidence must contain at least one statistical test result
        # (identified by having test_name, p_value, or result_id fields)
        has_test_result = any(
            ev.get("test_name") or ev.get("result_id") or ev.get("p_value") is not None
            for ev in historical_evidence
        )
        if not has_test_result:
            raise ValueError(
                f"historical_evidence for {hypothesis_class} hypothesis must contain at least one "
                f"statistical test result (with test_name/result_id/p_value). "
                f"Run the appropriate data_tasks.py command first."
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
        expected_magnitude_pct, expected_timeframe_days,
        hypothesis_class=hypothesis_class, spec_json=spec_json,
    )

    hypothesis = {
        "id": uuid.uuid4().hex[:8],
        "created": datetime.now().isoformat(),
        "prediction_hash": prediction_hash,
        "idempotency_key": idempotency_key,
        "status": "pending",  # pending -> active -> completed | invalidated

        # Hypothesis class (event, exposure, lead_lag, cointegration, etc.)
        "hypothesis_class": hypothesis_class,
        "spec_json": spec_json,

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
        "success_criteria": success_criteria,
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

    # Auto-attach market context: macro + geopolitical + news themes (cached 12hr)
    try:
        from tools.market_context import get_market_context
        ctx = get_market_context(datetime.now().strftime("%Y-%m-%d"))
        hypothesis["market_context"] = {
            "narrative": ctx.get("narrative"),
            "themes": ctx.get("themes_detected"),
            "gpr": ctx.get("gpr"),
            "macro": ctx.get("macro"),
        }
        # Enrich market_regime_note with full context narrative
        if ctx.get("narrative"):
            hypothesis["market_regime_note"] = (
                f"{market_regime_note} | {ctx['narrative']}"
            )
    except Exception as e:
        import sys
        print(f"[research] market context unavailable: {e}", file=sys.stderr)

    # Pre-registration: log prediction to SQLite BEFORE any trade
    _log_pre_registration(hypothesis)

    _db.save_hypothesis(hypothesis)
    return hypothesis


def _log_pre_registration(hypothesis):
    """Log prediction to SQLite at creation time for pre-registration."""
    # Idempotency: check if already logged
    existing = _db.get_pre_registrations()
    for entry in existing:
        data = entry.get("data", {})
        if isinstance(data, dict) and data.get("id") == hypothesis["id"]:
            return  # Already logged

    _db.append_pre_registration(
        hypothesis_id=hypothesis["id"],
        prediction_hash=hypothesis["prediction_hash"],
        data={
            "type": "pre_registration",
            "id": hypothesis["id"],
            "prediction_hash": hypothesis["prediction_hash"],
            "event_type": hypothesis["event_type"],
            "symbol": hypothesis["expected_symbol"],
            "direction": hypothesis["expected_direction"],
            "magnitude_pct": hypothesis["expected_magnitude_pct"],
            "timeframe_days": hypothesis["expected_timeframe_days"],
            "backtest_symbols": hypothesis.get("backtest_symbols"),
        },
    )


def activate_hypothesis(hypothesis_id, entry_price, position_size, order_id=None,
                        spy_price=None, vix_level=None, sector_etf_price=None,
                        stop_loss_pct=None, take_profit_pct=None):
    """Mark a hypothesis as active (trade placed). Records market context at entry.
    Enforces max_concurrent_experiments from methodology.json.

    Args:
        stop_loss_pct: Max loss before auto-close (default: 10%). Cannot be disabled.
        take_profit_pct: Profit target for auto-close (default: None = hold to deadline).
    """
    from self_review import load_methodology
    from config import DEFAULT_STOP_LOSS_PCT, DEFAULT_TAKE_PROFIT_PCT, MIN_STOP_LOSS_PCT
    from trader import check_portfolio_drawdown

    m = load_methodology()
    max_concurrent = m["defaults"].get("max_concurrent_experiments", 5)

    # Portfolio drawdown check — refuse to open new trades if portfolio is in drawdown
    dd = check_portfolio_drawdown()
    if not dd.get("safe_to_trade"):
        raise ValueError(
            f"Cannot activate: portfolio drawdown {dd.get('drawdown_pct', '?')}% "
            f"exceeds limit. {dd.get('error', '')}"
        )

    hypotheses = load_hypotheses()
    active_count = sum(1 for h in hypotheses if h["status"] == "active")
    if active_count >= max_concurrent:
        raise ValueError(
            f"Cannot activate: {active_count} active experiments already "
            f"(max {max_concurrent}). Close or invalidate existing experiments first."
        )

    # Per-signal concurrency limit — correlated positions are not independent
    from config import MAX_CONCURRENT_PER_SIGNAL
    target_h = next((h for h in hypotheses if h["id"] == hypothesis_id), None)
    if target_h:
        target_type = target_h.get("event_type", "")
        signal_active = sum(1 for h in hypotheses
                           if h["status"] == "active" and h.get("event_type") == target_type)
        if signal_active >= MAX_CONCURRENT_PER_SIGNAL:
            raise ValueError(
                f"Cannot activate: {signal_active} active experiments already on "
                f"'{target_type}' (max {MAX_CONCURRENT_PER_SIGNAL}). "
                f"Correlated positions in the same time window do not provide independent data."
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
            # Enforce minimum stop loss — every trade MUST have one
            effective_stop = stop_loss_pct if stop_loss_pct is not None else DEFAULT_STOP_LOSS_PCT
            if effective_stop < MIN_STOP_LOSS_PCT:
                effective_stop = DEFAULT_STOP_LOSS_PCT

            h["status"] = "active"
            h["trade"] = {
                "entry_price": entry_price,
                "position_size": position_size,
                "entry_time": datetime.now().isoformat(),
                "order_id": order_id,
                "deadline": (datetime.now() + timedelta(days=h["expected_timeframe_days"])).isoformat(),
                # Risk controls
                "stop_loss_pct": effective_stop,
                "take_profit_pct": take_profit_pct if take_profit_pct is not None else DEFAULT_TAKE_PROFIT_PCT,
                # Market context at entry — needed for computing abnormal returns at exit
                "spy_at_entry": spy_price,
                "vix_at_entry": vix_level,
                "sector_etf_at_entry": sector_etf_price,
            }
            break
    if not found:
        raise ValueError(f"Hypothesis {hypothesis_id} not found.")
    _db.save_hypothesis(h)


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
            # Propagate key result fields to top-level for calibration/reporting
            h["outcome_correct"] = direction_correct
            h["actual_return"] = round(abnormal_return, 2)

            log_result(h)
            _update_pattern(h)

            # Auto-generate investigation report and store it
            try:
                _db.save_hypothesis(h)  # save first so report can read it
                report = generate_investigation_report(hypothesis_id)
                _db.update_hypothesis_fields(hypothesis_id, extra={
                    **(h.get("extra") or {}),
                    "investigation_report": report,
                })
            except Exception:
                _db.save_hypothesis(h)  # ensure save even if report fails

            return h
    if not found:
        raise ValueError(f"Hypothesis {hypothesis_id} not found.")
    _db.save_hypothesis(h)


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
    _db.save_hypothesis(h)


def _compute_pattern_state(pattern):
    """Derive the lifecycle state of a signal pattern.

    States:
        EXPLORING — not enough independent data to judge
        PROMISING — enough data, accuracy above threshold
        FAILING — enough data, accuracy below threshold
        VALIDATED — enough data for full validation, meets all criteria
        RETIRED — enough data to confirm signal doesn't work
    """
    from self_review import load_methodology
    m = load_methodology()
    criteria = m.get("promotion_criteria", {})
    min_tests = criteria.get("min_live_tests", 3)
    min_acc = criteria.get("min_live_accuracy", 0.6)
    min_mag = criteria.get("min_live_magnitude_ratio", 0.3)
    retire_tests = criteria.get("retirement_min_tests", 5)
    retire_acc = criteria.get("retirement_max_accuracy", 0.3)

    total = pattern.get("effective_independent_n", pattern.get("total_tests", 0))
    if total == 0:
        return "EXPLORING"

    correct = pattern.get("effective_correct_n", pattern.get("direction_correct_count", 0))
    accuracy = correct / total

    if total < min_tests:
        return "EXPLORING"

    # Enough data to judge
    if total >= retire_tests and accuracy <= retire_acc:
        return "RETIRED"

    if accuracy >= min_acc:
        # Check magnitude ratio for full validation
        exps = pattern.get("experiments", [])
        mag_ratios = [abs(e.get("actual_pct", 0)) / abs(e["expected_pct"])
                      for e in exps
                      if e.get("expected_pct", 0) != 0 and e.get("direction_correct")]
        avg_mag = sum(mag_ratios) / len(mag_ratios) if mag_ratios else 0
        if total >= retire_tests and avg_mag >= min_mag:
            return "VALIDATED"
        return "PROMISING"

    return "FAILING"


def check_revalidation_due():
    """Check which validated signals show signs of decay.

    Validated signals trade continuously. This function checks whether
    recent experiments suggest the signal is weakening — not whether
    enough time has passed. A signal that keeps winning doesn't need
    special revalidation. One that starts losing does.
    """
    from self_review import load_methodology
    m = load_methodology()
    reval_months = m.get("revalidation", {}).get("revalidation_months", m.get("revalidation_months", 6))

    patterns = load_patterns()
    due = []
    now = datetime.now()

    for event_type, pat in patterns.items():
        if pat.get("state") != "VALIDATED":
            continue

        exps = pat.get("experiments", [])
        if len(exps) < 5:
            continue  # not enough data to detect decay

        # Check last 3 experiments — if majority wrong, signal may be decaying
        recent = exps[-3:]
        recent_correct = sum(1 for e in recent if e.get("direction_correct"))
        if recent_correct <= 1:  # 0 or 1 out of 3
            due.append({
                "event_type": event_type,
                "reason": f"Last 3 experiments: {recent_correct}/3 correct — signal may be decaying",
            })

        # Also flag if no experiments in a long time (signal going stale)
        last_date = exps[-1].get("date", "") if exps else ""
        if last_date:
            try:
                last_dt = datetime.fromisoformat(last_date[:19])
                months_since = (now - last_dt).days / 30
                if months_since >= reval_months:
                    due.append({
                        "event_type": event_type,
                        "reason": f"No live test in {months_since:.0f} months",
                    })
            except (ValueError, TypeError):
                pass

    # Queue research tasks for decaying signals
    for item in due:
        all_h = load_hypotheses()
        pipeline = [h for h in all_h
                    if h.get("event_type") == item["event_type"]
                    and h["status"] in ("pending", "active")]
        if not pipeline:
            _db.add_research_task(
                category="signal_decay_check",
                question=(
                    f"Signal '{item['event_type']}' shows signs of decay: {item['reason']}. "
                    f"Investigate whether the signal has stopped working. "
                    f"Check for regime change, arbitrage, or structural shift."
                ),
                priority=9,
                reasoning="Validated signals must be continuously monitored. Decay is caught by tracking recent performance.",
            )

    return due


def _count_independent_experiments(experiments, window_days=5):
    """Count independent time windows in a list of experiments.

    Experiments within `window_days` trading days of each other are clustered
    as one independent observation. 4 shorts in the same week = 1 data point.

    Returns dict with:
        count: number of independent time windows
        correct: number of clusters where majority was direction_correct
    """
    if not experiments:
        return {"count": 0, "correct": 0}

    # Sort by date, filter out entries without dates
    dated = [(e, e.get("date", "")) for e in experiments]
    dated = [(e, d) for e, d in dated if d]
    if not dated:
        return {"count": len(experiments), "correct": sum(1 for e in experiments if e.get("direction_correct"))}

    dated.sort(key=lambda x: x[1])

    clusters = []
    current_cluster = [dated[0][0]]
    last_date = dated[0][1][:10]

    for exp, d in dated[1:]:
        d_str = d[:10]
        try:
            delta = abs((datetime.fromisoformat(d_str) - datetime.fromisoformat(last_date)).days)
        except (ValueError, TypeError):
            delta = window_days + 1  # treat parse failures as independent
        if delta > window_days:
            clusters.append(current_cluster)
            current_cluster = [exp]
            last_date = d_str
        else:
            current_cluster.append(exp)

    clusters.append(current_cluster)

    # Each cluster's result = majority vote of direction_correct
    correct = 0
    for cluster in clusters:
        correct_count = sum(1 for e in cluster if e.get("direction_correct"))
        if correct_count > len(cluster) / 2:
            correct += 1

    return {"count": len(clusters), "correct": correct}


def _update_pattern(completed_hypothesis):
    """Update the pattern library with results from a completed experiment.

    patterns is a dict keyed by event_type (per db.load_patterns / db.save_patterns).
    """
    patterns = load_patterns()
    # Ensure patterns is a dict (db.load_patterns returns dict, not list)
    if isinstance(patterns, list):
        # Migrate list format to dict format
        patterns = {p["event_type"]: p for p in patterns}
    h = completed_hypothesis
    event_type = h["event_type"]

    # Find or create pattern entry for this event type
    if event_type not in patterns:
        patterns[event_type] = {
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

    pattern = patterns[event_type]
    # Ensure experiments is a list
    if not isinstance(pattern.get("experiments"), list):
        pattern["experiments"] = []

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

    # Compute experiment independence — correlated experiments are not independent data
    indep = _count_independent_experiments(exps)
    pattern["effective_independent_n"] = indep["count"]
    pattern["effective_correct_n"] = indep["correct"]

    # Compute pattern state
    pattern["state"] = _compute_pattern_state(pattern)

    save_patterns(patterns)

    # --- Auto-promote or retire if thresholds met ---
    try:
        result = check_promotion_or_retirement(event_type)
        if result["action"] == "promote":
            record_known_effect(event_type, {
                "description": f"Live-validated: {event_type.replace('_', ' ')}",
                "direction": h.get("expected_direction", "unknown"),
                "avg_magnitude_pct": pattern["avg_actual_magnitude"],
                "timeframe_days": h.get("expected_timeframe_days"),
                "sample_size": pattern["effective_independent_n"],
                "reliability": pattern.get("effective_correct_n", 0) / max(pattern["effective_independent_n"], 1),
                "our_tests": pattern["total_tests"],
                "status": "validated_live",
                "validated_at": datetime.now().isoformat(),
                "next_revalidation": (datetime.now() + timedelta(days=180)).isoformat(),
            })
        elif result["action"] == "retire":
            record_dead_end(event_type, result["reason"])
    except Exception:
        pass  # don't let promotion errors break the pattern update

    # --- Feedback loop: check if this signal needs more experiments ---
    _check_signal_continuation(event_type, pattern)


def _check_signal_continuation(event_type, pattern):
    """After an experiment completes, ensure the signal stays in the pipeline
    until there's enough data to promote or retire it.

    A single result — right or wrong — proves nothing. The system keeps testing
    until promotion_criteria or retirement_criteria are met.
    """
    from self_review import load_methodology
    m = load_methodology()
    criteria = m.get("promotion_criteria", {})
    min_tests = criteria.get("min_live_tests", 3)
    retire_tests = criteria.get("retirement_min_tests", 5)
    min_acc = criteria.get("min_live_accuracy", 0.6)
    retire_acc = criteria.get("retirement_max_accuracy", 0.3)
    total = pattern.get("effective_independent_n", pattern.get("total_tests", 0))
    accuracy = pattern.get("reliability_score", 0) or 0
    if total > 0 and pattern.get("effective_correct_n") is not None:
        accuracy = pattern["effective_correct_n"] / total

    # Retired signals stop testing
    retired = total >= retire_tests and accuracy <= retire_acc
    if retired:
        return

    # Check if there's already a pending or active hypothesis for this signal
    all_h = load_hypotheses()
    pipeline = [h for h in all_h
                if h.get("event_type") == event_type
                and h["status"] in ("pending", "active")]
    if pipeline:
        return  # already has experiments queued

    # Check if scanners have already found opportunities for this signal
    scanner_map = {
        "sp500_52w_low_momentum_short": "52w_low",
        "sp500_52w_low_catalyst_short": "52w_low",
        "sp500_index_addition": "sp500_additions",
        "insider_buying_cluster": "insider_cluster",
        "ceo_performance_failure_departure_short": "ceo_departure",
    }
    scanner_name = scanner_map.get(event_type)
    existing_opportunities = []
    if scanner_name:
        try:
            import json as _json
            signals = _db.get_scanner_signals(scanner_name, limit=20)
            # Find signals from the last 7 days not already tied to a hypothesis
            recent_cutoff = (datetime.now() - timedelta(days=7)).isoformat()
            active_symbols = {h.get("expected_symbol") for h in all_h
                              if h["status"] in ("pending", "active") and h.get("event_type") == event_type}
            for s in signals:
                data = _json.loads(s["data"]) if isinstance(s.get("data"), str) else s.get("data", {})
                ticker = data.get("ticker", data.get("symbol", ""))
                ts = s.get("timestamp", "")
                if ticker and ts > recent_cutoff and ticker not in active_symbols:
                    existing_opportunities.append(ticker)
        except Exception:
            pass

    promoted = total >= min_tests and accuracy >= min_acc

    if existing_opportunities:
        syms = ", ".join(existing_opportunities[:5])
        more = f" (+{len(existing_opportunities)-5} more)" if len(existing_opportunities) > 5 else ""
        status = "Validated signal" if promoted else f"Signal under test ({pattern.get('effective_correct_n', 0)}/{total} correct)"
        _db.add_research_task(
            category="signal_opportunity",
            question=(
                f"{status} '{event_type}' — scanners found opportunities: {syms}{more}. "
                f"Evaluate and create hypotheses for the best candidates."
            ),
            priority=9 if promoted else 8,
            reasoning=f"Scanners already detected events matching this signal. Don't wait — act on existing data.",
        )
        return

    # No scanner hits — queue a general search
    if promoted:
        _db.add_research_task(
            category="signal_continuation",
            question=(
                f"Validated signal '{event_type}' ({pattern.get('effective_correct_n', 0)}/{total} correct). "
                f"Scan for the next opportunity to trade this signal."
            ),
            priority=7,
            reasoning="Validated signals keep trading every opportunity.",
        )
    else:
        needed = max(min_tests - total, 1)
        _db.add_research_task(
            category="signal_retest",
            question=(
                f"Signal '{event_type}' has {total} independent test(s) "
                f"({pattern.get('effective_correct_n', 0)}/{total} correct). "
                f"Needs {needed} more. Find the next opportunity."
            ),
            priority=8,
            reasoning=f"Needs {needed} more independent test(s) before promotion or retirement.",
        )


def get_active_hypotheses():
    return [h for h in load_hypotheses() if h["status"] == "active"]


def get_pending_hypotheses():
    return [h for h in load_hypotheses() if h["status"] == "pending"]


def get_completed_hypotheses():
    return [h for h in load_hypotheses() if h["status"] == "completed"]


def log_result(hypothesis):
    """Append a completed hypothesis result to the pre-registrations log."""
    r = hypothesis["result"]

    # Idempotency: check if already logged
    existing = _db.get_pre_registrations()
    for entry in existing:
        data = entry.get("data", {})
        if isinstance(data, dict) and data.get("type") != "pre_registration" and data.get("id") == hypothesis["id"]:
            return  # Already logged

    _db.append_pre_registration(
        hypothesis_id=hypothesis["id"],
        prediction_hash=hypothesis.get("prediction_hash"),
        data={
            "type": "result",
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
        },
    )


def get_research_summary():
    """Summarize the state of all research."""
    hypotheses = load_hypotheses()
    completed = [h for h in hypotheses if h["status"] == "completed"]
    active = [h for h in hypotheses if h["status"] == "active"]
    pending = [h for h in hypotheses if h["status"] == "pending"]
    patterns = load_patterns()

    direction_correct = sum(1 for h in completed if h.get("result") and h["result"].get("direction_correct")) if completed else 0

    # Find best and worst performing patterns
    reliable_patterns = [p for p in patterns.values() if p.get("total_tests", 0) >= 3]
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
        if h.get("result") and h["result"].get("direction_correct"):
            types[t]["correct"] += 1
    return {
        t: {
            "accuracy": f"{v['correct']}/{v['total']}",
            "avg_confidence": round(sum(v['avg_confidence']) / len(v['avg_confidence']), 1),
        }
        for t, v in types.items()
    }


# --- Investigation Report ---

def _fmt_pct(val):
    """Format a percentage value, handling None and non-numeric gracefully."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):+.2f}%"
    except (TypeError, ValueError):
        return str(val)


def _wrap(text, width=78, indent=""):
    """Wrap text to width with indent prefix on continuation lines."""
    import textwrap
    return textwrap.fill(text, width=width, initial_indent=indent,
                         subsequent_indent=indent)


def _is_substantive(text):
    """Check whether a free-text field contains real content vs junk."""
    if not text or not isinstance(text, str):
        return False
    stripped = text.strip()
    # Filter out pure numbers, single characters, "N/A", "none", etc.
    if len(stripped) < 5:
        return False
    if stripped.lower() in ("n/a", "none", "unknown", "null", "tbd"):
        return False
    return True


def _post_mortem_contradicts_numbers(post_mortem, direction_correct):
    """Detect if the post-mortem narrative contradicts the numerical verdict."""
    if not _is_substantive(post_mortem):
        return False
    pm_lower = post_mortem.lower()
    contradiction_phrases = [
        "direction wrong", "opposite direction", "did not hold",
        "failed", "does not work", "signal insufficient",
        "no meaningful", "bounced", "stabilized", "reversed",
    ]
    confirmation_phrases = [
        "as predicted", "confirmed", "as expected", "signal works",
        "hypothesis validated", "effect held",
    ]
    has_negative = any(phrase in pm_lower for phrase in contradiction_phrases)
    has_positive = any(phrase in pm_lower for phrase in confirmation_phrases)
    # Contradiction: numbers say correct but narrative says wrong (or vice versa)
    if direction_correct and has_negative and not has_positive:
        return True
    if not direction_correct and has_positive and not has_negative:
        return True
    return False


def _summarize_dead_end(text):
    """Extract a one-sentence summary from a verbose dead-end warning."""
    if not text:
        return text
    # Take up to the first period that ends a sentence (after at least 40 chars)
    # to get the core finding without the full data dump
    sentences = re.split(r'(?<=[.!])\s+', text)
    # The first sentence is usually "Related dead end: X — description"
    # Grab 1-2 sentences that capture the conclusion
    summary_parts = []
    chars = 0
    for s in sentences:
        summary_parts.append(s)
        chars += len(s)
        if chars > 80:
            break
    result = " ".join(summary_parts)
    # If we truncated, add ellipsis
    if len(result) < len(text) - 10:
        result = result.rstrip(".") + "."
    return result


def generate_investigation_report(hypothesis_id):
    """
    Generate a human-readable narrative report for a hypothesis, structured
    around the 6-step investigation method.

    Reads like a research memo. Works at any hypothesis stage.
    Returns a plain-text report string.
    """
    hypotheses = load_hypotheses()
    h = None
    for hyp in hypotheses:
        if hyp["id"] == hypothesis_id:
            h = hyp
            break
    if h is None:
        raise ValueError(f"Hypothesis {hypothesis_id} not found.")

    result = h.get("result") or {}
    trade = h.get("trade") or {}
    oos = h.get("out_of_sample_split") or {}
    confounders = h.get("confounders") or {}

    direction_word = "rise" if h["expected_direction"] == "long" else "fall"
    symbol = h["expected_symbol"]

    sections = []

    # --- Header ---
    sections.append(f"Investigation Report: {h['id']}")
    sections.append(f"{'=' * 60}")
    sections.append(f"Status: {h['status']}  —  Created {h['created'][:10]}")
    sections.append("")

    # =================================================================
    # Step 1: Hypothesis — Given/When/Then
    # =================================================================
    sections.append("1. Hypothesis")
    sections.append("-" * 60)

    event_desc = h.get("event_description", "").strip()
    mechanism = h.get("causal_mechanism", "")

    # Build a clean Given/When/Then from the structured fields
    event_type_readable = h["event_type"].replace("_", " ")
    sections.append(_wrap(
        f"Given that {event_type_readable} events have historically produced "
        f"abnormal returns in affected stocks,"
    ))
    # Normalize: strip leading "when" since we provide it
    desc = event_desc.rstrip(".")
    if desc.lower().startswith("when "):
        desc = desc[5:]
    sections.append(_wrap(f"when {desc.lower()},"))
    sections.append(_wrap(
        f"then {symbol} should {direction_word} by approximately "
        f"{h['expected_magnitude_pct']}% within "
        f"{h['expected_timeframe_days']} trading days."
    ))
    sections.append("")

    if mechanism:
        sections.append(_wrap(
            f"The proposed causal mechanism: {mechanism}"
        ))
        sections.append("")

    criteria = h.get("causal_mechanism_criteria") or []
    if isinstance(criteria, dict):
        criteria_items = criteria
    elif isinstance(criteria, list):
        criteria_items = {c: None for c in criteria}
    else:
        criteria_items = {}

    if criteria_items:
        sections.append("Mechanism evidence:")
        for c, explanation in criteria_items.items():
            label = c.replace("_", " ").capitalize()
            if _is_substantive(explanation):
                sections.append(_wrap(f"  - {label}: {explanation}"))
            else:
                sections.append(f"  - {label}")
        sections.append("")

    # =================================================================
    # Step 2: Test Design
    # =================================================================
    sections.append("2. Test Design")
    sections.append("-" * 60)

    sample = h.get("sample_size", 0)
    bt_symbols = h.get("backtest_symbols") or []
    if isinstance(bt_symbols, list) and bt_symbols:
        symbols_str = ", ".join(bt_symbols[:8])
        if len(bt_symbols) > 8:
            symbols_str += f" and {len(bt_symbols) - 8} others"
        sections.append(_wrap(
            f"The backtest covered {sample} historical events across "
            f"{len(bt_symbols)} stocks ({symbols_str}). All returns are "
            f"measured as abnormal returns against SPY to isolate the event "
            f"effect from broad market moves."
        ))
    else:
        sections.append(_wrap(
            f"The backtest studied {sample} historical instances. All returns "
            f"are measured as abnormal returns against SPY."
        ))
    sections.append("")

    # Out-of-sample
    disc_n = oos.get("discovery_count", 0)
    val_n = oos.get("validation_count", 0)
    if disc_n and val_n:
        sections.append(_wrap(
            f"Data was split temporally: {disc_n} earlier events for pattern "
            f"discovery, {val_n} later events for out-of-sample validation."
        ))
        sections.append("")

    # Bias controls — separate paragraphs for readability
    surv = h.get("survivorship_bias_note")
    if _is_substantive(surv):
        sections.append(_wrap(f"Survivorship bias control: {surv}"))
        sections.append("")
    sel = h.get("selection_bias_note")
    if _is_substantive(sel):
        sections.append(_wrap(f"Selection bias control: {sel}"))
        sections.append("")

    # Confounders
    if confounders:
        confounder_summary = []
        for k, v in confounders.items():
            if _is_substantive(str(v)):
                confounder_summary.append(f"{k.replace('_', ' ')}: {v}")
        if confounder_summary:
            sections.append("Confounders recorded at entry:")
            for cs in confounder_summary:
                sections.append(_wrap(cs, indent="  "))
            sections.append("")

    # =================================================================
    # Step 3: Success Criteria
    # =================================================================
    sections.append("3. Success Criteria (locked before testing)")
    sections.append("-" * 60)

    sc = h.get("success_criteria")
    if sc:
        sections.append(_wrap(sc))
    else:
        # Reconstruct from available fields for older hypotheses
        parts = []
        cons = h.get("consistency_pct")
        if cons:
            parts.append(f"effect appears in at least {cons:.0f}% of instances")
        parts.append(
            f"abnormal return exceeds {h['expected_magnitude_pct']}%"
        )
        mt = h.get("passes_multiple_testing")
        if mt is not None:
            parts.append(
                "significance survives multiple testing correction"
                if mt else
                "WARNING: did not pass multiple testing correction"
            )
        sections.append(_wrap(
            f"To count as valid: {'; '.join(parts)}."
        ))
    sections.append("")
    sections.append(
        f"Confidence: {h.get('confidence', '?')}/10  |  "
        f"Pre-registration: {h.get('prediction_hash', 'N/A')}"
    )
    sections.append("")

    # =================================================================
    # Step 4: Outcome — facts only, no interpretation
    # =================================================================
    sections.append("4. Outcome")
    sections.append("-" * 60)

    if h["status"] == "invalidated":
        reason = result.get("reason", "No reason recorded.")
        sections.append(_wrap(
            f"This hypothesis was invalidated before testing. {reason}"
        ))

    elif h["status"] == "completed" and result:
        raw = result.get("raw_return_pct")
        spy = result.get("spy_return_pct")
        abn = result.get("abnormal_return_pct")
        mag = result.get("magnitude_ratio")
        direction_correct = result.get("direction_correct", False)

        # Raw numbers — no editorializing
        sections.append(f"  Stock return:     {_fmt_pct(raw)}")
        sections.append(f"  SPY return:       {_fmt_pct(spy)}")
        sections.append(f"  Abnormal return:  {_fmt_pct(abn)}")
        sections.append(
            f"  Direction:        "
            f"{'correct' if direction_correct else 'WRONG'}"
        )
        if mag is not None:
            sections.append(
                f"  Magnitude ratio:  {mag:.2f}x of predicted"
            )
        timing = result.get("timing_accuracy")
        if _is_substantive(timing):
            sections.append(f"  Timing:           {timing}")

        # Note early exit or special conditions
        post_mortem = result.get("post_mortem", "")
        if "early" in post_mortem.lower() or "closed early" in post_mortem.lower():
            sections.append("")
            # Extract the early-exit reason
            for sentence in re.split(r'(?<=[.!])\s+', post_mortem):
                if "early" in sentence.lower() or "closed" in sentence.lower():
                    sections.append(_wrap(f"Note: {sentence}"))
                    break

    elif h["status"] == "active":
        entry_time = trade.get("entry_time", "")[:10]
        entry_price = trade.get("entry_price")
        deadline = trade.get("deadline", "")[:10]
        stop = trade.get("stop_loss_pct")
        sections.append(f"  Entry:    {entry_time} at ${entry_price}")
        sections.append(f"  Deadline: {deadline}")
        if stop:
            sections.append(f"  Stop:     {stop}%")
        sections.append("")
        sections.append("Trade is active. No outcome yet.")

    else:
        sections.append("Not yet tested.")

    sections.append("")

    # =================================================================
    # Step 5: Conclusion — reconcile numbers with narrative
    # =================================================================
    sections.append("5. Conclusion")
    sections.append("-" * 60)

    if h["status"] == "completed" and result:
        direction_correct = result.get("direction_correct", False)
        mag = result.get("magnitude_ratio", 0) or 0
        mech = result.get("mechanism_validated")
        attr = result.get("confounder_attribution")
        surprise = result.get("surprise_factor")
        post_mortem = result.get("post_mortem", "")

        # Detect contradiction between numbers and narrative
        has_contradiction = _post_mortem_contradicts_numbers(
            post_mortem, direction_correct
        )

        if has_contradiction:
            # The post-mortem disagrees with the numerical verdict.
            # The narrative wins — numbers can be technically correct
            # but misleading (e.g., early exit, confounders, etc.)
            sections.append(_wrap(
                "CONTRADICTORY RESULT. The numerical outcome and the "
                "qualitative analysis disagree."
            ))
            sections.append("")
            if direction_correct:
                sections.append(_wrap(
                    f"The numbers say the direction was correct with "
                    f"{mag:.0%} of expected magnitude. However, the "
                    f"post-mortem analysis tells a different story:"
                ))
            else:
                sections.append(_wrap(
                    f"The numbers say the direction was wrong. However, "
                    f"the post-mortem analysis suggests otherwise:"
                ))
            sections.append("")
            sections.append(_wrap(post_mortem, indent="  "))
            sections.append("")
            sections.append(_wrap(
                "When numbers and narrative disagree, investigate the "
                "narrative. The numbers may reflect an early exit, a "
                "confounder, or a technicality rather than the true "
                "outcome of the hypothesis."
            ))
        else:
            # No contradiction — straightforward verdict
            if direction_correct and mag >= 0.5:
                sections.append(_wrap(
                    f"The hypothesis is supported. The predicted direction "
                    f"was correct and the move reached {mag:.0%} of the "
                    f"expected magnitude."
                ))
            elif direction_correct and mag >= 0.25:
                sections.append(_wrap(
                    f"The hypothesis is weakly supported. The direction was "
                    f"correct but the move reached only {mag:.0%} of "
                    f"expected magnitude — the effect may be real but "
                    f"smaller than hypothesized."
                ))
            elif direction_correct:
                sections.append(_wrap(
                    f"The hypothesis is not supported. The direction was "
                    f"technically correct but the magnitude ({mag:.0%} of "
                    f"expected) is indistinguishable from noise."
                ))
            else:
                sections.append(_wrap(
                    "The hypothesis is not supported. The stock moved in "
                    "the opposite direction from what was predicted."
                ))

            # Mechanism validation — the most important qualitative check
            if _is_substantive(mech):
                sections.append("")
                sections.append(_wrap(f"Mechanism check: {mech}"))

            # Attribution
            if _is_substantive(attr):
                sections.append("")
                sections.append(_wrap(f"Attribution: {attr}"))

            # Surprise — only if it's real content
            if _is_substantive(surprise):
                sections.append("")
                sections.append(_wrap(f"What was unexpected: {surprise}"))

            # Post-mortem as the full narrative
            if _is_substantive(post_mortem):
                sections.append("")
                sections.append(_wrap(post_mortem, indent="  "))

        sections.append("")

    elif h["status"] == "invalidated":
        reason = result.get("reason", "")
        sections.append(_wrap(
            f"No conclusion — invalidated before testing. "
            f"{reason if _is_substantive(reason) else ''}"
        ))
        sections.append("")

    else:
        sections.append("Awaiting outcome.")
        sections.append("")

    # =================================================================
    # Step 6: What Comes Next — derived from the ACTUAL conclusion
    # =================================================================
    sections.append("6. What Comes Next")
    sections.append("-" * 60)

    if h["status"] == "completed" and result:
        direction_correct = result.get("direction_correct", False)
        mag = result.get("magnitude_ratio", 0) or 0
        post_mortem = result.get("post_mortem", "")
        has_contradiction = _post_mortem_contradicts_numbers(
            post_mortem, direction_correct
        )

        if has_contradiction:
            # The conclusion was ambiguous — don't pretend we know
            sections.append(_wrap(
                "The result is ambiguous. Before moving forward, resolve "
                "the contradiction between the numbers and the narrative. "
                "Was this an early exit? A confounder? A flawed test "
                "design? The answer determines whether to retry the same "
                "hypothesis under cleaner conditions, refine it, or "
                "abandon the line of inquiry."
            ))
        elif direction_correct and mag >= 0.5:
            sections.append(_wrap(
                "The signal held. Next: stress-test across different "
                "market regimes, time periods, or related assets. If it "
                "survives, promote to a known effect."
            ))
        elif direction_correct:
            sections.append(_wrap(
                "The direction was right but the effect was weak. Possible "
                "next steps: tighten entry conditions, control for the "
                "dominant confounder, or accept that the effect is smaller "
                "than originally hypothesized and re-evaluate whether it's "
                "still worth trading after costs."
            ))
        else:
            # Failed — but derive next step from the post-mortem if possible
            if _is_substantive(post_mortem):
                # Look for the agent's own suggestion in the post-mortem
                pm_lower = post_mortem.lower()
                has_refined = any(
                    phrase in pm_lower for phrase in
                    ["improved signal", "refined", "better signal",
                     "should instead", "alternative"]
                )
                if has_refined:
                    sections.append(_wrap(
                        "The hypothesis failed, but the post-mortem "
                        "identified a refined approach. The next hypothesis "
                        "should test that specific refinement — not a "
                        "parameter tweak, but the new condition that the "
                        "failure revealed."
                    ))
                else:
                    sections.append(_wrap(
                        "The hypothesis failed. Before generating a new one: "
                        "can you explain WHY it failed? Was the causal "
                        "mechanism wrong, or did a specific condition "
                        "invalidate it? A new hypothesis is only warranted "
                        "if the failure produced a specific, testable "
                        "insight."
                    ))
            else:
                sections.append(_wrap(
                    "The hypothesis failed and no post-mortem was recorded. "
                    "Without understanding why it failed, generating a new "
                    "hypothesis risks repeating the same mistake. Write the "
                    "post-mortem first."
                ))

    elif h["status"] == "invalidated":
        sections.append(_wrap(
            "Invalidated before testing. If the reason points to a "
            "fixable flaw in the test design, revise and retry. If "
            "conditions changed, move to a different question."
        ))
    else:
        sections.append("Complete the current investigation first.")

    sections.append("")

    # =================================================================
    # Warnings — summarized, not dumped
    # =================================================================
    raw_warnings = []
    if h.get("dead_end_warnings"):
        w = h["dead_end_warnings"]
        raw_warnings.extend(w if isinstance(w, list) else [w])
    if h.get("symbol_warnings"):
        w = h["symbol_warnings"]
        raw_warnings.extend(w if isinstance(w, list) else [w])
    if h.get("multiple_testing_warning"):
        raw_warnings.append(h["multiple_testing_warning"])

    if raw_warnings:
        sections.append("Warnings")
        sections.append("-" * 60)
        for w in raw_warnings:
            sections.append(_wrap(f"- {_summarize_dead_end(w)}"))
        sections.append("")

    return "\n".join(sections)


# --- Knowledge Base ---

def load_knowledge():
    """Load the knowledge base."""
    return _db.load_knowledge()


def save_knowledge(kb):
    _db.save_knowledge(kb)


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
    _db.record_literature(event_type, findings)


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
    _db.record_known_effect(event_type, effect)


def record_dead_end(event_type, reason):
    """Record a research direction that didn't pan out, so we don't revisit it."""
    _db.record_dead_end(event_type, reason)


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
    if isinstance(patterns, dict):
        pattern = patterns.get(event_type)
    else:
        pattern = None
        for p in patterns:
            if p["event_type"] == event_type:
                pattern = p
                break

    if not pattern:
        return {"action": "none", "reason": "No pattern data yet.", "stats": None}

    raw_total = pattern["total_tests"]
    experiments = pattern.get("experiments", [])

    # Use effective independent experiments, not raw count
    # Correlated experiments (same week) are one data point
    indep = _count_independent_experiments(experiments)
    total = indep["count"] or raw_total
    correct = indep["correct"]
    accuracy = correct / total if total > 0 else 0

    # Only count magnitude ratio for direction-correct experiments
    # For shorts, actual_pct is negative when correct — use abs() for both
    mag_ratios = []
    for e in experiments:
        if e.get("expected_pct", 0) != 0 and e.get("direction_correct", False):
            mag_ratios.append(abs(e.get("actual_pct", 0)) / abs(e["expected_pct"]))
    avg_mag_ratio = sum(mag_ratios) / len(mag_ratios) if mag_ratios else 0

    stats = {
        "total_tests": raw_total,
        "effective_independent_n": total,
        "accuracy": round(accuracy, 2),
        "avg_magnitude_ratio": round(avg_mag_ratio, 2),
    }

    # Check promotion (uses effective independent N)
    if total >= min_tests and accuracy >= min_acc and avg_mag_ratio >= min_mag:
        return {
            "action": "promote",
            "reason": (
                f"Pattern qualifies: {total} independent tests "
                f"({raw_total} raw), {accuracy:.0%} accuracy, "
                f"{avg_mag_ratio:.2f} magnitude ratio."
            ),
            "stats": stats,
        }

    # Check retirement (uses effective independent N)
    if total >= retire_tests and accuracy <= retire_acc:
        return {
            "action": "retire",
            "reason": (
                f"Pattern retired: {total} independent tests "
                f"({raw_total} raw), {accuracy:.0%} accuracy."
            ),
            "stats": stats,
        }

    return {
        "action": "none",
        "reason": f"Pattern has {total} independent tests ({raw_total} raw), {accuracy:.0%} accuracy — needs more data.",
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

    # Check research queue references
    rq = _db.load_queue()
    if rq:

        # Check session_handoff hypothesis_ids
        handoff = rq.get("session_handoff", {})
        if isinstance(handoff, str):
            handoff = {}  # string handoffs have no structured hypothesis_ids to check
        for key, hid in handoff.get("hypothesis_ids", {}).items():
            if hid not in hyp_ids:
                issues.append(
                    f"MISSING HYPOTHESIS: session_handoff references "
                    f"'{hid}' ({key}) but it does not exist in the database. "
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

    # Check pre_registrations for orphaned entries
    for entry in _db.get_pre_registrations():
        data = entry.get("data", {})
        if isinstance(data, dict):
            rid = data.get("id")
            if rid and data.get("type") == "pre_registration" and rid not in hyp_ids:
                issues.append(
                    f"ORPHANED PRE-REGISTRATION: pre_registrations row {entry.get('id')} "
                    f"has pre-registration for '{rid}' but hypothesis is missing."
                )

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
            deadline = (h.get("trade") or {}).get("deadline")
            if deadline and deadline < datetime.now().isoformat():
                issues.append(
                    f"OVERDUE: Hypothesis {h['id']} ({h['event_type']}) is past deadline "
                    f"{deadline[:10]}. Complete or invalidate it."
                )

    return {"ok": len(issues) == 0, "issues": issues}
