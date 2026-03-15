"""
Self-review engine — analyzes research performance and updates methodology.

This is the meta-learning layer. It looks at completed experiments and asks:
- Are our confidence scores calibrated?
- Which event types are worth pursuing?
- Are our timeframes right?
- Which confounders actually matter?
- What should we change about how we research?

Triggered every N completed experiments (configured in methodology.json).
"""

import json
import os
import tempfile
from datetime import datetime, timedelta

METHODOLOGY_FILE = os.path.join(os.path.dirname(__file__), "methodology.json")


def load_methodology():
    with open(METHODOLOGY_FILE) as f:
        return json.load(f)


def save_methodology(m):
    dir_name = os.path.dirname(METHODOLOGY_FILE)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(m, f, indent=2)
        os.replace(tmp_path, METHODOLOGY_FILE)
    except Exception:
        os.unlink(tmp_path)
        raise


def needs_review(completed_count):
    """Check if it's time for a self-review."""
    m = load_methodology()
    last_review = m["last_review_at_experiment"]
    interval = m["review_every_n_experiments"]
    return completed_count >= last_review + interval


def run_self_review(completed_hypotheses):
    """
    Analyze all completed experiments and update methodology.

    Returns a report dict describing what was found and what changed.
    """
    m = load_methodology()
    report = {
        "timestamp": datetime.now().isoformat(),
        "experiments_analyzed": len(completed_hypotheses),
        "findings": [],
        "changes_made": [],
    }

    if len(completed_hypotheses) < 3:
        report["findings"].append("Too few experiments for meaningful review.")
        return report

    # --- 0. Magnitude-Aware Accuracy ---
    magnitude_finding = _analyze_magnitude_accuracy(completed_hypotheses)
    report["findings"].append(magnitude_finding)

    # --- 1. Confidence Calibration ---
    calibration = _analyze_confidence_calibration(completed_hypotheses)
    report["findings"].append(calibration["finding"])
    m["confidence_calibration"]["buckets"] = calibration["buckets"]
    if calibration.get("recommendation"):
        report["changes_made"].append(calibration["recommendation"])

    # --- 2. Per-Category Performance (direction + magnitude) ---
    category_analysis = _analyze_categories(completed_hypotheses)
    for cat_name, cat_data in category_analysis.items():
        m["category_status"][cat_name] = cat_data["status"]

        # Adjust per-category settings if we have enough data
        if cat_data["total"] >= 5:
            if cat_name not in m["per_category"]:
                m["per_category"][cat_name] = {}

            # If accuracy is high, we can increase position size
            if cat_data["accuracy"] >= 0.75 and cat_data["total"] >= 8:
                old_size = m["per_category"].get(cat_name, {}).get("position_size_pct", m["defaults"]["position_size_pct"])
                new_size = min(10, old_size + 1)
                if new_size != old_size:
                    m["per_category"][cat_name]["position_size_pct"] = new_size
                    report["changes_made"].append(
                        f"{cat_name}: increased position size to {new_size}% (accuracy {cat_data['accuracy']:.0%} over {cat_data['total']} tests)"
                    )

            # If accuracy is low, reduce position size or flag for review
            if cat_data["accuracy"] < 0.4:
                m["per_category"][cat_name]["position_size_pct"] = 2
                report["changes_made"].append(
                    f"{cat_name}: reduced position size to 2% (accuracy only {cat_data['accuracy']:.0%})"
                )

            # If accuracy is terrible, mark category for retirement
            if cat_data["accuracy"] < 0.3 and cat_data["total"] >= 8:
                m["category_status"][cat_name] = "retired"
                report["changes_made"].append(
                    f"{cat_name}: RETIRED — accuracy {cat_data['accuracy']:.0%} over {cat_data['total']} tests is below random chance"
                )

        report["findings"].append(
            f"{cat_name}: {cat_data['accuracy']:.0%} accuracy over {cat_data['total']} tests — {cat_data['status']}"
        )

    # --- 3. Timeframe Analysis ---
    timeframe_finding = _analyze_timeframes(completed_hypotheses)
    if timeframe_finding:
        report["findings"].append(timeframe_finding)

    # --- 4. Sample Size Effectiveness ---
    sample_finding = _analyze_sample_size_impact(completed_hypotheses)
    if sample_finding.get("recommendation"):
        report["findings"].append(sample_finding["finding"])
        if sample_finding.get("new_min_sample"):
            old = m["defaults"]["min_sample_size"]
            m["defaults"]["min_sample_size"] = sample_finding["new_min_sample"]
            report["changes_made"].append(
                f"Adjusted default min_sample_size from {old} to {sample_finding['new_min_sample']}"
            )

    # --- 5. Update version and changelog ---
    m["version"] += 1
    m["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    m["last_review_at_experiment"] = len(completed_hypotheses)

    changelog_entry = {
        "version": m["version"],
        "date": m["last_updated"],
        "changes": "; ".join(report["changes_made"]) if report["changes_made"] else "No methodology changes needed",
        "reason": f"Self-review after {len(completed_hypotheses)} experiments",
    }
    m["methodology_changelog"].append(changelog_entry)

    save_methodology(m)
    return report


def get_category_settings(event_type):
    """
    Get the current methodology settings for a specific event category.
    Falls back to defaults if no category-specific overrides exist.
    """
    m = load_methodology()
    defaults = m["defaults"]
    overrides = m.get("per_category", {}).get(event_type, {})

    return {
        "min_sample_size": overrides.get("min_sample_size", defaults["min_sample_size"]),
        "min_consistency_pct": overrides.get("min_consistency_pct", defaults["min_consistency_pct"]),
        "position_size_pct": overrides.get("position_size_pct", defaults["position_size_pct"]),
        "measurement_horizons_days": overrides.get("measurement_horizons_days", defaults["measurement_horizons_days"]),
        "status": m.get("category_status", {}).get(event_type, "active"),
    }


def _analyze_confidence_calibration(hypotheses):
    """Check if confidence scores actually predict accuracy."""
    buckets = {
        "1-3": {"predictions": 0, "correct": 0},
        "4-6": {"predictions": 0, "correct": 0},
        "7-10": {"predictions": 0, "correct": 0},
    }

    for h in hypotheses:
        conf = h.get("confidence", 5)
        correct = h.get("result", {}).get("direction_correct", False)

        if conf <= 3:
            bucket = "1-3"
        elif conf <= 6:
            bucket = "4-6"
        else:
            bucket = "7-10"

        buckets[bucket]["predictions"] += 1
        if correct:
            buckets[bucket]["correct"] += 1

    # Check if high confidence actually predicts better
    rates = {}
    for bucket_name, data in buckets.items():
        if data["predictions"] > 0:
            rates[bucket_name] = data["correct"] / data["predictions"]
        else:
            rates[bucket_name] = None

    finding = "Confidence calibration: "
    for bucket_name, rate in rates.items():
        if rate is not None:
            finding += f"[{bucket_name}]: {rate:.0%} ({buckets[bucket_name]['predictions']} tests) "
        else:
            finding += f"[{bucket_name}]: no data "

    recommendation = None
    if rates.get("7-10") is not None and rates.get("1-3") is not None:
        if rates["7-10"] <= rates["1-3"]:
            recommendation = "WARNING: High confidence predictions are NOT more accurate than low confidence. Confidence scoring needs recalibration."
            finding += "— MISCALIBRATED"
        else:
            finding += "— well calibrated"

    return {"finding": finding, "buckets": buckets, "recommendation": recommendation}


def _analyze_timeframes(hypotheses):
    """Check if our expected timeframes match actual best-return windows."""
    timeframe_mismatches = 0
    total = 0
    for h in hypotheses:
        if h.get("result", {}).get("direction_correct"):
            expected_days = h.get("expected_timeframe_days", 0)
            magnitude_ratio = h.get("result", {}).get("magnitude_ratio")
            if magnitude_ratio is not None and magnitude_ratio < 0.5:
                timeframe_mismatches += 1
            total += 1

    if total >= 5 and timeframe_mismatches / total > 0.5:
        return f"Timeframe issue: {timeframe_mismatches}/{total} correct-direction trades had less than half the expected magnitude. Our timeframes may be off."
    return None


def _analyze_magnitude_accuracy(hypotheses):
    """
    Check whether predicted magnitudes match actual magnitudes.
    Direction-only accuracy is misleading: predicting +5% and seeing +0.1%
    counts as 'correct' by direction but is not a useful prediction.
    """
    ratios = []
    direction_correct_but_weak = 0
    total_direction_correct = 0

    for h in hypotheses:
        result = h.get("result", {})
        mag_ratio = result.get("magnitude_ratio")
        if result.get("direction_correct"):
            total_direction_correct += 1
            if mag_ratio is not None:
                ratios.append(mag_ratio)
                if mag_ratio < 0.25:
                    direction_correct_but_weak += 1

    if not ratios:
        return "Magnitude accuracy: no data yet."

    avg_ratio = sum(ratios) / len(ratios)
    median_ratio = sorted(ratios)[len(ratios) // 2]
    weak_pct = direction_correct_but_weak / len(ratios) * 100 if ratios else 0

    finding = (
        f"Magnitude accuracy: avg magnitude ratio {avg_ratio:.2f} "
        f"(median {median_ratio:.2f}). "
        f"{direction_correct_but_weak}/{len(ratios)} ({weak_pct:.0f}%) 'correct' calls "
        f"achieved <25% of predicted magnitude — these are effectively noise, not signal."
    )

    if weak_pct > 50:
        finding += " WARNING: Most 'correct' predictions are barely distinguishable from random. Direction accuracy is overstating real predictive power."

    return finding


def _analyze_categories(hypotheses):
    """Analyze accuracy per event category — both direction and magnitude."""
    categories = {}
    for h in hypotheses:
        cat = h["event_type"]
        if cat not in categories:
            categories[cat] = {"total": 0, "correct": 0, "magnitude_ratios": []}
        categories[cat]["total"] += 1
        result = h.get("result", {})
        if result.get("direction_correct"):
            categories[cat]["correct"] += 1
        mag_ratio = result.get("magnitude_ratio")
        if mag_ratio is not None:
            categories[cat]["magnitude_ratios"].append(mag_ratio)

    result = {}
    for cat, data in categories.items():
        accuracy = data["correct"] / data["total"] if data["total"] > 0 else 0
        avg_mag = (sum(data["magnitude_ratios"]) / len(data["magnitude_ratios"])
                   if data["magnitude_ratios"] else None)

        # Status considers both direction accuracy AND magnitude accuracy
        if accuracy >= 0.7 and data["total"] >= 5 and (avg_mag is None or avg_mag >= 0.4):
            status = "high_performer"
        elif accuracy >= 0.5:
            status = "active"
        elif accuracy >= 0.3:
            status = "underperforming"
        elif data["total"] >= 8:
            status = "retired"
        else:
            status = "needs_more_data"

        # Downgrade if magnitude is terrible even with good direction
        if avg_mag is not None and avg_mag < 0.2 and accuracy >= 0.5:
            status = "overfit_direction"

        result[cat] = {
            "total": data["total"],
            "correct": data["correct"],
            "accuracy": accuracy,
            "avg_magnitude_ratio": round(avg_mag, 2) if avg_mag is not None else None,
            "status": status,
        }

    return result


def check_knowledge_decay():
    """
    Check the knowledge base for effects that haven't been revalidated recently.
    Returns a list of event types that need revalidation.
    """
    import os
    KNOWLEDGE_FILE = os.path.join(os.path.dirname(__file__), "knowledge_base.json")
    if not os.path.exists(KNOWLEDGE_FILE):
        return []

    with open(KNOWLEDGE_FILE) as f:
        kb = json.load(f)

    m = load_methodology()
    max_months = m["defaults"].get("knowledge_revalidation_months", 12)
    cutoff = datetime.now() - timedelta(days=max_months * 30)
    cutoff_str = cutoff.isoformat()

    stale = []
    for event_type, effect in kb.get("known_effects", {}).items():
        last_validated = effect.get("last_updated", effect.get("last_validated", ""))
        if last_validated and last_validated < cutoff_str:
            stale.append({
                "event_type": event_type,
                "last_validated": last_validated,
                "current_status": effect.get("status", "unknown"),
            })

    return stale


def compute_confidence_score(sample_size, consistency_pct, avg_return, stdev_return, has_literature=False):
    """
    Compute confidence score from evidence using the rubric in methodology.json.
    Returns an integer 1-10.

    This replaces vibes-based confidence assignment. Use this when forming hypotheses.
    """
    score = 0

    # Sample size component (max 3)
    if sample_size >= 15:
        score += 3
    elif sample_size >= 8:
        score += 2
    elif sample_size >= 5:
        score += 1

    # Consistency component (max 3)
    if consistency_pct >= 80:
        score += 3
    elif consistency_pct >= 70:
        score += 2
    elif consistency_pct >= 60:
        score += 1

    # Effect vs noise — signal-to-noise ratio (max 3)
    if stdev_return > 0:
        snr = abs(avg_return) / stdev_return
        if snr >= 1.0:
            score += 3
        elif snr >= 0.5:
            score += 2
        elif snr >= 0.3:
            score += 1

    # Literature support (max 1 here, keeping total ≤ 10)
    if has_literature:
        score += 1

    return max(1, min(10, score))


def _analyze_sample_size_impact(hypotheses):
    """Check if hypotheses with larger historical sample sizes perform better."""
    high_sample = [h for h in hypotheses if h.get("sample_size", 0) >= 10]
    low_sample = [h for h in hypotheses if h.get("sample_size", 0) < 10]

    if len(high_sample) < 3 or len(low_sample) < 3:
        return {"finding": "Not enough data to compare sample size impact yet."}

    high_acc = sum(1 for h in high_sample if h.get("result", {}).get("direction_correct")) / len(high_sample)
    low_acc = sum(1 for h in low_sample if h.get("result", {}).get("direction_correct")) / len(low_sample)

    finding = f"Sample size impact: >=10 samples accuracy {high_acc:.0%} ({len(high_sample)} tests) vs <10 samples accuracy {low_acc:.0%} ({len(low_sample)} tests)"

    recommendation = None
    new_min = None
    if high_acc > low_acc + 0.15:
        recommendation = f"Higher sample sizes correlate with better accuracy (+{high_acc-low_acc:.0%}). Consider raising minimum."
        new_min = 8
        finding += " — raising minimum recommended"

    return {"finding": finding, "recommendation": recommendation, "new_min_sample": new_min}
