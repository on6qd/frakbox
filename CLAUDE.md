# Stock Market Causal Research

## Mission
Discover cause-and-effect relationships between real-world events and stock price movements. Paper trading on Alpaca ($100K) validates hypotheses. P&L doesn't matter — learning does.

## Files

| File | Purpose |
|---|---|
| `research.py` | Hypothesis lifecycle, knowledge base, pattern library |
| `market_data.py` | Historical prices, event impact measurement, power analysis |
| `self_review.py` | Confidence scoring, methodology management |
| `research_queue.py` | Task queue, event watchlist, cross-session handoffs |
| `trader.py` | Paper trades via Alpaca |
| `run.py` | `python run.py --status` or `--review` |
| `email_report.py` | HTML email digest |
| `config.py` | Environment config |
| `tools/` | Custom analysis tools (e.g., `insider_cluster_detector.py`) |

## API Quick Reference

```python
# Backtesting — multi-symbol
market_data.measure_event_impact(event_dates=[
    {"symbol": "AAPL", "date": "2024-01-15", "timing": "after_hours"},
    {"symbol": "MSFT", "date": "2024-04-20"},
], benchmark="SPY", sector_etf="XLK")

# Single-symbol shorthand
market_data.measure_event_impact("AAPL", ["2024-01-15", "2024-04-20"])

# Regime-filtered backtesting (VIX: "calm"<20, "elevated"20-30, "crisis">30)
market_data.measure_event_impact(event_dates=[...], regime_filter="calm")

# Cross-category FDR correction (Benjamini-Hochberg)
market_data.apply_cross_category_fdr({"earnings": 0.03, "fda": 0.001})

# Hypothesis lifecycle
research.create_hypothesis(...)   # see required fields below
research.activate_hypothesis(id, entry_price, position_size)
research.complete_hypothesis(id, exit_price, return_pct, post_mortem,
    spy_return_pct=..., timing_accuracy=..., mechanism_validated=...,
    confounder_attribution=..., surprise_factor=...)
research.check_promotion_or_retirement(event_type)

# Data integrity (run at session start)
research.verify_data_integrity()  # returns {"ok": bool, "issues": [...]}

# Knowledge base
research.record_literature(event_type, {
    "summary": "...", "known_magnitude": "...", "known_timeframe": "...",
    "sources": [...], "gaps": "..."
})
research.record_known_effect(event_type, effect)
research.record_dead_end(event_type, reason)

# Research queue
research_queue.add_research_task(category, question, priority, reasoning)
research_queue.complete_research_task(task_id, findings)
research_queue.add_event_to_watchlist(event, date, symbol, hypothesis)
research_queue.set_next_session_priorities(priorities, handoff={...})

# Self-review
self_review.compute_confidence_score(sample_size, consistency, avg_return, stdev,
    literature_strength="none"|"partial"|"strong")
```

## create_hypothesis() Validation Rules

These are enforced in code and will raise `ValueError` if violated:
- `expected_magnitude_pct` must be >= `min_abnormal_return_pct` (1.5%)
- Expected return after transaction costs (0.1% round-trip) must exceed 1.0%
- All confounders from `methodology.json` must be provided (use "unknown" if no data)
- Historical evidence cannot be all placeholders — need real measured data
- Causal mechanism must satisfy 2 of 3 criteria
- `expected_symbol` can be "TBD" but must be resolved before `activate_hypothesis()`

## measure_event_impact() Return Structure

```python
result['avg_abnormal_1d']            # mean abnormal return at 1d
result['median_abnormal_1d']
result['positive_rate_abnormal_1d']  # % of events positive
result['stdev_abnormal_1d']
result['skewness_abnormal_1d']       # skewness (>1.0 triggers warning)
result['wilcoxon_p_abnormal_1d']     # non-parametric robustness check
result['passes_multiple_testing']    # True/False
result['events_measured']
result['data_quality_warning']       # None or warning string
result['regime_filter']              # None or "calm"/"elevated"/"crisis"
result['regime_filtered_count']      # events excluded by regime filter
# Horizons: 1d, 3d, 5d, 10d, 20d. Types: raw, abnormal, sector_adj
# Per-event: result['individual_impacts'][i]['abnormal_1d'], ['event_date'], ['symbol']
```

## Alpaca Paper Trading
Cash: $100,000 | Fractional shares | Shorting enabled | $5,000 per experiment
