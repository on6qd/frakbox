# API Reference

Read this file only when you need a specific function signature. Do NOT load at session start.

## measure_event_impact()

```python
# Multi-symbol
market_data.measure_event_impact(event_dates=[
    {"symbol": "AAPL", "date": "2024-01-15", "timing": "after_hours"},
    {"symbol": "MSFT", "date": "2024-04-20"},
], benchmark="SPY", sector_etf="XLK")

# Single-symbol shorthand
market_data.measure_event_impact("AAPL", ["2024-01-15", "2024-04-20"])

# Options: entry_price="open", estimate_costs=True, event_type="...", regime_filter="calm"
# Standalone cost estimate: market_data.estimate_transaction_cost(event_type="sp500_index_addition")
# Per-event: {"symbol": "AAPL", "date": "...", "timing": "after_hours", "entry_price": "open"}
```

### Return structure
```python
result['avg_abnormal_1d']            # mean abnormal return at 1d
result['median_abnormal_1d']
result['positive_rate_abnormal_1d']  # % of events positive
result['stdev_abnormal_1d']
result['skewness_abnormal_1d']       # >1.0 triggers warning
result['wilcoxon_p_abnormal_1d']     # non-parametric robustness
result['passes_multiple_testing']    # True/False
result['events_measured']
result['data_quality_warning']       # None or warning string
result['regime_filter']              # None or "calm"/"elevated"/"crisis"
result['bootstrap_ci_abnormal_1d']   # {"point_estimate", "ci_lower", "ci_upper", "ci_excludes_zero"}
result['entry_price_mode']           # "close" or "open"
result['avg_estimated_cost_pct']     # when estimate_costs=True
# Horizons: 1d, 3d, 5d, 10d, 20d. Types: raw, abnormal, sector_adj
# Per-event: result['individual_impacts'][i]['abnormal_1d'], ['event_date'], ['symbol']
# Per-event: ['entry_price_type'], ['avg_daily_volume'], ['volume_ratio'], ['estimated_cost']
```

## create_hypothesis() Full Signature

```python
research.create_hypothesis(
    event_type="insider_buying_cluster",       # string key for knowledge base
    event_description="...",                   # human-readable description
    causal_mechanism="...",                    # full causal chain explanation
    causal_mechanism_criteria={               # must satisfy 2 of 3
        "actors_and_incentives": "...",
        "transmission_channel": "...",
        "academic_reference": "..."
    },
    expected_symbol="AAPL",                   # ticker or "TBD"
    expected_direction="long",                 # "long" or "short"
    expected_magnitude_pct=5.0,               # abnormal return (>= 1.5%)
    expected_timeframe_days=5,                # hold period in trading days
    historical_evidence={                      # must contain real measured data
        "avg_abnormal_return": -23.88,
        "sample_size": 6,
        "consistency_pct": 100.0,
        "time_period": "2019-2024",
        "stdev": 8.74,
        "passes_multiple_testing": True,
        "p_value": 0.001
    },
    sample_size=6,
    consistency_pct=100.0,
    confounders={                             # all keys from methodology.json required
        "market_regime": "...",
        "sector_trend": "...",
        "concurrent_news": "..."
    },
    market_regime_note="...",
    confidence=8,                             # integer from compute_confidence_score()
    out_of_sample_split={                     # temporal split required
        "discovery_period": "2019-2022",
        "validation_period": "2023-2024",
        "validation_indices": [3, 4, 5],      # 0-indexed, min 3 required
        "validation_consistency_pct": 100.0
    },
    survivorship_bias_note="...",
    selection_bias_note="...",
    literature_reference=None,                # optional
    event_timing="after_hours",              # optional
    passes_multiple_testing=True,            # optional
    backtest_symbols=["AAPL", "MSFT"],       # optional
    backtest_events=[{"symbol": "AAPL", "date": "2024-01-15"}]  # optional
)
```

**Validation Rules** (raises `ValueError`):
- `expected_magnitude_pct` >= 1.5%
- Return after costs must exceed 1.0%
- All confounders from `methodology.json` required (use "unknown" if no data)
- Historical evidence needs real data (not placeholders)
- Causal mechanism must satisfy 2 of 3 criteria
- `out_of_sample_split.validation_indices` >= 3 entries

## Other API

```python
# Hypothesis lifecycle
research.activate_hypothesis(id, entry_price, position_size)
research.complete_hypothesis(hypothesis_id, exit_price, actual_return_pct, post_mortem,
    spy_return_pct=None, sector_etf_return_pct=None, confounders_at_exit=None,
    timing_accuracy=None, mechanism_validated=None,
    confounder_attribution=None, surprise_factor=None)
research.check_promotion_or_retirement(event_type)
research.verify_data_integrity()  # returns {"ok": bool, "issues": [...]}

# Knowledge base — record_literature takes a dict, NOT a string
research.record_literature(event_type, {
    "summary": "...", "known_magnitude": "...", "known_timeframe": "...",
    "sources": [...], "gaps": "..."
})
research.record_known_effect(event_type, effect)   # effect can be dict or string
research.record_dead_end(event_type, reason)       # reason is a string

# Research queue
research_queue.add_research_task(category, question, priority, reasoning, depends_on=None)
research_queue.complete_research_task(task_id, findings_summary)  # NOT "findings"
research_queue.add_event_to_watchlist(event_description, expected_date, symbol, hypothesis_template)
research_queue.set_next_session_priorities(priorities, handoff=None)

# Self-review — note exact parameter names (NOT consistency, stdev)
self_review.compute_confidence_score(sample_size, consistency_pct, avg_return, stdev_return,
    has_literature=False, literature_strength=None)
# literature_strength: "none"|"partial"|"strong" — only used when has_literature=True

# Targeted hypothesis update (use instead of load_hypotheses/save_hypotheses)
db.update_hypothesis_fields(hypothesis_id, trigger="next_market_open", trigger_stop_loss_pct=10)

# Journal and friction (all data is in SQLite — no JSONL files)
db.append_journal_entry(date, session_type, investigated, findings, surprised_by, next_step)
db.append_friction(date, category, description, turns_wasted=0, potential_fix=None)
db.get_recent_journal(n=5)         # list of dicts, chronological
db.get_friction_summary(top_n=3)   # [{category, count, latest_description}]

# Key-value state (replaces all JSON state files)
db.get_state(key)                  # returns dict or None
db.set_state(key, value_dict)      # upsert

# Scanner signals
db.append_scanner_signal(scanner_name, data_dict)
db.get_scanner_signals(scanner_name, limit=50)

# Cross-category FDR correction
market_data.apply_cross_category_fdr({"earnings": 0.03, "fda": 0.001})
```

## EDGAR EFTS API — Ticker Lookup

**CRITICAL**: `entity_name` is always null. Use `display_names` instead.

```python
import requests, re
url = 'https://efts.sec.gov/LATEST/search-index?q=%22PHRASE%22&forms=8-K&dateRange=custom&startdt=2021-01-01&enddt=2024-12-31'
resp = requests.get(url, headers={'User-Agent': 'research@example.com'})
for h in resp.json()['hits']['hits']:
    display_names = h['_source'].get('display_names', [])
    for dn in display_names:
        matches = re.findall(r'\(([A-Z]{1,5})\)', dn)
        if matches:
            ticker = matches[0]
            break
```
