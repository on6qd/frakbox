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

## API Quick Reference

```python
# Backtesting — multi-symbol
market_data.measure_event_impact(event_dates=[
    {"symbol": "AAPL", "date": "2024-01-15", "timing": "after_hours"},
    {"symbol": "MSFT", "date": "2024-04-20"},
], benchmark="SPY", sector_etf="XLK")

# Single-symbol shorthand
market_data.measure_event_impact("AAPL", ["2024-01-15", "2024-04-20"])

# Hypothesis lifecycle
research.create_hypothesis(...)
research.activate_hypothesis(id, entry_price, position_size)
research.complete_hypothesis(id, exit_price, return_pct, post_mortem,
    spy_return_pct=..., timing_accuracy=..., mechanism_validated=...,
    confounder_attribution=..., surprise_factor=...)
research.check_promotion_or_retirement(event_type)

# Knowledge base
research.record_literature(event_type, findings)
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

# Out-of-sample validation
research.validate_out_of_sample(evidence)
research.validate_out_of_sample(evidence, discovery_cutoff_date="2023-01-01")
```

## measure_event_impact() Return Structure

```python
result['avg_abnormal_1d']            # mean abnormal return at 1d
result['median_abnormal_1d']
result['positive_rate_abnormal_1d']  # % of events positive
result['stdev_abnormal_1d']
result['passes_multiple_testing']    # True/False
result['events_measured']
result['data_quality_warning']       # None or warning string
# Horizons: 1d, 3d, 5d, 10d, 20d. Types: raw, abnormal, sector_adj
# Per-event: result['individual_impacts'][i]['abnormal_1d'], ['event_date'], ['symbol']
```

## Alpaca Paper Trading
Cash: $100,000 | Fractional shares | Shorting enabled | $5,000 per experiment
