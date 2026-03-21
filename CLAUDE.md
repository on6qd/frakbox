# Stock Market Causal Research

## Mission
Learn to trade perfectly. Discover cause-and-effect relationships between real-world events and stock price movements, then trade on them. Paper trading on Alpaca ($100K) validates hypotheses. Rigor is the method — profitable, repeatable trading is the goal.

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
| `config.py` | Environment config (Alpaca, Gmail, Tiingo keys) |
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

# Realistic entry price (use next-day open instead of close for after-hours events)
market_data.measure_event_impact(event_dates=[...], entry_price="open")

# Per-event entry override
market_data.measure_event_impact(event_dates=[
    {"symbol": "AAPL", "date": "2024-01-15", "timing": "after_hours", "entry_price": "open"},
])

# Transaction cost estimation (uses volume data when available, event-type defaults otherwise)
market_data.measure_event_impact(event_dates=[...], estimate_costs=True, event_type="sp500_index_addition")
market_data.estimate_transaction_cost(event_type="sp500_index_addition")  # standalone

# Regime-filtered backtesting (VIX: "calm"<20, "elevated"20-30, "crisis">30)
market_data.measure_event_impact(event_dates=[...], regime_filter="calm")

# Cross-category FDR correction (Benjamini-Hochberg)
market_data.apply_cross_category_fdr({"earnings": 0.03, "fda": 0.001})

# Hypothesis lifecycle
research.create_hypothesis(...)   # see required fields below
research.activate_hypothesis(id, entry_price, position_size)
research.complete_hypothesis(hypothesis_id, exit_price, actual_return_pct, post_mortem,
    spy_return_pct=None, sector_etf_return_pct=None, confounders_at_exit=None,
    timing_accuracy=None, mechanism_validated=None,
    confounder_attribution=None, surprise_factor=None)
research.check_promotion_or_retirement(event_type)

# Data integrity (run at session start)
research.verify_data_integrity()  # returns {"ok": bool, "issues": [...]}

# Knowledge base — record_literature takes a dict, NOT a string
research.record_literature(event_type, {           # findings is a dict
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
```

## create_hypothesis() Validation Rules

These are enforced in code and will raise `ValueError` if violated:
- `expected_magnitude_pct` must be >= `min_abnormal_return_pct` (1.5%)
- Expected return after transaction costs must exceed 1.0% (use `estimate_transaction_cost()` for event-specific costs, default 0.1%)
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
result['entry_price_mode']           # "close" or "open"
result['avg_estimated_cost_pct']     # avg round-trip cost (when estimate_costs=True)
# Horizons: 1d, 3d, 5d, 10d, 20d. Types: raw, abnormal, sector_adj
# Per-event: result['individual_impacts'][i]['abnormal_1d'], ['event_date'], ['symbol']
# Per-event: ['entry_price_type'] ("close"/"open"), ['avg_daily_volume'], ['volume_ratio']
# Per-event: ['estimated_cost'] (when estimate_costs=True)
```

## Data Sources
- **yfinance**: Primary source for historical prices (free, no key needed)
- **Tiingo**: Fallback for delisted tickers (free tier, 500 req/day, needs `TIINGO_API_KEY` in `.env`)
- **SEC EDGAR**: Form 4 bulk data for insider transactions (free, no key needed)

## Alpaca Paper Trading
Cash: $100,000 | Fractional shares | Shorting enabled | $5,000 per experiment
