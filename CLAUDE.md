# Stock Market Causal Research Project

## Mission
Discover cause-and-effect relationships between real-world events and stock price movements. Paper trading on Alpaca ($100,000) validates hypotheses. P&L doesn't matter — learning does.

## Files

### Core research
| File | Purpose |
|---|---|
| `research.py` | Hypothesis lifecycle, knowledge base, pattern library, promotion/retirement |
| `market_data.py` | Historical prices, event impact measurement, power analysis, contamination checks |
| `self_review.py` | Meta-learning: confidence calibration, category analysis, weekly diagnostics |
| `research_queue.py` | Task queue, event watchlist, cross-session handoffs |
| `trader.py` | Paper trades via Alpaca |

### Data (JSON state)
| File | Purpose |
|---|---|
| `hypotheses.json` | All hypotheses (pending, active, completed, invalidated) |
| `patterns.json` | Validated pattern library |
| `knowledge_base.json` | Literature reviews, known effects, dead ends |
| `methodology.json` | Living research parameters (auto-updated by self-review) |
| `research_queue.json` | Research tasks, event watchlist, session handoffs |
| `results.jsonl` | Pre-registrations and completed experiment results |

### Logs
| File | Purpose |
|---|---|
| `logs/research_notes.md` | Cumulative research journal (append-only) |
| `logs/sessions.jsonl` | Structured session log |
| `logs/session_state.json` | Crash recovery state |
| `logs/friction_log.jsonl` | What slows you down — drives process improvement |

### Operations
| File | Purpose |
|---|---|
| `daily_research.sh` | Headless session runner |
| `run.py` | `python run.py --status` or `--review` |
| `email_report.py` | HTML email digest (called automatically by shell harness) |
| `smoke_test.py` | End-to-end pipeline validation |
| `health.sh` | Session history, research progress, scheduler status |
| `tools/` | Researcher-built tools and scripts |

### Reference
| File | Purpose |
|---|---|
| `METHODOLOGY.md` | Full research methodology reference |
| `methodology.json` | Current parameter values (evolves through self-review) |
| `.claude/agents/financial-researcher.md` | Agent constitution (immutable) |

## API Quick Reference

```python
# Backtesting — multi-symbol
market_data.measure_event_impact(event_dates=[
    {"symbol": "AAPL", "date": "2024-01-15", "timing": "after_hours"},
    {"symbol": "MSFT", "date": "2024-04-20"},
], benchmark="SPY", sector_etf="XLK")

# Single-symbol shorthand
market_data.measure_event_impact("AAPL", ["2024-01-15", "2024-04-20"])

# Out-of-sample validation (temporal)
research.validate_out_of_sample(evidence)  # auto-splits by date
research.validate_out_of_sample(evidence, discovery_cutoff_date="2023-01-01")

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
research_queue.add_research_task(category, question, priority, reasoning, depends_on="task-id")
research_queue.complete_research_task(task_id, findings)
research_queue.add_event_to_watchlist(event, date, symbol, hypothesis)
research_queue.set_next_session_priorities(priorities, handoff={...})

# Self-review
self_review.compute_confidence_score(sample_size, consistency, avg_return, stdev,
    literature_strength="none"|"partial"|"strong")
self_review.needs_bootstrap_review(completed_count)
self_review.run_bootstrap_review(completed_hypotheses)
self_review.check_knowledge_decay()
self_review.run_weekly_research_diagnostic()
```

## Alpaca Paper Trading
Cash: $100,000 | Fractional shares | Shorting enabled
Base: https://paper-api.alpaca.markets
