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
    sample_size=6,                            # number of historical events
    consistency_pct=100.0,                    # % of events in predicted direction
    confounders={                             # all keys from methodology.json required
        "market_regime": "...",
        "sector_trend": "...",
        "concurrent_news": "..."
    },
    market_regime_note="...",                 # e.g. "tested across calm/elevated VIX"
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
    event_timing="after_hours",              # optional: "after_hours"/"pre_market"/"unknown"
    passes_multiple_testing=True,            # optional, from backtest result
    backtest_symbols=["AAPL", "MSFT"],       # optional list
    backtest_events=[{"symbol": "AAPL", "date": "2024-01-15"}]  # optional list
)
```

**Validation Rules** (enforced in code, raises `ValueError` if violated):
- `expected_magnitude_pct` must be >= `min_abnormal_return_pct` (1.5%)
- Expected return after transaction costs must exceed 1.0% (use `estimate_transaction_cost()` for event-specific costs, default 0.1%)
- All confounders from `methodology.json` must be provided (use "unknown" if no data)
- Historical evidence cannot be all placeholders — need real measured data
- Causal mechanism must satisfy 2 of 3 criteria
- `expected_symbol` can be "TBD" but must be resolved before `activate_hypothesis()`
- `out_of_sample_split.validation_indices` must have >= 3 entries

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
result['bootstrap_ci_abnormal_1d']    # {"point_estimate", "ci_lower", "ci_upper", "ci_excludes_zero"}
result['entry_price_mode']           # "close" or "open"
result['avg_estimated_cost_pct']     # avg round-trip cost (when estimate_costs=True)
# Horizons: 1d, 3d, 5d, 10d, 20d. Types: raw, abnormal, sector_adj
# Per-event: result['individual_impacts'][i]['abnormal_1d'], ['event_date'], ['symbol']
# Per-event: ['entry_price_type'] ("close"/"open"), ['avg_daily_volume'], ['volume_ratio']
# Per-event: ['estimated_cost'] (when estimate_costs=True)
```

## Custom Tools (tools/)

| Tool | Purpose |
|---|---|
| `insider_cluster_detector.py` | Find SEC Form 4 clusters from bulk quarterly data |
| `realtime_insider_scanner.py` | Scan recent Form 4 filings via EDGAR daily index |
| `openinsider_scraper.py` | Scrape OpenInsider cluster screener for fresh signals |
| `largecap_filter.py` | Filter event lists to >500M or >5B market cap (prevents yfinance delistment failures) |
| `verify_event_date.py` | Find actual crash date within ±5 days of expected date (prevents wrong-date backtests) |
| `activate_go_trade.py` | Activate GO insider cluster paper trade |
| `activate_wd_trade.py` | Activate WD insider cluster paper trade |
| `close_go_trade.py` | Close GO paper trade and complete hypothesis |
| `close_wd_trade.py` | Close WD paper trade and complete hypothesis |
| `cluster_auto_scanner.py` | Daily auto-scanner for fresh insider clusters (runs via launchd 9:15 PM ET) |
| `edgar_clinical_failure_scanner.py` | Scan EDGAR 8-K filings for Phase 2/3 clinical failures |

**Standard backtest workflow:**
```python
# 1. Verify dates BEFORE backtesting (prevents the #1 source of errors)
from tools.verify_event_date import verify_event_date
result = verify_event_date("AAPL", "2024-01-15", crash_threshold=-0.30, window_days=5)

# 2. Filter large-caps to avoid yfinance delistment failures
from tools.largecap_filter import filter_to_largecap
events = filter_to_largecap(events, min_market_cap=500_000_000)

# 3. Always use entry_price="open" for after-hours events
result = market_data.measure_event_impact(event_dates=[...], entry_price="open")
```

## Data Sources
- **yfinance**: Primary source for historical prices (free, no key needed)
- **Tiingo**: Fallback for delisted tickers (free tier, 500 req/day, needs `TIINGO_API_KEY` in `.env`)
- **SEC EDGAR**: Form 4 bulk data for insider transactions (free, no key needed)

## Alpaca Paper Trading
Cash: $100,000 | Fractional shares | Shorting enabled | $5,000 per experiment

## Trade Execution

Trades run on a **separate deterministic loop** (`trade_loop.py`, every 2 min via launchd).
You do NOT need to call `activate_hypothesis()` or `place_experiment()` directly.
Instead, set a trigger on the hypothesis and the trade loop will execute it.

```python
# Set a trigger — trade_loop.py will execute when the condition is met
hypotheses = research.load_hypotheses()
for h in hypotheses:
    if h["id"] == hypothesis_id:
        h["trigger"] = "next_market_open"       # execute at 9:30 ET
        # h["trigger"] = "immediate"            # execute now (if market open)
        # h["trigger"] = "2026-06-07T09:30"     # execute at specific time
        h["trigger_position_size"] = 5000       # optional, default $5000
        h["trigger_stop_loss_pct"] = 10         # optional, default 10%
        h["trigger_take_profit_pct"] = 15       # optional, default None
        break
research.save_hypotheses(hypotheses)
# Done — trade_loop.py handles the rest
```

The trade loop also handles:
- Stop-loss (default 10%), take-profit, deadline auto-close
- Position reconciliation (Alpaca vs hypotheses)
- Email notifications on every trade action
- Portfolio drawdown protection (blocks new trades at -15%)

Manual commands:
- `python trade_loop.py --dry-run` — show pending triggers without executing
- `python trader.py --check-stops` — run stop-loss check manually
