# Stock Market Causal Research

Learn to trade perfectly through causal research. Paper trading on Alpaca ($100K, $5K/experiment).

## Multi-Agent Architecture

The system uses three agents with model tiering to cut token costs:

| Agent | Model | Role |
|---|---|---|
| **Orchestrator** | Opus | Plans sessions, evaluates results, forms hypotheses, delegates work |
| **Reviewer** | Sonnet | Self-review, post-mortems, confidence scoring, methodology updates |
| **Data Worker** | Haiku | Interprets SEC filings, news, text extraction (rare — most data work is pure Python) |

Data-heavy tasks (backtesting, scanning, price fetching) run as pure Python via `data_tasks.py` — no LLM needed.

## Files

| File | Purpose |
|---|---|
| `db.py` | SQLite — all CRUD for hypotheses, knowledge, queue, task_results |
| `research.py` | Hypothesis lifecycle, knowledge base (uses db.py) |
| `market_data.py` | Prices, event impact, power analysis |
| `data_tasks.py` | CLI dispatcher — runs backtests/scans without LLM, stores results in SQLite |
| `self_review.py` | Confidence scoring, methodology |
| `research_queue.py` | Task queue, watchlist, handoffs (uses db.py) |
| `trader.py` | Paper trades via Alpaca |
| `trade_loop.py` | Deterministic loop — triggers, stops, reconciliation |
| `run.py` | `--status`, `--review`, `--context` (compressed state) |
| `email_report.py` | HTML email digest (uses db.py) |
| `config.py` | Risk parameters, subagent model config |
| `tools/` | Custom tools (insider scanners, largecap filter, date verifier, yfinance_utils) |

## Storage

SQLite `research.db` (WAL mode). Tables: hypotheses, known_effects, dead_ends, literature, research_queue, event_watchlist, session_priorities, session_handoff, task_results.

## Standard Backtest Workflow

Use `data_tasks.py` instead of calling `measure_event_impact()` directly — it stores full results in SQLite and returns compact summaries:

```bash
# Multi-symbol backtest
python3 data_tasks.py backtest --events '[{"symbol":"AAPL","date":"2024-01-15"}]'

# Single-symbol with multiple dates
python3 data_tasks.py backtest --symbol AAPL --dates '["2024-01-15","2024-04-20"]' --entry-price open

# Verify dates, filter symbols, fetch prices
python3 data_tasks.py verify-date --event "AAPL S&P 500 addition" --expected-date 2024-03-15
python3 data_tasks.py largecap-filter --symbols '["AAPL","MSFT","TINY"]'
python3 data_tasks.py price-history --symbol AAPL --days 90

# Retrieve full stored result if summary isn't enough
python3 data_tasks.py get-result --id T-abc12345
```

For direct Python use (in custom tools):
```python
from tools.verify_event_date import verify_event_date    # verify dates BEFORE backtesting
from tools.largecap_filter import filter_to_largecap      # filter >500M cap
from tools.yfinance_utils import safe_download, get_close_prices  # ALWAYS use (not raw yf.download)
result = market_data.measure_event_impact(event_dates=[...], entry_price="open")  # open for after-hours
```

## Trade Execution

Set triggers — `trade_loop.py` (every 2 min) handles execution, stops, reconciliation, emails.

```python
import db
db.update_hypothesis_fields(hypothesis_id,
    trigger="next_market_open",        # or "immediate" or "2026-06-07T09:30"
    trigger_position_size=5000,
    trigger_stop_loss_pct=10,
    trigger_take_profit_pct=15,        # optional, default None
)
```

## Data Sources

- **yfinance**: Historical prices (free) — always use `tools/yfinance_utils.py`
- **Tiingo**: Fallback for delisted tickers (needs `TIINGO_API_KEY`)
- **SEC EDGAR**: Form 4 bulk data (free) — use `display_names` not `entity_name`

## API Reference

For full function signatures (`create_hypothesis`, `measure_event_impact`, `complete_hypothesis`, etc.): **read `API_REFERENCE.md`** — only when you need a specific signature.
