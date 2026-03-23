# Stock Market Causal Research

Learn to trade perfectly through causal research. Paper trading on Alpaca ($100K, $5K/experiment).

## Files

| File | Purpose |
|---|---|
| `db.py` | SQLite — all CRUD for hypotheses, knowledge, queue |
| `research.py` | Hypothesis lifecycle, knowledge base (uses db.py) |
| `market_data.py` | Prices, event impact, power analysis |
| `self_review.py` | Confidence scoring, methodology |
| `research_queue.py` | Task queue, watchlist, handoffs (uses db.py) |
| `trader.py` | Paper trades via Alpaca |
| `trade_loop.py` | Deterministic loop — triggers, stops, reconciliation |
| `run.py` | `--status`, `--review`, `--context` (compressed state) |
| `email_report.py` | HTML email digest (uses db.py) |
| `config.py` | Risk parameters (MAX_POSITION_PCT=5%, STOP_LOSS=10%, DRAWDOWN=15%) |
| `tools/` | Custom tools (insider scanners, largecap filter, date verifier, yfinance_utils) |

## Storage

SQLite `research.db` (WAL mode). Tables: hypotheses, known_effects, dead_ends, literature, research_queue, event_watchlist, session_priorities, session_handoff.

## Standard Backtest Workflow

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
