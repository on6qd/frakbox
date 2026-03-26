import os
from pathlib import Path

# Load .env if vars aren't already set (centralized — other modules import from here)
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# Tiingo (fallback for delisted tickers — free tier, 500 req/day)
TIINGO_API_KEY = os.environ.get("TIINGO_API_KEY", "")

# FRED (macro data — free, 120 req/min)
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

# Email reporting
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
REPORT_RECIPIENT = os.environ.get("REPORT_RECIPIENT", GMAIL_ADDRESS)

# ---------------------------------------------------------------------------
# Risk Configuration — single source of truth for all risk parameters
# ---------------------------------------------------------------------------
# Previously scattered across trader.py, methodology.json, market_data.py.
# All risk limits are defined here and imported by other modules.

# Per-position limits
MAX_POSITION_PCT = 0.05                # 5% of portfolio per experiment
DEFAULT_STOP_LOSS_PCT = 10.0           # close if position loses more than 10%
MIN_STOP_LOSS_PCT = 1.0               # absolute minimum — every trade MUST have a stop loss
DEFAULT_TAKE_PROFIT_PCT = None         # no take-profit by default (hold to deadline)
DEFAULT_POSITION_SIZE_USD = 5000       # default trade size in dollars

# Portfolio-level limits (paper account — optimized for research throughput)
MAX_PORTFOLIO_DRAWDOWN_PCT = 30.0      # paper account — don't halt research over paper losses
MAX_CONCURRENT_EXPERIMENTS = 10        # more concurrent experiments = faster signal validation

# Transaction cost assumptions
ESTIMATED_ROUND_TRIP_COST_PCT = 0.10   # default round-trip cost (spread + impact)
MIN_NET_RETURN_AFTER_COSTS_PCT = 1.0   # minimum expected net return to be viable
MIN_ABNORMAL_RETURN_PCT = 1.5          # minimum expected abnormal return

# Event-type cost defaults (round-trip %) — used when volume data unavailable
EVENT_COST_DEFAULTS = {
    "sp500_index_addition": 0.25,
    "sp500_index_deletion": 0.25,
    "fda_decision": 0.30,
    "insider_buying_cluster": 0.10,
    "earnings_surprise": 0.20,
}
DEFAULT_EVENT_COST_PCT = 0.10


def require_alpaca():
    """Raise if Alpaca credentials are missing. Call at point-of-use, not import time."""
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise RuntimeError(
            "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in .env or environment."
        )


def require_gmail():
    """Raise if Gmail credentials are missing. Call at point-of-use, not import time."""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        raise RuntimeError(
            "GMAIL_ADDRESS and GMAIL_APP_PASSWORD must be set in .env or environment."
        )


def require_fred():
    """Raise if FRED API key is missing. Call at point-of-use, not import time."""
    if not FRED_API_KEY:
        raise RuntimeError(
            "FRED_API_KEY must be set in .env or environment. "
            "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )


def load_env():
    """Explicitly load .env file. No-op if already loaded at import time.

    Provided for scripts that need to ensure env is loaded (trade_loop.py,
    health_check.py) without duplicating the parsing logic.
    """
    if _env_file.exists():
        for line in _env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))
