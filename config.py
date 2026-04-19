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

# Focus discipline — prevent scatter
MAX_ACTIVE_SIGNAL_TYPES = 20           # raised 3→5→20 on 2026-03-30; see methodology_changelog. Cap was preventing deployment of validated signals. Real scatter protection is via session priorities.
MAX_CONCURRENT_PER_SIGNAL = 2          # max positions on same signal (correlated = 1 bet)

# Signal-family budgets — enforce when multiple pre-registered hypotheses share a
# parent trigger (e.g. VIX>30 fires 6 sibling sector-basket hypotheses at once).
# Without a family cap, $5K × 6 = $30K of exposure to one shared systemic signal.
# See knowledge: vix30_basket_internal_correlation_concentration_risk_2026_04_19.
#
# Format: family_id -> {
#   "max_total_usd": hard cap on total deployed across all family members,
#   "preferred_symbols": list — fill these first (orthogonal core),
#   "diversifier_max_count": N — only take top-N from non-preferred members,
# }
SIGNAL_FAMILY_BUDGETS = {
    "vix30_basket": {
        "max_total_usd": 10000,
        "preferred_symbols": ["XLB", "EFA"],     # orthogonal core per 2026-04-19 audit
        "diversifier_max_count": 1,              # pick 1 of {EEM,HYG,XME,SMH} — all correlate 0.67-0.76 pairwise
    },
}


def classify_signal_family(signal_type: str | None) -> str | None:
    """Map a hypothesis.signal_type to a family id, or None if no family.

    Centralized so trade_loop and any activator share the same logic.
    """
    if not signal_type:
        return None
    s = signal_type.lower()
    if s.startswith("vix30_"):
        return "vix30_basket"
    return None

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


# ---------------------------------------------------------------------------
# Subagent Model Configuration — controls which models run which tasks
# ---------------------------------------------------------------------------
ORCHESTRATOR_MODEL = "opus"      # strategic reasoning, hypothesis evaluation
REVIEWER_MODEL = "sonnet"        # self-review, post-mortems, methodology
DATA_INTERPRETER_MODEL = "haiku" # SEC filing interpretation, news extraction


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
