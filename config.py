import os
from pathlib import Path

# Load .env if vars aren't already set
_env_file = Path(__file__).parent / ".env"
if _env_file.exists() and "ALPACA_API_KEY" not in os.environ:
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))

ALPACA_API_KEY = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# Max percentage of portfolio per experiment
MAX_POSITION_PCT = 0.05  # 5% per hypothesis test

# Email reporting
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
REPORT_RECIPIENT = os.environ.get("REPORT_RECIPIENT", GMAIL_ADDRESS)
