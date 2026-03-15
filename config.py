import os

ALPACA_API_KEY = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
ALPACA_BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# Max percentage of portfolio per experiment
MAX_POSITION_PCT = 0.05  # 5% per hypothesis test

# Email reporting
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
REPORT_RECIPIENT = os.environ.get("REPORT_RECIPIENT", GMAIL_ADDRESS)
