"""
cluster_auto_scanner.py - Daily automated scanner for insider cluster buying opportunities.

Runs at market close to find fresh clusters filed in the last 24 hours.
When a qualifying cluster is found, creates a research queue entry and
optionally sets a trigger on an existing hypothesis for next-day execution.

This prevents missing opportunities that go stale between LLM sessions.

Usage:
    python tools/cluster_auto_scanner.py [--dry-run] [--hours N]

Schedule via launchd to run daily at 4:15 PM ET after market close.

Qualifications (matching hypothesis 1cb6140f):
    - 3+ insiders buying within 30 days
    - Total purchase value >= $500K
    - Market cap >= $500M (prevents delistment failures)
    - Filed within last 24 hours (48 if run twice per day)
    - Not already in active/pending experiments

VIX + Cluster Size Trading Gate (full-population analysis, N=1566, 2021-2025):
    Refined 2026-03-22: n=6-9 is the OPTIMAL tier. n>=10 is NOISE (p=0.64, pos_rate=49%).
    The n>=10 signal disappears because 2021 IPO lockup expiry Form 4 filings contaminate
    the dataset (CXM, BRZE, IOT, GLUE, DOCS, DNUT etc. all filed en-masse at lockup expiry).

    Tier 1 - HIGH CONFIDENCE (VIX < 20):
        n=6-9: EV = +4.32% (post-2021), 60.6% consistency. PRIMARY SIGNAL.
        n=3-5: EV = +3.75% calm VIX. Trade freely.
        n>=10: EV = -0.08% (noise). DO NOT TRADE.
    Tier 2 - MODERATE (VIX 20-25, n >= 5):
        n=6-9: EV = +2.78%, 55.6% pos rate, p=0.0021. TRADEABLE.
        n=3-5: EV = +0.68%, p=0.27. NOT SIGNIFICANT. Only trade if high total value.
        n>=10: Still noise. DO NOT TRADE.
    Tier 3 - CONDITIONAL (VIX 25-30, n = 6-9):
        n=6-9: EV = +2.78%, 55.6% pos rate, p=0.0021. Tradeable.
        n=3-5: EV = -0.48%, 38% positive -- DO NOT TRADE.
        n>=10: Still noise. DO NOT TRADE.
    Tier 4 - DO NOT TRADE (VIX > 30):
        Crisis regime -- signal noisy, N=94. Avoid.

Key implication: When VIX is elevated (20-30), ONLY trade n=6-9 clusters.
    n=3-5 in elevated VIX = coin flip. n>=10 in any VIX = noise.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

import requests
import yfinance as yf

SEC_HEADERS = {"User-Agent": "research_bot cluster_scanner@research.com"}

# Cache for CIK lookups to avoid repeated HTTP requests
_cik_cache: dict[str, str] = {}

# CEO/CFO hypothesis ID (insider_buying_cluster_ceo_cfo, registered 2026-03-22)
CEO_CFO_HYPOTHESIS_ID = "2bbe0f04"


def is_ceo_or_cfo(title: str) -> bool:
    """Check if an insider title indicates CEO or CFO role."""
    if not title:
        return False
    t = title.upper()
    return any(kw in t for kw in [
        'CHIEF EXECUTIVE', 'CEO', 'CHIEF FINANCIAL', 'CFO',
        'CHIEF OPERATING', 'COO', 'PRESIDENT AND', 'PRESIDENT &',
    ])


def _load_cik_lookup() -> dict[str, str]:
    """Load EDGAR company_tickers.json and build ticker->CIK mapping."""
    try:
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=SEC_HEADERS,
            timeout=20,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        return {
            info["ticker"].upper(): str(info["cik_str"]).zfill(10)
            for info in data.values()
            if "ticker" in info and "cik_str" in info
        }
    except Exception:
        return {}


def get_cik_for_ticker(ticker: str) -> str | None:
    """Return zero-padded 10-digit CIK for a ticker, or None if not found."""
    global _cik_cache
    if not _cik_cache:
        _cik_cache = _load_cik_lookup()
    return _cik_cache.get(ticker.upper())


def verify_edgar_open_market_purchases(
    ticker: str,
    days_back: int = 35,
    min_value_usd: float = 50_000,
    verbose: bool = True,
) -> tuple[int, list[str], bool, list[str]]:
    """
    Verify a cluster candidate actually has open-market purchases (Form 4 code P)
    by fetching EDGAR Form 4 XMLs directly.

    OpenInsider sometimes counts RSU award grants (code A) as 'cluster buys',
    inflating the insider count. This function cross-checks against EDGAR source
    data and returns only insiders with confirmed open-market purchases above
    the minimum value threshold.

    Also extracts <officerTitle> from Form 4 XML for each buyer to determine
    whether a CEO or CFO is among the cluster buyers (hypothesis 2bbe0f04).

    Args:
        ticker: Stock ticker (e.g. 'POOL')
        days_back: Look-back window in calendar days (default 35 to cover 1-month clusters)
        min_value_usd: Minimum purchase value per insider to count (default $50K)
        verbose: If True, print progress

    Returns:
        (verified_n_insiders, list_of_insider_names, has_ceo_cfo, list_of_titles)
        Returns (0, [], False, []) on failure or if no qualifying purchases found.
    """
    cik = get_cik_for_ticker(ticker)
    if not cik:
        if verbose:
            print(f"  EDGAR VERIFY: No CIK found for {ticker} — skipping verification")
        return 0, [], False, []

    try:
        resp = requests.get(
            f"https://data.sec.gov/submissions/CIK{cik}.json",
            headers=SEC_HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            if verbose:
                print(f"  EDGAR VERIFY: Submissions API failed for {ticker} (HTTP {resp.status_code})")
            return 0, []
        data = resp.json()
    except Exception as e:
        if verbose:
            print(f"  EDGAR VERIFY: Request failed for {ticker}: {e}")
        return 0, [], False, []

    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    dates = filings.get("filingDate", [])
    acc_nums = filings.get("accessionNumber", [])

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    form4_list = [
        (d, a)
        for f, d, a in zip(forms, dates, acc_nums)
        if f == "4" and d >= cutoff
    ]

    if verbose:
        print(f"  EDGAR VERIFY: {len(form4_list)} Form 4 filings for {ticker} in last {days_back} days")

    # Parse each Form 4 XML — collect insiders with confirmed code-P purchases
    # above the minimum value threshold. Also extract officerTitle for CEO/CFO detection.
    buyers: dict[str, float] = {}  # name -> total purchase value
    buyer_titles: dict[str, str] = {}  # name -> officerTitle (if present)
    cik_int = str(int(cik))  # EDGAR folder uses non-zero-padded CIK

    for _filing_date, acc in form4_list:
        acc_nodash = acc.replace("-", "")
        folder_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/"
        try:
            fold_resp = requests.get(folder_url, headers=SEC_HEADERS, timeout=10)
            time.sleep(0.08)  # Respect SEC rate limit
            if fold_resp.status_code != 200:
                continue
        except Exception:
            continue

        xml_hrefs = re.findall(
            r'href="(/Archives/edgar/data[^"]*\.xml)"',
            fold_resp.text,
            re.IGNORECASE,
        )
        if not xml_hrefs:
            continue

        xml_url = "https://www.sec.gov" + xml_hrefs[0]
        try:
            xml_resp = requests.get(xml_url, headers=SEC_HEADERS, timeout=10)
            time.sleep(0.08)
            if xml_resp.status_code != 200:
                continue
        except Exception:
            continue

        xml = xml_resp.text

        # Only process filings that contain at least one open-market purchase
        if "<transactionCode>P</transactionCode>" not in xml:
            continue

        # Extract reporter name
        name_match = re.search(r"<rptOwnerName>(.*?)</rptOwnerName>", xml)
        if not name_match:
            continue
        name = name_match.group(1).strip()

        # Extract officer title from reportingOwnerRelationship block
        # This is present for officer-filers; absent for director-only filers.
        title_match = re.search(r"<officerTitle>(.*?)</officerTitle>", xml, re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""

        # Extract all non-derivative transactions (table 1)
        # Iterate over transaction blocks: look for P-code entries with shares/price
        # Pattern: find each <nonDerivativeTransaction> block
        transaction_blocks = re.findall(
            r"<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>",
            xml,
            re.DOTALL,
        )

        total_value = 0.0
        for block in transaction_blocks:
            code_m = re.search(r"<transactionCode>(.*?)</transactionCode>", block)
            if not code_m or code_m.group(1).strip() != "P":
                continue

            shares_m = re.search(
                r"<transactionShares>.*?<value>([\d.]+)</value>",
                block,
                re.DOTALL,
            )
            price_m = re.search(
                r"<transactionPricePerShare>.*?<value>([\d.]+)</value>",
                block,
                re.DOTALL,
            )
            if shares_m and price_m:
                try:
                    val = float(shares_m.group(1)) * float(price_m.group(1))
                    total_value += val
                except ValueError:
                    pass

        if total_value >= min_value_usd:
            buyers[name] = buyers.get(name, 0.0) + total_value
            if title and name not in buyer_titles:
                buyer_titles[name] = title

    verified = [(name, val) for name, val in buyers.items()]
    verified.sort(key=lambda x: -x[1])

    # Determine if any confirmed buyer has a CEO/CFO title
    titles_found = [buyer_titles.get(name, "") for name, _ in verified]
    has_ceo_cfo = any(is_ceo_or_cfo(t) for t in titles_found)

    if verbose:
        print(f"  EDGAR VERIFY: {len(verified)} unique insiders with open-market purchases >= ${min_value_usd:,.0f}")
        for name, val in verified:
            title_str = f" [{buyer_titles[name]}]" if name in buyer_titles else ""
            ceo_flag = " *** CEO/CFO ***" if is_ceo_or_cfo(buyer_titles.get(name, "")) else ""
            print(f"    {name}{title_str}{ceo_flag}: ${val:,.0f}")
        if has_ceo_cfo:
            print(f"  EDGAR VERIFY: CEO/CFO detected — qualifies for hypothesis {CEO_CFO_HYPOTHESIS_ID}")

    return len(verified), [name for name, _ in verified], has_ceo_cfo, titles_found

ET = ZoneInfo("America/New_York")

# Qualification thresholds (matching hypothesis 1cb6140f)
MIN_INSIDERS = 3
MIN_TOTAL_VALUE_K = 500    # $500K minimum
MIN_MARKET_CAP_M = 500     # $500M minimum
MAX_STALE_HOURS = 48       # Don't trade if cluster is older than 48 hours

# Hypothesis IDs to check for existing pending triggers
CLUSTER_HYPOTHESIS_IDS = ["1cb6140f", "76678219"]  # 3d and 5d cluster hypotheses

# VIX regime thresholds (from full-population analysis N=1566, 2021-2025)
# See VIX + Cluster Size Trading Gate in module docstring for full decision matrix.
VIX_CALM_THRESHOLD = 20.0      # VIX < 20: Tier 1 (HIGH CONFIDENCE, any cluster size)
VIX_MODERATE_THRESHOLD = 25.0  # VIX 20-25: Tier 2 (n>=5 recommended, n>=3 marginal)
VIX_ELEVATED_THRESHOLD = 30.0  # VIX 25-30: Tier 3 (n>=6 only, n<6 DO NOT TRADE)
# VIX > 30: Tier 4 (DO NOT TRADE - crisis regime, N=94 noisy)


def get_current_vix() -> tuple[float, str, str]:
    """
    Get current VIX level and classify by regime.

    Returns (vix_level, regime_label, confidence_label).
    regime_label: "calm" | "moderate" | "elevated" | "crisis"
    confidence_label: human-readable label for output/email
    """
    try:
        vix_data = yf.Ticker("^VIX").history(period="2d")
        if vix_data.empty:
            return (None, "unknown", "UNKNOWN (VIX data unavailable)")
        vix_level = float(vix_data['Close'].iloc[-1])
    except Exception:
        return (None, "unknown", "UNKNOWN (VIX fetch failed)")

    if vix_level < VIX_CALM_THRESHOLD:
        regime = "calm"
        label = f"TIER 1 (calm VIX {vix_level:.1f} < {VIX_CALM_THRESHOLD}, EV=+3.31%, any cluster size)"
    elif vix_level < VIX_MODERATE_THRESHOLD:
        regime = "moderate"
        label = f"TIER 2 (VIX {vix_level:.1f} 20-25, EV=+1.4-1.85%, prefer n>=5)"
    elif vix_level < VIX_ELEVATED_THRESHOLD:
        regime = "elevated"
        label = f"TIER 3 (elevated VIX {vix_level:.1f} 25-30, EV=+2.37% for n>=6 only, -0.48% for n<6)"
    else:
        regime = "crisis"
        label = f"TIER 4 / DO NOT TRADE (crisis VIX {vix_level:.1f} > {VIX_ELEVATED_THRESHOLD}, signal unreliable)"

    return (vix_level, regime, label)


def get_vix_action_recommendation(vix_regime: str, n_insiders: int, total_value_k: float) -> str:
    """
    Return a trading action recommendation based on VIX tier + cluster size.

    Uses the refined VIX + Cluster Size Trading Gate (N=1566, updated 2026-03-22).
    Key finding: n=6-9 is the optimal tier (regime-robust). n>=10 is NOISE. n=3-5 fails in elevated VIX.
    """
    # n>=10 is noise regardless of VIX (p=0.64, pos_rate=49% -- dominated by IPO lockup events)
    if n_insiders >= 10:
        return (f"DO NOT TRADE (n={n_insiders}>=10). "
                "Signal disappears for large clusters: p=0.64, pos_rate=49% (coin flip). "
                "Likely IPO/lockup Form 4 contamination or large-cap 10b5-1 plans. SKIP.")

    if vix_regime == "calm":
        if n_insiders >= 6:
            return (f"HIGH CONFIDENCE (VIX<20, n={n_insiders} in 6-9 tier). "
                    "EV=+4.32% post-2021, 60.6% pos rate. PRIMARY SIGNAL. Trade at next open.")
        else:
            return (f"HIGH CONFIDENCE (VIX<20, n={n_insiders}). "
                    "EV=+3.75% calm VIX, 56% pos rate. Trade at next open per standard protocol.")
    elif vix_regime == "moderate":
        if n_insiders >= 6:
            return (f"STRONG (VIX 20-25, n={n_insiders} in 6-9 tier). "
                    "EV=+2.78%, 55.6% pos rate, p=0.0021. n=6-9 is regime-robust. Proceed.")
        elif n_insiders >= 5:
            return (f"MODERATE (VIX 20-25, n={n_insiders}). "
                    "EV~+1.85%, p=0.032. Tradeable but marginal. Proceed if total value is high.")
        else:
            return (f"WEAK SIGNAL (VIX 20-25, n={n_insiders}<5). "
                    "EV=+0.68%, p=0.27 — NOT significant. Do not trade unless total>${total_value_k/1000:.1f}M>>$5M.")
    elif vix_regime == "elevated":
        if 6 <= n_insiders <= 9:
            return (f"CONDITIONAL STRONG (VIX 25-30, n={n_insiders} in 6-9 tier). "
                    "EV=+2.78%, 55.6% pos rate, p=0.0021. n=6-9 overcomes VIX penalty. Proceed.")
        elif n_insiders >= 5:
            return (f"MARGINAL (VIX 25-30, n={n_insiders}=5). "
                    "EV uncertain in elevated VIX at n=5. Only trade if cluster is exceptional (CEO + $5M+).")
        else:
            return (f"DO NOT TRADE (VIX 25-30, n={n_insiders}<5). "
                    "EV=-0.48%, 38% pos rate for n<5 in elevated VIX — historically a coin flip. SKIP.")
    elif vix_regime == "crisis":
        return (f"DO NOT TRADE (VIX>30 crisis regime). "
                "Signal unreliable in crisis. N=94, mixed results. Wait for VIX to fall below 25.")
    else:
        return "VIX regime unknown. Review manually before trading."


def get_current_market_cap_m(ticker: str) -> float:
    """Get current market cap in millions. Returns 0 if unavailable."""
    try:
        info = yf.Ticker(ticker).info
        mktcap = info.get("marketCap", 0) or 0
        return mktcap / 1_000_000
    except Exception:
        return 0.0


def find_fresh_clusters(hours: int = 48) -> list[dict]:
    """
    Fetch fresh insider clusters from OpenInsider filed within the last N hours.

    Returns list of qualifying clusters with details.
    """
    from tools.openinsider_scraper import get_cluster_buys

    cutoff = datetime.now(ET) - timedelta(hours=hours)

    clusters = get_cluster_buys(min_insiders=MIN_INSIDERS, days=7, min_value_k=100)

    fresh = []
    for c in clusters:
        # Check recency
        try:
            filing_date = datetime.strptime(c['filing_date'], '%Y-%m-%d')
            filing_date = filing_date.replace(tzinfo=ET)
            if filing_date < cutoff:
                continue
        except (ValueError, KeyError):
            continue

        # Check minimum thresholds
        n_insiders = c.get('n_insiders', 0)
        total_value_k = c.get('total_value_k', 0) or 0

        if n_insiders < MIN_INSIDERS:
            continue
        if total_value_k < MIN_TOTAL_VALUE_K:
            continue

        fresh.append(c)

    return fresh


def check_existing_positions(ticker: str) -> bool:
    """Return True if ticker already has an active experiment."""
    try:
        with open(BASE_DIR / "hypotheses.json") as f:
            hypotheses = json.load(f)
        for h in hypotheses:
            if (h.get("expected_symbol") == ticker and
                    h.get("status") in ["active", "pending"] and
                    h.get("trigger") is not None):
                return True
    except Exception:
        pass
    return False


def get_52w_position(ticker: str) -> dict:
    """Get price relative to 52-week high/low."""
    try:
        info = yf.Ticker(ticker).info
        price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
        high_52w = info.get("fiftyTwoWeekHigh", 0)
        low_52w = info.get("fiftyTwoWeekLow", 0)

        pct_from_high = (price - high_52w) / high_52w * 100 if high_52w > 0 else None
        pct_from_low = (price - low_52w) / low_52w * 100 if low_52w > 0 else None

        return {
            "price": price,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "pct_from_52w_high": pct_from_high,
            "pct_from_52w_low": pct_from_low,
        }
    except Exception:
        return {}


def set_cluster_trigger(ticker: str, hypothesis_id: str = "1cb6140f",
                         position_size: int = 5000, dry_run: bool = False) -> bool:
    """
    Set trigger on the cluster hypothesis for next market open.

    Creates a fresh hypothesis entry or sets trigger on existing one.
    Returns True if trigger was set.
    """
    hyp_path = BASE_DIR / "hypotheses.json"

    try:
        with open(hyp_path) as f:
            hypotheses = json.load(f)

        # Find the template hypothesis
        template = None
        for h in hypotheses:
            if h["id"] == hypothesis_id:
                template = h
                break

        if template is None:
            print(f"  ERROR: Hypothesis {hypothesis_id} not found")
            return False

        # Check if this ticker already has a pending trigger
        for h in hypotheses:
            if (h.get("expected_symbol") == ticker and
                    h.get("status") == "pending" and
                    h.get("trigger") is not None):
                print(f"  SKIP: {ticker} already has pending trigger")
                return False

        if dry_run:
            print(f"  DRY RUN: Would set trigger on {hypothesis_id} for {ticker}")
            return True

        # For auto-scanner, we log the opportunity but don't auto-create trades
        # The LLM agent should review and decide. We create a research queue entry instead.
        return True

    except Exception as e:
        print(f"  ERROR setting trigger: {e}")
        return False


def log_opportunity(cluster: dict, market_cap_m: float, position_52w: dict,
                    dry_run: bool = False):
    """Log a qualifying opportunity to the research queue for LLM review."""
    import research_queue

    ticker = cluster['ticker']
    company = cluster.get('company', ticker)
    n_insiders = cluster.get('n_insiders', 0)
    total_value_k = cluster.get('total_value_k', 0)
    filing_date = cluster.get('filing_date', '')
    price = cluster.get('price_per_share', 0)
    vix_label = cluster.get('vix_label', 'UNKNOWN')
    vix_regime = cluster.get('vix_regime', 'unknown')
    vix_level = cluster.get('vix_level')

    has_ceo_cfo = cluster.get('has_ceo_cfo', False)
    buyer_titles = cluster.get('buyer_titles', [])
    pct_from_high = position_52w.get('pct_from_52w_high', 0) or 0

    # Build regime-aware action recommendation (n_insiders-aware)
    action_note = get_vix_action_recommendation(vix_regime, n_insiders, total_value_k)

    # CEO/CFO presence note for hypothesis 2bbe0f04
    ceo_cfo_note = ""
    if has_ceo_cfo:
        ceo_cfo_titles = [t for t in buyer_titles if is_ceo_or_cfo(t)]
        ceo_cfo_note = (
            f" CEO/CFO PRESENT ({', '.join(ceo_cfo_titles)}): "
            f"also qualifies for hypothesis {CEO_CFO_HYPOTHESIS_ID} "
            f"(insider_buying_cluster_ceo_cfo, EV=+7.01% vs +4.56% baseline)."
        )

    vix_str = f"{vix_level:.1f}" if vix_level is not None else "N/A"
    description = (
        f"AUTO-DETECTED insider cluster: {ticker} ({company}). "
        f"{n_insiders} insiders filed {filing_date}. "
        f"Total value: ${total_value_k/1000:.1f}M. "
        f"Price: ${price:.2f} ({pct_from_high:.1f}% from 52W high). "
        f"Market cap: ${market_cap_m:.0f}M. "
        f"VIX={vix_str} -> {vix_label}. "
        f"QUALIFYING for hypothesis 1cb6140f (3d hold).{ceo_cfo_note} "
        f"{action_note}"
    )

    if dry_run:
        print(f"  DRY RUN: Would log to research queue: {description[:100]}...")
        return

    research_queue.add_research_task(
        category="insider_buying_cluster",
        question=description,
        priority=0,  # Highest priority
        reasoning=(
            f"Auto-detected cluster with {n_insiders} insiders, ${total_value_k/1000:.1f}M total. "
            f"Qualifying for 1cb6140f. Filed {filing_date}. Must act before 3d window expires. "
            f"VIX regime: {vix_label}. {action_note}"
            + (f" CEO/CFO present: also tag hypothesis {CEO_CFO_HYPOTHESIS_ID}." if has_ceo_cfo else "")
        )
    )

    # Also log to scanner_signals so the signal continuation pipeline can find it
    import db as _db
    _db.init_db()
    _db.append_scanner_signal('insider_cluster', {
        'ticker': ticker,
        'date': filing_date,
        'n_insiders': n_insiders,
        'total_value_k': total_value_k,
        'market_cap_m': market_cap_m,
        'vix_level': vix_level,
        'vix_label': vix_label,
        'has_ceo_cfo': has_ceo_cfo,
        'action': 'LONG at next market open',
        'hold_days': 3,
        'logged_at': datetime.now().isoformat(),
    })

    print(f"  LOGGED to research queue + scanner: {ticker}" + (" [CEO/CFO cluster]" if has_ceo_cfo else ""))


def scan(hours: int = 48, dry_run: bool = False, verbose: bool = True) -> list[dict]:
    """
    Main scan function. Returns list of qualifying opportunities.

    Args:
        hours: How far back to look (default 48h)
        dry_run: If True, log findings but don't modify any files
        verbose: Print progress
    """
    if verbose:
        print(f"=== Insider Cluster Auto-Scanner ===")
        print(f"Scanning for clusters filed in last {hours} hours...")
        print(f"Thresholds: {MIN_INSIDERS}+ insiders, ${MIN_TOTAL_VALUE_K}K+ value, ${MIN_MARKET_CAP_M}M+ market cap")
        print()

    # VIX regime check (from full-population analysis N=1566, 2021-2025)
    vix_level, vix_regime, vix_label = get_current_vix()
    if verbose:
        print(f"VIX Regime Check:")
        print(f"  Current VIX: {vix_level:.2f}" if vix_level is not None else "  Current VIX: UNAVAILABLE")
        print(f"  Signal regime: {vix_label}")
        if vix_regime == "calm":
            print(f"  -> Tier 1: Calm regime. n=6-9: EV=+4.32% (primary signal). n=3-5: EV=+3.75%. n>=10: SKIP (noise).")
        elif vix_regime == "moderate":
            print(f"  -> Tier 2: Moderate regime (VIX 20-25). n=6-9: EV=+2.78% p=0.002 (TRADEABLE). n=3-5: EV=+0.68% p=0.27 (WEAK). n>=10: SKIP.")
        elif vix_regime == "elevated":
            print(f"  -> Tier 3: Elevated regime (VIX 25-30). n=6-9 ONLY is tradeable (EV=+2.78%, p=0.002).")
            print(f"     n=3-5: EV=-0.48%, p=0.27 (coin flip). n>=10: noise. Skip all but n=6-9.")
        elif vix_regime == "crisis":
            print(f"  -> Tier 4: Crisis regime (VIX>30). DO NOT TRADE. Signal unreliable in crisis conditions.")
        else:
            print(f"  -> VIX regime unknown. Proceed with caution.")
        print()

    fresh_clusters = find_fresh_clusters(hours=hours)

    if verbose:
        print(f"Found {len(fresh_clusters)} clusters meeting insider/value thresholds")
        print()

    qualifying = []

    for cluster in fresh_clusters:
        ticker = cluster.get('ticker', '')
        if not ticker:
            continue

        if verbose:
            print(f"Checking {ticker} ({cluster.get('company', '')[:30]})...")

        # Check market cap
        mktcap_m = get_current_market_cap_m(ticker)
        if mktcap_m < MIN_MARKET_CAP_M:
            if verbose:
                print(f"  SKIP: Market cap ${mktcap_m:.0f}M < ${MIN_MARKET_CAP_M}M threshold")
            continue

        # EDGAR Form 4 verification: confirm OpenInsider counts are code-P open-market
        # purchases and not RSU award grants (code A). OpenInsider can inflate counts
        # by including non-purchase transactions (discovered 2026-03-22, POOL case).
        openinsider_n = cluster.get('n_insiders', 0)
        verified_n, verified_names, has_ceo_cfo, buyer_titles = verify_edgar_open_market_purchases(
            ticker, days_back=35, min_value_usd=50_000, verbose=verbose
        )
        if verified_n < MIN_INSIDERS:
            if verbose:
                print(
                    f"  SKIP: EDGAR verified only {verified_n} open-market buyers "
                    f"(OpenInsider reported {openinsider_n}, min={MIN_INSIDERS}). "
                    f"Likely RSU/award inflation."
                )
            continue
        if verbose and verified_n != openinsider_n:
            print(
                f"  NOTE: OpenInsider reported {openinsider_n} insiders; "
                f"EDGAR confirms {verified_n} open-market buyers."
            )
        # Update cluster with verified count and CEO/CFO flag
        cluster['n_insiders_verified'] = verified_n
        cluster['n_insiders_openinsider'] = openinsider_n
        cluster['n_insiders'] = verified_n  # Use verified count going forward
        cluster['has_ceo_cfo'] = has_ceo_cfo
        cluster['buyer_titles'] = buyer_titles

        # Get 52W position
        pos_52w = get_52w_position(ticker)

        # Check if already trading
        if check_existing_positions(ticker):
            if verbose:
                print(f"  SKIP: {ticker} already has active/pending position")
            continue

        # Qualifying!
        cluster['market_cap_m'] = mktcap_m
        cluster['position_52w'] = pos_52w
        cluster['vix_level'] = vix_level
        cluster['vix_regime'] = vix_regime
        cluster['vix_label'] = vix_label
        qualifying.append(cluster)

        if verbose:
            pct = pos_52w.get('pct_from_52w_high', 0) or 0
            ceo_flag = " [CEO/CFO PRESENT -> hypothesis 2bbe0f04]" if cluster.get('has_ceo_cfo') else ""
            print(f"  QUALIFYING: {cluster['n_insiders']} insiders, ${cluster.get('total_value_k', 0)/1000:.1f}M, "
                  f"${mktcap_m:.0f}M mktcap, {pct:.1f}% from 52W high{ceo_flag}")
            print(f"  Signal confidence: {vix_label}")

        # Log to research queue (includes CEO/CFO flag)
        log_opportunity(cluster, mktcap_m, pos_52w, dry_run=dry_run)

    if verbose:
        print(f"\n{'='*40}")
        print(f"VIX regime at scan time: {vix_label}")
        print(f"Qualifying opportunities: {len(qualifying)}")
        for q in qualifying:
            pct = q.get('position_52w', {}).get('pct_from_52w_high', 0) or 0
            q_vix_label = q.get('vix_label', 'unknown')
            print(f"  {q['ticker']}: {q['n_insiders']} insiders, ${q.get('total_value_k', 0)/1000:.1f}M, "
                  f"{pct:.1f}% from 52W high | {q_vix_label}")
        print()

        if not dry_run and qualifying:
            print("NEXT STEP: Set trigger on hypothesis 1cb6140f for qualifying tickers.")
            print("  1. Verify company fundamentals (not in financial distress)")
            print("  2. Verify no upcoming earnings that would confound the signal")
            print("  3. Check VIX regime + cluster size (refined rule, 2026-03-22):")
            print("     - VIX<20, n=6-9: PRIMARY SIGNAL (EV=+4.32%). Trade freely.")
            print("     - VIX<20, n=3-5: HIGH CONFIDENCE (EV=+3.75%). Trade freely.")
            print("     - VIX 20-30, n=6-9: TRADEABLE (EV=+2.78%, p=0.002). Proceed.")
            print("     - VIX 20-30, n=3-5: COIN FLIP (EV~+0%, p=0.27). Skip unless extraordinary.")
            print("     - Any VIX, n>=10: NOISE (p=0.64). DO NOT TRADE.")
            print("  4. Set trigger: h['trigger'] = 'next_market_open'")

    return qualifying


def main():
    parser = argparse.ArgumentParser(description="Scan for fresh insider cluster opportunities")
    parser.add_argument("--dry-run", action="store_true", help="Log findings without modifying files")
    parser.add_argument("--hours", type=int, default=48, help="How far back to scan (default: 48h)")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = parser.parse_args()

    results = scan(hours=args.hours, dry_run=args.dry_run, verbose=not args.quiet)

    # Exit code: 0 if no qualifying opportunities, 1 if opportunities found
    sys.exit(0 if not results else 0)  # Always exit 0 for cron compatibility


if __name__ == "__main__":
    main()
