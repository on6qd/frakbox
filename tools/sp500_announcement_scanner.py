#!/usr/bin/env python3
"""
S&P 500 Announcement Scanner
Detects S&P 500 index addition announcements and flags tradeable windows.
Hypothesis 061ae3a8: S&P 500 addition, expected +5% at 5d (confidence 9).

Sources (in order):
  1. press.spglobal.com archive page  — primary, HTML scrape
  2. EDGAR 8-K full-text search       — secondary, catches company-filed notices

Run:  python tools/sp500_announcement_scanner.py
Schedule: Daily at 9:15 PM ET (add to launchd or cron alongside cluster_auto_scanner)
"""

import sys
import os
import re
import json
from datetime import datetime, timedelta

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HYPOTHESIS_ID = "061ae3a8"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
STATE_FILE = os.path.join(LOGS_DIR, "sp500_announcement_state.json")
os.makedirs(LOGS_DIR, exist_ok=True)

# Archive URL — returns HTML listing of press releases, filterable by keyword
PRESS_ARCHIVE_URL = "https://press.spglobal.com/index.php?keywords=s%26p+500+index&l=30&s=2429"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Keywords that signal an S&P 500 addition (checked against press release titles, lowercased)
ADDITION_TITLE_KEYWORDS = [
    "set to join s&p 500",
    "to join the s&p 500",
    "added to the s&p 500",
    "will join the s&p 500",
    "s&p 500 changes",
    "s&p 500 index changes",
    "s&p 500 rebalancing",
    "s&p 500 rebalanc",
]


# ------------------------------------------------------------------
# State helpers
# ------------------------------------------------------------------

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"seen_links": [], "last_check": None}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


# ------------------------------------------------------------------
# Title filter
# ------------------------------------------------------------------

def is_addition_title(title: str) -> bool:
    t = title.lower().replace("&amp;", "&")
    for kw in ADDITION_TITLE_KEYWORDS:
        if kw in t:
            return True
    if "s&p 500" in t and any(
        k in t for k in ("change", "recompos", "rebalanc", "addition", "join", "replac")
    ):
        return True
    return False


# ------------------------------------------------------------------
# Ticker extraction
# ------------------------------------------------------------------

# Match: NYSE: TICK  |  (NYSE: TICK)  |  Nasdaq: TICK  |  bare (TICK) after a company name
TICKER_RE = re.compile(
    r"""
    (?:
        \b(?:NYSE|Nasdaq|NASDAQ|NYSE\s*American|AMEX)\s*:\s*([A-Z]{1,5})\b   # exchange: TICK
        |
        \((?:NYSE|Nasdaq|NASDAQ|NYSE\s*American|AMEX)\s*:\s*([A-Z]{1,5})\)  # (exchange: TICK)
        |
        \b(?:NYSE|Nasdaq|NASDAQ|NYSE\s*American|AMEX)\s*\u2013\s*([A-Z]{1,5})\b  # exchange – TICK (en-dash)
    )
    """,
    re.VERBOSE,
)

# Words that are all-caps in press releases but are NOT tickers
FALSE_POS = {
    "SP", "ETF", "CEO", "CFO", "COO", "CTO", "EVP", "SVP", "NYSE", "NASDAQ",
    "USD", "USA", "THE", "FOR", "AND", "INC", "LLC", "LTD", "PLC", "CORP",
    "SPX", "SPY", "QQQ", "INDEX", "SPDJI", "DJIA", "ESG", "ETF",
}


def extract_tickers(text: str) -> list:
    tickers = set()
    for m in TICKER_RE.finditer(text):
        for grp in m.groups():
            if grp:
                tickers.add(grp.upper())
    return sorted(tickers - FALSE_POS)


# ------------------------------------------------------------------
# Effective date extraction
# ------------------------------------------------------------------

EFFECTIVE_DATE_RE = re.compile(
    r"""
    (?:
        effective\s+(?:before|prior\s+to)\s+the\s+open(?:\s+of\s+trading)?\s+on
        | effective\s+after\s+the\s+close\s+(?:of\s+trading\s+)?on
        | effective\s+on
        | prior\s+to\s+the\s+open\s+of\s+trading\s+on
        | before\s+the\s+open\s+of\s+trading\s+on
        | changes\s+(?:will\s+)?(?:be\s+)?effective\s+(?:before\s+the\s+open\s+of\s+trading\s+)?on
        | will\s+be\s+effective\s+before\s+the\s+open\s+of\s+trading\s+on
    )
    \s+
    (?:
        (?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+
    )?
    (?P<date>
        (?:January|February|March|April|May|June|July|August|September|October|November|December)
        \s+\d{1,2},?\s+\d{4}
        |
        \d{1,2}/\d{1,2}/\d{4}
        |
        \d{4}-\d{2}-\d{2}
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_effective_date(text: str):
    """Return first effective date found as 'YYYY-MM-DD', or None."""
    m = EFFECTIVE_DATE_RE.search(text)
    if not m:
        return None
    raw = m.group("date").strip().rstrip(",")
    for fmt in ("%B %d, %Y", "%B %d %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


# ------------------------------------------------------------------
# Parse press release date from URL slug
# ------------------------------------------------------------------

def date_from_url(url: str) -> str:
    """Extract YYYY-MM-DD from URL slug like /2026-03-06-..."""
    m = re.search(r"/(\d{4}-\d{2}-\d{2})-", url)
    return m.group(1) if m else ""


# ------------------------------------------------------------------
# Source 1: press.spglobal.com archive page
# ------------------------------------------------------------------

def check_press_archive(days_back=45):
    """
    Scrape the press.spglobal.com archive page for S&P 500 addition announcements.
    Returns (list_of_items, error_string_or_None).
    Each item: {"title", "link", "pub_date"}
    """
    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    try:
        resp = requests.get(PRESS_ARCHIVE_URL, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code} from press.spglobal.com archive"

        html = resp.text

        # Extract all press release links with titles
        # Pattern: <a href="/2026-03-06-...">Title text</a>
        link_re = re.compile(
            r'<a\s+[^>]*href="((?:https://press\.spglobal\.com)?/\d{4}-\d{2}-\d{2}-[^"]+)"[^>]*>'
            r'\s*(.*?)\s*</a>',
            re.DOTALL | re.IGNORECASE,
        )

        items = []
        seen_links = set()

        for m in link_re.finditer(html):
            href = m.group(1).strip()
            raw_title = re.sub(r"<[^>]+>", "", m.group(2))  # strip any inner HTML
            raw_title = re.sub(r"&amp;", "&", raw_title)
            raw_title = re.sub(r"&#\d+;", "", raw_title)
            raw_title = re.sub(r"\s+", " ", raw_title).strip()

            if not raw_title or not is_addition_title(raw_title):
                continue

            if not href.startswith("http"):
                href = "https://press.spglobal.com" + href

            pub_date = date_from_url(href)
            if pub_date and pub_date < cutoff:
                continue  # Too old

            if href in seen_links:
                continue
            seen_links.add(href)

            items.append({"title": raw_title, "link": href, "pub_date": pub_date})

        return items, None

    except requests.RequestException as e:
        return None, f"Network error: {e}"
    except Exception as e:
        return None, f"Unexpected error: {e}"


# ------------------------------------------------------------------
# Source 2: EDGAR 8-K full-text search
# ------------------------------------------------------------------

def check_edgar_8k(days_back=14):
    """
    Search EDGAR for 8-K filings where companies announce joining the S&P 500.
    Returns (list_of_items, error_string_or_None).
    """
    today = datetime.now()
    start_date = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")

    edgar_headers = {
        "User-Agent": "financial-research-agent admin@research.local",
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json",
    }

    url = (
        "https://efts.sec.gov/LATEST/search-index?q=%22added+to+the+S%26P+500%22"
        "+OR+%22will+join+the+S%26P+500%22+OR+%22set+to+join+the+S%26P+500%22"
        f"&dateRange=custom&startdt={start_date}&enddt={today.strftime('%Y-%m-%d')}"
        "&forms=8-K"
    )

    try:
        resp = requests.get(url, headers=edgar_headers, timeout=15)
        if resp.status_code != 200:
            return None, f"EDGAR HTTP {resp.status_code}"

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        items = []
        for hit in hits[:20]:
            src = hit.get("_source", {})
            entity = src.get("entity_name", "Unknown")
            file_date = src.get("file_date", "")
            items.append({
                "title": f"8-K: {entity} — S&P 500 addition",
                "link": "",
                "pub_date": file_date,
                "description": f"Entity: {entity}, Filed: {file_date}",
                "entity": entity,
            })
        return items, None

    except requests.RequestException as e:
        return None, f"EDGAR network error: {e}"
    except Exception as e:
        return None, f"EDGAR unexpected error: {e}"


# ------------------------------------------------------------------
# Fetch individual press release to extract structured data
# ------------------------------------------------------------------

def _cell_text(cell_html: str) -> str:
    """Strip HTML tags from a table cell and return clean text."""
    text = re.sub(r"<[^>]+>", " ", cell_html)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#\d+;", "", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_press_release_table(html: str) -> list:
    """
    Parse the structured table in S&P Global press releases.
    Table columns (in order): Effective Date | Index Name | Action | Company Name | Ticker | GICS Sector
    Returns list of dicts: {"effective_date": "YYYY-MM-DD", "index": "S&P 500", "action": "Addition",
                             "company": "...", "ticker": "XXX"}
    Only returns rows where Action == "Addition" and Index contains "S&P 500".
    """
    rows = []
    # Find all <tr> blocks
    tr_re = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    td_re = re.compile(r"<td\b[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)

    for tr_match in tr_re.finditer(html):
        row_html = tr_match.group(1)
        cells = [_cell_text(td.group(1)) for td in td_re.finditer(row_html)]
        if len(cells) < 5:
            continue

        # Expected: [date, index_name, action, company_name, ticker, ...]
        date_raw, index_name, action, company, ticker = cells[0], cells[1], cells[2], cells[3], cells[4]

        if "addition" not in action.lower():
            continue
        if "s&p 500" not in index_name.lower():
            continue

        # Parse date: "Mar 23, 2026" or "March 23, 2026"
        effective_date = None
        date_clean = date_raw.rstrip(",").strip()
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"):
            try:
                effective_date = datetime.strptime(date_clean, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                pass

        ticker_clean = re.sub(r"\s+", "", ticker).upper()
        if not ticker_clean or ticker_clean in FALSE_POS:
            continue

        rows.append({
            "effective_date": effective_date,
            "effective_date_raw": date_raw,
            "index": index_name,
            "action": action,
            "company": company,
            "ticker": ticker_clean,
        })

    return rows


def fetch_press_release(url: str) -> dict:
    """
    Fetch a single press release page. Extract tickers and effective date.
    Uses structured table parser first; falls back to regex extraction.
    Returns {"tickers": [...], "effective_date": "YYYY-MM-DD" or None,
             "additions": [{"ticker", "effective_date", "company"}]}
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {"tickers": [], "effective_date": None, "additions": []}

        html = resp.text

        # Primary: structured table parse
        additions = parse_press_release_table(html)
        if additions:
            tickers = [a["ticker"] for a in additions]
            # Pick the earliest effective date among S&P 500 additions
            dates = [a["effective_date"] for a in additions if a["effective_date"]]
            effective_date = min(dates) if dates else None
            return {"tickers": tickers, "effective_date": effective_date, "additions": additions}

        # Fallback: regex extraction from plain text
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"\s+", " ", text)

        idx = text.lower().find("s&p 500")
        snippet = text[max(0, idx - 300): idx + 3000] if idx >= 0 else text[:4000]

        tickers = extract_tickers(snippet)
        effective_date = parse_effective_date(snippet)

        return {"tickers": tickers, "effective_date": effective_date, "additions": []}

    except Exception as e:
        return {"tickers": [], "effective_date": None, "additions": [], "error": str(e)}


# ------------------------------------------------------------------
# Portfolio cross-check helpers
# ------------------------------------------------------------------

def get_covered_symbols() -> set:
    """Return symbols already on watchlist or in active/pending hypotheses."""
    covered = set()
    try:
        import research_queue
        rq = research_queue.load_queue()
        for item in rq.get("event_watchlist", []):
            sym = (item.get("symbol") or "").upper()
            if sym:
                covered.add(sym)
    except Exception:
        pass
    try:
        import research
        for h in research.load_hypotheses():
            if h.get("status") in ("pending", "active"):
                sym = (h.get("expected_symbol") or "").upper()
                if sym and sym != "TBD":
                    covered.add(sym)
    except Exception:
        pass
    return covered


# ------------------------------------------------------------------
# Build actionable signals
# ------------------------------------------------------------------

def build_signals(announcements: list) -> list:
    today = datetime.now().date()
    signals = []

    for ann in announcements:
        pub_date = ann.get("pub_date", "")
        try:
            announced_date = datetime.strptime(pub_date[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            announced_date = today

        # Prefer structured additions (one per ticker with individual effective dates)
        additions = ann.get("additions", [])
        if additions:
            for a in additions:
                effective_str = a.get("effective_date")
                effective_date = None
                if effective_str:
                    try:
                        effective_date = datetime.strptime(effective_str, "%Y-%m-%d").date()
                    except ValueError:
                        pass
                days_until = (effective_date - today).days if effective_date else None
                is_open = effective_date is None or days_until is None or days_until >= 0
                if is_open:
                    signals.append({
                        "symbol": a["ticker"],
                        "company": a.get("company", ""),
                        "event": "sp500_addition",
                        "announced": str(announced_date),
                        "effective": effective_str,
                        "days_until_effective": days_until,
                        "hypothesis_id": HYPOTHESIS_ID,
                        "source": ann.get("source", "unknown"),
                        "press_release_title": ann.get("title", ""),
                        "press_release_link": ann.get("link", ""),
                    })
        else:
            # Fallback: use regex-extracted tickers with single effective date
            effective_str = ann.get("effective_date")
            tickers = ann.get("tickers", [])
            effective_date = None
            if effective_str:
                try:
                    effective_date = datetime.strptime(effective_str, "%Y-%m-%d").date()
                except ValueError:
                    pass

            for ticker in tickers:
                days_until = (effective_date - today).days if effective_date else None
                is_open = effective_date is None or days_until is None or days_until >= 0
                if is_open:
                    signals.append({
                        "symbol": ticker,
                        "event": "sp500_addition",
                        "announced": str(announced_date),
                        "effective": effective_str,
                        "days_until_effective": days_until,
                        "hypothesis_id": HYPOTHESIS_ID,
                        "source": ann.get("source", "unknown"),
                        "press_release_title": ann.get("title", ""),
                        "press_release_link": ann.get("link", ""),
                    })

    return signals


# ------------------------------------------------------------------
# Main scan
# ------------------------------------------------------------------

def run_scan(verbose=True, days_back=45) -> list:
    """
    Run full scan. Returns list of actionable signal dicts.
    Signals with days_until_effective >= 0 (or None) represent open trading windows.
    """

    def log(msg=""):
        if verbose:
            print(msg)

    log("=== S&P 500 Announcement Scanner ===")
    log(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Hypothesis: {HYPOTHESIS_ID}  |  Expected: +5% at 5d  |  Confidence: 9")
    log()

    state = load_state()
    seen_links = set(state.get("seen_links", []))
    all_raw = []

    # --- Source 1: press.spglobal.com archive ---
    log(f"[1/2] press.spglobal.com archive (last {days_back} days)...")
    archive_items, archive_err = check_press_archive(days_back=days_back)
    if archive_err:
        log(f"      FAILED: {archive_err}")
    elif archive_items is not None:
        log(f"      Found {len(archive_items)} S&P 500 addition press release(s)")
        for item in archive_items:
            item["source"] = "press.spglobal.com"
            all_raw.append(item)
    else:
        log("      No items returned")

    # --- Source 2: EDGAR 8-K ---
    log("[2/2] EDGAR 8-K full-text search (last 14 days)...")
    edgar_items, edgar_err = check_edgar_8k(days_back=14)
    if edgar_err:
        log(f"      FAILED: {edgar_err}")
    elif edgar_items is not None:
        log(f"      Found {len(edgar_items)} S&P 500 addition 8-K(s)")
        for item in edgar_items:
            item["source"] = "EDGAR 8-K"
            all_raw.append(item)
    else:
        log("      No items returned")

    log()

    if not all_raw:
        log("No announcements found.")
        state["last_check"] = datetime.now().isoformat()
        save_state(state)
        return []

    # Deduplicate by link (new items only)
    new_items = []
    for item in all_raw:
        link = item.get("link", "")
        if link and link in seen_links:
            continue  # Already processed
        new_items.append(item)
        if link:
            seen_links.add(link)

    log(f"Total: {len(all_raw)} announcement(s), {len(new_items)} new since last check")
    log()

    # Enrich each new item with tickers + effective date
    log("Extracting tickers and effective dates...")
    for item in new_items:
        link = item.get("link", "")
        log(f"  [{item.get('pub_date', '?')}] {item.get('title', '')[:80]}")
        if link and link.startswith("http"):
            parsed = fetch_press_release(link)
        else:
            desc = item.get("description", item.get("title", ""))
            parsed = {
                "tickers": extract_tickers(desc),
                "effective_date": parse_effective_date(desc),
                "additions": [],
            }
        item["tickers"] = parsed["tickers"]
        item["effective_date"] = parsed["effective_date"]
        item["additions"] = parsed.get("additions", [])
        if parsed.get("additions"):
            for a in parsed["additions"]:
                log(f"    + S&P 500 Addition: {a['ticker']} ({a['company']}) "
                    f"eff. {a['effective_date'] or a.get('effective_date_raw', '?')}")
        else:
            log(f"    Tickers (regex): {parsed['tickers'] or '(none)'}")
            log(f"    Effective date: {parsed['effective_date'] or '(not found)'}")

    log()

    # Build signals
    signals = build_signals(new_items)
    covered = get_covered_symbols()

    new_signals = [s for s in signals if s["symbol"] not in covered]
    already_covered = [s for s in signals if s["symbol"] in covered]

    log("=== RESULTS ===")
    if not signals:
        log("No actionable signals. Either no tickers were extracted, all windows "
            "have closed, or announcements were already seen.")
    else:
        if already_covered:
            log(f"Already in watchlist/active: {sorted({s['symbol'] for s in already_covered})}")

        if new_signals:
            log(f"NEW OPEN TRADING WINDOWS ({len(new_signals)} symbol(s)):")
            log()
            for s in new_signals:
                eff = s["effective"] or "unknown"
                days_str = (
                    f"{s['days_until_effective']}d until effective"
                    if s["days_until_effective"] is not None
                    else "effective date unknown — check manually"
                )
                log(f"  SIGNAL  {s['symbol']}")
                log(f"    Announced:  {s['announced']}")
                log(f"    Effective:  {eff}  ({days_str})")
                log(f"    Hypothesis: {s['hypothesis_id']}")
                log(f"    Source:     {s['source']}")
                log(f"    Title:      {s['press_release_title'][:70]}")
                log()
                log(f"  ACTION: Enter LONG {s['symbol']} at next market open")
                log(f"          Set trigger on hypothesis {HYPOTHESIS_ID}")
                log(f"          $5,000 position, 5-day hold")
                log()
            log()
            log("STRUCTURED OUTPUT (JSON):")
            print(json.dumps(new_signals, indent=2))
        else:
            log("All extracted signals already covered in watchlist/active hypotheses.")

    # Persist state
    state["last_check"] = datetime.now().isoformat()
    state["seen_links"] = sorted(seen_links)
    save_state(state)

    return signals


if __name__ == "__main__":
    run_scan(verbose=True)
    sys.exit(0)
