"""
Market context tool — fetches current market-moving headlines and builds
a regime narrative for hypothesis creation.

Uses free RSS feeds (no API key needed):
- MarketWatch top stories (macro, geopolitical)
- CNBC economy
- CNBC market news

Also integrates GPR index and FRED macro snapshot for a complete picture.

Usage:
    from tools.market_context import get_market_context
    ctx = get_market_context()  # today's context
    ctx = get_market_context("2026-03-24")  # specific date
"""

import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path

import requests

# Daily cache — context only refreshes once per day
CACHE_DIR = Path.home() / ".market_context_cache"
CACHE_TTL_HOURS = 12  # refresh at most twice a day

RSS_FEEDS = {
    "marketwatch": {
        "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
        "label": "MarketWatch",
    },
    "cnbc_economy": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258",
        "label": "CNBC Economy",
    },
    "cnbc_market": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069",
        "label": "CNBC Markets",
    },
}

# Keywords that flag major market-moving themes
THEME_KEYWORDS = {
    "war": ["war", "invasion", "missile", "military", "troops", "nato", "conflict", "bombing"],
    "tariff": ["tariff", "trade war", "sanctions", "embargo", "import duty", "trade deal"],
    "fed": ["fed ", "federal reserve", "rate hike", "rate cut", "fomc", "powell", "monetary policy", "interest rate"],
    "recession": ["recession", "downturn", "contraction", "gdp decline", "economic slowdown"],
    "inflation": ["inflation", "cpi", "consumer prices", "price surge", "stagflation"],
    "banking_crisis": ["bank failure", "bank run", "banking crisis", "svb", "credit suisse", "systemic risk"],
    "energy": ["oil price", "crude oil", "opec", "energy crisis", "natural gas", "oil surge"],
    "earnings": ["earnings season", "earnings beat", "earnings miss", "profit warning", "revenue miss"],
    "election": ["election", "presidential", "midterm", "political uncertainty", "policy uncertainty"],
    "tech": ["ai bubble", "tech selloff", "tech rally", "nvidia", "magnificent seven"],
}


def _clean_html(text):
    """Strip HTML tags and decode entities."""
    text = unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_rss(url, max_items=15):
    """Fetch and parse an RSS feed. Returns list of {title, description, pub_date}."""
    try:
        resp = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (financial-research-bot)"
        })
        resp.raise_for_status()
    except Exception as e:
        print(f"[market_context] RSS fetch failed: {e}", file=sys.stderr)
        return []

    items = []
    try:
        root = ET.fromstring(resp.content)
        for item in root.iter("item"):
            title_el = item.find("title")
            desc_el = item.find("description")
            date_el = item.find("pubDate")
            if title_el is not None:
                items.append({
                    "title": _clean_html(title_el.text or ""),
                    "description": _clean_html(desc_el.text or "") if desc_el is not None else "",
                    "pub_date": date_el.text if date_el is not None else None,
                })
            if len(items) >= max_items:
                break
    except ET.ParseError as e:
        print(f"[market_context] RSS parse error: {e}", file=sys.stderr)

    return items


def _detect_themes(headlines):
    """Scan headlines for market-moving themes. Returns dict of theme -> matching headlines."""
    detected = {}
    for theme, keywords in THEME_KEYWORDS.items():
        matches = []
        for h in headlines:
            text = (h["title"] + " " + h["description"]).lower()
            for kw in keywords:
                if kw in text:
                    matches.append(h["title"])
                    break
        if matches:
            detected[theme] = matches
    return detected


def get_headlines(max_per_feed=10):
    """Fetch current headlines from all RSS feeds.

    Returns list of {title, description, source, pub_date}.
    """
    all_headlines = []
    for feed_key, feed_info in RSS_FEEDS.items():
        items = _fetch_rss(feed_info["url"], max_items=max_per_feed)
        for item in items:
            item["source"] = feed_info["label"]
        all_headlines.extend(items)
        print(f"[market_context] {feed_info['label']}: {len(items)} headlines", file=sys.stderr)
    return all_headlines


def _context_cache_path(date):
    return CACHE_DIR / f"context_{date}.json"


def _load_context_cache(date):
    path = _context_cache_path(date)
    if not path.exists():
        return None
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    if datetime.now() - mtime > timedelta(hours=CACHE_TTL_HOURS):
        return None
    try:
        with open(path) as f:
            print("[market_context] Using cached context", file=sys.stderr)
            return json.load(f)
    except Exception:
        return None


def _save_context_cache(date, ctx):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(_context_cache_path(date), "w") as f:
            json.dump(ctx, f, indent=2)
    except Exception as e:
        print(f"[market_context] Cache write failed: {e}", file=sys.stderr)


def get_market_context(date=None):
    """Build a complete market context snapshot.

    Combines:
    - Current news headlines and detected themes
    - GPR geopolitical risk level (if available)
    - FRED macro snapshot (if available)

    Cached for 12 hours — does NOT re-fetch every call.

    Returns dict suitable for attaching to hypothesis confounders/notes.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    # Check daily cache first
    cached = _load_context_cache(date)
    if cached is not None:
        return cached

    result = {
        "date": date,
        "headlines": [],
        "themes_detected": {},
        "narrative": "",
        "gpr": None,
        "macro": None,
    }

    # Fetch headlines
    headlines = get_headlines(max_per_feed=10)
    result["headlines"] = [h["title"] for h in headlines[:20]]  # top 20

    # Detect themes
    themes = _detect_themes(headlines)
    result["themes_detected"] = {t: len(matches) for t, matches in themes.items()}

    # GPR index
    try:
        from tools.gpr_index import get_gpr_context
        gpr = get_gpr_context(date)
        if "error" not in gpr:
            result["gpr"] = gpr
    except (ImportError, Exception) as e:
        print(f"[market_context] GPR unavailable: {e}", file=sys.stderr)

    # FRED macro snapshot
    try:
        from tools.fred_data import get_macro_snapshot
        macro = get_macro_snapshot(date)
        result["macro"] = macro
    except (ImportError, Exception) as e:
        print(f"[market_context] FRED unavailable: {e}", file=sys.stderr)

    # Build narrative
    parts = []

    # Theme narrative
    if themes:
        theme_labels = {
            "war": "active military conflict",
            "tariff": "trade war / tariff tensions",
            "fed": "Federal Reserve policy in focus",
            "recession": "recession fears",
            "inflation": "inflation concerns",
            "banking_crisis": "banking sector stress",
            "energy": "energy market disruption",
            "earnings": "earnings season dynamics",
            "election": "political / election uncertainty",
            "tech": "tech sector concentration risk",
        }
        active_themes = [theme_labels.get(t, t) for t in themes.keys()]
        parts.append("Headlines signal: " + ", ".join(active_themes))

    # GPR narrative
    gpr = result.get("gpr")
    if gpr and "error" not in gpr:
        level = gpr["gpr_level"]
        regime = gpr["gpr_regime"]
        trend = gpr["gpr_trend"]
        pct = gpr["gpr_percentile"]
        parts.append(
            f"Geopolitical risk: {regime} (GPR={level}, {trend}, "
            f"{pct}th percentile historically)"
        )

    # Macro narrative
    macro = result.get("macro")
    if macro:
        macro_parts = []
        if macro.get("rate_regime"):
            macro_parts.append(f"Fed {macro['rate_regime']}")
        if macro.get("yield_curve_status"):
            macro_parts.append(f"yield curve {macro['yield_curve_status']}")
        if macro.get("cpi_yoy_pct") is not None:
            macro_parts.append(f"CPI {macro['cpi_yoy_pct']}% YoY")
        if macro.get("fed_funds_rate") is not None:
            macro_parts.append(f"FFR {macro['fed_funds_rate']}%")
        if macro_parts:
            parts.append("Macro: " + ", ".join(macro_parts))

    result["narrative"] = ". ".join(parts) if parts else "No significant themes detected"

    _save_context_cache(date, result)
    return result


def get_context_note(date=None):
    """Return a one-line context string suitable for market_regime_note."""
    ctx = get_market_context(date)
    return ctx["narrative"]


if __name__ == "__main__":
    print("=== Market Context Test ===\n")
    ctx = get_market_context()
    print(f"Date: {ctx['date']}")
    print(f"\nNarrative: {ctx['narrative']}")
    print(f"\nThemes: {ctx['themes_detected']}")
    print(f"\nTop headlines:")
    for h in ctx["headlines"][:10]:
        print(f"  - {h}")
    if ctx["gpr"]:
        print(f"\nGPR: {ctx['gpr']['gpr_level']} ({ctx['gpr']['gpr_regime']}, {ctx['gpr']['gpr_trend']})")
    if ctx["macro"]:
        m = ctx["macro"]
        print(f"Macro: FFR={m.get('fed_funds_rate')}%, CPI={m.get('cpi_yoy_pct')}% YoY, "
              f"yield curve {m.get('yield_curve_status')}")
