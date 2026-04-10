#!/usr/bin/env python3
"""Real-time SEO bought-deal scanner.

Checks EDGAR EFTS for recent 424B4 filings and cross-references with same-CIK 8-K
filings within 1 business day. Outputs GO/NO-GO for each candidate.

Run daily after 18:00 ET (EDGAR filings typically appear by 17:30 ET).

Usage:
    python tools/seo_realtime_scanner.py              # scan last 3 days
    python tools/seo_realtime_scanner.py --days 7     # scan last 7 days
    python tools/seo_realtime_scanner.py --evaluate   # also run GO/NO-GO filter
"""
import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, '/Users/frakbox/Bots/financial_researcher')

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None

HEADERS = {"User-Agent": "financial-researcher research@example.com"}
MIN_MARKET_CAP = 500_000_000  # $500M


def search_efts(form_type: str, start_date: str, end_date: str) -> list[dict]:
    """Search EDGAR EFTS for filings by form type and date range."""
    url = (
        f"https://efts.sec.gov/LATEST/search-index"
        f"?forms={form_type}"
        f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"EFTS error: {resp.status_code}")
        return []

    data = resp.json()
    total = data.get("hits", {}).get("total", {}).get("value", 0)
    hits = data.get("hits", {}).get("hits", [])

    # EFTS returns max 10 by default. Paginate if needed.
    all_hits = list(hits)
    if total > 10:
        # Fetch remaining pages
        for offset in range(10, min(total, 100), 10):
            url_page = url + f"&from={offset}"
            resp2 = requests.get(url_page, headers=HEADERS, timeout=30)
            if resp2.status_code == 200:
                page_hits = resp2.json().get("hits", {}).get("hits", [])
                all_hits.extend(page_hits)
            time.sleep(0.2)

    results = []
    for h in all_hits:
        src = h.get("_source", {})
        ciks = src.get("ciks", [])
        names = src.get("display_names", [])
        file_date = src.get("file_date", "")

        # Extract ticker from display_name: "Company Name  (TICK)  (CIK ...)"
        ticker = None
        if names:
            m = re.search(r'\(([A-Z]{1,5})\)', names[0])
            if m:
                ticker = m.group(1)

        results.append({
            "cik": ciks[0].lstrip("0") if ciks else "",
            "display_name": names[0] if names else "",
            "ticker": ticker,
            "file_date": file_date,
            "form_type": form_type,
        })

    return results


def check_cik_for_8k(cik: str, around_date: str, window_days: int = 3) -> str | None:
    """Check if a CIK has an 8-K filing within window_days of the given date.

    Uses EDGAR submissions API (per-company, not global search).
    Returns the 8-K filing date if found, else None.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])

        target = datetime.strptime(around_date, "%Y-%m-%d")
        for form, fdate in zip(forms, dates):
            if form == "8-K":
                d = datetime.strptime(fdate, "%Y-%m-%d")
                if abs((d - target).days) <= window_days:
                    return fdate
    except Exception:
        pass
    return None


def find_realtime_bought_deals(days: int = 3) -> list[dict]:
    """Find potential bought deals from the last N days.

    Strategy: get all 424B4 filings (few per week), then for each,
    check that specific company's filing history for a nearby 8-K.
    This avoids the EFTS 8-K volume limit.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    print(f"Scanning EDGAR for 424B4 filings: {start_date} to {end_date}")

    # Fetch 424B4 filings (few per week — EFTS handles this fine)
    filings_424 = search_efts("424B4", start_date, end_date)
    print(f"  424B4 filings found: {len(filings_424)}")

    # For each 424B4, check that company's own filing history for 8-K
    bought_deals = []
    for f424 in filings_424:
        cik = f424["cik"]
        if not cik:
            continue

        eightk_date = check_cik_for_8k(cik, f424["file_date"], window_days=3)
        time.sleep(0.12)  # SEC rate limit

        if eightk_date:
            f424_dt = datetime.strptime(f424["file_date"], "%Y-%m-%d")
            eightk_dt = datetime.strptime(eightk_date, "%Y-%m-%d")
            gap = abs((f424_dt - eightk_dt).days)

            bought_deals.append({
                "cik": cik,
                "ticker": f424["ticker"],
                "display_name": f424["display_name"],
                "filing_date_424b4": f424["file_date"],
                "filing_date_8k": eightk_date,
                "gap_days": gap,
            })

    print(f"  Matched bought deals (424B4 + 8-K within 3 days): {len(bought_deals)}")
    return bought_deals


def evaluate_candidates(deals: list[dict]) -> list[dict]:
    """Filter deals to tradeable candidates (market cap, liquidity)."""
    if not yf:
        print("yfinance not available — cannot evaluate")
        return deals

    evaluated = []
    for d in deals:
        ticker = d.get("ticker")
        if not ticker:
            d["decision"] = "SKIP_NO_TICKER"
            evaluated.append(d)
            continue

        try:
            info = yf.Ticker(ticker).info
            mc = info.get("marketCap", 0) or 0
            avg_vol = info.get("averageVolume", 0) or 0
            price = info.get("currentPrice") or info.get("regularMarketPrice", 0) or 0
            sector = (info.get("sector") or "").lower()
            industry = (info.get("industry") or "").lower()

            d["market_cap"] = mc
            d["avg_volume"] = avg_vol
            d["price"] = price
            d["sector"] = sector
            d["industry"] = industry
            # Biotech/healthcare flag — see seo_biotech_vs_nonbiotech_split_2026_04_11.
            # Biotech SEO 5d neg_rate 53.6% p=0.42 (weak) vs non-biotech 64.5% p=0.020.
            # Not yet a hard exclusion (biotech N=28 underpowered). Flagged for tracking.
            is_biotech = sector == "healthcare" and any(
                k in industry for k in ["biotech", "drug", "pharma", "medical"]
            )
            d["is_biotech"] = is_biotech

            if mc < MIN_MARKET_CAP:
                d["decision"] = f"SKIP_SMALL_CAP (${mc/1e6:.0f}M)"
            elif avg_vol < 100_000:
                d["decision"] = f"SKIP_LOW_VOLUME ({avg_vol:,})"
            elif price < 5:
                d["decision"] = f"SKIP_PENNY_STOCK (${price:.2f})"
            else:
                d["decision"] = "GO_BIOTECH_WEAKER" if is_biotech else "GO"
                d["position_size"] = 5000
                d["shares_to_short"] = int(5000 / price) if price > 0 else 0
        except Exception as e:
            d["decision"] = f"SKIP_ERROR ({e})"

        evaluated.append(d)
        time.sleep(0.1)

    return evaluated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time SEO bought-deal scanner")
    parser.add_argument("--days", type=int, default=3, help="Look back N days (default 3)")
    parser.add_argument("--evaluate", action="store_true", help="Run GO/NO-GO evaluation")
    args = parser.parse_args()

    deals = find_realtime_bought_deals(days=args.days)

    if args.evaluate:
        deals = evaluate_candidates(deals)

    # Deduplicate: same ticker seen multiple times
    seen_tickers: set = set()
    unique_deals = []
    for d in deals:
        t = d.get("ticker") or d.get("cik")
        if t not in seen_tickers:
            seen_tickers.add(t)
            unique_deals.append(d)
    deals = unique_deals

    # Output
    print(f"\n{'='*60}")
    print(f"BOUGHT DEAL CANDIDATES ({len(deals)} found)")
    print(f"{'='*60}")

    go_count = 0
    for d in deals:
        decision = d.get("decision", "UNEVAL")
        mc_str = f"${d.get('market_cap', 0)/1e6:.0f}M" if d.get("market_cap") else "N/A"
        ticker = d.get("ticker", "N/A")
        bio_tag = " [BIOTECH]" if d.get("is_biotech") else ""
        print(f"  {decision:30s} | {(ticker or 'N/A'):8s} | {mc_str:>10s} | {d.get('display_name', '')[:40]}{bio_tag}")
        print(f"    424B4: {d['filing_date_424b4']}  8-K: {d['filing_date_8k']}  gap: {d['gap_days']}d")
        if decision.startswith("GO"):
            go_count += 1
            print(f"    -> SHORT {d['shares_to_short']} shares @ ~${d.get('price', 0):.2f}")
            if d.get("is_biotech"):
                print(f"    ⚠ Biotech SEO signal is weaker (N=28 5d p=0.42 vs non-biotech p=0.020). Track separately.")

    # JSON output for automation
    output = {
        "scan_time": datetime.now().isoformat(),
        "days_scanned": args.days,
        "total_found": len(deals),
        "go_count": go_count,
        "candidates": [d for d in deals if d.get("decision") == "GO"],
    }
    print(f"\n{json.dumps(output, indent=2, default=str)}")
