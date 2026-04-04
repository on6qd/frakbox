"""
SEC EDGAR Real-Time Insider Buying Cluster Scanner

Replaces OpenInsider.com dependency (134 friction events from downtime) with
direct EDGAR EFTS (Electronic Full-Text Search System) queries.

Scans recent Form 4 filings for open-market purchases (code "P") and identifies
insider buying clusters: 3+ unique corporate insiders each buying >$50K within
30 days at the same company.

Usage:
    python3 tools/edgar_realtime_insider_scanner.py [--days 14] [--min-insiders 3] [--min-value 50000] [--output json|table]

    # Programmatic use:
    from tools.edgar_realtime_insider_scanner import scan_insider_clusters
    clusters = scan_insider_clusters(days=14, min_insiders=3, min_value=50000)

Data source: SEC EDGAR EFTS full-text search + Form 4 XML filings
"""

import os
import re
import sys
import json
import time
import hashlib
import pickle
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_AGENT = os.environ.get(
    "SEC_USER_AGENT", "Financial Research Bot contact@example.com"
)
CACHE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "edgar_form4_cache"
)

EFTS_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
EDGAR_FILING_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

# SEC rate limiting: max 10 req/sec -> 0.12s between requests (with margin)
SEC_DELAY = 0.12
SEC_TIMEOUT = 20
SEC_MAX_RETRIES = 3
SEC_RETRY_BACKOFF = 2.0

# EFTS returns at most 100 results per page
EFTS_PAGE_SIZE = 100

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_last_request_time = 0.0


def _rate_limit():
    """Enforce SEC rate limit (max 10 requests/second)."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < SEC_DELAY:
        time.sleep(SEC_DELAY - elapsed)
    _last_request_time = time.time()


def sec_get(url: str, timeout: int = SEC_TIMEOUT, retries: int = SEC_MAX_RETRIES) -> Optional[requests.Response]:
    """GET request to SEC with rate limiting and exponential backoff retry."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    }
    delay = SEC_DELAY
    for attempt in range(retries):
        try:
            _rate_limit()
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                # Rate limited — back off aggressively
                wait = delay * (SEC_RETRY_BACKOFF ** (attempt + 1))
                print(f"  Rate limited (429), waiting {wait:.1f}s...")
                time.sleep(wait)
                continue
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < retries - 1:
                wait = delay * (SEC_RETRY_BACKOFF ** attempt)
                time.sleep(wait)
                continue
            print(f"  Request failed after {retries} attempts: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            return None
    return None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_key(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def _get_cached(url: str, max_age_hours: int = 12) -> Optional[bytes]:
    """Return cached response content if fresh enough."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, _cache_key(url))
    if os.path.exists(path):
        age = time.time() - os.path.getmtime(path)
        if age < max_age_hours * 3600:
            with open(path, "rb") as f:
                return f.read()
    return None


def _set_cache(url: str, content: bytes):
    """Store response content in cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, _cache_key(url))
    with open(path, "wb") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# EFTS Search: find recent Form 4 filings
# ---------------------------------------------------------------------------

def search_form4_filings(start_date: str, end_date: str, quiet: bool = False) -> list[dict]:
    """Search EDGAR EFTS for recent Form 4 filings containing open-market purchases.

    Uses the full-text search to find Form 4 filings. The EFTS API returns
    filing metadata including accession numbers and CIKs.

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        quiet: suppress progress output

    Returns:
        List of filing metadata dicts with keys:
            accession_number, cik, filing_date, file_url, entity_name
    """
    all_filings = []
    start_from = 0

    while True:
        # EFTS search for Form 4 with transaction code P (open-market purchase)
        params = {
            "q": '"transactionCode>P</transactionCode"',
            "forms": "4",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": start_from,
        }

        url = EFTS_SEARCH_URL + "?" + "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())

        cached = _get_cached(url, max_age_hours=4)
        if cached:
            try:
                data = json.loads(cached)
            except json.JSONDecodeError:
                cached = None

        if not cached:
            resp = sec_get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                # Fallback: try simpler query
                if not quiet:
                    print(f"  EFTS search returned {resp.status_code if resp else 'None'}, trying fallback query...")
                break
            try:
                data = resp.json()
            except json.JSONDecodeError:
                if not quiet:
                    print("  EFTS returned non-JSON response, trying fallback...")
                break
            _set_cache(url, resp.content)

        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = total or 0

        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            filing = {
                "accession_number": source.get("file_num", "") or hit.get("_id", ""),
                "cik": str(source.get("ciks", [""])[0]) if source.get("ciks") else "",
                "filing_date": source.get("file_date", ""),
                "entity_name": (source.get("entity_name", "") or
                                (source.get("display_names", [""])[0] if source.get("display_names") else "")),
                "file_url": source.get("file_url", ""),
            }
            # Extract accession from file_url if available
            if filing["file_url"]:
                acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', filing["file_url"])
                if acc_match:
                    filing["accession_number"] = acc_match.group(1)
            all_filings.append(filing)

        if not quiet:
            print(f"  EFTS page: got {len(hits)} hits (total: {total_count}, fetched so far: {len(all_filings)})")

        start_from += len(hits)
        if start_from >= total_count or start_from >= 1000:
            break

    return all_filings


def search_form4_via_fullindex(start_date: str, end_date: str, quiet: bool = False) -> list[dict]:
    """Fallback: fetch recent Form 4 filings from EDGAR full-text search API.

    Uses the simpler search endpoint that is more reliable.
    """
    all_filings = []
    from_offset = 0

    while True:
        url = (
            f"https://efts.sec.gov/LATEST/search-index?"
            f"q=%22open+market+purchase%22&forms=4"
            f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
            f"&from={from_offset}"
        )

        cached = _get_cached(url, max_age_hours=4)
        if cached:
            try:
                data = json.loads(cached)
            except json.JSONDecodeError:
                cached = None

        if not cached:
            resp = sec_get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                break
            try:
                data = resp.json()
            except json.JSONDecodeError:
                break
            _set_cache(url, resp.content)

        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = total or 0

        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            filing = {
                "accession_number": hit.get("_id", ""),
                "cik": str(source.get("ciks", [""])[0]) if source.get("ciks") else "",
                "filing_date": source.get("file_date", ""),
                "entity_name": (source.get("display_names", [""])[0] if source.get("display_names") else ""),
                "file_url": source.get("file_url", ""),
            }
            if filing["file_url"]:
                acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', filing["file_url"])
                if acc_match:
                    filing["accession_number"] = acc_match.group(1)
            all_filings.append(filing)

        if not quiet:
            print(f"  Search page: got {len(hits)} hits (total: {total_count}, fetched: {len(all_filings)})")

        from_offset += len(hits)
        if from_offset >= total_count or from_offset >= 1000:
            break

    return all_filings


def get_recent_form4_filings(days: int = 14, quiet: bool = False) -> list[dict]:
    """Get recent Form 4 filings using EDGAR's EFTS search, with fallback to
    the company submissions API.

    Returns list of filing metadata dicts.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    if not quiet:
        print(f"Searching EDGAR for Form 4 filings: {start_date} to {end_date}")

    # Strategy 1: EFTS full-text search
    filings = search_form4_filings(start_date, end_date, quiet=quiet)

    if not filings:
        if not quiet:
            print("  Primary EFTS search returned no results, trying fallback...")
        filings = search_form4_via_fullindex(start_date, end_date, quiet=quiet)

    if not filings:
        if not quiet:
            print("  Fallback EFTS also empty, trying RSS/company search approach...")
        filings = _search_via_company_filings(start_date, end_date, quiet=quiet)

    if not quiet:
        print(f"  Total filing metadata entries: {len(filings)}")

    return filings


def _search_via_company_filings(start_date: str, end_date: str, quiet: bool = False) -> list[dict]:
    """Last resort: use the EDGAR company search to find recent Form 4 filings.

    This queries the structured EDGAR filing search which is always available.
    """
    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"q=%22P%22&forms=4"
        f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
    )

    cached = _get_cached(url, max_age_hours=4)
    if cached:
        try:
            data = json.loads(cached)
        except json.JSONDecodeError:
            cached = None

    if not cached:
        resp = sec_get(url, timeout=30)
        if resp is None or resp.status_code != 200:
            return []
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return []
        _set_cache(url, resp.content)

    filings = []
    hits = data.get("hits", {}).get("hits", [])
    for hit in hits:
        source = hit.get("_source", {})
        filing = {
            "accession_number": hit.get("_id", ""),
            "cik": str(source.get("ciks", [""])[0]) if source.get("ciks") else "",
            "filing_date": source.get("file_date", ""),
            "entity_name": (source.get("display_names", [""])[0] if source.get("display_names") else ""),
            "file_url": source.get("file_url", ""),
        }
        filings.append(filing)

    return filings


# ---------------------------------------------------------------------------
# Form 4 XML parsing
# ---------------------------------------------------------------------------

def parse_form4_xml(xml_text: str) -> Optional[dict]:
    """Parse a Form 4 XML filing and extract purchase transaction data.

    Returns dict with issuer and transaction info, or None if not a qualifying purchase.
    """
    try:
        # Remove XML declaration and namespace issues
        xml_clean = re.sub(r'<\?xml[^>]*\?>', '', xml_text)
        xml_clean = re.sub(r'xmlns="[^"]*"', '', xml_clean)
        xml_clean = re.sub(r'xmlns:[a-z]+="[^"]*"', '', xml_clean)

        root = ET.fromstring(xml_clean)
    except ET.ParseError:
        # Fall back to regex parsing
        return _parse_form4_regex(xml_text)

    result = {
        "issuer_name": "",
        "issuer_ticker": "",
        "issuer_cik": "",
        "owner_name": "",
        "owner_cik": "",
        "is_officer": False,
        "is_director": False,
        "officer_title": "",
        "transactions": [],
    }

    # Issuer info
    issuer = root.find(".//issuer")
    if issuer is not None:
        result["issuer_name"] = _xml_text(issuer, "issuerName")
        result["issuer_ticker"] = _xml_text(issuer, "issuerTradingSymbol").upper().strip()
        result["issuer_cik"] = _xml_text(issuer, "issuerCik")

    # Reporting owner info
    owner = root.find(".//reportingOwner")
    if owner is not None:
        owner_id = owner.find("reportingOwnerId")
        if owner_id is not None:
            result["owner_name"] = _xml_text(owner_id, "rptOwnerName")
            result["owner_cik"] = _xml_text(owner_id, "rptOwnerCik")

        relationship = owner.find("reportingOwnerRelationship")
        if relationship is not None:
            result["is_officer"] = _xml_text(relationship, "isOfficer") == "1"
            result["is_director"] = _xml_text(relationship, "isDirector") == "1"
            result["officer_title"] = _xml_text(relationship, "officerTitle")

    # Non-derivative transactions (where open-market purchases appear)
    for txn in root.findall(".//nonDerivativeTransaction"):
        code_elem = txn.find(".//transactionCoding")
        if code_elem is None:
            continue

        trans_code = _xml_text(code_elem, "transactionCode")
        if trans_code != "P":
            continue

        # Check if acquired (A) not disposed (D)
        acq_disp = ""
        ad_elem = txn.find(".//transactionAmounts/transactionAcquiredDisposedCode")
        if ad_elem is not None:
            acq_disp = _xml_text(ad_elem, "value") or _xml_text_direct(ad_elem)
        if acq_disp == "D":
            continue

        shares = _xml_float(txn, ".//transactionAmounts/transactionShares/value")
        price = _xml_float(txn, ".//transactionAmounts/transactionPricePerShare/value")
        trans_date = _xml_text(txn, ".//transactionDate/value")

        if shares and price and shares > 0 and price > 0:
            result["transactions"].append({
                "date": trans_date,
                "shares": shares,
                "price": price,
                "value": round(shares * price, 2),
            })

    if not result["transactions"]:
        return None

    return result


def _parse_form4_regex(xml_text: str) -> Optional[dict]:
    """Fallback regex parser for malformed XML."""
    if "<transactionCode>P</transactionCode>" not in xml_text:
        return None

    result = {
        "issuer_name": "",
        "issuer_ticker": "",
        "issuer_cik": "",
        "owner_name": "",
        "owner_cik": "",
        "is_officer": False,
        "is_director": False,
        "officer_title": "",
        "transactions": [],
    }

    m = re.search(r"<issuerName>(.*?)</issuerName>", xml_text)
    if m:
        result["issuer_name"] = m.group(1).strip()

    m = re.search(r"<issuerTradingSymbol>(.*?)</issuerTradingSymbol>", xml_text)
    if m:
        result["issuer_ticker"] = m.group(1).strip().upper()

    m = re.search(r"<issuerCik>(.*?)</issuerCik>", xml_text)
    if m:
        result["issuer_cik"] = m.group(1).strip()

    m = re.search(r"<rptOwnerName>(.*?)</rptOwnerName>", xml_text)
    if m:
        result["owner_name"] = m.group(1).strip()

    m = re.search(r"<rptOwnerCik>(.*?)</rptOwnerCik>", xml_text)
    if m:
        result["owner_cik"] = m.group(1).strip()

    result["is_officer"] = "<isOfficer>1</isOfficer>" in xml_text
    result["is_director"] = "<isDirector>1</isDirector>" in xml_text

    m = re.search(r"<officerTitle>(.*?)</officerTitle>", xml_text)
    if m:
        result["officer_title"] = m.group(1).strip()

    # Find all P-type transactions
    # Split on nonDerivativeTransaction blocks
    blocks = re.findall(
        r"<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>",
        xml_text, re.DOTALL
    )
    for block in blocks:
        if "<transactionCode>P</transactionCode>" not in block:
            continue

        shares_m = re.search(
            r"<transactionShares>.*?<value>([\d.]+)</value>", block, re.DOTALL
        )
        price_m = re.search(
            r"<transactionPricePerShare>.*?<value>([\d.]+)</value>", block, re.DOTALL
        )
        date_m = re.search(
            r"<transactionDate>.*?<value>(\d{4}-\d{2}-\d{2})</value>", block, re.DOTALL
        )

        if shares_m and price_m:
            shares = float(shares_m.group(1))
            price = float(price_m.group(1))
            if shares > 0 and price > 0:
                result["transactions"].append({
                    "date": date_m.group(1) if date_m else "",
                    "shares": shares,
                    "price": price,
                    "value": round(shares * price, 2),
                })

    if not result["transactions"]:
        return None

    return result


def _xml_text(parent, tag: str) -> str:
    """Get text of a child element, empty string if missing."""
    elem = parent.find(tag)
    if elem is not None and elem.text:
        return elem.text.strip()
    # Try nested value element
    elem = parent.find(f"{tag}/value")
    if elem is not None and elem.text:
        return elem.text.strip()
    return ""


def _xml_text_direct(elem) -> str:
    """Get direct text content of an element."""
    if elem is not None and elem.text:
        return elem.text.strip()
    return ""


def _xml_float(root, xpath: str) -> Optional[float]:
    """Get float value from xpath, None if missing."""
    elem = root.find(xpath)
    if elem is not None and elem.text:
        try:
            return float(elem.text.strip())
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Filing fetcher: given metadata, fetch and parse the actual Form 4 XML
# ---------------------------------------------------------------------------

def fetch_and_parse_filing(cik: str, accession: str, file_url: str = "") -> Optional[dict]:
    """Fetch a Form 4 filing from EDGAR and parse it.

    Tries multiple URL strategies:
    1. Direct file_url if provided
    2. Construct from CIK + accession number
    3. Filing index page to find XML link

    Returns parsed form data or None.
    """
    # Normalize accession: remove dashes for directory path
    acc_clean = accession.replace("-", "")
    acc_dashed = accession
    if "-" not in acc_dashed and len(acc_dashed) >= 18:
        acc_dashed = f"{acc_clean[:10]}-{acc_clean[10:12]}-{acc_clean[12:]}"

    # Check cache first
    cache_key_str = f"form4_{cik}_{acc_clean}"
    cache_path = os.path.join(CACHE_DIR, f"{cache_key_str}.pkl")
    os.makedirs(CACHE_DIR, exist_ok=True)

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass

    # Strategy 1: Use file_url directly if it points to XML
    xml_text = None
    if file_url and file_url.endswith(".xml"):
        full_url = file_url if file_url.startswith("http") else f"https://www.sec.gov{file_url}"
        resp = sec_get(full_url)
        if resp and resp.status_code == 200 and "<" in resp.text[:100]:
            xml_text = resp.text

    # Strategy 2: Construct the filing index URL and find XML
    if xml_text is None:
        cik_clean = cik.lstrip("0")
        index_url = f"{EDGAR_ARCHIVES}/{cik_clean}/{acc_clean}/{acc_dashed}-index.htm"
        resp = sec_get(index_url)

        if resp and resp.status_code == 200:
            # Find XML file link in the index page
            xml_links = re.findall(
                r'href="([^"]*\.xml)"', resp.text, re.IGNORECASE
            )
            for link in xml_links:
                # Skip R files (XBRL rendering), prefer primary document
                if link.startswith("R") or "FilingSummary" in link:
                    continue
                if link.startswith("/"):
                    xml_url = f"https://www.sec.gov{link}"
                elif link.startswith("http"):
                    xml_url = link
                else:
                    xml_url = f"{EDGAR_ARCHIVES}/{cik_clean}/{acc_clean}/{link}"

                xml_resp = sec_get(xml_url)
                if xml_resp and xml_resp.status_code == 200:
                    xml_text = xml_resp.text
                    break

    # Strategy 3: Try common XML filename patterns
    if xml_text is None:
        cik_clean = cik.lstrip("0")
        for pattern in [
            f"{EDGAR_ARCHIVES}/{cik_clean}/{acc_clean}/primary_doc.xml",
            f"{EDGAR_ARCHIVES}/{cik_clean}/{acc_clean}/doc4.xml",
            f"{EDGAR_ARCHIVES}/{cik_clean}/{acc_clean}/form4.xml",
        ]:
            resp = sec_get(pattern)
            if resp and resp.status_code == 200 and "<ownershipDocument" in resp.text:
                xml_text = resp.text
                break

    if xml_text is None:
        return None

    parsed = parse_form4_xml(xml_text)

    # Cache the result (even None, to avoid re-fetching)
    with open(cache_path, "wb") as f:
        pickle.dump(parsed, f)

    return parsed


# ---------------------------------------------------------------------------
# Alternative approach: use EDGAR recent filings RSS/JSON
# ---------------------------------------------------------------------------

def get_recent_form4_from_rss(days: int = 14, quiet: bool = False) -> list[dict]:
    """Fetch recent Form 4 filings using EDGAR's structured search API.

    This is the most reliable method: queries the full-text search for
    Form 4 filings and returns metadata including file URLs.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    if not quiet:
        print(f"Querying EDGAR full-text search for Form 4 filings ({start_date} to {end_date})...")

    # Use the EDGAR full-text search API (EFTS)
    all_results = []
    from_offset = 0

    while True:
        url = (
            f"https://efts.sec.gov/LATEST/search-index?"
            f"q=%22transactionCode%3EP%3C%2FtransactionCode%22"  # "transactionCode>P</transactionCode"
            f"&forms=4"
            f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
            f"&from={from_offset}"
        )

        cached = _get_cached(url, max_age_hours=4)
        data = None

        if cached:
            try:
                data = json.loads(cached)
            except json.JSONDecodeError:
                pass

        if data is None:
            resp = sec_get(url, timeout=30)
            if resp is None or resp.status_code != 200:
                if not quiet:
                    status = resp.status_code if resp else "no response"
                    print(f"  EFTS query failed ({status}), trying alternative approach...")
                break
            try:
                data = resp.json()
            except json.JSONDecodeError:
                if not quiet:
                    print("  EFTS returned non-JSON, trying alternative...")
                break
            _set_cache(url, resp.content)

        hits = data.get("hits", {}).get("hits", [])
        total = data.get("hits", {}).get("total", {})
        if isinstance(total, dict):
            total_count = total.get("value", 0)
        else:
            total_count = total or 0

        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            ciks = source.get("ciks", [])
            filing = {
                "accession_number": hit.get("_id", ""),
                "cik": str(ciks[0]) if ciks else "",
                "filing_date": source.get("file_date", ""),
                "entity_name": "",
                "file_url": source.get("file_url", ""),
            }
            # Use display_names (not entity_name) per CLAUDE.md
            display_names = source.get("display_names", [])
            if display_names:
                filing["entity_name"] = display_names[0]
            else:
                filing["entity_name"] = source.get("entity_name", "")

            # Extract proper accession from file_url or _id
            if filing["file_url"]:
                acc_match = re.search(r"(\d{10}-\d{2}-\d{6})", filing["file_url"])
                if acc_match:
                    filing["accession_number"] = acc_match.group(1)

            if filing["cik"]:
                all_results.append(filing)

        if not quiet:
            print(f"  Page {from_offset // EFTS_PAGE_SIZE + 1}: {len(hits)} hits (total available: {total_count})")

        from_offset += len(hits)
        if from_offset >= total_count or from_offset >= 2000:
            # Cap at 2000 filings to stay within runtime budget
            break

    return all_results


# ---------------------------------------------------------------------------
# Core scanner: fetch filings, parse XML, identify clusters
# ---------------------------------------------------------------------------

def scan_insider_clusters(
    days: int = 14,
    min_insiders: int = 3,
    min_value: float = 50_000,
    cluster_window_days: int = 30,
    filter_10b5: bool = True,
    quiet: bool = False,
) -> list[dict]:
    """Scan recent EDGAR Form 4 filings for insider buying clusters.

    Args:
        days: How many days back to search
        min_insiders: Minimum unique insiders for a cluster
        min_value: Minimum purchase value per insider ($)
        cluster_window_days: Max days between first and last purchase in cluster
        filter_10b5: Exclude 10b5-1 pre-planned trades (checked via XML)
        quiet: Suppress progress output

    Returns:
        List of cluster dicts with keys:
            ticker, cluster_date, n_insiders, total_value, insiders, issuer_name
    """

    def log(*args, **kwargs):
        if not quiet:
            print(*args, **kwargs)

    # Step 1: Get filing metadata from EFTS
    filings = get_recent_form4_from_rss(days=days, quiet=quiet)

    if not filings:
        log("No Form 4 filings found via EFTS. Trying full-index approach...")
        filings = get_recent_form4_filings(days=days, quiet=quiet)

    if not filings:
        log("No filings found. EDGAR may be experiencing issues.")
        return []

    log(f"\nFound {len(filings)} Form 4 filing metadata entries")

    # Deduplicate by accession number
    seen_acc = set()
    unique_filings = []
    for f in filings:
        acc = f["accession_number"].replace("-", "")
        if acc and acc not in seen_acc:
            seen_acc.add(acc)
            unique_filings.append(f)
    filings = unique_filings
    log(f"After dedup: {len(filings)} unique filings")

    # Step 2: Parse each filing's XML
    log(f"\nParsing {len(filings)} Form 4 filings...")

    # Collect all qualifying purchases grouped by ticker
    # ticker -> list of {owner_name, owner_cik, date, shares, value, issuer_name}
    purchases_by_ticker = defaultdict(list)
    parsed_count = 0
    error_count = 0
    skipped_count = 0

    for i, filing in enumerate(filings):
        if not quiet and (i + 1) % 50 == 0:
            print(f"  Scanning {i + 1}/{len(filings)} filings...", end="\r", flush=True)

        cik = filing.get("cik", "")
        accession = filing.get("accession_number", "")
        file_url = filing.get("file_url", "")

        if not cik or not accession:
            skipped_count += 1
            continue

        parsed = fetch_and_parse_filing(cik, accession, file_url)

        if parsed is None:
            error_count += 1
            continue

        parsed_count += 1

        ticker = parsed.get("issuer_ticker", "").strip().upper()
        if not ticker or len(ticker) > 5 or not re.match(r'^[A-Z.\-]+$', ticker):
            continue

        issuer_name = parsed.get("issuer_name", "")
        owner_name = parsed.get("owner_name", "")
        owner_cik = parsed.get("owner_cik", "")

        # Only count officers and directors (skip 10%+ holders who aren't officers)
        if not parsed.get("is_officer") and not parsed.get("is_director"):
            continue

        for txn in parsed.get("transactions", []):
            if txn["value"] < min_value:
                continue

            purchases_by_ticker[ticker].append({
                "name": owner_name,
                "cik": owner_cik,
                "date": txn["date"],
                "shares": txn["shares"],
                "price": txn["price"],
                "value": txn["value"],
                "issuer_name": issuer_name,
                "is_officer": parsed.get("is_officer", False),
                "is_director": parsed.get("is_director", False),
                "officer_title": parsed.get("officer_title", ""),
            })

    if not quiet:
        print()  # Clear progress line
    log(f"  Parsed: {parsed_count}, Errors: {error_count}, Skipped: {skipped_count}")
    log(f"  Tickers with qualifying purchases: {len(purchases_by_ticker)}")

    # Step 3: Identify clusters
    clusters = []

    for ticker, purchases in purchases_by_ticker.items():
        # Sort by date
        purchases.sort(key=lambda p: p["date"])

        # Get unique insiders (by CIK, fallback to name)
        insider_ids = set()
        for p in purchases:
            insider_id = p["cik"] if p["cik"] else p["name"]
            insider_ids.add(insider_id)

        if len(insider_ids) < min_insiders:
            continue

        # Check if purchases fall within the cluster window
        # Use sliding window approach
        dates = [p["date"] for p in purchases if p["date"]]
        if not dates:
            continue

        # Try to find a window of cluster_window_days containing min_insiders unique buyers
        best_cluster = _find_best_cluster(
            purchases, cluster_window_days, min_insiders
        )

        if best_cluster is None:
            continue

        cluster_purchases, cluster_date = best_cluster

        # Build the cluster record
        insiders = []
        seen_insiders = set()
        total_value = 0.0

        for p in cluster_purchases:
            insider_id = p["cik"] if p["cik"] else p["name"]
            if insider_id in seen_insiders:
                # Sum additional purchases by same insider
                for existing in insiders:
                    if (existing["cik"] == p["cik"] and p["cik"]) or existing["name"] == p["name"]:
                        existing["value"] += p["value"]
                        existing["shares"] += p["shares"]
                        total_value += p["value"]
                        break
                continue

            seen_insiders.add(insider_id)
            role = ""
            if p.get("officer_title"):
                role = p["officer_title"]
            elif p.get("is_officer"):
                role = "Officer"
            elif p.get("is_director"):
                role = "Director"

            insiders.append({
                "name": p["name"],
                "cik": p["cik"],
                "date": p["date"],
                "shares": p["shares"],
                "price": p["price"],
                "value": p["value"],
                "role": role,
            })
            total_value += p["value"]

        clusters.append({
            "ticker": ticker,
            "issuer_name": cluster_purchases[0].get("issuer_name", ""),
            "cluster_date": cluster_date,
            "n_insiders": len(insiders),
            "total_value": round(total_value, 2),
            "insiders": insiders,
        })

    # Sort by cluster_date descending, then by n_insiders descending
    clusters.sort(key=lambda c: (c["cluster_date"], c["n_insiders"]), reverse=True)

    log(f"\nClusters found ({min_insiders}+ insiders, >=${min_value:,.0f} each): {len(clusters)}")

    return clusters


def _find_best_cluster(
    purchases: list[dict],
    window_days: int,
    min_insiders: int,
) -> Optional[tuple]:
    """Find the best cluster window for a set of purchases.

    Returns (cluster_purchases, cluster_date) or None.
    The cluster_date is when the min_insiders threshold was first met.
    """
    if not purchases:
        return None

    # Sort by date
    dated = [p for p in purchases if p.get("date")]
    if not dated:
        return None
    dated.sort(key=lambda p: p["date"])

    best = None
    best_n = 0

    # Sliding window: for each purchase as the window end
    for i, end_purchase in enumerate(dated):
        end_date = end_purchase["date"]
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            continue

        start_dt = end_dt - timedelta(days=window_days)
        start_date = start_dt.strftime("%Y-%m-%d")

        # Collect all purchases in [start_date, end_date]
        window_purchases = []
        window_insiders = set()

        for p in dated:
            if start_date <= p["date"] <= end_date:
                window_purchases.append(p)
                insider_id = p["cik"] if p["cik"] else p["name"]
                window_insiders.add(insider_id)

        n_insiders = len(window_insiders)
        if n_insiders >= min_insiders and n_insiders > best_n:
            best_n = n_insiders
            best = (window_purchases, end_date)

    return best


# ---------------------------------------------------------------------------
# CLI output formatting
# ---------------------------------------------------------------------------

def format_table(clusters: list[dict]) -> str:
    """Format clusters as a human-readable table."""
    if not clusters:
        return "No clusters found."

    lines = []
    lines.append(f"{'='*72}")
    lines.append(f"INSIDER CLUSTER REPORT -- {datetime.now().strftime('%Y-%m-%d')}")
    lines.append(f"{'='*72}")
    lines.append("")

    for c in clusters:
        issuer = c.get("issuer_name", "")
        if issuer and len(issuer) > 40:
            issuer = issuer[:37] + "..."

        lines.append(f"  {c['ticker']:6} | {c['n_insiders']} insiders | ${c['total_value']:>12,.0f} | {issuer}")
        lines.append(f"  {'':6} | cluster date: {c['cluster_date']}")

        for ins in c.get("insiders", []):
            role = ins.get("role", "")
            role_str = f" ({role})" if role else ""
            lines.append(
                f"  {'':6}   - {ins['name'][:30]}{role_str}: "
                f"${ins['value']:,.0f} on {ins['date']}"
            )
        lines.append("")

    lines.append(f"{'='*72}")
    lines.append(f"Total clusters: {len(clusters)}")
    return "\n".join(lines)


def format_json(clusters: list[dict]) -> str:
    """Format clusters as JSON."""
    return json.dumps(clusters, indent=2, default=str)


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="SEC EDGAR real-time insider buying cluster scanner"
    )
    parser.add_argument(
        "--days", type=int, default=14,
        help="How many days back to scan (default: 14)"
    )
    parser.add_argument(
        "--min-insiders", type=int, default=3,
        help="Minimum unique insiders in a cluster (default: 3)"
    )
    parser.add_argument(
        "--min-value", type=float, default=50000,
        help="Minimum purchase value per insider in $ (default: 50000)"
    )
    parser.add_argument(
        "--window", type=int, default=30,
        help="Cluster window in days (default: 30)"
    )
    parser.add_argument(
        "--output", choices=["json", "table"], default="table",
        help="Output format (default: table)"
    )
    parser.add_argument(
        "--output-file", type=str, default=None,
        help="Write results to file"
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress progress output"
    )
    parser.add_argument(
        "--no-10b5-filter", action="store_true",
        help="Include 10b5-1 pre-planned trades"
    )

    args = parser.parse_args()

    clusters = scan_insider_clusters(
        days=args.days,
        min_insiders=args.min_insiders,
        min_value=args.min_value,
        cluster_window_days=args.window,
        filter_10b5=not args.no_10b5_filter,
        quiet=args.quiet,
    )

    if args.output == "json":
        output_text = format_json(clusters)
    else:
        output_text = format_table(clusters)

    print(output_text)

    if args.output_file:
        with open(args.output_file, "w") as f:
            if args.output == "json":
                f.write(output_text)
            else:
                # Always save JSON to file for programmatic use
                f.write(format_json(clusters))
        print(f"\nResults saved to {args.output_file}")

    return clusters


if __name__ == "__main__":
    main()
