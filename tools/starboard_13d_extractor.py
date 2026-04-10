#!/usr/bin/env python3
"""Extract target companies from Starboard Value SC 13D filings on EDGAR."""

import requests
import time
import json
import re
import sys

HEADERS = {
    "User-Agent": "FrakboxResearch research@frakbox.io",
    "Accept-Encoding": "gzip, deflate"
}

FILINGS_2020_2021 = [
    {"date": "2020-01-13", "accession": "0000921895-20-000096"},
    {"date": "2020-02-03", "accession": "0000921895-20-000224"},
    {"date": "2020-02-13", "accession": "0000921895-20-000442"},
    {"date": "2020-03-30", "accession": "0000921895-20-000941"},
    {"date": "2020-10-19", "accession": "0000921895-20-002646"},
    {"date": "2020-11-30", "accession": "0000921895-20-003136"},
    {"date": "2021-03-22", "accession": "0000921895-21-000813"},
    {"date": "2021-08-09", "accession": "0001011438-21-000191"},
    {"date": "2021-09-27", "accession": "0000921895-21-002333"},
    {"date": "2021-12-27", "accession": "0001193805-21-001797"},
]

STARBOARD_CIK = "1517137"

def get_subject_cik(accession_no):
    """Extract subject company CIK from filing index page."""
    acc_clean = accession_no.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{STARBOARD_CIK}/{acc_clean}/{accession_no}-index.htm"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        return None
    
    text = resp.text
    # Find the Subject company CIK (first CIK on page, before filer CIK)
    cik_pattern = re.findall(r'browse-edgar\?CIK=(\d+)&amp;action=getcompany', text)
    # First CIK is subject company, second is filer (Starboard)
    if cik_pattern:
        subject_cik = cik_pattern[0].lstrip('0')
        return subject_cik
    return None

def lookup_company(cik):
    """Look up company name and ticker from CIK using EDGAR submissions API."""
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        return None, None
    data = resp.json()
    name = data.get("name", "")
    tickers = data.get("tickers", [])
    ticker = tickers[0] if tickers else None
    return name, ticker

def main():
    results = []
    for filing in FILINGS_2020_2021:
        acc = filing["accession"]
        date = filing["date"]
        print(f"Processing {date}...", file=sys.stderr)
        
        subject_cik = get_subject_cik(acc)
        time.sleep(0.12)
        
        name, ticker = None, None
        if subject_cik:
            name, ticker = lookup_company(subject_cik)
            time.sleep(0.12)
        
        results.append({
            "filing_date": date,
            "subject_cik": subject_cik,
            "company": name,
            "ticker": ticker
        })
    
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
