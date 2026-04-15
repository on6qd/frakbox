#!/usr/bin/env python3
"""Scan EDGAR EFTS for 8-K Item 4.02 (Non-reliance on financials) filings.
Returns list of {cik, company, ticker, date, accession} for backtesting."""

import requests
import json
import time
import sys
from datetime import datetime, timedelta

HEADERS = {'User-Agent': 'FrakBox research@frakbox.io'}

def search_item_402(start_date, end_date, max_results=200):
    """Search EDGAR EFTS for 8-K filings mentioning Item 4.02."""
    events = []
    offset = 0
    
    while offset < max_results:
        url = (
            f'https://efts.sec.gov/LATEST/search-index'
            f'?q=%22item+4.02%22&forms=8-K'
            f'&dateRange=custom&startdt={start_date}&enddt={end_date}'
            f'&from={offset}&size=50'
        )
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                print(f"EFTS error: {r.status_code}", file=sys.stderr)
                break
            data = r.json()
            hits = data.get('hits', {}).get('hits', [])
            if not hits:
                break
            for hit in hits:
                src = hit.get('_source', {})
                events.append({
                    'cik': src.get('entity_id', [''])[0] if src.get('entity_id') else '',
                    'company': src.get('display_names', [''])[0] if src.get('display_names') else src.get('entity_name', ''),
                    'tickers': src.get('tickers', []),
                    'date': src.get('file_date', ''),
                    'accession': src.get('file_num', ''),
                    'form': src.get('form_type', ''),
                })
            offset += 50
            time.sleep(0.2)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            break
    
    return events

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='2023-01-01')
    parser.add_argument('--end', default='2025-12-31')
    parser.add_argument('--max', type=int, default=500)
    args = parser.parse_args()
    
    events = search_item_402(args.start, args.end, args.max)
    
    # Deduplicate by company+date
    seen = set()
    unique = []
    for e in events:
        key = (e['company'], e['date'])
        if key not in seen:
            seen.add(key)
            unique.append(e)
    
    print(json.dumps({
        'total': len(unique),
        'events': unique
    }, indent=2))

if __name__ == '__main__':
    main()
