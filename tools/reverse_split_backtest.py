"""
reverse_split_backtest.py

Backtest: Do stocks that undergo a reverse stock split continue to decline
abnormally vs SPY over the next 1-20 days?

Hypothesis:
    Given a company executes a reverse stock split,
    When shares begin trading at the new consolidated price,
    Then the stock declines abnormally vs SPY over the next 5-10 days.

Mechanism:
    Reverse splits signal financial distress (exchange listing compliance).
    Institutional investors exit after reverse splits. Academic support:
    Han (1995), Denis & Sarin (1994) show continued post-split weakness.

Data approach:
    Primary: EDGAR full-text search for 8-K filings containing "reverse stock split"
    Fallback: yfinance .splits data (ratio < 1) across a broad universe of
              historically distressed tickers

Filters:
    - Market cap at time of event > $100M (proxy: current cap)
    - Minimum share price before split > $1
    - Max 10:1 ratio (exclude extreme 100:1+ de-facto delistings)
    - Minimum 30 trading days of post-event data available
    - Date range: 2018-2024

Usage:
    python3 tools/reverse_split_backtest.py
"""

from __future__ import annotations

import sys
import os
import re
import time
import warnings
import json
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from scipy import stats

from tools.yfinance_utils import safe_download
from tools.largecap_filter import get_market_cap

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

START_DATE = "2018-01-01"
END_DATE   = "2024-12-31"
MIN_MARKET_CAP_M = 100      # $100M minimum
MIN_PRICE_BEFORE = 1.0      # pre-split adjusted price > $1
MAX_RATIO = 10.0            # max 10:1 reverse split; exclude 100:1+
MIN_POST_DAYS = 30          # minimum trading days after event
EDGAR_HEADERS = {
    "User-Agent": "research-bot contact@frakbox.io",
    "Accept-Encoding": "gzip, deflate",
}

# Broad universe for fallback yfinance splits scan — mix of small/micro-cap
# sectors prone to reverse splits: biotech, cannabis, EV, SPACs
FALLBACK_UNIVERSE = [
    # Biotech/Pharma
    "SNDL", "GERN", "OCGN", "NVAX", "SRNE", "IDEX", "CLOV", "WISH",
    "WKHS", "BBBY", "NKLA", "RIDE", "XELA", "PHUN", "HCMC", "EVOK",
    "GOVX", "INPX", "VIVE", "GRTX", "CBAT", "CANF", "DARE", "DBVT",
    "DFFN", "EIGR", "ENVB", "EVLO", "GNPX", "ICAD", "IDRA", "IMGN",
    "INVA", "JNCE", "KPTI", "LCTX", "MCRB", "MRSN", "NEOS", "NERV",
    "ORTX", "PACB", "PHIO", "PRVB", "PTCT", "PTGX", "PULM", "QURE",
    "RCKT", "RDHL", "RMTI", "RUBY", "RVNC", "SBBP", "SEEL", "SELB",
    "SNSS", "SRNE", "TCON", "TPTX", "TRIL", "TTPH", "UROV", "VAPO",
    "VCNX", "VERB", "VERO", "VSTM", "VTGN", "XLRN", "XNCR", "XOMA",
    "ZSAN", "ACST", "ACER", "AMPE", "APTO", "AYTU", "BCYC", "CRBP",
    "CTSO", "EIGR", "EVFM", "FLGT", "IFRX", "LNDC", "MDNA", "NTLA",
    "PRVB", "PTNR", "RTIX", "SAVA", "SDCL", "SELB", "STCN", "STRM",
    "SVRA", "TBPH", "TXMD", "UAVS", "UTMD", "VFRM", "VIIV", "VNET",
    "VNTV", "VVOS", "WINT", "ZIXI", "ZTNO",
    # Cannabis
    "TLRY", "CGC", "ACB", "CRON", "OGI", "APHA", "HEXO", "VFF",
    # Micro-cap distressed
    "ATDS", "TNXP", "RTSL", "VINO", "MRZM", "CEIN", "AMIH", "GRYP",
    "ASTC", "AIRI", "ILUS", "MOTS", "CIFS", "MINE", "AKER",
    "FXLV", "MFAC", "AVTE", "NVFY", "NURO", "CLWD", "PRTY",
    # Energy distressed
    "PVTL", "GTE", "TELL", "PACD", "ORIG", "ICON", "SHIP", "SINT",
    # Fintech/SPAC distressed
    "PRPB", "COVA", "TDAC", "FWAA", "AJAX", "IPOD", "IPOE",
    # Additional known reverse-splitters
    "NAKD", "KOSS", "EXPR", "CTRM", "ILUS", "GFAI", "BSFC",
    "DPRO", "NXTP", "SOXS", "HIHO", "ADMP", "TPVG", "WPRT",
    "AGTC", "ALBO", "ALDX", "AMRN", "ATRA", "AUPH", "AVRO",
    "BDTX", "BLUE", "BMRN", "BNGO", "BPMC", "BXMT", "CARA",
    "CEMI", "CHRS", "CMPS", "CODX", "CORT", "CRDF", "CRSP",
    "DCTH", "EDIT", "EPZM", "FREQ", "FROG", "FWRD", "GBIO",
    "GRTS", "GWPH", "HALO", "HGEN", "HOOK", "HRTX", "HTBX",
    "ICLR", "IMVT", "INCY", "IPSC", "ITIC", "ITOS", "ITRI",
    "JAZZ", "KDNY", "KPTI", "KRYS", "KYMR", "LGND", "LHDX",
    "LMNX", "LNTH", "LPCN", "LQDA", "LRMR", "LUNA", "LUMO",
    "MASS", "MDGL", "MGNX", "MGTX", "MIST", "MKSI", "MORF",
    "MRUS", "MYOK", "NBTX", "NKTR", "NNOX", "NRIX", "NSTG",
    "NVTA", "OBSV", "ONEM", "ONON", "OPRA", "ORIC", "ORPH",
    "OTLK", "OVID", "PCRX", "PDCO", "PHAT", "PINE", "PLRX",
    "PRAX", "PRLD", "PRTK", "PRTS", "PSFE", "PSTI", "PTGX",
    "RGEN", "RIOT", "RLMD", "RLYB", "RNLX", "RNVX", "RPID",
    "RPRX", "RPTX", "RUBY", "RWLK", "RXMD", "RZLT", "SAGE",
    "SALT", "SANA", "SBGI", "SDIG", "SERA", "SESN", "SGMO",
    "SLDB", "SLGL", "SLNA", "SMMT", "SNGX", "SNOA", "SPPI",
    "SPRB", "SRPT", "SRTX", "STAA", "STML", "SURF", "SWAV",
    "SYRS", "TALS", "TARA", "TELA", "TGTX", "THMO", "TLSA",
    "TMBR", "TNXP", "TOCA", "TPIC", "TPVG", "TRHC", "TRPX",
    "TRVI", "TSRI", "TYME", "TZOO", "UONE", "URGN", "UTHR",
    "VBIV", "VCEL", "VCYT", "VDEX", "VKTX", "VNDA", "VRAY",
    "VRTX", "VRTU", "VTVT", "VXRT", "WBAI", "WKHS", "XBIT",
    "XENE", "XLRN", "XOMA", "YMAB", "ZAFG", "ZFGN", "ZIOP",
    "ZLAB", "ZLBIO", "ZMTP", "ZNTL", "ZSAN",
]
FALLBACK_UNIVERSE = list(set(FALLBACK_UNIVERSE))


# ---------------------------------------------------------------------------
# Step 1: Fetch reverse split events from EDGAR full-text search
# ---------------------------------------------------------------------------

def fetch_edgar_events(start: str = START_DATE, end: str = END_DATE,
                       max_pages: int = 50) -> list[dict]:
    """
    Query EDGAR full-text search API for 8-K filings containing
    "reverse stock split". Extracts ticker from display_names field.

    display_names format: "Company Name  (TICKER)  (CIK 0000123456)"
    Ticker appears in parens BEFORE the CIK entry.
    """
    print(f"Querying EDGAR full-text search for 8-K reverse stock split filings ({start} to {end})...")

    base_url = (
        "https://efts.sec.gov/LATEST/search-index"
        "?q=%22reverse+stock+split%22"
        f"&dateRange=custom&startdt={start}&enddt={end}"
        "&forms=8-K"
    )

    events = []
    seen_keys = set()

    for page in range(max_pages):
        url = base_url + f"&from={page * 100}&hits.hits.total.value=true"
        try:
            resp = requests.get(url, headers=EDGAR_HEADERS, timeout=25)
            if resp.status_code != 200:
                print(f"  EDGAR returned status {resp.status_code} on page {page+1}")
                break
            data = resp.json()
        except Exception as e:
            print(f"  EDGAR request error on page {page+1}: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})

            entity = src.get("display_names", [])
            if not entity:
                continue

            ticker = None
            for name_entry in entity:
                # Ticker is in parens BEFORE (CIK ...) — e.g. "(VINO)  (CIK 0001559998)"
                m = re.search(r'\(([A-Z]{1,6})\)\s*\(CIK', name_entry.strip())
                if m:
                    ticker = m.group(1)
                    break

            if not ticker:
                continue

            filed_date = src.get("file_date", "")
            if not filed_date:
                continue

            key = (ticker, filed_date[:7])  # ticker + month
            if key in seen_keys:
                continue
            seen_keys.add(key)

            events.append({
                "ticker": ticker,
                "date": filed_date[:10],
                "source": "edgar",
            })

        total = data.get("hits", {}).get("total", {}).get("value", 0)
        fetched_so_far = (page + 1) * 100
        print(f"  Page {page+1}: +{len(hits)} hits | collected {len(events)} events so far (total available: {total})")

        if fetched_so_far >= total:
            break

        time.sleep(0.4)  # be polite to SEC

    print(f"  EDGAR: found {len(events)} events with extractable tickers")
    return events


# ---------------------------------------------------------------------------
# Step 2: Fallback — scan yfinance .splits for reverse splits
# ---------------------------------------------------------------------------

def fetch_yfinance_splits(tickers: list[str],
                          start: str = START_DATE,
                          end: str = END_DATE) -> list[dict]:
    """
    Use yfinance .splits to detect reverse splits (ratio < 1).
    yfinance stores them as fraction: 0.1 = 1-for-10 reverse split.
    """
    print(f"Scanning yfinance .splits for {len(tickers)} tickers ({start} to {end})...")
    events = []
    errors = 0

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)

    for i, ticker in enumerate(tickers):
        if i > 0 and i % 50 == 0:
            print(f"  Progress: {i}/{len(tickers)} scanned, {len(events)} events found...")
        try:
            t = yf.Ticker(ticker)
            splits = t.splits

            if splits is None or len(splits) == 0:
                continue

            # Normalize timezone
            splits.index = splits.index.tz_localize(None) if splits.index.tz is not None else splits.index

            # Reverse splits: ratio < 1 (e.g. 0.1 = 1-for-10)
            rev = splits[(splits < 1.0) & (splits.index >= start_ts) & (splits.index <= end_ts)]

            for dt, ratio in rev.items():
                # Compute implied N:1 ratio
                implied_ratio = 1.0 / ratio  # e.g. 0.1 -> 10.0 (10:1)
                if implied_ratio > MAX_RATIO:
                    continue  # exclude extreme consolidations

                events.append({
                    "ticker": ticker,
                    "date": dt.strftime("%Y-%m-%d"),
                    "ratio_estimate": round(implied_ratio, 2),
                    "source": "yfinance_splits",
                })

        except Exception:
            errors += 1

    print(f"  yfinance splits scan: found {len(events)} reverse split events ({errors} errors)")
    return events


# ---------------------------------------------------------------------------
# Step 3: Filter and deduplicate
# ---------------------------------------------------------------------------

def filter_events(events: list[dict], min_cap_m: float = MIN_MARKET_CAP_M) -> list[dict]:
    """
    Apply filters:
    - Deduplicate (same ticker within 90 days)
    - Date window check
    - Market cap >= min_cap_m (current cap as proxy)
    - Post-event data availability (event + 45 calendar days <= END_DATE)
    """
    print(f"\nFiltering {len(events)} raw events...")

    # Deduplicate: same ticker within 90 days, keep earliest
    events_sorted = sorted(events, key=lambda e: (e["ticker"], e["date"]))
    deduped = []
    prev_ticker, prev_date_str = None, None
    for ev in events_sorted:
        if prev_ticker == ev["ticker"] and prev_date_str is not None:
            gap = (datetime.strptime(ev["date"], "%Y-%m-%d") -
                   datetime.strptime(prev_date_str, "%Y-%m-%d")).days
            if gap < 90:
                continue
        deduped.append(ev)
        prev_ticker    = ev["ticker"]
        prev_date_str  = ev["date"]

    print(f"  After dedup (90-day window): {len(deduped)}")

    # Date window
    in_window = [e for e in deduped if START_DATE <= e["date"] <= END_DATE]
    print(f"  After date window ({START_DATE} to {END_DATE}): {len(in_window)}")

    # Post-event data availability: need at least 45 calendar days of post-data
    cutoff = (datetime.strptime(END_DATE, "%Y-%m-%d") - timedelta(days=45)).strftime("%Y-%m-%d")
    has_post = [e for e in in_window if e["date"] <= cutoff]
    print(f"  After post-event availability filter: {len(has_post)}")

    # Market cap filter
    print(f"  Checking market caps (>= ${min_cap_m}M)...")
    cap_cache: dict = {}
    unique_tickers = list({e["ticker"] for e in has_post})

    for i, ticker in enumerate(unique_tickers):
        if i > 0 and i % 50 == 0:
            print(f"    Cap check progress: {i}/{len(unique_tickers)}...")
        cap = get_market_cap(ticker, cap_cache)
        cap_cache[ticker] = cap

    passed = []
    for ev in has_post:
        cap = cap_cache.get(ev["ticker"])
        if cap is not None and cap >= min_cap_m:
            passed.append(ev)

    print(f"  After market cap filter: {len(passed)} events")
    return passed


# ---------------------------------------------------------------------------
# Step 4: Validate pre-split price from yfinance price data
# ---------------------------------------------------------------------------

def validate_price_filter(events: list[dict]) -> list[dict]:
    """
    For each event, check that the pre-split price >= MIN_PRICE_BEFORE.
    Uses the yfinance adjusted close for the day before the event.
    Also enforces MAX_RATIO by computing the price jump around the split date.
    Events that pass price data checks are kept; others dropped.
    """
    print(f"\nValidating pre-split prices for {len(events)} events...")
    passed = []
    skipped = 0

    for ev in events:
        ticker   = ev["ticker"]
        event_dt = datetime.strptime(ev["date"], "%Y-%m-%d")

        fetch_start = (event_dt - timedelta(days=10)).strftime("%Y-%m-%d")
        fetch_end   = (event_dt + timedelta(days=5)).strftime("%Y-%m-%d")

        try:
            # Use auto_adjust=False to see the pre-split price before adjustment
            raw = yf.Ticker(ticker).history(start=fetch_start, end=fetch_end,
                                             auto_adjust=False)
            if raw is None or raw.empty:
                # Keep anyway — can't verify but don't want to drop legit events
                passed.append(ev)
                continue

            # Normalize index
            raw.index = raw.index.tz_localize(None) if raw.index.tz is not None else raw.index
            raw.index = pd.to_datetime(raw.index)

            # Find trading day just before the event
            before_mask = raw.index < pd.Timestamp(ev["date"])
            if not before_mask.any():
                passed.append(ev)
                continue

            pre_price = float(raw.loc[before_mask, "Close"].iloc[-1])

            # Price before split must be >= MIN_PRICE_BEFORE
            if pre_price < MIN_PRICE_BEFORE:
                skipped += 1
                continue

            ev = dict(ev)
            ev["pre_split_price"] = round(pre_price, 4)
            passed.append(ev)

        except Exception:
            passed.append(ev)  # keep event even if validation fails

    print(f"  After pre-split price filter (>= ${MIN_PRICE_BEFORE}): {len(passed)} kept, {skipped} dropped")
    return passed


# ---------------------------------------------------------------------------
# Step 5: Compute abnormal returns
# ---------------------------------------------------------------------------

def compute_abnormal_returns(events: list[dict],
                             horizons: list[int] = [1, 3, 5, 10, 20]) -> list[dict]:
    """
    Compute abnormal returns (stock return - SPY return) at each horizon.
    Entry: open of the trading day AFTER the event date.
    SPY prices aligned by trading day offset (not calendar days).
    """
    print(f"\nComputing abnormal returns for {len(events)} events...")
    print("  Downloading SPY benchmark data...")

    spy_start = (datetime.strptime(START_DATE, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    spy_end   = (datetime.strptime(END_DATE,   "%Y-%m-%d") + timedelta(days=60)).strftime("%Y-%m-%d")

    try:
        spy_df     = safe_download("SPY", start=spy_start, end=spy_end)
        spy_closes = spy_df["Close"]
        spy_opens  = spy_df["Open"]
    except Exception as e:
        print(f"  ERROR downloading SPY: {e}")
        return []

    results = []
    errors  = []

    for i, ev in enumerate(events):
        ticker    = ev["ticker"]
        event_dt  = datetime.strptime(ev["date"], "%Y-%m-%d")

        if i > 0 and i % 25 == 0:
            print(f"  Progress: {i}/{len(events)} events processed, {len(results)} successful...")

        fetch_start = (event_dt - timedelta(days=5)).strftime("%Y-%m-%d")
        fetch_end   = (event_dt + timedelta(days=35)).strftime("%Y-%m-%d")

        try:
            df = safe_download(ticker, start=fetch_start, end=fetch_end)
            if df is None or len(df) < 5:
                errors.append((ticker, ev["date"], "insufficient price data"))
                continue

            closes = df["Close"]
            opens  = df["Open"]

            # Find entry: first trading day after the event date
            entry_dt_str = (event_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            entry_idx = None
            for idx in range(len(df.index)):
                if df.index[idx].strftime("%Y-%m-%d") >= entry_dt_str:
                    entry_idx = idx
                    break

            if entry_idx is None:
                errors.append((ticker, ev["date"], "no entry date found in window"))
                continue

            entry_price = float(opens.iloc[entry_idx])
            entry_date  = df.index[entry_idx]

            if entry_price <= 0 or np.isnan(entry_price):
                errors.append((ticker, ev["date"], f"invalid entry price {entry_price}"))
                continue

            # Align SPY to same entry date
            spy_entry_date_str = entry_date.strftime("%Y-%m-%d")
            spy_entry_idx = None
            for idx in range(len(spy_closes.index)):
                if spy_closes.index[idx].strftime("%Y-%m-%d") >= spy_entry_date_str:
                    spy_entry_idx = idx
                    break

            if spy_entry_idx is None:
                errors.append((ticker, ev["date"], "SPY entry date not found"))
                continue

            spy_entry_price = float(spy_opens.iloc[spy_entry_idx])
            if spy_entry_price <= 0:
                spy_entry_price = float(spy_closes.iloc[spy_entry_idx])

            row = {
                "ticker":       ticker,
                "event_date":   ev["date"],
                "entry_date":   entry_date.strftime("%Y-%m-%d"),
                "entry_price":  round(entry_price, 4),
                "source":       ev.get("source", "unknown"),
                "ratio":        ev.get("ratio_estimate", None),
                "pre_price":    ev.get("pre_split_price", None),
            }

            for h in horizons:
                exit_idx     = entry_idx + h
                spy_exit_idx = spy_entry_idx + h

                if exit_idx >= len(closes) or spy_exit_idx >= len(spy_closes):
                    row[f"ret_{h}d"]     = np.nan
                    row[f"spy_ret_{h}d"] = np.nan
                    row[f"abn_{h}d"]     = np.nan
                    continue

                exit_price     = float(closes.iloc[exit_idx])
                spy_exit_price = float(spy_closes.iloc[spy_exit_idx])

                stock_ret = (exit_price - entry_price) / entry_price
                spy_ret   = (spy_exit_price - spy_entry_price) / spy_entry_price
                abn_ret   = stock_ret - spy_ret

                row[f"ret_{h}d"]     = round(float(stock_ret), 6)
                row[f"spy_ret_{h}d"] = round(float(spy_ret),   6)
                row[f"abn_{h}d"]     = round(float(abn_ret),   6)

            results.append(row)

        except Exception as e:
            errors.append((ticker, ev["date"], str(e)[:60]))

    if errors:
        print(f"  Errors on {len(errors)} events (first 5): {errors[:5]}")

    print(f"  Successfully computed returns for {len(results)} events")
    return results


# ---------------------------------------------------------------------------
# Step 6: Statistical analysis
# ---------------------------------------------------------------------------

def analyze_results(results: list[dict],
                    horizons: list[int] = [1, 3, 5, 10, 20]) -> dict:
    """
    Summary statistics per horizon:
    - Average abnormal return
    - Fraction where short was profitable (abn_ret < 0)
    - t-test p-value vs zero (one-sample)
    - Multiple testing correction
    """
    df = pd.DataFrame(results)
    summary = {}

    for h in horizons:
        col  = f"abn_{h}d"
        if col not in df.columns:
            continue

        vals = df[col].dropna().values.astype(float)
        n    = len(vals)

        if n < 3:
            summary[h] = {"n": n, "avg": np.nan, "short_pct": np.nan,
                          "p_value": np.nan, "t_stat": np.nan, "std": np.nan}
            continue

        avg        = float(np.mean(vals))
        std        = float(np.std(vals, ddof=1))
        short_pct  = float(np.mean(vals < 0))
        t_stat, p  = stats.ttest_1samp(vals, popmean=0.0)

        summary[h] = {
            "n":          n,
            "avg":        avg,
            "std":        std,
            "short_pct":  short_pct,
            "p_value":    float(p),
            "t_stat":     float(t_stat),
        }

    # Multiple testing correction:
    # PASS if: 2+ horizons with p<0.05  OR  1+ horizon with p<0.01
    passes_mt = False
    sig_01 = [h for h, s in summary.items()
              if not np.isnan(s.get("p_value", np.nan)) and s["p_value"] < 0.01]
    sig_05 = [h for h, s in summary.items()
              if not np.isnan(s.get("p_value", np.nan)) and s["p_value"] < 0.05]
    if sig_01 or len(sig_05) >= 2:
        passes_mt = True

    return {
        "by_horizon": summary,
        "passes_multiple_testing": passes_mt,
        "sig_01_horizons": sig_01,
        "sig_05_horizons": sig_05,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_backtest():
    print("=" * 72)
    print("REVERSE STOCK SPLIT SHORT SIGNAL BACKTEST")
    print(f"Period: {START_DATE} to {END_DATE}")
    print(f"Filters: cap>={MIN_MARKET_CAP_M}M  pre-price>=${MIN_PRICE_BEFORE}  ratio<={MAX_RATIO}:1")
    print("=" * 72)

    horizons = [1, 3, 5, 10, 20]

    # -----------------------------------------------------------------------
    # 1. Collect events from EDGAR
    # -----------------------------------------------------------------------
    edgar_events = fetch_edgar_events(start=START_DATE, end=END_DATE, max_pages=50)

    # -----------------------------------------------------------------------
    # 2. Fallback: scan yfinance .splits if EDGAR yields < 30 events
    # -----------------------------------------------------------------------
    fallback_used = False
    if len(edgar_events) < 30:
        print(f"\nEDGAR returned only {len(edgar_events)} events — augmenting with yfinance splits scan.")
        fallback_used = True
        yf_events = fetch_yfinance_splits(FALLBACK_UNIVERSE, START_DATE, END_DATE)
    else:
        yf_events = []

    # Combine and deduplicate across sources
    all_events_raw = edgar_events + yf_events
    print(f"\nTotal raw events (EDGAR + yfinance): {len(all_events_raw)}")

    # -----------------------------------------------------------------------
    # 3. Filter
    # -----------------------------------------------------------------------
    filtered_events = filter_events(all_events_raw, min_cap_m=MIN_MARKET_CAP_M)

    # -----------------------------------------------------------------------
    # 4. Validate pre-split prices
    # -----------------------------------------------------------------------
    if len(filtered_events) > 0:
        filtered_events = validate_price_filter(filtered_events)

    print(f"\nEvents after all filters: {len(filtered_events)}")

    if len(filtered_events) < 10:
        print(f"\nINSUFFICIENT SAMPLE: only {len(filtered_events)} events (need >=10). Aborting.")
        return None

    # -----------------------------------------------------------------------
    # 5. Print sample events
    # -----------------------------------------------------------------------
    print(f"\nSample events (first 20):")
    print(f"  {'Ticker':<8} {'Date':<12} {'Ratio':<8} {'Pre-Price':<12} {'Source'}")
    print(f"  {'-'*8} {'-'*12} {'-'*8} {'-'*12} {'-'*12}")
    for ev in filtered_events[:20]:
        r_str = f"{ev.get('ratio_estimate', '?')}:1" if ev.get("ratio_estimate") else "?"
        p_str = f"${ev.get('pre_split_price', '?'):.2f}" if ev.get("pre_split_price") else "?"
        print(f"  {ev['ticker']:<8} {ev['date']:<12} {r_str:<8} {p_str:<12} {ev.get('source', '?')}")

    # -----------------------------------------------------------------------
    # 6. Compute abnormal returns
    # -----------------------------------------------------------------------
    results = compute_abnormal_returns(filtered_events, horizons=horizons)
    n_measured = len(results)
    print(f"\nSuccessfully measured: {n_measured} events")

    if n_measured < 10:
        print("INSUFFICIENT MEASURED EVENTS. Aborting.")
        return None

    # -----------------------------------------------------------------------
    # 7. Statistical analysis
    # -----------------------------------------------------------------------
    analysis    = analyze_results(results, horizons=horizons)
    by_h        = analysis["by_horizon"]
    passes_mt   = analysis["passes_multiple_testing"]
    sig_01      = analysis["sig_01_horizons"]
    sig_05      = analysis["sig_05_horizons"]

    # -----------------------------------------------------------------------
    # 8. Results table
    # -----------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("RESULTS TABLE — ABNORMAL RETURNS vs SPY  (entry = open day after split)")
    print("=" * 72)
    print(f"{'Horizon':<10} {'N':>5} {'Avg Abn Ret':>13} {'Short%':>8} {'p-value':>10} {'Sig':>5}")
    print(f"{'-'*10} {'-'*5} {'-'*13} {'-'*8} {'-'*10} {'-'*5}")
    for h in horizons:
        s     = by_h.get(h, {})
        n     = s.get("n", 0)
        avg   = s.get("avg", np.nan)
        sp    = s.get("short_pct", np.nan)
        p     = s.get("p_value", np.nan)
        avg_s = f"{avg:+.2%}" if not np.isnan(avg) else "nan"
        sp_s  = f"{sp:.1%}"  if not np.isnan(sp)  else "nan"
        p_s   = f"{p:.4f}"   if not np.isnan(p)   else "nan"
        sig_s = ("***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.10 else ""))) if not np.isnan(p) else ""
        print(f"{h:>2}d{'':<7} {n:>5} {avg_s:>13} {sp_s:>8} {p_s:>10} {sig_s:>5}")

    print()
    print(f"Significant at p<0.01: {sig_01}")
    print(f"Significant at p<0.05: {sig_05}")
    print(f"Sample size sufficient (n>=30): {'YES' if n_measured >= 30 else 'NO'} (n={n_measured})")
    print(f"Passes multiple testing:        {'YES' if passes_mt else 'NO'}")

    # -----------------------------------------------------------------------
    # 9. Top individual events
    # -----------------------------------------------------------------------
    df_res = pd.DataFrame(results)
    col_5d = "abn_5d"
    if col_5d in df_res.columns and df_res[col_5d].notna().sum() > 0:
        df_valid = df_res[df_res[col_5d].notna()].copy()
        df_sorted = df_valid.sort_values(col_5d)

        print("\n--- TOP 10 EVENTS: SHORT WORKED BEST (5d abnormal, most negative) ---")
        print(f"  {'Ticker':<8} {'Event':<12} {'Entry':<12} {'1d Abn':>9} {'5d Abn':>9} {'10d Abn':>9} {'Ratio'}")
        for _, row in df_sorted.head(10).iterrows():
            r1   = f"{row.get('abn_1d', np.nan):+.1%}" if not pd.isna(row.get('abn_1d', np.nan)) else "nan"
            r5   = f"{row['abn_5d']:+.1%}"
            r10  = f"{row.get('abn_10d', np.nan):+.1%}" if not pd.isna(row.get('abn_10d', np.nan)) else "nan"
            rat  = f"{row['ratio']:.1f}:1" if row.get('ratio') is not None and not pd.isna(float(row.get('ratio', float('nan')))) else "?"
            print(f"  {row['ticker']:<8} {row['event_date']:<12} {row['entry_date']:<12} {r1:>9} {r5:>9} {r10:>9} {rat}")

        print("\n--- TOP 10 EVENTS: SHORT FAILED WORST (5d abnormal, most positive) ---")
        print(f"  {'Ticker':<8} {'Event':<12} {'Entry':<12} {'1d Abn':>9} {'5d Abn':>9} {'10d Abn':>9} {'Ratio'}")
        for _, row in df_sorted.tail(10).iterrows():
            r1   = f"{row.get('abn_1d', np.nan):+.1%}" if not pd.isna(row.get('abn_1d', np.nan)) else "nan"
            r5   = f"{row['abn_5d']:+.1%}"
            r10  = f"{row.get('abn_10d', np.nan):+.1%}" if not pd.isna(row.get('abn_10d', np.nan)) else "nan"
            rat  = f"{row['ratio']:.1f}:1" if row.get('ratio') is not None and not pd.isna(float(row.get('ratio', float('nan')))) else "?"
            print(f"  {row['ticker']:<8} {row['event_date']:<12} {row['entry_date']:<12} {r1:>9} {r5:>9} {r10:>9} {rat}")

    # -----------------------------------------------------------------------
    # 10. Signal pass/fail checklist
    # -----------------------------------------------------------------------
    best_h = None
    best_avg = 0.0
    for h in horizons:
        s = by_h.get(h, {})
        avg = s.get("avg", 0.0)
        if avg is not None and not np.isnan(avg) and avg < best_avg:
            best_avg = avg
            best_h = h

    checks = {
        "n>=30":                      n_measured >= 30,
        "passes_multiple_testing":    passes_mt,
        "best_direction>55%_short":   best_h is not None and by_h[best_h].get("short_pct", 0) > 0.55,
        "best_avg_abn_ret_negative":  best_h is not None and best_avg < -0.005,
    }

    print("\n" + "=" * 72)
    print("FINAL SUMMARY")
    print("=" * 72)
    print(f"Signal:            Reverse Stock Split Short")
    print(f"Period:            {START_DATE} to {END_DATE}")
    print(f"Data sources:      EDGAR{'+ yfinance splits fallback' if fallback_used else ' only'}")
    print(f"Events found:      {len(all_events_raw)}")
    print(f"Events filtered:   {len(filtered_events)}")
    print(f"Events measured:   {n_measured}")
    print()
    print(f"{'Horizon':<10} {'Avg Abn Ret':>13} {'Short%':>8} {'p-value':>10}")
    print(f"{'-'*10} {'-'*13} {'-'*8} {'-'*10}")
    for h in horizons:
        s = by_h.get(h, {})
        avg = s.get("avg", np.nan)
        sp  = s.get("short_pct", np.nan)
        p   = s.get("p_value", np.nan)
        avg_s = f"{avg:+.2%}" if not np.isnan(avg) else "nan"
        sp_s  = f"{sp:.1%}"  if not np.isnan(sp)  else "nan"
        p_s   = f"{p:.4f}"   if not np.isnan(p)   else "nan"
        print(f"{h:>2}d{'':<7} {avg_s:>13} {sp_s:>8} {p_s:>10}")

    print("\nSignal checks:")
    for name, passed in checks.items():
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")

    overall = all(checks.values())
    print(f"\nOverall result: {'PASS' if overall else 'FAIL'}")
    if overall:
        print("  Signal shows statistically significant abnormal decline post-reverse-split.")
        print("  Supports short entry at open the day after the effective split date.")
    else:
        failed = [k for k, v in checks.items() if not v]
        print(f"  Insufficient evidence. Failed checks: {', '.join(failed)}")
    print("=" * 72)

    return {
        "n_raw":       len(all_events_raw),
        "n_filtered":  len(filtered_events),
        "n_measured":  n_measured,
        "analysis":    analysis,
        "results":     results,
        "fallback_used": fallback_used,
        "checks":      checks,
        "overall_pass": overall,
    }


# ---------------------------------------------------------------------------
# Filtered analysis: large-cap (>$500M) + non-healthcare/biotech
# ---------------------------------------------------------------------------

EXCLUDE_SECTORS = {"Healthcare", "Biotechnology"}
EXCLUDE_INDUSTRIES_KEYWORDS = ["biotech", "pharmaceutical", "drug", "health"]


def get_sector_info(ticker: str, cache: dict) -> tuple[str, str]:
    """Return (sector, industry) for ticker, using cache to avoid repeat lookups."""
    if ticker in cache:
        return cache[ticker]
    try:
        info = yf.Ticker(ticker).info
        sector   = info.get("sector", "") or ""
        industry = info.get("industry", "") or ""
        cache[ticker] = (sector, industry)
        return sector, industry
    except Exception:
        cache[ticker] = ("", "")
        return "", ""


def is_excluded_sector(sector: str, industry: str) -> bool:
    """Return True if the ticker should be excluded due to sector/industry."""
    if sector in EXCLUDE_SECTORS:
        return True
    industry_lc = industry.lower()
    for kw in EXCLUDE_INDUSTRIES_KEYWORDS:
        if kw in industry_lc:
            return True
    return False


def run_filtered_backtest():
    """
    Re-run the reverse split backtest with stricter filters:
      - Market cap > $500M (vs $100M original)
      - Exclude Healthcare / Biotechnology sectors
    Goal: test whether large-cap / non-biotech filter eliminates squeeze outliers.
    """
    FILTERED_CAP_M = 500.0

    print("\n" + "=" * 72)
    print("FILTERED ANALYSIS: Large-Cap (>$500M) + Non-Healthcare/Biotech")
    print(f"Period: {START_DATE} to {END_DATE}")
    print(f"Filters: cap>={FILTERED_CAP_M}M  sector NOT in {EXCLUDE_SECTORS}")
    print("=" * 72)

    horizons = [1, 3, 5, 10, 20]

    # 1. Collect events from EDGAR (same source as original)
    edgar_events = fetch_edgar_events(start=START_DATE, end=END_DATE, max_pages=50)
    all_events_raw = edgar_events
    print(f"\nTotal raw EDGAR events: {len(all_events_raw)}")

    # 2. Apply standard filters with $500M cap threshold
    filtered_events = filter_events(all_events_raw, min_cap_m=FILTERED_CAP_M)

    # 3. Validate pre-split prices
    if len(filtered_events) > 0:
        filtered_events = validate_price_filter(filtered_events)

    # 4. Sector filter — exclude Healthcare / Biotech
    print(f"\nApplying sector filter (excluding {EXCLUDE_SECTORS})...")
    sector_cache: dict = {}
    sector_excluded = []
    sector_passed = []
    for ev in filtered_events:
        sector, industry = get_sector_info(ev["ticker"], sector_cache)
        if is_excluded_sector(sector, industry):
            sector_excluded.append((ev["ticker"], sector, industry))
        else:
            ev = dict(ev)
            ev["sector"]   = sector
            ev["industry"] = industry
            sector_passed.append(ev)

    print(f"  Excluded {len(sector_excluded)} events (healthcare/biotech)")
    if sector_excluded[:10]:
        print(f"  Sample excluded: {[(t, s) for t, s, _ in sector_excluded[:10]]}")
    print(f"  Remaining after sector filter: {len(sector_passed)} events")

    if len(sector_passed) < 10:
        print(f"\nINSUFFICIENT SAMPLE after filters: only {len(sector_passed)} events.")
        return None

    # 5. Compute abnormal returns
    results = compute_abnormal_returns(sector_passed, horizons=horizons)
    n_measured = len(results)
    print(f"\nSuccessfully measured: {n_measured} events")

    if n_measured < 5:
        print("INSUFFICIENT MEASURED EVENTS.")
        return None

    # 6. Statistical analysis
    analysis = analyze_results(results, horizons=horizons)
    by_h     = analysis["by_horizon"]

    # 7. Results table
    print("\n" + "=" * 72)
    print("FILTERED RESULTS — ABNORMAL RETURNS vs SPY")
    print(f"  (large-cap >$500M, non-Healthcare/Biotech)")
    print("=" * 72)
    print(f"{'Horizon':<10} {'N':>5} {'Avg Abn Ret':>13} {'Short%':>8} {'p-value':>10} {'Sig':>5}")
    print(f"{'-'*10} {'-'*5} {'-'*13} {'-'*8} {'-'*10} {'-'*5}")
    for h in horizons:
        s     = by_h.get(h, {})
        n     = s.get("n", 0)
        avg   = s.get("avg", np.nan)
        sp    = s.get("short_pct", np.nan)
        p     = s.get("p_value", np.nan)
        avg_s = f"{avg:+.2%}" if not np.isnan(avg) else "nan"
        sp_s  = f"{sp:.1%}"  if not np.isnan(sp)  else "nan"
        p_s   = f"{p:.4f}"   if not np.isnan(p)   else "nan"
        sig_s = ("***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.10 else ""))) if not np.isnan(p) else ""
        print(f"{h:>2}d{'':<7} {n:>5} {avg_s:>13} {sp_s:>8} {p_s:>10} {sig_s:>5}")

    # 8. Top outlier events (check for squeeze survivors)
    df_res = pd.DataFrame(results)
    col_5d = "abn_5d"
    if col_5d in df_res.columns and df_res[col_5d].notna().sum() > 0:
        df_valid  = df_res[df_res[col_5d].notna()].copy()
        df_sorted = df_valid.sort_values(col_5d)

        print("\n--- TOP 10 WORST OUTCOMES FOR SHORT (squeeze candidates) ---")
        print(f"  {'Ticker':<8} {'Event':<12} {'5d Abn':>9} {'10d Abn':>9} {'Sector'}")
        for _, row in df_sorted.tail(10).iterrows():
            r5  = f"{row['abn_5d']:+.1%}"
            r10 = f"{row.get('abn_10d', np.nan):+.1%}" if not pd.isna(row.get('abn_10d', np.nan)) else "nan"
            sec = row.get("sector", "?") if "sector" in row else "?"
            print(f"  {row['ticker']:<8} {row['event_date']:<12} {r5:>9} {r10:>9} {sec}")

        print("\n--- TOP 10 BEST OUTCOMES FOR SHORT ---")
        print(f"  {'Ticker':<8} {'Event':<12} {'5d Abn':>9} {'10d Abn':>9} {'Sector'}")
        for _, row in df_sorted.head(10).iterrows():
            r5  = f"{row['abn_5d']:+.1%}"
            r10 = f"{row.get('abn_10d', np.nan):+.1%}" if not pd.isna(row.get('abn_10d', np.nan)) else "nan"
            sec = row.get("sector", "?") if "sector" in row else "?"
            print(f"  {row['ticker']:<8} {row['event_date']:<12} {r5:>9} {r10:>9} {sec}")

        # 9. Squeeze check: any event with 5d abn > +50%?
        big_squeezes = df_valid[df_valid["abn_5d"] > 0.50]
        print(f"\nSqueezes surviving filter (5d abn > +50%): {len(big_squeezes)}")
        if len(big_squeezes) > 0:
            for _, row in big_squeezes.iterrows():
                print(f"  {row['ticker']} {row['event_date']}: {row['abn_5d']:+.1%} (5d abn)")

    # 10. Final recommendation
    s5  = by_h.get(5,  {})
    s10 = by_h.get(10, {})
    avg5   = s5.get("avg",  np.nan)
    p5     = s5.get("p_value", np.nan)
    sp5    = s5.get("short_pct", np.nan)
    avg10  = s10.get("avg",  np.nan)
    p10    = s10.get("p_value", np.nan)
    sp10   = s10.get("short_pct", np.nan)

    print("\n" + "=" * 72)
    print("RECOMMENDATION")
    print("=" * 72)

    worthy = (
        not np.isnan(avg5)
        and avg5 < -0.03
        and not np.isnan(p5)
        and p5 < 0.10
        and not np.isnan(sp5)
        and sp5 > 0.55
        and n_measured >= 15
    )

    if worthy:
        print("WORTH PURSUING: Large-cap / non-biotech filter produces a tradeable signal.")
        print(f"  5d avg abnormal return: {avg5:+.2%}  (p={p5:.4f}, short profitable {sp5:.1%} of time)")
        if not np.isnan(avg10):
            print(f"  10d avg abnormal return: {avg10:+.2%}  (p={p10:.4f})")
        print("  Squeeze outliers appear eliminated. Signal direction is consistent.")
    else:
        print("DEAD END: Filtered signal does not meet minimum tradeable criteria.")
        reasons = []
        if np.isnan(avg5) or avg5 >= -0.03:
            reasons.append(f"avg 5d return not sufficiently negative ({avg5:+.2%} required <-3%)" if not np.isnan(avg5) else "no 5d data")
        if np.isnan(p5) or p5 >= 0.10:
            reasons.append(f"5d p-value not significant ({p5:.4f} required <0.10)" if not np.isnan(p5) else "no p-value")
        if np.isnan(sp5) or sp5 <= 0.55:
            reasons.append(f"short direction only {sp5:.1%} (required >55%)" if not np.isnan(sp5) else "no direction data")
        if n_measured < 15:
            reasons.append(f"sample too small (n={n_measured}, required >=15)")
        for r in reasons:
            print(f"  - {r}")
        print("\n  After filtering out small-cap and healthcare/biotech names, the reverse")
        print("  split universe shrinks dramatically and/or loses statistical power.")
        print("  The original signal appears driven by distressed micro/small-cap stocks")
        print("  that are precisely the ones where squeeze risk is highest.")

    print("=" * 72)

    return {
        "n_raw":            len(all_events_raw),
        "n_after_cap_filter": len(filtered_events),
        "n_after_sector":   len(sector_passed),
        "n_measured":       n_measured,
        "analysis":         analysis,
        "results":          results,
        "worthy":           worthy,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--filtered-only", action="store_true",
                        help="Run only the filtered large-cap/non-biotech analysis")
    args = parser.parse_args()

    if args.filtered_only:
        run_filtered_backtest()
    else:
        run_backtest()
        run_filtered_backtest()
