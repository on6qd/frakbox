"""
S&P 500 Addition Candidate Screener

Identifies companies most likely to be added to the S&P 500 in the next quarterly rebalance.
Screens S&P MidCap 400 members and other large non-S&P 500 companies for eligibility.

Eligibility criteria (as of 2025):
  - Market cap >= $22.7B (updated periodically by S&P)
  - Positive GAAP earnings: sum of last 4 quarters > 0 AND most recent quarter > 0
  - Adequate liquidity (250K shares/month for 6 months)
  - U.S. domicile
  - Not already in S&P 500

Usage:
    python tools/sp500_candidate_screener.py                # Full screen
    python tools/sp500_candidate_screener.py --top 10       # Top 10 candidates
    python tools/sp500_candidate_screener.py --refresh-sp500  # Refresh SP500 universe first
    python tools/sp500_candidate_screener.py --min-cap 20    # Override min cap ($B)
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

# S&P MidCap 400 tickers (from Wikipedia, April 2026)
# This is the primary feeder index for S&P 500 promotions
SP400_TICKERS = [
    "AA","AAL","AAON","ACI","ACM","ADC","AEIS","AFG","AGCO","AHR","AIT","ALGM",
    "ALK","ALLY","ALV","AM","AMG","AMH","AMKR","AN","ANF","APG","APPF","AR",
    "ARMK","ARW","ARWR","ASB","ASH","ATI","ATR","AVAV","AVNT","AVT","AVTR",
    "AXTA","AYI","BAH","BBWI","BC","BCO","BDC","BHF","BILL","BIO","BJ","BKH",
    "BLD","BLKB","BMRN","BRBR","BRKR","BROS","BRX","BSY","BURL","BWA","BWXT",
    "BYD","CACI","CAR","CART","CASY","CAVA","CBSH","CBT","CCK","CDP","CELH",
    "CFR","CG","CGNX","CHDN","CHE","CHH","CHRD","CHWY","CLF","CLH","CMC","CNH",
    "CNM","CNO","CNX","CNXC","COKE","COLB","COLM","COTY","CPRI","CR","CRBG",
    "CROX","CRS","CRUS","CSL","CTRE","CUBE","CUZ","CVLT","CW","CXT","CYTK",
    "DAR","DBX","DCI","DINO","DKS","DLB","DOCS","DOCU","DT","DTM","DUOL","DY",
    "EEFT","EGP","EHC","ELAN","ELF","ELS","ENS","ENSG","ENTG","EPR","EQH",
    "ESAB","ESNT","EVR","EWBC","EXEL","EXLS","EXP","EXPO","FAF","FBIN","FCFS",
    "FCN","FFIN","FHI","FHN","FIVE","FLEX","FLG","FLO","FLR","FLS","FN","FNB",
    "FND","FNF","FOUR","FR","FTI","G","GAP","GATX","GBCI","GEF","GGG","GHC",
    "GLPI","GME","GMED","GNTX","GPK","GT","GTLS","GWRE","GXO","H","HAE","HALO",
    "HGV","HIMS","HL","HLI","HLNE","HOG","HOMB","HQY","HR","HRB","HWC","HXL",
    "IBOC","IDA","IDCC","ILMN","INGR","IPGP","IRT","ITT","JAZZ","JEF","JHG",
    "JLL","KBH","KBR","KD","KEX","KNF","KNSL","KNX","KRC","KRG","KTOS","LAD",
    "LAMR","LEA","LECO","LFUS","LIVN","LNTH","LOPE","LPX","LSCC","LSTR","M",
    "MANH","MASI","MAT","MEDP","MIDD","MKSI","MLI","MMS","MOG.A","MORN","MP",
    "MSA","MSM","MTDR","MTG","MTN","MTSI","MTZ","MUR","MUSA","NBIX","NEU",
    "NFG","NJR","NLY","NNN","NOV","NOVT","NVST","NVT","NWE","NXST","NXT","NYT",
    "OC","OGE","OGS","OHI","OKTA","OLED","OLLI","OLN","ONB","ONTO","OPCH","ORA",
    "ORI","OSK","OVV","OZK","PAG","PATH","PB","PBF","PCTY","PEGA","PEN","PFGC",
    "PII","PINS","PK","PLNT","PNFP","POR","POST","PPC","PR","PRI","PSN","PSTG",
    "PVH","QLYS","R","RBA","RBC","REXR","RGA","RGEN","RGLD","RH","RLI","RMBS",
    "RNR","ROIV","RPM","RRC","RRX","RS","RYAN","RYN","SAIA","SAIC",
    # Estimated remaining S&P 400 tickers (S-Z range, common mid-caps)
    "SAM","SBRA","SCI","SEIC","SF","SFM","SITE","SKX","SLGN","SM","SMPL","SNV",
    "SON","SPB","SSD","STAG","STE","STRA","STR","SWX","SXT","TECH","TDC","TDOC",
    "TDS","TGNA","THC","TKO","TKR","TNET","TNL","TOL","TPX","TXRH","UFPI","UHAL",
    "UMBF","UNFI","USFD","VEEV","VFC","VIRT","VLY","VNOM","VRNT","WFRD","WH",
    "WING","WK","WMS","WOOF","WPC","WSFS","WSM","WSO","WTFC","WWD","X","XPO",
    "YETI","ZI","ZWS",
]

# Additional large companies that may not be in S&P 400 but could be candidates
# (recent large IPOs, de-SPACs, or foreign-listed that became US-listed)
EXTRA_CANDIDATES = [
    "TOST","RKLB","SOFI","PLTR","DASH","U","RIVN","LCID","AFRM","DKNG",
    "MELI","SPOT","SE","NU","GRAB","BIRK","ARM","CELZ","MNDY","CRDO",
    "IOT","VRT","SMCI","TXRH","CPNG",
]

# Default S&P 500 minimum market cap for inclusion ($B)
SP500_MIN_CAP_B = 22.7


def load_sp500_tickers():
    """Load current S&P 500 constituent tickers."""
    # Try additions state first (most current, 503 tickers)
    state_file = Path(__file__).parent.parent / "data" / "sp500_additions_state.json"
    if state_file.exists():
        with open(state_file) as f:
            state = json.load(f)
        tickers = state.get("wikipedia_tickers", [])
        if len(tickers) > 400:
            return set(t.upper() for t in tickers)

    # Fallback to universe file
    universe_file = Path(__file__).parent.parent / "data" / "sp500_universe.json"
    if universe_file.exists():
        with open(universe_file) as f:
            data = json.load(f)
        return set(t.upper() for t in data.get("tickers", []))

    # Last resort: import from build tool
    from tools.build_sp500_universe import load_sp500_universe
    return set(t.upper() for t in load_sp500_universe())


def screen_candidates(min_cap_b=SP500_MIN_CAP_B, top_n=20, verbose=True):
    """Screen for S&P 500 addition candidates.

    Returns list of dicts sorted by market cap descending:
    [{"ticker": str, "name": str, "market_cap_b": float, "trailing_eps": float,
      "sector": str, "source": str, "eligible": bool, "blockers": list}]
    """
    from tools.yfinance_utils import safe_download
    import yfinance as yf

    sp500 = load_sp500_tickers()
    if verbose:
        print(f"Loaded {len(sp500)} S&P 500 tickers")

    # Build candidate universe: S&P 400 + extras, minus anyone already in S&P 500
    all_candidates = set()
    for t in SP400_TICKERS + EXTRA_CANDIDATES:
        t_upper = t.upper().replace(".", "-")  # yfinance uses - not .
        if t_upper not in sp500:
            all_candidates.add(t_upper)

    if verbose:
        print(f"Screening {len(all_candidates)} non-S&P-500 candidates...")

    # Batch fetch market data using yfinance Tickers
    results = []
    batch_size = 50
    candidate_list = sorted(all_candidates)

    for i in range(0, len(candidate_list), batch_size):
        batch = candidate_list[i:i + batch_size]
        if verbose and i > 0:
            print(f"  Processing batch {i // batch_size + 1}...")

        for ticker_str in batch:
            try:
                ticker = yf.Ticker(ticker_str)
                info = ticker.info
                if not info or info.get("regularMarketPrice") is None:
                    continue

                market_cap = info.get("marketCap", 0) or 0
                market_cap_b = market_cap / 1e9

                # Quick filter: skip if way below threshold
                if market_cap_b < min_cap_b * 0.7:
                    continue

                trailing_eps = info.get("trailingEps")
                name = info.get("shortName", info.get("longName", ticker_str))
                sector = info.get("sector", "Unknown")
                country = info.get("country", "Unknown")

                # Check eligibility blockers
                blockers = []
                if market_cap_b < min_cap_b:
                    blockers.append(f"Market cap ${market_cap_b:.1f}B < ${min_cap_b}B threshold")
                if trailing_eps is not None and trailing_eps <= 0:
                    blockers.append(f"Negative trailing EPS: ${trailing_eps:.2f}")
                if trailing_eps is None:
                    blockers.append("Trailing EPS not available")
                if country and country not in ("United States", "Unknown"):
                    blockers.append(f"Non-US domicile: {country}")

                results.append({
                    "ticker": ticker_str,
                    "name": name,
                    "market_cap_b": round(market_cap_b, 2),
                    "trailing_eps": round(trailing_eps, 2) if trailing_eps else None,
                    "sector": sector,
                    "country": country,
                    "source": "SP400" if ticker_str in [t.upper().replace(".", "-") for t in SP400_TICKERS] else "EXTRA",
                    "eligible": len(blockers) == 0,
                    "blockers": blockers,
                })
            except Exception as e:
                continue

        # Rate limit between batches
        if i + batch_size < len(candidate_list):
            time.sleep(0.5)

    # Sort by market cap descending
    results.sort(key=lambda x: x["market_cap_b"], reverse=True)

    # Trim to top_n
    if top_n:
        results = results[:top_n]

    return results


def print_report(candidates, min_cap_b=SP500_MIN_CAP_B):
    """Print a formatted report of candidates."""
    eligible = [c for c in candidates if c["eligible"]]
    near_eligible = [c for c in candidates if not c["eligible"] and c["market_cap_b"] >= min_cap_b * 0.8]

    print(f"\n{'='*80}")
    print(f"S&P 500 ADDITION CANDIDATE SCREEN — {datetime.now().strftime('%Y-%m-%d')}")
    print(f"Threshold: ${min_cap_b}B market cap | Positive trailing EPS | US domicile")
    print(f"{'='*80}")

    print(f"\n## ELIGIBLE CANDIDATES ({len(eligible)}) — Meet all criteria")
    print(f"{'Rank':<5} {'Ticker':<8} {'Name':<35} {'Mkt Cap':>10} {'EPS':>8} {'Sector':<20} {'Source':<8}")
    print("-" * 100)
    for i, c in enumerate(eligible, 1):
        eps_str = f"${c['trailing_eps']:.2f}" if c['trailing_eps'] is not None else "N/A"
        print(f"{i:<5} {c['ticker']:<8} {c['name'][:34]:<35} ${c['market_cap_b']:>8.1f}B {eps_str:>8} {c['sector'][:19]:<20} {c['source']:<8}")

    if near_eligible:
        print(f"\n## NEAR-ELIGIBLE ({len(near_eligible)}) — Close but have blockers")
        print(f"{'Ticker':<8} {'Name':<30} {'Mkt Cap':>10} {'Blockers'}")
        print("-" * 80)
        for c in near_eligible:
            print(f"{c['ticker']:<8} {c['name'][:29]:<30} ${c['market_cap_b']:>8.1f}B {'; '.join(c['blockers'])}")

    # Summary
    print(f"\n## SUMMARY")
    if eligible:
        print(f"Top candidate: {eligible[0]['ticker']} ({eligible[0]['name']}) — ${eligible[0]['market_cap_b']:.1f}B")
        print(f"Total eligible: {len(eligible)} companies above ${min_cap_b}B with positive earnings")
        print(f"\nNote: S&P committee exercises discretion. Market cap rank is the strongest predictor,")
        print(f"but sector balance, recent IPO seasoning, and pending M&A also matter.")
        print(f"Most S&P 500 additions come from the S&P MidCap 400 (marked 'SP400' in Source column).")
    else:
        print("No eligible candidates found above threshold.")

    return {"eligible": len(eligible), "near_eligible": len(near_eligible), "top_candidates": eligible[:5]}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="S&P 500 Addition Candidate Screener")
    parser.add_argument("--top", type=int, default=30, help="Number of top candidates to return")
    parser.add_argument("--min-cap", type=float, default=SP500_MIN_CAP_B, help="Minimum market cap ($B)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--refresh-sp500", action="store_true", help="Refresh S&P 500 universe first")
    args = parser.parse_args()

    if args.refresh_sp500:
        from tools.build_sp500_universe import build_sp500_universe
        build_sp500_universe()

    candidates = screen_candidates(min_cap_b=args.min_cap, top_n=args.top)

    if args.json:
        print(json.dumps(candidates, indent=2))
    else:
        print_report(candidates, min_cap_b=args.min_cap)


if __name__ == "__main__":
    main()
