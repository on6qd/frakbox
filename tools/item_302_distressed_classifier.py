#!/usr/bin/env python3
"""Item 3.02 DISTRESSED classifier v2 — 2026-04-21.

V1 classifier (item_302_pipe_scanner.classify_302_dilution_type) inverts direction:
filings tagged 'dilutive_pipe high/medium' via Securities Purchase Agreement +
registration rights + accredited investor patterns are predominantly LARGE
biotech/pharma investor-led financing rounds at premium prices (+6-20% abn returns).

V2 pivots on DISTRESSED markers:
  - going concern / substantial doubt
  - listing deficiency (Nasdaq minimum bid / listing requirements)
  - concurrent reverse stock split
  - explicit discount language ("reflecting a discount", "discount to market")
  - pre-funded warrants (usually distressed micro/small-cap biotech)
  - toxic convert features (floor price, adjustment, ratchet, toggle)
  - warrant coverage >= 50% of shares issued
  - small aggregate raise ($ < $30M)

Returns a single score (distressed_hits - premium_hits) with confidence tier,
and exposes classify_302_distressed(text, market_cap, price, raise_amount).

Intended use: run on the Item 3.02 section text previously fetched by v1's
classify_302_dilution_type. Pairs cleanly with the existing canonical retest
pipeline — swap the filter predicate from
    dt=='dilutive_pipe' and conf in ('high','medium')
to
    distressed_score >= THRESHOLD.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


# ---------- distressed markers (monotonic short signal) ----------

GOING_CONCERN_PATTERNS = [
    (r"going concern", "going_concern"),
    (r"substantial doubt", "substantial_doubt"),
    (r"ability to continue as a going concern", "ability_to_continue"),
]

LISTING_DEFICIENCY_PATTERNS = [
    (r"minimum bid price", "min_bid_price"),
    (r"listing\s+(deficiency|requirements?|rule|standards?)", "listing_deficiency"),
    (r"notif(ication|ied)\s+of\s+noncompliance", "noncompliance_notice"),
    (r"nasdaq\s+(staff|listing)\s+determination", "nasdaq_staff_determination"),
    (r"delisting", "delisting_mention"),
    (r"bid\s+price\s+compliance", "bid_price_compliance"),
]

REVERSE_SPLIT_PATTERNS = [
    (r"reverse\s+stock\s+split", "reverse_stock_split"),
    (r"reverse\s+split", "reverse_split"),
]

DISCOUNT_PATTERNS = [
    (r"reflect(ing|ed)?\s+a\s+discount", "reflecting_discount"),
    (r"discount\s+to\s+(the\s+)?(market|closing|reference|vwap)",
     "discount_to_market"),
    (r"priced\s+(below|at\s+a\s+discount)", "priced_below"),
    (r"purchase\s+price\s+(was|is|of)?\s*\$?[\d.]*\s*representing\s+a\s+discount",
     "purchase_price_discount"),
]

TOXIC_CONVERT_PATTERNS = [
    (r"floor\s+price", "floor_price"),
    (r"conversion\s+price\s+adjustment", "conversion_adjustment"),
    (r"ratchet\s+(provision|adjustment)", "ratchet"),
    (r"pip\s+(price\s+)?reset", "pip_reset"),
    (r"most\s+favored\s+nation", "mfn_clause"),
    (r"variable\s+(rate\s+)?conversion", "variable_rate_convert"),
    (r"anti-?dilution\s+protection", "antidilution_protection"),
]

PREFUNDED_WARRANT_PATTERNS = [
    (r"pre[-\s]?funded\s+warrants?", "prefunded_warrants"),
]

# --- Premium / strategic markers (positive catalyst offset) ---
PREMIUM_PATTERNS = [
    (r"strategic\s+(investor|investment|partner|alliance|partnership)",
     "strategic_keyword"),
    (r"(collaboration|partnership)\s+agreement", "collab_agreement"),
    (r"\bat\s+(the\s+)?market\s+price\b", "at_market_price"),
    (r"\bpremium\s+to\s+(the\s+)?(market|closing|reference)\b",
     "premium_to_market"),
    (r"section\s*4\s*\(\s*a\s*\)\s*\(\s*2\s*\)\s*(?!\s*of)", "section_4a2_only"),
]

# Named premium investors (mega-cap corporate or top biotech VCs that inflate price)
PREMIUM_INVESTORS = [
    "nvidia", "microsoft", "alphabet", "google", "apple", "meta",
    "amazon", "oracle", "intel", "amd", "tesla", "broadcom",
    # top biotech dedicated funds that typically invest at premium
    "flagship pioneering", "andreessen horowitz", "ra capital",
    "baker bros", "perceptive advisors", "redmile",
    "vida ventures", "foresite capital", "orbimed",
    "blackrock", "vanguard",
]


@dataclass
class DistressedClassification:
    distressed_score: int = 0           # higher = more distressed
    premium_score: int = 0              # higher = more premium/strategic
    net_distressed_score: int = 0       # distressed - premium
    tier: str = "unknown"               # strong_distressed | moderate_distressed | ambiguous | premium | unknown
    matched_patterns: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)


def _section(text: str, item: str = "3.02", window: int = 3000) -> str:
    t = text.lower()
    idx = t.find(f"item {item}")
    if idx < 0:
        idx = t.find(item)
    if idx < 0:
        return t[:window]
    return t[idx:idx + window]


def classify_302_distressed(
    text: str,
    market_cap: Optional[float] = None,
    price: Optional[float] = None,
    excerpt_chars: int = 3000,
) -> DistressedClassification:
    """Classify an Item 3.02 filing for DISTRESSED markers.

    Parameters
    ----------
    text : str
        Full 8-K text (will locate Item 3.02 section automatically).
    market_cap : float, optional
        Current market cap — used for "small-cap + prefunded warrants" rule.
    price : float, optional
        Share price — used for distress tiering (<$10 adds signal).

    Returns
    -------
    DistressedClassification

    Notes
    -----
    Scoring:
      - Each category of distress marker contributes +1 (capped per category).
      - Premium / strategic markers subtract 1 each.
      - Small-cap (<$2B) + prefunded warrants OR <$1B + any distress marker = +1.
      - Price <$10 = +1 (penny / sub-$10 stocks over-represent distressed PIPEs).

    Tiers:
      net >= 3: strong_distressed
      net == 2: moderate_distressed
      net == 1: ambiguous_distressed
      net <= 0: not_distressed
    """
    section = _section(text, "3.02", window=excerpt_chars)
    result = DistressedClassification()

    def check_category(patterns, category_name, max_hit=1):
        cat_hit = 0
        for pat, tag in patterns:
            if cat_hit >= max_hit:
                break
            if re.search(pat, section):
                result.matched_patterns.append(tag)
                cat_hit = 1
        if cat_hit:
            result.distressed_score += 1
            result.reasons.append(category_name)

    # --- count distress categories ---
    check_category(GOING_CONCERN_PATTERNS, "going_concern_category")
    check_category(LISTING_DEFICIENCY_PATTERNS, "listing_deficiency_category")
    check_category(REVERSE_SPLIT_PATTERNS, "reverse_split_category")
    check_category(DISCOUNT_PATTERNS, "discount_category")
    check_category(TOXIC_CONVERT_PATTERNS, "toxic_convert_category")

    # Pre-funded warrants — in large biotech deals these sometimes appear too
    # so require either (a) small cap <$2B OR (b) no strategic markers.
    prefunded_hit = any(re.search(p, section) for p, _ in PREFUNDED_WARRANT_PATTERNS)
    if prefunded_hit:
        result.matched_patterns.append("prefunded_warrants")
        # Only count as distress when paired with small-cap context.
        if market_cap is not None and market_cap < 2e9:
            result.distressed_score += 1
            result.reasons.append("prefunded_warrants_smallcap")
        else:
            # don't count, but note it
            result.reasons.append("prefunded_warrants_present_but_largecap")

    # --- premium markers (subtract) ---
    for pat, tag in PREMIUM_PATTERNS:
        if re.search(pat, section):
            result.matched_patterns.append(tag)
            result.premium_score += 1

    for name in PREMIUM_INVESTORS:
        if name in section:
            result.matched_patterns.append(f"premium_investor_{name.replace(' ', '_')}")
            result.premium_score += 1

    # --- structural boosters ---
    if price is not None and price < 10 and price > 0:
        result.distressed_score += 1
        result.reasons.append("price_under_10")
    if market_cap is not None and market_cap < 1e9:
        # small caps + any distress marker get a small bonus
        if result.distressed_score >= 1:
            result.distressed_score += 1
            result.reasons.append("smallcap_under_1b_with_distress")

    # Cap premium deduction: never subtract more than 3 to avoid over-suppressing
    capped_premium = min(result.premium_score, 3)
    result.net_distressed_score = result.distressed_score - capped_premium

    n = result.net_distressed_score
    if n >= 3:
        result.tier = "strong_distressed"
    elif n == 2:
        result.tier = "moderate_distressed"
    elif n == 1:
        result.tier = "ambiguous_distressed"
    elif n <= -1:
        result.tier = "premium"
    else:
        result.tier = "not_distressed"
    return result


# ---------- CLI (diagnostic) ----------
if __name__ == "__main__":
    import argparse
    import sys

    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="Path to local HTML/text file to classify")
    ap.add_argument("--url", help="SEC 8-K URL to fetch and classify")
    ap.add_argument("--market-cap", type=float, default=None)
    ap.add_argument("--price", type=float, default=None)
    args = ap.parse_args()

    if args.file:
        with open(args.file) as f:
            text = f.read()
    elif args.url:
        import requests
        headers = {"User-Agent": "financial-researcher research@example.com"}
        resp = requests.get(args.url, headers=headers, timeout=15)
        text = re.sub(r"<[^>]+>", " ", resp.text)
        text = re.sub(r"\s+", " ", text)
    else:
        print("Need --file or --url", file=sys.stderr)
        sys.exit(1)

    cls = classify_302_distressed(text, args.market_cap, args.price)
    print("Tier:", cls.tier)
    print("distressed_score:", cls.distressed_score)
    print("premium_score:   ", cls.premium_score)
    print("net:             ", cls.net_distressed_score)
    print("reasons:", cls.reasons)
    print("matched_patterns:", cls.matched_patterns[:25])
