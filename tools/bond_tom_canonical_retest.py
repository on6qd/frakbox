"""
Bond ETF Turn-of-Month (TOM) canonical retest

Scan hit f58c3df4 flagged TOM effect across SHY/AGG/IEF/VCSH/LQD/HYG/JNK/EMB.
Canonical retest: require effect present in BOTH discovery (2008-2019)
AND validation (2020-01-01 onward) with same sign and p<0.05.

TOM = last trading day + first 3 trading days of next month (academic std).
Non-TOM = all other trading days.

Transaction cost model: 0.10% round-trip (IG bond ETFs are liquid, 1-3 bps per side)
Economic threshold: TOM - non-TOM mean diff > 0.05%/day (after costs)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from scipy import stats
from tools.yfinance_utils import safe_download

TICKERS = ["SHY", "AGG", "IEF", "VCSH", "LQD", "HYG", "JNK", "EMB"]
DISCOVERY_END = "2019-12-31"
VALIDATION_START = "2020-01-01"
FULL_START = "2008-01-01"
FULL_END = "2026-04-21"

def get_close(df, ticker=None):
    if isinstance(df.columns, pd.MultiIndex):
        if ticker and ticker in df["Close"].columns:
            return df["Close"][ticker]
        return df["Close"].iloc[:, 0]
    return df["Close"]

def label_tom(daily):
    """Last trading day + first 3 trading days of next month = TOM.
    This is the academic Lakonishok/Ariel definition (5-6 day window)."""
    daily = daily.copy()
    daily["ym"] = daily.index.to_period("M")
    daily["tom_type"] = "non_tom"

    periods = sorted(daily["ym"].unique())
    for i, ym in enumerate(periods):
        idx = daily[daily["ym"] == ym].index
        if len(idx) < 3:
            continue
        # Last trading day of this month
        last_day = idx[-1]
        daily.loc[last_day, "tom_type"] = "last1"
        # First 3 trading days of next month
        if i + 1 < len(periods):
            next_idx = daily[daily["ym"] == periods[i + 1]].index
            if len(next_idx) >= 3:
                first3 = next_idx[:3]
                daily.loc[first3, "tom_type"] = "first3"
    daily["is_tom"] = daily["tom_type"] != "non_tom"
    return daily

def analyze(ticker, price, label):
    ret = price.pct_change().dropna() * 100  # in %
    df = pd.DataFrame({"ret": ret})
    df = label_tom(df)

    tom = df.loc[df["is_tom"], "ret"]
    non_tom = df.loc[~df["is_tom"], "ret"]

    if len(tom) < 30 or len(non_tom) < 30:
        return None

    t, p = stats.ttest_ind(tom, non_tom, equal_var=False)
    diff = tom.mean() - non_tom.mean()

    # Mann-Whitney (non-parametric, robust to tails)
    u, p_mw = stats.mannwhitneyu(tom, non_tom, alternative="two-sided")

    return {
        "ticker": ticker,
        "label": label,
        "n_tom": len(tom),
        "n_non_tom": len(non_tom),
        "tom_mean": tom.mean(),
        "non_tom_mean": non_tom.mean(),
        "diff": diff,
        "t": t,
        "p": p,
        "p_mw": p_mw,
        "tom_std": tom.std(),
        "start": df.index[0].date(),
        "end": df.index[-1].date(),
    }

def main():
    print("=" * 95)
    print("BOND ETF TURN-OF-MONTH (TOM) — CANONICAL RETEST")
    print("Discovery: 2008-01-01 to 2019-12-31 | Validation: 2020-01-01 to 2026-04-21")
    print("TOM = last trading day of month + first 3 trading days of next month")
    print("=" * 95)

    rows = []
    for t in TICKERS:
        try:
            raw = safe_download(t, start=FULL_START, end=FULL_END)
            if raw is None or raw.empty:
                print(f"  [SKIP] {t}: no data")
                continue
            price = get_close(raw, t).sort_index().dropna()
            if price.empty:
                print(f"  [SKIP] {t}: empty close series")
                continue

            # Discovery subsample
            disc_price = price.loc[:DISCOVERY_END]
            val_price = price.loc[VALIDATION_START:]
            full_price = price

            r_full = analyze(t, full_price, "FULL") if len(full_price) > 60 else None
            r_disc = analyze(t, disc_price, "DISC") if len(disc_price) > 60 else None
            r_val = analyze(t, val_price, "VAL") if len(val_price) > 60 else None

            for r in (r_full, r_disc, r_val):
                if r is not None:
                    rows.append(r)

        except Exception as e:
            print(f"  [ERROR] {t}: {e}")
            continue

    if not rows:
        print("NO RESULTS")
        return

    df = pd.DataFrame(rows)
    # Print nicely
    for t in TICKERS:
        sub = df[df["ticker"] == t]
        if sub.empty:
            continue
        print(f"\n--- {t} ---")
        print(f"  {'Sample':<5} {'Window':<24} {'N_TOM':>5} {'N_ND':>6} {'TOM':>8} {'NON-TOM':>9} {'DIFF':>8} {'t':>7} {'p-t':>10} {'p-mw':>10}")
        for _, r in sub.iterrows():
            sig_t = "***" if r["p"] < 0.01 else ("**" if r["p"] < 0.05 else ("*" if r["p"] < 0.10 else ""))
            print(f"  {r['label']:<5} {str(r['start'])+'→'+str(r['end']):<24} "
                  f"{r['n_tom']:>5} {r['n_non_tom']:>6} "
                  f"{r['tom_mean']:>+7.3f}% {r['non_tom_mean']:>+8.3f}% "
                  f"{r['diff']:>+7.3f}% {r['t']:>7.2f} {r['p']:>9.4f}{sig_t:<1} {r['p_mw']:>9.4f}")

    print("\n" + "=" * 95)
    print("CANONICAL PASS/FAIL VERDICT")
    print("=" * 95)

    verdicts = []
    for t in TICKERS:
        sub = df[df["ticker"] == t]
        disc = sub[sub["label"] == "DISC"].iloc[0] if not sub[sub["label"] == "DISC"].empty else None
        val = sub[sub["label"] == "VAL"].iloc[0] if not sub[sub["label"] == "VAL"].empty else None

        if disc is None or val is None:
            verdicts.append({"ticker": t, "verdict": "INSUFFICIENT_DATA"})
            continue

        disc_sig = disc["p"] < 0.05
        val_sig = val["p"] < 0.05
        same_sign = (disc["diff"] > 0) == (val["diff"] > 0)
        disc_econ = abs(disc["diff"]) > 0.05  # 0.05% threshold
        val_econ = abs(val["diff"]) > 0.05

        # CANONICAL: sig + sig + same sign + econ + econ
        passes = disc_sig and val_sig and same_sign and disc_econ and val_econ

        reason = []
        if not disc_sig:
            reason.append(f"disc_p={disc['p']:.3f}")
        if not val_sig:
            reason.append(f"val_p={val['p']:.3f}")
        if not same_sign:
            reason.append("SIGN_FLIP")
        if not disc_econ:
            reason.append(f"disc_diff={disc['diff']:.3f}%_sub_thresh")
        if not val_econ:
            reason.append(f"val_diff={val['diff']:.3f}%_sub_thresh")

        verdicts.append({
            "ticker": t,
            "disc_diff": disc["diff"],
            "disc_p": disc["p"],
            "val_diff": val["diff"],
            "val_p": val["p"],
            "canonical_passes": passes,
            "fail_reason": ",".join(reason) if reason else "OK",
        })

    print(f"\n  {'Ticker':<7} {'DiscDiff':>10} {'DiscP':>8} {'ValDiff':>10} {'ValP':>8} {'PASS':>6}  Reason")
    for v in verdicts:
        if "disc_diff" in v:
            p_mark = "YES" if v["canonical_passes"] else "NO"
            print(f"  {v['ticker']:<7} {v['disc_diff']:>+9.3f}% {v['disc_p']:>8.4f} {v['val_diff']:>+9.3f}% {v['val_p']:>8.4f} {p_mark:>6}  {v['fail_reason']}")
        else:
            print(f"  {v['ticker']:<7} {'---':>10} {'---':>8} {'---':>10} {'---':>8} {'SKIP':>6}  {v['verdict']}")

    passed = [v for v in verdicts if v.get("canonical_passes")]
    print(f"\n  Canonical-pass count: {len(passed)} / {len([v for v in verdicts if 'disc_diff' in v])}")
    if passed:
        print("  Survivors: " + ", ".join(v["ticker"] for v in passed))
    print()

if __name__ == "__main__":
    main()
