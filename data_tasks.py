#!/usr/bin/env python3
"""
Data task dispatcher — runs formulaic data tasks without an LLM.

The orchestrator calls this CLI instead of reasoning through data work at Opus cost.
Each task stores full results in SQLite (task_results table) and prints a compact
JSON summary to stdout that the orchestrator can read cheaply.

Usage:
    python3 data_tasks.py backtest --events '[{"symbol":"AAPL","date":"2024-01-15"}]'
    python3 data_tasks.py backtest --symbol AAPL --dates '["2024-01-15","2024-04-20"]'
    python3 data_tasks.py verify-date --event "AAPL S&P 500 addition" --expected-date 2024-03-15
    python3 data_tasks.py largecap-filter --symbols '["AAPL","MSFT","TINY"]'
    python3 data_tasks.py price-history --symbol AAPL --days 90
    python3 data_tasks.py get-result --id <result_id>
"""

import argparse
import json
import sys
import uuid
from datetime import datetime

import db


def _store_result(task_type, parameters, result, summary):
    """Store full result in SQLite and return the result ID."""
    result_id = f"T-{uuid.uuid4().hex[:8]}"
    db.store_task_result(
        result_id=result_id,
        task_type=task_type,
        parameters=parameters,
        result=result,
        summary=summary,
    )
    return result_id


def _backtest_summary(result):
    """Extract compact summary from measure_event_impact result."""
    if "error" in result:
        return {"status": "error", "error": result["error"]}

    summary = {
        "status": "ok",
        "events_measured": result.get("events_measured", 0),
        "passes_multiple_testing": result.get("passes_multiple_testing", False),
        "entry_price_mode": result.get("entry_price_mode", "close"),
    }

    # Key metrics at each horizon
    for h in ["1d", "3d", "5d", "10d", "20d"]:
        avg_key = f"avg_abnormal_{h}"
        med_key = f"median_abnormal_{h}"
        pos_key = f"positive_rate_abnormal_{h}"
        p_key = f"p_value_abnormal_{h}"
        ci_key = f"bootstrap_ci_abnormal_{h}"

        if avg_key in result:
            summary[f"abnormal_{h}"] = {
                "avg": result[avg_key],
                "median": result.get(med_key),
                "positive_rate": result.get(pos_key),
                "p_value": result.get(p_key),
            }
            ci = result.get(ci_key)
            if ci and isinstance(ci, dict):
                summary[f"abnormal_{h}"]["ci_excludes_zero"] = ci.get("ci_excludes_zero")

    # Cost info
    if result.get("avg_estimated_cost_pct") is not None:
        summary["avg_cost_pct"] = result["avg_estimated_cost_pct"]

    # Data quality
    if result.get("data_quality_warning"):
        summary["warning"] = result["data_quality_warning"]

    # Sample sufficiency
    if "sample_sufficient" in result:
        summary["sample_sufficient"] = result["sample_sufficient"]

    return summary


def cmd_backtest(args):
    """Run measure_event_impact and store results."""
    import market_data

    params = {}

    if args.events:
        event_dates = json.loads(args.events)
        params["event_dates"] = event_dates
    elif args.symbol and args.dates:
        event_dates = json.loads(args.dates)
        params["symbol"] = args.symbol
        params["event_dates"] = event_dates
    else:
        print(json.dumps({"status": "error", "error": "Provide --events or --symbol + --dates"}))
        return

    kwargs = {}
    if args.symbol:
        kwargs["symbol"] = args.symbol
    kwargs["event_dates"] = event_dates
    kwargs["benchmark"] = args.benchmark or "SPY"
    if args.sector_etf:
        kwargs["sector_etf"] = args.sector_etf
    if args.entry_price:
        kwargs["entry_price"] = args.entry_price
    if args.event_timing:
        kwargs["event_timing"] = args.event_timing
    if args.event_type:
        kwargs["event_type"] = args.event_type
        kwargs["estimate_costs"] = True
    if args.estimate_costs:
        kwargs["estimate_costs"] = True
    if args.regime_filter:
        kwargs["regime_filter"] = args.regime_filter

    params.update(kwargs)

    result = market_data.measure_event_impact(**kwargs)
    summary = _backtest_summary(result)
    result_id = _store_result("backtest", params, result, json.dumps(summary))
    summary["result_id"] = result_id

    print(json.dumps(summary, indent=2))


def cmd_verify_date(args):
    """Verify an event date using the date verification tool."""
    from tools.verify_event_date import verify_event_date

    result = verify_event_date(args.event, args.expected_date)
    summary = {
        "status": "ok",
        "event": args.event,
        "expected_date": args.expected_date,
        "verified": result.get("verified", False),
        "actual_date": result.get("actual_date"),
        "source": result.get("source"),
    }
    if result.get("error"):
        summary["status"] = "error"
        summary["error"] = result["error"]

    result_id = _store_result(
        "verify_date",
        {"event": args.event, "expected_date": args.expected_date},
        result,
        json.dumps(summary),
    )
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2))


def cmd_largecap_filter(args):
    """Filter symbols to large-cap only."""
    from tools.largecap_filter import filter_to_largecap

    symbols = json.loads(args.symbols)
    import pandas as pd
    df = pd.DataFrame({"ticker": symbols})
    result_df = filter_to_largecap(df, ticker_col="ticker", verbose=False)
    largecap = result_df["ticker"].tolist()
    summary = {
        "status": "ok",
        "input_count": len(symbols),
        "output_count": len(largecap),
        "largecap_symbols": largecap,
        "filtered_out": [s for s in symbols if s not in largecap],
    }

    result_id = _store_result(
        "largecap_filter",
        {"symbols": symbols},
        {"largecap": largecap, "filtered_out": summary["filtered_out"]},
        json.dumps(summary),
    )
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2))


def cmd_price_history(args):
    """Fetch price history for a symbol."""
    import market_data

    prices = market_data.get_price_history(args.symbol, days=args.days)
    if not prices:
        summary = {"status": "error", "error": f"No data for {args.symbol}"}
        print(json.dumps(summary))
        return

    # Summary: first/last date, price range, current price
    summary = {
        "status": "ok",
        "symbol": args.symbol,
        "days_requested": args.days,
        "days_returned": len(prices),
        "first_date": prices[0]["date"],
        "last_date": prices[-1]["date"],
        "first_close": prices[0]["close"],
        "last_close": prices[-1]["close"],
        "high": max(p["high"] for p in prices),
        "low": min(p["low"] for p in prices),
        "return_pct": round((prices[-1]["close"] / prices[0]["close"] - 1) * 100, 2),
    }

    result_id = _store_result(
        "price_history",
        {"symbol": args.symbol, "days": args.days},
        prices,
        json.dumps(summary),
    )
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2))


def cmd_scan_insiders(args):
    """Scan EDGAR for insider buying clusters."""
    from tools.edgar_insider_scanner_v2 import scan_insider_clusters

    clusters = scan_insider_clusters(
        days=args.days,
        min_insiders=args.min_insiders,
        min_value_per_insider=args.min_value,
        quiet=True,
    )

    summary = {
        "status": "ok",
        "days": args.days,
        "clusters_found": len(clusters),
        "clusters": [
            {
                "ticker": c["ticker"],
                "issuer_name": c["issuer_name"][:40],
                "n_insiders": c["n_insiders"],
                "total_value": c["total_value"],
                "top_insider": c["insiders"][0]["name"] if c["insiders"] else "",
            }
            for c in clusters
        ],
    }

    result_id = _store_result(
        "scan_insiders",
        {"days": args.days, "min_insiders": args.min_insiders, "min_value": args.min_value},
        clusters,
        json.dumps(summary, default=str),
    )
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_scan_insiders_evaluate(args):
    """Scan EDGAR for insider clusters AND run GO/NO-GO evaluator on each."""
    from tools.edgar_insider_scanner_v2 import scan_insider_clusters
    from tools.insider_cluster_evaluator import evaluate_cluster

    clusters = scan_insider_clusters(
        days=args.days,
        min_insiders=args.min_insiders,
        min_value_per_insider=args.min_value,
        quiet=True,
    )

    evaluated = []
    for c in clusters:
        ticker = c.get("ticker", "")
        # Clean exchange-prefixed tickers (e.g., "NASDAQ:SVC" -> "SVC")
        if ":" in ticker:
            ticker = ticker.split(":")[-1]
        if not ticker or ticker in ("N/A", "NONE"):
            evaluated.append({
                "ticker": ticker,
                "issuer_name": c["issuer_name"][:40],
                "decision": "SKIP",
                "reason": "No ticker / not a stock",
            })
            continue

        # Determine if CEO/CFO present
        has_ceo = False
        has_cfo = False
        for ins in c.get("insiders", []):
            title = ins.get("title", "").upper()
            if "CEO" in title or "CHIEF EXECUTIVE" in title:
                has_ceo = True
            if "CFO" in title or "CHIEF FINANCIAL" in title:
                has_cfo = True

        try:
            result = evaluate_cluster(
                ticker=ticker,
                n_insiders=c["n_insiders"],
                total_value_usd=c["total_value"],
                has_ceo=has_ceo,
                has_cfo=has_cfo,
                days_since_latest_filing=c.get("days_since_latest_filing"),
                max_trans_to_filing_lag=c.get("max_trans_to_filing_lag"),
                acceptance_time=c.get("latest_accept_time"),
            )
            evaluated.append({
                "ticker": ticker,
                "issuer_name": c["issuer_name"][:40],
                "n_insiders": c["n_insiders"],
                "total_value": c["total_value"],
                "decision": result["decision"],
                "score": result["score"],
                "has_ceo": has_ceo,
                "has_cfo": has_cfo,
                "latest_filing_date": c.get("latest_filing_date"),
                "days_since_latest_filing": c.get("days_since_latest_filing"),
                "max_trans_to_filing_lag": c.get("max_trans_to_filing_lag"),
                "trigger_class": result.get("trigger_class", "unknown"),
                "blockers": result["blockers"],
                "warnings": result["warnings"],
                "market_cap_m": result["market_data"].get("market_cap_m"),
            })
        except Exception as e:
            evaluated.append({
                "ticker": ticker,
                "issuer_name": c["issuer_name"][:40],
                "decision": "ERROR",
                "reason": str(e)[:100],
            })

    summary = {
        "status": "ok",
        "days": args.days,
        "clusters_found": len(clusters),
        "go_count": sum(1 for e in evaluated if e.get("decision") == "GO"),
        "weak_go_count": sum(1 for e in evaluated if e.get("decision") == "WEAK_GO"),
        "no_go_count": sum(1 for e in evaluated if e.get("decision") in ("NO_GO", "SKIP", "ERROR")),
        "evaluated": evaluated,
    }

    result_id = _store_result(
        "scan_insiders_evaluate",
        {"days": args.days, "min_insiders": args.min_insiders, "min_value": args.min_value},
        {"clusters": clusters, "evaluations": evaluated},
        json.dumps(summary, default=str),
    )
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_get_result(args):
    """Retrieve a stored task result by ID."""
    result = db.get_task_result(args.id)
    if result is None:
        print(json.dumps({"status": "error", "error": f"No result with id {args.id}"}))
        return
    # Print the full result (may be large — orchestrator should pipe through head)
    print(json.dumps(result, indent=2, default=str))


# ---------------------------------------------------------------------------
# Non-event hypothesis commands
# ---------------------------------------------------------------------------

def _causal_summary(result):
    """Extract compact summary from any causal_tests result."""
    if "error" in result:
        return {"status": "error", "error": result["error"]}
    summary = {
        "status": "ok",
        "test_name": result.get("test_name"),
        "hypothesis_class": result.get("hypothesis_class"),
        "effect_size": result.get("effect_size"),
        "p_value": result.get("p_value"),
        "significant": result.get("significant"),
        "r_squared": result.get("r_squared"),
        "n_observations": result.get("n_observations"),
        "oos_significant": result.get("oos_result", {}).get("significant") if result.get("oos_result") else None,
        "summary": result.get("summary"),
    }
    # Propagate scan-artifact suppression flags to the summary so orchestrator
    # can see them without re-reading full result.
    if result.get("scan_artifact_check") is not None:
        summary["scan_artifact_check"] = result["scan_artifact_check"]
        summary["scan_artifact_suppressed"] = result.get("scan_artifact_suppressed", False)
        summary["scan_artifact_reason"] = result.get("scan_artifact_reason")
    return summary


# ---------------------------------------------------------------------------
# Scan-artifact auto-suppression
# (dgs10_structural_break_scan_artifact_rule_2026_04_19,
#  dgs10_granger_lead_lag_systematic_dead_end,
#  dgs30_granger_lead_lag_extends_systematic_dead_end_2026_04_19)
# ---------------------------------------------------------------------------

# Rate-sensitive ETFs where DGS10/DGS30 breaks/lead-lag are known secular drift.
_RATE_SENSITIVE_ETFS = {
    "XLRE", "IYR", "VNQ",                       # REITs
    "HYG", "JNK", "LQD", "TLT", "IEF",          # bonds
    "GLD", "GDX", "GDXJ", "SLV",                # precious metals
    "XLE", "XOP",                               # energy
    "XLU",                                      # utilities
    "XLI", "IWM",                               # industrials / smallcap
    "EEM", "VWO", "FXI",                        # EM
    "XLV", "IBB", "XBI",                        # healthcare
    "KRE", "KBE", "XLF",                        # banks / financials
    "XLB", "XME",                               # materials
    "XHB", "ITB",                               # homebuilders
    "XLK", "SMH",                               # tech
}

_DGS_FACTORS = {"FRED:DGS10", "FRED:DGS30", "FRED:DGS2", "FRED:DGS5"}

# Canonical 2020-2024 rate-normalization break date where most scan hits cluster.
_DGS_CANONICAL_BREAK = "2022-03-16"


def _is_dgs_rate_sensitive_pair(factor, target):
    """Return True if this is a DGS rate -> rate-sensitive ETF pair where
    scan hits are known systematic artifacts."""
    return factor in _DGS_FACTORS and target.upper() in _RATE_SENSITIVE_ETFS


def _check_dgs_structural_break_artifact(target_rets, factor_rets, target, factor,
                                          break_date, target_f_stat):
    """Auto-run alt-date falsification for DGS -> rate-sensitive ETF structural breaks.
    Per dgs10_structural_break_scan_artifact_rule_2026_04_19: the target-date F-statistic
    must be >= 3x the max of alt-date F-stats for the break to be a genuine regime
    change (rather than secular drift detected by Chow at any date).
    Returns a dict of diagnostic info, or None if the check doesn't apply.
    """
    if not _is_dgs_rate_sensitive_pair(factor, target):
        return None

    # Only apply when break date is near the canonical 2022 break window.
    import pandas as pd
    try:
        bd = pd.Timestamp(break_date)
        canonical = pd.Timestamp(_DGS_CANONICAL_BREAK)
        if abs((bd - canonical).days) > 120:
            return None
    except Exception:
        return None

    import causal_tests
    alt_dates = ["2020-06-15", "2024-01-15"]
    alt_results = {}
    for alt in alt_dates:
        try:
            r = causal_tests.test_structural_break(target_rets, factor_rets, alt)
            if "error" not in r:
                alt_results[alt] = {"f_stat": float(r.get("statistic", 0.0)),
                                    "p_value": float(r.get("p_value", 1.0))}
        except Exception as e:
            alt_results[alt] = {"error": str(e)}

    valid_alt_fs = [v["f_stat"] for v in alt_results.values() if "f_stat" in v]
    if not valid_alt_fs:
        return {"check": "dgs_structural_break_alt_date_falsification",
                "alt_dates": alt_dates, "alt_results": alt_results,
                "target_f_stat": target_f_stat,
                "error": "no alt-date results succeeded"}

    max_alt_f = max(valid_alt_fs)
    ratio = target_f_stat / max_alt_f if max_alt_f > 0 else float("inf")
    suppressed = ratio < 3.0  # canonical 3x threshold per rule

    return {
        "check": "dgs_structural_break_alt_date_falsification",
        "rule": "dgs10_structural_break_scan_artifact_rule_2026_04_19",
        "target_f_stat": float(target_f_stat),
        "alt_dates": alt_dates,
        "alt_results": alt_results,
        "max_alt_f": float(max_alt_f),
        "ratio_target_over_max_alt": float(ratio),
        "threshold_ratio": 3.0,
        "suppressed": bool(suppressed),
        "reason": (
            f"Target F={target_f_stat:.2f} vs max alt-date F={max_alt_f:.2f} "
            f"(ratio={ratio:.2f}x). "
            + ("BELOW 3x threshold -> likely secular drift, not discrete break."
               if suppressed else "ABOVE 3x threshold -> meaningful break at target date.")
        ),
    }


def _check_dgs_leadlag_artifact(factor, target):
    """Flag DGS -> rate-sensitive ETF lead-lag tests as known-systematic artifacts.
    Per dgs10_granger_lead_lag_systematic_dead_end + dgs30 extension.
    Returns a warning dict, or None if the check doesn't apply.
    """
    if not _is_dgs_rate_sensitive_pair(factor, target):
        return None
    return {
        "check": "dgs_leadlag_systematic_artifact_warning",
        "rule": "dgs10_granger_lead_lag_systematic_dead_end "
                "+ dgs30_granger_lead_lag_extends_systematic_dead_end_2026_04_19",
        "suppressed": True,
        "reason": (
            f"DGS rate -> {target.upper()} lead-lag is a documented systematic artifact. "
            "Full-window Granger significance reflects secular drift 2020-2024, not true "
            "lead-lag. Regime-restricted retest (start >= 2022-04-01, oos >= 2024-01-01) "
            "has repeatedly shown lag wandering + OOS non-significance. DO NOT queue as "
            "scan hit without explicit regime-restricted IS/OOS validation."
        ),
    }


def cmd_regression(args):
    """Run exposure, lead-lag, or structural break regression test."""
    from tools.timeseries import get_returns, get_aligned_returns
    import causal_tests

    identifiers = [args.target, args.factor]
    if args.controls:
        identifiers.extend(args.controls.split(","))

    try:
        rets = get_aligned_returns(identifiers, args.start, args.end)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        return

    target_rets = rets[args.target]
    factor_rets = rets[args.factor]
    control_rets = None
    if args.controls:
        control_cols = args.controls.split(",")
        control_rets = rets[control_cols]

    test_type = args.test_type or "exposure"
    params = {"target": args.target, "factor": args.factor, "test_type": test_type,
              "controls": args.controls, "start": args.start, "end": args.end, "oos_start": args.oos_start}

    if test_type == "exposure":
        result = causal_tests.test_exposure(target_rets, factor_rets, control_rets, oos_start=args.oos_start)
    elif test_type == "lead_lag":
        max_lags = args.max_lags or 10
        result = causal_tests.test_lead_lag(factor_rets, target_rets, max_lags=max_lags, oos_start=args.oos_start)
        params["max_lags"] = max_lags
        # Auto-suppression: DGS10/DGS30 -> rate-sensitive ETF lead-lag is a documented
        # systematic artifact (secular drift 2020-2024, not true lead-lag).
        artifact = _check_dgs_leadlag_artifact(args.factor, args.target)
        if artifact is not None:
            result["scan_artifact_check"] = artifact
            result["scan_artifact_suppressed"] = artifact.get("suppressed", False)
            result["scan_artifact_reason"] = artifact.get("reason")
            # Tag the summary so the orchestrator sees the warning inline.
            if artifact.get("suppressed"):
                result["summary"] = (result.get("summary", "") or "") + \
                    " | SCAN_ARTIFACT_SUPPRESSED: " + artifact["reason"]
    elif test_type == "structural_break":
        if not args.break_date:
            print(json.dumps({"status": "error", "error": "--break-date required for structural_break test"}))
            return
        result = causal_tests.test_structural_break(target_rets, factor_rets, args.break_date)
        params["break_date"] = args.break_date
        # Auto-suppression: DGS10/DGS30 -> rate-sensitive ETF structural breaks near
        # 2022-03-16 are recurring scan artifacts. Run alt-date falsification and
        # require target-F >= 3x max(alt-F) or flag as likely secular drift.
        if "error" not in result:
            target_f = result.get("statistic", 0.0)
            artifact = _check_dgs_structural_break_artifact(
                target_rets, factor_rets, args.target, args.factor,
                args.break_date, target_f,
            )
            if artifact is not None:
                result["scan_artifact_check"] = artifact
                result["scan_artifact_suppressed"] = artifact.get("suppressed", False)
                result["scan_artifact_reason"] = artifact.get("reason")
                if artifact.get("suppressed"):
                    result["summary"] = (result.get("summary", "") or "") + \
                        " | SCAN_ARTIFACT_SUPPRESSED: " + artifact["reason"]
    elif test_type == "regime":
        # Regime test: factor is used as regime indicator (LEVEL, not return).
        # Rule (2026-04-11 methodology update):
        #   - Fit tercile thresholds on IS factor LEVELS only (not full sample).
        #   - Apply same thresholds to OOS, compute OOS regime coverage.
        #   - Refuse sign-preservation validation if any OOS regime has <15% obs.
        from tools.timeseries import get_series
        import pandas as pd
        import numpy as np

        indicator = get_series(args.factor, args.start, args.end)
        aligned_full = pd.DataFrame({"returns": target_rets, "factor_level": indicator}).dropna()

        # Auto-pick an OOS cutoff if user didn't supply one (default: last 30% of sample)
        oos_start = args.oos_start
        if not oos_start:
            split_idx = int(len(aligned_full) * 0.70)
            oos_start = str(aligned_full.index[split_idx].date())
            params["oos_start_auto"] = oos_start

        is_df = aligned_full[aligned_full.index < oos_start].copy()
        oos_df = aligned_full[aligned_full.index >= oos_start].copy()

        if len(is_df) < 60 or len(oos_df) < 30:
            result = {
                "test_name": "regime_comparison",
                "hypothesis_class": "regime",
                "error": f"Insufficient data IS={len(is_df)} OOS={len(oos_df)}",
                "summary": f"Regime test aborted: insufficient data IS={len(is_df)} OOS={len(oos_df)}.",
            }
        else:
            # Fit terciles on IS factor levels only
            q33, q66 = is_df["factor_level"].quantile([1 / 3, 2 / 3]).values

            def _label(x):
                if pd.isna(x):
                    return None
                if x <= q33:
                    return "low"
                if x <= q66:
                    return "mid"
                return "high"

            is_df["regime"] = is_df["factor_level"].apply(_label)
            oos_df["regime"] = oos_df["factor_level"].apply(_label)

            # Run IS regime comparison
            result = causal_tests.test_regime(is_df["returns"], is_df["regime"])

            # Compute OOS stats + coverage
            oos_counts = oos_df["regime"].value_counts().to_dict()
            n_oos = int(len(oos_df))
            oos_coverage = {
                r: {"n": int(oos_counts.get(r, 0)),
                    "pct": float(oos_counts.get(r, 0) / n_oos) if n_oos else 0.0}
                for r in ("low", "mid", "high")
            }
            min_coverage_pct = min(v["pct"] for v in oos_coverage.values())
            coverage_ok = min_coverage_pct >= 0.15

            # Per-regime OOS mean/std/n
            oos_stats = {}
            for r in ("low", "mid", "high"):
                g = oos_df[oos_df["regime"] == r]["returns"]
                if len(g) >= 5:
                    oos_stats[r] = {
                        "mean": float(g.mean()),
                        "std": float(g.std()),
                        "n": int(len(g)),
                    }

            # Sign preservation: IS high-low spread sign vs OOS high-low spread sign
            is_regime_stats = result.get("details", {}).get("regime_stats", {})
            is_spread = None
            if "high" in is_regime_stats and "low" in is_regime_stats:
                is_spread = is_regime_stats["high"]["mean"] - is_regime_stats["low"]["mean"]
            oos_spread = None
            if "high" in oos_stats and "low" in oos_stats:
                oos_spread = oos_stats["high"]["mean"] - oos_stats["low"]["mean"]

            spread_sign_match = (
                is_spread is not None and oos_spread is not None
                and np.sign(is_spread) == np.sign(oos_spread)
                and abs(oos_spread) > 1e-6
            )

            # REFUSE sign-preservation validation if coverage fails
            oos_validated = bool(spread_sign_match and coverage_ok)
            validation_refused_reason = None
            if spread_sign_match and not coverage_ok:
                validation_refused_reason = (
                    f"OOS regime coverage imbalance: min regime pct={min_coverage_pct:.1%} "
                    f"(<15% threshold). Refusing sign-preservation validation."
                )

            result["oos_result"] = {
                "oos_start": oos_start,
                "n_oos": n_oos,
                "coverage": oos_coverage,
                "min_coverage_pct": float(min_coverage_pct),
                "coverage_ok": bool(coverage_ok),
                "oos_stats": oos_stats,
                "is_high_low_spread": float(is_spread) if is_spread is not None else None,
                "oos_high_low_spread": float(oos_spread) if oos_spread is not None else None,
                "spread_sign_match_raw": bool(spread_sign_match),
                "oos_validated": oos_validated,
                "validation_refused_reason": validation_refused_reason,
                "significant": oos_validated,  # For _causal_summary convenience
            }
            # Append coverage note to summary
            coverage_note = (
                f" | OOS coverage: low={oos_coverage['low']['pct']:.0%} "
                f"mid={oos_coverage['mid']['pct']:.0%} high={oos_coverage['high']['pct']:.0%}"
                f" (min={min_coverage_pct:.0%}, ok={coverage_ok})"
            )
            if validation_refused_reason:
                coverage_note += f" | VALIDATION REFUSED: {validation_refused_reason}"
            elif oos_validated:
                coverage_note += " | OOS sign-preserved AND coverage ok -> VALIDATED"
            elif is_spread is not None and oos_spread is not None:
                coverage_note += " | OOS sign-flipped"
            result["summary"] = (result.get("summary", "") or "") + coverage_note
    elif test_type == "network":
        spokes = args.controls.split(",") if args.controls else []
        if not spokes:
            print(json.dumps({"status": "error", "error": "--controls must list spoke symbols for network test"}))
            return
        hub_rets = rets[args.target]
        spoke_rets = rets[spokes]
        result = causal_tests.test_network(hub_rets, spoke_rets, max_lag=args.max_lags or 5)
    else:
        print(json.dumps({"status": "error", "error": f"Unknown test_type: {test_type}"}))
        return

    summary = _causal_summary(result)
    result_id = _store_result(f"regression_{test_type}", params, result, json.dumps(summary, default=str))
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_cointegration(args):
    """Run Engle-Granger cointegration test."""
    from tools.timeseries import get_series
    import causal_tests

    try:
        series_a = get_series(args.series_a, args.start, args.end)
        series_b = get_series(args.series_b, args.start, args.end)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        return

    result = causal_tests.test_cointegration(series_a, series_b, oos_start=args.oos_start)
    params = {"series_a": args.series_a, "series_b": args.series_b,
              "start": args.start, "end": args.end, "oos_start": args.oos_start}

    summary = _causal_summary(result)
    result_id = _store_result("cointegration", params, result, json.dumps(summary, default=str))
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_threshold(args):
    """Run threshold-triggered event study.

    When the raw test is significant (p<0.05), automatically runs a canonical
    retest using first-close cluster-buffered event counting + SPY-adjusted
    abnormal returns on two samples (pooled + post-2020 recency). This guards
    against raw-crossing inflation and regime-specific signals.

    See threshold_scan_hit_canonical_retest_rule_2026_04_18.
    """
    from tools.timeseries import get_series, get_returns
    import causal_tests

    try:
        trigger = get_series(args.trigger, args.start, args.end)
        target_rets = get_returns(args.target, args.start, args.end)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        return

    horizons = [int(h) for h in args.horizons.split(",")] if args.horizons else [5, 10, 20]
    result = causal_tests.test_threshold(trigger, target_rets, args.threshold_value,
                                          direction=args.direction, horizons=horizons)
    params = {"trigger": args.trigger, "target": args.target, "threshold": args.threshold_value,
              "direction": args.direction, "horizons": horizons, "start": args.start, "end": args.end}

    summary = _causal_summary(result)

    # Auto-run canonical retest when raw test is significant (unless explicitly skipped)
    skip_canonical = getattr(args, "skip_canonical", False)
    raw_sig = result.get("p_value") is not None and result.get("p_value") < 0.05
    if raw_sig and not skip_canonical:
        canonical_horizons = sorted(set([1, 3] + horizons))
        canonical = causal_tests.canonical_retest_threshold(
            trigger_identifier=args.trigger,
            target_symbol=args.target,
            threshold=args.threshold_value,
            direction=args.direction,
            horizons=canonical_horizons,
            start=args.start or "2010-01-01",
            end=args.end,
        )
        # Keep the canonical result inside `result` for storage, and expose a compact
        # summary on the top-level summary so scanners can gate on `canonical_passes`.
        result["canonical_retest"] = canonical
        if "error" in canonical:
            summary["canonical_passes"] = False
            summary["canonical_error"] = canonical["error"]
        else:
            summary["canonical_passes"] = canonical.get("passes", False)
            summary["canonical_fail_reason"] = canonical.get("fail_reason")
            summary["canonical_n_pooled"] = canonical.get("n_events_pooled")
            summary["canonical_n_recent"] = canonical.get("n_events_recent")
            pooled = canonical.get("pooled", {})
            recent = canonical.get("recent", {})
            if pooled.get("best_horizon"):
                bh = pooled["best_horizon"]
                hs = pooled["horizons"][bh]
                summary["canonical_pooled_best"] = {
                    "horizon": bh,
                    "mean": hs["abnormal_mean"],
                    "p_value": hs["p_value"],
                    "positive_rate": hs["positive_rate"],
                }
            if recent.get("best_horizon"):
                bh = recent["best_horizon"]
                hs = recent["horizons"][bh]
                summary["canonical_recent_best"] = {
                    "horizon": bh,
                    "mean": hs["abnormal_mean"],
                    "p_value": hs["p_value"],
                    "positive_rate": hs["positive_rate"],
                }
            summary["canonical_summary"] = canonical.get("summary")
    elif not raw_sig:
        summary["canonical_passes"] = False
        summary["canonical_skipped"] = "raw_not_significant"

    result_id = _store_result("threshold", params, result, json.dumps(summary, default=str))
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_calendar(args):
    """Run calendar anomaly test."""
    from tools.timeseries import get_returns
    import causal_tests

    try:
        rets = get_returns(args.symbol, args.start, args.end)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        return

    pattern_spec = None
    if args.pattern_month:
        pattern_spec = {"month": int(args.pattern_month)}

    result = causal_tests.test_calendar(rets, args.pattern, pattern_spec=pattern_spec,
                                         oos_start_year=args.oos_start_year)
    params = {"symbol": args.symbol, "pattern": args.pattern, "pattern_spec": pattern_spec,
              "oos_start_year": args.oos_start_year, "start": args.start, "end": args.end}

    summary = _causal_summary(result)
    result_id = _store_result("calendar", params, result, json.dumps(summary, default=str))
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_fetch_series(args):
    """Fetch and display time series data."""
    from tools.timeseries import get_aligned_series

    identifiers = [s.strip() for s in args.identifiers.split(",")]
    try:
        df = get_aligned_series(identifiers, args.start, args.end)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}))
        return

    summary = {
        "status": "ok",
        "identifiers": identifiers,
        "start": str(df.index[0].date()),
        "end": str(df.index[-1].date()),
        "n_days": len(df),
        "stats": {},
    }
    for col in df.columns:
        s = df[col]
        summary["stats"][col] = {
            "mean": round(float(s.mean()), 4),
            "std": round(float(s.std()), 4),
            "min": round(float(s.min()), 4),
            "max": round(float(s.max()), 4),
            "last": round(float(s.iloc[-1]), 4),
        }

    # df.to_dict() produces Timestamp keys which json.dumps rejects. Convert index to strings first.
    df_serializable = df.copy()
    df_serializable.index = df_serializable.index.astype(str)
    result_id = _store_result("fetch_series", {"identifiers": identifiers, "start": args.start, "end": args.end},
                               df_serializable.to_dict(), json.dumps(summary, default=str))
    summary["result_id"] = result_id
    print(json.dumps(summary, indent=2, default=str))


def cmd_oos(args):
    """OOS observation tracker dispatcher."""
    import oos_tracker

    if args.oos_command == "register":
        result = oos_tracker.register_observation(
            signal_type=args.signal_type,
            symbol=args.symbol,
            entry_date=args.entry_date,
            hold_days=args.hold_days,
            direction=args.direction,
            threshold=args.threshold,
            benchmark=args.benchmark,
            hypothesis_id=args.hypothesis_id,
            notes=args.notes,
        )
    elif args.oos_command == "update":
        result = oos_tracker.update_all_active()
    elif args.oos_command == "status":
        result = oos_tracker.get_status_summary(
            signal_type=args.signal_type,
            include_completed=args.show_all,
        )
    elif args.oos_command == "close":
        result = oos_tracker.close_observation(args.id, args.result)
    else:
        result = {"status": "error", "error": f"Unknown oos command: {args.oos_command}"}

    print(json.dumps(result, indent=2, default=str))


def main():
    db.init_db()

    parser = argparse.ArgumentParser(description="Data task dispatcher")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # backtest
    bt = subparsers.add_parser("backtest", help="Run measure_event_impact")
    bt.add_argument("--events", help="JSON list of {symbol, date, timing} dicts")
    bt.add_argument("--symbol", help="Single symbol (use with --dates)")
    bt.add_argument("--dates", help="JSON list of date strings (use with --symbol)")
    bt.add_argument("--benchmark", default="SPY")
    bt.add_argument("--sector-etf")
    bt.add_argument("--entry-price", choices=["close", "open"])
    bt.add_argument("--event-timing", choices=["pre_market", "intraday", "after_hours", "unknown"])
    bt.add_argument("--event-type", help="Event type for cost estimation")
    bt.add_argument("--estimate-costs", action="store_true")
    bt.add_argument("--regime-filter")

    # verify-date
    vd = subparsers.add_parser("verify-date", help="Verify an event date")
    vd.add_argument("--event", required=True)
    vd.add_argument("--expected-date", required=True)

    # largecap-filter
    lf = subparsers.add_parser("largecap-filter", help="Filter to large-cap symbols")
    lf.add_argument("--symbols", required=True, help="JSON list of symbols")

    # price-history
    ph = subparsers.add_parser("price-history", help="Fetch price history")
    ph.add_argument("--symbol", required=True)
    ph.add_argument("--days", type=int, default=90)

    # get-result
    gr = subparsers.add_parser("get-result", help="Get stored result by ID")
    gr.add_argument("--id", required=True)

    # scan-insiders
    si = subparsers.add_parser("scan-insiders", help="Scan EDGAR for insider buying clusters")
    si.add_argument("--days", type=int, default=14, help="Days to look back")
    si.add_argument("--min-insiders", type=int, default=3)
    si.add_argument("--min-value", type=int, default=50000)

    # scan-insiders-evaluate
    sie = subparsers.add_parser("scan-insiders-evaluate", help="Scan + GO/NO-GO evaluate clusters")
    sie.add_argument("--days", type=int, default=14, help="Days to look back")
    sie.add_argument("--min-insiders", type=int, default=3)
    sie.add_argument("--min-value", type=int, default=50000)

    # --- Non-event hypothesis commands ---

    # regression (exposure / lead_lag / structural_break / regime / network)
    reg = subparsers.add_parser("regression", help="Run regression-based causal test")
    reg.add_argument("--target", required=True, help="Target symbol or series ID")
    reg.add_argument("--factor", required=True, help="Factor/driver symbol or series ID")
    reg.add_argument("--controls", help="Comma-separated control symbols (e.g., SPY,XLE)")
    reg.add_argument("--test-type", default="exposure",
                     choices=["exposure", "lead_lag", "structural_break", "regime", "network"],
                     help="Type of regression test")
    reg.add_argument("--start", default="2020-01-01", help="Start date")
    reg.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"), help="End date")
    reg.add_argument("--oos-start", help="OOS validation start date (default: auto 70/30)")
    reg.add_argument("--max-lags", type=int, help="Max lags for lead_lag/network (default 10)")
    reg.add_argument("--break-date", help="Break date for structural_break test")

    # cointegration
    coint = subparsers.add_parser("cointegration", help="Run Engle-Granger cointegration test")
    coint.add_argument("--series-a", required=True, help="First series identifier")
    coint.add_argument("--series-b", required=True, help="Second series identifier")
    coint.add_argument("--start", default="2020-01-01")
    coint.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"))
    coint.add_argument("--oos-start", help="OOS validation start date")

    # threshold
    thr = subparsers.add_parser("threshold", help="Threshold-triggered event study")
    thr.add_argument("--trigger", required=True, help="Trigger series (e.g., ^VIX)")
    thr.add_argument("--target", required=True, help="Target series for measuring returns")
    thr.add_argument("--threshold-value", required=True, type=float)
    thr.add_argument("--direction", default="above", choices=["above", "below"])
    thr.add_argument("--horizons", default="5,10,20", help="Comma-separated horizons in days")
    thr.add_argument("--start", default="2015-01-01")
    thr.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"))
    thr.add_argument("--skip-canonical", action="store_true",
                     help="Skip auto-canonical-retest (for debugging / raw-only runs)")

    # calendar
    cal = subparsers.add_parser("calendar", help="Calendar anomaly test")
    cal.add_argument("--symbol", required=True)
    cal.add_argument("--pattern", required=True, choices=["monthly", "dow", "tom"])
    cal.add_argument("--pattern-month", type=int, help="Specific month to test (1-12)")
    cal.add_argument("--oos-start-year", type=int, help="Year to start OOS validation")
    cal.add_argument("--start", default="2005-01-01")
    cal.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"))

    # fetch-series
    fs = subparsers.add_parser("fetch-series", help="Fetch and display time series data")
    fs.add_argument("--identifiers", required=True, help="Comma-separated series identifiers")
    fs.add_argument("--start", default="2020-01-01")
    fs.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"))

    # oos — OOS observation tracker
    oos = subparsers.add_parser("oos", help="OOS observation tracker")
    oos_sub = oos.add_subparsers(dest="oos_command", required=True)

    oos_reg = oos_sub.add_parser("register", help="Register new OOS observation")
    oos_reg.add_argument("--signal-type", required=True)
    oos_reg.add_argument("--symbol", required=True)
    oos_reg.add_argument("--entry-date", required=True, help="ISO date, e.g. 2026-04-15")
    oos_reg.add_argument("--hold-days", type=int, default=5)
    oos_reg.add_argument("--direction", required=True, choices=["long", "short"])
    oos_reg.add_argument("--threshold", type=float, default=None, help="Success threshold (e.g., -2.5 for short)")
    oos_reg.add_argument("--benchmark", default="SPY")
    oos_reg.add_argument("--hypothesis-id", default=None)
    oos_reg.add_argument("--notes", default=None)

    oos_sub.add_parser("update", help="Update all active OOS observations with latest prices")

    oos_st = oos_sub.add_parser("status", help="Show OOS observation status")
    oos_st.add_argument("--signal-type", default=None)
    oos_st.add_argument("--all", action="store_true", dest="show_all", help="Include completed")

    oos_cl = oos_sub.add_parser("close", help="Close an OOS observation")
    oos_cl.add_argument("--id", required=True)
    oos_cl.add_argument("--result", required=True, choices=["validated", "failed"])

    args = parser.parse_args()

    commands = {
        "backtest": cmd_backtest,
        "verify-date": cmd_verify_date,
        "largecap-filter": cmd_largecap_filter,
        "price-history": cmd_price_history,
        "get-result": cmd_get_result,
        "scan-insiders": cmd_scan_insiders,
        "scan-insiders-evaluate": cmd_scan_insiders_evaluate,
        "regression": cmd_regression,
        "cointegration": cmd_cointegration,
        "threshold": cmd_threshold,
        "calendar": cmd_calendar,
        "fetch-series": cmd_fetch_series,
        "oos": cmd_oos,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
