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

    args = parser.parse_args()

    commands = {
        "backtest": cmd_backtest,
        "verify-date": cmd_verify_date,
        "largecap-filter": cmd_largecap_filter,
        "price-history": cmd_price_history,
        "get-result": cmd_get_result,
        "scan-insiders": cmd_scan_insiders,
        "scan-insiders-evaluate": cmd_scan_insiders_evaluate,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
