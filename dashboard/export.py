#!/usr/bin/env python3
"""Export research data to static JSON for the Frakbox dashboard."""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db
import config

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def _atomic_write(path, data):
    """Write JSON atomically via temp file + rename."""
    fd, tmp = tempfile.mkstemp(dir=DATA_DIR, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def export_fund():
    """Build fund.json: NAV history, performance metrics, risk status."""
    data = {"nav_history": [], "current": {}, "performance": {}, "risk": {}}
    alpaca_ok = False

    # Current account state from Alpaca
    try:
        import trader
        acct = trader.get_account_summary()
        data["current"] = {
            "equity": acct["equity"],
            "cash": acct["cash"],
            "buying_power": acct["buying_power"],
        }
        # Snapshot today's NAV
        today = datetime.now().strftime("%Y-%m-%d")
        db.snapshot_nav(today, acct["equity"], acct["cash"], len(acct["positions"]))
        alpaca_ok = True
    except Exception as e:
        print(f"[export] Alpaca unavailable: {e}", file=sys.stderr)

    # NAV history from snapshots
    data["nav_history"] = db.get_nav_history()

    # Performance from completed hypotheses
    completed = db.get_hypotheses_by_status("completed")
    if completed:
        wins = [h for h in completed if h.get("result", {}).get("direction_correct")]
        losses = [h for h in completed if not h.get("result", {}).get("direction_correct")]
        win_returns = [h["result"]["raw_return_pct"] for h in wins if h.get("result", {}).get("raw_return_pct") is not None]
        loss_returns = [h["result"]["raw_return_pct"] for h in losses if h.get("result", {}).get("raw_return_pct") is not None]

        data["performance"] = {
            "total_trades": len(completed),
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate_pct": round(len(wins) / len(completed) * 100, 1) if completed else 0,
            "avg_win_pct": round(sum(win_returns) / len(win_returns), 2) if win_returns else 0,
            "avg_loss_pct": round(sum(loss_returns) / len(loss_returns), 2) if loss_returns else 0,
            "direction_accuracy": f"{len(wins)}/{len(completed)}",
        }

        # Total return and drawdown from NAV history
        nav = data["nav_history"]
        if len(nav) >= 2:
            first_equity = nav[0]["equity"]
            last_equity = nav[-1]["equity"]
            data["performance"]["total_return_pct"] = round(
                (last_equity - first_equity) / first_equity * 100, 2
            )
            # Max drawdown from NAV series
            peak = nav[0]["equity"]
            max_dd = 0
            for point in nav:
                if point["equity"] > peak:
                    peak = point["equity"]
                dd = (peak - point["equity"]) / peak * 100
                if dd > max_dd:
                    max_dd = dd
            data["performance"]["max_drawdown_pct"] = round(max_dd, 2)

            # Sharpe estimate (annualized) — need 20+ daily points
            if len(nav) >= 20:
                daily_returns = []
                for i in range(1, len(nav)):
                    r = (nav[i]["equity"] - nav[i - 1]["equity"]) / nav[i - 1]["equity"]
                    daily_returns.append(r)
                if daily_returns:
                    import statistics
                    mean_r = statistics.mean(daily_returns)
                    std_r = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 1
                    if std_r > 0:
                        data["performance"]["sharpe_estimate"] = round(
                            mean_r / std_r * (252 ** 0.5), 2
                        )

    # Risk status
    try:
        import trader
        dd = trader.check_portfolio_drawdown()
        data["risk"] = {
            "drawdown_pct": dd.get("drawdown_pct", 0),
            "drawdown_limit_pct": config.MAX_PORTFOLIO_DRAWDOWN_PCT,
            "safe_to_trade": dd.get("safe_to_trade", False),
            "max_position_pct": config.MAX_POSITION_PCT * 100,
            "stop_loss_pct": config.DEFAULT_STOP_LOSS_PCT,
            "max_concurrent": config.MAX_CONCURRENT_EXPERIMENTS,
            "active_positions": len(db.get_hypotheses_by_status("active")),
        }
    except Exception:
        data["risk"] = {
            "drawdown_limit_pct": config.MAX_PORTFOLIO_DRAWDOWN_PCT,
            "max_position_pct": config.MAX_POSITION_PCT * 100,
            "stop_loss_pct": config.DEFAULT_STOP_LOSS_PCT,
            "max_concurrent": config.MAX_CONCURRENT_EXPERIMENTS,
        }

    _atomic_write(os.path.join(DATA_DIR, "fund.json"), data)
    return alpaca_ok


def export_positions():
    """Build positions.json: delayed active positions + recent closed trades."""
    data = {"active": [], "recent_closed": []}

    # Active positions — deliberately limited info
    for h in db.get_hypotheses_by_status("active"):
        data["active"].append({
            "symbol": h.get("expected_symbol", ""),
            "direction": h.get("expected_direction", ""),
            "event_type": h.get("event_type", ""),
            "thesis": (h.get("event_description") or "")[:120],
            "opened_date": (h.get("trade", {}) or {}).get("entry_date", h.get("created", "")[:10]),
        })

    # Recent closed trades — full results (last 10)
    completed = db.get_hypotheses_by_status("completed")
    completed.sort(key=lambda h: h.get("result", {}).get("exit_time", ""), reverse=True)
    for h in completed[:10]:
        result = h.get("result", {}) or {}
        data["recent_closed"].append({
            "symbol": h.get("expected_symbol", ""),
            "direction": h.get("expected_direction", ""),
            "event_type": h.get("event_type", ""),
            "result_pct": result.get("raw_return_pct"),
            "exit_reason": result.get("exit_reason", ""),
            "closed_date": (result.get("exit_time") or "")[:10],
            "thesis": (h.get("event_description") or "")[:120],
            "direction_correct": result.get("direction_correct"),
        })

    _atomic_write(os.path.join(DATA_DIR, "positions.json"), data)


def export_research():
    """Build research.json: hypothesis stats, knowledge, journal, activity."""
    import research

    data = {}

    # Summary stats
    data["summary"] = research.get_research_summary()

    # Knowledge base — names and counts only
    kb = db.load_knowledge()
    signals = []
    for name, effect in kb.get("known_effects", {}).items():
        signals.append({
            "name": name,
            "status": effect.get("status", "unknown"),
            "magnitude_pct": effect.get("avg_magnitude_pct"),
        })
    dead_ends = []
    for de in kb.get("dead_ends", []):
        dead_ends.append({
            "name": de.get("event_type", ""),
            "reason": de.get("reason", ""),
        })
    data["knowledge"] = {
        "signals": signals,
        "dead_ends": dead_ends,
        "signal_count": len(signals),
        "dead_end_count": len(dead_ends),
        "literature_count": len(kb.get("literature", {})),
    }

    # Activity — token usage by day (last 30 days)
    today = datetime.now()
    sessions_by_day = []
    for i in range(30):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        usage = db.get_daily_token_usage(d)
        if usage["sessions"] > 0:
            sessions_by_day.append({
                "date": d,
                "sessions": usage["sessions"],
                "tokens": usage["total_tokens"],
            })
    data["activity"] = {
        "sessions_today": db.get_daily_token_usage()["sessions"],
        "tokens_today": db.get_daily_token_usage()["total_tokens"],
        "sessions_by_day": sessions_by_day,
    }

    # Journal — last 10 entries
    journal = db.get_recent_journal(10)
    data["journal"] = [
        {
            "date": j.get("date", ""),
            "investigated": j.get("investigated", ""),
            "findings": j.get("findings", ""),
        }
        for j in journal
    ]

    _atomic_write(os.path.join(DATA_DIR, "research.json"), data)


def export_meta(alpaca_ok):
    """Build meta.json: export timestamp, health."""
    data = {
        "exported_at": datetime.now().isoformat(),
        "alpaca_connected": alpaca_ok,
        "export_version": 1,
    }
    _atomic_write(os.path.join(DATA_DIR, "meta.json"), data)


def backfill_nav():
    """One-time: backfill NAV history from Alpaca portfolio history API."""
    try:
        import trader
        api = trader.get_api()
        history = api.get_portfolio_history(period="all", timeframe="1D")
        if history and history.equity:
            for ts, equity in zip(history.timestamp, history.equity):
                if equity is None:
                    continue
                date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                # Alpaca doesn't give cash breakdown in history, use 0
                db.snapshot_nav(date, float(equity), 0, 0)
            print(f"[export] Backfilled {len(history.equity)} NAV snapshots")
    except Exception as e:
        print(f"[export] Backfill failed: {e}", file=sys.stderr)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    db.init_db()
    config.load_env()

    alpaca_ok = False
    try:
        alpaca_ok = export_fund()
    except Exception as e:
        print(f"[export] fund.json failed: {e}", file=sys.stderr)

    try:
        export_positions()
    except Exception as e:
        print(f"[export] positions.json failed: {e}", file=sys.stderr)

    try:
        export_research()
    except Exception as e:
        print(f"[export] research.json failed: {e}", file=sys.stderr)

    export_meta(alpaca_ok)
    print(f"[export] Done at {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        db.init_db()
        config.load_env()
        backfill_nav()
    else:
        main()
