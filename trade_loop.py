"""
Deterministic trading loop — runs independently of the LLM research agent.

The LLM creates hypotheses and sets triggers. This loop checks triggers
and executes trades without waiting for an LLM session.

Trigger types:
  "immediate"              — execute now (market hours only)
  "next_market_open"       — execute at next market open (9:30 ET)
  "after_hours_immediate"  — execute now as an extended-hours limit order
                             (works 4:00 PM – 8:00 PM ET on weekdays)
  "2026-06-07T09:30"       — execute at specific datetime (market hours)
  None                     — no trigger, LLM will activate manually

Also handles:
  - Stop-loss / take-profit / deadline enforcement
  - Position reconciliation (Alpaca vs hypotheses)
  - Trade result logging

Runs every 2 minutes via launchd. No LLM needed.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).parent
HYPOTHESES_PATH = BASE_DIR / "hypotheses.json"
TRADE_LOG_PATH = BASE_DIR / "logs" / "trade_log.jsonl"

ET = ZoneInfo("America/New_York")

# Add project to path
sys.path.insert(0, str(BASE_DIR))


def _load_hypotheses():
    try:
        with open(HYPOTHESES_PATH) as f:
            return json.load(f)
    except Exception:
        return []


def _save_hypotheses(hypotheses):
    tmp = str(HYPOTHESES_PATH) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(hypotheses, f, indent=2)
    os.replace(tmp, str(HYPOTHESES_PATH))


def _log_trade_action(action):
    """Append a trade action to the trade log."""
    action["timestamp"] = datetime.now().isoformat()
    os.makedirs(BASE_DIR / "logs", exist_ok=True)
    with open(TRADE_LOG_PATH, "a") as f:
        f.write(json.dumps(action) + "\n")


def _market_is_open():
    """Check if US stock market is currently open (9:30-16:00 ET, weekdays)."""
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:  # Saturday/Sunday
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close


def _is_near_open():
    """Check if we're within 5 minutes of market open."""
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:
        return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    diff = (now_et - market_open).total_seconds()
    return 0 <= diff <= 300  # within 5 minutes after open


def _extended_hours_is_available():
    """
    Check if the after-hours session is currently active.

    Alpaca supports extended-hours limit orders during:
      - Pre-market:  4:00 AM – 9:30 AM ET
      - After-hours: 4:00 PM – 8:00 PM ET
    on weekdays only.

    For CEO departure shorts we target the after-hours window only
    (news typically drops after the close), so we check 4:00–8:00 PM ET.
    """
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:  # Saturday/Sunday
        return False
    after_hours_start = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    after_hours_end = now_et.replace(hour=20, minute=0, second=0, microsecond=0)
    return after_hours_start <= now_et <= after_hours_end


def _trigger_is_ready(trigger):
    """
    Check if a trigger condition is met.

    Returns a tuple (ready: bool, use_extended_hours: bool) so callers can
    decide which order type to use.
    """
    if trigger is None:
        return False, False

    if trigger == "immediate":
        return _market_is_open(), False

    if trigger == "next_market_open":
        return _is_near_open(), False

    if trigger == "after_hours_immediate":
        return _extended_hours_is_available(), True

    # Specific datetime trigger: "2026-06-07T09:30"
    try:
        trigger_dt = datetime.fromisoformat(trigger)
        # If no timezone, assume ET
        if trigger_dt.tzinfo is None:
            trigger_dt = trigger_dt.replace(tzinfo=ET)
        now = datetime.now(ET)
        # Trigger if we're past the time and market is open
        return now >= trigger_dt and _market_is_open(), False
    except (ValueError, TypeError):
        return False, False


def _send_trade_email(subject, actions):
    """Send email notification for trade actions."""
    try:
        from email_report import send_email
        rows = ""
        for a in actions:
            color = "#2e7d32" if a.get("success") else "#c62828"
            rows += f"""
            <tr>
                <td style="padding: 6px;">{a.get('symbol', '?')}</td>
                <td style="padding: 6px;">{a.get('action', '?')}</td>
                <td style="padding: 6px; color: {color};">{a.get('detail', '')}</td>
            </tr>"""

        html = f"""
        <html><body style="font-family: -apple-system, Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2>Trade Execution</h2>
        <p style="color: #888;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <table style="border-collapse: collapse; width: 100%;">
            <tr style="background: #f0f0f0;">
                <th style="padding: 6px; text-align: left;">Symbol</th>
                <th style="padding: 6px; text-align: left;">Action</th>
                <th style="padding: 6px; text-align: left;">Detail</th>
            </tr>
            {rows}
        </table>
        <hr>
        <p style="color: #aaa; font-size: 11px;">Sent by trade_loop.py</p>
        </body></html>
        """
        send_email(subject, html)
    except Exception as e:
        print(f"Email failed: {e}", file=sys.stderr)


def execute_pending_triggers():
    """Check all pending hypotheses for ready triggers and execute trades."""
    from trader import (
        place_experiment, check_portfolio_drawdown, get_current_price,
        DEFAULT_STOP_LOSS_PCT, DEFAULT_TAKE_PROFIT_PCT,
    )

    hypotheses = _load_hypotheses()
    actions = []
    modified = False

    for h in hypotheses:
        if h.get("status") != "pending":
            continue

        trigger = h.get("trigger")
        if not trigger:
            continue

        trigger_ready, use_extended_hours = _trigger_is_ready(trigger)
        if not trigger_ready:
            continue

        symbol = h.get("expected_symbol")
        if not symbol or symbol == "TBD":
            actions.append({
                "action": "skip",
                "symbol": "TBD",
                "hypothesis_id": h["id"],
                "detail": "Symbol still TBD — cannot execute",
                "success": False,
            })
            continue

        direction = h.get("expected_direction", "long")
        position_size = h.get("trigger_position_size", 5000)

        # Portfolio drawdown check
        dd = check_portfolio_drawdown()
        if not dd.get("safe_to_trade"):
            actions.append({
                "action": "blocked",
                "symbol": symbol,
                "hypothesis_id": h["id"],
                "detail": f"Portfolio drawdown {dd.get('drawdown_pct', '?')}% exceeds limit",
                "success": False,
            })
            continue

        # Place the trade (extended_hours=True for after_hours_immediate trigger)
        if use_extended_hours:
            print(f"[TRADE LOOP] after_hours_immediate trigger for {symbol} — using extended hours limit order")
        result = place_experiment(symbol, direction, position_size, extended_hours=use_extended_hours)

        if result.get("success"):
            # Get current SPY price for context
            spy_price = get_current_price("SPY")

            # Activate the hypothesis
            h["status"] = "active"
            h["trade"] = {
                "entry_price": result["price_at_order"],
                "position_size": position_size,
                "entry_time": datetime.now().isoformat(),
                "order_id": result.get("order_id"),
                "deadline": (datetime.now() + timedelta(days=h.get("expected_timeframe_days", 5))).isoformat(),
                "stop_loss_pct": h.get("trigger_stop_loss_pct", DEFAULT_STOP_LOSS_PCT),
                "take_profit_pct": h.get("trigger_take_profit_pct", DEFAULT_TAKE_PROFIT_PCT),
                "spy_at_entry": spy_price,
                "vix_at_entry": None,
                "sector_etf_at_entry": None,
                "activated_by": "trade_loop",
                "extended_hours": use_extended_hours,
                "limit_price": result.get("limit_price"),
            }
            h["trigger"] = None  # consumed
            modified = True

            actions.append({
                "action": "activated",
                "symbol": symbol,
                "hypothesis_id": h["id"],
                "detail": f"{direction.upper()} ${position_size} @ ${result['price_at_order']:.2f}",
                "success": True,
            })
            _log_trade_action({
                "type": "activate",
                "hypothesis_id": h["id"],
                "symbol": symbol,
                "direction": direction,
                "entry_price": result["price_at_order"],
                "position_size": position_size,
                "order_id": result.get("order_id"),
                "trigger": trigger,
            })
        else:
            actions.append({
                "action": "failed",
                "symbol": symbol,
                "hypothesis_id": h["id"],
                "detail": result.get("error", "Unknown error"),
                "success": False,
            })
            _log_trade_action({
                "type": "activate_failed",
                "hypothesis_id": h["id"],
                "symbol": symbol,
                "error": result.get("error"),
            })

    if modified:
        _save_hypotheses(hypotheses)

    return actions


def reconcile_positions():
    """Check that Alpaca positions match hypothesis state. Returns warnings."""
    from trader import get_api

    hypotheses = _load_hypotheses()
    active_symbols = {
        h["expected_symbol"]: h["id"]
        for h in hypotheses if h.get("status") == "active" and h.get("expected_symbol")
    }

    warnings = []
    try:
        api = get_api()
        positions = {p.symbol: p for p in api.list_positions()}
    except Exception as e:
        return [f"Could not connect to Alpaca: {e}"]

    # Hypotheses with no matching position
    for symbol, hyp_id in active_symbols.items():
        if symbol not in positions:
            warnings.append(f"Orphan hypothesis: {hyp_id[:8]} expects {symbol} but no Alpaca position found")

    # Positions with no matching hypothesis
    for symbol in positions:
        if symbol not in active_symbols:
            warnings.append(f"Orphan position: {symbol} in Alpaca but no active hypothesis tracks it")

    return warnings


def run_trading_cycle():
    """One full trading cycle: triggers, stops, reconciliation."""
    from trader import check_stop_losses

    all_actions = []

    # 1. Execute pending triggers
    trigger_actions = execute_pending_triggers()
    all_actions.extend(trigger_actions)

    # 2. Stop-loss / take-profit / deadline enforcement
    stop_actions = check_stop_losses()
    for a in stop_actions:
        if a["action"] in ("closed", "close_failed", "drawdown_alert"):
            all_actions.append({
                "action": a["action"],
                "symbol": a.get("symbol", ""),
                "hypothesis_id": a.get("hypothesis_id", ""),
                "detail": a.get("reason", a.get("message", "")),
                "success": a["action"] == "closed",
            })

    # 3. Position reconciliation
    recon_warnings = reconcile_positions()
    for w in recon_warnings:
        all_actions.append({
            "action": "reconciliation_warning",
            "symbol": "",
            "detail": w,
            "success": False,
        })

    # Email if anything happened
    if all_actions:
        subject_parts = []
        for a in all_actions:
            if a["action"] == "activated":
                subject_parts.append(f"Opened {a['symbol']}")
            elif a["action"] == "closed":
                subject_parts.append(f"Closed {a['symbol']}")
            elif a["action"] == "failed":
                subject_parts.append(f"FAILED {a['symbol']}")
        if subject_parts:
            _send_trade_email(f"Trade: {', '.join(subject_parts[:3])}", all_actions)
        else:
            # Only warnings/recon — don't email unless serious
            serious = [a for a in all_actions if a["action"] in ("close_failed", "drawdown_alert")]
            if serious:
                _send_trade_email("Trade alert", all_actions)

    return all_actions


if __name__ == "__main__":
    # Load .env
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))

    if len(sys.argv) > 1 and sys.argv[1] == "--dry-run":
        # Show what would happen without executing
        hypotheses = _load_hypotheses()
        print(f"Market open: {_market_is_open()}")
        print(f"Near open: {_is_near_open()}")
        print(f"Extended hours available: {_extended_hours_is_available()}")
        print()
        for h in hypotheses:
            trigger = h.get("trigger")
            if trigger:
                ready, ext = _trigger_is_ready(trigger)
                ext_flag = " [ext-hours]" if ext else ""
                print(f"  {h['id'][:8]} | {h.get('expected_symbol','TBD'):6s} | trigger={trigger} | ready={ready}{ext_flag}")
        active = [h for h in hypotheses if h.get("status") == "active"]
        if active:
            print(f"\nActive positions: {len(active)}")
            for h in active:
                print(f"  {h['id'][:8]} | {h['expected_symbol']} | deadline={h.get('trade',{}).get('deadline','?')[:10]}")
        recon = reconcile_positions()
        if recon:
            print(f"\nReconciliation warnings:")
            for w in recon:
                print(f"  {w}")
    else:
        actions = run_trading_cycle()
        if actions:
            for a in actions:
                print(f"[{a['action']}] {a.get('symbol', '')} — {a.get('detail', '')}")
        else:
            print(f"No actions. Market {'open' if _market_is_open() else 'closed'}.")
