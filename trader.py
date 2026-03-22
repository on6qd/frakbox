"""
Trading execution for research experiments.
Places and closes paper trades to test hypotheses.

Risk controls:
- Per-position stop-loss (default 10%)
- Per-position take-profit (optional)
- Portfolio-level max drawdown (default 15% from peak)
- Trade deadline enforcement (auto-close past deadline)
- check_stop_losses() runs independently of the LLM agent
"""

import json
import os
import sys
from datetime import datetime

import alpaca_trade_api as tradeapi
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, MAX_POSITION_PCT, require_alpaca

# Default risk limits
DEFAULT_STOP_LOSS_PCT = 10.0        # close if position loses more than 10%
DEFAULT_TAKE_PROFIT_PCT = None      # no take-profit by default (let winners run to deadline)
MAX_PORTFOLIO_DRAWDOWN_PCT = 15.0   # halt new trades if portfolio drops 15% from peak

_HYPOTHESES_PATH = os.path.join(os.path.dirname(__file__), "hypotheses.json")
_PEAK_EQUITY_PATH = os.path.join(os.path.dirname(__file__), "logs", "peak_equity.json")


def get_api():
    require_alpaca()
    return tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version="v2")


def get_account_summary():
    """Get current account state."""
    api = get_api()
    account = api.get_account()
    positions = api.list_positions()

    pos_list = []
    for p in positions:
        pos_list.append({
            "symbol": p.symbol,
            "qty": p.qty,
            "side": "long" if float(p.qty) > 0 else "short",
            "entry_price": float(p.avg_entry_price),
            "current_price": float(p.current_price),
            "market_value": float(p.market_value),
            "unrealized_pl": float(p.unrealized_pl),
            "unrealized_plpc": float(p.unrealized_plpc) * 100,
        })

    return {
        "equity": float(account.equity),
        "cash": float(account.cash),
        "buying_power": float(account.buying_power),
        "portfolio_value": float(account.portfolio_value),
        "positions": pos_list,
    }


def place_experiment(symbol, direction, notional_amount, extended_hours=False):
    """
    Place a paper trade for a research experiment.

    Args:
        symbol: Stock ticker
        direction: "long" or "short"
        notional_amount: Dollar amount to invest
        extended_hours: If True, place a limit order for extended hours trading
            (pre-market / after-hours). Only limit orders work outside regular
            hours on Alpaca. time_in_force is forced to "day".

    Returns:
        dict with order details
    """
    if not symbol or symbol == "TBD":
        return {"success": False, "error": f"Cannot trade symbol '{symbol}' — resolve to a real ticker first"}

    api = get_api()
    side = "buy" if direction == "long" else "sell"

    # Validate position size against portfolio limits
    try:
        account = api.get_account()
        portfolio_value = float(account.portfolio_value)
        max_notional = portfolio_value * MAX_POSITION_PCT
        if notional_amount > max_notional:
            return {
                "success": False,
                "error": f"Notional ${notional_amount:,.0f} exceeds {MAX_POSITION_PCT*100:.0f}% "
                         f"of portfolio (max ${max_notional:,.0f})"
            }
    except Exception as e:
        return {"success": False, "error": f"Could not validate position size: {e}"}

    try:
        quote = api.get_latest_trade(symbol)
        price = float(quote.price)
    except Exception as e:
        return {"success": False, "error": f"Could not get price: {e}"}

    try:
        # Extended-hours orders must use limit orders with time_in_force="day".
        # Regular-hours orders use market orders (or notional for longs).
        if extended_hours:
            # Set limit price slightly aggressive to improve fill probability:
            #   shorts: limit slightly below last price (we sell, so lower is worse for us
            #           but still within the spread — use -0.1% to stay near bid)
            #   longs:  limit slightly above last price (+0.1% to stay near ask)
            if direction == "short":
                limit_price = round(price * 0.999, 2)
            else:
                limit_price = round(price * 1.001, 2)

            qty = int(notional_amount / price)
            if qty < 1:
                return {"success": False, "error": f"Notional ${notional_amount} too small for {symbol} at ${price}"}

            order_kwargs = dict(
                symbol=symbol,
                side=side,
                type="limit",
                time_in_force="day",
                limit_price=limit_price,
                qty=qty,
                extended_hours=True,
            )
            print(f"[EXTENDED HOURS] Placing {side.upper()} limit order: {symbol} x{qty} @ ${limit_price:.2f} "
                  f"(last trade ${price:.2f})")
        else:
            # Alpaca only supports notional for buy-side market orders.
            # For short sells, compute qty from notional amount.
            order_kwargs = dict(
                symbol=symbol,
                side=side,
                type="market",
                time_in_force="day",
            )
            if direction == "short":
                qty = int(notional_amount / price)
                if qty < 1:
                    return {"success": False, "error": f"Notional ${notional_amount} too small for {symbol} at ${price}"}
                order_kwargs["qty"] = qty
            else:
                order_kwargs["notional"] = round(notional_amount, 2)

        order = api.submit_order(**order_kwargs)
        result = {
            "success": True,
            "order_id": order.id,
            "symbol": symbol,
            "side": side,
            "notional": notional_amount,
            "approx_qty": round(notional_amount / price, 4),
            "price_at_order": price,
            "extended_hours": extended_hours,
        }
        if extended_hours:
            result["limit_price"] = limit_price
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


def close_position(symbol):
    """Close an entire position in a symbol."""
    api = get_api()
    try:
        api.close_position(symbol)
        return {"success": True, "symbol": symbol}
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_current_price(symbol):
    """Get the current price for a symbol."""
    api = get_api()
    try:
        quote = api.get_latest_trade(symbol)
        return float(quote.price)
    except Exception:
        return None


def _load_hypotheses():
    """Load hypotheses directly (avoids circular import with research.py)."""
    try:
        with open(_HYPOTHESES_PATH) as f:
            return json.load(f)
    except Exception:
        return []


def _save_hypotheses(hypotheses):
    """Save hypotheses directly."""
    import tempfile
    tmp = _HYPOTHESES_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(hypotheses, f, indent=2)
    os.replace(tmp, _HYPOTHESES_PATH)


def _update_peak_equity(current_equity):
    """Track peak portfolio equity for drawdown calculation."""
    peak = current_equity
    try:
        with open(_PEAK_EQUITY_PATH) as f:
            data = json.load(f)
            peak = max(data.get("peak_equity", current_equity), current_equity)
    except Exception:
        pass
    with open(_PEAK_EQUITY_PATH, "w") as f:
        json.dump({"peak_equity": peak, "updated": datetime.now().isoformat()}, f)
    return peak


def check_stop_losses():
    """
    Check all active hypotheses for stop-loss, take-profit, and deadline violations.
    Closes positions that breach limits. Runs independently of the LLM agent.

    Returns list of actions taken.
    """
    hypotheses = _load_hypotheses()
    active = [h for h in hypotheses if h.get("status") == "active"]
    if not active:
        return []

    try:
        api = get_api()
        positions = {p.symbol: p for p in api.list_positions()}
        account = api.get_account()
        current_equity = float(account.equity)
    except Exception as e:
        return [{"action": "error", "message": f"Could not connect to Alpaca: {e}"}]

    # Portfolio-level drawdown check
    peak_equity = _update_peak_equity(current_equity)
    drawdown_pct = ((peak_equity - current_equity) / peak_equity) * 100 if peak_equity > 0 else 0

    actions = []
    now = datetime.now()
    modified = False

    for h in hypotheses:
        if h.get("status") != "active":
            continue

        symbol = h.get("expected_symbol")
        trade = h.get("trade", {})
        if not symbol or not trade:
            continue

        entry_price = trade.get("entry_price", 0)
        direction = h.get("expected_direction", "long")
        stop_loss_pct = trade.get("stop_loss_pct", DEFAULT_STOP_LOSS_PCT)
        take_profit_pct = trade.get("take_profit_pct", DEFAULT_TAKE_PROFIT_PCT)
        deadline = trade.get("deadline")

        # Get current position
        pos = positions.get(symbol)
        if not pos:
            actions.append({
                "action": "warning",
                "hypothesis_id": h["id"],
                "symbol": symbol,
                "message": f"No Alpaca position found for active hypothesis",
            })
            continue

        current_price = float(pos.current_price)
        unrealized_pct = float(pos.unrealized_plpc) * 100

        # For shorts, loss is positive price move
        if direction == "short":
            position_return_pct = -unrealized_pct
        else:
            position_return_pct = unrealized_pct

        reason = None

        # Stop-loss check
        if stop_loss_pct and position_return_pct <= -stop_loss_pct:
            reason = f"STOP-LOSS: {symbol} down {position_return_pct:+.1f}% (limit: -{stop_loss_pct}%)"

        # Take-profit check
        elif take_profit_pct and position_return_pct >= take_profit_pct:
            reason = f"TAKE-PROFIT: {symbol} up {position_return_pct:+.1f}% (target: +{take_profit_pct}%)"

        # Deadline check
        elif deadline:
            try:
                deadline_dt = datetime.fromisoformat(deadline)
                if now > deadline_dt:
                    reason = f"DEADLINE: {symbol} held past deadline ({deadline[:10]}), return {position_return_pct:+.1f}%"
            except (ValueError, TypeError):
                pass

        if reason:
            # Close the position
            result = close_position(symbol)
            if result.get("success"):
                # Mark hypothesis as completed
                h["status"] = "completed"
                h["result"] = {
                    "exit_price": current_price,
                    "exit_time": now.isoformat(),
                    "raw_return_pct": round(unrealized_pct, 2),
                    "exit_reason": reason,
                    "auto_closed": True,
                    "spy_at_entry": trade.get("spy_at_entry"),
                }
                modified = True
                actions.append({
                    "action": "closed",
                    "hypothesis_id": h["id"],
                    "symbol": symbol,
                    "reason": reason,
                    "return_pct": round(unrealized_pct, 2),
                })
            else:
                actions.append({
                    "action": "close_failed",
                    "hypothesis_id": h["id"],
                    "symbol": symbol,
                    "reason": reason,
                    "error": result.get("error"),
                })

    # Portfolio drawdown alert
    if drawdown_pct > MAX_PORTFOLIO_DRAWDOWN_PCT:
        actions.append({
            "action": "drawdown_alert",
            "message": f"Portfolio drawdown {drawdown_pct:.1f}% exceeds {MAX_PORTFOLIO_DRAWDOWN_PCT}% limit. "
                       f"Peak: ${peak_equity:,.0f}, Current: ${current_equity:,.0f}. "
                       f"New trades should be halted.",
            "drawdown_pct": round(drawdown_pct, 1),
        })

    if modified:
        _save_hypotheses(hypotheses)

    return actions


def check_portfolio_drawdown():
    """Check if portfolio drawdown exceeds the safety limit. Returns True if safe to trade."""
    try:
        api = get_api()
        account = api.get_account()
        current_equity = float(account.equity)
        peak = _update_peak_equity(current_equity)
        drawdown = ((peak - current_equity) / peak) * 100 if peak > 0 else 0
        return {
            "safe_to_trade": drawdown < MAX_PORTFOLIO_DRAWDOWN_PCT,
            "current_equity": current_equity,
            "peak_equity": peak,
            "drawdown_pct": round(drawdown, 1),
            "limit_pct": MAX_PORTFOLIO_DRAWDOWN_PCT,
        }
    except Exception as e:
        return {"safe_to_trade": False, "error": str(e)}


if __name__ == "__main__":
    # Can be run directly: python trader.py --check-stops
    if len(sys.argv) > 1 and sys.argv[1] == "--check-stops":
        actions = check_stop_losses()
        if actions:
            for a in actions:
                print(f"[{a['action']}] {a.get('symbol', '')} {a.get('reason', a.get('message', ''))}")
        else:
            print("No stop-loss triggers.")
