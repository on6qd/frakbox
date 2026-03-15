"""
Trading execution for research experiments.
Places and closes paper trades to test hypotheses.
"""

import alpaca_trade_api as tradeapi
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL


def get_api():
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


def place_experiment(symbol, direction, notional_amount):
    """
    Place a paper trade for a research experiment.

    Args:
        symbol: Stock ticker
        direction: "long" or "short"
        notional_amount: Dollar amount to invest

    Returns:
        dict with order details
    """
    api = get_api()
    side = "buy" if direction == "long" else "sell"

    try:
        quote = api.get_latest_trade(symbol)
        price = float(quote.price)
    except Exception as e:
        return {"success": False, "error": f"Could not get price: {e}"}

    try:
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
        return {
            "success": True,
            "order_id": order.id,
            "symbol": symbol,
            "side": side,
            "notional": notional_amount,
            "approx_qty": round(notional_amount / price, 4),
            "price_at_order": price,
        }
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
