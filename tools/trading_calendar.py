"""
US Stock Market Trading Calendar Helper.

Prevents scheduling trades on market holidays (Good Friday, etc.).
Uses NYSE holiday calendar to determine if a given date is a trading day.

Usage:
    from tools.trading_calendar import is_trading_day, next_trading_day, trading_days_between

    is_trading_day(date(2026, 4, 3))    # Good Friday -> False
    next_trading_day(date(2026, 4, 3))  # -> date(2026, 4, 6) (Monday)
    trading_days_between(date(2026, 4, 1), date(2026, 4, 8))  # -> 4
"""

from datetime import date, timedelta
from typing import Optional

# NYSE observed holidays for 2025-2027
# Source: https://www.nyse.com/markets/hours-calendars
# Markets close at 1 PM on early close days (not listed — those are still trading days)
NYSE_HOLIDAYS = {
    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 20),  # MLK Jr. Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Jr. Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed, Jul 4 is Saturday)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
    # 2027
    date(2027, 1, 1),   # New Year's Day
    date(2027, 1, 18),  # MLK Jr. Day
    date(2027, 2, 15),  # Presidents' Day
    date(2027, 3, 26),  # Good Friday
    date(2027, 5, 31),  # Memorial Day
    date(2027, 6, 18),  # Juneteenth (observed, Jun 19 is Saturday)
    date(2027, 7, 5),   # Independence Day (observed, Jul 4 is Sunday)
    date(2027, 9, 6),   # Labor Day
    date(2027, 11, 25), # Thanksgiving
    date(2027, 12, 24), # Christmas (observed, Dec 25 is Saturday)
}


def is_trading_day(d: date) -> bool:
    """Check if a date is a US stock market trading day (not weekend, not holiday)."""
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if d in NYSE_HOLIDAYS:
        return False
    return True


def next_trading_day(d: date) -> date:
    """Get the next trading day on or after the given date."""
    while not is_trading_day(d):
        d += timedelta(days=1)
    return d


def next_trading_day_after(d: date) -> date:
    """Get the next trading day strictly after the given date."""
    return next_trading_day(d + timedelta(days=1))


def prev_trading_day(d: date) -> date:
    """Get the previous trading day on or before the given date."""
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def trading_days_between(start: date, end: date) -> int:
    """Count trading days between two dates (exclusive of end)."""
    count = 0
    d = start
    while d < end:
        if is_trading_day(d):
            count += 1
        d += timedelta(days=1)
    return count


def trading_days_from(start: date, n: int) -> date:
    """Get the date that is N trading days after start."""
    d = start
    counted = 0
    while counted < n:
        d += timedelta(days=1)
        if is_trading_day(d):
            counted += 1
    return d


def validate_trade_date(d: date, description: str = "trade") -> Optional[str]:
    """
    Validate that a date is suitable for trading.
    Returns None if OK, or a warning string if not.
    """
    if d.weekday() >= 5:
        return f"WARNING: {description} date {d} is a {d.strftime('%A')} (weekend). Next trading day: {next_trading_day(d)}"
    if d in NYSE_HOLIDAYS:
        holiday_name = _get_holiday_name(d)
        return f"WARNING: {description} date {d} is {holiday_name} (market closed). Next trading day: {next_trading_day_after(d)}"
    return None


def _get_holiday_name(d: date) -> str:
    """Get the name of a NYSE holiday."""
    month_day = (d.month, d.day)
    names = {
        (1, 1): "New Year's Day",
        (12, 25): "Christmas",
        (11, 26): "Thanksgiving", (11, 27): "Thanksgiving",
        (6, 19): "Juneteenth", (6, 18): "Juneteenth (observed)",
        (7, 4): "Independence Day", (7, 3): "Independence Day (observed)", (7, 5): "Independence Day (observed)",
    }
    if month_day in names:
        return names[month_day]
    if d.month == 1 and d.weekday() == 0 and 15 <= d.day <= 21:
        return "MLK Jr. Day"
    if d.month == 2 and d.weekday() == 0 and 15 <= d.day <= 21:
        return "Presidents' Day"
    if d.month in (3, 4) and d.weekday() == 4:  # Friday in March/April
        return "Good Friday"
    if d.month == 5 and d.weekday() == 0 and d.day >= 25:
        return "Memorial Day"
    if d.month == 9 and d.weekday() == 0 and d.day <= 7:
        return "Labor Day"
    return "NYSE Holiday"


if __name__ == '__main__':
    # Quick tests
    from datetime import date

    # Good Friday 2026
    assert not is_trading_day(date(2026, 4, 3)), "Good Friday should not be a trading day"
    assert next_trading_day(date(2026, 4, 3)) == date(2026, 4, 6), "Next after Good Friday should be Monday"

    # Weekend
    assert not is_trading_day(date(2026, 4, 4)), "Saturday should not be a trading day"
    assert not is_trading_day(date(2026, 4, 5)), "Sunday should not be a trading day"

    # Regular day
    assert is_trading_day(date(2026, 4, 6)), "Monday April 6 should be a trading day"

    # Trading days April 1-8, 2026 (Apr 1,2,6,7,8 = 5, Good Friday Apr 3 is closed)
    td = trading_days_between(date(2026, 4, 1), date(2026, 4, 9))
    assert td == 5, f"April 1-8 should have 5 trading days (Apr 3 Good Friday closed), got {td}"

    # Validate trade date
    warning = validate_trade_date(date(2026, 4, 3), "SYK close order")
    assert warning is not None and "Good Friday" in warning

    print("All tests passed!")
    print(f"\nNext 10 trading days from April 4, 2026:")
    d = date(2026, 4, 4)
    for i in range(10):
        d = next_trading_day_after(d) if i > 0 else next_trading_day(d)
        print(f"  {d} ({d.strftime('%A')})")
