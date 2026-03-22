"""
Generates rolling date windows and (outbound, return) date pairs
for any combination of departure/return weekdays.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta

from .models import ScanParams, SearchWindow


def generate_windows(params: ScanParams, today: date | None = None) -> list[SearchWindow]:
    """Return enabled look-ahead SearchWindows from ScanParams."""
    if today is None:
        today = date.today()

    windows: list[SearchWindow] = []

    if params.near_term_enabled:
        windows.append(SearchWindow(
            label="near_term",
            date_from=(today + timedelta(weeks=params.near_term_weeks_from)).strftime("%d/%m/%Y"),
            date_to=(today + timedelta(weeks=params.near_term_weeks_to)).strftime("%d/%m/%Y"),
        ))

    if params.long_term_enabled:
        windows.append(SearchWindow(
            label="long_term",
            date_from=(today + relativedelta(months=params.long_term_months_from)).strftime("%d/%m/%Y"),
            date_to=(today + relativedelta(months=params.long_term_months_to)).strftime("%d/%m/%Y"),
        ))

    return windows


def get_departure_return_pairs(
    window: SearchWindow,
    params: ScanParams,
) -> list[tuple[date, date]]:
    """
    Walk every day in the window. For each day matching a departure_day weekday,
    find the nearest following day matching a return_day weekday (up to 14 days ahead).
    Returns deduplicated (outbound_date, return_date) pairs.
    """
    start = datetime.strptime(window.date_from, "%d/%m/%Y").date()
    end   = datetime.strptime(window.date_to,   "%d/%m/%Y").date()

    dep_weekdays = set(params.departure_days.keys())
    ret_weekdays = set(params.return_days.keys())

    pairs: list[tuple[date, date]] = []
    seen: set[tuple[date, date]] = set()

    current = start
    while current <= end:
        if current.weekday() in dep_weekdays:
            for offset in range(1, 15):
                ret_date = current + timedelta(days=offset)
                if ret_date.weekday() in ret_weekdays:
                    pair = (current, ret_date)
                    if pair not in seen:
                        seen.add(pair)
                        pairs.append(pair)
                    break
        current += timedelta(days=1)

    return pairs
