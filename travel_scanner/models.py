from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

# Weekday constants (matching Python's datetime.weekday())
MON, TUE, WED, THU, FRI, SAT, SUN = 0, 1, 2, 3, 4, 5, 6
DAY_NAMES = {MON: "Monday", TUE: "Tuesday", WED: "Wednesday",
             THU: "Thursday", FRI: "Friday", SAT: "Saturday", SUN: "Sunday"}
DAY_SHORT = {MON: "Mo", TUE: "Tu", WED: "We", THU: "Th",
             FRI: "Fr", SAT: "Sa", SUN: "Su"}


@dataclass
class Deal:
    id: str
    origin: str
    destination: str
    destination_city: str
    destination_country: str
    outbound_departure: datetime
    return_departure: datetime
    price_gbp: float
    airline: str
    stops: int
    deep_link: str
    nights: int
    search_window: str
    region_tag: str
    region_score: float
    score: float
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    notified: bool = False


@dataclass
class SearchWindow:
    label: str
    date_from: str   # dd/mm/yyyy
    date_to: str     # dd/mm/yyyy


@dataclass
class ScanParams:
    """
    Runtime scan parameters.

    departure_days maps weekday int (0=Mon…6=Sun) to (earliest_hhmm, latest_hhmm).
      e.g. {3: ("17:00","23:59"), 4: ("00:00","11:00")}
    return_days    same format.
      e.g. {6: ("17:00","23:59")}
    """
    origins: list[str]

    departure_days: dict[int, tuple[str, str]]   # {weekday: (from, to)}
    return_days: dict[int, tuple[str, str]]       # {weekday: (from, to)}

    near_term_enabled: bool
    near_term_weeks_from: int
    near_term_weeks_to: int

    long_term_enabled: bool
    long_term_months_from: int
    long_term_months_to: int

    max_price_gbp: float
    min_price_gbp: float
    max_stopovers: int
    results_per_call: int

    preferred_regions: list[dict] = field(default_factory=list)
    show_all: bool = True
    top_deals_count: int = 50
    price_bucket_gbp: float = 5.0
    use_serpapi: bool = True
    use_ryanair: bool = False
