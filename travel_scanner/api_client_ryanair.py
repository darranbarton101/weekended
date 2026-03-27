"""
Ryanair farefinder — unofficial API client.

No API key required. Makes one request per (outbound_date, return_date) pair.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime, timedelta
from typing import Callable

import requests
from requests.exceptions import RequestException

from .date_windows import get_departure_return_pairs
from .models import Deal, ScanParams, SearchWindow

logger = logging.getLogger(__name__)

RYANAIR_FAREFINDER = "https://www.ryanair.com/api/farfnd/v4/roundTripFares"
# Skyscanner search URL — lowercase IATA codes, dates in YYMMDD (e.g. 260417)
SKYSCANNER_URL = (
    "https://www.skyscanner.net/transport/flights"
    "/{origin}/{dest}/{out}/{ret}/?adults=1&currency=GBP"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.ryanair.com/",
}


def _make_deal_id(origin, destination, out_date, price, bucket):
    price_bucketed = round(price / bucket) * bucket
    key = f"{origin}|{destination}|{out_date}|{price_bucketed:.0f}|ryanair"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _call_ryanair(
    origin: str,
    out_date: date,
    ret_date: date,
    params: ScanParams,
    max_retries: int = 3,
    backoff_base: int = 2,
    on_retry: Callable[[str], None] | None = None,
) -> list[dict]:
    dep_wd = out_date.weekday()
    dep_config = params.departure_days.get(dep_wd, ("00:00", "23:59"))
    ret_wd = ret_date.weekday()
    ret_config = params.return_days.get(ret_wd, ("00:00", "23:59"))

    query = {
        "departureAirportIataCode": origin,
        "outboundDepartureDateFrom": out_date.strftime("%Y-%m-%d"),
        "outboundDepartureDateTo":   out_date.strftime("%Y-%m-%d"),
        "inboundDepartureDateFrom":  ret_date.strftime("%Y-%m-%d"),
        "inboundDepartureDateTo":    ret_date.strftime("%Y-%m-%d"),
        "currency": "GBP",
        "priceValueTo": int(params.max_price_gbp),
        "outboundDepartureTimeFrom": dep_config[0],
        "outboundDepartureTimeTo":   dep_config[1],
        "inboundDepartureTimeFrom":  ret_config[0],
        "inboundDepartureTimeTo":    ret_config[1],
        "adultPaxCount": 1,
    }

    for attempt in range(max_retries):
        try:
            resp = requests.get(RYANAIR_FAREFINDER, params=query, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                return resp.json().get("fares", [])
            elif resp.status_code == 429:
                wait = backoff_base ** (attempt + 2)
                if on_retry:
                    on_retry(f"↻ Ryanair rate limited — waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            elif resp.status_code in (403, 451):
                logger.debug("Ryanair blocked %s %s (status %d)", origin, out_date, resp.status_code)
                return []
            else:
                return []
        except RequestException as exc:
            wait = backoff_base ** (attempt + 1)
            if on_retry:
                on_retry(f"↻ Ryanair timeout — retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
            logger.debug("Ryanair network error: %s", exc)

    return []


def _parse_fare(fare, origin, out_date, ret_date, window_label, params) -> Deal | None:
    try:
        outbound = fare.get("outbound", {})
        inbound  = fare.get("inbound",  {})

        out_price = float(outbound.get("price", {}).get("value", 0) or 0)
        ret_price = float(inbound.get("price",  {}).get("value", 0) or 0)
        total = out_price + ret_price
        if total <= 0:
            total = float(fare.get("summary", {}).get("price", {}).get("value", 0) or 0)
        if total <= 0:
            return None

        dest_iata    = outbound.get("arrivalAirport", {}).get("iataCode", "")
        dest_city    = outbound.get("arrivalAirport", {}).get("city", {}).get("name", dest_iata)
        dest_country = outbound.get("arrivalAirport", {}).get("countryCode", "")
        if not dest_iata:
            return None

        def parse_dt(s):
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except (ValueError, TypeError):
                    continue
            return None

        out_dep_str = outbound.get("departureDate", "")
        ret_dep_str = inbound.get("departureDate", "")
        out_dt = parse_dt(out_dep_str) or datetime(out_date.year, out_date.month, out_date.day, 18, 0)
        ret_dt = parse_dt(ret_dep_str) or datetime(ret_date.year, ret_date.month, ret_date.day, 19, 0)

        # Validate return day + time
        ret_wd = ret_dt.weekday()
        if ret_wd not in params.return_days:
            return None
        ret_config = params.return_days[ret_wd]
        min_ret_hour = int(ret_config[0].split(":")[0])
        if ret_dt.hour < min_ret_hour:
            return None

        nights = (ret_dt.date() - out_dt.date()).days
        out_str = out_dt.strftime("%Y-%m-%d")
        ret_str = ret_dt.strftime("%Y-%m-%d")

        return Deal(
            id=_make_deal_id(origin, dest_iata, out_str, total, params.price_bucket_gbp),
            origin=origin,
            destination=dest_iata,
            destination_city=dest_city,
            destination_country=dest_country,
            outbound_departure=out_dt,
            return_departure=ret_dt,
            price_gbp=total,
            airline="Ryanair",
            stops=0,
            deep_link=SKYSCANNER_URL.format(
            origin=origin.lower(),
            dest=dest_iata.lower(),
            out=out_dt.strftime("%y%m%d"),
            ret=ret_dt.strftime("%y%m%d"),
        ),
            nights=nights,
            search_window=window_label,
            region_tag="",
            region_score=1.0,
            score=0.0,
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.debug("Ryanair parse error: %s", exc)
        return None


def fetch_deals_ryanair(
    origin: str,
    window: SearchWindow,
    params: ScanParams,
    config: dict,
    progress_callback: Callable[[str], None] | None = None,
) -> list[Deal]:
    """Fetch all Ryanair round trips for every departure/return date pair in window."""
    pairs = get_departure_return_pairs(window, params)
    if not pairs:
        return []

    delay = config.get("api", {}).get("rate_limit_delay_seconds", 2)
    all_deals: list[Deal] = []

    for i, (out_date, ret_date) in enumerate(pairs):
        if progress_callback:
            progress_callback(
                f"Ryanair {origin}: {out_date.strftime('%d %b')} → {ret_date.strftime('%d %b')} "
                f"({i+1}/{len(pairs)})"
            )
        fares = _call_ryanair(origin, out_date, ret_date, params)
        for fare in fares:
            deal = _parse_fare(fare, origin, out_date, ret_date, window.label, params)
            if deal and params.min_price_gbp <= deal.price_gbp <= params.max_price_gbp:
                all_deals.append(deal)
        if i < len(pairs) - 1:
            time.sleep(delay)

    return all_deals


def count_date_pairs(window: SearchWindow, params: ScanParams) -> int:
    """Return number of date pairs for progress estimation (no API calls)."""
    return len(get_departure_return_pairs(window, params))


def enrich_with_times(
    origin: str,
    destination: str,
    out_date: date,
    ret_date: date,
    params: ScanParams,
) -> dict | None:
    """
    Make a single Ryanair farefinder call for a specific origin + dates,
    then filter to the target destination to get exact flight times.

    Returns dict with keys: outbound_time, return_time, outbound_price,
    return_price, total_price — or None if no matching fare found.
    """
    fares = _call_ryanair(origin, out_date, ret_date, params)
    for fare in fares:
        outbound = fare.get("outbound", {})
        inbound = fare.get("inbound", {})
        dest_iata = outbound.get("arrivalAirport", {}).get("iataCode", "")
        if dest_iata.upper() != destination.upper():
            continue

        out_time_str = outbound.get("departureDate", "")
        ret_time_str = inbound.get("departureDate", "")
        out_price = float(outbound.get("price", {}).get("value", 0) or 0)
        ret_price = float(inbound.get("price", {}).get("value", 0) or 0)
        total = out_price + ret_price
        if total <= 0:
            total = float(fare.get("summary", {}).get("price", {}).get("value", 0) or 0)

        return {
            "outbound_time": out_time_str,
            "return_time": ret_time_str,
            "outbound_price": out_price,
            "return_price": ret_price,
            "total_price": total,
        }

    return None
