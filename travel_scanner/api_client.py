"""
Kiwi.com Tequila API client.

Handles HTTP calls, retry/backoff, response parsing, and post-validation
(Tequila's time/day filters are advisory — we re-check on our side).
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime
from typing import Callable

import requests
from requests.exceptions import RequestException

from .models import Deal, ScanParams

logger = logging.getLogger(__name__)

TEQUILA_BASE_URL = "https://tequila.kiwi.com/v2/search"
KIWI_SEARCH_URL = "https://www.kiwi.com/en/search/results/{origin}/{dest}/{out}/{ret}/"


def _make_deal_id(origin: str, destination: str, outbound_date: str, price_gbp: float, bucket: float) -> str:
    price_bucketed = round(price_gbp / bucket) * bucket
    key = f"{origin}|{destination}|{outbound_date}|{price_bucketed:.0f}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_dt(iso: str) -> datetime:
    """Parse Tequila's ISO-like datetime strings."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(iso, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {iso!r}")


def _parse_flight(raw: dict, window_label: str, params: ScanParams) -> Deal | None:
    """Convert a single Tequila flight dict to a Deal, or None if it fails validation."""
    try:
        out_dt = _parse_dt(raw["local_departure"])
        ret_dt = _parse_dt(raw["route"][-1]["local_arrival"]) if raw.get("route") else None

        # Find the return departure time (first leg of the return journey)
        return_departure_dt = None
        fly_duration = raw.get("fly_duration", "")
        # Walk routes to find the return leg's departure
        for leg in raw.get("route", []):
            if leg.get("return", 0) == 1:
                return_departure_dt = _parse_dt(leg["local_departure"])
                break

        if return_departure_dt is None:
            # Fall back to using last leg arrival as proxy — still store the deal
            return_departure_dt = ret_dt or out_dt

        # Post-validation: confirm return departs on a Sunday after our minimum time
        if return_departure_dt.weekday() != 6:  # 6 = Sunday
            logger.debug("Skipping deal — return not on Sunday: %s", raw.get("id"))
            return None

        min_ret_hour = int(params.return_earliest.split(":")[0])
        if return_departure_dt.hour < min_ret_hour:
            logger.debug("Skipping deal — return before %s: %s", params.return_earliest, raw.get("id"))
            return None

        price = float(raw["price"])
        origin = raw["flyFrom"]
        destination = raw["flyTo"]
        out_date_str = out_dt.strftime("%Y-%m-%d")
        ret_date_str = return_departure_dt.strftime("%Y-%m-%d")

        deal_id = _make_deal_id(origin, destination, out_date_str, price, params.price_bucket_gbp)

        deep_link = KIWI_SEARCH_URL.format(
            origin=origin,
            dest=destination,
            out=out_dt.strftime("%Y-%m-%d"),
            ret=return_departure_dt.strftime("%Y-%m-%d"),
        )

        # Count total stops across outbound legs only
        outbound_stops = sum(
            1 for leg in raw.get("route", []) if leg.get("return", 0) == 0
        ) - 1
        stops = max(0, outbound_stops)

        nights = raw.get("nightsInDest", 0)
        if nights == 0 and ret_dt:
            nights = (return_departure_dt.date() - out_dt.date()).days

        city_to = raw.get("cityTo", raw.get("flyTo", ""))
        country_to = raw.get("countryTo", {}).get("code", "") if isinstance(raw.get("countryTo"), dict) else raw.get("countryTo", "")

        airline = raw.get("airlines", [""])[0] if raw.get("airlines") else ""

        return Deal(
            id=deal_id,
            origin=origin,
            destination=destination,
            destination_city=city_to,
            destination_country=country_to,
            outbound_departure=out_dt,
            return_departure=return_departure_dt,
            price_gbp=price,
            airline=airline,
            stops=stops,
            deep_link=deep_link,
            nights=nights,
            search_window=window_label,
            region_tag="",
            region_score=1.0,
            score=0.0,
        )

    except (KeyError, TypeError, ValueError) as exc:
        logger.debug("Failed to parse flight %s: %s", raw.get("id", "?"), exc)
        return None


def _call_api(
    api_params: dict,
    config: dict,
    api_key: str,
) -> list[dict]:
    """
    Make a single Tequila API call with retry/backoff.
    Returns the raw list of flight dicts, or [] on failure.
    """
    # Remove internal _label key before sending
    params = {k: v for k, v in api_params.items() if not k.startswith("_")}
    headers = {"apikey": api_key}
    max_retries = config["api"].get("max_retries", 3)
    backoff_base = config["api"].get("retry_backoff_base", 2)

    for attempt in range(max_retries):
        try:
            resp = requests.get(
                TEQUILA_BASE_URL,
                params=params,
                headers=headers,
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json().get("data", [])
                logger.info("API call %s → %d results", api_params.get("_label", ""), len(data))
                return data

            elif resp.status_code == 429:
                wait = backoff_base ** (attempt + 2)
                logger.warning("Rate limited. Waiting %ds (attempt %d)", wait, attempt + 1)
                time.sleep(wait)

            elif resp.status_code == 403:
                logger.error("API key invalid or access denied. Check TEQUILA_API_KEY env var.")
                return []

            elif resp.status_code >= 500:
                wait = backoff_base ** (attempt + 1)
                logger.warning("Server error %d. Retry in %ds", resp.status_code, wait)
                time.sleep(wait)

            else:
                logger.error("Unexpected status %d: %s", resp.status_code, resp.text[:200])
                return []

        except RequestException as exc:
            wait = backoff_base ** (attempt + 1)
            logger.warning("Network error: %s. Retry in %ds", exc, wait)
            time.sleep(wait)

    logger.error("Max retries exceeded for %s. Skipping.", api_params.get("_label", "call"))
    return []


def fetch_deals(
    api_params: dict,
    window_label: str,
    scan_params: ScanParams,
    config: dict,
    progress_callback: Callable[[str], None] | None = None,
) -> list[Deal]:
    """
    Execute one API call, parse results into Deals, apply price filter.
    """
    api_key = os.environ.get("TEQUILA_API_KEY", "")
    if not api_key:
        logger.error("TEQUILA_API_KEY environment variable not set.")
        if progress_callback:
            progress_callback("TEQUILA_API_KEY not set — skipping API calls.")
        return []

    label = api_params.get("_label", window_label)
    if progress_callback:
        progress_callback(f"Searching: {label}…")

    raw_flights = _call_api(api_params, config, api_key)

    deals: list[Deal] = []
    for raw in raw_flights:
        deal = _parse_flight(raw, window_label, scan_params)
        if deal is None:
            continue
        if deal.price_gbp < scan_params.min_price_gbp:
            continue
        if deal.price_gbp > scan_params.max_price_gbp:
            continue
        deals.append(deal)

    # Delay between successive API calls to respect rate limits
    delay = config["api"].get("rate_limit_delay_seconds", 2)
    time.sleep(delay)

    return deals
