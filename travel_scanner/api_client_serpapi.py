"""
SerpAPI — Google Travel Explore client.

Uses the google_travel_explore engine to find the cheapest destinations
from a given origin for a date range (open-destination search). Ryanair
results are excluded here; the Ryanair farefinder handles those separately.

Free tier: 250 searches/month. At 4 calls/run, weekly = ~16/month.

Docs: https://serpapi.com/google-travel-explore
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import requests
from requests.exceptions import RequestException

from .models import Deal, ScanParams, SearchWindow

logger = logging.getLogger(__name__)

# ── SerpAPI response cache (Supabase-backed, shared across all users) ────────
CACHE_TTL_HOURS = 24

# Lazy Supabase client for cache — avoids import-time env var requirement
_cache_conn = None


def _get_cache_conn():
    """Get or create Supabase client for cache operations."""
    global _cache_conn
    if _cache_conn is None:
        try:
            from .deal_store import get_connection
            _cache_conn = get_connection()
        except Exception:
            pass
    return _cache_conn


def _cache_get(key: str) -> list[dict] | None:
    """Return cached results from Supabase if fresh, else None."""
    conn = _get_cache_conn()
    if not conn:
        return _cache_get_file(key)  # fallback to file cache
    try:
        resp = conn.table("api_cache").select("data,cached_at").eq("cache_key", key).execute()
        if resp.data:
            row = resp.data[0]
            cached_at = datetime.fromisoformat(row["cached_at"])
            age_hours = (datetime.utcnow() - cached_at.replace(tzinfo=None)).total_seconds() / 3600
            if age_hours < CACHE_TTL_HOURS:
                logger.info("SerpAPI cache hit: %s (%.1fh old)", key[:40], age_hours)
                return json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
    except Exception as exc:
        logger.debug("Supabase cache read failed: %s — falling back to file", exc)
        return _cache_get_file(key)
    return None


def _cache_put(key: str, data: list[dict]) -> None:
    """Store results in Supabase cache (shared across all users)."""
    conn = _get_cache_conn()
    if not conn:
        _cache_put_file(key, data)
        return
    try:
        conn.table("api_cache").upsert({
            "cache_key": key,
            "data": json.dumps(data),
            "cached_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as exc:
        logger.debug("Supabase cache write failed: %s — falling back to file", exc)
        _cache_put_file(key, data)


# ── File-based fallback (used if Supabase is unavailable) ────────────────────
CACHE_DIR = Path("data")
CACHE_FILE = CACHE_DIR / "serpapi_cache.json"


def _load_file_cache() -> dict:
    try:
        if CACHE_FILE.exists():
            return json.loads(CACHE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def _cache_get_file(key: str) -> list[dict] | None:
    cache = _load_file_cache()
    entry = cache.get(key)
    if entry:
        cached_at = datetime.fromisoformat(entry["ts"])
        age_hours = (datetime.utcnow() - cached_at).total_seconds() / 3600
        if age_hours < CACHE_TTL_HOURS:
            logger.info("SerpAPI file cache hit: %s (%.1fh old)", key[:40], age_hours)
            return entry["data"]
    return None


def _cache_put_file(key: str, data: list[dict]) -> None:
    cache = _load_file_cache()
    cache[key] = {"data": data, "ts": datetime.utcnow().isoformat()}
    cutoff = datetime.utcnow() - timedelta(hours=CACHE_TTL_HOURS)
    cache = {k: v for k, v in cache.items() if datetime.fromisoformat(v["ts"]) > cutoff}
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(cache))

def _log_api_call(origin: str, cache_key: str, result_count: int, was_cached: bool) -> None:
    """Log each API call to Supabase for usage tracking."""
    conn = _get_cache_conn()
    if not conn:
        return
    try:
        conn.table("api_usage").insert({
            "called_at": datetime.utcnow().isoformat(),
            "origin": origin,
            "cache_key": cache_key[:80],
            "result_count": result_count,
            "was_cached": was_cached,
        }).execute()
    except Exception:
        pass  # non-critical — don't break scans for logging


SERPAPI_BASE = "https://serpapi.com/search"
# Skyscanner search URL — lowercase IATA codes, dates in YYMMDD (e.g. 260417)
SKYSCANNER_URL = (
    "https://www.skyscanner.net/transport/flights"
    "/{origin}/{dest}/{out}/{ret}/?adults=1&currency=GBP"
)

# Ryanair IATA carrier code — filter these out so they don't duplicate
# results from the dedicated Ryanair farefinder client.
RYANAIR_CARRIER_CODES = {"fr", "ryanair", "ryanair uk", "ryanair sun"}


def _make_deal_id_serpapi(origin: str, destination: str, out_date: str, price: float, bucket: float) -> str:
    import hashlib
    price_bucketed = round(price / bucket) * bucket
    key = f"{origin}|{destination}|{out_date}|{price_bucketed:.0f}|serp"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _is_ryanair(result: dict) -> bool:
    """Return True if any flight in this result is operated by Ryanair."""
    # SerpAPI google_travel_explore results have a 'flights' key or 'airline' key
    airline = result.get("airline", "")
    if isinstance(airline, str) and airline.lower() in RYANAIR_CARRIER_CODES:
        return True
    # Sometimes nested in flights list
    for flight in result.get("flights", []):
        carrier = flight.get("airline", "").lower()
        if carrier in RYANAIR_CARRIER_CODES or "ryanair" in carrier:
            return True
    # Also check airline_logo URL
    logo = result.get("airline_logo", "")
    if "ryanair" in logo.lower():
        return True
    return False


def _parse_explore_result(
    result: dict,
    origin: str,
    window_label: str,
    params: ScanParams,
) -> Deal | None:
    """
    Parse a single result from the google_travel_explore response.
    These results represent the cheapest available price to a destination,
    not a specific flight time — time filtering happens in a follow-up
    google_flights call (see fetch_flight_times).
    """
    try:
        from datetime import datetime

        # google_travel_explore returns results in a "destinations" key with this structure:
        # destination_airport.code, name, country, flight_price, start_date, end_date,
        # number_of_stops, airline, airline_code
        dest_iata = (result.get("destination_airport") or {}).get("code", "")
        # Fallback: older explore_results format
        if not dest_iata:
            destination = result.get("destination", {})
            dest_iata = destination.get("airport_code") or destination.get("id", "")
        if not dest_iata:
            return None

        dest_city    = result.get("name") or (result.get("destination") or {}).get("city", dest_iata)
        dest_country = result.get("country") or (result.get("destination") or {}).get("country_code", "")

        price = float(result.get("flight_price") or result.get("price") or 0)
        if price <= 0:
            return None

        out_date_str = result.get("start_date") or result.get("departure_date", "")
        ret_date_str = result.get("end_date")   or result.get("return_date", "")

        if not out_date_str:
            return None

        try:
            out_dt = datetime.strptime(out_date_str, "%Y-%m-%d")
        except ValueError:
            return None

        try:
            ret_dt = datetime.strptime(ret_date_str, "%Y-%m-%d") if ret_date_str else None
        except ValueError:
            ret_dt = None

        if ret_dt is None:
            ret_dt = out_dt + timedelta(days=2)

        # google_travel_explore gives a price for the cheapest date in the window,
        # not a specific flight time. Skip weekday filtering — SerpAPI is used for
        # destination + price discovery; Ryanair handles day-specific matching.

        nights = (ret_dt.date() - out_dt.date()).days

        deal_id = _make_deal_id_serpapi(origin, dest_iata, out_date_str, price, params.price_bucket_gbp)

        deep_link = SKYSCANNER_URL.format(
            origin=origin.lower(),
            dest=dest_iata.lower(),
            out=out_dt.strftime("%y%m%d"),
            ret=ret_dt.strftime("%y%m%d"),
        )

        airline = result.get("airline", "")
        if isinstance(airline, list):
            airline = ", ".join(airline)

        return Deal(
            id=deal_id,
            origin=origin,
            destination=dest_iata,
            destination_city=dest_city,
            destination_country=dest_country,
            outbound_departure=out_dt,
            return_departure=ret_dt,
            price_gbp=price,
            airline=str(airline),
            stops=int(result.get("number_of_stops") or result.get("stops") or 0),
            deep_link=deep_link,
            nights=nights,
            search_window=window_label,
            region_tag="",
            region_score=1.0,
            score=0.0,
        )

    except (KeyError, TypeError, ValueError) as exc:
        logger.debug("Failed to parse SerpAPI result: %s — %s", result.get("name") or result.get("destination"), exc)
        return None


def _call_serpapi(
    params_dict: dict,
    api_key: str,
    max_retries: int = 3,
    backoff_base: int = 2,
    force_refresh: bool = False,
    on_retry: Callable[[str], None] | None = None,
) -> tuple[list[dict], bool]:
    """Make a SerpAPI call with retry/backoff and 24h caching.

    Returns (results_list, from_cache) so callers can skip delays on cache hits.
    """
    # Build cache key from query params (exclude api_key)
    cache_key = (
        f"{params_dict.get('engine', '')}|"
        f"{params_dict.get('departure_id', '')}|"
        f"{params_dict.get('arrival_id', '')}|"
        f"{params_dict.get('outbound_date', '')}|"
        f"{params_dict.get('return_date', '')}|"
        f"{params_dict.get('outbound_times', '')}|"
        f"{params_dict.get('return_times', '')}"
    )

    _origin = params_dict.get("departure_id", "")

    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            _log_api_call(_origin, cache_key, len(cached), was_cached=True)
            return cached, True

    params_dict["api_key"] = api_key

    for attempt in range(max_retries):
        try:
            resp = requests.get(SERPAPI_BASE, params=params_dict, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                # google_travel_explore returns results in various keys
                results = (
                    data.get("explore_results")
                    or data.get("destinations")
                    or data.get("results")
                    or data.get("best_flights")
                    or []
                )
                logger.info("SerpAPI call → %d results", len(results))
                _cache_put(cache_key, results)
                _log_api_call(_origin, cache_key, len(results), was_cached=False)
                return results, False

            elif resp.status_code == 429:
                wait = backoff_base ** (attempt + 2)
                logger.warning("SerpAPI rate limited. Waiting %ds", wait)
                if on_retry:
                    on_retry(f"↻ SerpAPI rate limited — waiting {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)

            elif resp.status_code == 401:
                logger.error("SerpAPI key invalid. Check SERPAPI_KEY env var.")
                return [], False

            else:
                logger.error("SerpAPI unexpected status %d: %s", resp.status_code, resp.text[:200])
                return [], False

        except RequestException as exc:
            wait = backoff_base ** (attempt + 1)
            logger.warning("SerpAPI network error: %s. Retry in %ds", exc, wait)
            if on_retry:
                on_retry(f"↻ SerpAPI timeout — retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)

    logger.error("SerpAPI max retries exceeded.")
    return [], False


def _monthly_weekend_pairs(
    window: SearchWindow,
    params: ScanParams,
) -> list[tuple[date, date]]:
    """
    For each month in the window, find the first qualifying departure day and
    the nearest following return day. Returns one (outbound, return) pair per
    month — so SerpAPI gets realistic short-trip dates rather than a 6-month
    range which gives nonsensical prices and night counts.
    """
    from datetime import datetime as _dt
    from dateutil.relativedelta import relativedelta

    start = _dt.strptime(window.date_from, "%d/%m/%Y").date()
    end   = _dt.strptime(window.date_to,   "%d/%m/%Y").date()

    dep_weekdays = sorted(params.departure_days.keys()) or [3]  # default Thu
    ret_weekdays = set(params.return_days.keys()) or {6}         # default Sun

    pairs: list[tuple[date, date]] = []
    month_cursor = start.replace(day=1)

    while month_cursor <= end:
        # Walk from max(month start, window start) to find a departure day
        day = max(month_cursor, start)
        found = False
        for _ in range(31):
            if day > end:
                break
            if day.weekday() in dep_weekdays:
                # Find the nearest return day within the next 14 days
                for offset in range(1, 15):
                    ret = day + timedelta(days=offset)
                    if ret.weekday() in ret_weekdays:
                        pairs.append((day, ret))
                        found = True
                        break
                if found:
                    break
            day += timedelta(days=1)

        month_cursor += relativedelta(months=1)

    return pairs


def fetch_deals_serpapi(
    origin: str,
    window: SearchWindow,
    params: ScanParams,
    config: dict,
    progress_callback: Callable[[str], None] | None = None,
    force_refresh: bool = False,
) -> list[Deal]:
    """
    Fetch open-destination deals from a given origin for a search window.

    Makes one SerpAPI call per month in the window, each using a real departure
    + return date pair (e.g. Thu 17 Apr → Sun 20 Apr).  This gives meaningful
    prices and dates rather than the garbled results that come from sending
    a 6-month window as a single outbound/return date pair.

    Ryanair results are excluded; the dedicated Ryanair farefinder handles those.
    """
    api_key = os.environ.get("SERPAPI_KEY", "")
    if not api_key:
        logger.error("SERPAPI_KEY environment variable not set.")
        if progress_callback:
            progress_callback("SERPAPI_KEY not set — skipping SerpAPI calls.")
        return []

    date_pairs = _monthly_weekend_pairs(window, params)
    if not date_pairs:
        logger.warning("SerpAPI: no date pairs generated for window %s", window.label)
        return []

    # Only filter Ryanair from SerpAPI results when the Ryanair farefinder is
    # also running — otherwise we'd strip e.g. Shannon/Cork/Belfast which are
    # served exclusively by Ryanair from Scottish airports and have no other
    # SerpAPI result to replace them.
    filter_ryanair = params.use_ryanair

    delay = config.get("api", {}).get("rate_limit_delay_seconds", 2)
    all_deals: list[Deal] = []
    seen_dest_dates: set[str] = set()   # avoid exact duplicates across months

    for out_date, ret_date in date_pairs:
        out_str = out_date.strftime("%Y-%m-%d")
        ret_str = ret_date.strftime("%Y-%m-%d")

        if progress_callback:
            progress_callback(
                f"SerpAPI: {origin} {out_date.strftime('%d %b')} → "
                f"{ret_date.strftime('%d %b')} …"
            )

        api_params = {
            "engine": "google_travel_explore",
            "departure_id": origin,
            "outbound_date": out_str,
            "return_date":   ret_str,
            "currency": "GBP",
            "hl": "en",
            "gl": "uk",
            "trip_type": "2",
        }

        raw_results, _cached = _call_serpapi(api_params, api_key, force_refresh=force_refresh)
        batch_deals = 0
        ryanair_skipped = 0

        for result in raw_results:
            if filter_ryanair and _is_ryanair(result):
                ryanair_skipped += 1
                continue

            deal = _parse_explore_result(result, origin, window.label, params)
            if deal is None:
                continue
            if deal.price_gbp < params.min_price_gbp or deal.price_gbp > params.max_price_gbp:
                continue

            # Skip if we've already seen this destination on the same outbound date
            dedup_key = f"{deal.destination}|{out_str}"
            if dedup_key in seen_dest_dates:
                continue
            seen_dest_dates.add(dedup_key)

            all_deals.append(deal)
            batch_deals += 1

        logger.info(
            "SerpAPI %s %s→%s → %d deals (%d Ryanair excluded)",
            origin, out_str, ret_str, batch_deals, ryanair_skipped,
        )

        if (out_date, ret_date) != date_pairs[-1]:
            time.sleep(delay)

    logger.info(
        "SerpAPI %s %s total → %d deals across %d calls",
        origin, window.label, len(all_deals), len(date_pairs),
    )
    return all_deals


# ── Google Flights enrichment ────────────────────────────────────────────────

def enrich_flight_times(
    origin: str,
    destination: str,
    out_date_str: str,
    ret_date_str: str,
    outbound_times: str | None = None,
    return_times: str | None = None,
    force_refresh: bool = False,
) -> list[dict]:
    """
    Call SerpAPI google_flights engine for a specific route + dates.
    Returns a list of flight options, each with departure/arrival times,
    airline, price, stops, and duration.

    outbound_times/return_times use SerpAPI format: "17,23" = 5pm–midnight.
    These map directly to the user's departure_days time windows.

    Uses the same 24h cache as discovery calls.
    """
    api_key = os.environ.get("SERPAPI_KEY", "")
    if not api_key:
        logger.error("SERPAPI_KEY not set — cannot enrich flight times.")
        return []

    params = {
        "engine": "google_flights",
        "departure_id": origin,
        "arrival_id": destination,
        "outbound_date": out_date_str,
        "return_date": ret_date_str,
        "currency": "GBP",
        "hl": "en",
        "gl": "uk",
        "type": "1",  # round trip
    }
    if outbound_times:
        params["outbound_times"] = outbound_times
    if return_times:
        params["return_times"] = return_times

    raw, _cached = _call_serpapi(params, api_key, force_refresh=force_refresh)

    # google_flights returns best_flights + other_flights
    # _call_serpapi already extracts best_flights via the fallback chain,
    # but we want both lists. Re-fetch from cache if available.
    # Actually, _call_serpapi stores the raw results list which came from
    # best_flights. Let's also check for other_flights by re-reading cache.
    # For simplicity, raw already contains the flights list.

    flights = []
    for flight_group in raw:
        try:
            # Each flight group has "flights" (legs) and top-level price/duration
            legs = flight_group.get("flights", [])
            if not legs:
                continue

            price = flight_group.get("price")
            total_duration = flight_group.get("total_duration", 0)
            stops = len(legs) - 1  # number of connections

            # Outbound first leg
            first_leg = legs[0]
            dep_airport = first_leg.get("departure_airport", {})
            arr_airport = first_leg.get("arrival_airport", {})
            airline = first_leg.get("airline", "")
            flight_number = first_leg.get("flight_number", "")

            dep_time = dep_airport.get("time", "")
            arr_time = arr_airport.get("time", "")

            flights.append({
                "airline": airline,
                "flight_number": flight_number,
                "departure_time": dep_time,
                "arrival_time": arr_time,
                "departure_airport": dep_airport.get("id", origin),
                "arrival_airport": arr_airport.get("id", destination),
                "price": price,
                "duration_mins": total_duration,
                "stops": stops,
                "legs": [
                    {
                        "airline": leg.get("airline", ""),
                        "flight_number": leg.get("flight_number", ""),
                        "dep_time": leg.get("departure_airport", {}).get("time", ""),
                        "arr_time": leg.get("arrival_airport", {}).get("time", ""),
                        "dep_airport": leg.get("departure_airport", {}).get("id", ""),
                        "arr_airport": leg.get("arrival_airport", {}).get("id", ""),
                        "duration": leg.get("duration", 0),
                    }
                    for leg in legs
                ],
            })
        except (KeyError, TypeError, IndexError) as exc:
            logger.debug("Failed to parse google_flights result: %s", exc)
            continue

    logger.info(
        "google_flights enrichment %s→%s %s→%s → %d options",
        origin, destination, out_date_str, ret_date_str, len(flights),
    )
    return flights
