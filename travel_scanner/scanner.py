"""
Core orchestration module.

run_scan_streaming() is a generator used by the dashboard — yields partial
results after each API batch so the UI updates live.

run_scan() is a blocking wrapper used by the background cron.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import time
import uuid
from typing import Callable, Generator

import yaml

from .api_client_ryanair import _call_ryanair, _parse_fare, count_date_pairs
from .api_client_serpapi import (
    _call_serpapi,
    _monthly_weekend_pairs,
    _parse_explore_result,
    _is_ryanair,
    fetch_deals_serpapi,
)
from .date_windows import generate_windows, get_departure_return_pairs
from .deal_filter import deduplicate, score_and_rank
from .deal_store import cleanup_stale, get_connection, load_deals, upsert_deals
from .models import FRI, SUN, THU, Deal, ScanParams

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def params_from_config(config: dict) -> ScanParams:
    dw  = config.get("departure_windows", {})
    sw  = config.get("search_windows", {})
    rw  = config.get("return_window", {})
    api = config.get("api", {})
    dp  = config.get("destination_preferences", {})
    thu   = dw.get("thursday_evening", {})
    fri   = dw.get("friday_morning", {})
    near  = sw.get("near_term", {})
    long_ = sw.get("long_term", {})

    departure_days: dict[int, tuple[str, str]] = {}
    if thu.get("enabled", True):
        departure_days[THU] = (thu.get("earliest", "17:00"), thu.get("latest", "23:59"))
    if fri.get("enabled", True):
        departure_days[FRI] = (fri.get("earliest", "00:00"), fri.get("latest", "11:00"))

    return ScanParams(
        origins=config.get("origins", ["GLA", "EDI"]),
        departure_days=departure_days,
        return_days={SUN: (rw.get("earliest", "17:00"), rw.get("latest", "23:59"))},
        near_term_enabled=near.get("enabled", True),
        near_term_weeks_from=near.get("weeks_from", 4),
        near_term_weeks_to=near.get("weeks_to", 8),
        long_term_enabled=long_.get("enabled", True),
        long_term_months_from=long_.get("months_from", 3),
        long_term_months_to=long_.get("months_to", 6),
        max_price_gbp=float(dp.get("max_price_gbp", 300)),
        min_price_gbp=float(dp.get("min_price_gbp", 20)),
        max_stopovers=int(api.get("max_stopovers", 2)),
        results_per_call=int(api.get("results_per_call", 50)),
        preferred_regions=dp.get("preferred_regions", []),
        show_all=dp.get("show_all", True),
        top_deals_count=int(dp.get("top_deals_count", 50)),
        price_bucket_gbp=float(dp.get("price_bucket_gbp", 5)),
    )


def setup_logging(config: dict) -> None:
    log_cfg  = config.get("logging", {})
    level    = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = log_cfg.get("file", "logs/scanner.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=log_cfg.get("max_bytes", 5_242_880),
        backupCount=log_cfg.get("backup_count", 3),
    )
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        handlers=[handler, logging.StreamHandler()],
    )


# ── Streaming generator (dashboard) ──────────────────────────────────────────

def run_scan_streaming(
    params: ScanParams,
    config: dict,
    force_refresh: bool = False,
) -> Generator[tuple[list[Deal], str, int, int], None, None]:
    """
    Yields (current_deals, message, step, total_steps) after each individual
    API call so the UI updates live with every request.
    The final yield contains the fully saved and ranked result set.
    """
    db_path     = config.get("database", {}).get("path", "data/deals.db")
    use_serpapi = bool(os.environ.get("SERPAPI_KEY")) and params.use_serpapi
    use_ryanair = config.get("ryanair", {}).get("enabled", True) and params.use_ryanair
    # Shorter delay for SerpAPI — only needed between live API calls (not cache hits)
    serp_delay  = 0.5
    ryanair_delay = config.get("api", {}).get("rate_limit_delay_seconds", 2)

    windows = generate_windows(params)
    if not windows:
        yield [], "No search windows enabled.", 0, 1
        return

    # Count individual API calls for accurate progress tracking
    serp_calls = (
        sum(len(_monthly_weekend_pairs(w, params)) for w in windows) * len(params.origins)
        if use_serpapi else 0
    )
    ryanair_pairs = (
        sum(count_date_pairs(w, params) for w in windows) * len(params.origins)
        if use_ryanair else 0
    )
    total_steps = max(serp_calls + ryanair_pairs, 1)

    scan_id = str(uuid.uuid4())[:8]   # short ID tags this scan's DB rows
    raw_deals: list[Deal] = []
    step = 0
    seen_dest_dates: set[str] = set()  # dedup across SerpAPI calls
    _retry_msgs: list[str] = []        # collect retry messages between yields

    # Only filter Ryanair from SerpAPI when Ryanair primary is also running
    filter_ryanair = params.use_ryanair
    api_key = os.environ.get("SERPAPI_KEY", "")

    def _snapshot() -> list[Deal]:
        return score_and_rank(deduplicate(raw_deals[:]), params)

    def _pop_retry_prefix(base_msg: str) -> str:
        """Prepend any pending retry messages then clear the list."""
        if _retry_msgs:
            prefix = " · ".join(_retry_msgs)
            _retry_msgs.clear()
            return f"{prefix} · {base_msg}"
        return base_msg

    for window in windows:
        for origin in params.origins:

            # ── SerpAPI: yield after EACH monthly-pair call ──────────
            if use_serpapi:
                date_pairs = _monthly_weekend_pairs(window, params)
                for pair_idx, (out_date, ret_date) in enumerate(date_pairs):
                    step += 1
                    out_str = out_date.strftime("%Y-%m-%d")
                    ret_str = ret_date.strftime("%Y-%m-%d")

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

                    raw_results, was_cached = _call_serpapi(
                        api_params, api_key, force_refresh=force_refresh,
                        on_retry=_retry_msgs.append,
                    )

                    batch_count = 0
                    for result in raw_results:
                        if filter_ryanair and _is_ryanair(result):
                            continue
                        deal = _parse_explore_result(
                            result, origin, window.label, params,
                        )
                        if deal is None:
                            continue
                        if deal.price_gbp < params.min_price_gbp or deal.price_gbp > params.max_price_gbp:
                            continue
                        dedup_key = f"{deal.destination}|{out_str}"
                        if dedup_key in seen_dest_dates:
                            continue
                        seen_dest_dates.add(dedup_key)
                        raw_deals.append(deal)
                        batch_count += 1

                    cache_tag = " [cached]" if was_cached else ""
                    _msg = (f"SerpAPI {origin} "
                            f"{out_date.strftime('%d %b')} → {ret_date.strftime('%d %b')} "
                            f"— {batch_count} deals{cache_tag}")
                    yield _snapshot(), _pop_retry_prefix(_msg), step, total_steps

                    # Only pause between live API calls — cache hits are instant
                    if not was_cached and pair_idx < len(date_pairs) - 1:
                        time.sleep(serp_delay)

            # ── Ryanair: yield after each date-pair call ─────────────
            if use_ryanair:
                pairs = get_departure_return_pairs(window, params)
                for i, (out_date, ret_date) in enumerate(pairs):
                    step += 1
                    fares = _call_ryanair(origin, out_date, ret_date, params,
                                         on_retry=_retry_msgs.append)
                    batch = [
                        d for fare in fares
                        if (d := _parse_fare(fare, origin, out_date, ret_date, window.label, params))
                        and params.min_price_gbp <= d.price_gbp <= params.max_price_gbp
                    ]
                    raw_deals.extend(batch)
                    _msg = (f"Ryanair {origin} "
                            f"{out_date.strftime('%d %b')} → {ret_date.strftime('%d %b')} "
                            f"({i + 1}/{len(pairs)}) — {len(batch)} deals")
                    yield _snapshot(), _pop_retry_prefix(_msg), step, total_steps
                    if i < len(pairs) - 1:
                        time.sleep(ryanair_delay)

    # Save to DB, delete old scans, reload — all after scan completes so
    # concurrent visitors always see the previous scan's results, never empty.
    final = score_and_rank(deduplicate(raw_deals), params)
    try:
        conn = get_connection(db_path)
        new_count, updated_count = upsert_deals(conn, final, scan_id=scan_id)
        # cleanup_old_scans removed — all users' deals coexist; only stale cleanup
        cleanup_stale(conn, config.get("database", {}).get("stale_after_days", 30))
        all_deals = load_deals(conn)
        completion_msg = f"Complete — {new_count} new, {updated_count} updated"
    except Exception as exc:
        logger.error("DB operations failed after scan: %s", exc)
        all_deals = final   # fall back to in-memory results
        completion_msg = f"Complete (DB error: {exc}) — showing in-memory results"

    yield all_deals, completion_msg, total_steps, total_steps


# ── Blocking wrapper (background cron) ───────────────────────────────────────

def run_scan(
    params: ScanParams,
    config: dict,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[list[Deal], int, int]:
    all_deals: list[Deal] = []
    for deals, msg, step, total in run_scan_streaming(params, config):
        all_deals = deals
        if progress_callback:
            progress_callback(f"[{step}/{total}] {msg}")
    return all_deals, 0, 0


def main() -> None:
    config = load_config()
    setup_logging(config)
    params = params_from_config(config)
    logger.info("Starting background scan…")
    run_scan(params, config)


if __name__ == "__main__":
    main()
