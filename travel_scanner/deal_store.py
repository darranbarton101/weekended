"""
Supabase persistence for deals.

Handles upsert (new deals inserted, existing deals get last_seen + price
updated), notified flag management, and stale deal cleanup.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from supabase import create_client, Client

from .models import Deal

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dt_to_str(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def get_connection(db_path: str = "") -> Client:
    """Return a Supabase client. db_path is ignored (kept for API compat)."""
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def upsert_deals(conn: Client, deals: list[Deal]) -> tuple[int, int]:
    """
    Insert new deals; update last_seen/price/score for existing ones.
    Returns (new_count, updated_count).
    """
    if not deals:
        return 0, 0

    now = _now()

    # Get existing IDs in one query
    deal_ids = [d.id for d in deals]
    existing_ids = set()
    # Supabase has a limit on filter length, batch if needed
    for i in range(0, len(deal_ids), 100):
        batch = deal_ids[i:i + 100]
        resp = conn.table("deals").select("id").in_("id", batch).execute()
        existing_ids.update(r["id"] for r in resp.data)

    new_count = 0
    updated_count = 0

    new_rows = []
    update_rows = []

    for deal in deals:
        if deal.id in existing_ids:
            update_rows.append({
                "id": deal.id,
                "last_seen": now,
                "price_gbp": deal.price_gbp,
                "score": deal.score,
                "region_tag": deal.region_tag,
                "region_score": deal.region_score,
                "airline": deal.airline,
                "deep_link": deal.deep_link,
            })
            updated_count += 1
        else:
            new_rows.append({
                "id": deal.id,
                "origin": deal.origin,
                "destination": deal.destination,
                "destination_city": deal.destination_city,
                "destination_country": deal.destination_country,
                "outbound_departure": _dt_to_str(deal.outbound_departure),
                "return_departure": _dt_to_str(deal.return_departure),
                "price_gbp": deal.price_gbp,
                "airline": deal.airline,
                "stops": deal.stops,
                "deep_link": deal.deep_link,
                "nights": deal.nights,
                "search_window": deal.search_window,
                "region_tag": deal.region_tag,
                "region_score": deal.region_score,
                "score": deal.score,
                "first_seen": now,
                "last_seen": now,
                "notified": False,
            })
            new_count += 1

    # Batch insert new deals
    for i in range(0, len(new_rows), 100):
        conn.table("deals").insert(new_rows[i:i + 100]).execute()

    # Batch update existing deals
    for row in update_rows:
        conn.table("deals").update({
            "last_seen": row["last_seen"],
            "price_gbp": row["price_gbp"],
            "score": row["score"],
            "region_tag": row["region_tag"],
            "region_score": row["region_score"],
            "airline": row["airline"],
            "deep_link": row["deep_link"],
        }).eq("id", row["id"]).execute()

    return new_count, updated_count


def mark_notified(conn: Client, deal_ids: list[str]) -> None:
    """Mark deals as notified (shown in dashboard or email)."""
    if not deal_ids:
        return
    for i in range(0, len(deal_ids), 100):
        batch = deal_ids[i:i + 100]
        conn.table("deals").update({"notified": True}).in_("id", batch).execute()


def load_deals(conn: Client) -> list[Deal]:
    """Load all deals from the database, ordered by score desc."""
    resp = conn.table("deals").select("*").order("score", desc=True).execute()
    return [_row_to_deal(r) for r in resp.data]


def _row_to_deal(row: dict) -> Deal:
    def _str_to_dt(s: str | None) -> datetime:
        if not s:
            return datetime.now(timezone.utc)
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return datetime.now(timezone.utc)

    return Deal(
        id=row["id"],
        origin=row["origin"],
        destination=row["destination"],
        destination_city=row.get("destination_city") or "",
        destination_country=row.get("destination_country") or "",
        outbound_departure=_str_to_dt(row.get("outbound_departure")),
        return_departure=_str_to_dt(row.get("return_departure")),
        price_gbp=row.get("price_gbp") or 0.0,
        airline=row.get("airline") or "",
        stops=row.get("stops") or 0,
        deep_link=row.get("deep_link") or "",
        nights=row.get("nights") or 0,
        search_window=row.get("search_window") or "",
        region_tag=row.get("region_tag") or "",
        region_score=row.get("region_score") or 1.0,
        score=row.get("score") or 0.0,
        first_seen=_str_to_dt(row.get("first_seen")),
        last_seen=_str_to_dt(row.get("last_seen")),
        notified=bool(row.get("notified")),
    )


def clear_all_deals(conn: Client) -> int:
    """Delete all deals from the database. Used before a fresh scan."""
    resp = conn.table("deals").select("id").execute()
    count = len(resp.data)
    if count:
        # Delete in batches
        ids = [r["id"] for r in resp.data]
        for i in range(0, len(ids), 100):
            batch = ids[i:i + 100]
            conn.table("deals").delete().in_("id", batch).execute()
        logger.info("Cleared %d old deals before fresh scan.", count)
    return count


def cleanup_stale(conn: Client, stale_after_days: int) -> int:
    """
    Delete deals whose departure date has passed, or that haven't been seen
    in stale_after_days. Returns the number of rows deleted.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=stale_after_days)).isoformat()

    # Delete past departures
    resp1 = conn.table("deals").delete().lt("outbound_departure", today).execute()
    # Delete stale (not seen recently)
    resp2 = conn.table("deals").delete().lt("last_seen", stale_cutoff).execute()

    deleted = len(resp1.data) + len(resp2.data)
    if deleted:
        logger.info("Cleaned up %d stale deals.", deleted)
    return deleted
