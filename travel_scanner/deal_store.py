"""
Supabase persistence for deals and user preferences.

Handles upsert (new deals inserted, existing deals get last_seen + price
updated), notified flag management, stale deal cleanup, and per-user
preference storage keyed by browser UUID.
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


def upsert_deals(conn: Client, deals: list[Deal], scan_id: str = "") -> tuple[int, int]:
    """
    Insert new deals; update last_seen/price/score/scan_id for existing ones.
    Returns (new_count, updated_count).
    """
    if not deals:
        return 0, 0

    try:
        now = _now()

        # Get existing IDs in one query
        deal_ids = [d.id for d in deals]
        existing_ids = set()
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
                    "scan_id": scan_id,
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
                    "scan_id": scan_id,
                })
                new_count += 1

        for i in range(0, len(new_rows), 100):
            conn.table("deals").insert(new_rows[i:i + 100]).execute()

        for row in update_rows:
            conn.table("deals").update({
                "last_seen": row["last_seen"],
                "price_gbp": row["price_gbp"],
                "score": row["score"],
                "region_tag": row["region_tag"],
                "region_score": row["region_score"],
                "airline": row["airline"],
                "deep_link": row["deep_link"],
                "scan_id": row["scan_id"],
            }).eq("id", row["id"]).execute()

        return new_count, updated_count

    except Exception as exc:
        logger.error("upsert_deals failed: %s", exc)
        return 0, 0


def cleanup_old_scans(conn: Client, keep_scan_id: str) -> int:
    """
    Delete deals from all previous scans, keeping only those tagged with
    keep_scan_id. Called after a new scan completes successfully so old
    results are never wiped before new ones are safely saved.
    """
    try:
        resp = (
            conn.table("deals")
            .delete()
            .neq("scan_id", keep_scan_id)
            .execute()
        )
        deleted = len(resp.data) if resp.data else 0
        if deleted:
            logger.info("Removed %d deals from previous scans.", deleted)
        return deleted
    except Exception as exc:
        logger.error("cleanup_old_scans failed: %s", exc)
        return 0


def mark_notified(conn: Client, deal_ids: list[str]) -> None:
    """Mark deals as notified (shown in dashboard or email)."""
    if not deal_ids:
        return
    try:
        for i in range(0, len(deal_ids), 100):
            batch = deal_ids[i:i + 100]
            conn.table("deals").update({"notified": True}).in_("id", batch).execute()
    except Exception as exc:
        logger.error("mark_notified failed: %s", exc)


def load_deals(conn: Client) -> list[Deal]:
    """Load all deals from the database, ordered by score desc."""
    try:
        resp = conn.table("deals").select("*").order("score", desc=True).execute()
        return [_row_to_deal(r) for r in resp.data]
    except Exception as exc:
        logger.error("load_deals failed: %s", exc)
        return []


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
        scan_id=row.get("scan_id") or "",
    )


def clear_all_deals(conn: Client) -> int:
    """Delete all deals. Kept for admin/manual use — not called during scans."""
    try:
        resp = conn.table("deals").select("id").execute()
        count = len(resp.data)
        if count:
            ids = [r["id"] for r in resp.data]
            for i in range(0, len(ids), 100):
                batch = ids[i:i + 100]
                conn.table("deals").delete().in_("id", batch).execute()
            logger.info("Cleared %d deals.", count)
        return count
    except Exception as exc:
        logger.error("clear_all_deals failed: %s", exc)
        return 0


def cleanup_stale(conn: Client, stale_after_days: int) -> int:
    """
    Delete deals whose departure date has passed, or that haven't been seen
    in stale_after_days. Returns the number of rows deleted.
    """
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=stale_after_days)).isoformat()
        resp1 = conn.table("deals").delete().lt("outbound_departure", today).execute()
        resp2 = conn.table("deals").delete().lt("last_seen", stale_cutoff).execute()
        deleted = len(resp1.data) + len(resp2.data)
        if deleted:
            logger.info("Cleaned up %d stale deals.", deleted)
        return deleted
    except Exception as exc:
        logger.error("cleanup_stale failed: %s", exc)
        return 0


# ── User preferences ──────────────────────────────────────────────────────────

def get_user_prefs(conn: Client, uid: str) -> dict:
    """
    Load user preferences for the given browser UUID.
    Returns {} if not found or on error.
    """
    try:
        resp = conn.table("user_prefs").select("*").eq("uid", uid).execute()
        if resp.data:
            row = resp.data[0]
            return {
                "airports":   row.get("airports") or [],
                "max_price":  row.get("max_price") or 300,
                "month_range": row.get("month_range") or [1, 6],
                "stops":      row.get("stops") or "Any",
                "dep_days":   row.get("dep_days") or {},
                "ret_days":   row.get("ret_days") or {},
                "favourites": row.get("favourites") or [],
            }
    except Exception as exc:
        logger.error("get_user_prefs failed: %s", exc)
    return {}


def save_user_prefs(conn: Client, uid: str, prefs: dict) -> None:
    """
    Upsert user preferences. Only the keys present in prefs are written;
    existing columns not mentioned are left unchanged on update.
    Silent fail on error.
    """
    try:
        row = {"uid": uid, "updated_at": _now()}
        if "airports" in prefs:
            row["airports"] = prefs["airports"]
        if "max_price" in prefs:
            row["max_price"] = prefs["max_price"]
        if "month_range" in prefs:
            row["month_range"] = prefs["month_range"]
        if "stops" in prefs:
            row["stops"] = prefs["stops"]
        if "dep_days" in prefs:
            row["dep_days"] = prefs["dep_days"]
        if "ret_days" in prefs:
            row["ret_days"] = prefs["ret_days"]
        if "favourites" in prefs:
            row["favourites"] = prefs["favourites"]
        conn.table("user_prefs").upsert(row).execute()
    except Exception as exc:
        logger.error("save_user_prefs failed: %s", exc)
