"""
Soft destination scoring and ranking.

Score = (1000 / price_gbp) × region_boost

This means cheaper flights always rank higher within the same region boost tier,
but a strong regional preference can lift a slightly more expensive flight above
a cheaper one in a less-preferred region. The user sees all deals; preferred
destinations simply float to the top.
"""

from __future__ import annotations

from .models import Deal, ScanParams


def _get_region_boost(deal: Deal, preferred_regions: list[dict]) -> tuple[str, float]:
    """
    Return the (region_name, boost) for a deal, using the highest matching boost.
    Returns ('', 1.0) if no region matches.
    """
    best_name = ""
    best_boost = 1.0

    for region in preferred_regions:
        boost = float(region.get("boost", 1.0))
        if boost <= best_boost:
            continue  # Only consider if this region would improve the score

        # Match by destination country code
        if deal.destination_country.upper() in [c.upper() for c in region.get("countries", [])]:
            best_boost = boost
            best_name = region["name"]
            continue

        # Match by destination airport IATA code (airports list)
        if deal.destination.upper() in [a.upper() for a in region.get("airports", [])]:
            best_boost = boost
            best_name = region["name"]
            continue

        # Match by destination IATA code (cities list — Tequila uses airport codes here)
        if deal.destination.upper() in [c.upper() for c in region.get("cities", [])]:
            best_boost = boost
            best_name = region["name"]
            continue

    return best_name, best_boost


def score_and_rank(deals: list[Deal], params: ScanParams) -> list[Deal]:
    """
    Score, tag, and sort deals. Returns up to params.top_deals_count items.
    If params.show_all is False, only preferred-region deals are returned.
    """
    for deal in deals:
        tag, boost = _get_region_boost(deal, params.preferred_regions)
        deal.region_tag = tag
        deal.region_score = boost
        deal.score = (1000.0 / deal.price_gbp) * boost if deal.price_gbp > 0 else 0.0

    if not params.show_all:
        deals = [d for d in deals if d.region_score > 1.0]

    deals.sort(key=lambda d: d.score, reverse=True)

    # Apply cap only if show_all is False (preferred-only mode).
    # When showing all deals, keep every result so cheap short-haul flights
    # don't push long-haul / pricier destinations out of the saved set.
    if not params.show_all and params.top_deals_count > 0:
        deals = deals[:params.top_deals_count]

    return deals


def deduplicate(deals: list[Deal]) -> list[Deal]:
    """Remove exact duplicate deal IDs, keeping the first occurrence (lowest price)."""
    seen: set[str] = set()
    unique: list[Deal] = []
    for deal in deals:
        if deal.id not in seen:
            seen.add(deal.id)
            unique.append(deal)
    return unique
