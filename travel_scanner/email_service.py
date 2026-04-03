"""
Email service — Resend integration for deal alerts and verification.

Resend free tier: 100 emails/day, 3,000/month.
API docs: https://resend.com/docs/api-reference
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

RESEND_API = "https://api.resend.com/emails"
FROM_EMAIL = "Weekended <alerts@weekended.co.uk>"  # Update once domain verified
FROM_EMAIL_FALLBACK = "onboarding@resend.dev"  # Resend sandbox — works without domain


def _get_api_key() -> str | None:
    return os.environ.get("RESEND_API_KEY")


def _get_from_email() -> str:
    """Use verified domain if available, else Resend sandbox."""
    key = _get_api_key()
    if not key:
        return FROM_EMAIL_FALLBACK
    # TODO: switch to FROM_EMAIL once domain is verified in Resend
    return FROM_EMAIL_FALLBACK


def send_email(to: str, subject: str, html_body: str) -> tuple[bool, str]:
    """Send an email via Resend. Returns (success, message_or_id)."""
    api_key = _get_api_key()
    if not api_key:
        logger.warning("RESEND_API_KEY not set — email not sent to %s", to)
        return False, "Email service not configured"

    try:
        resp = requests.post(
            RESEND_API,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": _get_from_email(),
                "to": [to],
                "subject": subject,
                "html": html_body,
            },
            timeout=10,
        )
        if resp.status_code in (200, 201):
            email_id = resp.json().get("id", "sent")
            logger.info("Email sent to %s — id: %s", to, email_id)
            return True, email_id
        else:
            logger.error("Resend error %d: %s", resp.status_code, resp.text[:200])
            return False, f"Email service error ({resp.status_code})"
    except Exception as exc:
        logger.error("send_email failed: %s", exc)
        return False, str(exc)


# ── Email templates ──────────────────────────────────────────────────────────

def _base_template(content: str, unsubscribe_url: str = "") -> str:
    """Wrap content in a styled email template."""
    unsub = ""
    if unsubscribe_url:
        unsub = (
            f'<p style="margin-top:24px;font-size:11px;color:#999">'
            f'<a href="{unsubscribe_url}" style="color:#999">Unsubscribe</a> '
            f'from deal alerts</p>'
        )
    return f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:560px;margin:0 auto;
                background:#ffffff;padding:24px">
        <div style="background:linear-gradient(to right,#e09010,#f5b835);padding:8px 14px;
                    margin-bottom:16px">
            <span style="color:#fff;font-size:14px;font-weight:700;letter-spacing:1px">
                ✈ WEEKENDED
            </span>
        </div>
        {content}
        <hr style="border:none;border-top:1px solid #eee;margin:20px 0">
        <p style="font-size:11px;color:#999;line-height:1.6">
            Prices are indicative returns per person. Always confirm before booking.
        </p>
        {unsub}
    </div>
    """


def send_welcome_email(to: str) -> tuple[bool, str]:
    """Send a welcome/confirmation email to new subscriber."""
    content = """
    <h2 style="color:#1a1a4a;font-size:18px;margin:0 0 12px">Welcome to Weekended!</h2>
    <p style="color:#333;font-size:14px;line-height:1.6">
        You're now signed up for cheap flight deal alerts. We'll scan Google Flights
        and send you the best weekend return flights matching your preferences.
    </p>
    <p style="color:#333;font-size:14px;line-height:1.6">
        Deals land in your inbox — no need to check the site unless you want to browse.
    </p>
    <p style="color:#4a5bcc;font-size:14px;font-weight:700;margin-top:16px">
        Happy travels!
    </p>
    """
    html = _base_template(content)
    return send_email(to, "Welcome to Weekended ✈", html)


def send_deal_alert(to: str, deals: list[dict], unsubscribe_url: str = "") -> tuple[bool, str]:
    """
    Send a deal alert email with a list of deals.
    Each deal dict should have: city, country, price, airline, dep, ret, nights, book_url
    """
    if not deals:
        return False, "No deals to send"

    deal_rows = ""
    for d in deals[:10]:  # Cap at 10 deals per email
        deal_rows += f"""
        <tr>
            <td style="padding:8px 12px;border-bottom:1px solid #eee">
                <b style="color:#4a5bcc;font-size:15px">£{d.get('price', 0):.0f}</b>
                &nbsp;&nbsp;
                <b style="color:#1a1a4a">{d.get('city', '').upper()}</b>
                <span style="color:#999;font-size:12px">{d.get('country', '')}</span>
                <br>
                <span style="color:#666;font-size:12px">
                    {d.get('dep', '')} → {d.get('ret', '')} · {d.get('nights', '')}N
                    · {d.get('airline', '')}
                </span>
            </td>
            <td style="padding:8px;border-bottom:1px solid #eee;text-align:right">
                <a href="{d.get('book_url', '#')}" style="background:#4a5bcc;color:#fff;
                   padding:6px 12px;text-decoration:none;font-size:12px;font-weight:700">
                    Book →
                </a>
            </td>
        </tr>
        """

    n_deals = len(deals)
    content = f"""
    <h2 style="color:#1a1a4a;font-size:18px;margin:0 0 4px">
        {n_deals} cheap flight{'s' if n_deals != 1 else ''} found
    </h2>
    <p style="color:#999;font-size:12px;margin:0 0 16px">
        {datetime.utcnow().strftime('%d %B %Y')}
    </p>
    <table style="width:100%;border-collapse:collapse">
        {deal_rows}
    </table>
    <p style="margin-top:16px;font-size:13px;color:#333">
        <a href="https://weekended.streamlit.app" style="color:#4a5bcc;font-weight:700">
            Browse all deals on Weekended →
        </a>
    </p>
    """
    html = _base_template(content, unsubscribe_url)
    subject = f"✈ {n_deals} cheap weekend flights from £{deals[0].get('price', 0):.0f}"
    return send_email(to, subject, html)
