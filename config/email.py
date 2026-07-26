"""
Single source of truth for every email the backend sends.

One shell, one sender. Call sites describe *content* (heading, paragraphs,
details, CTA) and never write markup — that keeps all mail visually identical
and means a change to the shell lands everywhere at once.

Layout is table-based on purpose: Outlook on Windows ignores `max-width` on a
`<div>`, so div-based emails go full-bleed there.
"""

import html as _html
import logging
import os
import re
from typing import Iterable, Sequence

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

RESEND_URL = "https://api.resend.com/emails"
EMAIL_FROM = os.getenv("EMAIL_FROM", "Pupscribe <no-reply@no-reply.pupscribe.in>")
LOGO_URL = os.getenv(
    "EMAIL_LOGO_URL", "https://assets.pupscribe.in/branding/pupscribe_logo.jpg"
)
SITE_URL = os.getenv("FRONTEND_URL", "https://marketplace.pupscribe.in").rstrip("/")

# ── design tokens ─────────────────────────────────────────────────────────────
# Brand pink is the constant: it carries the logo and every CTA, in every email.
# The eyebrow is the one variable — it signals what this particular mail is for.
PINK = "#D92681"
INK = "#1A1014"
BODY = "#4A3A42"
MUTED = "#7C6570"
FAINT = "#9C8B94"
HAIRLINE = "#EAE0E5"
HAIRLINE_SOFT = "#F2EAEE"

TONES = {
    "brand": {"accent": PINK, "tint": "#FBE9F3", "ink": "#8E1354"},
    "action": {"accent": "#B4610E", "tint": "#FCF6F1", "ink": "#7A4A14"},
    "success": {"accent": "#1B7F5A", "tint": "#F0F8F4", "ink": "#126043"},
    "danger": {"accent": "#B3261E", "tint": "#FCF2F1", "ink": "#8A1D17"},
    "info": {"accent": "#2D04AA", "tint": "#F3F1FD", "ink": "#230383"},
}

FONT = "Arial,Helvetica,sans-serif"  # email clients can't load the Prompt brand face


def esc(value) -> str:
    """Escape a value for safe interpolation into email HTML."""
    return _html.escape(str(value if value is not None else ""), quote=True)


def _to_text(html_fragment: str) -> str:
    """Crude HTML → text for the plain-text alternative."""
    text = re.sub(r"<br\s*/?>", "\n", html_fragment)
    text = re.sub(r"<[^>]+>", "", text)
    return _html.unescape(text).strip()


def render_email(
    *,
    heading: str,
    eyebrow: str = "",
    tone: str = "brand",
    context: str = "Marketplace",
    greeting: str = "",
    paragraphs: Sequence[str] = (),
    details: Sequence[tuple] = (),
    callout: str = "",
    cta: tuple | None = None,
    meta: str = "",
    preheader: str = "",
) -> tuple[str, str]:
    """Render the standard shell. Returns (html, plain_text).

    `paragraphs` and `callout` are treated as trusted HTML so call sites can
    bold a name — pass user-supplied values through `esc()` first. Everything
    else is escaped here.
    """
    t = TONES.get(tone, TONES["brand"])
    rows: list[str] = []

    # Header — logo left, context label right, sitting on a pink rule.
    rows.append(
        f'<tr><td style="padding:0 0 16px;border-bottom:2px solid {PINK};">'
        f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%"><tr>'
        f'<td><img src="{LOGO_URL}" alt="Pupscribe" width="150" height="35" '
        f'style="display:block;width:150px;height:35px;border:0;outline:none;text-decoration:none;" /></td>'
        f'<td align="right" style="font-family:{FONT};font-size:11px;color:{FAINT};'
        f'letter-spacing:1.4px;text-transform:uppercase;">{esc(context)}</td>'
        f"</tr></table></td></tr>"
    )

    # Eyebrow + heading.
    eyebrow_html = (
        f'<div style="font-size:11px;font-weight:bold;letter-spacing:1.4px;'
        f'text-transform:uppercase;color:{t["accent"]};padding-bottom:10px;">{esc(eyebrow)}</div>'
        if eyebrow
        else ""
    )
    rows.append(
        f'<tr><td style="padding:34px 0 0;font-family:{FONT};">{eyebrow_html}'
        f'<div style="font-size:24px;font-weight:bold;color:{INK};line-height:1.25;">{esc(heading)}</div>'
        f"</td></tr>"
    )

    # Greeting + body copy.
    body_bits = []
    if greeting:
        body_bits.append(f'<p style="margin:0 0 14px;">{esc(greeting)}</p>')
    for para in paragraphs:
        body_bits.append(f'<p style="margin:0 0 14px;">{para}</p>')
    if body_bits:
        rows.append(
            f'<tr><td style="padding:16px 0 0;font-family:{FONT};font-size:15px;'
            f'line-height:1.7;color:{BODY};">{"".join(body_bits)}</td></tr>'
        )

    # Detail rows — label/value pairs on hairlines.
    if details:
        detail_rows = []
        for label, value in details:
            detail_rows.append(
                f'<tr><td style="padding:12px 0 11px;font-family:{FONT};font-size:13.5px;'
                f'color:{MUTED};border-bottom:1px solid {HAIRLINE_SOFT};width:42%;'
                f'vertical-align:top;">{esc(label)}</td>'
                f'<td style="padding:12px 0 11px;font-family:{FONT};font-size:13.5px;color:{INK};'
                f'font-weight:bold;border-bottom:1px solid {HAIRLINE_SOFT};">{esc(value)}</td></tr>'
            )
        rows.append(
            f'<tr><td style="padding:22px 0 0;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" '
            f'style="border-top:1px solid {HAIRLINE};">{"".join(detail_rows)}</table></td></tr>'
        )

    # Callout — a tinted box carrying the one thing they must read.
    if callout:
        rows.append(
            f'<tr><td style="padding:22px 0 0;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" '
            f'style="background:{t["tint"]};border-left:3px solid {t["accent"]};border-radius:0 8px 8px 0;">'
            f'<tr><td style="padding:14px 16px;font-family:{FONT};font-size:14px;'
            f'line-height:1.6;color:{t["ink"]};">{callout}</td></tr></table></td></tr>'
        )

    # CTA — always pink, never more than one.
    if cta:
        label, url = cta
        rows.append(
            f'<tr><td style="padding:26px 0 0;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td style="background:{PINK};border-radius:8px;">'
            f'<a href="{esc(url)}" style="display:inline-block;padding:13px 28px;font-family:{FONT};'
            f'font-size:15px;font-weight:bold;color:#FFFFFF;text-decoration:none;">{esc(label)}</a>'
            f"</td></tr></table></td></tr>"
        )

    if meta:
        rows.append(
            f'<tr><td style="padding:24px 0 0;font-family:{FONT};font-size:13.5px;'
            f'line-height:1.65;color:{MUTED};">{esc(meta)}</td></tr>'
        )

    rows.append(
        f'<tr><td style="padding:30px 0 0;">'
        f'<div style="border-top:1px solid {HAIRLINE};padding-top:16px;font-family:{FONT};'
        f'font-size:12px;line-height:1.6;color:{FAINT};">'
        f'Sent by Pupscribe · <a href="{SITE_URL}" style="color:{FAINT};text-decoration:underline;">'
        f'{SITE_URL.replace("https://", "")}</a><br>'
        f'<span style="color:#B9A9B2;">© Pupscribe. All rights reserved.</span>'
        f"</div></td></tr>"
    )

    preheader_html = (
        f'<div style="display:none;max-height:0;overflow:hidden;opacity:0;'
        f'mso-hide:all;">{esc(preheader)}</div>'
        if preheader
        else ""
    )

    html_out = (
        f'<div style="background:#FFFFFF;">{preheader_html}'
        f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" '
        f'style="background:#FFFFFF;border-collapse:collapse;">'
        f'<tr><td align="center" style="padding:24px 12px;">'
        f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" '
        f'style="max-width:520px;font-family:{FONT};">'
        f'{"".join(rows)}'
        f"</table></td></tr></table></div>"
    )

    # ── plain-text alternative ────────────────────────────────────────────────
    lines = [heading, ""]
    if greeting:
        lines.append(greeting)
    lines.extend(_to_text(p) for p in paragraphs)
    if details:
        lines.append("")
        lines.extend(f"{label}: {value}" for label, value in details)
    if callout:
        lines += ["", _to_text(callout)]
    if cta:
        lines += ["", f"{cta[0]}: {cta[1]}"]
    if meta:
        lines += ["", meta]
    lines += ["", "— Pupscribe", SITE_URL]
    text_out = "\n".join(str(line) for line in lines)

    return html_out, text_out


def send_email(
    to: str | Iterable[str],
    subject: str,
    *,
    heading: str,
    eyebrow: str = "",
    tone: str = "brand",
    context: str = "Marketplace",
    greeting: str = "",
    paragraphs: Sequence[str] = (),
    details: Sequence[tuple] = (),
    callout: str = "",
    cta: tuple | None = None,
    meta: str = "",
    preheader: str = "",
    cc: str | Iterable[str] | None = None,
    reply_to: str | None = None,
    attachments: Sequence[dict] | None = None,
) -> bool:
    """Render the standard shell and send it via Resend.

    `attachments` takes Resend's shape: [{"filename": ..., "content": <base64 str>}].
    Never raises — a failed notification must not fail the request that triggered it.
    """
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        logger.warning("RESEND_API_KEY not set; skipping email '%s'", subject)
        return False

    recipients = [to] if isinstance(to, str) else [addr for addr in to if addr]
    if not recipients:
        logger.warning("No recipients for email '%s'; skipping", subject)
        return False

    # Fall back to the opening line so inboxes show something deliberate.
    if not preheader and paragraphs:
        preheader = _to_text(paragraphs[0])[:140]

    html_body, text_body = render_email(
        heading=heading,
        eyebrow=eyebrow,
        tone=tone,
        context=context,
        greeting=greeting,
        paragraphs=paragraphs,
        details=details,
        callout=callout,
        cta=cta,
        meta=meta,
        preheader=preheader,
    )

    payload = {
        "from": EMAIL_FROM,
        "to": recipients,
        "subject": subject,
        "html": html_body,
        "text": text_body,
    }
    if cc:
        payload["cc"] = [cc] if isinstance(cc, str) else list(cc)
    if reply_to:
        payload["reply_to"] = reply_to
    if attachments:
        payload["attachments"] = list(attachments)

    try:
        resp = requests.post(
            RESEND_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        logger.info("Email '%s' sent to %s", subject, ", ".join(recipients))
        return True
    except requests.exceptions.RequestException as e:
        detail = getattr(e.response, "text", "") if getattr(e, "response", None) else ""
        logger.error("Email '%s' failed for %s: %s %s", subject, recipients, e, detail)
        return False
