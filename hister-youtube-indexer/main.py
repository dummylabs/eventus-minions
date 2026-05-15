"""
Hister YouTube Indexer minion.

Triggers on `web.youtube.watch` events in state `actionable`, after upstream
YouTube minions have attached `youtube.metadata`. It posts a Hister document with
pseudo-HTML so Hister's extractor pipeline runs. Hister's yt-dlp extractor (when
enabled) matches by URL; the pseudo-HTML is the non-empty content gate and a
fallback for installations without yt-dlp enabled.
"""

from __future__ import annotations

import html
import json
import time
import urllib.error
import urllib.request
from typing import Any

from eventus_sdk import minion

ARTIFACT_KIND = "hister.indexed"
METADATA_KIND = "youtube.metadata"


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _latest_metadata(ctx, event_uid: str) -> dict[str, Any] | None:
    artifacts = ctx.get_artifacts(event_uid, kind=METADATA_KIND, limit=1)
    if not artifacts:
        return None
    data = artifacts[0].get("data")
    return data if isinstance(data, dict) else None


def _metadata_has_indexable_content(meta: dict[str, Any]) -> bool:
    return any(_clean(meta.get(key)) for key in ("title", "description", "channel", "channel_id"))


def _build_text(meta: dict[str, Any]) -> str:
    parts: list[str] = []
    title = _clean(meta.get("title"))
    description = _clean(meta.get("description"))
    channel = _clean(meta.get("channel"))
    channel_id = _clean(meta.get("channel_id"))

    if title:
        parts.append(title)
    if channel:
        parts.append(f"Channel: {channel}")
    if channel_id:
        parts.append(f"Channel ID: {channel_id}")
    if description:
        parts.append(description)
    return "\n\n".join(parts)


def _build_html(meta: dict[str, Any], url: str) -> str:
    title = _clean(meta.get("title")) or url
    description = _clean(meta.get("description"))
    channel = _clean(meta.get("channel"))
    channel_id = _clean(meta.get("channel_id"))

    channel_bits = []
    if channel:
        channel_bits.append(f"<p><strong>Channel:</strong> {html.escape(channel)}</p>")
    if channel_id:
        channel_bits.append(f"<p><strong>Channel ID:</strong> {html.escape(channel_id)}</p>")
    description_html = ""
    if description:
        escaped = html.escape(description).replace("\n", "<br>\n")
        description_html = f"<section><h2>Description</h2><p>{escaped}</p></section>"

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '  <meta charset="utf-8">',
            f"  <title>{html.escape(title)}</title>",
            "</head>",
            "<body>",
            "  <article>",
            f"    <h1>{html.escape(title)}</h1>",
            *(f"    {bit}" for bit in channel_bits),
            f'    <p><a href="{html.escape(url, quote=True)}">{html.escape(url)}</a></p>',
            f"    {description_html}" if description_html else "",
            "  </article>",
            "</body>",
            "</html>",
        ]
    )


def _build_payload(event, meta: dict[str, Any]) -> dict[str, Any]:
    url = (event.url or "").split("#", 1)[0]
    title = _clean(meta.get("title")) or _clean(event.description) or url
    text = _build_text(meta)
    payload: dict[str, Any] = {
        "url": url,
        "title": title,
        "text": text,
        "html": _build_html(meta, url),
        "metadata": {
            "source": "eventus",
            "event_uid": event.uid,
            "youtube_metadata_artifact": True,
        },
    }
    return payload


def _post(api_url: str, payload: dict, timeout: float) -> int:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=body,
        headers={
            "Content-Type": "application/json; charset=UTF-8",
            "Origin": "hister://",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


@minion
def run(ctx):
    event = ctx.event
    if event is None:
        return {
            "message": {
                "text": "Minion invoked without an event",
                "severity": "warning",
                "code": "no_event",
            },
        }

    raw_url = (event.url or "").strip()
    if not raw_url:
        ctx.log.warning("Event %s has no URL; skipping", event.uid)
        return {
            "message": {
                "text": "Event has no URL",
                "severity": "warning",
                "code": "missing_url",
            },
        }

    meta = _latest_metadata(ctx, event.uid)
    if not meta or not _metadata_has_indexable_content(meta):
        ctx.log.warning("Event %s has no usable %s artifact; skipping", event.uid, METADATA_KIND)
        return {
            "message": {
                "text": "Missing YouTube metadata artifact",
                "severity": "warning",
                "code": "missing_metadata",
            },
            "data": {"url": raw_url, "metadata_kind": METADATA_KIND},
        }

    config = ctx.config
    base = str(config["hister_url"]).rstrip("/")
    api_url = f"{base}/api/add"
    timeout = float(config["request_timeout_seconds"])
    max_attempts = int(config["max_attempts"])
    backoff = list(config.get("backoff_seconds") or [])

    payload = _build_payload(event, meta)
    ctx.log.info("POST %s url=%s html_bytes=%d", api_url, payload["url"], len(payload["html"]))

    last_error: str | None = None
    status: int | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            status = _post(api_url, payload, timeout)
            if status in (201, 406, 422):
                break
            last_error = f"unexpected HTTP {status}"
            ctx.log.warning(
                "Hister returned %s on attempt %d/%d", status, attempt, max_attempts
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            ctx.log.warning(
                "Hister POST failed on attempt %d/%d: %s", attempt, max_attempts, last_error
            )
        if attempt < max_attempts:
            delay = backoff[attempt - 1] if attempt - 1 < len(backoff) else 0
            if delay > 0:
                time.sleep(delay)

    if status not in (201, 406, 422):
        raise RuntimeError(
            f"Hister POST failed after {max_attempts} attempts: {last_error}"
        )

    ctx.add_artifact(
        event.uid,
        kind=ARTIFACT_KIND,
        data={
            "url": payload["url"],
            "status_code": status,
            "attempts": attempt,
            "html_present": bool(payload.get("html")),
            "text_present": bool(payload.get("text")),
        },
        title="Hister indexing result",
    )

    if status == 201:
        return {
            "message": {
                "text": "Indexed in Hister",
                "severity": "info",
                "code": "hister_indexed",
            },
            "data": {"status_code": 201, "url": payload["url"]},
        }
    if status == 406:
        return {
            "message": {
                "text": "Hister skipped URL (matches skip rule)",
                "severity": "info",
                "code": "hister_skipped",
            },
            "data": {"status_code": 406, "url": payload["url"]},
        }
    return {
        "message": {
            "text": "Hister rejected as sensitive content",
            "severity": "info",
            "code": "hister_sensitive",
        },
        "data": {"status_code": 422, "url": payload["url"]},
    }
