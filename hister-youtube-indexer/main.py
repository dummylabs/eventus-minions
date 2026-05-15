"""
Hister YouTube Indexer minion.

Triggers on `web.youtube.watch` events in state `new` and posts the URL to a
local Hister instance for full-text indexing. Mirrors how the Hister browser
extension submits pages (POST /api/add with JSON {url, title, ...}).
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from eventus_sdk import minion

ARTIFACT_KIND = "hister.indexed"


def _build_payload(event) -> dict:
    url = (event.url or "").split("#", 1)[0]
    payload: dict = {"url": url}
    if event.description:
        payload["title"] = event.description
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

    config = ctx.config
    base = str(config["hister_url"]).rstrip("/")
    api_url = f"{base}/api/add"
    timeout = float(config["request_timeout_seconds"])
    max_attempts = int(config["max_attempts"])
    backoff = list(config.get("backoff_seconds") or [])

    payload = _build_payload(event)
    ctx.log.info("POST %s url=%s", api_url, payload["url"])

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
        data={"url": payload["url"], "status_code": status, "attempts": attempt},
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
