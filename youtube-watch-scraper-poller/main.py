from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from eventus_sdk import minion

MINION_ID = "youtube-scraper"
AGENT_NAME = "minion:youtube_watch_scraper_poller"
MAX_SCRAPE_ATTEMPTS = 3


def _extract_url(event) -> str | None:
    candidates: list[Any] = [
        event.url,
        event.payload.fetch("url"),
        event.payload.fetch("href"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _has_live_claim(event) -> bool:
    until = event.claim_until
    if until is None:
        return False
    if until.tzinfo is None:
        until = until.replace(tzinfo=UTC)
    return until > datetime.now(UTC)


@minion
def run(ctx):
    event = (
        ctx.events.filter(type="web.youtube.watch", state="new")
        .since(hours=24)
        .last()
    )

    if event is None:
        ctx.log.info("No pending YouTube watch events in the last 24 h")
        return

    if _has_live_claim(event):
        ctx.log.info(
            "Skipping %s — already claimed by %s until %s",
            event.uid, event.claim_owner, event.claim_until,
        )
        return

    if (event.claim_attempt or 0) >= MAX_SCRAPE_ATTEMPTS:
        message = (
            f"Exhausted scrape attempts after {event.claim_attempt} tries "
            f"(limit {MAX_SCRAPE_ATTEMPTS})"
        )
        ctx.log.warning("%s — event=%s", message, event.uid)
        ctx.fail_step(
            event.uid,
            agent=AGENT_NAME,
            error=message,
            new_state="failed",
            comment="YouTube scrape attempts exhausted",
            details={"claim_attempt": event.claim_attempt or 0},
            release=False,
        )
        return

    url = _extract_url(event)
    if not url:
        message = "YouTube watch event has no URL in event.url, payload.url, or payload.href"
        ctx.log.warning("%s — event=%s", message, event.uid)
        ctx.complete_step(
            event.uid,
            agent=AGENT_NAME,
            new_state="skipped",
            comment="YouTube scrape skipped: missing URL",
            error=message,
        )
        return

    ctx.log.info("Delegating YouTube watch event %s to %s", event.uid, MINION_ID)
    result = ctx.run_minion(MINION_ID, params={"event_uid": event.uid, "url": url}, wait=False)

    if result.get("skipped") and result.get("reason") == "skipped_duplicate":
        ctx.log.info(
            "YouTube scraper already has an active duplicate run for event %s", event.uid
        )
    else:
        ctx.log.info("YouTube scraper dispatch result for %s: %s", event.uid, result)
