"""
YouTube Scraper minion.

Extracts YouTube video metadata, subtitles, and ranked useful comments.
Invoked via ctx.run_minion("youtube-scraper", {"event_uid": ..., "url": ...}).
No triggers/cron — called only by youtube-watch-scraper-poller.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Any

from eventus_sdk import minion

# scraper.py and models.py live in the same directory; add it to sys.path
import os as _os
_THIS_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from models import ScrapeResponse  # noqa: E402
from scraper import extract_video_id, fetch_comments, fetch_metadata, fetch_subtitles  # noqa: E402

logger = logging.getLogger(__name__)

AGENT = "minion:youtube-scraper"


def _as_positive_int(value: Any, *, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be >= 1")
    return parsed


def _param_int(params: dict, config: dict, name: str, default: int, *aliases: str) -> int:
    for key in (name, *aliases):
        if key in params:
            return _as_positive_int(params[key], name=key)
    for key in (name, *aliases):
        if key in config:
            return _as_positive_int(config[key], name=key)
    return default


def _get_nested_url(event: Any) -> str | None:
    """Extract URL from an EventData object (event.url or event.payload.url/href)."""
    if event is None:
        return None
    candidates: list[Any] = [
        getattr(event, "url", None),
    ]
    payload = getattr(event, "payload", None)
    if payload is not None:
        candidates.append(payload.get("url") if hasattr(payload, "get") else None)
        candidates.append(payload.get("href") if hasattr(payload, "get") else None)
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _match_excluded_category(
    categories: list[str], excluded: list[str]
) -> str | None:
    excluded_lower = {c.lower() for c in excluded}
    for cat in categories:
        if cat.lower() in excluded_lower:
            return cat
    return None


def _result_details(result: ScrapeResponse) -> dict[str, Any]:
    return {
        "video_id": result.video_id,
        "comments_count": len(result.comments),
        "has_subtitles": result.subtitles is not None,
        "errors_count": len(result.errors),
        "errors": result.errors,
    }


def _empty_error_result(message: str, video_id: str = "") -> ScrapeResponse:
    return ScrapeResponse(
        video_id=video_id,
        errors=[message],
    )


def scrape_video(
    url: str,
    *,
    output_top_n: int,
    candidate_top_level_limit: int,
    max_scan: int,
    reply_patience: int,
    prefetched_meta: dict[str, Any] | None = None,
) -> ScrapeResponse:
    errors: list[str] = []
    t0 = time.time()

    video_id = extract_video_id(url)
    logger.info(
        "Scraping video_id=%s output_top_n=%d candidate_top_level_limit=%d "
        "max_scan=%d reply_patience=%d",
        video_id, output_top_n, candidate_top_level_limit, max_scan, reply_patience,
    )

    title = None
    description = None
    channel = None
    duration = None
    upload_date = None
    view_count = None
    like_count = None
    channel_id = None
    categories: list = []
    tags: list = []

    step_t0 = time.time()
    if prefetched_meta is not None:
        meta = prefetched_meta
        logger.info("Using prefetched metadata video_id=%s", video_id)
    else:
        logger.info("Request start metadata video_id=%s", video_id)
        try:
            meta = fetch_metadata(url)
            logger.info(
                "Request done metadata video_id=%s elapsed=%.2fs title_present=%s",
                video_id, time.time() - step_t0, meta.get("title") is not None,
            )
            logger.info(
                "Metadata fetched video_id=%s title=%r channel=%r",
                video_id, meta.get("title"), meta.get("channel"),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Request failed metadata video_id=%s elapsed=%.2fs error=%s",
                video_id, time.time() - step_t0, exc,
            )
            errors.append(f"metadata: {exc}")
            meta = {}

    title = meta.get("title")
    description = meta.get("description")
    channel = meta.get("channel")
    duration = meta.get("duration")
    upload_date = meta.get("upload_date")
    view_count = meta.get("view_count")
    like_count = meta.get("like_count")
    channel_id = meta.get("channel_id")
    categories = meta.get("categories", [])
    tags = meta.get("tags", [])

    subtitles = None
    step_t0 = time.time()
    logger.info("Request start subtitles.list video_id=%s", video_id)
    try:
        subtitles = fetch_subtitles(video_id)
        if subtitles is None:
            errors.append("subtitles: no suitable subtitles found")
        logger.info(
            "Request done subtitles video_id=%s elapsed=%.2fs found=%s",
            video_id, time.time() - step_t0, subtitles is not None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Request failed subtitles video_id=%s elapsed=%.2fs error=%s",
            video_id, time.time() - step_t0, exc,
        )
        errors.append(f"subtitles: {exc}")

    comments = []
    comments_meta = None
    step_t0 = time.time()
    logger.info(
        "Request start comments video_id=%s output_top_n=%d "
        "candidate_top_level_limit=%d max_scan=%d reply_patience=%d",
        video_id, output_top_n, candidate_top_level_limit, max_scan, reply_patience,
    )
    try:
        comments, comments_meta = fetch_comments(
            url,
            output_top_n=output_top_n,
            candidate_top_level_limit=candidate_top_level_limit,
            max_scan=max_scan,
            reply_patience=reply_patience,
        )
        logger.info(
            "Request done comments video_id=%s elapsed=%.2fs comments=%d "
            "scanned=%d stopped_reason=%s",
            video_id, time.time() - step_t0, len(comments),
            comments_meta.scanned, comments_meta.stopped_reason,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Request failed comments video_id=%s elapsed=%.2fs error=%s",
            video_id, time.time() - step_t0, exc,
        )
        errors.append(f"comments: {exc}")

    elapsed = round(time.time() - t0, 2)
    logger.info(
        "Done video_id=%s elapsed=%.2fs comments=%d errors=%d",
        video_id, elapsed, len(comments), len(errors),
    )

    return ScrapeResponse(
        video_id=video_id,
        title=title,
        description=description,
        channel=channel,
        duration=duration,
        upload_date=upload_date,
        view_count=view_count,
        like_count=like_count,
        channel_id=channel_id,
        categories=categories,
        tags=tags,
        subtitles=subtitles,
        comments=comments,
        comments_meta=comments_meta,
        errors=errors,
    )


@minion
def run(ctx):
    t0 = time.time()
    params = ctx.params
    config = ctx.config

    event_uid: str | None = params.get("event_uid")
    url: str | None = params.get("url")

    # Fall back to event payload if url not in params
    if not url and ctx.event:
        url = _get_nested_url(ctx.event)

    if not isinstance(url, str) or not url.strip():
        message = "input: url parameter is required"
        logger.error(message)
        if event_uid:
            ctx.complete_step(
                event_uid,
                agent=AGENT,
                new_state="skipped",
                comment="YouTube scrape skipped: missing URL",
                error=message,
                details={"phase": "input"},
            )
        return {"message": {"text": message, "severity": "error", "code": "missing_url"}}

    url = url.strip()
    excluded_categories: list[str] = config.get("excluded_categories") or []

    # Pre-fetch metadata for early category check
    prefetched_meta: dict[str, Any] | None = None
    try:
        prefetched_meta = fetch_metadata(url)
        logger.info(
            "Metadata fetched video_id=%s title=%r channel=%r",
            extract_video_id(url),
            prefetched_meta.get("title"),
            prefetched_meta.get("channel"),
        )
        video_categories: list[str] = prefetched_meta.get("categories") or []
        matched = _match_excluded_category(video_categories, excluded_categories)
        if matched:
            comment = f"not scrapped due to category: {matched}"
            logger.info(comment)
            video_id = extract_video_id(url)

            if event_uid:
                # Add metadata artifact even for excluded categories
                ctx.add_artifact(
                    event_uid,
                    kind="youtube.metadata",
                    agent=AGENT,
                    title=f"Metadata: {video_id}",
                    data={
                        "title": prefetched_meta.get("title"),
                        "description": prefetched_meta.get("description"),
                        "channel": prefetched_meta.get("channel"),
                        "duration": prefetched_meta.get("duration"),
                        "view_count": prefetched_meta.get("view_count"),
                        "like_count": prefetched_meta.get("like_count"),
                        "channel_id": prefetched_meta.get("channel_id"),
                        "categories": prefetched_meta.get("categories") or [],
                        "tags": prefetched_meta.get("tags") or [],
                        "upload_date": prefetched_meta.get("upload_date"),
                        "video_id": video_id,
                    },
                )
                ctx.complete_step(
                    event_uid,
                    agent=AGENT,
                    new_state="actionable",
                    comment=comment,
                    details={
                        "video_id": video_id,
                        "category": matched,
                        "comments_count": 0,
                        "has_subtitles": False,
                        "errors_count": 0,
                        "errors": [],
                    },
                )
            return {
                "message": {
                    "text": comment,
                    "severity": "info",
                    "code": "category_excluded",
                },
                "data": {"video_id": video_id, "category": matched},
            }
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Pre-scrape metadata fetch failed, proceeding without category check: %s", exc
        )
        prefetched_meta = None

    # Full scrape
    try:
        output_top_n = _param_int(params, config, "output_top_n", 10, "top_n")
        candidate_top_level_limit = _param_int(params, config, "candidate_top_level_limit", 30)
        max_scan = _param_int(params, config, "max_scan", 150)
        reply_patience = _param_int(params, config, "reply_patience", 50)
        if candidate_top_level_limit < output_top_n:
            raise ValueError("candidate_top_level_limit must be >= output_top_n")
        result = scrape_video(
            url,
            output_top_n=output_top_n,
            candidate_top_level_limit=candidate_top_level_limit,
            max_scan=max_scan,
            reply_patience=reply_patience,
            prefetched_meta=prefetched_meta,
        )
    except ValueError as exc:
        message = f"input: {exc}"
        logger.error(message)
        if event_uid:
            ctx.complete_step(
                event_uid,
                agent=AGENT,
                new_state="skipped",
                comment="YouTube scrape skipped: invalid input",
                error=message,
                details={"phase": "input"},
            )
        return {"message": {"text": message, "severity": "error", "code": "invalid_input"}}
    except Exception as exc:
        message = f"runtime: {exc}"
        logger.error(message)
        if event_uid:
            ctx.fail_step(
                event_uid,
                agent=AGENT,
                error=message,
                comment="YouTube scrape failed",
                details={"phase": "youtube_scrape"},
                release=True,
            )
        raise

    video_id = result.video_id
    elapsed = round(time.time() - t0, 2)

    if event_uid:
        # Metadata artifact (always)
        ctx.add_artifact(
            event_uid,
            kind="youtube.metadata",
            agent=AGENT,
            title=f"Metadata: {video_id}",
            data={
                "title": result.title,
                "description": result.description,
                "channel": result.channel,
                "duration": result.duration,
                "view_count": result.view_count,
                "like_count": result.like_count,
                "channel_id": result.channel_id,
                "categories": result.categories,
                "tags": result.tags,
                "upload_date": result.upload_date,
                "video_id": video_id,
            },
        )

        # Subtitles artifact (only if found)
        subtitles_chars = 0
        if result.subtitles is not None:
            # Subtitles is a Subtitles pydantic model with .text
            subtitle_text = result.subtitles.text if hasattr(result.subtitles, "text") else str(result.subtitles)
            subtitles_chars = len(subtitle_text)
            ctx.add_artifact(
                event_uid,
                kind="youtube.subtitles",
                agent=AGENT,
                title=f"Subtitles: {video_id}",
                text=subtitle_text,
            )

        # Comments artifact
        comments_data = [
            c.model_dump() if hasattr(c, "model_dump") else c
            for c in result.comments
        ]
        ctx.add_artifact(
            event_uid,
            kind="youtube.comments",
            agent=AGENT,
            title=f"Comments: {video_id}",
            data=comments_data,
        )

        ctx.complete_step(
            event_uid,
            agent=AGENT,
            new_state="actionable",
            comment="YouTube scrape completed",
            details=_result_details(result),
        )
    else:
        # Manual/API call without event_uid — compute subtitles_chars for return value
        subtitles_chars = (
            len(result.subtitles.text)
            if result.subtitles is not None and hasattr(result.subtitles, "text")
            else 0
        )

    return {
        "message": {
            "text": f"{video_id}: {len(result.comments)} comments",
            "severity": "info",
            "code": "scraped",
        },
        "data": {
            "video_id": video_id,
            "comments_count": len(result.comments),
            "subtitles_chars": subtitles_chars,
            "errors": result.errors,
            "elapsed_seconds": elapsed,
        },
    }
