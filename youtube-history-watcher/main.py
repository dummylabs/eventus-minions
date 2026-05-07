"""
YouTube History Watcher minion.

Fetches recent YouTube watch history via yt-dlp and emits events to Eventus.
Runs on cron (55 * * * *).  No henchman_sdk, no HTTP calls to Eventus.
"""

from __future__ import annotations

import json
from pathlib import Path

from eventus_sdk import minion

HISTORY_URL = "https://www.youtube.com/feed/history"
EVENT_TYPE = "web.youtube.watch"


def load_seen_ids(state_file: str) -> set[str]:
    path = Path(state_file)
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text()))
    except Exception as e:  # noqa: BLE001
        # Corrupt state file — start fresh rather than crash
        return set()


def save_seen_ids(state_file: str, seen_ids: set[str]) -> None:
    Path(state_file).write_text(json.dumps(sorted(seen_ids)))


def fetch_youtube_history(cookies_file: str, limit: int) -> list[dict] | None:
    import yt_dlp  # local import so the module can be imported in tests without yt-dlp installed

    ydl_opts = {
        "cookiefile": cookies_file,
        "extract_flat": True,
        "playlistend": limit,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "extractor_args": {"youtube": ["player_client=web"]},
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(HISTORY_URL, download=False)

    if not info or "entries" not in info:
        return None

    videos = []
    for entry in info.get("entries") or []:
        if not entry:
            continue
        video_id = entry.get("id", "")
        url = entry.get("url") or entry.get("original_url", "")
        if url and not url.startswith("http"):
            url = f"https://www.youtube.com/watch?v={url}"
        videos.append({
            "id": video_id,
            "url": url,
            "title": entry.get("title", "Без названия"),
        })
    return videos


@minion
def run(ctx):
    config = ctx.config
    cookies_file: str = config["cookies_file"]
    state_file: str = config["state_file"]
    fetch_limit: int = int(config["fetch_limit"])
    stop_after: int = int(config["stop_after_consecutive_dupes"])
    eventus_channel: str = config["eventus_channel"]
    eventus_initiator_id: str = config["eventus_initiator_id"]
    eventus_ttl: str = config["eventus_ttl"]

    # Per Q6: cookies missing → run fails (raise, don't return _outcome.status=error)
    if not Path(cookies_file).exists():
        raise FileNotFoundError(f"Cookies file not found: {cookies_file}")

    ctx.log.info("Fetching last %d videos from YouTube history...", fetch_limit)
    videos = fetch_youtube_history(cookies_file, fetch_limit)

    if videos is None:
        msg = "YouTube returned empty result — cookies may be expired"
        ctx.log.warning(msg)
        return {"_outcome": {"code": "yt_dlp_empty", "status": "warning", "message": msg}}

    seen_ids = load_seen_ids(state_file)
    ctx.log.info("Loaded %d known video IDs from state", len(seen_ids))

    new_videos = []
    consecutive_dupes = 0
    for video in videos:
        if video["id"] in seen_ids:
            if stop_after > 0:
                consecutive_dupes += 1
                if consecutive_dupes >= stop_after:
                    ctx.log.info(
                        "Reached %d consecutive duplicates, stopping scan", stop_after
                    )
                    break
        else:
            consecutive_dupes = 0
            new_videos.append(video)

    if not new_videos:
        ctx.log.info("No new videos found")
        return {"_outcome": {"code": "no_new_videos", "status": "success", "message": "No new videos"}}

    ctx.log.info("Sending %d new videos to Eventus...", len(new_videos))
    confirmed = 0
    for video in new_videos:
        try:
            event = ctx.create_event(
                channel=eventus_channel,
                initiator_id=eventus_initiator_id,
                type=EVENT_TYPE,
                state="new",
                ttl=eventus_ttl,
                url=video["url"],
                description=video["title"],
            )
            seen_ids.add(video["id"])
            confirmed += 1
            ctx.log.info(
                "NEW id=%s | %s | eventus_uid=%s", video["id"], video["title"], event.uid
            )
        except Exception as exc:  # noqa: BLE001 — one video failure must not abort the run
            ctx.log.error("FAILED id=%s | %s | error=%s", video["id"], video["title"], exc)

    save_seen_ids(state_file, seen_ids)
    ctx.log.info("State saved: %d total known IDs", len(seen_ids))

    message = f"Found {confirmed} new videos"
    return {
        "_outcome": {"code": "new_videos_found", "status": "success", "message": message},
        "new_videos": confirmed,
    }
