from eventus_sdk import minion


@minion
def run(ctx):
    event = ctx.event
    if event is None:
        ctx.log.warning("No event in context — skipping")
        return

    url = event.url
    if not url:
        ctx.log.warning("No URL found in event %s — skipping", event.uid)
        return

    current_duration = event.payload.fetch("duration_ms", 0)

    prev_events = (
        ctx.events.filter(type="web.page.visit", url=url)
        .exclude(state="deleted")
        .without(event)
        .since(hours=8)
        .until(event.recorded_at)
        .all()
    )

    total_duration = current_duration
    total_visits = 1
    for ev in prev_events:
        total_duration += ev.payload.fetch("duration_ms", 0)
        total_visits += ev.payload.fetch("visit_count", 1)

    ctx.log.info(
        "Aggregating %d previous event(s) for URL %s → duration=%d ms, visits=%d",
        len(prev_events),
        url,
        total_duration,
        total_visits,
    )

    updated_payload = dict(event.payload)
    updated_payload["duration_ms"] = total_duration
    updated_payload["visit_count"] = total_visits

    ctx.modify_event(
        event.uid,
        state="processed",
        payload=updated_payload,
        comment=(
            f"Aggregated {len(prev_events)} previous visit(s): "
            f"total duration {total_duration} ms, {total_visits} visit(s)"
        ),
    )

    for ev in prev_events:
        ctx.modify_event(
            ev.uid,
            state="deleted",
            comment=f"Superseded by aggregated event {event.uid}",
        )
