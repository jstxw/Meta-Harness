"""SSE endpoints.

Durable mode (Postgres store active): events stream from the per-run
``run_events`` log via LISTEN/NOTIFY. The SSE ``id:`` is the run's
monotonic, gap-free sequence number (I7), so ``Last-Event-ID``
reconnects resume exactly where the client left off — across API
restarts and regardless of which worker process emitted the event.

Memory fallback: the original in-process registry.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, AsyncIterator

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse
from psycopg import AsyncConnection, sql

from app.meta_harness.persistence import get_dsn
from app.meta_harness.store import notify_channel_for_run
from app.streaming import channel_for_run, event_registry


router = APIRouter(tags=["events"])


def _format_stored(event: Any) -> str:
    payload = dict(event.payload)
    payload["seq"] = event.seq
    data = json.dumps(payload, default=str, separators=(",", ":"))
    return f"event: {event.event_type}\nid: {event.seq}\ndata: {data}\n\n"


async def _durable_stream(
    store: Any,
    run_id: str,
    *,
    last_seq: int,
    heartbeat_interval: float = 15.0,
) -> AsyncIterator[str]:
    conn = await AsyncConnection.connect(conninfo=get_dsn(), autocommit=True)
    try:
        # LISTEN before replay so no event can fall between them.
        await conn.execute(
            sql.SQL("LISTEN {};").format(
                sql.Identifier(notify_channel_for_run(run_id))
            )
        )
        seq = last_seq
        for event in await store.list_events(run_id=run_id, after_seq=seq):
            seq = event.seq
            yield _format_stored(event)

        while True:
            gen = conn.notifies(timeout=heartbeat_interval)
            notified = False
            async for _notification in gen:
                notified = True
                break
            await gen.aclose()

            fresh = await store.list_events(run_id=run_id, after_seq=seq)
            for event in fresh:
                seq = event.seq
                yield _format_stored(event)
            if not notified and not fresh:
                yield ": heartbeat\n\n"
    finally:
        await conn.close()


@router.get("/runs/{run_id}/stream")
async def stream_run_events(
    run_id: str,
    request: Request,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    store = getattr(request.app.state, "store", None)

    if store is not None:
        try:
            last_seq = int(last_event_id) if last_event_id else 0
        except ValueError:
            last_seq = 0

        async def _durable():
            async for chunk in _durable_stream(store, run_id, last_seq=last_seq):
                if await request.is_disconnected():
                    break
                yield chunk

        stream = _durable()
    else:

        async def _events():
            async for chunk in event_registry.subscribe(
                channel_for_run(run_id),
                last_event_id=last_event_id,
            ):
                if await request.is_disconnected():
                    break
                yield chunk

        stream = _events()

    return StreamingResponse(
        stream,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
