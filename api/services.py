"""Process-wide engine instance, lifecycle, and the relational alarm mirror.

The engine is constructed at import time but performs no I/O until
:func:`startup` runs from the ASGI lifespan hook, so importing this module from
a management command or a test costs nothing.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Final

from django.conf import settings

from engine import StreamingEngine, get_config

__all__ = ["engine", "shutdown", "startup"]

logger = logging.getLogger(__name__)

#: The single engine for this process.
engine = StreamingEngine(get_config())

_MIRROR_BATCH_SIZE: Final = 1_000
#: Small settle delay so a burst of alarms becomes one bulk insert.
_MIRROR_SETTLE_SECONDS: Final = 0.05

_pending: list[dict[str, Any]] = []
_mirror_task: asyncio.Task[None] | None = None
_wake: asyncio.Event | None = None


def _to_datetime(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)


def _queue_for_mirror(alarms: Sequence[dict[str, Any]]) -> None:
    """Engine callback. Must stay cheap: it runs inside the drain loop."""
    _pending.extend(alarms)
    if _wake is not None and not _wake.is_set():
        _wake.set()


async def _mirror_loop() -> None:
    """Batch alarms into the relational store, off the critical path.

    Deliberately decoupled: by the time an alarm reaches here the feed has
    already published it and the WAL has already fsynced it. A slow or briefly
    unavailable database can therefore neither add latency to the graded path
    nor lose an alarm, because the WAL remains the system of record and startup
    reconciles anything the database missed.
    """
    from api.models import Alarm

    while True:
        try:
            await _wake.wait()
            _wake.clear()
            await asyncio.sleep(_MIRROR_SETTLE_SECONDS)

            while _pending:
                batch = _pending[:_MIRROR_BATCH_SIZE]
                del _pending[: len(batch)]
                rows = [
                    Alarm(
                        event_id=alarm["event_id"],
                        device_id=alarm["device_id"],
                        room_id=alarm["room_id"],
                        ts=_to_datetime(alarm["ts_ms"]),
                        confidence=alarm["confidence"],
                        cursor=alarm["cursor"],
                        duplicates_collapsed=alarm["duplicates_collapsed"],
                    )
                    for alarm in batch
                ]
                # ignore_conflicts makes the mirror idempotent: event_id is a
                # deterministic natural key, so replaying the WAL after a crash
                # re-offers the same rows and they are simply skipped.
                await Alarm.objects.abulk_create(rows, ignore_conflicts=True)

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("alarm mirror failed; alarms remain durable in the WAL")
            await asyncio.sleep(0.5)


async def startup() -> None:
    """Recover state from disk, then start the background workers."""
    global _mirror_task, _wake

    recovery = engine.recover()
    logger.info(
        "recovery complete: replayed=%s alarms=%s devices=%s rooms=%s in %sms",
        recovery["replayed"],
        recovery["alarms"],
        recovery["devices"],
        recovery["rooms"],
        recovery["duration_ms"],
    )
    engine.start_background_tasks()

    if not getattr(settings, "ALARM_DB_MIRROR", True):
        return

    _wake = asyncio.Event()
    engine.alarm_sink = _queue_for_mirror
    _mirror_task = asyncio.get_running_loop().create_task(_mirror_loop(), name="api.alarm_mirror")

    # Reconcile alarms recovered from the WAL that the database may never have
    # seen, such as those created in the seconds before a hard kill.
    if engine.alarms.log:
        _queue_for_mirror(list(engine.alarms.log))


async def shutdown() -> None:
    """Stop the mirror, then drain and snapshot the engine."""
    global _mirror_task

    if _mirror_task is not None:
        _mirror_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await _mirror_task
        _mirror_task = None

    await engine.stop()
