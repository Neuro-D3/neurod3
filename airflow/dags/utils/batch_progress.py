"""
Progress lines for long paper-mapping batch tasks.

A citation batch can run for 20+ minutes (a primary paper with 1,000+ citing
papers, each fetched and its full text cached) and used to log nothing until it
returned, so a working batch looked the same as a hung one. ``BatchProgress``
logs a line when each item starts and a heartbeat at most every
``every_seconds`` inside it, carrying the batch's counters, OpenAlex request
and 429 counts, and elapsed time.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)


def format_elapsed(seconds: float) -> str:
    s = max(int(seconds), 0)
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"


class BatchProgress:
    """
    Usage::

        progress = BatchProgress(f"Citations batch {i}", len(rows), "primary papers",
                                 counters=metrics, telemetry=telemetry)
        for n, row in enumerate(rows):
            progress.update(n, note=row_label, force=True)     # one line per item
            for j, sub in enumerate(subitems):
                progress.update(note=f"{row_label}: {j}/{len(subitems)}")  # throttled heartbeat
        progress.update(len(rows), note="finished", force=True)

    ``counters`` is read, not copied, so each line shows the current values.
    """

    def __init__(
        self,
        label: str,
        total: int,
        unit: str,
        *,
        counters: Optional[Mapping[str, Any]] = None,
        telemetry: Any = None,
        every_seconds: float = 60.0,
        log: Optional[logging.Logger] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.label = label
        self.total = int(total)
        self.unit = unit
        self.counters = counters
        self.telemetry = telemetry
        self.every_seconds = float(every_seconds)
        self.log = log or logger
        self.clock = clock
        self.done = 0
        self._started = clock()
        self._last_logged = self._started

    def line(self, note: str = "") -> str:
        head = f"{self.label}: {self.done}/{self.total} {self.unit}"
        if self.total:
            head += f" ({100 * self.done // self.total}%)"
        parts = [head]
        if self.counters:
            parts.append(" ".join(f"{k}={v}" for k, v in self.counters.items()))
        t = self.telemetry
        if t is not None:
            parts.append(
                f"OpenAlex requests={getattr(t, 'total_requests', 0)} "
                f"429s={getattr(t, 'api_429_count', 0)} retries={getattr(t, 'api_retry_count', 0)}"
            )
        parts.append(f"{format_elapsed(self.clock() - self._started)} elapsed")
        if note:
            parts.append(note)
        return " | ".join(parts)

    def update(self, done: Optional[int] = None, note: str = "", force: bool = False) -> bool:
        """Record progress; log when forced or ``every_seconds`` passed since the last line. Returns whether it logged."""
        if done is not None:
            self.done = int(done)
        now = self.clock()
        if not force and now - self._last_logged < self.every_seconds:
            return False
        self._last_logged = now
        self.log.info("%s", self.line(note))
        return True
