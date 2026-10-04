"""
Progress lines for long paper-mapping batch tasks.

A citation batch can run for 20+ minutes (a primary paper with 1,000+ citing
papers, each fetched and its full text cached) and used to log nothing until it
returned, so a working batch looked the same as a hung one. ``BatchProgress``
logs a line when each item starts and a heartbeat at most every
``every_seconds`` inside it, carrying the batch's counters, API request
and 429 counts, and elapsed time. ``ticking()`` keeps the heartbeat going
through one long call that has no loop of its own to report from.
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Mapping, Optional

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
    ``telemetry`` is a find_reuse_core.Telemetry or a dict with the same keys;
    ``requests_label`` says whose requests it counts ("API" when the step also
    calls Crossref and DataCite).
    """

    def __init__(
        self,
        label: str,
        total: int,
        unit: str,
        *,
        counters: Optional[Mapping[str, Any]] = None,
        telemetry: Any = None,
        requests_label: str = "OpenAlex",
        every_seconds: float = 60.0,
        log: Optional[logging.Logger] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.label = label
        self.total = int(total)
        self.unit = unit
        self.counters = counters
        self.telemetry = telemetry
        self.requests_label = requests_label
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
            def count(key: str) -> Any:
                return t.get(key, 0) if isinstance(t, Mapping) else getattr(t, key, 0)

            parts.append(
                f"{self.requests_label} requests={count('total_requests')} "
                f"429s={count('api_429_count')} retries={count('api_retry_count')}"
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

    @contextmanager
    def ticking(self, note: str) -> Iterator["BatchProgress"]:
        """
        Log a heartbeat every ``every_seconds`` while the block runs: for one
        long call, such as resolving a dataset's papers through retries, that
        has no loop of its own to call update() from.
        """
        stop = threading.Event()

        def beat() -> None:
            while not stop.wait(self.every_seconds):
                self.update(note=f"{note}: still working", force=True)

        thread = threading.Thread(target=beat, name=f"progress-{self.label}", daemon=True)
        thread.start()
        try:
            yield self
        finally:
            stop.set()
            thread.join()
