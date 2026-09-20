"""Producer-side Dask admission using only public submit/future APIs.

This is NOT a scheduler or worker plugin. The sensor watches the producer's
host (or configured Linux PSI paths); remote worker hosts need their own policy.
Do not block worker slots on a governor: dependent tasks can otherwise deadlock.
"""
from __future__ import annotations

import logging
import queue
from collections.abc import Callable, Iterable, Iterator
from typing import Any, TypeVar

from ..governor import Lease, PressureGovernor

T = TypeVar("T")
R = TypeVar("R")
log = logging.getLogger(__name__)


def _discard(future: Any, *, cancel: bool) -> None:
    try:
        if cancel:
            future.cancel()
    except Exception:
        log.exception("Could not cancel a Dask future during cleanup")
    finally:
        try:
            future.release()
        except Exception:
            log.exception("Could not release a Dask future during cleanup")


class DaskAdmission:
    """Bound admitted futures, not Dask's number of workers or physical processes.

    Future cancellation is logical: already executing Python work may continue.
    Completion is not proof that remote buffers were freed. Do not retain every
    result/future indefinitely in a memory-sensitive application.
    """
    def __init__(self, client: Any, governor: PressureGovernor):
        if not callable(getattr(client, "submit", None)):
            raise TypeError("client must expose Dask's public submit API")
        self.client = client
        self.governor = governor

    def _submit_with_lease(self, lease: Lease, function: Callable[..., Any],
                           args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        future = None
        try:
            kwargs.setdefault("pure", False)
            future = self.client.submit(function, *args, **kwargs)
            future.add_done_callback(lambda finished: lease.release())
            return future
        except BaseException:
            if future is not None:
                _discard(future, cancel=True)
            lease.release()
            raise

    def submit(self, function: Callable[..., Any], *args: Any,
               admission_timeout: float | None = None, **kwargs: Any) -> Any:
        """Blocks producer admission; returns a normal distributed.Future."""
        lease = self.governor.acquire_sync(timeout=admission_timeout)
        return self._submit_with_lease(lease, function, args, kwargs)

    async def asubmit(self, function: Callable[..., Any], *args: Any,
                      admission_timeout: float | None = None, **kwargs: Any) -> Any:
        """Async admission; Client.submit itself is immediate in Dask's public API."""
        lease = await self.governor.acquire(timeout=admission_timeout)
        return self._submit_with_lease(lease, function, args, kwargs)

    def map_unordered(self, function: Callable[[T], R], items: Iterable[T],
                      **submit_kwargs: Any) -> Iterator[R]:
        """Stream unary independent tasks with bounded pending and retained results.

        Sync Dask Client only. Completed results are drained EVEN WHILE PRESSURED;
        otherwise retaining completed worker data can prevent recovery forever.
        At most max_in_flight futures plus one input lookahead are retained.
        Consume/close the iterator before closing the governor/client. On early
        exit use contextlib.closing so pending futures are cancelled/released.
        """
        if getattr(self.client, "asynchronous", False):
            raise TypeError("map_unordered requires a synchronous Dask Client; use asubmit for async")
        iterator = iter(items)
        completed: queue.Queue[int] = queue.Queue()
        pending: dict[int, tuple[Any, Lease]] = {}
        exhausted = False
        missing = object()
        lookahead: Any = missing
        sequence = 0
        options = dict(submit_kwargs)
        options.setdefault("pure", False)

        def prefetch() -> None:
            nonlocal exhausted, lookahead
            if lookahead is missing and not exhausted:
                try:
                    lookahead = next(iterator)
                except StopIteration:
                    exhausted = True

        def submit_one(lease: Lease) -> None:
            nonlocal lookahead, sequence
            item = lookahead
            lookahead = missing
            token = sequence
            sequence += 1
            future = None
            try:
                future = self.client.submit(function, item, **options)
                pending[token] = (future, lease)
                # Capture token by value. The callback may run on Dask's thread
                # or synchronously for an already-completed future.
                future.add_done_callback(lambda finished, key=token: completed.put(key))
            except BaseException:
                pending.pop(token, None)
                if future is not None:
                    _discard(future, cancel=True)
                lease.release()
                raise

        try:
            while True:
                self.governor.check_health()
                prefetch()  # Detect empty/end-of-input even while pressure is latched.
                while not exhausted and len(pending) < self.governor.max_in_flight:
                    lease = self.governor.try_acquire()
                    if lease is None:
                        break
                    submit_one(lease)
                    prefetch()
                if not pending:
                    if exhausted:
                        return
                    # No result needs draining; waiting for pressure recovery is
                    # safe here and propagates sensor failures/close promptly.
                    submit_one(self.governor.acquire_sync())
                    continue
                try:
                    token = completed.get(timeout=0.1)
                except queue.Empty:
                    # Local completion wait, not pressure/RAM polling. Recheck
                    # health so a dead sensor cannot strand a pending map.
                    continue
                future, lease = pending.pop(token)
                try:
                    # Holding the lease across yield bounds completed-but-not-
                    # consumed results too. Release the remote Future afterward.
                    yield future.result()
                finally:
                    _discard(future, cancel=False)
                    lease.release()
        finally:
            for future, lease in pending.values():
                _discard(future, cancel=True)
                lease.release()
