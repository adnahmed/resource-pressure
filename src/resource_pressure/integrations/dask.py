"""Producer-side Dask admission using only public submit/future APIs.

This is NOT a scheduler or worker plugin. The sensor watches the producer's
host (or configured Linux PSI paths); remote worker hosts need their own policy.
Do not block worker slots on a governor: dependent tasks can otherwise deadlock.
"""
from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable, Iterable, Iterator
from typing import Any, TypeVar

from ..governor import Lease, PressureGovernor

T = TypeVar("T")
R = TypeVar("R")
log = logging.getLogger(__name__)

__all__ = ["DaskAdmission", "ManagedSubmission"]


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


class ManagedSubmission:
    """A Dask Future plus an admission lease retained until explicit release.

    This is intended for custom producer loops that keep a set/dict of active
    futures and drain results themselves. Keeping the lease until ``release()``
    means completed-but-not-consumed results still occupy admission capacity,
    preventing a fast producer from replacing them indefinitely.

    ``future`` is the ordinary Dask Future to pass to ``distributed.wait`` or
    other Dask APIs. Call ``release()`` after consuming/persisting the result, or
    ``cancel()`` during cleanup. Both methods are idempotent.
    """

    def __init__(self, future: Any, lease: Lease):
        self.future = future
        self._lease = lease
        self._released = False
        self._lock = threading.Lock()

    def result(self, *args: Any, **kwargs: Any) -> Any:
        return self.future.result(*args, **kwargs)

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            _discard(self.future, cancel=False)
        finally:
            self._lease.release()

    def cancel(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            _discard(self.future, cancel=True)
        finally:
            self._lease.release()

    def __enter__(self) -> ManagedSubmission:
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


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

    def _submit_with_lease(
        self,
        lease: Lease,
        function: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Submit and release admission when the Dask Future becomes done."""
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

    def _submit_managed_with_lease(
        self,
        lease: Lease,
        function: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> ManagedSubmission:
        """Submit while retaining admission until ManagedSubmission.release()."""
        future = None
        try:
            kwargs.setdefault("pure", False)
            future = self.client.submit(function, *args, **kwargs)
            return ManagedSubmission(future, lease)
        except BaseException:
            if future is not None:
                _discard(future, cancel=True)
            lease.release()
            raise

    def submit(
        self,
        function: Callable[..., Any],
        *args: Any,
        admission_timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Block producer admission; release the lease when the Future is done.

        For custom loops that must continue draining completions while admission
        is closed, prefer :meth:`try_submit_managed` rather than blocking here.
        """
        lease = self.governor.acquire_sync(timeout=admission_timeout)
        return self._submit_with_lease(lease, function, args, kwargs)

    def try_submit(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any | None:
        """Submit immediately if admission is available, otherwise return ``None``.

        The returned object is an ordinary Dask Future and its lease is released
        on Future completion. This is appropriate when the caller does not retain
        completed results. For a custom coordinator retaining active futures,
        use :meth:`try_submit_managed` so completed-but-undrained results continue
        to consume admission capacity.
        """
        lease = self.governor.try_acquire()
        if lease is None:
            return None
        return self._submit_with_lease(lease, function, args, kwargs)

    def submit_managed(
        self,
        function: Callable[..., Any],
        *args: Any,
        admission_timeout: float | None = None,
        **kwargs: Any,
    ) -> ManagedSubmission:
        """Block for admission and hold it until the caller explicitly releases it."""
        lease = self.governor.acquire_sync(timeout=admission_timeout)
        return self._submit_managed_with_lease(lease, function, args, kwargs)

    def try_submit_managed(
        self,
        function: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> ManagedSubmission | None:
        """Non-blocking submission for custom producer/drain loops.

        Returns ``None`` when pressure, capacity, or configured admission pacing
        currently prevents a new reservation. The caller can then drain existing
        Dask futures and retry later without sleeping inside ``submit()``.
        """
        lease = self.governor.try_acquire()
        if lease is None:
            return None
        return self._submit_managed_with_lease(lease, function, args, kwargs)

    async def asubmit(
        self,
        function: Callable[..., Any],
        *args: Any,
        admission_timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Async admission; Client.submit itself is immediate in Dask's public API."""
        lease = await self.governor.acquire(timeout=admission_timeout)
        return self._submit_with_lease(lease, function, args, kwargs)

    def map_unordered(
        self,
        function: Callable[[T], R],
        items: Iterable[T],
        **submit_kwargs: Any,
    ) -> Iterator[R]:
        """Stream unary independent tasks with bounded pending and retained results.

        Sync Dask Client only. Completed results are drained EVEN WHILE PRESSURED;
        otherwise retaining completed worker data can prevent recovery forever.
        At most max_in_flight futures plus one input lookahead are retained.
        Consume/close the iterator before closing the governor/client. On early
        exit use contextlib.closing so pending futures are cancelled/released.
        """
        if getattr(self.client, "asynchronous", False):
            raise TypeError(
                "map_unordered requires a synchronous Dask Client; use asubmit for async"
            )
        iterator = iter(items)
        completed: queue.Queue[int] = queue.Queue()
        pending: dict[int, ManagedSubmission] = {}
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
            submission = self._submit_managed_with_lease(lease, function, (item,), options.copy())
            pending[token] = submission
            try:
                # Capture token by value. The callback may run on Dask's thread
                # or synchronously for an already-completed future.
                submission.future.add_done_callback(
                    lambda finished, key=token: completed.put(key)
                )
            except BaseException:
                pending.pop(token, None)
                submission.cancel()
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
                    # No result needs draining; waiting for pressure recovery,
                    # capacity or admission pacing is safe here and propagates
                    # sensor failures/close promptly.
                    submit_one(self.governor.acquire_sync())
                    continue
                try:
                    token = completed.get(timeout=0.1)
                except queue.Empty:
                    # Local completion wait, not pressure/RAM polling. Recheck
                    # health so a dead sensor cannot strand a pending map.
                    continue
                submission = pending.pop(token)
                try:
                    # Managed admission intentionally stays held across yield so
                    # completed-but-not-consumed remote results remain bounded.
                    yield submission.result()
                finally:
                    submission.release()
        finally:
            for submission in pending.values():
                submission.cancel()
