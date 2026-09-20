"""Producer-side Dask admission using only public submit/future APIs.

This is NOT a scheduler or worker plugin. The sensor watches the producer's
host (or configured Linux PSI paths); remote worker hosts need their own policy.
Do not block worker slots on a governor: dependent tasks can otherwise deadlock.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TypeVar

from ..governor import Lease, PressureGovernor

T = TypeVar("T")
R = TypeVar("R")
log = logging.getLogger(__name__)

__all__ = ["DaskAdmission", "DaskProducer", "ManagedSubmission"]


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

    def __init__(self, future: Any, lease: Lease | None):
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
            if self._lease is not None:
                self._lease.release()

    def cancel(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        try:
            _discard(self.future, cancel=True)
        finally:
            if self._lease is not None:
                self._lease.release()

    def __enter__(self) -> ManagedSubmission:
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class _Reservation:
    def __init__(self, producer: DaskProducer, lease: Lease | None):
        self.producer, self.lease = producer, lease
        self.used = False

    def submit(self, function: Callable[..., Any], *args: Any,
               metadata: Any = None, **kwargs: Any) -> Any:
        return self.producer._submit_reserved(self, function, args, metadata, kwargs)


class DaskProducer(Mapping[Any, Any]):
    """Own pending futures, metadata and admission through result consumption.

    ``drain(futures)`` must wait for physical business cleanup; logical Future
    cancellation is insufficient. Closing stops admission, drains, then releases
    every owned Future without cancelling running work. ``admit=False`` supports
    coordination work that must not hold a business admission slot.
    Submissions may be concurrent; completion consumption and closing must have
    one owner. The submit callable must return distinct pending Futures and owns
    scheduler options (for example, pass ``pure=False`` with Client.submit).
    """

    def __init__(self, submit: Callable[..., Any], governor: PressureGovernor, *,
                 drain: Callable[[Iterable[Any]], None],
                 stop_event: threading.Event | None = None, max_pending: int | None = None):
        if max_pending is not None and (
            isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1
        ):
            raise ValueError("max_pending must be a positive integer or None")
        self.governor, self.max_pending = governor, max_pending
        self._submit, self._drain = submit, drain
        self._stop = stop_event if stop_event is not None else threading.Event()
        self._lock = threading.RLock()
        self._pending: dict[Any, tuple[ManagedSubmission, Any]] = {}
        self._reservations: set[_Reservation] = set()
        self._ready: queue.Queue[Any] = queue.Queue()
        self._closed = False

    def __len__(self) -> int:
        with self._lock:
            return len(self._pending)

    def __iter__(self) -> Iterator[Any]:
        with self._lock:
            return iter(tuple(self._pending))

    def __getitem__(self, future: Any) -> Any:
        with self._lock:
            return self._pending[future][1]

    def items(self):
        with self._lock:
            return tuple((future, entry[1]) for future, entry in self._pending.items())

    def values(self):
        with self._lock:
            return tuple(entry[1] for entry in self._pending.values())

    def update_metadata(self, future: Any, metadata: Any) -> None:
        with self._lock:
            self._pending[future] = (self._pending[future][0], metadata)

    def _stopped(self) -> bool:
        return self._closed or self._stop.is_set()

    def _release_reservation(self, reservation: _Reservation) -> None:
        with self._lock:
            if reservation not in self._reservations:
                return
            self._reservations.remove(reservation)
        if reservation.lease is not None:
            reservation.lease.release()

    @contextmanager
    def reserve(self, *, admit: bool = True) -> Iterator[_Reservation | None]:
        reservation = None
        with self._lock:
            available = self.max_pending is None or (
                len(self._pending) + len(self._reservations) < self.max_pending
            )
            if not self._stopped() and available:
                lease = self.governor.try_acquire() if admit else None
                if not admit or lease is not None:
                    reservation = _Reservation(self, lease)
                    self._reservations.add(reservation)
        try:
            yield reservation
        finally:
            if reservation is not None:
                self._release_reservation(reservation)

    def _submit_reserved(self, reservation: _Reservation, function: Callable[..., Any],
                         args: tuple[Any, ...], metadata: Any, kwargs: dict[str, Any]) -> Any:
        with self._lock:
            if self._stopped() or reservation.used or reservation not in self._reservations:
                raise RuntimeError("Reservation is closed or already submitted")
            reservation.used = True
            future = self._submit(function, *args, **kwargs)
            if future in self._pending:
                raise ValueError("submit must return distinct pending Futures")
            self._pending[future] = (ManagedSubmission(future, reservation.lease), metadata)
            self._reservations.remove(reservation)
            future.add_done_callback(self._ready.put)
            return future

    def try_submit(self, function: Callable[..., Any], *args: Any, metadata: Any = None,
                   admit: bool = True, **kwargs: Any) -> Any | None:
        with self.reserve(admit=admit) as reservation:
            return None if reservation is None else reservation.submit(
                function, *args, metadata=metadata, **kwargs
            )

    def _release(self, future: Any) -> None:
        with self._lock:
            entry = self._pending.pop(future, None)
        if entry is not None:
            entry[0].release()

    def completed(self, *, timeout: float = 0.0) -> Iterator[tuple[Any, Any]]:
        """Wait for a completion, then drain ready results; hold leases across yield."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                future = self._ready.get(timeout=max(0.0, min(0.2, deadline - time.monotonic())))
            except queue.Empty:
                if self._stopped():
                    return
                self.governor.check_health()
                if time.monotonic() >= deadline:
                    return
                continue
            with self._lock:
                entry = self._pending.get(future)
            if entry is not None:
                try:
                    yield future, entry[1]
                finally:
                    self._release(future)
                deadline = 0.0

    def refill(self, function: Callable[[T], R], select: Callable[[int], Iterable[T]], *,
               on_submit: Callable[[T], None] | None = None, **kwargs: Any
               ) -> Iterator[tuple[Any, T]]:
        """Select bounded batches, retaining denied candidates until admission resumes.

        ``select(count)`` returns at most count candidates. Empty selection is
        temporary while work is pending. Selection errors propagate after already
        accepted results have been yielded; ``on_submit(item)`` runs only on acceptance.
        """
        candidates: deque[T] = deque()
        error: Exception | None = None
        limit = self.max_pending or self.governor.max_in_flight
        while True:
            if self._stopped():
                return
            if error is None:
                try:
                    capacity = limit - len(self)
                    if capacity > 0 and not candidates:
                        candidates.extend(select(capacity))
                    while candidates and len(self) < limit:
                        item = candidates[0]
                        if self.try_submit(function, item, metadata=item, **kwargs) is None:
                            break
                        candidates.popleft()
                        if on_submit is not None:
                            on_submit(item)
                except Exception as exc:
                    error = exc
            if self:
                for completed in self.completed(timeout=0.2):
                    if self._stopped():
                        return
                    yield completed
            elif error is not None:
                raise error
            elif not candidates:
                return
            else:
                self.governor.check_health()
                self._stop.wait(0.2)

    def drain(self) -> None:
        self._drain(tuple(self))

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            self.drain()
        finally:
            with self._lock:
                reservations = tuple(self._reservations)
            for items, release in ((reservations, self._release_reservation),
                                   (tuple(self), self._release)):
                for item in items:
                    try:
                        release(item)
                    except BaseException:
                        log.exception("Could not release producer ownership during cleanup")

    def __enter__(self) -> DaskProducer:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


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
