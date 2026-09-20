"""Admission control; no RAM percentages, worker-memory estimates, or AIMD."""
from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

from .backends import PSIConfig, auto_backend
from .backends.base import PressureBackend
from .model import (
    BackendUnavailable, GovernorClosed, MonitorFailed, PressureEvent, PressureLevel,
)

log = logging.getLogger(__name__)


def _deadline(timeout: float | None) -> float | None:
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be finite and non-negative, or None")
    return time.monotonic() + timeout


def _remaining(deadline: float | None) -> float | None:
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def _notify(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


class Lease:
    """One admitted operation. Release after it completes, not after submitting it."""

    def __init__(self, governor: PressureGovernor):
        self._governor = governor
        self._released = False
        self._lock = threading.Lock()

    def release(self) -> None:
        self._governor._check_pid()
        with self._lock:
            if not self._released:
                self._released = True
                self._governor._release()

    def __enter__(self) -> Lease:
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()

    async def __aenter__(self) -> Lease:
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.release()


class _Slot:
    def __init__(self, governor: PressureGovernor, timeout: float | None):
        self._governor = governor
        self._timeout = timeout
        self._used = False
        self._lease: Lease | None = None

    def _enter_once(self) -> None:
        if self._used:
            raise RuntimeError("A slot context is single-use; call governor.slot() again")
        self._used = True

    def __enter__(self) -> Lease:
        self._enter_once()
        self._lease = self._governor.acquire_sync(timeout=self._timeout)
        return self._lease

    def __exit__(self, *args: Any) -> None:
        if self._lease is not None:
            self._lease.release()

    async def __aenter__(self) -> Lease:
        self._enter_once()
        self._lease = await self._governor.acquire(timeout=self._timeout)
        return self._lease

    async def __aexit__(self, *args: Any) -> None:
        if self._lease is not None:
            self._lease.release()


class PressureGovernor:
    """A process-local admission gate driven by native pressure notifications.

    NORMAL admits up to max_in_flight operations. UNKNOWN, PRESSURED and
    CRITICAL block new admission. Existing leases are never revoked. Async
    waiting uses loop futures, not a blocked thread per waiter. Fairness is
    best-effort, not FIFO. Create a new governor in each spawned process.
    """

    def __init__(
        self,
        backend: PressureBackend,
        *,
        max_in_flight: int | None = None,
        min_admission_interval: float = 0.0,
    ):
        if max_in_flight is None:
            cpu_count = getattr(os, "process_cpu_count", os.cpu_count)
            max_in_flight = max(1, cpu_count() or 1)
        if (
            isinstance(max_in_flight, bool)
            or not isinstance(max_in_flight, int)
            or max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be a positive integer")
        if (
            isinstance(min_admission_interval, bool)
            or not isinstance(min_admission_interval, (int, float))
            or not math.isfinite(min_admission_interval)
            or min_admission_interval < 0
        ):
            raise ValueError("min_admission_interval must be finite and non-negative")
        self.backend = backend
        self.max_in_flight = max_in_flight
        self.min_admission_interval = float(min_admission_interval)
        self._pid = os.getpid()
        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._closed = False
        self._error: BaseException | None = None
        self._in_flight = 0
        self._last_admission_at: float | None = None
        self._event = PressureEvent(PressureLevel.UNKNOWN, backend.name, "not started")
        self._async_waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = set()
        self._callbacks: set[Callable[[PressureEvent], None]] = set()

    @classmethod
    def auto(
        cls,
        *,
        max_in_flight: int | None = None,
        min_admission_interval: float = 0.0,
        psi: PSIConfig | None = None,
        psi_paths: Sequence[str] | None = None,
    ) -> PressureGovernor:
        """Select the OS backend. Start via with/async with or start()/astart().

        ``min_admission_interval`` optionally paces new admissions. It is not a
        memory estimate or adaptive worker-count algorithm; it simply limits how
        quickly a large configured ceiling can be filled while native pressure
        notification is still catching up with new allocations.
        """
        return cls(
            auto_backend(psi=psi, psi_paths=psi_paths),
            max_in_flight=max_in_flight,
            min_admission_interval=min_admission_interval,
        )

    def _check_pid(self) -> None:
        if os.getpid() != self._pid:
            raise GovernorClosed("Do not reuse a governor after fork; construct one in the child")

    def _check_locked(self) -> None:
        if self._closed:
            raise GovernorClosed("Governor is closed")
        if self._error is not None:
            if isinstance(self._error, BackendUnavailable):
                raise BackendUnavailable(str(self._error)) from self._error
            raise MonitorFailed(f"{self.backend.name}: {self._error}") from self._error
        if self._thread is None:
            raise RuntimeError("Start the governor with a context manager or start() first")

    def _wake_locked(self) -> None:
        self._cv.notify_all()
        for loop, future in tuple(self._async_waiters):
            try:
                loop.call_soon_threadsafe(_notify, future)
            except RuntimeError:  # A caller abandoned/closed its event loop.
                self._async_waiters.discard((loop, future))

    def _emit(self, event: PressureEvent) -> None:
        if not isinstance(event.level, PressureLevel):
            raise TypeError("Backends must emit a PressureLevel")
        with self._cv:
            if self._closed:
                return
            changed = event.level != self._event.level
            self._event = event
            self._wake_locked()
            callbacks = tuple(self._callbacks) if changed else ()
        for callback in callbacks:
            try:
                callback(event)
            except Exception:
                log.exception("Pressure subscriber failed")

    def _run(self) -> None:
        try:
            self.backend.run(self._stop, self._emit, self._ready.set)
            if not self._stop.is_set():
                raise RuntimeError("Native monitor exited unexpectedly")
        except BaseException as exc:
            with self._cv:
                self._error = exc
                self._event = PressureEvent(PressureLevel.UNKNOWN, self.backend.name,
                                            f"monitor failed: {exc}")
                self._wake_locked()
        finally:
            self._ready.set()

    def _launch(self) -> None:
        self._check_pid()
        with self._cv:
            if self._closed:
                raise GovernorClosed("A closed governor cannot be restarted")
            if self._thread is None:
                self._thread = threading.Thread(target=self._run,
                                                name=f"resource-pressure:{self.backend.name}",
                                                daemon=True)
                self._thread.start()

    def start(self, *, timeout: float = 5.0) -> PressureGovernor:
        _deadline(timeout)
        self._launch()
        if not self._ready.wait(timeout):
            self.close()
            raise BackendUnavailable("Native monitor initialization timed out")
        with self._cv:
            self._check_locked()
        return self

    async def astart(self, *, timeout: float = 5.0) -> PressureGovernor:
        # Only initialization joins a helper thread; admission never does.
        try:
            return await asyncio.to_thread(self.start, timeout=timeout)
        except BaseException:
            self._request_close()
            raise

    def _request_close(self) -> threading.Thread | None:
        self._check_pid()
        with self._cv:
            self._closed = True
            self._stop.set()
            self._wake_locked()
            return self._thread

    def close(self) -> None:
        thread = self._request_close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise MonitorFailed("Monitor did not stop; its thread still owns native handles")

    async def aclose(self) -> None:
        self._request_close()  # Close admission even if the awaiting task is cancelled.
        await asyncio.to_thread(self.close)

    def __enter__(self) -> PressureGovernor:
        return self.start()

    def __exit__(self, *args: Any) -> None:
        self.close()

    async def __aenter__(self) -> PressureGovernor:
        return await self.astart()

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    @property
    def event(self) -> PressureEvent:
        self._check_pid()
        with self._cv:
            return self._event

    @property
    def level(self) -> PressureLevel:
        return self.event.level

    @property
    def in_flight(self) -> int:
        self._check_pid()
        with self._cv:
            return self._in_flight

    def check_health(self) -> None:
        self._check_pid()
        with self._cv:
            self._check_locked()

    def subscribe(self, callback: Callable[[PressureEvent], None]) -> Callable[[], None]:
        """Subscribe to transitions; callback runs on monitor thread, must not block.

        Returns an unsubscribe function. No initial replay. Callback failures are
        logged and isolated; they do not grant admission or stop the monitor.
        """
        self._check_pid()
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._cv:
            if self._closed:
                raise GovernorClosed("Governor is closed")
            self._callbacks.add(callback)

        def unsubscribe() -> None:
            self._check_pid()
            with self._cv:
                self._callbacks.discard(callback)
        return unsubscribe

    def _pacing_delay_locked(self) -> float:
        if self.min_admission_interval == 0.0 or self._last_admission_at is None:
            return 0.0
        return max(
            0.0,
            self._last_admission_at + self.min_admission_interval - time.monotonic(),
        )

    def _can_admit(self, reserve: bool) -> bool:
        if self._event.level != PressureLevel.NORMAL:
            return False
        if not reserve:
            return True
        return (
            self._in_flight < self.max_in_flight
            and self._pacing_delay_locked() == 0.0
        )

    def _take_locked(self) -> Lease:
        self._in_flight += 1
        self._last_admission_at = time.monotonic()
        return Lease(self)

    def _next_wake_locked(self, reserve: bool, remaining: float | None) -> float | None:
        """Return a condition/async wait timeout, accounting for admission pacing."""
        wait_for = remaining
        if (
            reserve
            and self._event.level == PressureLevel.NORMAL
            and self._in_flight < self.max_in_flight
        ):
            pacing = self._pacing_delay_locked()
            if pacing > 0.0:
                wait_for = pacing if wait_for is None else min(wait_for, pacing)
        return wait_for

    def try_acquire(self) -> Lease | None:
        """Atomically reserve a slot, or return None without waiting."""
        self._check_pid()
        with self._cv:
            self._check_locked()
            return self._take_locked() if self._can_admit(True) else None

    def _wait_sync(self, reserve: bool, timeout: float | None) -> Lease | None:
        self._check_pid()
        deadline = _deadline(timeout)
        with self._cv:
            while True:
                self._check_locked()
                if self._can_admit(reserve):
                    return self._take_locked() if reserve else None
                remaining = _remaining(deadline)
                if remaining == 0:
                    raise TimeoutError("Timed out waiting for memory-pressure admission")
                self._cv.wait(self._next_wake_locked(reserve, remaining))

    async def _wait_async(self, reserve: bool, timeout: float | None) -> Lease | None:
        self._check_pid()
        deadline = _deadline(timeout)
        loop = asyncio.get_running_loop()
        while True:
            with self._cv:
                self._check_locked()
                if self._can_admit(reserve):
                    return self._take_locked() if reserve else None
                remaining = _remaining(deadline)
                if remaining == 0:
                    raise TimeoutError("Timed out waiting for memory-pressure admission")
                wait_for = self._next_wake_locked(reserve, remaining)
                future: asyncio.Future[None] = loop.create_future()
                waiter = (loop, future)
                self._async_waiters.add(waiter)
            try:
                try:
                    if wait_for is None:
                        await future
                    else:
                        await asyncio.wait_for(future, wait_for)
                except asyncio.TimeoutError:
                    # A pacing timer can expire without an OS event or lease
                    # release. Recheck state under the condition lock. The next
                    # iteration enforces the caller's overall deadline.
                    pass
            finally:
                with self._cv:
                    self._async_waiters.discard(waiter)
                if not future.done():
                    future.cancel()

    def acquire_sync(self, *, timeout: float | None = None) -> Lease:
        result = self._wait_sync(True, timeout)
        assert result is not None
        return result

    async def acquire(self, *, timeout: float | None = None) -> Lease:
        result = await self._wait_async(True, timeout)
        assert result is not None
        return result

    def wait_until_safe_sync(self, *, timeout: float | None = None) -> None:
        """Observe NORMAL only. Does not reserve a concurrency slot."""
        self._wait_sync(False, timeout)

    async def wait_until_safe(self, *, timeout: float | None = None) -> None:
        """Observe NORMAL only. Prefer slot() when admitting actual work."""
        await self._wait_async(False, timeout)

    def slot(self, *, timeout: float | None = None) -> _Slot:
        """Use with or async with; holds admission until the block exits."""
        return _Slot(self, timeout)

    def _release(self) -> None:
        with self._cv:
            self._in_flight -= 1
            self._wake_locked()

    def process_group(self, *, max_memory: int | None = None,
                      max_processes: int | None = None,
                      allow_process_group_fallback: bool = False) -> Any:
        """Optional governed process runner; containment extra required."""
        from .integrations.processkit import GovernedProcessGroup
        return GovernedProcessGroup(self, max_memory=max_memory, max_processes=max_processes,
                                    allow_process_group_fallback=allow_process_group_fallback)
