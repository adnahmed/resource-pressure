import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from resource_pressure import (
    BackendUnavailable, GovernorClosed, MonitorFailed, PressureGovernor, PressureLevel,
)
from resource_pressure.testing import ManualBackend
from conftest import wait_for, wait_for_level


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "2"])
def test_invalid_limits(limit):
    with pytest.raises(ValueError):
        PressureGovernor(ManualBackend(), max_in_flight=limit)


def test_default_limit_positive():
    assert PressureGovernor(ManualBackend()).max_in_flight >= 1


def test_requires_start():
    governor = PressureGovernor(ManualBackend())
    with pytest.raises(RuntimeError, match="Start"):
        governor.try_acquire()
    governor.close()


def test_capacity_and_idempotent_release(normal_governor):
    governor, _ = normal_governor
    a = governor.try_acquire()
    b = governor.try_acquire()
    assert a and b
    assert governor.try_acquire() is None
    assert governor.in_flight == 2
    a.release()
    a.release()
    assert governor.in_flight == 1
    b.release()
    assert governor.in_flight == 0


@pytest.mark.parametrize("level", [PressureLevel.UNKNOWN, PressureLevel.PRESSURED, PressureLevel.CRITICAL])
def test_pressure_blocks_but_does_not_revoke(normal_governor, level):
    governor, backend = normal_governor
    existing = governor.acquire_sync()
    backend.set_level(level)
    wait_for_level(governor, level)
    assert governor.try_acquire() is None
    assert governor.in_flight == 1
    with pytest.raises(TimeoutError):
        governor.acquire_sync(timeout=0.01)
    existing.release()
    assert governor.in_flight == 0
    backend.set_level(PressureLevel.NORMAL)
    with governor.slot(timeout=1):
        assert governor.in_flight == 1


def test_wait_until_safe_is_not_a_lease(normal_governor):
    governor, _ = normal_governor
    a, b = governor.acquire_sync(), governor.acquire_sync()
    governor.wait_until_safe_sync(timeout=0)
    assert governor.in_flight == 2
    a.release()
    b.release()


@pytest.mark.parametrize("timeout", [-1, float("inf"), float("nan"), True])
def test_invalid_timeout(normal_governor, timeout):
    governor, _ = normal_governor
    with pytest.raises(ValueError):
        governor.acquire_sync(timeout=timeout)


def test_slot_exception_releases(normal_governor):
    governor, _ = normal_governor
    with pytest.raises(ValueError):
        with governor.slot():
            raise ValueError("task failed")
    assert governor.in_flight == 0


def test_slot_is_single_use(normal_governor):
    governor, _ = normal_governor
    slot = governor.slot()
    with slot:
        pass
    with pytest.raises(RuntimeError):
        with slot:
            pass


def test_close_wakes_blocked_thread():
    governor = PressureGovernor(ManualBackend(PressureLevel.PRESSURED)).start()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(governor.acquire_sync)
        time.sleep(0.01)
        governor.close()
        with pytest.raises(GovernorClosed):
            future.result(timeout=1)
    governor.close()
    with pytest.raises(GovernorClosed):
        governor.start()


def test_sensor_failure_wakes_and_fails_closed(normal_governor):
    governor, backend = normal_governor
    backend.set_level(PressureLevel.PRESSURED)
    wait_for_level(governor, PressureLevel.PRESSURED)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(governor.acquire_sync)
        backend.fail(OSError("sensor vanished"))
        with pytest.raises(MonitorFailed, match="sensor vanished"):
            future.result(timeout=1)
    assert governor.level == PressureLevel.UNKNOWN
    with pytest.raises(MonitorFailed):
        governor.try_acquire()


def test_unavailable_initialization():
    class Missing:
        name = "missing"
        def run(self, stop, emit, ready):
            raise BackendUnavailable("no sensor")
    with pytest.raises(BackendUnavailable, match="no sensor"):
        with PressureGovernor(Missing()):
            pass


def test_unexpected_exit_is_failure():
    class Exits:
        name = "exits"
        def run(self, stop, emit, ready):
            return
    with pytest.raises(MonitorFailed, match="unexpectedly"):
        PressureGovernor(Exits()).start()


def test_subscriber_isolation_and_unsubscribe(normal_governor, caplog):
    governor, backend = normal_governor
    seen = []
    unsubscribe = governor.subscribe(lambda event: seen.append(event.level))
    def bad_callback(event):
        raise ValueError("subscriber error")
    stop_bad = governor.subscribe(bad_callback)
    backend.set_level(PressureLevel.PRESSURED)
    wait_for(lambda: len(seen) == 1)
    assert seen == [PressureLevel.PRESSURED]
    stop_bad()
    unsubscribe()
    backend.set_level(PressureLevel.NORMAL)
    governor.wait_until_safe_sync(timeout=1)
    assert len(seen) == 1
    assert "subscriber error" in caplog.text


def test_fork_guard(normal_governor, monkeypatch):
    governor, _ = normal_governor
    import resource_pressure.governor as module
    with monkeypatch.context() as patch:
        patch.setattr(module.os, "getpid", lambda: governor._pid + 1)
        with pytest.raises(GovernorClosed, match="fork"):
            governor.try_acquire()


def test_thread_concurrency_bound():
    with PressureGovernor(ManualBackend(), max_in_flight=3) as governor:
        current = peak = 0
        lock = threading.Lock()
        def work(i):
            nonlocal current, peak
            with governor.slot(timeout=3):
                with lock:
                    current += 1
                    peak = max(peak, current)
                time.sleep(0.003)
                with lock:
                    current -= 1
            return i
        with ThreadPoolExecutor(max_workers=12) as pool:
            assert list(pool.map(work, range(60))) == list(range(60))
        assert peak <= 3
        assert peak > 1
        assert governor.in_flight == 0


def test_async_cancellation_and_recovery():
    async def run():
        backend = ManualBackend(PressureLevel.PRESSURED)
        async with PressureGovernor(backend, max_in_flight=1) as governor:
            waiter = asyncio.create_task(governor.acquire())
            await asyncio.sleep(0.01)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert governor.in_flight == 0
            assert not governor._async_waiters
            backend.set_level(PressureLevel.NORMAL)
            async with governor.slot(timeout=1):
                assert governor.in_flight == 1
            assert governor.in_flight == 0
    asyncio.run(run())


def test_async_timeout_no_leak():
    async def run():
        async with PressureGovernor(ManualBackend(PressureLevel.CRITICAL)) as governor:
            with pytest.raises(TimeoutError):
                await governor.acquire(timeout=0.01)
            assert not governor._async_waiters
            assert governor.in_flight == 0
    asyncio.run(run())


def test_async_close_wakes():
    async def run():
        governor = await PressureGovernor(ManualBackend(PressureLevel.PRESSURED)).astart()
        waiter = asyncio.create_task(governor.wait_until_safe())
        await asyncio.sleep(0.01)
        await governor.aclose()
        with pytest.raises(GovernorClosed):
            await waiter
    asyncio.run(run())


def test_async_running_cancel_releases():
    async def run():
        async with PressureGovernor(ManualBackend(), max_in_flight=1) as governor:
            entered = asyncio.Event()
            async def work():
                async with governor.slot():
                    entered.set()
                    await asyncio.sleep(100)
            task = asyncio.create_task(work())
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert governor.in_flight == 0
    asyncio.run(run())


def test_many_async_waiters_stay_bounded():
    async def run():
        async with PressureGovernor(ManualBackend(), max_in_flight=3) as governor:
            peak = 0
            async def work():
                nonlocal peak
                async with governor.slot():
                    peak = max(peak, governor.in_flight)
                    await asyncio.sleep(0.001)
            await asyncio.gather(*(work() for _ in range(100)))
            assert peak == 3
            assert governor.in_flight == 0
            assert not governor._async_waiters
    asyncio.run(run())


@pytest.mark.parametrize("interval", [-1, float("inf"), float("nan"), True, "0.1"])
def test_invalid_admission_interval(interval):
    with pytest.raises(ValueError):
        PressureGovernor(ManualBackend(), min_admission_interval=interval)


def test_admission_pacing_limits_immediate_burst():
    with PressureGovernor(
        ManualBackend(), max_in_flight=3, min_admission_interval=0.04
    ) as governor:
        first = governor.try_acquire()
        assert first is not None
        first.release()
        assert governor.try_acquire() is None
        time.sleep(0.05)
        second = governor.try_acquire()
        assert second is not None
        second.release()


def test_blocking_acquire_wakes_when_pacing_interval_expires():
    with PressureGovernor(
        ManualBackend(), max_in_flight=2, min_admission_interval=0.03
    ) as governor:
        first = governor.acquire_sync()
        first.release()
        started = time.monotonic()
        second = governor.acquire_sync(timeout=1)
        elapsed = time.monotonic() - started
        second.release()
        assert elapsed >= 0.02


def test_async_acquire_wakes_when_pacing_interval_expires():
    async def run():
        async with PressureGovernor(
            ManualBackend(), max_in_flight=2, min_admission_interval=0.03
        ) as governor:
            first = await governor.acquire()
            first.release()
            started = time.monotonic()
            second = await governor.acquire(timeout=1)
            elapsed = time.monotonic() - started
            second.release()
            assert elapsed >= 0.02
            assert not governor._async_waiters

    asyncio.run(run())
