import asyncio
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing

import pytest

from resource_pressure import PressureGovernor, PressureLevel
from resource_pressure.integrations.dask import DaskAdmission
from resource_pressure.testing import ManualBackend
from conftest import wait_for, wait_for_level


class FakeFuture:
    def __init__(self, future):
        self.inner = future
        self.released = False
        self.cancel_called = False
    def add_done_callback(self, fn): self.inner.add_done_callback(lambda f: fn(self))
    def result(self): return self.inner.result(timeout=2)
    def release(self): self.released = True
    def cancel(self):
        self.cancel_called = True
        self.inner.cancel()


class Client:
    asynchronous = False
    def __init__(self, executor=None):
        self.futures = []
        self.executor = executor
    def submit(self, fn, *args, **kwargs):
        assert kwargs.get("pure") is False
        inner = self.executor.submit(fn, *args) if self.executor else Future()
        future = FakeFuture(inner)
        self.futures.append(future)
        return future


def test_submit_holds_until_completion(normal_governor):
    governor, _ = normal_governor
    client = Client()
    adapter = DaskAdmission(client, governor)
    future = adapter.submit(lambda: 1)
    assert governor.in_flight == 1
    future.inner.set_result(1)
    assert governor.in_flight == 0


def test_cancelled_future_releases_logical_lease(normal_governor):
    governor, _ = normal_governor
    future = DaskAdmission(Client(), governor).submit(lambda: 1)
    future.cancel()
    assert governor.in_flight == 0


def test_submit_error_releases(normal_governor):
    governor, _ = normal_governor
    class BadClient:
        def submit(self, *args, **kwargs): raise ValueError("bad submit")
    with pytest.raises(ValueError):
        DaskAdmission(BadClient(), governor).submit(lambda: 1)
    assert governor.in_flight == 0


def test_asubmit_cancellation_no_submission():
    async def run():
        client = Client()
        async with PressureGovernor(ManualBackend(PressureLevel.PRESSURED)) as governor:
            task = asyncio.create_task(DaskAdmission(client, governor).asubmit(lambda: 1))
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError): await task
            assert not client.futures
            assert governor.in_flight == 0
    asyncio.run(run())


def test_map_results_released_and_bounded(normal_governor):
    governor, _ = normal_governor
    consumed = []
    def inputs():
        for i in range(30):
            consumed.append(i)
            yield i
    with ThreadPoolExecutor(max_workers=2) as pool:
        client = Client(pool)
        stream = DaskAdmission(client, governor).map_unordered(lambda i: i * i, inputs())
        first = next(stream)
        assert len(consumed) <= 3  # two admitted plus one input lookahead
        result = [first, *stream]
        assert sorted(result) == [i * i for i in range(30)]
        assert all(f.released for f in client.futures)
        assert governor.in_flight == 0


def test_map_drains_and_ends_during_pressure(normal_governor):
    governor, backend = normal_governor
    complete = threading.Event()
    def work(i):
        assert complete.wait(2)
        return i
    with ThreadPoolExecutor(max_workers=2) as workers, ThreadPoolExecutor(max_workers=1) as producer:
        client = Client(workers)
        stream = DaskAdmission(client, governor).map_unordered(work, [1, 2])
        result = producer.submit(list, stream)
        wait_for(lambda: len(client.futures) == 2)
        backend.set_level(PressureLevel.PRESSURED)
        wait_for_level(governor, PressureLevel.PRESSURED)
        complete.set()
        assert sorted(result.result(timeout=2)) == [1, 2]
        assert governor.level == PressureLevel.PRESSURED
        assert governor.in_flight == 0


def test_empty_map_does_not_wait_for_normal():
    with PressureGovernor(ManualBackend(PressureLevel.PRESSURED)) as governor:
        assert list(DaskAdmission(Client(), governor).map_unordered(str, [])) == []


def test_generator_close_cleans_pending(normal_governor):
    governor, _ = normal_governor
    with ThreadPoolExecutor(max_workers=2) as pool:
        client = Client(pool)
        stream = DaskAdmission(client, governor).map_unordered(str, range(20))
        with closing(stream):
            next(stream)
        assert all(f.released for f in client.futures)
        assert governor.in_flight == 0


def test_work_failure_cleans_all(normal_governor):
    governor, _ = normal_governor
    def fail(i): raise ValueError("worker broke")
    with ThreadPoolExecutor(max_workers=2) as pool:
        client = Client(pool)
        with pytest.raises(ValueError, match="worker broke"):
            list(DaskAdmission(client, governor).map_unordered(fail, range(4)))
        assert all(f.released for f in client.futures)
        assert governor.in_flight == 0


def test_iterable_failure_cleans_all(normal_governor):
    governor, _ = normal_governor
    def inputs():
        yield 1
        raise ValueError("input broke")
    client = Client()
    with pytest.raises(ValueError, match="input broke"):
        list(DaskAdmission(client, governor).map_unordered(str, inputs()))
    assert all(f.released for f in client.futures)
    assert governor.in_flight == 0


def test_async_client_rejected_for_sync_map(normal_governor):
    governor, _ = normal_governor
    client = Client()
    client.asynchronous = True
    with pytest.raises(TypeError):
        list(DaskAdmission(client, governor).map_unordered(str, [1]))


def test_try_submit_returns_none_without_blocking_under_pressure(normal_governor):
    governor, backend = normal_governor
    client = Client()
    adapter = DaskAdmission(client, governor)
    backend.set_level(PressureLevel.PRESSURED)
    wait_for_level(governor, PressureLevel.PRESSURED)
    assert adapter.try_submit(lambda: 1) is None
    assert not client.futures
    assert governor.in_flight == 0


def test_try_submit_managed_holds_capacity_until_explicit_release(normal_governor):
    governor, _ = normal_governor
    client = Client()
    adapter = DaskAdmission(client, governor)

    first = adapter.try_submit_managed(lambda: 1)
    second = adapter.try_submit_managed(lambda: 2)
    assert first is not None and second is not None
    assert governor.in_flight == 2

    first.future.inner.set_result(1)
    second.future.inner.set_result(2)
    # Completion alone does not reopen capacity for managed submissions.
    assert governor.in_flight == 2
    assert adapter.try_submit_managed(lambda: 3) is None

    assert first.result() == 1
    first.release()
    assert governor.in_flight == 1

    third = adapter.try_submit_managed(lambda: 3)
    assert third is not None
    assert governor.in_flight == 2
    third.cancel()
    second.release()
    assert governor.in_flight == 0
    assert first.future.released
    assert second.future.released
    assert third.future.cancel_called


def test_try_submit_raw_future_releases_on_completion(normal_governor):
    governor, _ = normal_governor
    client = Client()
    future = DaskAdmission(client, governor).try_submit(lambda: 1)
    assert future is not None
    assert governor.in_flight == 1
    future.inner.set_result(1)
    assert governor.in_flight == 0


def test_managed_submit_preserves_dask_kwargs(normal_governor):
    governor, _ = normal_governor
    seen = {}

    class RecordingClient(Client):
        def submit(self, fn, *args, **kwargs):
            seen.update(kwargs)
            return super().submit(fn, *args, **kwargs)

    submission = DaskAdmission(RecordingClient(), governor).try_submit_managed(
        str,
        1,
        key="custom-key",
        resources={"gpu": 1},
        priority=7,
        fifo_timeout="0ms",
        retries=0,
    )
    assert submission is not None
    assert seen["key"] == "custom-key"
    assert seen["resources"] == {"gpu": 1}
    assert seen["priority"] == 7
    assert seen["fifo_timeout"] == "0ms"
    assert seen["retries"] == 0
    assert seen["pure"] is False
    submission.cancel()
