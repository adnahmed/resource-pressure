import errno
import queue
import sys
import threading

import pytest

from resource_pressure import BackendUnavailable, PSIConfig, PressureLevel
from resource_pressure.backends import auto_backend
from resource_pressure.backends.linux import LinuxPSIBackend, _PSIState
from resource_pressure.backends.macos import MacOSMemoryBackend, _map_flags
from resource_pressure.backends.windows import WindowsMemoryBackend, _map_state


@pytest.mark.parametrize("kwargs", [
    {"some_stall_us": 0}, {"full_stall_us": -1}, {"window_us": 1_000_000},
    {"window_us": 12_000_000}, {"some_stall_us": 3_000_000},
    {"full_stall_us": True}, {"quiet_seconds": 3}, {"quiet_seconds": float("inf")},
    {"window_us": 2_000_000.0}, {"quiet_seconds": float("nan")},
])
def test_invalid_psi_policy(kwargs):
    with pytest.raises(ValueError):
        PSIConfig(**kwargs)


def test_psi_policy_recovery_and_latched_critical():
    state = _PSIState(PSIConfig(), 0)
    assert state.level(0) == PressureLevel.UNKNOWN
    assert state.level(2) == PressureLevel.NORMAL
    state.trigger(PressureLevel.PRESSURED, 3)
    assert state.level(3.1) == PressureLevel.PRESSURED
    state.trigger(PressureLevel.CRITICAL, 4)
    state.trigger(PressureLevel.PRESSURED, 6)
    assert state.level(7.9) == PressureLevel.CRITICAL
    assert state.level(8.1) == PressureLevel.PRESSURED
    assert state.level(10.1) == PressureLevel.NORMAL


def test_psi_repeated_event_extends_quiet_period():
    state = _PSIState(PSIConfig(), 0)
    state.trigger(PressureLevel.CRITICAL, 3)
    state.trigger(PressureLevel.CRITICAL, 6)
    assert state.level(9) == PressureLevel.CRITICAL
    assert state.level(10.01) == PressureLevel.NORMAL


@pytest.mark.parametrize("paths", [[], "memory.pressure"])
def test_invalid_psi_paths(paths):
    with pytest.raises((ValueError, TypeError)):
        LinuxPSIBackend(paths=paths)


def test_psi_deduplicates_scopes():
    assert LinuxPSIBackend(paths=["a", "b", "a"]).paths == ("a", "b")


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="select.poll is Linux/Unix")
def test_linux_real_control_flow_with_fake_kernel(monkeypatch):
    import resource_pressure.backends.linux as module
    stop = threading.Event()
    opened, writes, closed, registered, emitted = [], [], [], [], []
    clock = [0.0]
    def fake_open(path, flags):
        fd = 100 + len(opened)
        opened.append((path, flags))
        return fd
    class Poll:
        def __init__(self): self.i = 0
        def register(self, fd, flags): registered.append((fd, flags))
        def poll(self, timeout):
            self.i += 1
            clock[0] = {1: 0.1, 2: 0.2, 3: 4.3, 4: 4.4}[self.i]
            if self.i == 1: return [(100, module.select.POLLPRI)]
            if self.i == 2: return [(101, module.select.POLLPRI)]
            if self.i == 4: stop.set()
            return []
    monkeypatch.setattr(module.os, "open", fake_open)
    monkeypatch.setattr(module.os, "write", lambda fd, data: (writes.append((fd, data)), len(data))[1])
    monkeypatch.setattr(module.os, "close", closed.append)
    monkeypatch.setattr(module.select, "poll", Poll)
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    ready = []
    LinuxPSIBackend().run(stop, emitted.append, lambda: ready.append(True))
    assert ready == [True]
    assert [e.level for e in emitted] == [PressureLevel.UNKNOWN, PressureLevel.PRESSURED,
                                         PressureLevel.CRITICAL, PressureLevel.NORMAL]
    assert writes == [(100, b"some 150000 2000000\0"), (101, b"full 50000 2000000\0")]
    assert closed == [101, 100]
    assert len(registered) == 2


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux-specific I/O")
def test_linux_partial_initialization_closes_fd(monkeypatch):
    import resource_pressure.backends.linux as module
    closed = []
    monkeypatch.setattr(module.os, "open", lambda *a: 45)
    def deny(*a): raise PermissionError(errno.EACCES, "denied")
    monkeypatch.setattr(module.os, "write", deny)
    monkeypatch.setattr(module.os, "close", closed.append)
    with pytest.raises(BackendUnavailable, match="write permission"):
        LinuxPSIBackend().run(threading.Event(), lambda e: None, lambda: None)
    assert closed == [45]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux-specific I/O")
def test_linux_missing_source_is_not_normal(tmp_path):
    events = []
    with pytest.raises(BackendUnavailable):
        LinuxPSIBackend(paths=[str(tmp_path / "no-such-psi")]).run(
            threading.Event(), events.append, lambda: None)
    assert not events


@pytest.mark.parametrize("low,high,previous,expected", [
    (True, False, PressureLevel.NORMAL, PressureLevel.PRESSURED),
    (False, True, PressureLevel.PRESSURED, PressureLevel.NORMAL),
    (True, True, PressureLevel.NORMAL, PressureLevel.PRESSURED),
    (False, False, PressureLevel.PRESSURED, PressureLevel.PRESSURED),
    (False, False, PressureLevel.NORMAL, PressureLevel.NORMAL),
    (False, False, PressureLevel.UNKNOWN, PressureLevel.PRESSURED),
])
def test_windows_state_semantics(low, high, previous, expected):
    assert _map_state(low, high, previous) == expected


def test_windows_waits_only_on_opposite_condition(monkeypatch):
    import resource_pressure.backends.windows as module
    stop = threading.Event()
    states = [(False, True), (True, False), (False, True)]
    waits, closes, events = [], [], []
    class API:
        def __init__(self): self.index = 0
        def create(self, kind): return kind + 10
        def query(self, handle): return states[self.index][handle - 10]
        def wait(self, handle, timeout):
            waits.append(handle)
            self.index += 1
            if self.index >= len(states):
                stop.set()
                return False
            return True
        def close(self, handle): closes.append(handle)
    monkeypatch.setattr(module.sys, "platform", "win32")
    monkeypatch.setattr(module, "_WindowsAPI", API)
    WindowsMemoryBackend().run(stop, events.append, lambda: None)
    assert waits == [10, 11, 10]
    assert closes == [11, 10]
    assert [e.level for e in events] == [PressureLevel.NORMAL, PressureLevel.PRESSURED,
                                       PressureLevel.NORMAL]


def test_windows_partial_handle_cleanup(monkeypatch):
    import resource_pressure.backends.windows as module
    closes = []
    class API:
        def create(self, kind):
            if kind: raise OSError("no second handle")
            return 99
        def close(self, handle): closes.append(handle)
    monkeypatch.setattr(module.sys, "platform", "win32")
    monkeypatch.setattr(module, "_WindowsAPI", API)
    with pytest.raises(BackendUnavailable):
        WindowsMemoryBackend().run(threading.Event(), lambda e: None, lambda: None)
    assert closes == [99]


@pytest.mark.parametrize("flags,level", [(0, -1), (1, 0), (2, 1), (4, 2), (3, 1), (7, 2)])
def test_macos_flags(flags, level):
    assert _map_flags(flags) == PressureLevel(level)


def test_macos_unknown_flags_fail():
    with pytest.raises(OSError):
        _map_flags(8)


def test_macos_backend_queue_and_cleanup(monkeypatch):
    import resource_pressure.backends.macos as module
    stop = threading.Event()
    closed, events = [], []
    class Source:
        def __init__(self):
            self.events = queue.Queue()
            for flag in (1, 2, 4, 1): self.events.put(flag)
        def close(self): closed.append(True)
    def emit(event):
        events.append(event.level)
        if len(events) == 4: stop.set()
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_DarwinSource", Source)
    MacOSMemoryBackend().run(stop, emit, lambda: None)
    assert events == [PressureLevel.NORMAL, PressureLevel.PRESSURED, PressureLevel.CRITICAL,
                      PressureLevel.NORMAL]
    assert closed == [True]


def test_auto_unsupported_is_explicit(monkeypatch):
    import resource_pressure.backends as module
    monkeypatch.setattr(module.sys, "platform", "unsupported")
    with pytest.raises(BackendUnavailable):
        auto_backend()


def test_linux_arguments_not_silently_ignored(monkeypatch):
    import resource_pressure.backends as module
    monkeypatch.setattr(module.sys, "platform", "win32")
    with pytest.raises(ValueError):
        auto_backend(psi=PSIConfig())
