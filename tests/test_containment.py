import asyncio
import sys
import types

import pytest

from resource_pressure import ContainmentUnavailable, PressureGovernor, PressureLevel
from resource_pressure.integrations.processkit import GovernedProcessGroup
from resource_pressure.testing import ManualBackend


class Command:
    def __init__(self, program, args):
        self.program, self.args = program, args
        self.limit = self.duration = None
    def output_limit(self, *, max_bytes):
        self.limit = max_bytes
        return self
    def timeout(self, duration):
        self.duration = duration
        return self


class Group:
    mechanism = "job_object"
    instances = []
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.commands = []
        self.instances.append(self)
    def __enter__(self): return self
    def __exit__(self, *args): self.closed = True
    async def __aenter__(self): return self
    async def __aexit__(self, *args): self.closed = True
    def output(self, command):
        self.commands.append(command)
        return types.SimpleNamespace(code=0, stdout="ok", stderr="")
    async def aoutput(self, command): return self.output(command)


@pytest.fixture
def processkit(monkeypatch):
    Group.instances = []
    monkeypatch.setitem(sys.modules, "processkit", types.SimpleNamespace(Command=Command, ProcessGroup=Group))
    return Group


def test_unified_sync_runner(normal_governor, processkit):
    governor, _ = normal_governor
    with governor.process_group(max_memory=12345, max_processes=4) as children:
        assert children.mechanism == "job_object"
        result = children.run("python", ["--version"], timeout=5)
        assert result.stdout == "ok"
        native = processkit.instances[-1]
        assert native.kwargs == {"max_memory": 12345, "max_processes": 4}
        assert native.commands[-1].limit == 1_048_576
        assert native.commands[-1].duration == 5
    assert native.closed
    assert governor.in_flight == 0


def test_process_execution_is_pressure_gated(processkit):
    with PressureGovernor(ManualBackend(PressureLevel.PRESSURED)) as governor:
        with governor.process_group() as children:
            with pytest.raises(TimeoutError):
                children.run("python", admission_timeout=0.01)
            assert not processkit.instances[-1].commands


def test_rejects_silent_fallback(normal_governor, processkit, monkeypatch):
    governor, _ = normal_governor
    monkeypatch.setattr(processkit, "mechanism", "process_group")
    with pytest.raises(ContainmentUnavailable, match="explicit opt-in"):
        with governor.process_group(): pass
    assert processkit.instances[-1].closed


def test_explicit_posix_fallback_without_limits(normal_governor, processkit, monkeypatch):
    governor, _ = normal_governor
    monkeypatch.setattr(processkit, "mechanism", "process_group")
    with governor.process_group(allow_process_group_fallback=True) as children:
        assert children.run("program").code == 0


def test_no_false_limit_guarantee_on_posix(normal_governor, processkit, monkeypatch):
    governor, _ = normal_governor
    monkeypatch.setattr(processkit, "mechanism", "process_group")
    with pytest.raises(ContainmentUnavailable, match="cannot enforce"):
        with governor.process_group(max_memory=100, allow_process_group_fallback=True): pass
    assert processkit.instances[-1].closed


def test_unknown_mechanism_never_accepted(normal_governor, processkit, monkeypatch):
    governor, _ = normal_governor
    monkeypatch.setattr(processkit, "mechanism", "unknown")
    with pytest.raises(ContainmentUnavailable):
        with governor.process_group(allow_process_group_fallback=True): pass


def test_missing_optional_dependency(normal_governor, monkeypatch):
    governor, _ = normal_governor
    monkeypatch.setitem(sys.modules, "processkit", None)
    with pytest.raises(ContainmentUnavailable, match="containment extra"):
        with governor.process_group(): pass


@pytest.mark.parametrize("kwargs", [{"max_memory": 0}, {"max_processes": True}, {"max_memory": -1}])
def test_invalid_limits(normal_governor, kwargs):
    governor, _ = normal_governor
    with pytest.raises(ValueError):
        GovernedProcessGroup(governor, **kwargs)


def test_no_shell_string_args(normal_governor, processkit):
    governor, _ = normal_governor
    with governor.process_group() as children:
        with pytest.raises(TypeError): children.run("python", "--version")
    assert governor.in_flight == 0


def test_native_run_exception_releases_lease(normal_governor, processkit, monkeypatch):
    governor, _ = normal_governor
    def fail(self, command): raise OSError("spawn failed")
    monkeypatch.setattr(processkit, "output", fail)
    with governor.process_group() as children:
        with pytest.raises(OSError): children.run("missing")
    assert governor.in_flight == 0


def test_async_contained_execution(processkit):
    async def run():
        async with PressureGovernor(ManualBackend()) as governor:
            async with governor.process_group() as children:
                assert (await children.arun("program")).stdout == "ok"
            assert governor.in_flight == 0
            assert processkit.instances[-1].closed
    asyncio.run(run())
