"""One public facade for pressure-gated commands and kernel-backed containment."""
from __future__ import annotations

import math
import os
from typing import Any, Sequence

from ..governor import PressureGovernor
from ..model import ContainmentUnavailable


def _positive_int(name: str, value: int | None) -> None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        raise ValueError(f"{name} must be a positive integer or None")


class GovernedProcessGroup:
    """Run new child commands inside one owned containment group.

    NOT a wrapper around already-running Dask workers. Only children launched
    through run/arun are contained. Limits are optional explicit deployment
    budgets, never estimates derived from free RAM. Use this context inside a
    live governor context and finish all calls before leaving it.
    """
    def __init__(self, governor: PressureGovernor, *, max_memory: int | None = None,
                 max_processes: int | None = None, allow_process_group_fallback: bool = False):
        _positive_int("max_memory", max_memory)
        _positive_int("max_processes", max_processes)
        self.governor = governor
        self.max_memory = max_memory
        self.max_processes = max_processes
        self.allow_fallback = allow_process_group_fallback
        self._group: Any = None
        self._command_type: Any = None
        self._entered = False
        self._used = False

    def _construct(self) -> Any:
        if self._used:
            raise RuntimeError("A governed process group is single-use")
        self.governor.check_health()
        self._used = True
        try:
            from processkit import Command, ProcessGroup
        except ImportError as exc:
            raise ContainmentUnavailable(
                'Install the containment extra from the supplied local project: '
                'uv add ./resource-pressure --extra containment'
            ) from exc
        self._command_type = Command
        try:
            self._group = ProcessGroup(max_memory=self.max_memory, max_processes=self.max_processes)
        except Exception as exc:
            raise ContainmentUnavailable(f"Cannot create requested process containment: {exc}") from exc
        return self._group

    def _validate(self) -> None:
        mechanism = self.mechanism
        if mechanism not in ("job_object", "cgroup_v2"):
            if mechanism != "process_group" or not self.allow_fallback:
                raise ContainmentUnavailable(
                    f"Expected Job Object/cgroup v2, got {mechanism!r}. "
                    "POSIX process-group fallback requires explicit opt-in and is not a sandbox."
                )
            if self.max_memory is not None or self.max_processes is not None:
                raise ContainmentUnavailable("POSIX process groups cannot enforce whole-tree limits")
        if not callable(getattr(self._group, "output", None)) or not callable(
                getattr(self._group, "aoutput", None)):
            raise ContainmentUnavailable("Installed processkit-py lacks group output/aoutput; upgrade 1.x")

    def __enter__(self) -> GovernedProcessGroup:
        group = self._construct()
        group.__enter__()
        try:
            self._validate()
        except BaseException:
            group.__exit__(None, None, None)
            raise
        self._entered = True
        return self

    def __exit__(self, *args: Any) -> None:
        self._entered = False
        self._group.__exit__(*args)

    async def __aenter__(self) -> GovernedProcessGroup:
        group = self._construct()
        await group.__aenter__()
        try:
            self._validate()
        except BaseException:
            await group.__aexit__(None, None, None)
            raise
        self._entered = True
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._entered = False
        await self._group.__aexit__(*args)

    @property
    def mechanism(self) -> str:
        if self._group is None:
            raise RuntimeError("Enter the process-group context before inspecting its mechanism")
        return str(self._group.mechanism)

    def _command(self, program: str | os.PathLike[str], args: Sequence[str],
                 timeout: float | None, output_limit: int) -> Any:
        if not self._entered:
            raise RuntimeError("Use run/arun inside the process-group context")
        if isinstance(args, (str, bytes)):
            raise TypeError("args must be a sequence of argument strings, not a shell command")
        program = os.fspath(program)
        if not isinstance(program, str) or not program:
            raise ValueError("program must be a non-empty string or path")
        if any(not isinstance(arg, str) for arg in args):
            raise TypeError("Each argument must be a string")
        _positive_int("output_limit", output_limit)
        command = self._command_type(program, list(args)).output_limit(max_bytes=output_limit)
        if timeout is not None:
            if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("timeout must be finite and positive or None")
            command = command.timeout(timeout)
        return command

    def run(self, program: str | os.PathLike[str], args: Sequence[str] = (), *,
            timeout: float | None = None, admission_timeout: float | None = None,
            output_limit: int = 1_048_576) -> Any:
        """Return processkit.ProcessResult; a nonzero exit is data, not an exception."""
        command = self._command(program, args, timeout, output_limit)
        with self.governor.slot(timeout=admission_timeout):
            if not self._entered:
                raise RuntimeError("Process group closed while waiting for admission")
            return self._group.output(command)

    async def arun(self, program: str | os.PathLike[str], args: Sequence[str] = (), *,
                   timeout: float | None = None, admission_timeout: float | None = None,
                   output_limit: int = 1_048_576) -> Any:
        command = self._command(program, args, timeout, output_limit)
        async with self.governor.slot(timeout=admission_timeout):
            if not self._entered:
                raise RuntimeError("Process group closed while waiting for admission")
            return await self._group.aoutput(command)
