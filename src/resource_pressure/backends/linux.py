"""Linux PSI triggers. Stall-time policy is explicit; no RAM-usage thresholds."""
from __future__ import annotations

import math
import os
import select
import sys
import time
from dataclasses import dataclass
from threading import Event
from typing import Sequence

from ..model import BackendUnavailable, PressureEvent, PressureLevel
from .base import Emit, Ready


@dataclass(frozen=True)
class PSIConfig:
    """Library policy, NOT severity levels supplied by the Linux kernel.

    Each trigger has its own fd. A 2-second window also satisfies the current
    kernel's unprivileged-monitor window restriction. File permissions still
    apply. Recovery is inferred after quiet_seconds without a trigger; PSI has
    no explicit 'pressure cleared' event.
    """
    some_stall_us: int = 150_000
    full_stall_us: int = 50_000
    window_us: int = 2_000_000
    quiet_seconds: float = 4.0

    def __post_init__(self) -> None:
        for name in ("some_stall_us", "full_stall_us", "window_us"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer number of microseconds")
        if not 500_000 <= self.window_us <= 10_000_000:
            raise ValueError("window_us must be between 500,000 and 10,000,000")
        if self.window_us % 2_000_000:
            raise ValueError("Use a multiple of 2,000,000 us for unprivileged PSI monitors")
        if not 0 < self.some_stall_us <= self.window_us:
            raise ValueError("some_stall_us must be in (0, window_us]")
        if not 0 < self.full_stall_us <= self.window_us:
            raise ValueError("full_stall_us must be in (0, window_us]")
        if isinstance(self.quiet_seconds, bool) or not math.isfinite(self.quiet_seconds):
            raise ValueError("quiet_seconds must be finite")
        if self.quiet_seconds < 2 * self.window_us / 1_000_000:
            raise ValueError("quiet_seconds must cover at least two trigger windows")


class _PSIState:
    """Pure state machine, separately testable without putting a host under load."""
    def __init__(self, config: PSIConfig, started: float):
        self.config = config
        # Do not admit a burst before the newly registered triggers have observed
        # a full tracking window. This is observation time, not an OOM guarantee.
        self.warm_until = started + config.window_us / 1_000_000
        self.some_at = float("-inf")
        self.full_at = float("-inf")

    def trigger(self, level: PressureLevel, now: float) -> None:
        if level == PressureLevel.CRITICAL:
            self.full_at = now
        else:
            self.some_at = now

    def level(self, now: float) -> PressureLevel:
        if now - self.full_at < self.config.quiet_seconds:
            return PressureLevel.CRITICAL
        if now - self.some_at < self.config.quiet_seconds:
            return PressureLevel.PRESSURED
        if now < self.warm_until:
            return PressureLevel.UNKNOWN
        return PressureLevel.NORMAL


class LinuxPSIBackend:
    name = "linux-psi"

    def __init__(self, *, config: PSIConfig | None = None, paths: Sequence[str] | None = None):
        self.config = config or PSIConfig()
        if isinstance(paths, (str, bytes)):
            raise TypeError("paths must be a sequence of PSI file paths, not a single string")
        self.paths = tuple(dict.fromkeys(os.fspath(p) for p in (
            paths if paths is not None else ("/proc/pressure/memory",))))
        if not self.paths:
            raise ValueError("At least one PSI path is required")

    def run(self, stop: Event, emit: Emit, ready: Ready) -> None:
        if not sys.platform.startswith("linux"):
            raise BackendUnavailable("Linux PSI requires Linux")
        opened: list[int] = []
        fd_info: dict[int, tuple[str, PressureLevel]] = {}
        poller = select.poll()
        last_level: PressureLevel | None = None
        last_source = ", ".join(self.paths)
        try:
            for path in self.paths:
                for metric, amount, level in (
                    ("some", self.config.some_stall_us, PressureLevel.PRESSURED),
                    ("full", self.config.full_stall_us, PressureLevel.CRITICAL),
                ):
                    try:
                        fd = os.open(path, os.O_RDWR | os.O_NONBLOCK | os.O_CLOEXEC)
                        opened.append(fd)
                        trigger = f"{metric} {amount} {self.config.window_us}\0".encode("ascii")
                        if os.write(fd, trigger) != len(trigger):
                            raise OSError("Incomplete PSI trigger registration")
                    except OSError as exc:
                        raise BackendUnavailable(
                            f"Cannot register PSI trigger on {path!r}: {exc}. "
                            "PSI support and write permission are required. Use a delegated "
                            "cgroup memory.pressure file or an administrator-provisioned "
                            "sensor; no RAM-percentage fallback is used."
                        ) from exc
                    fd_info[fd] = (path, level)
                    poller.register(fd, select.POLLPRI | select.POLLERR | select.POLLHUP)
            state = _PSIState(self.config, time.monotonic())
            emit(PressureEvent(PressureLevel.UNKNOWN, self.name,
                               "observing initial PSI tracking window", last_source))
            last_level = PressureLevel.UNKNOWN
            ready()
            while not stop.is_set():
                # A timed poll only makes shutdown/recovery responsive. It never
                # samples RAM, PSI averages, or /proc/meminfo.
                events = poller.poll(200)
                now = time.monotonic()
                for fd, flags in events:
                    if flags & (select.POLLERR | select.POLLHUP | select.POLLNVAL):
                        raise OSError(f"PSI source disappeared or failed: {fd_info[fd][0]}")
                    if flags & select.POLLPRI:
                        source, level = fd_info[fd]
                        state.trigger(level, now)
                        if level == PressureLevel.CRITICAL or state.level(now) != PressureLevel.CRITICAL:
                            last_source = source
                level = state.level(now)
                if level != last_level:
                    reason = ("quiet period elapsed; recovery inferred by library policy"
                              if level == PressureLevel.NORMAL else
                              "kernel full-stall trigger" if level == PressureLevel.CRITICAL else
                              "kernel some-stall trigger" if level == PressureLevel.PRESSURED else
                              "observing initial PSI tracking window")
                    emit(PressureEvent(level, self.name, reason, last_source))
                    last_level = level
        finally:
            for fd in reversed(opened):
                os.close(fd)  # Closing unregisters the trigger.
