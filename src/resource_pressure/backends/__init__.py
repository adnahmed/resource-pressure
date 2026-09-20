"""Backends are loaded lazily; foreign-platform ctypes are never loaded on import."""
from __future__ import annotations

import sys
from typing import Sequence

from ..model import BackendUnavailable
from .base import PressureBackend
from .linux import PSIConfig


def auto_backend(*, psi: PSIConfig | None = None,
                 psi_paths: Sequence[str] | None = None) -> PressureBackend:
    if sys.platform.startswith("linux"):
        from .linux import LinuxPSIBackend
        return LinuxPSIBackend(config=psi, paths=psi_paths)
    if psi is not None or psi_paths is not None:
        raise ValueError("psi and psi_paths only apply to Linux")
    if sys.platform == "win32":
        from .windows import WindowsMemoryBackend
        return WindowsMemoryBackend()
    if sys.platform == "darwin":
        from .macos import MacOSMemoryBackend
        return MacOSMemoryBackend()
    raise BackendUnavailable(f"No native memory-pressure backend for {sys.platform!r}")
