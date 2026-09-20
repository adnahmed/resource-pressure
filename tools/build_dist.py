"""Build with an already-installed recent setuptools, without fetching anything.

Normal online workflow: python -m build (or uv build). This helper is useful in
an offline environment with setuptools>=77 already installed.
"""
from pathlib import Path
import os


def main() -> None:
    from setuptools.build_meta import build_sdist, build_wheel
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    (root / "dist").mkdir(exist_ok=True)
    print(build_wheel("dist"))
    print(build_sdist("dist"))


if __name__ == "__main__":
    main()
