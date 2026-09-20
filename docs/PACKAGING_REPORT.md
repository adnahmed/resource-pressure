# Packaging validation — 0.1.1

The artifacts were built offline using the installed setuptools 82.0.1 backend.

- Universal core wheel: `resource_pressure-0.1.1-py3-none-any.whl`.
- Source distribution: `resource_pressure-0.1.1.tar.gz`.
- Wheel archive CRC validation passed and the `py.typed` marker is present.
- Metadata reports `Name: resource-pressure`, `Version: 0.1.1`, and
  `Requires-Python: >=3.10`.
- All runtime third-party requirements remain extra-conditional; the core has no
  mandatory third-party runtime dependency.
- Project URLs point to `https://github.com/adnahmed/resource-pressure`.
- `pip install --no-index --no-deps --target <isolated> <wheel>` passed.
- Import, version, managed-Dask API import, and admission-pacing smoke tests from
  the installed wheel passed.
- Full deterministic suite rerun against the installed wheel: **104 passed,
  3 skipped**.

This validates the package structure and tested Python paths. It does not replace
real Windows/macOS/Linux pressure testing or real optional-dependency integration;
see `TEST_REPORT.md` for those boundaries.
