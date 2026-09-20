# Packaging validation — 0.1.0

The artifacts were built offline using the installed setuptools 82.0.1 backend.

- Universal core wheel: `resource_pressure-0.1.0-py3-none-any.whl`.
- Source distribution: `resource_pressure-0.1.0.tar.gz`.
- Wheel archive CRC validation passed; the `py.typed` marker is present.
- Metadata inspection confirmed that every `Requires-Dist` entry is conditional
  on an extra: there are no mandatory core runtime dependencies.
- `pip install --no-index --no-deps --target <isolated directory> <wheel>` passed.
- Import and async admission smoke test from the installed wheel passed.
- Full test suite rerun against the installed wheel, with the source-tree
  pythonpath setting disabled: **92 passed, 3 skipped**.

This validates Python packaging and the tested implementation paths; it does not
establish native Windows/macOS, Linux PSI delivery, or optional-dependency
compatibility. See TEST_REPORT.md for the explicit validation boundaries.
