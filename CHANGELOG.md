# Changelog

## 0.1.0 — 2026-09-20

Initial alpha implementation: native Windows low/high memory-resource notification
backend; Linux PSI trigger backend with explicit stall-time policy and multiple
scopes; macOS libdispatch backend with native semantic state bootstrap; sync and
async admission leases; explicit sensor/containment failures; public-API Dask
submission and bounded streaming; optional processkit containment facade; CLI,
examples, deterministic tests, and opt-in real integration smoke tests.

This source/wheel delivery is not a published PyPI release. The test report
separates local deterministic validation from native/third-party validation.
