import json

from resource_pressure import BackendUnavailable, PressureGovernor
from resource_pressure.testing import ManualBackend
from resource_pressure.cli import main


def test_doctor_reports_actual_backend(monkeypatch, capsys):
    monkeypatch.setattr(PressureGovernor, "auto", lambda **kwargs: PressureGovernor(ManualBackend()))
    assert main(["doctor"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["backend"] == "manual-test-backend"
    assert payload["level"] == "NORMAL"


def test_doctor_reports_failure(monkeypatch, capsys):
    def fail(**kwargs): raise BackendUnavailable("sensor missing")
    monkeypatch.setattr(PressureGovernor, "auto", fail)
    assert main(["doctor"]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"] == "BackendUnavailable"
