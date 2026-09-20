from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict

from . import PressureError, PressureEvent, PressureGovernor


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Native memory-pressure monitor diagnostics")
    parser.add_argument("command", choices=("doctor", "watch"), nargs="?", default="doctor")
    parser.add_argument("--psi-path", action="append", dest="psi_paths",
                        help="Linux PSI file; repeat to monitor multiple scopes (replaces default)")
    args = parser.parse_args(argv)
    try:
        governor = PressureGovernor.auto(psi_paths=args.psi_paths)
        with governor:
            event = governor.event
            print(json.dumps({"backend": governor.backend.name, "level": event.level.name,
                              "source": event.source, "reason": event.reason,
                              "max_in_flight": governor.max_in_flight,
                              "containment": "not enabled; optional per process-group context"}), flush=True)
            if args.command == "watch":
                def report(event: PressureEvent) -> None:
                    data = asdict(event)
                    data["level"] = event.level.name
                    print(json.dumps(data), flush=True)
                governor.subscribe(report)
                while True:
                    time.sleep(0.2)
                    governor.check_health()
        return 0
    except KeyboardInterrupt:
        return 130
    except (PressureError, ValueError) as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
