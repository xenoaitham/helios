"""CLI: python -m dq {gate,replay,status} (ADR-011 D3/D7).

gate    — the DAG task body: every suite, dead-letter, nonzero on failure.
replay  — fix-then-rebuild-then-replay resolution pass; nonzero if any open
          incident remains (an operator tool that cannot half-succeed).
status  — quarantine report; informational, always 0.

Exit codes: 0 clean · 1 gate/replay FAILURE on data (the DAG goes red at the
gate) · 2 configuration error (missing env — loud, never guessed) · 3 unexpected
CRASH — a crash must never masquerade as a data verdict (logged as
dq.gate_crashed / dq.replay_crashed; still nonzero, still blocks the DAG).
"""

from __future__ import annotations

import argparse
import sys

from dq.config import ConfigError, load_config
from dq.gate import format_summary, run_gate
from dq.log import log
from dq.replay import format_replay_summary, run_replay
from dq.status import run_status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dq", description="HELIOS Great Expectations gate (ADR-011)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("gate", help="run every suite; dead-letter failures; nonzero exit on failure")
    sub.add_parser("replay", help="resolve open incidents that no longer reproduce; nonzero if any remain")
    sub.add_parser("status", help="report quarantine status (informational)")
    args = parser.parse_args(argv)

    try:
        cfg = load_config()
    except ConfigError as exc:
        log("dq.config_error", level="error", error=str(exc))
        return 2

    try:
        if args.command == "gate":
            report = run_gate(cfg)
            print(format_summary(report))
            return 0 if report.success else 1
        if args.command == "replay":
            report = run_replay(cfg)
            print(format_replay_summary(report))
            return 0 if report.remaining == 0 else 1
        run_status(cfg)
        return 0
    except Exception as exc:  # noqa: BLE001 — the crash/verdict boundary IS the contract
        import traceback

        log("dq.{}_crashed".format(args.command), level="error",
            error=type(exc).__name__, message=str(exc)[:500],
            traceback=traceback.format_exc()[:1500])
        return 3


if __name__ == "__main__":
    sys.exit(main())
