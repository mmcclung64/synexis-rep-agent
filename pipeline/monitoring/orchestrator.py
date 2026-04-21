"""Monitoring orchestrator — runs all active feeds in sequence.

Usage:
    python3 -m pipeline.monitoring.orchestrator           # dry run: all feeds
    python3 -m pipeline.monitoring.orchestrator --confirm # full run: all feeds
    python3 -m pipeline.monitoring.orchestrator --feed synexis_web --confirm

Feeds run in declaration order. Each feed module exposes a run(confirm, dry_run)
function. The orchestrator collects their results and writes a summary heartbeat
to logs/monitoring.jsonl.

Scheduling:
    Add a cron entry or a Render Cron Job pointing at this module for daily runs.
    Example crontab (runs at 06:00 local time):
        0 6 * * * cd ~/Desktop/Claude/synexis-bot/synexis-rep-agent && \
            python3 -m pipeline.monitoring.orchestrator --confirm >> logs/orchestrator.log 2>&1
"""
from __future__ import annotations

import argparse
import datetime as _dt
import sys
import traceback
from typing import Any, Dict, List, Optional

from pipeline.monitoring import feed_synexis_web
from pipeline.monitoring.utils import append_monitoring_log

# Registered feeds in run order. Add new feeds here as they are implemented.
FEEDS: List[Dict[str, Any]] = [
    {
        "name": "synexis_web",
        "module": feed_synexis_web,
        "description": "synexis.com change monitor",
    },
    # Future feeds — uncomment as implemented:
    # {"name": "outbreaks", "module": feed_outbreaks, "description": "ProMED + FDA outbreak monitor"},
]


def run_all(confirm: bool = False, dry_run: bool = True, feed_filter: Optional[str] = None) -> None:
    """Run all registered feeds (or a single named feed) and log results."""
    mode = "confirm" if confirm else "dry_run"
    timestamp = _dt.datetime.utcnow().isoformat() + "Z"

    targets = [f for f in FEEDS if feed_filter is None or f["name"] == feed_filter]
    if not targets:
        print(f"[orchestrator] No feed named '{feed_filter}' found. Available: "
              f"{[f['name'] for f in FEEDS]}")
        sys.exit(1)

    print(f"[orchestrator] Starting run — mode={mode}, feeds={[f['name'] for f in targets]}")

    results = []
    for feed in targets:
        name = feed["name"]
        print(f"\n{'─'*60}")
        print(f"[orchestrator] Running feed: {name} ({feed['description']})")
        print(f"{'─'*60}")
        try:
            result = feed["module"].run(confirm=confirm, dry_run=dry_run)
            results.append({"feed": name, "status": "ok", "result": result})
        except Exception as exc:  # noqa: BLE001
            tb = traceback.format_exc()
            print(f"[orchestrator] Feed '{name}' raised an exception:\n{tb}")
            results.append({"feed": name, "status": "error", "error": str(exc)})

    # Summary heartbeat
    summary = {
        "event": "orchestrator_run",
        "timestamp": timestamp,
        "mode": mode,
        "feeds_run": [r["feed"] for r in results],
        "results": results,
    }
    append_monitoring_log(summary)

    ok = sum(1 for r in results if r["status"] == "ok")
    err = len(results) - ok
    print(f"\n[orchestrator] Run complete — {ok} ok, {err} errors. "
          f"Summary appended to logs/monitoring.jsonl.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Synexis monitoring orchestrator")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Execute full run with all writes and notifications (default: dry run)",
    )
    parser.add_argument(
        "--feed",
        metavar="NAME",
        default=None,
        help="Run a single named feed instead of all feeds (e.g. --feed synexis_web)",
    )
    args = parser.parse_args()
    run_all(confirm=args.confirm, dry_run=not args.confirm, feed_filter=args.feed)


if __name__ == "__main__":
    main()
