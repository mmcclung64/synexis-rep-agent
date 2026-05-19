#!/bin/bash
# Synexis Outbreak Watcher — full confirm run
# Run from the synexis-rep-agent directory.
# Output is logged to logs/confirm_run.log and tailed live.
#
# Usage:
#   bash run_outbreak_watcher.sh            # dry run (default)
#   bash run_outbreak_watcher.sh --confirm  # full run (creates HubSpot tasks + sends email)

cd "$(dirname "$0")"

MODE=""
if [ "$1" = "--confirm" ]; then
  MODE="--confirm"
  echo "=== FULL CONFIRM RUN — HubSpot tasks will be created, digest email will be sent ==="
else
  echo "=== DRY RUN — no writes, no API calls beyond Haiku ==="
fi

python3 -m pipeline.monitoring.feed_outbreaks $MODE 2>&1 | tee logs/confirm_run.log
echo ""
echo "Log saved to: logs/confirm_run.log"
