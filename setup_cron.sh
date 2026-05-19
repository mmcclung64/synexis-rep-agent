#!/bin/bash
# setup_cron.sh — Install the Outbreak Watcher daily cron job on macOS
#
# Runs the monitoring orchestrator every day at 6:00 AM local time.
# Output is appended to logs/orchestrator.log in the repo directory.
#
# Usage:
#   bash setup_cron.sh          # install
#   bash setup_cron.sh --remove # uninstall

REPO_DIR="/Users/michaelmcclung/Desktop/Claude/synexis-bot/synexis-rep-agent"
PYTHON=$(which python3)
LOG="$REPO_DIR/logs/orchestrator.log"
CRON_MARKER="pipeline.monitoring.orchestrator"
CRON_ENTRY="0 6 * * * cd $REPO_DIR && $PYTHON -m pipeline.monitoring.orchestrator --confirm >> $LOG 2>&1"

if [[ "$1" == "--remove" ]]; then
    crontab -l 2>/dev/null | grep -v "$CRON_MARKER" | crontab -
    echo "Cron job removed."
    exit 0
fi

# Check if already installed
if crontab -l 2>/dev/null | grep -q "$CRON_MARKER"; then
    echo "Cron job already installed:"
    crontab -l | grep "$CRON_MARKER"
    exit 0
fi

# Install
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
echo "Cron job installed successfully."
echo "Schedule: daily at 6:00 AM"
echo "Command:  $CRON_ENTRY"
echo ""
echo "Verify with: crontab -l"
echo "Remove with: bash setup_cron.sh --remove"
