#!/bin/bash
# push.sh — safe git commit + push for synexis-rep-agent
# Usage: bash push.sh "your commit message" [file1 file2 ...]
# If no files are given, stages all changes (git add -A).
#
# Clears stale git lock files before every operation so that
# cross-filesystem lock leftovers from the Cowork sandbox never block a push.

set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"

# ── 1. Clear any stale lock files ────────────────────────────────────────────
find .git -name "*.lock" -print -delete 2>/dev/null || true

# ── 2. Validate commit message ────────────────────────────────────────────────
if [ -z "$1" ]; then
  echo "Usage: bash push.sh \"commit message\" [file1 file2 ...]"
  exit 1
fi

COMMIT_MSG="$1"
shift   # remaining args (if any) are specific files to stage

# ── 3. Stage files ────────────────────────────────────────────────────────────
if [ "$#" -gt 0 ]; then
  git add "$@"
else
  git add -A
fi

# ── 4. Commit (skip if nothing to commit) ────────────────────────────────────
if git diff --cached --quiet; then
  echo "Nothing to commit — working tree clean."
else
  git commit -m "$COMMIT_MSG"
fi

# ── 5. Pull + push ────────────────────────────────────────────────────────────
git pull --rebase origin main
git push origin main

echo "Done."
