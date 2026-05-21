"""Append feedback records to a JSONL file in the GitHub repo via the Contents API.

Each feedback submission fires a background task that:
  1. GETs the current file (to retrieve its SHA + existing content)
  2. Appends the new JSON line
  3. PUTs the updated file back

This gives us a persistent, human-readable audit trail that survives Render
deploys. Race conditions at low UAT volume are acceptable; if two writes land
simultaneously the second will 409 and log a warning (no crash, no data loss on
the first write).

Env vars required:
    GITHUB_TOKEN   — Personal Access Token with Contents:write on the repo
    GITHUB_REPO    — owner/repo  (default: mmcclung64/synexis-rep-agent)
    GITHUB_BRANCH  — branch to commit to (default: main)
    GITHUB_LOG_PATH — path inside the repo (default: logs/feedback_log.jsonl)
"""
from __future__ import annotations

import base64
import json
import logging
import os
from typing import Optional

import httpx

log = logging.getLogger("synexis-rep-agent")

_GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
_GITHUB_REPO     = os.getenv("GITHUB_REPO", "mmcclung64/synexis-rep-agent")
_GITHUB_BRANCH   = os.getenv("GITHUB_BRANCH", "main")
_GITHUB_LOG_PATH = os.getenv("GITHUB_LOG_PATH", "logs/feedback_log.jsonl")

_API_BASE = "https://api.github.com"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def append_feedback(record: dict) -> None:
    """Synchronous worker — call from a BackgroundTask so it's off the hot path."""
    if not _GITHUB_TOKEN:
        log.warning("github_logger: GITHUB_TOKEN not set — skipping remote log write")
        return

    url = f"{_API_BASE}/repos/{_GITHUB_REPO}/contents/{_GITHUB_LOG_PATH}"
    new_line = json.dumps(record, default=str, ensure_ascii=False) + "\n"

    # 1. Fetch current file (may not exist yet)
    sha: Optional[str] = None
    existing_content = ""
    try:
        resp = httpx.get(url, headers=_headers(), params={"ref": _GITHUB_BRANCH}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            sha = data["sha"]
            existing_content = base64.b64decode(data["content"].replace("\n", "")).decode("utf-8")
        elif resp.status_code != 404:
            log.warning("github_logger: unexpected GET status %s — %s", resp.status_code, resp.text[:200])
            return
    except Exception as exc:
        log.warning("github_logger: GET failed — %s", exc)
        return

    # 2. Build updated content
    updated = existing_content + new_line
    encoded = base64.b64encode(updated.encode("utf-8")).decode("ascii")

    # 3. Commit back
    body: dict = {
        "message": f"log: feedback record {record.get('timestamp', 'unknown')}",
        "content": encoded,
        "branch": _GITHUB_BRANCH,
    }
    if sha:
        body["sha"] = sha

    try:
        put_resp = httpx.put(url, headers=_headers(), json=body, timeout=15)
        if put_resp.status_code in (200, 201):
            log.info("github_logger: feedback record written to %s", _GITHUB_LOG_PATH)
        else:
            log.warning("github_logger: PUT failed %s — %s", put_resp.status_code, put_resp.text[:200])
    except Exception as exc:
        log.warning("github_logger: PUT exception — %s", exc)
