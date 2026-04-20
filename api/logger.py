"""Structured JSON logger with partner-key masking.

Two output sinks:
  - stdout (human-readable in dev, one JSON line per event)
  - rotating file `logs/queries.jsonl` (10 MB per file, 5 backups)

Functions:
  - log_event(event, **fields): short-form operational event (startup, received, error)
  - log_query_record(...):      full CODE_BRIEFING schema entry on a completed /query
  - log_feedback_record(...):   parallel schema entry on a /feedback submission
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, List, Optional


_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Logs live at the repo root's logs/ dir by default so they're inspectable
# without leaving the project. Override via LOG_DIR env var in deploy.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_LOG_DIR = Path(os.getenv("LOG_DIR", str(_REPO_ROOT / "logs")))
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_QUERY_LOG_PATH = _LOG_DIR / "queries.jsonl"

_logger = logging.getLogger("synexis-rep-agent")
if not _logger.handlers:
    _fmt = logging.Formatter("%(message)s")
    # stdout sink — useful in dev / uvicorn foreground
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    _logger.addHandler(_sh)
    # rotating file sink — one JSON line per query/feedback for VoC analysis
    _fh = RotatingFileHandler(
        _QUERY_LOG_PATH,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    _fh.setFormatter(_fmt)
    _logger.addHandler(_fh)
    _logger.setLevel(_LOG_LEVEL)


def _mask_key(key: Optional[str]) -> str:
    if not key:
        return "-"
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}…{key[-4:]}"


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds")


def log_event(event: str, **fields: Any) -> None:
    """Short-form operational event. Kept for startup/received/error trails."""
    payload = {"ts": time.time(), "event": event}
    for k, v in fields.items():
        if k in ("partner_key", "api_key"):
            v = _mask_key(v)
        payload[k] = v
    _logger.info(json.dumps(payload, default=str))


def log_query_record(
    *,
    session_id: Optional[str],
    turn_id: Optional[int],
    user: Optional[str],
    query_original: str,
    query_rewritten: Optional[str],
    sources: List[dict],
    response: str,
    timing: dict,
    context_utilization_pct: Optional[float],
    partner_key: Optional[str] = None,
) -> None:
    """One JSON entry per completed /query, matching the CODE_BRIEFING Beta-logging schema."""
    payload = {
        "timestamp": _now_iso(),
        "event_type": "query",
        "session_id": session_id,
        "turn_id": turn_id,
        "user": user,
        "partner_key": _mask_key(partner_key),
        "query_original": query_original,
        "query_rewritten": query_rewritten,
        "sources": sources,
        "response": response,
        "timing": timing,
        "context_utilization_pct": context_utilization_pct,
    }
    _logger.info(json.dumps(payload, default=str, ensure_ascii=False))


def log_reject_record(
    *,
    session_id: Optional[str],
    turn_id: Optional[int],
    user: Optional[str],
    query: str,
    reject_reason: str,
    elapsed_ms: int,
    partner_key: Optional[str] = None,
) -> None:
    """One JSON entry per input-validator rejection. Same file as query/feedback;
    event_type='rejected' lets VoC analysis filter separately."""
    payload = {
        "timestamp": _now_iso(),
        "event_type": "rejected",
        "reject_reason": reject_reason,
        "session_id": session_id,
        "turn_id": turn_id,
        "user": user,
        "partner_key": _mask_key(partner_key),
        "query_original": query,
        "elapsed_ms": elapsed_ms,
    }
    _logger.info(json.dumps(payload, default=str, ensure_ascii=False))


def log_feedback_record(
    *,
    session_id: Optional[str],
    turn_id: Optional[int],
    user: Optional[str],
    query: str,
    answer: str,
    citations: List[dict],
    rating: str,
    feedback_text: Optional[str] = None,
    partner_key: Optional[str] = None,
) -> None:
    """One JSON entry per /feedback submission, same sink as query events."""
    payload = {
        "timestamp": _now_iso(),
        "event_type": "feedback",
        "session_id": session_id,
        "turn_id": turn_id,
        "user": user,
        "partner_key": _mask_key(partner_key),
        "rating": rating,
        "feedback_text": feedback_text,
        "query": query,
        "answer": answer,
        "citations": citations,
    }
    _logger.info(json.dumps(payload, default=str, ensure_ascii=False))
