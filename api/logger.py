"""Structured JSON logger with partner-key masking.

Usage:
    from api.logger import log_event
    log_event("query.received", query_chars=len(q), partner_key=key)
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any


_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

_logger = logging.getLogger("synexis-rep-agent")
if not _logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(handler)
    _logger.setLevel(_LOG_LEVEL)


def _mask_key(key: str | None) -> str:
    if not key:
        return "-"
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}…{key[-4:]}"


def log_event(event: str, **fields: Any) -> None:
    payload = {"ts": time.time(), "event": event}
    for k, v in fields.items():
        if k in ("partner_key", "api_key"):
            v = _mask_key(v)
        payload[k] = v
    _logger.info(json.dumps(payload, default=str))
