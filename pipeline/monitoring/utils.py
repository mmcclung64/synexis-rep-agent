"""Shared utilities for monitoring feeds.

Provides:
  - send_email()  — email via Microsoft Graph API (client credentials flow)
  - append_monitoring_log()  — structured append to logs/monitoring.jsonl

Environment variables required for email (add to .env):
    AZURE_TENANT_ID      Directory (tenant) ID from Azure App Registration
    AZURE_CLIENT_ID      Application (client) ID — Backend Service app
    AZURE_CLIENT_SECRET  Client secret — Backend Service app
    GRAPH_SENDER_EMAIL   Sending address (must be a licensed mailbox in tenant)
    NOTIFY_EMAIL         Default recipient if none specified
"""
from __future__ import annotations

import datetime as _dt
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MONITORING_LOG_PATH = LOG_DIR / "monitoring.jsonl"

AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID", "").strip()
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "").strip()
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "").strip()
GRAPH_SENDER_EMAIL = os.getenv("GRAPH_SENDER_EMAIL", "").strip()
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "mmcclung@synexis.com")

_GRAPH_TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
_GRAPH_SEND_URL = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"

# Simple in-memory token cache — avoids a new auth round-trip on every call
# within the same process (e.g. multiple digest emails per run).
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0.0}


def _get_graph_token() -> Optional[str]:
    """Obtain a Graph API access token via client credentials flow.

    Returns the token string, or None if credentials are not configured.
    Caches the token in memory until 60 seconds before expiry.
    """
    import time

    if not all([AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET]):
        return None

    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    url = _GRAPH_TOKEN_URL.format(tenant=AZURE_TENANT_ID)
    data = {
        "grant_type": "client_credentials",
        "client_id": AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    }
    try:
        resp = requests.post(url, data=data, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        _token_cache["token"] = payload["access_token"]
        _token_cache["expires_at"] = now + payload.get("expires_in", 3600) - 60
        return _token_cache["token"]
    except requests.RequestException as exc:
        print(f"[utils] Graph token fetch failed: {exc}")
        return None


def send_email(subject: str, body: str, to: str | None = None,
               dry_run: bool = False) -> bool:
    """Send a plain-text email via Microsoft Graph API.

    Returns True on success, False on failure. Never raises — callers should
    check the return value and log accordingly.

    Args:
        subject:  Email subject line.
        body:     Plain-text body.
        to:       Recipient address. Defaults to NOTIFY_EMAIL env var.
        dry_run:  If True, prints the email to stdout instead of sending.
    """
    recipient = to or NOTIFY_EMAIL
    sender = GRAPH_SENDER_EMAIL or NOTIFY_EMAIL

    if dry_run:
        print(f"\n{'='*60}")
        print(f"[DRY RUN] Email would be sent via Graph API:")
        print(f"  From:    {sender}")
        print(f"  To:      {recipient}")
        print(f"  Subject: {subject}")
        print(f"  Body:\n{body}")
        print(f"{'='*60}\n")
        return True

    token = _get_graph_token()
    if not token:
        print(
            "[utils.send_email] Graph API not configured — set AZURE_TENANT_ID, "
            "AZURE_CLIENT_ID, AZURE_CLIENT_SECRET in .env to enable email."
        )
        return False

    url = _GRAPH_SEND_URL.format(sender=sender)
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [{"emailAddress": {"address": recipient}}],
        },
        "saveToSentItems": False,
    }
    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        print(f"[utils.send_email] Sent '{subject}' to {recipient} via Graph API.")
        return True
    except requests.RequestException as exc:
        print(f"[utils.send_email] Graph API send failed: {exc}")
        if hasattr(exc, "response") and exc.response is not None:
            print(f"  Response: {exc.response.text[:300]}")
        return False


def append_monitoring_log(record: Dict[str, Any]) -> None:
    """Append a structured JSON record to logs/monitoring.jsonl.

    Automatically adds a UTC timestamp if not present.
    """
    if "timestamp" not in record:
        record["timestamp"] = _dt.datetime.utcnow().isoformat() + "Z"
    with MONITORING_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
