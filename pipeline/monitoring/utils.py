"""Shared utilities for monitoring feeds.

Provides:
  - send_email()  — SMTP email via env-configured credentials
  - append_monitoring_log()  — structured append to logs/monitoring.jsonl

Environment variables required for email (add to .env):
    SMTP_HOST        e.g. smtp.gmail.com
    SMTP_PORT        e.g. 587
    SMTP_USER        sending address, e.g. michael@synexis.com
    SMTP_PASSWORD    app password or SMTP credential
    NOTIFY_EMAIL     recipient, e.g. mmcclung@synexis.com
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MONITORING_LOG_PATH = LOG_DIR / "monitoring.jsonl"

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "mmcclung@synexis.com")


def send_email(subject: str, body: str, to: str | None = None, dry_run: bool = False) -> bool:
    """Send a plain-text email via SMTP.

    Returns True on success, False on failure. Never raises — callers should
    check the return value and log accordingly.

    Args:
        subject:  Email subject line.
        body:     Plain-text body.
        to:       Recipient address. Defaults to NOTIFY_EMAIL env var.
        dry_run:  If True, prints the email to stdout instead of sending.
    """
    recipient = to or NOTIFY_EMAIL

    if dry_run:
        print(f"\n{'='*60}")
        print(f"[DRY RUN] Email would be sent:")
        print(f"  To:      {recipient}")
        print(f"  Subject: {subject}")
        print(f"  Body:\n{body}")
        print(f"{'='*60}\n")
        return True

    if not all([SMTP_HOST, SMTP_USER, SMTP_PASSWORD]):
        print(
            "[utils.send_email] Email not configured — set SMTP_HOST, SMTP_USER, "
            "SMTP_PASSWORD in .env to enable notifications."
        )
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = recipient
        msg.attach(MIMEText(body, "plain"))

        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipient, msg.as_string())
        return True

    except Exception as exc:  # noqa: BLE001
        print(f"[utils.send_email] Failed to send email: {exc}")
        return False


def append_monitoring_log(record: Dict[str, Any]) -> None:
    """Append a structured JSON record to logs/monitoring.jsonl.

    Automatically adds a UTC timestamp if not present.
    """
    if "timestamp" not in record:
        record["timestamp"] = _dt.datetime.utcnow().isoformat() + "Z"
    with MONITORING_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
