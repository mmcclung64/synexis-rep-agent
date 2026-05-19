"""SharePoint → Pinecone sync pipeline for the Synexis Rep Agent v2.

Watches golden-copy SharePoint folders via Microsoft Graph API webhooks.
When files are added, modified, or deleted, the pipeline automatically
re-ingests or forgets the corresponding Pinecone vectors.

Architecture
------------
  register_all_subscriptions()  — call once at startup or when config changes
  renew_all_subscriptions()     — call daily (Render cron, 3-day expiry limit)
  process_notification(payload) — called by POST /graph/notifications in api/main.py
  sync_delta(folder_name)       — catch-up on missed changes (startup or manual)
  ingest_file(...)              — download → extract → chunk → embed → upsert
  forget_file(...)              — delete Pinecone vectors by sp_item_id metadata filter

State files (in synexis-rep-agent/logs/)
-----------------------------------------
  sp_subscriptions.json     — {folder_name: {id, expiration_dt, resource}}
  sp_delta_tokens.json      — {folder_item_id: delta_link}
  hubspot_docs_registry.json — {sp_item_id: {hubspot_doc_id, share_url, title}}

Environment variables (from .env)
-----------------------------------
  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
  PINECONE_API_KEY, PINECONE_INDEX_NAME
  VOYAGE_API_KEY, VOYAGE_EMBEDDING_MODEL
  GRAPH_NOTIFICATION_URL — public HTTPS URL for webhook endpoint
                           e.g. https://synexis-rep-agent.onrender.com/graph/notifications
  HUBSPOT_ACCESS_TOKEN

CLI
---
  python -m pipeline.sharepoint_sync --register     # register all subscriptions
  python -m pipeline.sharepoint_sync --renew        # renew expiring subscriptions
  python -m pipeline.sharepoint_sync --delta        # sync delta on all folders
  python -m pipeline.sharepoint_sync --delta --folder "Marketing Approved Collateral"
  python -m pipeline.sharepoint_sync --ingest-item <drive_id> <item_id> <folder_name>
  python -m pipeline.sharepoint_sync --forget-item <sp_item_id>
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

SUBSCRIPTIONS_PATH = LOGS_DIR / "sp_subscriptions.json"
DELTA_TOKENS_PATH = LOGS_DIR / "sp_delta_tokens.json"
HUBSPOT_REGISTRY_PATH = LOGS_DIR / "hubspot_docs_registry.json"
WATCHED_FOLDERS_PATH = Path(__file__).resolve().parent / "watched_folders.json"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "sra")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
VOYAGE_MODEL = os.getenv("VOYAGE_EMBEDDING_MODEL", "voyage-3")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
GRAPH_NOTIFICATION_URL = os.getenv(
    "GRAPH_NOTIFICATION_URL",
    "https://synexis-rep-agent.onrender.com/graph/notifications",
)

# Subscription life — Graph max is 4320 min (3 days). We renew at 72h.
SUBSCRIPTION_RENEWAL_HOURS = 72

# Embed batch limits (same as embed_load.py)
EMBED_BATCH_MAX_CHUNKS = 128
EMBED_BATCH_MAX_TOKENS = 9_000
UPSERT_BATCH = 100
EMBED_INTER_CALL_SECONDS = 0.5
MAX_METADATA_TEXT_CHARS = 20_000
MIN_CHARS_FOR_INGEST = 200

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watched-folders config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Load and validate watched_folders.json."""
    if not WATCHED_FOLDERS_PATH.exists():
        raise FileNotFoundError(f"Config not found: {WATCHED_FOLDERS_PATH}")
    with WATCHED_FOLDERS_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def get_watched_folders(config: dict | None = None) -> list[dict]:
    """Return only folders with confirmed item_ids (skip TBD placeholders)."""
    if config is None:
        config = load_config()
    drive_ids = config["drive_ids"]
    folders = []
    for f in config["watched_folders"]:
        if f.get("item_id") == "TBD" or f.get("drive_id") == "TBD":
            log.warning("Skipping %s — item_id or drive_id not yet confirmed.", f["name"])
            continue
        if not f.get("ingest", True):
            log.info("Skipping %s — ingest=false.", f["name"])
            continue
        # Resolve drive_id alias → actual drive ID
        drive_key = f.get("drive_id", "")
        folder = dict(f)
        folder["drive_id"] = drive_ids.get(drive_key, drive_key)
        folders.append(folder)
    return folders


def should_exclude(filename: str, folder_config: dict) -> bool:
    """Return True if this filename matches any exclude_pattern for the folder."""
    for pat in folder_config.get("exclude_patterns", []):
        if fnmatch.fnmatch(filename, pat) or fnmatch.fnmatch(filename.lower(), pat.lower()):
            return True
    return False


def tier_for_item(item_name: str, folder_config: dict) -> int:
    """Resolve the effective tier for a file, respecting content_type_overrides if needed.
    For now returns default_tier; extend with filename-based override logic as content matures.
    """
    return folder_config.get("default_tier", 3)


# ---------------------------------------------------------------------------
# Microsoft Graph authentication
# ---------------------------------------------------------------------------

_token_cache: dict[str, Any] = {}


def _get_graph_token() -> str:
    """Return a valid Graph API access token, refreshing if needed."""
    now = time.time()
    if _token_cache.get("access_token") and now < _token_cache.get("expires_at", 0) - 60:
        return _token_cache["access_token"]

    url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        "grant_type": "client_credentials",
        "client_id": AZURE_CLIENT_ID,
        "client_secret": AZURE_CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    }, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600)
    return _token_cache["access_token"]


def _graph_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_get_graph_token()}", "Content-Type": "application/json"}


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=30))
def _graph_get(url: str, params: dict | None = None) -> dict:
    resp = requests.get(url, headers=_graph_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=30))
def _graph_post(url: str, payload: dict) -> dict:
    resp = requests.post(url, headers=_graph_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=30))
def _graph_patch(url: str, payload: dict) -> dict:
    resp = requests.patch(url, headers=_graph_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=30))
def _graph_delete(url: str) -> None:
    resp = requests.delete(url, headers=_graph_headers(), timeout=30)
    if resp.status_code != 204:
        resp.raise_for_status()


# ---------------------------------------------------------------------------
# Subscription state persistence
# ---------------------------------------------------------------------------

def _load_subscriptions() -> dict:
    if SUBSCRIPTIONS_PATH.exists():
        try:
            return json.loads(SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_subscriptions(subs: dict) -> None:
    SUBSCRIPTIONS_PATH.write_text(json.dumps(subs, indent=2), encoding="utf-8")


def _load_delta_tokens() -> dict:
    if DELTA_TOKENS_PATH.exists():
        try:
            return json.loads(DELTA_TOKENS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_delta_tokens(tokens: dict) -> None:
    DELTA_TOKENS_PATH.write_text(json.dumps(tokens, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Subscription management
# ---------------------------------------------------------------------------

def _subscription_expiry_dt() -> str:
    """Return an ISO-8601 expiration datetime ~72 hours from now (well under 4320 min max)."""
    dt = datetime.now(timezone.utc) + timedelta(hours=SUBSCRIPTION_RENEWAL_HOURS)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


def register_subscription(drive_id: str, drive_alias: str) -> dict:
    """Register a Graph change notification subscription on a drive root.

    Graph supports subscriptions on drives/{drive-id}/root (not on individual
    folder items). One subscription per drive covers all folders within it.
    Returns the raw Graph subscription object.
    """
    resource = f"drives/{drive_id}/root"
    payload = {
        "changeType": "updated",
        "notificationUrl": GRAPH_NOTIFICATION_URL,
        "resource": resource,
        "expirationDateTime": _subscription_expiry_dt(),
        "clientState": f"synexis-rep-agent:drive:{drive_alias}",
    }
    log.info("Registering subscription for drive %s (%s)", drive_alias, resource)
    sub = _graph_post(f"{GRAPH_BASE}/subscriptions", payload)
    log.info("Subscription %s registered for drive %s, expires %s",
             sub["id"], drive_alias, sub["expirationDateTime"])
    return sub


def register_all_subscriptions(force: bool = False) -> dict[str, dict]:
    """Register one Graph subscription per unique drive across all confirmed folders.

    Graph only supports drive-root subscriptions (not per-folder), so we deduplicate
    by drive_id and register once per drive. Keyed by drive alias in sp_subscriptions.json.
    Returns {drive_alias: subscription_object}.
    """
    subs = _load_subscriptions()
    config = load_config()
    drive_ids = config["drive_ids"]  # alias → actual drive_id
    folders = get_watched_folders(config)
    results = {}

    # Collect unique drives needed by confirmed folders
    needed_drives: dict[str, str] = {}  # alias → actual drive_id
    for folder in folders:
        actual_drive_id = folder["drive_id"]
        # Find alias for this drive_id
        alias = next((k for k, v in drive_ids.items() if v == actual_drive_id), actual_drive_id)
        needed_drives[alias] = actual_drive_id

    for alias, actual_drive_id in needed_drives.items():
        key = f"drive:{alias}"
        if not force and key in subs:
            log.info("Subscription already exists for drive %s (%s), skipping.",
                     alias, subs[key].get("id"))
            results[key] = subs[key]
            continue
        try:
            sub = register_subscription(actual_drive_id, alias)
            subs[key] = {
                "id": sub["id"],
                "expiration_dt": sub["expirationDateTime"],
                "resource": sub["resource"],
                "drive_id": actual_drive_id,
                "drive_alias": alias,
            }
            results[key] = subs[key]
        except Exception as exc:
            log.error("Failed to register subscription for drive %s: %s", alias, exc)

    _save_subscriptions(subs)
    return results


def renew_subscription(key: str, sub_id: str) -> str:
    """Extend a subscription's expiration by patching it. Returns new expiration string."""
    new_expiry = _subscription_expiry_dt()
    _graph_patch(f"{GRAPH_BASE}/subscriptions/{sub_id}", {"expirationDateTime": new_expiry})
    log.info("Renewed subscription %s for %s, new expiry: %s", sub_id, key, new_expiry)
    return new_expiry


def renew_all_subscriptions() -> None:
    """Renew all known drive-level subscriptions. Safe to call daily."""
    subs = _load_subscriptions()
    config = load_config()
    drive_ids = config["drive_ids"]
    folders = get_watched_folders(config)

    # Collect unique drives still needed
    needed_drives: dict[str, str] = {}
    for folder in folders:
        actual = folder["drive_id"]
        alias = next((k for k, v in drive_ids.items() if v == actual), actual)
        needed_drives[alias] = actual

    for name, sub in list(subs.items()):
        sub_id = sub.get("id")
        if not sub_id:
            continue
        try:
            new_expiry = renew_subscription(name, sub_id)
            subs[name]["expiration_dt"] = new_expiry
        except Exception as exc:
            log.error("Failed to renew subscription %s for %s: %s", sub_id, name, exc)
            if "404" in str(exc):
                log.warning("Subscription %s not found — re-registering %s", sub_id, name)
                drive_alias = sub.get("drive_alias") or name.replace("drive:", "")
                actual_drive_id = needed_drives.get(drive_alias)
                if actual_drive_id:
                    try:
                        new_sub = register_subscription(actual_drive_id, drive_alias)
                        subs[name] = {
                            "id": new_sub["id"],
                            "expiration_dt": new_sub["expirationDateTime"],
                            "resource": new_sub["resource"],
                            "drive_id": actual_drive_id,
                            "drive_alias": drive_alias,
                        }
                    except Exception as exc2:
                        log.error("Re-registration failed for %s: %s", name, exc2)
    _save_subscriptions(subs)


# ---------------------------------------------------------------------------
# Delta sync — enumerate all changes since last token
# ---------------------------------------------------------------------------

def _get_folder_config_by_drive_item(drive_id: str, item_id: str,
                                      folders: list[dict]) -> dict | None:
    """Find folder config matching a drive_id + item_id pair."""
    for f in folders:
        if f["drive_id"] == drive_id and f["item_id"] == item_id:
            return f
    return None


def sync_delta(folder_name: str | None = None) -> None:
    """Enumerate changes since the last delta token and process each one.

    If folder_name is given, only syncs that folder. Otherwise syncs all.
    On first run (no token), fetches the current state and stores the delta
    link for future incremental syncs — no files are processed on first run
    (prevents re-ingesting everything on startup).
    """
    folders = get_watched_folders()
    delta_tokens = _load_delta_tokens()

    for folder in folders:
        if folder_name and folder["name"] != folder_name:
            continue

        drive_id = folder["drive_id"]
        item_id = folder["item_id"]
        key = f"{drive_id}:{item_id}"

        if key in delta_tokens:
            # Incremental: follow the stored delta link
            url = delta_tokens[key]
            log.info("Delta sync %s — following stored delta link", folder["name"])
            first_run = False
        else:
            # First run: fetch current state to seed the delta token
            url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/delta"
            log.info("Delta sync %s — first run, seeding delta token (no files processed)", folder["name"])
            first_run = True

        changes_processed = 0
        while url:
            data = _graph_get(url)
            items = data.get("value", [])

            if not first_run:
                for item in items:
                    _process_delta_item(item, folder, drive_id)
                    changes_processed += 1

            next_link = data.get("@odata.nextLink")
            delta_link = data.get("@odata.deltaLink")

            if delta_link:
                delta_tokens[key] = delta_link
                _save_delta_tokens(delta_tokens)
                url = None
            elif next_link:
                url = next_link
            else:
                url = None

        if first_run:
            log.info("Delta seed complete for %s — delta token stored.", folder["name"])
        else:
            log.info("Delta sync complete for %s — %d changes processed.", folder["name"], changes_processed)


def _process_delta_item(item: dict, folder: dict, drive_id: str) -> None:
    """Process a single delta item (created, modified, or deleted)."""
    item_id = item.get("id")
    name = item.get("name", "")
    deleted = item.get("deleted") is not None

    # Skip folders themselves (we only care about files)
    if item.get("folder") and not deleted:
        return

    if deleted:
        log.info("Delta: deleted item %s in %s", item_id, folder["name"])
        forget_file(sp_item_id=item_id, source_path=f"sharepoint:{item_id}")
    else:
        if should_exclude(name, folder):
            log.info("Delta: skipping excluded file %s", name)
            return
        ext = Path(name).suffix.lstrip(".").lower()
        if ext not in ("pdf", "docx", "pptx"):
            log.debug("Delta: skipping unsupported extension %s (%s)", ext, name)
            return
        log.info("Delta: ingesting %s in %s", name, folder["name"])
        ingest_file(item_id=item_id, drive_id=drive_id, folder_config=folder,
                    item_name=name, item_drive_item=item)


# ---------------------------------------------------------------------------
# Notification processing — called by POST /graph/notifications
# ---------------------------------------------------------------------------

def process_notification(payload: dict) -> None:
    """Handle a raw Graph change-notification payload.

    Called asynchronously by the FastAPI endpoint after the 202 response
    has already been sent.

    Since Graph subscriptions are registered at the drive root level (one per
    drive), notifications identify the drive via clientState. On receipt we run
    delta sync for every watched folder belonging to that drive — delta already
    tracks what changed since the last token, so this is safe and idempotent.
    """
    config = load_config()
    drive_ids = config["drive_ids"]  # alias → actual drive_id
    folders = get_watched_folders(config)

    # Build map: actual_drive_id → [folder, ...]
    folders_by_drive: dict[str, list[dict]] = {}
    for folder in folders:
        folders_by_drive.setdefault(folder["drive_id"], []).append(folder)

    seen_drives: set[str] = set()

    for notification in payload.get("value", []):
        client_state = notification.get("clientState", "")

        # Validate clientState to reject spoofed notifications
        if not client_state.startswith("synexis-rep-agent:"):
            log.warning("Notification with unexpected clientState: %s — ignored", client_state)
            continue

        # clientState format: "synexis-rep-agent:drive:{alias}"
        drive_alias = client_state.replace("synexis-rep-agent:drive:", "").replace("synexis-rep-agent:", "")
        actual_drive_id = drive_ids.get(drive_alias)
        if not actual_drive_id:
            # Fall back: try treating the suffix as a literal drive_id
            actual_drive_id = drive_alias

        if actual_drive_id in seen_drives:
            continue  # Already processing this drive in this payload
        seen_drives.add(actual_drive_id)

        drive_folders = folders_by_drive.get(actual_drive_id, [])
        if not drive_folders:
            log.warning("No watched folders found for drive %s (alias: %s)", actual_drive_id, drive_alias)
            continue

        log.info("Notification for drive %s — running delta on %d folder(s)",
                 drive_alias, len(drive_folders))
        for folder in drive_folders:
            try:
                sync_delta(folder_name=folder["name"])
            except Exception as exc:
                log.error("Delta sync failed for %s after notification: %s", folder["name"], exc)


# ---------------------------------------------------------------------------
# File ingestion — download → extract → chunk → embed → upsert
# ---------------------------------------------------------------------------

def _download_item(drive_id: str, item_id: str, dest_path: Path) -> None:
    """Download a Drive item's content to dest_path."""
    # Graph download URL: GET /drives/{drive_id}/items/{item_id}/content
    token = _get_graph_token()
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=60)
    resp.raise_for_status()
    with dest_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)


def _extract_from_path(abs_path: Path, extension: str) -> tuple[str, list]:
    """Run the appropriate extractor on a local file. Returns (extractor_name, [PageText])."""
    # Import extractors from extract.py — reuse existing logic
    from pipeline.extract import _extract_pdf, _extract_docx, _extract_pptx, EXTRACTORS
    fn = EXTRACTORS.get(extension)
    if fn is None:
        raise ValueError(f"No extractor for .{extension}")
    return fn(abs_path)


def _doc_id_for_sp(sp_item_id: str) -> str:
    return "sp_" + hashlib.sha1(sp_item_id.encode()).hexdigest()[:14]


def _build_sp_vector(chunk_data: dict, embedding: list[float]) -> dict:
    """Build a Pinecone vector dict for a SharePoint-sourced chunk."""
    text = chunk_data["text"]
    if len(text) > MAX_METADATA_TEXT_CHARS:
        text = text[:MAX_METADATA_TEXT_CHARS]

    metadata: dict[str, Any] = {
        # Standard fields (match embed_load.py schema)
        "source": chunk_data.get("source", chunk_data.get("file_path", "")),
        "file_path": chunk_data["file_path"],
        "doc_id": chunk_data["doc_id"],
        "chunk_index": chunk_data["chunk_index"],
        "source_category": chunk_data.get("source_category", ""),
        "intake_mode": "sharepoint",
        "page_or_slide": chunk_data.get("page_or_slide", 1),
        "has_efficacy_claim": chunk_data.get("has_efficacy_claim", False),
        "has_material_compatibility": chunk_data.get("has_material_compatibility", False),
        "extension": chunk_data.get("extension", ""),
        "extractor_used": chunk_data.get("extractor_used", ""),
        "token_count": chunk_data.get("token_count", 0),
        "text": text,
        # SharePoint-specific fields
        "sp_item_id": chunk_data["sp_item_id"],
        "sp_drive_id": chunk_data.get("sp_drive_id", ""),
        "tier": chunk_data.get("tier", 3),
        "folder_name": chunk_data.get("folder_name", ""),
        "share_url": chunk_data.get("share_url", ""),
    }
    return {"id": chunk_data["chunk_id"], "values": embedding, "metadata": metadata}


def _voyage_embed(texts: list[str]) -> list[list[float]]:
    import voyageai
    client = voyageai.Client(api_key=VOYAGE_API_KEY)
    result = client.embed(texts, model=VOYAGE_MODEL, input_type="document")
    return result.embeddings


def _pinecone_index():
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    return pc.Index(PINECONE_INDEX_NAME)


def _pinecone_delete_by_filter(filter_dict: dict) -> int:
    """Delete all vectors matching a metadata filter. Returns approximate count deleted."""
    index = _pinecone_index()
    # Pinecone serverless supports delete by metadata filter
    index.delete(filter=filter_dict)
    return -1  # Pinecone doesn't return count on filtered delete


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def _upsert_batch_vecs(index, vectors: list[dict]) -> None:
    index.upsert(vectors=vectors)


def ingest_file(
    item_id: str,
    drive_id: str,
    folder_config: dict,
    item_name: str,
    item_drive_item: dict | None = None,
) -> dict:
    """Download a SharePoint file, extract, chunk, embed, and upsert to Pinecone.

    Also creates a HubSpot Documents link for Tier 1/2 content and stores
    the share_url in chunk metadata.

    Returns a summary dict.
    """
    import tiktoken
    from dataclasses import asdict
    from pipeline.chunk import chunk_doc, TOKENIZER

    ext = Path(item_name).suffix.lstrip(".").lower()
    if ext not in ("pdf", "docx", "pptx"):
        return {"skipped": True, "reason": f"unsupported_extension:{ext}", "item_id": item_id}

    # Before ingesting, delete any existing vectors for this item (handles updates)
    forget_file(sp_item_id=item_id, source_path=None, skip_hubspot=True)

    tier = tier_for_item(item_name, folder_config)
    doc_id = _doc_id_for_sp(item_id)
    source_path = f"sharepoint/{folder_config['name']}/{item_name}"
    folder_name = folder_config["name"]

    # ---------- Download ----------
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = Path(tmp_dir) / item_name
        log.info("Downloading %s (item %s) from drive %s", item_name, item_id, drive_id)
        _download_item(drive_id, item_id, tmp_file)

        # ---------- Extract ----------
        try:
            extractor_used, pages = _extract_from_path(tmp_file, ext)
        except Exception as exc:
            log.error("Extraction failed for %s: %s", item_name, exc)
            return {"skipped": True, "reason": f"extraction_error:{exc}", "item_id": item_id}

        total_chars = sum(len(p.text) for p in pages)
        if total_chars < MIN_CHARS_FOR_INGEST:
            log.warning("Skipping %s — only %d chars extracted (needs OCR?)", item_name, total_chars)
            return {"skipped": True, "reason": "near_empty", "item_id": item_id, "chars": total_chars}

        # ---------- Build doc_json for chunk.chunk_doc() ----------
        doc_json = {
            "doc_id": doc_id,
            "file_path": source_path,
            "source_category": folder_name,
            "intake_mode": "sharepoint",
            "description": item_name,
            "extension": ext,
            "extractor_used": extractor_used,
            "total_chars": total_chars,
            "pages": [{"number": p.number, "text": p.text} for p in pages],
        }

        # ---------- Chunk ----------
        chunks = chunk_doc(doc_json)
        if not chunks:
            log.warning("No chunks produced for %s", item_name)
            return {"skipped": True, "reason": "no_chunks", "item_id": item_id}

    # ---------- HubSpot Documents link ----------
    share_url = ""
    if tier in (1, 2):
        try:
            share_url = _ensure_hubspot_doc(item_id, item_name, drive_id, doc_id)
        except Exception as exc:
            log.warning("HubSpot Documents link failed for %s: %s (continuing without link)", item_name, exc)

    # ---------- Build chunk records with SP metadata ----------
    chunk_records = []
    for c in chunks:
        from dataclasses import asdict
        d = asdict(c)
        d["sp_item_id"] = item_id
        d["sp_drive_id"] = drive_id
        d["tier"] = tier
        d["folder_name"] = folder_name
        d["share_url"] = share_url
        chunk_records.append(d)

    # ---------- Batch embed + upsert ----------
    total_embedded = 0
    total_upserted = 0
    index = _pinecone_index()

    # Group into token-bounded batches
    batches: list[list[dict]] = []
    cur: list[dict] = []
    cur_tokens = 0
    for cr in chunk_records:
        tok = int(cr.get("token_count") or 0)
        if cur and (cur_tokens + tok > EMBED_BATCH_MAX_TOKENS or len(cur) >= EMBED_BATCH_MAX_CHUNKS):
            batches.append(cur)
            cur, cur_tokens = [], 0
        cur.append(cr)
        cur_tokens += tok
    if cur:
        batches.append(cur)

    last_embed_at = 0.0
    pending: list[tuple[dict, list[float]]] = []

    for batch in batches:
        texts = [cr["text"] for cr in batch]
        elapsed = time.time() - last_embed_at
        if last_embed_at and elapsed < EMBED_INTER_CALL_SECONDS:
            time.sleep(EMBED_INTER_CALL_SECONDS - elapsed)

        embeddings = _voyage_embed(texts)
        last_embed_at = time.time()
        total_embedded += len(embeddings)

        for cr, emb in zip(batch, embeddings):
            pending.append((cr, emb))

        while len(pending) >= UPSERT_BATCH:
            take = pending[:UPSERT_BATCH]
            pending = pending[UPSERT_BATCH:]
            vectors = [_build_sp_vector(cr, emb) for cr, emb in take]
            _upsert_batch_vecs(index, vectors)
            total_upserted += len(vectors)

    if pending:
        vectors = [_build_sp_vector(cr, emb) for cr, emb in pending]
        _upsert_batch_vecs(index, vectors)
        total_upserted += len(vectors)

    log.info("Ingested %s: %d chunks embedded, %d upserted (tier %d, share_url=%s)",
             item_name, total_embedded, total_upserted, tier, share_url or "none")

    # Clear API response cache so reps see updated content immediately
    _clear_api_cache()

    return {
        "ok": True,
        "item_id": item_id,
        "item_name": item_name,
        "folder": folder_name,
        "tier": tier,
        "chunks_embedded": total_embedded,
        "chunks_upserted": total_upserted,
        "share_url": share_url,
    }


# ---------------------------------------------------------------------------
# Forget a file — delete Pinecone vectors + HubSpot Documents link
# ---------------------------------------------------------------------------

def forget_file(sp_item_id: str, source_path: str | None, skip_hubspot: bool = False) -> dict:
    """Delete all Pinecone vectors for a SharePoint item and clean up HubSpot link.

    Uses a metadata filter on sp_item_id for precise, targeted deletion.
    """
    log.info("Forgetting item %s (source: %s)", sp_item_id, source_path)

    # Delete from Pinecone by sp_item_id metadata filter
    try:
        _pinecone_delete_by_filter({"sp_item_id": {"$eq": sp_item_id}})
        log.info("Pinecone vectors deleted for sp_item_id=%s", sp_item_id)
    except Exception as exc:
        log.error("Pinecone delete failed for %s: %s", sp_item_id, exc)

    # Remove HubSpot Files entry from registry
    hubspot_removed = False
    if not skip_hubspot:
        registry = _load_hubspot_registry()
        if sp_item_id in registry:
            doc_entry = registry.pop(sp_item_id)
            _save_hubspot_registry(registry)
            hs_file_id = doc_entry.get("hs_file_id")
            if hs_file_id:
                try:
                    _delete_hubspot_file(hs_file_id)
                    hubspot_removed = True
                    log.info("HubSpot file %s deleted", hs_file_id)
                except Exception as exc:
                    log.error("HubSpot file delete failed for %s: %s", hs_file_id, exc)

    # Clear API response cache
    _clear_api_cache()

    return {"ok": True, "sp_item_id": sp_item_id, "hubspot_removed": hubspot_removed}


# ---------------------------------------------------------------------------
# HubSpot Documents link registry
# ---------------------------------------------------------------------------

def _load_hubspot_registry() -> dict:
    if HUBSPOT_REGISTRY_PATH.exists():
        try:
            return json.loads(HUBSPOT_REGISTRY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_hubspot_registry(registry: dict) -> None:
    HUBSPOT_REGISTRY_PATH.write_text(json.dumps(registry, indent=2), encoding="utf-8")


def _ensure_hubspot_doc(sp_item_id: str, item_name: str, drive_id: str, doc_id: str) -> str:
    """Upload a SharePoint file to HubSpot File Manager and return its public URL.

    Checks the registry first to avoid duplicate uploads. Files are uploaded as
    PUBLIC_NOT_INDEXABLE — accessible via link, not indexed by search engines.
    This gives reps a reliable hubspotusercontent.com URL that clears corporate
    firewalls without requiring HubSpot Sales Hub Documents.
    """
    registry = _load_hubspot_registry()
    if sp_item_id in registry:
        return registry[sp_item_id].get("share_url", "")

    # Download file from SharePoint to temp dir, then upload to HubSpot
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file = Path(tmp_dir) / item_name
        _download_item(drive_id, sp_item_id, tmp_file)

        with tmp_file.open("rb") as fh:
            upload_resp = requests.post(
                "https://api.hubapi.com/files/v3/files",
                headers={"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"},
                files={"file": (item_name, fh)},
                data={
                    "folderPath": "/Rep Agent Content",
                    "options": json.dumps({
                        "access": "PUBLIC_NOT_INDEXABLE",
                        "duplicateValidationStrategy": "RETURN_EXISTING",
                        "duplicateValidationScope": "ENTIRE_PORTAL",
                    }),
                },
                timeout=120,
            )

    if not upload_resp.ok:
        log.error("HubSpot file upload failed: %s %s", upload_resp.status_code, upload_resp.text[:300])
        return ""

    file_data = upload_resp.json()
    hs_file_id = file_data.get("id")
    share_url = file_data.get("defaultHostingUrl") or file_data.get("url") or ""

    if not hs_file_id:
        log.error("HubSpot upload returned no file id: %s", file_data)
        return ""

    log.info("HubSpot file uploaded: id=%s url=%s", hs_file_id, share_url)

    # Persist to registry
    registry = _load_hubspot_registry()
    registry[sp_item_id] = {
        "hs_file_id": hs_file_id,
        "share_url": share_url,
        "title": item_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_hubspot_registry(registry)
    return share_url


def _delete_hubspot_file(hs_file_id: str) -> None:
    """Delete a file from HubSpot File Manager by file ID."""
    url = f"https://api.hubapi.com/files/v3/files/{hs_file_id}"
    resp = requests.delete(
        url,
        headers={"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"},
        timeout=30,
    )
    if not resp.ok and resp.status_code != 404:
        resp.raise_for_status()


# ---------------------------------------------------------------------------
# API cache clear — notify Render backend to bust response cache
# ---------------------------------------------------------------------------

def _clear_api_cache() -> None:
    """Tell the FastAPI backend to clear its in-memory query cache after any ingest/forget."""
    api_url = os.getenv("REP_AGENT_API_URL", "https://synexis-rep-agent.onrender.com")
    # Use the internal PARTNER_KEY if set, else skip (local dev with no auth)
    partner_key = os.getenv("PIPELINE_PARTNER_KEY", "")
    headers = {}
    if partner_key:
        headers["Authorization"] = f"Bearer {partner_key}"
    try:
        resp = requests.post(f"{api_url}/cache/clear", headers=headers, timeout=10)
        if resp.ok:
            log.debug("API cache cleared (%d entries)", resp.json().get("entries_cleared", 0))
        else:
            log.warning("Cache clear returned %d: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("Could not clear API cache: %s", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s — %(message)s")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SharePoint → Pinecone sync pipeline")
    ap.add_argument("--register", action="store_true", help="Register Graph webhook subscriptions")
    ap.add_argument("--force", action="store_true", help="Force re-register even if subscription exists")
    ap.add_argument("--renew", action="store_true", help="Renew all webhook subscriptions")
    ap.add_argument("--delta", action="store_true", help="Run delta sync on watched folders")
    ap.add_argument("--folder", type=str, default=None, help="Limit --delta to a specific folder by name")
    ap.add_argument("--ingest-item", nargs=3, metavar=("DRIVE_ID", "ITEM_ID", "FOLDER_NAME"),
                    help="Manually ingest a single SharePoint item")
    ap.add_argument("--forget-item", metavar="SP_ITEM_ID",
                    help="Delete Pinecone vectors for a SharePoint item ID")
    ap.add_argument("--list-subscriptions", action="store_true", help="Print current subscription state")
    ap.add_argument("--list-config", action="store_true", help="Print confirmed watched folders")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    _setup_logging(args.verbose)

    if args.register:
        results = register_all_subscriptions(force=args.force)
        print(json.dumps(results, indent=2, default=str))

    elif args.renew:
        renew_all_subscriptions()
        print("Renewal complete.")

    elif args.delta:
        sync_delta(folder_name=args.folder)

    elif args.ingest_item:
        drive_id, item_id, folder_name = args.ingest_item
        folders = {f["name"]: f for f in get_watched_folders()}
        folder = folders.get(folder_name)
        if not folder:
            print(f"ERROR: No confirmed folder config named '{folder_name}'.")
            print("Available:", list(folders.keys()))
            return 1
        item = _graph_get(f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}")
        result = ingest_file(
            item_id=item_id,
            drive_id=drive_id,
            folder_config=folder,
            item_name=item.get("name", item_id),
            item_drive_item=item,
        )
        print(json.dumps(result, indent=2, default=str))

    elif args.forget_item:
        result = forget_file(sp_item_id=args.forget_item, source_path=None)
        print(json.dumps(result, indent=2, default=str))

    elif args.list_subscriptions:
        subs = _load_subscriptions()
        print(json.dumps(subs, indent=2, default=str))

    elif args.list_config:
        folders = get_watched_folders()
        for f in folders:
            print(f"  [{f.get('default_tier')}] {f['name']}  drive={f['drive_id'][:20]}...  item={f['item_id']}")
        skipped = [f for f in load_config()["watched_folders"] if f.get("item_id") == "TBD"]
        if skipped:
            print("\n  PENDING (TBD):")
            for f in skipped:
                print(f"    {f['name']}")
    else:
        ap.print_help()

    return 0


if __name__ == "__main__":
    sys.exit(main())
