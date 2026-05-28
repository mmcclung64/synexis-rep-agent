"""
Resource library sync: SharePoint → HubSpot Files → resources_manifest.json

Pulls files from the configured SharePoint folder, uploads them to HubSpot
File Manager as PUBLIC_NOT_INDEXABLE, and writes a manifest JSON for the
resources.synexis.com page to consume.

Intentionally separate from sharepoint_sync.py / watched_folders.json —
this serves the resource-library use case (file serving + download tracking),
not the RAG/embedding pipeline. Auth and download primitives are imported
directly from sharepoint_sync to avoid duplication.

Usage (from repo root):
    python3 -m pipeline.sync_resources                        # dry run — lists what would change
    python3 -m pipeline.sync_resources --confirm              # upload new/changed files
    python3 -m pipeline.sync_resources --confirm \\
        --manifest-output /path/to/resources_manifest.json   # custom manifest path

Config:   pipeline/resources_config.json
Registry: logs/resources_registry.json  (auto-created)
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "pipeline" / "resources_config.json"
LOGS_DIR = REPO_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Reuse auth + download primitives from sharepoint_sync
# ---------------------------------------------------------------------------
from pipeline.sharepoint_sync import (  # noqa: E402
    _get_graph_token,
    _graph_get,
    _download_item,
    load_config as _load_sp_config,
    HUBSPOT_ACCESS_TOKEN,
    GRAPH_BASE,
)

# ---------------------------------------------------------------------------
# Config & registry
# ---------------------------------------------------------------------------

def load_resources_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _registry_path(config: dict) -> Path:
    return REPO_ROOT / config.get("registry_path", "logs/resources_registry.json")


def _load_registry(config: dict) -> dict:
    p = _registry_path(config)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_registry(config: dict, registry: dict) -> None:
    p = _registry_path(config)
    p.write_text(json.dumps(registry, indent=2, ensure_ascii=False), encoding="utf-8")


def _delta_tokens_path(config: dict) -> Path:
    base = config.get("registry_path", "logs/resources_registry.json")
    return REPO_ROOT / Path(base).parent / "resources_delta_tokens.json"


def _load_delta_tokens(config: dict) -> dict:
    p = _delta_tokens_path(config)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_delta_tokens(config: dict, tokens: dict) -> None:
    p = _delta_tokens_path(config)
    p.write_text(json.dumps(tokens, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# SharePoint listing
# ---------------------------------------------------------------------------

def _resolve_drive_id(config: dict) -> str:
    """Resolve drive alias → actual drive ID using watched_folders.json drive_ids."""
    sp_config = _load_sp_config()
    drive_ids = sp_config.get("drive_ids", {})
    alias = config["sharepoint"]["drive_alias"]
    return drive_ids.get(alias, alias)


def _list_children(drive_id: str, item_id: str) -> list[dict]:
    url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/children"
    return _graph_get(url).get("value", [])


def _resolve_url_shortcut(
    shortcut_drive_id: str,
    shortcut_item_id: str,
    drive_ids: dict,
) -> tuple[str, str] | None:
    """
    Read a Windows .url shortcut file and resolve it to (real_drive_id, real_item_id).
    The shortcuts in the Vincit folder point to files on the MarketingSite drive.
    Returns None if the target cannot be resolved.
    """
    token = _get_graph_token()
    content_resp = requests.get(
        f"{GRAPH_BASE}/drives/{shortcut_drive_id}/items/{shortcut_item_id}/content",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    url_match = re.search(r"URL=(.+)", content_resp.text)
    if not url_match:
        return None

    sp_url = url_match.group(1).strip()
    # Extract relative path after /Shared%20Documents/
    path_match = re.search(r"/Shared%20Documents/(.+?)(?:\?|$)", sp_url)
    if not path_match:
        return None
    rel_path = unquote(path_match.group(1))

    # Determine which drive hosts the target (match by site name in URL)
    target_drive_id = None
    for alias, actual_id in drive_ids.items():
        if alias.lower() in sp_url.lower():
            target_drive_id = actual_id
            break
    if not target_drive_id:
        # Fall back to MarketingSite (where collateral lives)
        target_drive_id = drive_ids.get("MarketingSite")
    if not target_drive_id:
        return None

    # Look up the item by path on the target drive
    encoded = requests.utils.quote(rel_path)
    lookup = requests.get(
        f"{GRAPH_BASE}/drives/{target_drive_id}/root:/{encoded}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    data = lookup.json()
    if "error" in data:
        return None
    return target_drive_id, data["id"]


def _list_folder_recursive(
    drive_id: str,
    item_id: str,
    subfolder_name: str | None = None,
    drive_ids: dict | None = None,
) -> list[dict]:
    """
    Recursively list all files. Returns flat list; each item has '_subfolder' added.
    .url shortcut files are resolved to their real targets on the source drive.
    """
    result = []
    for item in _list_children(drive_id, item_id):
        if "folder" in item:
            result.extend(
                _list_folder_recursive(
                    drive_id, item["id"],
                    subfolder_name=item["name"],
                    drive_ids=drive_ids,
                )
            )
        else:
            item = dict(item)
            name = item["name"]
            # Resolve Windows URL shortcuts (.url files)
            if name.lower().endswith(".url") and drive_ids:
                resolved = _resolve_url_shortcut(drive_id, item["id"], drive_ids)
                if resolved:
                    real_drive_id, real_item_id = resolved
                    real_item = requests.get(
                        f"{GRAPH_BASE}/drives/{real_drive_id}/items/{real_item_id}",
                        headers={"Authorization": f"Bearer {_get_graph_token()}"},
                        timeout=15,
                    ).json()
                    if "error" not in real_item:
                        real_item = dict(real_item)
                        real_item["_subfolder"] = subfolder_name
                        real_item["_real_drive_id"] = real_drive_id
                        real_item["_shortcut_sp_id"] = item["id"]
                        result.append(real_item)
                        continue
                # If resolution fails, skip the .url file
                print(f"  [WARN] Could not resolve shortcut: {name}")
                continue
            item["_subfolder"] = subfolder_name
            item["_real_drive_id"] = drive_id
            item["_shortcut_sp_id"] = None
            result.append(item)
    return result


# ---------------------------------------------------------------------------
# Display name cleaning
# ---------------------------------------------------------------------------

_DATE_PAT    = re.compile(r"[_\s]\d{4}(?:_\d{2})?(?=[_\s]|$)")   # _0526, _0925_Final
_FINAL_PAT   = re.compile(r"[_\s]+FINAL[_\s]*", re.IGNORECASE)
_ENG_PAT     = re.compile(r"[_\s]+ENG[_\s]*", re.IGNORECASE)
_MULTI_SPACE = re.compile(r"\s{2,}")

# Explicit overrides — checked first, before any regex cleaning.
# Add entries here whenever auto-cleaning produces something ambiguous.
_DISPLAY_OVERRIDES: dict[str, str] = {
    "FP_Protein_One Pager_0925_Final.pdf": "Food Processing — Protein One Pager",
}


def _clean_display_name(filename: str) -> str:
    """Convert a raw filename to a human-readable display label."""
    if filename in _DISPLAY_OVERRIDES:
        return _DISPLAY_OVERRIDES[filename]
    stem = Path(filename).stem
    name = _DATE_PAT.sub("", stem)
    name = _FINAL_PAT.sub(" ", name)
    name = _ENG_PAT.sub(" (English) ", name)
    name = name.replace("_", " ")
    name = _MULTI_SPACE.sub(" ", name).strip()
    return name


# ---------------------------------------------------------------------------
# HubSpot File Manager upload / delete
# ---------------------------------------------------------------------------

def _upload_to_hubspot(
    local_path: Path,
    filename: str,
    folder_path: str,
) -> tuple[str, str]:
    """Upload a file. Returns (hs_file_id, public_url)."""
    with local_path.open("rb") as fh:
        resp = requests.post(
            "https://api.hubapi.com/files/v3/files",
            headers={"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"},
            files={"file": (filename, fh)},
            data={
                "folderPath": folder_path,
                "options": json.dumps({
                    "access": "PUBLIC_NOT_INDEXABLE",
                    "duplicateValidationStrategy": "RETURN_EXISTING",
                    "duplicateValidationScope": "ENTIRE_PORTAL",
                }),
            },
            timeout=180,  # large PPTX files need generous timeout
        )
    resp.raise_for_status()
    data = resp.json()
    hs_file_id = data.get("id", "")
    url = data.get("defaultHostingUrl") or data.get("url") or ""
    return hs_file_id, url


def _delete_hubspot_file(hs_file_id: str) -> None:
    requests.delete(
        f"https://api.hubapi.com/files/v3/files/{hs_file_id}",
        headers={"Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"},
        timeout=30,
    )


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------

def _is_excluded(filename: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(filename, pat) for pat in patterns)


def _needs_update(item: dict, registry: dict) -> bool:
    """True if item is new or modified since last sync."""
    sp_id = item["id"]
    if sp_id not in registry:
        return True
    return item.get("lastModifiedDateTime") != registry[sp_id].get("sp_modified")


def _process_file_item(
    item: dict,
    registry: dict,
    hs_folder: str,
    config: dict,
    confirm: bool,
    fallback_drive_id: str,
) -> None:
    """Upload or skip one file item; update registry if uploaded."""
    sp_id     = item["id"]
    filename  = item["name"]
    ext       = Path(filename).suffix.lower().lstrip(".")
    size      = item.get("size", 0)
    modified  = item.get("lastModifiedDateTime", "")
    subfolder = item.get("_subfolder")
    real_drive = item.get("_real_drive_id", fallback_drive_id)
    shortcut_sp_id = item.get("_shortcut_sp_id")

    if not _needs_update(item, registry):
        print(f"  [OK]  {filename}")
        return

    action = "Upload" if confirm else "Would upload"
    print(f"  [{action}] {filename}  ({size:,} bytes)"
          + (f"  [subfolder: {subfolder}]" if subfolder else ""))

    if not confirm:
        return

    if sp_id in registry and registry[sp_id].get("hs_file_id"):
        try:
            _delete_hubspot_file(registry[sp_id]["hs_file_id"])
        except Exception as exc:
            print(f"    [WARN] Could not delete old HubSpot file: {exc}")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp) / filename
        _download_item(real_drive, sp_id, tmp_path)
        hs_file_id, hubspot_url = _upload_to_hubspot(tmp_path, filename, hs_folder)

    registry[sp_id] = {
        "hs_file_id":       hs_file_id,
        "hubspot_url":      hubspot_url,
        "filename":         filename,
        "display_name":     _clean_display_name(filename),
        "extension":        ext,
        "size_bytes":       size,
        "subfolder":        subfolder,
        "shortcut_sp_id":   shortcut_sp_id,
        "sp_modified":      modified,
        "synced_at":        datetime.now(timezone.utc).isoformat(),
    }
    _save_registry(config, registry)
    print(f"    → {hubspot_url}")


def _seed_delta_token(
    drive_id: str,
    item_id: str,
    key: str,
    delta_tokens: dict,
    config: dict,
) -> None:
    """Walk the delta endpoint to capture the current-state token (no processing)."""
    url: str | None = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/delta"
    while url:
        data = _graph_get(url)
        delta_link = data.get("@odata.deltaLink")
        if delta_link:
            delta_tokens[key] = delta_link
            _save_delta_tokens(config, delta_tokens)
            print("[sync_resources] Delta token seeded.")
            url = None
        else:
            url = data.get("@odata.nextLink")


def _sync_delta(
    drive_id: str,
    item_id: str,
    key: str,
    drive_ids: dict,
    registry: dict,
    hs_folder: str,
    config: dict,
    confirm: bool,
    delta_tokens: dict,
) -> None:
    """Follow the stored delta link and process only additions, changes, deletions."""
    # Reverse map: shortcut_sp_id → real item ID for deletion handling
    shortcut_to_real = {
        v["shortcut_sp_id"]: k
        for k, v in registry.items()
        if v.get("shortcut_sp_id")
    }

    url: str | None = delta_tokens[key]
    changes = 0

    while url:
        data = _graph_get(url)

        for item in data.get("value", []):
            if "folder" in item:
                continue

            # Deletion
            if item.get("deleted"):
                sp_id   = item["id"]
                real_id = shortcut_to_real.get(sp_id, sp_id)
                if real_id in registry:
                    print(f"  [Removed] {registry[real_id]['filename']}")
                    if confirm:
                        hs_id = registry[real_id].get("hs_file_id")
                        if hs_id:
                            try:
                                _delete_hubspot_file(hs_id)
                            except Exception as exc:
                                print(f"    [WARN] HubSpot delete failed: {exc}")
                        del registry[real_id]
                        _save_registry(config, registry)
                    changes += 1
                continue

            # .url shortcuts — resolve to the real file
            if item["name"].lower().endswith(".url"):
                resolved = _resolve_url_shortcut(drive_id, item["id"], drive_ids)
                if not resolved:
                    print(f"  [WARN] Could not resolve shortcut: {item['name']}")
                    continue
                real_drive_id, real_item_id = resolved
                token = _get_graph_token()
                real_item = requests.get(
                    f"{GRAPH_BASE}/drives/{real_drive_id}/items/{real_item_id}",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=15,
                ).json()
                if "error" in real_item:
                    continue
                real_item = dict(real_item)
                real_item["_real_drive_id"]   = real_drive_id
                real_item["_shortcut_sp_id"]  = item["id"]
                # Preserve subfolder from existing registry entry if known
                real_item["_subfolder"] = registry.get(real_item["id"], {}).get("subfolder")
            else:
                real_item = dict(item)
                real_item["_real_drive_id"]  = drive_id
                real_item["_shortcut_sp_id"] = None
                real_item["_subfolder"] = registry.get(item["id"], {}).get("subfolder")

            _process_file_item(real_item, registry, hs_folder, config, confirm, drive_id)
            changes += 1

        delta_link = data.get("@odata.deltaLink")
        if delta_link:
            delta_tokens[key] = delta_link
            _save_delta_tokens(config, delta_tokens)
            url = None
        else:
            url = data.get("@odata.nextLink")

    print(f"[sync_resources] Delta sync complete — {changes} change(s) processed.")


def sync(config: dict, confirm: bool = False) -> list[dict]:
    """
    Main entry point. Returns registry entries for manifest generation.

    First run:   full recursive scan (preserves subfolder context) → upload all
                 → seed delta token for future incremental runs.
    Subsequent:  follow stored delta link → process only additions/changes/deletions.
    """
    sp_cfg    = config["sharepoint"]
    hs_folder = config["hubspot"]["folder_path"]
    excludes  = sp_cfg.get("exclude_patterns", [])

    drive_id     = _resolve_drive_id(config)
    drive_ids    = _load_sp_config().get("drive_ids", {})
    registry     = _load_registry(config)
    delta_tokens = _load_delta_tokens(config)

    item_id = sp_cfg["item_id"]
    key     = f"{drive_id}:{item_id}"

    if key not in delta_tokens:
        # First run: full scan + seed
        all_items = _list_folder_recursive(drive_id, item_id, drive_ids=drive_ids)
        all_items = [i for i in all_items if not _is_excluded(i["name"], excludes)]
        print(f"[sync_resources] First run — {len(all_items)} files found")
        for item in all_items:
            _process_file_item(item, registry, hs_folder, config, confirm, drive_id)
        if confirm:
            _seed_delta_token(drive_id, item_id, key, delta_tokens, config)
        else:
            print("[sync_resources] (Dry run — delta token will be seeded on first --confirm run)")
    else:
        _sync_delta(drive_id, item_id, key, drive_ids, registry, hs_folder, config, confirm, delta_tokens)

    return list(registry.values())


# ---------------------------------------------------------------------------
# Manifest output
# ---------------------------------------------------------------------------

def write_manifest(entries: list[dict], output_path: Path) -> None:
    """Write resources_manifest.json consumed by resources.html."""
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [
            {
                "filename":     e["filename"],
                "display_name": e["display_name"],
                "extension":    e["extension"],
                "size_bytes":   e["size_bytes"],
                "hubspot_url":  e["hubspot_url"],
                "subfolder":    e.get("subfolder"),
                "updated_at":   e.get("sp_modified", ""),
            }
            for e in entries
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n[sync_resources] Manifest written → {output_path}  ({len(manifest['files'])} files)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Sync SharePoint resource folder → HubSpot Files → manifest JSON."
    )
    ap.add_argument(
        "--confirm", action="store_true",
        help="Actually upload/replace files (default: dry run).",
    )
    ap.add_argument(
        "--manifest-output", type=Path,
        default=REPO_ROOT.parent.parent / "HubSpot-partner-resources" / "resources_manifest.json",
        help="Path to write resources_manifest.json (default: ../../HubSpot-partner-resources/resources_manifest.json).",
    )
    args = ap.parse_args(argv)

    config  = load_resources_config()
    entries = sync(config, confirm=args.confirm)

    if args.manifest_output:
        write_manifest(entries, args.manifest_output)

    if not args.confirm:
        n_pending = sum(
            1 for e in entries
            if not e.get("hubspot_url")  # not yet uploaded
        )
        print(f"\n[sync_resources] Dry run complete."
              f" {n_pending} file(s) pending upload. Pass --confirm to upload.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
