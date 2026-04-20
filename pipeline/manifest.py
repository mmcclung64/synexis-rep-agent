"""Parse source_content_manifest.md and expose the ingest worklist."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List


MANIFEST_PATH_DEFAULT = Path(
    os.path.expanduser("~/Desktop/Claude/synexis-bot/source_content_manifest.md")
)


@dataclass(frozen=True)
class ManifestEntry:
    relative_path: str
    status: str
    intake_mode: str
    description: str

    @property
    def source_category(self) -> str:
        parts = self.relative_path.split("/", 1)
        return parts[0] if len(parts) > 1 else "Root"

    @property
    def extension(self) -> str:
        return Path(self.relative_path).suffix.lower().lstrip(".")


def load_manifest(manifest_path: Path | None = None) -> List[ManifestEntry]:
    path = manifest_path or MANIFEST_PATH_DEFAULT
    text = path.read_text(encoding="utf-8")
    entries: List[ManifestEntry] = []
    for line in text.splitlines():
        if not line.startswith("| "):
            continue
        if line.startswith("| File |") or line.startswith("|---"):
            continue
        cols = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cols) < 6:
            continue
        relative_path, _size, _first_seen, status, intake_mode, description = cols[:6]
        if relative_path == "File":
            continue
        entries.append(
            ManifestEntry(
                relative_path=relative_path,
                status=status,
                intake_mode=intake_mode,
                description=description,
            )
        )
    return entries


def ingest_worklist(
    manifest_path: Path | None = None,
    extensions: tuple[str, ...] = ("pdf", "docx", "pptx"),
) -> List[ManifestEntry]:
    """Return the list of entries with status='ingested' and a supported extension."""
    return [
        e
        for e in load_manifest(manifest_path)
        if e.status == "ingested" and e.extension in extensions
    ]


if __name__ == "__main__":
    items = ingest_worklist()
    print(f"{len(items)} ingested entries")
    by_ext: dict[str, int] = {}
    by_cat: dict[str, int] = {}
    for e in items:
        by_ext[e.extension] = by_ext.get(e.extension, 0) + 1
        by_cat[e.source_category] = by_cat.get(e.source_category, 0) + 1
    print("by extension:", by_ext)
    print("by category:")
    for k, v in sorted(by_cat.items(), key=lambda kv: -kv[1]):
        print(f"  {v:4d}  {k}")
