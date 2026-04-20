"""One-time backfill: tag existing Pinecone chunks with has_material_compatibility.

CODE_BRIEFING Retrieval Tuning #2 (option 3). Adds a metadata tag to chunks
that discuss equipment-material compatibility so the retrieval layer can
widen the candidate pool via a Pinecone $eq:true filter when the incoming
query is about materials.

Process:
  1. Read work/chunks.jsonl (source of truth for BM25 + what's in Pinecone).
  2. Classify each chunk with pipeline.chunk.has_material_compatibility.
  3. For positives: call index.update(id, set_metadata={..._compatibility: True}).
     Negatives are left absent — Pinecone's $eq:true filter treats that as false.
  4. Rewrite work/chunks.jsonl with the tag so BM25 corpus stays in sync.

Usage:
    python3 -m pipeline.backfill_material_tag           # dry run (counts only)
    python3 -m pipeline.backfill_material_tag --confirm # actually update Pinecone + chunks.jsonl
    python3 -m pipeline.backfill_material_tag --sample N  # print N positive chunks for spot-check
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from tqdm import tqdm

from pipeline.chunk import CHUNKS_PATH, has_material_compatibility


load_dotenv()

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "sra")


def _pinecone_index():
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    return pc.Index(PINECONE_INDEX_NAME)


def classify(chunks: List[dict]) -> tuple[list[dict], list[dict]]:
    """Return (positives, negatives). Adds has_material_compatibility to every
    chunk dict in-place so the caller can rewrite the jsonl file."""
    pos: List[dict] = []
    neg: List[dict] = []
    for c in chunks:
        flag = has_material_compatibility(
            c.get("text", ""),
            c.get("source_category", ""),
        )
        c["has_material_compatibility"] = flag
        (pos if flag else neg).append(c)
    return pos, neg


def rewrite_chunks_jsonl(chunks: List[dict]) -> None:
    """Overwrite chunks.jsonl, preserving original order."""
    with CHUNKS_PATH.open("w", encoding="utf-8") as f:
        for c in chunks:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")


def update_pinecone(positives: List[dict]) -> int:
    """Set metadata.has_material_compatibility=True on each positive chunk
    in Pinecone. Uses index.update() per chunk — 100-500 calls is tractable
    for a one-time backfill."""
    index = _pinecone_index()
    updated = 0
    for c in tqdm(positives, desc="pinecone.update"):
        try:
            index.update(id=c["chunk_id"], set_metadata={"has_material_compatibility": True})
            updated += 1
        except Exception as exc:
            print(f"  ! {c['chunk_id']}: {type(exc).__name__}: {exc}", file=sys.stderr)
    return updated


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--confirm", action="store_true",
                    help="Apply changes to Pinecone and rewrite chunks.jsonl. Without this, dry-run.")
    ap.add_argument("--sample", type=int, default=5,
                    help="Print N positive chunks (and N negatives that contain material-ish words) for spot-check.")
    args = ap.parse_args(argv)

    if not CHUNKS_PATH.exists():
        print(f"chunks.jsonl not found at {CHUNKS_PATH}", file=sys.stderr)
        return 2

    all_chunks: List[dict] = []
    with CHUNKS_PATH.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                all_chunks.append(json.loads(line))
    print(f"Loaded {len(all_chunks)} chunks from {CHUNKS_PATH}")

    positives, negatives = classify(all_chunks)
    print(f"Classified positives: {len(positives)}  negatives: {len(negatives)}")

    # Surface a small sample so the user can eyeball precision.
    if args.sample:
        print(f"\n=== First {args.sample} positives ===")
        for c in positives[:args.sample]:
            snippet = c["text"].replace("\n", " ")[:180]
            print(f"  [{c['chunk_id']}] {c.get('file_path','')}  p{c.get('page_or_slide','')}")
            print(f"    > {snippet}")

    # By source_category breakdown — sanity check that positives aren't landing
    # entirely in one category (they shouldn't).
    by_cat: dict = {}
    for c in positives:
        cat = c.get("source_category", "unknown")
        by_cat[cat] = by_cat.get(cat, 0) + 1
    print("\n=== Positives by source_category ===")
    for cat, n in sorted(by_cat.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4d}  {cat}")

    if not args.confirm:
        print("\nDry-run only. Re-run with --confirm to update Pinecone + rewrite chunks.jsonl.")
        return 0

    print("\nUpdating Pinecone metadata …")
    n = update_pinecone(positives)
    print(f"Updated {n}/{len(positives)} chunks.")
    # Pinecone updates are eventually consistent; a short pause avoids a stale
    # read if the next step hits the index.
    time.sleep(1)

    print("Rewriting chunks.jsonl …")
    rewrite_chunks_jsonl(all_chunks)
    print(f"Wrote {len(all_chunks)} chunks to {CHUNKS_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
