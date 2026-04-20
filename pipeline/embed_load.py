"""Embed chunks with Voyage voyage-3 and upsert to Pinecone with full metadata.

Inputs:  work/chunks.jsonl, work/skipped_docs.json
Outputs:
  work/upsert_log.jsonl         # one line per successful upsert batch
  work/spot_check_report.md      # 10-chunk spot-check + OCR worklist + counts
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
WORK_DIR = REPO_ROOT / "work"
CHUNKS_PATH = WORK_DIR / "chunks.jsonl"
SKIPPED_LOG = WORK_DIR / "skipped_docs.json"
UPSERT_LOG = WORK_DIR / "upsert_log.jsonl"
SPOTCHECK_PATH = WORK_DIR / "spot_check_report.md"

PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "synexis-rep-agent")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
VOYAGE_MODEL = os.getenv("VOYAGE_EMBEDDING_MODEL", "voyage-3")

EMBED_BATCH_MAX_CHUNKS = 128        # voyage per-request chunk limit
EMBED_BATCH_MAX_TOKENS = 9_000      # stay under Voyage free-tier 10K TPM per call
UPSERT_BATCH = 100                   # pinecone recommended batch size

# Pinecone per-vector metadata size limit is 40KB.
# Keep a generous margin for text + structured fields.
MAX_METADATA_TEXT_CHARS = 20_000


def _iter_chunks(chunks_path: Path = CHUNKS_PATH) -> Iterable[dict]:
    with chunks_path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def _voyage_client():
    import voyageai
    return voyageai.Client(api_key=VOYAGE_API_KEY)


def _pinecone_index():
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    return pc.Index(PINECONE_INDEX_NAME)


# Voyage free tier is ~3 RPM on voyage-3 — backoff must exceed 20s between retries.
@retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=2, min=20, max=90))
def _embed_batch(client, texts: List[str]) -> List[List[float]]:
    result = client.embed(texts, model=VOYAGE_MODEL, input_type="document")
    return result.embeddings


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def _upsert_batch(index, vectors: List[dict]) -> None:
    index.upsert(vectors=vectors)


# Minimum wall-clock gap between Voyage embed calls. Standard paid tier gives
# ~2000 RPM / 3M TPM, so a small gap is plenty; bump back up only if you're
# running on the free tier (which needs ≥60s).
EMBED_INTER_CALL_SECONDS = 0.5


def _build_token_batches(chunks: List[dict]) -> List[List[dict]]:
    """Group chunks so each batch is ≤ EMBED_BATCH_MAX_TOKENS and ≤ EMBED_BATCH_MAX_CHUNKS."""
    batches: List[List[dict]] = []
    cur: List[dict] = []
    cur_tokens = 0
    for c in chunks:
        tok = int(c.get("token_count") or 0)
        if cur and (cur_tokens + tok > EMBED_BATCH_MAX_TOKENS or len(cur) >= EMBED_BATCH_MAX_CHUNKS):
            batches.append(cur)
            cur, cur_tokens = [], 0
        cur.append(c)
        cur_tokens += tok
    if cur:
        batches.append(cur)
    return batches


def _build_vector(chunk: dict, embedding: List[float]) -> dict:
    text = chunk["text"]
    if len(text) > MAX_METADATA_TEXT_CHARS:
        text = text[:MAX_METADATA_TEXT_CHARS]
    metadata = {
        "source": chunk["source"],                   # top-level for filtered deletes
        "file_path": chunk["file_path"],
        "doc_id": chunk["doc_id"],
        "chunk_index": chunk["chunk_index"],
        "source_category": chunk["source_category"],
        "intake_mode": chunk["intake_mode"],
        "page_or_slide": chunk["page_or_slide"],
        "has_efficacy_claim": chunk["has_efficacy_claim"],
        "extension": chunk["extension"],
        "extractor_used": chunk["extractor_used"],
        "token_count": chunk["token_count"],
        "text": text,
    }
    return {"id": chunk["chunk_id"], "values": embedding, "metadata": metadata}


def run(limit: Optional[int] = None, dry_embed: bool = False) -> dict:
    if not CHUNKS_PATH.exists():
        raise SystemExit(f"No chunks file at {CHUNKS_PATH}. Run pipeline.chunk first.")
    if not VOYAGE_API_KEY:
        raise SystemExit("VOYAGE_API_KEY is missing from environment.")
    if not PINECONE_API_KEY and not dry_embed:
        raise SystemExit("PINECONE_API_KEY is missing from environment.")

    chunks = list(_iter_chunks())
    if limit is not None:
        chunks = chunks[:limit]

    if not chunks:
        raise SystemExit("chunks.jsonl is empty.")

    voyage = _voyage_client()
    index = None if dry_embed else _pinecone_index()

    UPSERT_LOG.parent.mkdir(parents=True, exist_ok=True)
    log_f = UPSERT_LOG.open("w", encoding="utf-8")

    total_embedded = 0
    total_upserted = 0
    pending: List[tuple[dict, List[float]]] = []
    last_embed_at = 0.0

    batches = _build_token_batches(chunks)
    try:
        for batch in tqdm(batches, desc="embed"):
            texts = [c["text"] for c in batch]
            elapsed = time.time() - last_embed_at
            if last_embed_at and elapsed < EMBED_INTER_CALL_SECONDS:
                time.sleep(EMBED_INTER_CALL_SECONDS - elapsed)
            embeddings = _embed_batch(voyage, texts)
            last_embed_at = time.time()
            total_embedded += len(embeddings)
            for c, emb in zip(batch, embeddings):
                pending.append((c, emb))

            # drain pending in UPSERT_BATCH-sized slices
            while len(pending) >= UPSERT_BATCH:
                take = pending[:UPSERT_BATCH]
                pending = pending[UPSERT_BATCH:]
                vectors = [_build_vector(c, e) for c, e in take]
                if not dry_embed:
                    _upsert_batch(index, vectors)
                total_upserted += len(vectors)
                log_f.write(json.dumps({
                    "ts": time.time(),
                    "batch_size": len(vectors),
                    "ids": [v["id"] for v in vectors],
                }) + "\n")

        if pending:
            vectors = [_build_vector(c, e) for c, e in pending]
            if not dry_embed:
                _upsert_batch(index, vectors)
            total_upserted += len(vectors)
            log_f.write(json.dumps({
                "ts": time.time(),
                "batch_size": len(vectors),
                "ids": [v["id"] for v in vectors],
            }) + "\n")
    finally:
        log_f.close()

    report_path = _write_spot_check_report(chunks, dry_embed=dry_embed, total_upserted=total_upserted)
    summary = {
        "chunks_embedded": total_embedded,
        "chunks_upserted": total_upserted,
        "dry_embed": dry_embed,
        "index": PINECONE_INDEX_NAME,
        "upsert_log": str(UPSERT_LOG),
        "spot_check_report": str(report_path),
    }
    print(json.dumps(summary, indent=2))
    return summary


def _load_skipped() -> List[dict]:
    if not SKIPPED_LOG.exists():
        return []
    try:
        return json.loads(SKIPPED_LOG.read_text(encoding="utf-8"))
    except Exception:
        return []


def _stratified_sample(chunks: List[dict], n: int = 10) -> List[dict]:
    """Pick `n` chunks that cover every distinct extractor_used in the corpus.

    Guarantees at least one chunk per extractor (when present), then fills the
    remainder randomly. Deterministic via seed for reproducible reports.
    """
    rng = random.Random(42)
    by_extractor: dict[str, List[dict]] = {}
    for c in chunks:
        by_extractor.setdefault(c.get("extractor_used", "unknown"), []).append(c)

    picked: List[dict] = []
    picked_ids: set[str] = set()
    for extractor, bucket in by_extractor.items():
        c = rng.choice(bucket)
        picked.append(c)
        picked_ids.add(c["chunk_id"])
        if len(picked) >= n:
            break

    remaining = [c for c in chunks if c["chunk_id"] not in picked_ids]
    rng.shuffle(remaining)
    while len(picked) < n and remaining:
        picked.append(remaining.pop())

    # Preserve chunk_id ordering for readability
    picked.sort(key=lambda c: c["chunk_id"])
    return picked


def _write_spot_check_report(chunks: List[dict], dry_embed: bool, total_upserted: int) -> Path:
    sample = _stratified_sample(chunks, n=10)
    skipped = _load_skipped()

    efficacy_total = sum(1 for c in chunks if c["has_efficacy_claim"])
    docs = {c["doc_id"] for c in chunks}
    categories: dict[str, int] = {}
    for c in chunks:
        categories[c["source_category"]] = categories.get(c["source_category"], 0) + 1

    lines: List[str] = []
    lines.append("# Spot-check Report\n")
    lines.append(f"- Chunks in sample file: **{len(chunks)}**")
    lines.append(f"- Unique docs: **{len(docs)}**")
    lines.append(f"- Chunks flagged `has_efficacy_claim`: **{efficacy_total}**")
    lines.append(f"- Chunks upserted to Pinecone: **{total_upserted}** (dry_embed={dry_embed})")
    lines.append("")
    lines.append("## Chunks by source_category")
    for cat, n in sorted(categories.items(), key=lambda kv: -kv[1]):
        lines.append(f"- {cat}: {n}")
    lines.append("")

    lines.append("## 10 Random Chunks")
    lines.append("")
    for i, c in enumerate(sample, start=1):
        lines.append(f"### Sample {i} — `{c['chunk_id']}`")
        lines.append("")
        lines.append(f"- **source**: `{c['source']}`")
        lines.append(f"- **file_path**: `{c['file_path']}`")
        lines.append(f"- **source_category**: {c['source_category']}")
        lines.append(f"- **intake_mode**: {c['intake_mode']}")
        lines.append(f"- **page_or_slide**: {c['page_or_slide']}")
        lines.append(f"- **extractor_used**: {c['extractor_used']}")
        lines.append(f"- **token_count**: {c['token_count']}")
        lines.append(f"- **has_efficacy_claim**: {c['has_efficacy_claim']}")
        lines.append("")
        lines.append("```text")
        lines.append(c["text"])
        lines.append("```")
        lines.append("")

    lines.append("## OCR / Remediation Worklist — Skipped Docs")
    lines.append("")
    if not skipped:
        lines.append("_No skipped docs recorded._")
    else:
        lines.append("| File | Reason | Extractor | Chars |")
        lines.append("|---|---|---|---|")
        for s in skipped:
            lines.append(
                f"| {s.get('file_path','')} | {s.get('reason','')} "
                f"| {s.get('extractor_used','—')} | {s.get('total_chars','—')} |"
            )
    lines.append("")

    SPOTCHECK_PATH.write_text("\n".join(lines), encoding="utf-8")
    return SPOTCHECK_PATH


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Embed chunks with Voyage and upsert to Pinecone.")
    ap.add_argument("--limit", type=int, default=None, help="Embed/upsert only the first N chunks.")
    ap.add_argument(
        "--dry-embed",
        action="store_true",
        help="Embed with Voyage but skip the Pinecone upsert. Still writes the spot-check report.",
    )
    ap.add_argument(
        "--startup-wait",
        type=int,
        default=0,
        help="Seconds to wait before the first Voyage call (useful to clear a prior TPM-window failure).",
    )
    ap.add_argument(
        "--report-only",
        action="store_true",
        help="Skip embedding/upsert; re-generate the spot-check report from existing chunks.jsonl.",
    )
    args = ap.parse_args(argv)
    if args.report_only:
        chunks = list(_iter_chunks())
        if args.limit is not None:
            chunks = chunks[: args.limit]
        path = _write_spot_check_report(chunks, dry_embed=True, total_upserted=0)
        print(f"Report-only: wrote {path}")
        return 0
    if args.startup_wait:
        print(f"Sleeping {args.startup_wait}s to clear any prior rate-limit window...")
        time.sleep(args.startup_wait)
    run(limit=args.limit, dry_embed=args.dry_embed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
