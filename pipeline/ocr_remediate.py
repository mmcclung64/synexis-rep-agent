"""OCR remediation for 5 scanned PDFs that returned 0 text chars during ingest.

CODE_BRIEFING — Retrieval Tuning → OCR Remediation. Four patents + one USDA
doc (143 pages total) are image-only and need tesseract OCR before they can
be chunked and embedded.

Usage:
    python3 -m pipeline.ocr_remediate           # dry run: OCR first page per file, print sample
    python3 -m pipeline.ocr_remediate --confirm  # full OCR, delete old chunks, embed+upsert

System requirements (install once):
    brew install tesseract poppler

Python requirements (in requirements.txt):
    pdf2image, pytesseract

Output:
    logs/ocr_remediation.jsonl  — one JSON line per file processed (page count,
                                   chunk count, duration, OCR sample, outcome)
    work/ocr_extracted/         — per-file OCR text caches so re-runs skip the
                                   expensive OCR step if the PDF is unchanged
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from tqdm import tqdm

from pipeline.chunk import Chunk, chunk_doc
from pipeline.extract import _resolve_source


load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
WORK_DIR = REPO_ROOT / "work"
OCR_CACHE_DIR = WORK_DIR / "ocr_extracted"
CHUNKS_PATH = WORK_DIR / "chunks.jsonl"
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
OCR_LOG_PATH = LOG_DIR / "ocr_remediation.jsonl"

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "sra")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
VOYAGE_MODEL = os.getenv("VOYAGE_EMBEDDING_MODEL", "voyage-3")

# Pinecone per-vector metadata cap is 40KB; leave margin for the other fields.
MAX_METADATA_TEXT_CHARS = 20_000
EMBED_BATCH_MAX_CHUNKS = 128
EMBED_BATCH_MAX_TOKENS = 9_000          # respect Voyage free-tier window if applicable
UPSERT_BATCH = 100
OCR_DPI = 300                            # patents scan well at 300 DPI; higher only if quality poor


# Five files from CODE_BRIEFING's remediation list. source/file_path match the
# manifest convention (no "Customer Facing Training Documents/" prefix); the
# extract module's path resolver finds them on disk either way.
TARGETS = [
    {
        "file_path": "Patents/US11751569.pdf",
        "source_category": "Patents",
        "intake_mode": "baseline",
        "psm": 3,   # Fully automatic page segmentation, per briefing
    },
    {
        "file_path": "Patents/US11980639.pdf",
        "source_category": "Patents",
        "intake_mode": "baseline",
        "psm": 3,
    },
    {
        "file_path": "Patents/US12102063.pdf",
        "source_category": "Patents",
        "intake_mode": "baseline",
        "psm": 3,
    },
    {
        "file_path": "Patents/US20230203676A1.pdf",
        "source_category": "Patents",
        "intake_mode": "baseline",
        "psm": 3,
    },
    {
        "file_path": "Public Domain Materials ex OSHA EPA etc/USDA Allowed Sanitizers for Food Contact Surfaces in Organic Spaces.pdf",
        "source_category": "Public Domain Materials ex OSHA EPA etc",
        "intake_mode": "baseline",
        "psm": 6,   # Uniform block of text, per briefing
    },
]


def _doc_id(relative_path: str) -> str:
    return hashlib.sha1(relative_path.encode("utf-8")).hexdigest()[:16]


def _ocr_cache_path(relative_path: str) -> Path:
    OCR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return OCR_CACHE_DIR / f"{_doc_id(relative_path)}.json"


def ocr_pdf(abs_path: Path, psm: int, *, first_page_only: bool = False) -> List[dict]:
    """Return [{number:int, text:str}] for every page, ordered."""
    import pdf2image
    import pytesseract

    kwargs = {"dpi": OCR_DPI}
    if first_page_only:
        kwargs["first_page"] = 1
        kwargs["last_page"] = 1
    images = pdf2image.convert_from_path(str(abs_path), **kwargs)
    config = f"--psm {psm}"
    pages: List[dict] = []
    for i, img in enumerate(images, start=1):
        try:
            text = pytesseract.image_to_string(img, config=config)
        except Exception as exc:
            text = ""
            print(f"  ! page {i} OCR failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        pages.append({"number": i, "text": text or ""})
    return pages


def _load_or_ocr(abs_path: Path, relative_path: str, psm: int) -> List[dict]:
    """OCR the file or return cached pages from a prior run."""
    cache = _ocr_cache_path(relative_path)
    if cache.exists():
        try:
            data = json.loads(cache.read_text(encoding="utf-8"))
            if data.get("source_bytes") == abs_path.stat().st_size:
                return data["pages"]
        except Exception:
            pass
    pages = ocr_pdf(abs_path, psm=psm)
    cache.write_text(
        json.dumps({
            "file_path": relative_path,
            "source_bytes": abs_path.stat().st_size,
            "pages": pages,
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    return pages


def build_chunks_for_file(relative_path: str, pages: List[dict], source_category: str,
                         intake_mode: str) -> List[Chunk]:
    """Feed the OCR'd pages through the standard chunker with OCR provenance."""
    doc_json = {
        "file_path": relative_path,
        "doc_id": _doc_id(relative_path),
        "source_category": source_category,
        "intake_mode": intake_mode,
        "description": "OCR-remediated via pytesseract",
        "extension": "pdf",
        "extractor_used": "pytesseract-ocr",
        "total_chars": sum(len(p["text"]) for p in pages),
        "pages": pages,
    }
    return chunk_doc(doc_json)


def _pinecone_index():
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    return pc.Index(PINECONE_INDEX_NAME)


def _build_vector(chunk: Chunk, embedding: List[float]) -> dict:
    text = chunk.text
    if len(text) > MAX_METADATA_TEXT_CHARS:
        text = text[:MAX_METADATA_TEXT_CHARS]
    md = {
        "source": chunk.source,
        "file_path": chunk.file_path,
        "doc_id": chunk.doc_id,
        "chunk_index": chunk.chunk_index,
        "source_category": chunk.source_category,
        "intake_mode": chunk.intake_mode,
        "page_or_slide": chunk.page_or_slide,
        "has_efficacy_claim": chunk.has_efficacy_claim,
        "has_material_compatibility": chunk.has_material_compatibility,
        "extension": chunk.extension,
        "extractor_used": chunk.extractor_used,
        "token_count": chunk.token_count,
        "text": text,
    }
    return {"id": chunk.chunk_id, "values": embedding, "metadata": md}


def _batch_by_tokens(chunks: List[Chunk]) -> List[List[Chunk]]:
    batches: List[List[Chunk]] = []
    cur: List[Chunk] = []
    cur_tokens = 0
    for c in chunks:
        tok = c.token_count or 0
        if cur and (cur_tokens + tok > EMBED_BATCH_MAX_TOKENS or len(cur) >= EMBED_BATCH_MAX_CHUNKS):
            batches.append(cur)
            cur, cur_tokens = [], 0
        cur.append(c)
        cur_tokens += tok
    if cur:
        batches.append(cur)
    return batches


def update_chunks_jsonl(new_chunks: List[Chunk], source_paths: List[str]) -> int:
    """Append the OCR'd chunks to work/chunks.jsonl so BM25 sees them too.

    Removes any stale lines for the same source paths first (shouldn't be any
    — the original ingest skipped these files — but this keeps re-runs clean).
    Returns the number of new lines written.
    """
    stale_sources = set(source_paths)
    existing: List[dict] = []
    if CHUNKS_PATH.exists():
        with CHUNKS_PATH.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                if obj.get("source") in stale_sources:
                    continue
                existing.append(obj)

    with CHUNKS_PATH.open("w", encoding="utf-8") as f:
        for obj in existing:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        for c in new_chunks:
            f.write(json.dumps(asdict(c), ensure_ascii=False) + "\n")
    return len(new_chunks)


def embed_and_upsert(index, chunks: List[Chunk]) -> int:
    import voyageai

    client = voyageai.Client(api_key=VOYAGE_API_KEY)
    total = 0
    for batch in tqdm(_batch_by_tokens(chunks), desc="embed+upsert"):
        texts = [c.text for c in batch]
        result = client.embed(texts, model=VOYAGE_MODEL, input_type="document")
        embeddings = result.embeddings
        vectors = [_build_vector(c, e) for c, e in zip(batch, embeddings)]
        # Send in UPSERT_BATCH slices so we don't oversize a single upsert.
        for i in range(0, len(vectors), UPSERT_BATCH):
            index.upsert(vectors=vectors[i : i + UPSERT_BATCH])
        total += len(vectors)
    return total


def delete_existing(index, relative_path: str) -> None:
    """Remove any existing chunks tied to this source file (no-op if none)."""
    try:
        index.delete(filter={"source": {"$eq": relative_path}})
    except Exception as exc:
        print(f"  ! delete filter failed for {relative_path}: {exc}", file=sys.stderr)


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds")


def _log(entry: dict) -> None:
    # Strip internal fields (leading underscore) — they carry non-serializable
    # objects like the live Chunk list we hand back to the caller.
    cleaned = {k: v for k, v in entry.items() if not k.startswith("_")}
    with OCR_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(cleaned, ensure_ascii=False) + "\n")


def _dry_run_one(target: dict) -> dict:
    """OCR just the first page of a target file to sanity-check quality."""
    rel = target["file_path"]
    abs_path = _resolve_source(rel)
    if abs_path is None:
        return {"file_path": rel, "status": "not_found"}
    t0 = time.time()
    pages = ocr_pdf(abs_path, psm=target["psm"], first_page_only=True)
    elapsed = int((time.time() - t0) * 1000)
    first_text = pages[0]["text"] if pages else ""
    preview = " ".join(first_text.split())[:500]
    return {
        "file_path": rel,
        "absolute_path": str(abs_path),
        "first_page_chars": len(first_text),
        "first_page_preview": preview,
        "psm": target["psm"],
        "ocr_ms_first_page": elapsed,
        "status": "dry_ok" if first_text.strip() else "empty_ocr",
    }


def _run_one(target: dict, index) -> dict:
    rel = target["file_path"]
    abs_path = _resolve_source(rel)
    if abs_path is None:
        return {"file_path": rel, "status": "not_found"}

    t_start = time.time()
    pages = _load_or_ocr(abs_path, rel, target["psm"])
    ocr_ms = int((time.time() - t_start) * 1000)

    total_chars = sum(len(p["text"]) for p in pages)
    if total_chars < 200:
        return {
            "file_path": rel,
            "status": "ocr_too_small",
            "pages": len(pages),
            "total_chars": total_chars,
            "ocr_ms": ocr_ms,
        }

    chunks = build_chunks_for_file(
        rel, pages,
        source_category=target["source_category"],
        intake_mode=target["intake_mode"],
    )

    delete_existing(index, rel)
    time.sleep(1)  # pinecone delete is eventually consistent
    upserted = embed_and_upsert(index, chunks)

    return {
        "file_path": rel,
        "status": "upserted",
        "pages": len(pages),
        "total_chars": total_chars,
        "chunks": len(chunks),
        "upserted": upserted,
        "ocr_ms": ocr_ms,
        "total_ms": int((time.time() - t_start) * 1000),
        "extractor_used": "pytesseract-ocr",
        "psm": target["psm"],
        # Internal — not serialized to the log; caller consumes to append to chunks.jsonl
        "_chunks": chunks,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--confirm", action="store_true",
                    help="Run full OCR, delete existing chunks, embed+upsert. Without this, dry-run samples first page only.")
    ap.add_argument("--file", action="append", default=None,
                    help="Process only this relative file path (repeatable). Default: all 5 targets.")
    args = ap.parse_args(argv)

    targets = TARGETS
    if args.file:
        wanted = set(args.file)
        targets = [t for t in targets if t["file_path"] in wanted]
        if not targets:
            print(f"No targets match --file {args.file}", file=sys.stderr)
            return 2

    if not args.confirm:
        print("Dry run — OCR-ing the first page of each target. Re-run with --confirm for full remediation.")
        for t in targets:
            print(f"\n=== {t['file_path']} (psm {t['psm']}) ===")
            result = _dry_run_one(t)
            _log({"timestamp": _now_iso(), "event": "ocr.dry_run", **result})
            print(f"  status:           {result.get('status')}")
            print(f"  first_page_chars: {result.get('first_page_chars')}")
            print(f"  ocr_ms (page 1):  {result.get('ocr_ms_first_page')}")
            preview = result.get("first_page_preview", "")
            if preview:
                print(f"  preview: {preview[:300]}")
            else:
                print(f"  (no preview)")
        print("\nDry run complete.")
        return 0

    print(f"Full run — OCR + chunk + embed + upsert for {len(targets)} file(s).")
    index = _pinecone_index()
    overall_chunks = 0
    all_new_chunks: List[Chunk] = []
    all_sources: List[str] = []
    for t in targets:
        print(f"\n=== {t['file_path']} (psm {t['psm']}) ===")
        result = _run_one(t, index)
        result["timestamp"] = _now_iso()
        result["event"] = "ocr.full_run"
        _log(result)
        overall_chunks += result.get("upserted", 0)
        print(f"  status:   {result.get('status')}")
        print(f"  pages:    {result.get('pages')}")
        print(f"  chars:    {result.get('total_chars')}")
        print(f"  chunks:   {result.get('chunks')}")
        print(f"  upserted: {result.get('upserted')}")
        print(f"  ocr_ms:   {result.get('ocr_ms')}")
        if result.get("status") == "upserted" and result.get("_chunks"):
            all_new_chunks.extend(result["_chunks"])
            all_sources.append(t["file_path"])

    if all_new_chunks:
        n_written = update_chunks_jsonl(all_new_chunks, all_sources)
        print(f"\nAppended {n_written} chunks to work/chunks.jsonl for BM25.")

    print(f"\nTotal chunks upserted: {overall_chunks}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
