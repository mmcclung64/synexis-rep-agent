"""Targeted re-extraction for Synexis Application Guide 20250501.pdf.

CODE_BRIEFING Retrieval-Tuning item: page 13's maintenance-interval table was
flattened by the default pdfplumber `extract_text()` path into a token stream
with no row/column structure ("6 6 24 or 60 Retail Stores 6 6 6 24 or 60 Spa ...").
This script re-extracts pages 12–15 with table-aware parsing, builds one chunk
per row with column headers prepended, and replaces the corresponding chunks in
Pinecone.

Overrides the standing 'broken-chunk remediation is programmatic' memory per
the Current Batch instruction (implement in order). A general extract-time
table parser remains a future task.

Usage:
    python3 -m pipeline.refine_app_guide --dry-run       # print what would change
    python3 -m pipeline.refine_app_guide --confirm       # delete + upsert
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from pipeline.extract import _resolve_source, EXTRACT_DIR
from pipeline.chunk import _count_tokens, EFFICACY_RE  # reuse token counter + efficacy detector


load_dotenv()

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "sra")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
VOYAGE_MODEL = os.getenv("VOYAGE_EMBEDDING_MODEL", "voyage-3")

TARGET_RELATIVE = "Manuals and Guides/Synexis Application Guide 20250501.pdf"
TARGET_PAGES = [12, 13, 14, 15]  # page 13 is the known failure; adjacent pages checked per briefing
SOURCE_CATEGORY = "Manuals and Guides"
INTAKE_MODE = "baseline"


def _doc_id(relative_path: str) -> str:
    return hashlib.sha1(relative_path.encode("utf-8")).hexdigest()[:16]


def _row_to_chunk_text(headers: List[str], row: List[str]) -> Optional[str]:
    """Render a table row as 'Row-label — col1: val1; col2: val2; ...' for retrieval.

    The first non-empty cell is treated as the row label; subsequent cells are
    paired with their header. Empty cells are skipped.
    """
    cells = [(h or "").strip() for h in row]
    if not any(cells):
        return None
    # Find the first non-empty cell as row label
    label_idx = next((i for i, c in enumerate(cells) if c), None)
    if label_idx is None:
        return None
    label = cells[label_idx]
    parts = []
    for i, val in enumerate(cells):
        if i == label_idx or not val:
            continue
        hdr = (headers[i] if i < len(headers) else f"Col{i+1}").strip() or f"Col{i+1}"
        parts.append(f"{hdr}: {val}")
    if not parts:
        return None
    return f"{label} — " + "; ".join(parts)


def _header_summary(headers: List[str]) -> str:
    return ", ".join(h.strip() for h in headers if h and h.strip())


def extract_tables_per_page(abs_path: Path) -> dict:
    """Return {page_number: [ {headers:[str], rows:[[str]]}, ... ]} for TARGET_PAGES."""
    import pdfplumber

    out: dict = {}
    with pdfplumber.open(str(abs_path)) as pdf:
        for page_no in TARGET_PAGES:
            if page_no < 1 or page_no > len(pdf.pages):
                continue
            page = pdf.pages[page_no - 1]
            tables_raw = page.extract_tables() or []
            if not tables_raw:
                continue
            page_tables = []
            for t in tables_raw:
                if not t or len(t) < 2:
                    continue
                headers = [(c or "").strip() for c in t[0]]
                rows = [[(c or "").strip() for c in r] for r in t[1:] if any(r)]
                if not rows:
                    continue
                page_tables.append({"headers": headers, "rows": rows})
            if page_tables:
                out[page_no] = page_tables
    return out


def build_chunks(tables_by_page: dict, doc_id: str, relative_path: str) -> List[dict]:
    """Produce one chunk per table row (plus an orientation chunk per table)."""
    chunks: List[dict] = []
    chunk_idx = 0
    for page_no, page_tables in tables_by_page.items():
        for t_idx, t in enumerate(page_tables):
            headers = t["headers"]
            rows = t["rows"]
            hdr_summary = _header_summary(headers)

            # Orientation chunk so queries like "what maintenance intervals does
            # the application guide document" retrieve the table at all.
            orient_text = (
                f"Synexis Application Guide — Maintenance intervals table "
                f"(page {page_no}). Columns: {hdr_summary}. "
                f"Row labels: {', '.join(r[0] for r in rows if r and r[0])[:500]}."
            )
            chunks.append(_make_chunk(
                chunk_idx, orient_text, doc_id, relative_path, page_no, extractor="pdfplumber-table",
            ))
            chunk_idx += 1

            for row in rows:
                text = _row_to_chunk_text(headers, row)
                if not text:
                    continue
                chunks.append(_make_chunk(
                    chunk_idx, text, doc_id, relative_path, page_no, extractor="pdfplumber-table",
                ))
                chunk_idx += 1
    return chunks


def _make_chunk(idx: int, text: str, doc_id: str, relative_path: str, page_or_slide: int,
                extractor: str) -> dict:
    chunk_id = f"{doc_id}_t{idx:04d}"  # 't' prefix distinguishes table-derived chunks
    return {
        "chunk_id": chunk_id,
        "doc_id": doc_id,
        "chunk_index": idx,
        "text": text,
        "token_count": _count_tokens(text),
        "file_path": relative_path,
        "source": relative_path,
        "source_category": SOURCE_CATEGORY,
        "intake_mode": INTAKE_MODE,
        "page_or_slide": page_or_slide,
        "has_efficacy_claim": bool(EFFICACY_RE.search(text)),
        "extension": "pdf",
        "extractor_used": extractor,
    }


def pinecone_index():
    from pinecone import Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    return pc.Index(PINECONE_INDEX_NAME)


def delete_existing_broken_chunks(index, relative_path: str, pages: List[int]) -> None:
    """Delete current chunks for this file on the target pages.

    We fetch-and-delete by source filter; page_or_slide stored in metadata
    may be a float or int depending on extraction, so match either.
    """
    # Pinecone $in supports list of values; send both int and float variants.
    page_values: list = []
    for p in pages:
        page_values.append(int(p))
        page_values.append(float(p))
    filter_expr = {
        "source": {"$eq": relative_path},
        "page_or_slide": {"$in": page_values},
    }
    index.delete(filter=filter_expr)


def embed_and_upsert(index, chunks: List[dict]) -> int:
    import voyageai
    client = voyageai.Client(api_key=VOYAGE_API_KEY)

    texts = [c["text"] for c in chunks]
    # Single batch — pages 12-15 will produce at most a few dozen chunks.
    result = client.embed(texts, model=VOYAGE_MODEL, input_type="document")
    embeddings = result.embeddings

    vectors = []
    for c, emb in zip(chunks, embeddings):
        md = {
            "source": c["source"],
            "file_path": c["file_path"],
            "doc_id": c["doc_id"],
            "chunk_index": c["chunk_index"],
            "source_category": c["source_category"],
            "intake_mode": c["intake_mode"],
            "page_or_slide": c["page_or_slide"],
            "has_efficacy_claim": c["has_efficacy_claim"],
            "extension": c["extension"],
            "extractor_used": c["extractor_used"],
            "token_count": c["token_count"],
            "text": c["text"],
        }
        vectors.append({"id": c["chunk_id"], "values": emb, "metadata": md})

    # Upsert in one shot — small batch.
    index.upsert(vectors=vectors)
    return len(vectors)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--confirm", action="store_true",
                    help="Delete broken chunks and upsert new ones. Without this flag, dry-run only.")
    args = ap.parse_args(argv)

    abs_path = _resolve_source(TARGET_RELATIVE)
    if abs_path is None:
        print(f"Could not locate source file: {TARGET_RELATIVE}", file=sys.stderr)
        return 2
    print(f"Source: {abs_path}")

    tables_by_page = extract_tables_per_page(abs_path)
    if not tables_by_page:
        print("No tables detected on pages", TARGET_PAGES)
        return 1

    for page_no, tables in tables_by_page.items():
        print(f"\n=== Page {page_no}: {len(tables)} table(s) ===")
        for i, t in enumerate(tables):
            print(f"  Table {i+1}: headers = {t['headers']}")
            print(f"           rows    = {len(t['rows'])}")

    doc_id = _doc_id(TARGET_RELATIVE)
    chunks = build_chunks(tables_by_page, doc_id, TARGET_RELATIVE)
    print(f"\nBuilt {len(chunks)} chunk(s). First 3:")
    for c in chunks[:3]:
        print(f"  [{c['chunk_id']}] p{c['page_or_slide']}  {c['text'][:200]}")
    print(f"  … and {max(0, len(chunks) - 3)} more")

    if not args.confirm:
        print("\nDry-run only. Re-run with --confirm to delete broken chunks and upsert.")
        return 0

    index = pinecone_index()
    print(f"\nDeleting existing chunks for {TARGET_RELATIVE} on pages {TARGET_PAGES} ...")
    delete_existing_broken_chunks(index, TARGET_RELATIVE, TARGET_PAGES)
    # Pinecone deletes are eventually consistent; give it a moment.
    time.sleep(2)
    print("Embedding + upserting new table-derived chunks ...")
    n = embed_and_upsert(index, chunks)
    print(f"Upserted {n} chunk(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
