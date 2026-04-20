"""Chunk extracted docs to ~400 tokens with efficacy-claim atomicity and conditional overlap.

Inputs:  work/extracted/*.json
Output:  work/chunks.jsonl   (one chunk per line, with metadata)

Rules:
  - Target chunk size: 400 tokens (tiktoken cl100k_base).
  - Never split an efficacy-claim sentence across chunks.
  - Chunks don't cross page/slide boundaries.
  - Overlap between chunks is conditional: 0 tokens when either neighbor
    has_efficacy_claim, else 50 tokens carried from the prior chunk's tail.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Optional

import tiktoken
from tqdm import tqdm


REPO_ROOT = Path(__file__).resolve().parent.parent
WORK_DIR = REPO_ROOT / "work"
EXTRACT_DIR = WORK_DIR / "extracted"
CHUNKS_PATH = WORK_DIR / "chunks.jsonl"

TARGET_TOKENS = 400
OVERLAP_TOKENS = 50
TOKENIZER = tiktoken.get_encoding("cl100k_base")

# Efficacy-claim detector: log reductions, % reductions tied to action verbs, CFU counts.
EFFICACY_RE = re.compile(
    r"""
    (?:\b\d+(?:\.\d+)?\s*-?\s*log(?:10)?\b(?:\s*[-\s]?\s*reduction|\s+kill|\s+decrease)?)  # 3-log, 4.5 log10 reduction
    | (?:\b\d{1,3}(?:\.\d+)?\s*%[^.]{0,80}?(?:reduc|efficac|kill|inactivat|eliminat|disinfect|remov|decreas|lower))
    | (?:(?:reduc|efficac|kill|inactivat|eliminat|disinfect|remov|decreas|lower)\w*[^.]{0,80}?\b\d{1,3}(?:\.\d+)?\s*%)
    | (?:\b\d+(?:,\d{3})*\s*CFU\b)
    | (?:\blog(?:10)?\s+reduction\b)
    """,
    re.IGNORECASE | re.VERBOSE,
)

_SENT_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'\(])")


@dataclass
class Chunk:
    chunk_id: str
    doc_id: str
    chunk_index: int
    text: str
    token_count: int
    file_path: str
    source: str                  # MUST mirror file_path; top-level for Pinecone filtered deletes
    source_category: str
    intake_mode: str
    page_or_slide: int
    has_efficacy_claim: bool
    extension: str
    extractor_used: str


def _count_tokens(text: str) -> int:
    return len(TOKENIZER.encode(text))


def _tail_tokens(text: str, n: int) -> str:
    ids = TOKENIZER.encode(text)
    if len(ids) <= n:
        return text
    return TOKENIZER.decode(ids[-n:])


def _split_sentences(page_text: str) -> List[str]:
    # Normalize newlines inside paragraphs but preserve hard breaks between them
    text = re.sub(r"\s+\n\s+", "\n", page_text)
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    sentences: List[str] = []
    for p in paragraphs:
        parts = _SENT_SPLIT_RE.split(p)
        for s in parts:
            s = s.strip()
            if s:
                sentences.append(s)
    return sentences


def _has_efficacy(sentence: str) -> bool:
    return bool(EFFICACY_RE.search(sentence))


def _sentences_for_page(page_text: str) -> List[tuple[str, bool, int]]:
    """Return (sentence, has_efficacy, token_count) for each sentence on a page."""
    out: List[tuple[str, bool, int]] = []
    for s in _split_sentences(page_text):
        out.append((s, _has_efficacy(s), _count_tokens(s)))
    return out


def _chunk_page(
    sentences: List[tuple[str, bool, int]],
    target: int = TARGET_TOKENS,
) -> List[tuple[str, bool, int]]:
    """Group sentences into chunks. Returns list of (text, has_efficacy_claim, token_count).

    Never splits a sentence. If a single sentence exceeds `target`, it occupies
    its own chunk regardless of size.
    """
    chunks: List[tuple[str, bool, int]] = []
    cur: List[str] = []
    cur_tokens = 0
    cur_flag = False

    for sent, flag, tok in sentences:
        if cur and cur_tokens + tok > target:
            chunks.append(("\n".join(cur), cur_flag, cur_tokens))
            cur, cur_tokens, cur_flag = [], 0, False
        cur.append(sent)
        cur_tokens += tok
        cur_flag = cur_flag or flag

    if cur:
        chunks.append(("\n".join(cur), cur_flag, cur_tokens))
    return chunks


def _apply_conditional_overlap(
    chunks: List[tuple[str, bool, int]],
) -> List[tuple[str, bool, int]]:
    """Prepend OVERLAP_TOKENS of prior-chunk tail when neither side has an efficacy claim."""
    if not chunks:
        return chunks
    out: List[tuple[str, bool, int]] = [chunks[0]]
    for i in range(1, len(chunks)):
        prev_text, prev_flag, _ = chunks[i - 1]
        cur_text, cur_flag, _ = chunks[i]
        if prev_flag or cur_flag:
            out.append((cur_text, cur_flag, _count_tokens(cur_text)))
            continue
        tail = _tail_tokens(prev_text, OVERLAP_TOKENS)
        merged = tail + "\n" + cur_text if tail else cur_text
        out.append((merged, cur_flag, _count_tokens(merged)))
    return out


def chunk_doc(doc_json: dict) -> List[Chunk]:
    chunks: List[Chunk] = []
    chunk_idx = 0

    for page in doc_json["pages"]:
        sents = _sentences_for_page(page["text"])
        if not sents:
            continue
        raw_chunks = _chunk_page(sents)
        raw_chunks = _apply_conditional_overlap(raw_chunks)

        for text, flag, tok in raw_chunks:
            chunk_id = f"{doc_json['doc_id']}_{chunk_idx:04d}"
            chunks.append(
                Chunk(
                    chunk_id=chunk_id,
                    doc_id=doc_json["doc_id"],
                    chunk_index=chunk_idx,
                    text=text,
                    token_count=tok,
                    file_path=doc_json["file_path"],
                    source=doc_json["file_path"],  # top-level mirror for Pinecone filtered deletes
                    source_category=doc_json["source_category"],
                    intake_mode=doc_json["intake_mode"],
                    page_or_slide=page["number"],
                    has_efficacy_claim=flag,
                    extension=doc_json["extension"],
                    extractor_used=doc_json["extractor_used"],
                )
            )
            chunk_idx += 1
    return chunks


def run(limit: Optional[int] = None) -> dict:
    if not EXTRACT_DIR.exists():
        raise SystemExit(f"Nothing to chunk — {EXTRACT_DIR} does not exist. Run pipeline.extract first.")

    doc_files = sorted(EXTRACT_DIR.glob("*.json"))
    if limit is not None:
        doc_files = doc_files[:limit]

    total_docs = 0
    total_chunks = 0
    efficacy_chunks = 0

    CHUNKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CHUNKS_PATH.open("w", encoding="utf-8") as out:
        for df in tqdm(doc_files, desc="chunk"):
            doc_json = json.loads(df.read_text(encoding="utf-8"))
            chunks = chunk_doc(doc_json)
            for c in chunks:
                out.write(json.dumps(asdict(c), ensure_ascii=False) + "\n")
                if c.has_efficacy_claim:
                    efficacy_chunks += 1
            total_chunks += len(chunks)
            total_docs += 1

    summary = {
        "docs_chunked": total_docs,
        "chunks_written": total_chunks,
        "efficacy_chunks": efficacy_chunks,
        "chunks_path": str(CHUNKS_PATH),
    }
    print(json.dumps(summary, indent=2))
    return summary


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Chunk extracted docs for embedding.")
    ap.add_argument("--limit", type=int, default=None, help="Chunk only the first N extracted docs (for testing).")
    args = ap.parse_args(argv)
    run(limit=args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
