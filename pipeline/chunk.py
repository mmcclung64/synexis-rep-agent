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


# Material-compatibility classifier. Used to tag chunks that discuss DHP's
# interaction with (or non-reaction with) specific equipment materials. The
# retrieval layer widens the candidate pool with a Pinecone metadata filter
# on `has_material_compatibility: true` when the incoming query is about
# materials — fixes the equipment-materials retrieval miss from CODE_BRIEFING
# Retrieval Tuning #2, option (3).
#
# A chunk qualifies when BOTH patterns hit:
#   - _MATERIAL_NOUNS_RE:  names an equipment material (stainless steel,
#                          polycarbonate, rubber, etc.)
#   - _COMPAT_CONTEXT_RE:  is in a compatibility context (corrosive, reacts
#                          with, compatible, safe on, damage/degrade, etc.)
# Requiring both keeps "a stainless steel table in a cleanroom" (no compat
# context) from triggering while letting "DHP is non-corrosive on stainless
# steel" through.

_MATERIAL_NOUNS_RE = re.compile(
    r"""\b(
        stainless\s+steel | carbon\s+steel | galvanized\s+\w+
        | aluminum | aluminium | brass | copper | nickel | chrome | chromium
        | metal\s+(?:mesh|oxide|housing|catalyst|component|parts?|surfaces?)
        | 18[-\s]?gauge\s+\w+
        | polycarbonate | ABS | PVC | polyethylene | HDPE | LDPE
        | polypropylene | polymer | polymers | polyester | PET | acetal
        | delrin | PEEK | PTFE | teflon | nylon
        | rubber | silicone | gasket(?:s)? | O[-\s]?ring(?:s)?
        | fiberglass | ceramic | fabric(?:s)? | plastic(?:s)?
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

_COMPAT_CONTEXT_RE = re.compile(
    r"""\b(
        non[-\s]?corrosive | non[-\s]?corroding | corrosion[-\s]?resistant
        | corros(?:ion|ive|ive\w*)
        | compatib(?:le|ility)
        | react(?:s|ed|ive|ion|s\s+with)
        | incompatib\w+
        | material\s+(?:compatibility|safe|safety)
        | equipment\s+material(?:s)?
        | safe\s+on\s+\w+
        | (?:does|doesn['']t)\s+not?\s+(?:corrode|react|damage|degrade|affect|harm)
        | (?:damages?|degrades?|deteriorat\w+)\s+(?:to|on|with)\s+\w+
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

# "Strong" compat claims that apply broadly across materials — these qualify
# a chunk as material-compat-relevant even without a specific material noun
# (e.g., SDS "Not reactive", marketing copy "DHP is non-corrosive"). Keeping
# this list narrow so it only catches genuine general-safety language.
_STRONG_COMPAT_CLAIM_RE = re.compile(
    r"""\b(
        non[-\s]?corrosive | non[-\s]?corroding | corrosion[-\s]?resistant
        | non[-\s]?reactive | not\s+reactive
        | does\s+not\s+(?:corrode | react\s+with | oxidize | damage | degrade)
        | doesn['']t\s+(?:corrode | react\s+with | oxidize | damage | degrade)
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)


# Source categories where a bare material mention is itself relevant: device
# manuals and product-maintenance guides describe what the hardware is made of,
# which the briefing explicitly calls out as equipment-material context (Sphere
# housing is 18-gauge stainless steel, etc.).
_DEVICE_CATEGORIES = {"Device Manuals", "Manuals and Guides"}


def has_material_compatibility(text: str, source_category: str = "") -> bool:
    """Should this chunk carry has_material_compatibility=true?

    Rules, in order:
      1. Strong general compat claim ("non-corrosive", "not reactive",
         "does not corrode/react/oxidize") — qualifies regardless of whether
         a specific material is named. These claims apply across substrates
         and are exactly what a rep asking "how does DHP do on various
         materials" needs.
      2. Device-manual / maintenance-guide chunks with ANY material noun —
         construction details ("18-gauge stainless steel housing") count as
         equipment-material context for retrieval-widening purposes.
      3. Everything else: requires BOTH a material noun AND a compat context
         word to avoid false positives from casual material mentions in prose.
    """
    if not text:
        return False
    if _STRONG_COMPAT_CLAIM_RE.search(text):
        return True
    has_material = bool(_MATERIAL_NOUNS_RE.search(text))
    if not has_material:
        return False
    if source_category in _DEVICE_CATEGORIES:
        return True
    return bool(_COMPAT_CONTEXT_RE.search(text))


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
    # Retrieval-tuning tag. Present from chunk-time forward; backfilled onto
    # existing chunks via pipeline/backfill_material_tag.py. When False the
    # field may be omitted from metadata — Pinecone's $eq:true filter treats
    # absent == false, which matches our intent.
    has_material_compatibility: bool = False


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
                    has_material_compatibility=has_material_compatibility(
                        text, doc_json.get("source_category", "")
                    ),
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
