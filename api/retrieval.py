"""Hybrid retriever: Voyage dense + BM25 sparse + Voyage rerank cross-encoder.

Usage (programmatic):
    from api.retrieval import get_retriever
    hits = get_retriever().retrieve("your question", top_n=10)

Usage (CLI, for validation):
    python -m api.retrieval "your question" --top-n 5
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from rank_bm25 import BM25Okapi
from tenacity import retry, stop_after_attempt, wait_exponential


load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
CHUNKS_PATH = REPO_ROOT / "work" / "chunks.jsonl"

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "sra")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
VOYAGE_EMBEDDING_MODEL = os.getenv("VOYAGE_EMBEDDING_MODEL", "voyage-3")
VOYAGE_RERANK_MODEL = os.getenv("VOYAGE_RERANK_MODEL", "rerank-2.5-lite")

# Candidate-pool sizes before rerank. Tune these from eval results; starting
# point is drawn from retrieval literature for ~5K-chunk corpora.
DENSE_TOP_K = 50
SPARSE_TOP_K = 50
RERANK_TOP_N = 10

_WORD_RE = re.compile(r"[A-Za-z0-9]+")


def _tokenize(text: str) -> List[str]:
    return [t.lower() for t in _WORD_RE.findall(text or "")]


@dataclass
class Hit:
    chunk_id: str
    text: str
    metadata: dict
    dense_score: Optional[float] = None
    sparse_score: Optional[float] = None
    rerank_score: Optional[float] = None


class Retriever:
    """Hybrid dense + sparse + rerank retriever against the Synexis rep agent corpus.

    Clients and the BM25 index are initialized lazily on first use so importing
    the module stays cheap (useful for FastAPI cold starts and test harnesses).
    """

    def __init__(self, chunks_path: Path = CHUNKS_PATH) -> None:
        self._chunks_path = chunks_path
        self._voyage = None
        self._pc_index = None
        self._bm25: Optional[BM25Okapi] = None
        self._corpus: List[dict] = []

    # ---------- lazy init ----------

    def _ensure_clients(self) -> None:
        if self._voyage is None:
            import voyageai
            if not VOYAGE_API_KEY:
                raise RuntimeError("VOYAGE_API_KEY is not set in the environment.")
            self._voyage = voyageai.Client(api_key=VOYAGE_API_KEY)
        if self._pc_index is None:
            from pinecone import Pinecone
            if not PINECONE_API_KEY:
                raise RuntimeError("PINECONE_API_KEY is not set in the environment.")
            pc = Pinecone(api_key=PINECONE_API_KEY)
            self._pc_index = pc.Index(PINECONE_INDEX_NAME)

    def _ensure_bm25(self) -> None:
        if self._bm25 is not None:
            return
        if not self._chunks_path.exists():
            raise RuntimeError(
                f"BM25 corpus file not found at {self._chunks_path}. "
                "Run the ingest pipeline (pipeline.chunk) or ship chunks.jsonl with the deploy."
            )
        corpus: List[dict] = []
        tokenized: List[List[str]] = []
        with self._chunks_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                c = json.loads(line)
                corpus.append(c)
                tokenized.append(_tokenize(c["text"]))
        self._corpus = corpus
        self._bm25 = BM25Okapi(tokenized)

    # ---------- retrieval primitives ----------

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=20))
    def _embed_query(self, query: str) -> List[float]:
        self._ensure_clients()
        result = self._voyage.embed([query], model=VOYAGE_EMBEDDING_MODEL, input_type="query")
        return result.embeddings[0]

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=20))
    def _dense_search(self, query_vec: List[float], top_k: int) -> List[Hit]:
        self._ensure_clients()
        resp = self._pc_index.query(vector=query_vec, top_k=top_k, include_metadata=True)
        hits: List[Hit] = []
        for m in resp.matches:
            md = dict(m.metadata or {})
            hits.append(
                Hit(
                    chunk_id=m.id,
                    text=md.get("text", ""),
                    metadata=md,
                    dense_score=float(m.score),
                )
            )
        return hits

    def _sparse_search(self, query: str, top_k: int) -> List[Hit]:
        self._ensure_bm25()
        q_tokens = _tokenize(query)
        if not q_tokens:
            return []
        scores = self._bm25.get_scores(q_tokens)
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        hits: List[Hit] = []
        for i in top_idx:
            if scores[i] <= 0:
                continue
            c = self._corpus[i]
            md = {k: v for k, v in c.items() if k != "text"}
            hits.append(
                Hit(
                    chunk_id=c["chunk_id"],
                    text=c["text"],
                    metadata=md,
                    sparse_score=float(scores[i]),
                )
            )
        return hits

    @staticmethod
    def _merge(dense: List[Hit], sparse: List[Hit]) -> List[Hit]:
        """Union by chunk_id. Prefer dense hit's metadata; carry sparse score over."""
        by_id: dict[str, Hit] = {}
        for h in dense:
            by_id[h.chunk_id] = h
        for h in sparse:
            if h.chunk_id in by_id:
                by_id[h.chunk_id].sparse_score = h.sparse_score
            else:
                by_id[h.chunk_id] = h
        return list(by_id.values())

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=20))
    def _rerank(self, query: str, candidates: List[Hit], top_n: int) -> List[Hit]:
        if not candidates:
            return []
        self._ensure_clients()
        documents = [h.text for h in candidates]
        result = self._voyage.rerank(
            query=query,
            documents=documents,
            model=VOYAGE_RERANK_MODEL,
            top_k=top_n,
        )
        reranked: List[Hit] = []
        for r in result.results:
            h = candidates[r.index]
            h.rerank_score = float(r.relevance_score)
            reranked.append(h)
        return reranked

    # ---------- public API ----------

    def retrieve(
        self,
        query: str,
        top_n: int = RERANK_TOP_N,
        dense_k: int = DENSE_TOP_K,
        sparse_k: int = SPARSE_TOP_K,
    ) -> List[Hit]:
        q_vec = self._embed_query(query)
        dense = self._dense_search(q_vec, dense_k)
        sparse = self._sparse_search(query, sparse_k)
        merged = self._merge(dense, sparse)
        return self._rerank(query, merged, top_n)


_default_retriever: Optional[Retriever] = None


def get_retriever() -> Retriever:
    """Process-wide singleton. Safe for FastAPI since init is lazy and idempotent."""
    global _default_retriever
    if _default_retriever is None:
        _default_retriever = Retriever()
    return _default_retriever


# ---------- CLI test harness ----------

def _format_hit(i: int, h: Hit, snippet_chars: int) -> str:
    dense = f"{h.dense_score:.4f}" if h.dense_score is not None else "—"
    sparse = f"{h.sparse_score:.3f}" if h.sparse_score is not None else "—"
    rerank = f"{h.rerank_score:.4f}" if h.rerank_score is not None else "—"
    md = h.metadata
    snippet = (h.text or "").replace("\n", " ").strip()
    if len(snippet) > snippet_chars:
        snippet = snippet[:snippet_chars] + "…"
    lines = [
        f"#{i:<2}  rerank={rerank}  dense={dense}  sparse={sparse}",
        f"     {h.chunk_id}  |  {md.get('file_path', '')}  |  page {md.get('page_or_slide', '')}",
        f"     category={md.get('source_category', '')}  intake={md.get('intake_mode', '')}  "
        f"efficacy={md.get('has_efficacy_claim')}",
        f"     > {snippet}",
    ]
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Ad-hoc hybrid retrieval harness.")
    ap.add_argument("query", type=str, help="Query text")
    ap.add_argument("--top-n", type=int, default=5, help="Final reranked hits to show.")
    ap.add_argument("--dense-k", type=int, default=DENSE_TOP_K)
    ap.add_argument("--sparse-k", type=int, default=SPARSE_TOP_K)
    ap.add_argument("--snippet", type=int, default=280, help="Snippet length in chars.")
    args = ap.parse_args(argv)

    r = get_retriever()
    hits = r.retrieve(
        args.query,
        top_n=args.top_n,
        dense_k=args.dense_k,
        sparse_k=args.sparse_k,
    )
    print(f"\nQuery: {args.query}")
    print(f"Returned {len(hits)} hits (dense_k={args.dense_k} sparse_k={args.sparse_k} top_n={args.top_n})\n")
    for i, h in enumerate(hits, start=1):
        print(_format_hit(i, h, args.snippet))
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
