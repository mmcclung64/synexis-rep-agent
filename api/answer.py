"""Answer generation: hybrid retrieval → dedup → Claude with full guardrail prompt.

Usage (programmatic):
    from api.answer import get_generator
    result = get_generator().generate("your question")
    # result -> {"answer": str, "citations": [ {file_path, page_or_slide, chunk_id}, ... ]}

Usage (CLI):
    python -m api.answer "your question"
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from api.retrieval import Hit, get_retriever
from api.rewriter import get_rewriter


load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
SYSTEM_PROMPT_PATH = REPO_ROOT / "system_prompt.txt"

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
MAX_TOKENS = int(os.getenv("ANSWER_MAX_TOKENS", "1024"))

# Retrieval/context sizing. Keep context tight so every rule in the system
# prompt has room to apply reliably.
RETRIEVE_TOP_N = 12      # pull this many from rerank before dedup
CONTEXT_MAX_CHUNKS = 8   # pass this many to Claude after dedup

# Multi-turn sizing.
MAX_HISTORY_TURNS = 8        # Alpha: simple last-N truncation (sliding-window summarization deferred to Beta)
# Rough context-window size for Sonnet 4.6; used only to report context_utilization
# as a % so the client can warn the rep when approaching the limit.
CONTEXT_WINDOW_TOKENS = 200_000

_WS_RE = re.compile(r"\s+")


def _text_fingerprint(text: str) -> str:
    """Hash for near-exact-duplicate detection across copy-pasted slide decks."""
    norm = _WS_RE.sub(" ", (text or "").strip().lower())
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()


def _dedup_by_text(hits: List[Hit]) -> List[Hit]:
    """Keep the highest-reranked occurrence of each unique chunk text."""
    seen: Dict[str, Hit] = {}
    for h in hits:
        fp = _text_fingerprint(h.text)
        prev = seen.get(fp)
        if prev is None:
            seen[fp] = h
            continue
        prev_score = prev.rerank_score or 0.0
        cur_score = h.rerank_score or 0.0
        if cur_score > prev_score:
            seen[fp] = h
    # Preserve rerank order
    return sorted(seen.values(), key=lambda h: (h.rerank_score or 0.0), reverse=True)


def _format_context(hits: List[Hit]) -> str:
    lines: List[str] = []
    for i, h in enumerate(hits, start=1):
        md = h.metadata or {}
        file_path = md.get("file_path", "(unknown file)")
        page = md.get("page_or_slide", "")
        lines.append(f"[{i}] source: {file_path}, page/slide: {page}")
        lines.append(h.text or "")
        lines.append("")
    return "\n".join(lines).rstrip()


# Citation numbers in the model's answer, e.g. [1], [2], [3, 5].
_CITE_RE = re.compile(r"\[(\d+(?:\s*,\s*\d+)*)\]")

# Lines that mark the start of Claude's own "Sources:" section at the bottom
# of the answer. We strip that section server-side so the extension (or /ui)
# renders a single, canonical, deduped-and-renumbered sources list rather than
# two overlapping ones.
_SOURCES_HEADER_RE = re.compile(
    r"^\s*(?:\*\*|#+\s*)?sources\s*:?\s*(?:\*\*)?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
# Trailing markdown separators / blank lines left behind after stripping the
# Sources section.
_TRAILING_SEPARATOR_RE = re.compile(r"(?:\s*(?:---|\*\*\*|___)\s*)+\s*$", re.MULTILINE)

SNIPPET_CHARS = 200


def _sanitize_history(history: List[dict]) -> List[dict]:
    """Prepare prior turns for inclusion in the messages array.

    - Keeps only ``role`` ∈ {"user", "assistant"} with non-empty content.
    - For assistant turns: strips any trailing Sources section and all `[N]`
      citation markers. Prior turns' citation numbers don't map to the current
      turn's context, so showing them risks confusing the model. Quoted
      content is preserved.
    - Drops a trailing user turn if present (that's the incomplete prior turn
      the new query is replacing).
    - Truncates to the most recent ``MAX_HISTORY_TURNS`` turns.
    - Ensures the messages array starts with a user turn (Anthropic requirement):
      if the first remaining turn is an assistant, drop it.
    """
    normalized: List[dict] = []
    for turn in history:
        role = turn.get("role") if isinstance(turn, dict) else None
        content = turn.get("content", "") if isinstance(turn, dict) else ""
        if role not in ("user", "assistant"):
            continue
        if not isinstance(content, str) or not content.strip():
            continue
        if role == "assistant":
            content = _strip_sources_section(content)
            content = _CITE_RE.sub("", content)
            content = re.sub(r"[ \t]+\n", "\n", content)
            content = re.sub(r"\n{3,}", "\n\n", content).strip()
            if not content:
                continue
        normalized.append({"role": role, "content": content})

    # Drop a trailing user turn — that's an orphan left over from a prior failed
    # generation; the new query supersedes it.
    if normalized and normalized[-1]["role"] == "user":
        normalized = normalized[:-1]

    # Keep the last MAX_HISTORY_TURNS turns.
    if len(normalized) > MAX_HISTORY_TURNS:
        normalized = normalized[-MAX_HISTORY_TURNS:]

    # Must start with user role.
    while normalized and normalized[0]["role"] != "user":
        normalized.pop(0)

    return normalized


def _referenced_citation_numbers(answer_text: str) -> List[int]:
    refs: List[int] = []
    for m in _CITE_RE.finditer(answer_text or ""):
        for part in m.group(1).split(","):
            part = part.strip()
            if part.isdigit():
                refs.append(int(part))
    seen: set[int] = set()
    ordered: List[int] = []
    for n in refs:
        if n not in seen:
            seen.add(n)
            ordered.append(n)
    return ordered


def _strip_sources_section(answer_text: str) -> str:
    """Remove the 'Sources:' block Claude appends, plus any trailing separators."""
    match = _SOURCES_HEADER_RE.search(answer_text)
    if match:
        answer_text = answer_text[: match.start()]
    answer_text = _TRAILING_SEPARATOR_RE.sub("", answer_text)
    return answer_text.rstrip()


def _rewrite_citations(answer_text: str, hits: List[Hit]) -> tuple[str, List[dict]]:
    """Dedup, renumber, and rewrite citation markers.

    - Collapses multiple old-N's that point to the same (file_path, page_or_slide)
      into a single new-N (the lowest).
    - Renumbers citations sequentially by order of first appearance in the body.
    - Rewrites every `[N]` / `[N, M]` marker in the body to use the new numbering
      (and dedupes within a single marker).
    - Returns the rewritten body plus the sorted canonical citations list.
    """
    # Build old-N → hit lookup for every old-N that actually appears in the body.
    referenced_old_ns = _referenced_citation_numbers(answer_text)
    old_to_hit: Dict[int, Hit] = {}
    for old_n in referenced_old_ns:
        if 1 <= old_n <= len(hits):
            old_to_hit[old_n] = hits[old_n - 1]

    # Canonical source key → new-N (assigned in order of first appearance).
    source_key_to_new_n: Dict[tuple, int] = {}
    old_to_new: Dict[int, int] = {}
    next_n = 1
    for old_n in referenced_old_ns:
        hit = old_to_hit.get(old_n)
        if hit is None:
            continue
        md = hit.metadata or {}
        key = (str(md.get("file_path", "")), str(md.get("page_or_slide", "")))
        if key not in source_key_to_new_n:
            source_key_to_new_n[key] = next_n
            next_n += 1
        old_to_new[old_n] = source_key_to_new_n[key]

    def _rewrite_marker(match: "re.Match[str]") -> str:
        parts = [p.strip() for p in match.group(1).split(",")]
        new_parts: List[str] = []
        seen: set[str] = set()
        for p in parts:
            if not p.isdigit():
                continue
            new_n = old_to_new.get(int(p))
            if new_n is None:
                # Out-of-range or untracked N — preserve as-is so we don't mask an issue.
                candidate = p
            else:
                candidate = str(new_n)
            if candidate not in seen:
                seen.add(candidate)
                new_parts.append(candidate)
        if not new_parts:
            return ""
        return f"[{', '.join(new_parts)}]"

    rewritten = _CITE_RE.sub(_rewrite_marker, answer_text)

    # Canonical citations list, one entry per unique (file_path, page_or_slide).
    new_n_to_hit: Dict[int, Hit] = {}
    for old_n, new_n in old_to_new.items():
        new_n_to_hit.setdefault(new_n, old_to_hit[old_n])

    citations: List[dict] = []
    for new_n in sorted(new_n_to_hit.keys()):
        h = new_n_to_hit[new_n]
        md = h.metadata or {}
        text = h.text or ""
        snippet = text.replace("\n", " ").strip()
        if len(snippet) > SNIPPET_CHARS:
            snippet = snippet[:SNIPPET_CHARS].rstrip() + "…"
        citations.append(
            {
                "n": new_n,
                "chunk_id": h.chunk_id,
                "file_path": md.get("file_path", ""),
                "page_or_slide": md.get("page_or_slide", ""),
                "source_category": md.get("source_category", ""),
                "snippet": snippet,
            }
        )
    return rewritten, citations


def _read_system_prompt() -> str:
    if not SYSTEM_PROMPT_PATH.exists():
        raise RuntimeError(f"System prompt file not found at {SYSTEM_PROMPT_PATH}.")
    return SYSTEM_PROMPT_PATH.read_text(encoding="utf-8").strip()


class AnswerGenerator:
    """End-to-end answer pipeline: retrieve → dedup → Claude → structured response."""

    def __init__(self) -> None:
        self._anthropic = None
        self._system_prompt: Optional[str] = None

    def _ensure_ready(self) -> None:
        if self._anthropic is None:
            import anthropic
            if not ANTHROPIC_API_KEY:
                raise RuntimeError("ANTHROPIC_API_KEY is not set in the environment.")
            self._anthropic = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        if self._system_prompt is None:
            self._system_prompt = _read_system_prompt()

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=20))
    def _claude_call(self, messages: List[dict]) -> tuple[str, Optional[int]]:
        """Call Claude with the full messages array and return (text, input_tokens)."""
        self._ensure_ready()
        resp = self._anthropic.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=MAX_TOKENS,
            # Prompt caching for the system prompt — it's constant across queries.
            system=[
                {
                    "type": "text",
                    "text": self._system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=messages,
        )
        parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        input_tokens = None
        usage = getattr(resp, "usage", None)
        if usage is not None:
            input_tokens = getattr(usage, "input_tokens", None)
        return "".join(parts).strip(), input_tokens

    def generate(self, query: str, history: Optional[List[dict]] = None) -> dict:
        # 1. Rewrite the query for retrieval when we have enough history.
        #    The rewriter resolves context references and normalizes non-standard
        #    terminology. Generation still receives the original history + the
        #    original last user turn — rewriting is for retrieval only.
        rewrite = get_rewriter().rewrite(query, history or [])
        retrieval_query = rewrite.rewritten

        # 2. Retrieve against the rewritten (or original) query.
        retriever = get_retriever()
        raw_hits = retriever.retrieve(retrieval_query, top_n=RETRIEVE_TOP_N)

        # 3. Dedup across copy-pasted slide decks
        hits = _dedup_by_text(raw_hits)[:CONTEXT_MAX_CHUNKS]

        rewrite_payload = {
            "original": rewrite.original,
            "rewritten": rewrite.rewritten,
            "skipped": rewrite.skipped,
            "reason": rewrite.reason,
            "elapsed_ms": rewrite.elapsed_ms,
        }

        if not hits:
            return {
                "answer": (
                    "I don't have that information in my knowledge base. "
                    "For this one, contact Synexis support."
                ),
                "citations": [],
                "context_chunks": [],
                "history_turns_used": 0,
                "context_utilization": None,
                "rewrite": rewrite_payload,
            }

        # 4. Build the messages array: sanitized prior history + new user turn.
        sanitized = _sanitize_history(history or [])
        context_block = _format_context(hits)
        new_user_message = (
            f"Question: {query}\n\n"
            f"Context:\n{context_block}"
        )
        messages: List[dict] = list(sanitized) + [
            {"role": "user", "content": new_user_message}
        ]

        # 5. Claude call
        raw_answer, input_tokens = self._claude_call(messages)

        # 6. Strip Claude's own Sources section — the client renders one canonical
        #    list from the citations array below.
        body = _strip_sources_section(raw_answer)

        # 7. Dedup by (file_path, page_or_slide), renumber [N] sequentially from
        #    first appearance, and rewrite markers in the body to match.
        answer_text, citations = _rewrite_citations(body, hits)

        context_utilization: Optional[float] = None
        if isinstance(input_tokens, int) and input_tokens > 0:
            context_utilization = round(input_tokens / CONTEXT_WINDOW_TOKENS * 100, 2)

        return {
            "answer": answer_text,
            "citations": citations,
            "context_chunks": [
                {
                    "n": i,
                    "chunk_id": h.chunk_id,
                    "file_path": (h.metadata or {}).get("file_path", ""),
                    "page_or_slide": (h.metadata or {}).get("page_or_slide", ""),
                    "rerank_score": h.rerank_score,
                    "has_efficacy_claim": (h.metadata or {}).get("has_efficacy_claim"),
                }
                for i, h in enumerate(hits, start=1)
            ],
            "history_turns_used": len(sanitized),
            "context_utilization": context_utilization,
            "rewrite": rewrite_payload,
        }


_default_generator: Optional[AnswerGenerator] = None


def get_generator() -> AnswerGenerator:
    global _default_generator
    if _default_generator is None:
        _default_generator = AnswerGenerator()
    return _default_generator


# ---------- CLI harness ----------

def _print_result(result: dict, show_context: bool) -> None:
    print("\n=== ANSWER ===")
    print(result["answer"])
    print("\n=== CITATIONS USED ===")
    if not result["citations"]:
        print("(none cited)")
    else:
        for c in result["citations"]:
            print(f"[{c['n']}] {c['file_path']} — page/slide {c['page_or_slide']}  ({c['chunk_id']})")
    if show_context:
        print("\n=== CONTEXT CHUNKS (top after dedup) ===")
        for c in result["context_chunks"]:
            rr = f"{c['rerank_score']:.4f}" if c["rerank_score"] is not None else "—"
            print(f"[{c['n']}] rerank={rr}  eff={c['has_efficacy_claim']}  "
                  f"{c['file_path']}  p{c['page_or_slide']}  ({c['chunk_id']})")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Ad-hoc end-to-end answer harness.")
    ap.add_argument("query", type=str)
    ap.add_argument(
        "--show-context",
        action="store_true",
        help="Print the deduped context chunk list below the answer.",
    )
    args = ap.parse_args(argv)
    result = get_generator().generate(args.query)
    _print_result(result, show_context=args.show_context)
    return 0


if __name__ == "__main__":
    sys.exit(main())
