"""Haiku-backed query rewriter for multi-turn retrieval.

Pre-retrieval step. Takes the conversation history + new user question and
produces a standalone, context-resolved, terminology-normalized query that
Pinecone can retrieve against. Used for retrieval only — the generation call
still sees the original history and the original last user turn.

Skipped when history has fewer than `REWRITER_MIN_HISTORY` messages (i.e., the
rep is on turn 0 or 1 of a session, so there's nothing meaningful to resolve
against). Skipped on API failure too — the caller falls back to the original
query rather than blocking the whole /query path on a rewriter hiccup.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import List, Optional

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential


load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
REWRITER_MODEL = os.getenv("ANTHROPIC_REWRITER_MODEL", "claude-haiku-4-5")
REWRITER_MAX_TOKENS = int(os.getenv("REWRITER_MAX_TOKENS", "256"))
REWRITER_MIN_HISTORY = int(os.getenv("REWRITER_MIN_HISTORY", "2"))  # min sanitized messages to trigger rewrite
REWRITER_MAX_HISTORY_TURNS = 4    # last N turns we hand to the rewriter
ASSISTANT_TURN_CLIP_CHARS = 600   # truncate long assistant turns to keep rewriter cost down
REWRITER_MIN_OUTPUT_CHARS = 5     # shorter output is treated as empty/failed


REWRITER_SYSTEM_PROMPT = """\
You are a query rewriter for a Synexis Dry Hydrogen Peroxide (DHP) knowledge base.

Given the conversation history and a new user question, produce a single standalone query that:
1. Resolves pronouns, ellipsis, and context references ("what about there?", "does it work with that?", "What about the door?") using the prior turns, so the query stands alone without the conversation.
2. Normalizes non-standard terminology to canonical industry terms. Examples: "walking cooler" → "walk-in cooler"; "hatchery" alone → "poultry hatchery" when the conversation is about poultry; "caustic" → "caustic cleaning agents"; "the bot" → "Synexis device".
3. Preserves the user's specific intent. Do not broaden the question, add new topics, or speculate about what they meant.

Output ONLY the rewritten query on a single line. No preamble, no explanation, no quotation marks, no markdown.

If the latest question is already fully self-contained (no pronouns, no context references, standard terminology), output it unchanged."""


@dataclass
class RewriteResult:
    original: str
    rewritten: str
    skipped: bool       # True when rewrite was not attempted (no/short history)
    reason: str         # "no_history" | "rewritten" | "unchanged" | "rewriter_failed:<ExcType>" | "rewriter_empty_output"
    elapsed_ms: int


def _sanitize_for_rewriter(history: List[dict]) -> List[dict]:
    """Strip stale citation markers and Sources sections, then clip long turns.

    Imports the shared sanitizer from api.answer lazily to avoid a circular
    module-load cycle (answer imports rewriter at the top of its module).
    """
    from api.answer import _sanitize_history
    sanitized = _sanitize_history(history) if history else []
    if not sanitized:
        return []
    # Keep last REWRITER_MAX_HISTORY_TURNS turns — the resolver needs recent
    # context, not the whole session.
    sanitized = sanitized[-REWRITER_MAX_HISTORY_TURNS:]
    out: List[dict] = []
    for turn in sanitized:
        content = turn.get("content", "") or ""
        if turn.get("role") == "assistant" and len(content) > ASSISTANT_TURN_CLIP_CHARS:
            content = content[:ASSISTANT_TURN_CLIP_CHARS].rstrip() + "…"
        out.append({"role": turn["role"], "content": content})
    return out


def _format_rewriter_input(history: List[dict], query: str) -> str:
    lines: List[str] = ["Conversation so far:"]
    for t in history:
        role = "User" if t["role"] == "user" else "Assistant"
        lines.append(f"{role}: {t['content']}")
    lines.append("")
    lines.append(f"Latest user question: {query}")
    lines.append("")
    lines.append("Rewrite the latest user question per the rules. Output only the rewritten query.")
    return "\n".join(lines)


class QueryRewriter:
    def __init__(self) -> None:
        self._client = None

    def _ensure_client(self) -> None:
        if self._client is None:
            import anthropic
            if not ANTHROPIC_API_KEY:
                raise RuntimeError("ANTHROPIC_API_KEY is not set in the environment.")
            self._client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _call(self, user_message: str) -> str:
        self._ensure_client()
        resp = self._client.messages.create(
            model=REWRITER_MODEL,
            max_tokens=REWRITER_MAX_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": REWRITER_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_message}],
        )
        parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        return "".join(parts).strip()

    def rewrite(self, query: str, history: List[dict]) -> RewriteResult:
        started = time.time()
        sanitized = _sanitize_for_rewriter(history)
        if len(sanitized) < REWRITER_MIN_HISTORY:
            return RewriteResult(
                original=query,
                rewritten=query,
                skipped=True,
                reason="no_history",
                elapsed_ms=int((time.time() - started) * 1000),
            )

        user_message = _format_rewriter_input(sanitized, query)
        try:
            raw = self._call(user_message)
        except Exception as exc:
            return RewriteResult(
                original=query,
                rewritten=query,
                skipped=False,
                reason=f"rewriter_failed:{type(exc).__name__}",
                elapsed_ms=int((time.time() - started) * 1000),
            )

        cleaned = raw.strip().strip('"').strip("'").splitlines()[0].strip() if raw else ""
        if len(cleaned) < REWRITER_MIN_OUTPUT_CHARS:
            return RewriteResult(
                original=query,
                rewritten=query,
                skipped=False,
                reason="rewriter_empty_output",
                elapsed_ms=int((time.time() - started) * 1000),
            )

        reason = "unchanged" if cleaned == query.strip() else "rewritten"
        return RewriteResult(
            original=query,
            rewritten=cleaned,
            skipped=False,
            reason=reason,
            elapsed_ms=int((time.time() - started) * 1000),
        )


_default_rewriter: Optional[QueryRewriter] = None


def get_rewriter() -> QueryRewriter:
    global _default_rewriter
    if _default_rewriter is None:
        _default_rewriter = QueryRewriter()
    return _default_rewriter
