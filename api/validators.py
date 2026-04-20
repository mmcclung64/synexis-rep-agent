"""Pydantic request/response schemas and partner-key auth dependency."""
from __future__ import annotations

import os
from typing import List, Literal, Optional

from fastapi import Depends, Header, HTTPException, status
from pydantic import BaseModel, Field


# Partner keys are provisioned out-of-band for Alpha/Beta and loaded from env.
# Empty PARTNER_KEYS means "auth not configured yet" — we allow requests in
# that mode so local development and the first internal test runs work, but
# log it as a warning. Set PARTNER_KEYS before opening the API to anyone
# outside Synexis.
def _active_partner_keys() -> List[str]:
    raw = os.getenv("PARTNER_KEYS", "") or ""
    return [k.strip() for k in raw.split(",") if k.strip()]


class HistoryTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(..., max_length=20_000)


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=3, max_length=1000, description="Rep's question.")
    top_n: Optional[int] = Field(
        default=None,
        ge=1,
        le=20,
        description="Override the default number of reranked chunks to show (debug).",
    )
    # Multi-turn support. Empty or absent history = stateless (first turn or the
    # caller has opted out of history). Server applies its own safety truncation
    # on top of anything the client sends.
    history: Optional[List[HistoryTurn]] = Field(
        default=None,
        max_length=50,
        description="Prior turns in the conversation, alternating user/assistant.",
    )
    session_id: Optional[str] = Field(
        default=None,
        max_length=64,
        description="Client-generated session UUID, used for log correlation across turns.",
    )
    turn_id: Optional[int] = Field(
        default=None,
        ge=0,
        le=10_000,
        description="Zero-based turn index within the session.",
    )


class Citation(BaseModel):
    n: int
    chunk_id: str
    file_path: str
    page_or_slide: Optional[object] = None
    source_category: Optional[str] = None
    snippet: Optional[str] = None  # first ~200 chars of the source chunk; used for hover tooltips in the client


class QueryRewriteInfo(BaseModel):
    original: str
    rewritten: str
    skipped: bool
    reason: str
    elapsed_ms: int


class QueryResponse(BaseModel):
    answer: str
    citations: List[Citation]
    query_time_ms: int  # server-side wall-clock for the end-to-end /query handler
    history_turns_used: int = 0   # number of prior turns the server actually included in generation
    context_utilization: Optional[float] = None  # % of context window used (input_tokens / window * 100)
    rewrite: Optional[QueryRewriteInfo] = None   # debug visibility into the multi-turn query rewriter


class HealthResponse(BaseModel):
    status: str
    model: str
    index: str
    auth_configured: bool


def require_partner_key(
    authorization: Optional[str] = Header(default=None),
    x_partner_key: Optional[str] = Header(default=None, alias="X-Partner-Key"),
) -> str:
    """Accept either `Authorization: Bearer <key>` or `X-Partner-Key: <key>`.

    When PARTNER_KEYS is empty (not yet configured), allow anonymous access and
    return the sentinel "anonymous" so logging/rate-limiting still has a key to
    bucket on. This lowers friction for local dev and the first smoke tests;
    before Beta goes live, set PARTNER_KEYS in the deploy environment.
    """
    presented: Optional[str] = None
    if authorization and authorization.lower().startswith("bearer "):
        presented = authorization.split(" ", 1)[1].strip()
    elif x_partner_key:
        presented = x_partner_key.strip()

    active = _active_partner_keys()
    if not active:
        return presented or "anonymous"

    if presented and presented in active:
        return presented

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing or invalid partner key.",
    )
