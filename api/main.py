"""FastAPI app for the Synexis Rep Agent.

Endpoints:
    GET  /health       — cheap liveness probe (also used by Render)
    POST /query        — rep question → grounded answer with citations

Auth:
    Either `Authorization: Bearer <partner-key>` or `X-Partner-Key: <partner-key>`.
    If PARTNER_KEYS is unset, anonymous access is allowed for local dev.

Start locally:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.answer import ANTHROPIC_MODEL, get_generator
from api.logger import log_event
from api.rate_limiter import check_rate_limit
from api.retrieval import PINECONE_INDEX_NAME
from api.validators import (
    HealthResponse,
    QueryRequest,
    QueryResponse,
    QueryRewriteInfo,
    _active_partner_keys,
    require_partner_key,
)


_STATIC_DIR = Path(__file__).resolve().parent / "static"


app = FastAPI(title="Synexis Rep Agent", version="0.1.0")

# CORS: Alpha defaults permissive so the browser extension and any early
# partner-side frontend can call /query. Tighten before public Beta.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)


@app.on_event("startup")
async def _startup() -> None:
    log_event(
        "app.startup",
        model=ANTHROPIC_MODEL,
        index=PINECONE_INDEX_NAME,
        auth_configured=bool(_active_partner_keys()),
    )


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        model=ANTHROPIC_MODEL,
        index=PINECONE_INDEX_NAME,
        auth_configured=bool(_active_partner_keys()),
    )


@app.post("/query", response_model=QueryResponse)
async def query(
    body: QueryRequest,
    request: Request,
    partner_key: str = Depends(require_partner_key),
) -> QueryResponse:
    check_rate_limit(partner_key)

    started = time.time()
    history_payload = [h.model_dump() for h in body.history] if body.history else None

    log_event(
        "query.received",
        partner_key=partner_key,
        session_id=body.session_id,
        turn_id=body.turn_id,
        query_chars=len(body.query),
        history_length=len(history_payload) if history_payload else 0,
        client=request.client.host if request.client else None,
    )

    try:
        result = get_generator().generate(body.query, history=history_payload)
    except Exception as exc:
        log_event(
            "query.error",
            partner_key=partner_key,
            session_id=body.session_id,
            turn_id=body.turn_id,
            error_type=type(exc).__name__,
            error=str(exc)[:500],
            elapsed_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail="Internal error generating answer.") from exc

    elapsed_ms = int((time.time() - started) * 1000)
    rewrite = result.get("rewrite") or {}
    log_event(
        "query.ok",
        partner_key=partner_key,
        session_id=body.session_id,
        turn_id=body.turn_id,
        citations=len(result.get("citations", [])),
        context_chunks=len(result.get("context_chunks", [])),
        answer_chars=len(result.get("answer", "")),
        history_turns_used=result.get("history_turns_used", 0),
        context_utilization=result.get("context_utilization"),
        rewrite_original=(rewrite.get("original") or "")[:500],
        rewrite_rewritten=(rewrite.get("rewritten") or "")[:500],
        rewrite_skipped=rewrite.get("skipped"),
        rewrite_reason=rewrite.get("reason"),
        rewrite_ms=rewrite.get("elapsed_ms"),
        elapsed_ms=elapsed_ms,
    )

    return QueryResponse(
        answer=result["answer"],
        citations=result["citations"],
        query_time_ms=elapsed_ms,
        history_turns_used=result.get("history_turns_used", 0),
        context_utilization=result.get("context_utilization"),
        rewrite=QueryRewriteInfo(**rewrite) if rewrite else None,
    )


# Browser-based UI for testing and demos. Mounted last so /health and /query
# routes take precedence. Serves api/static/index.html at /ui.
if _STATIC_DIR.is_dir():
    app.mount("/ui", StaticFiles(directory=str(_STATIC_DIR), html=True), name="ui")
