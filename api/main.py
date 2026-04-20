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

import json
import os
import time
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from api.answer import ANTHROPIC_MODEL, get_generator
from api.input_validator import canned_response, get_validator
from api.logger import (
    log_event,
    log_feedback_record,
    log_query_record,
    log_reject_record,
)
from api.rate_limiter import check_rate_limit
from api.retrieval import PINECONE_INDEX_NAME
from api.validators import (
    FeedbackRequest,
    FeedbackResponse,
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


def _wants_stream(request: Request) -> bool:
    """Client opts into NDJSON streaming via the Accept header. Without it we
    return the legacy single JSON response (preserves the eval harness path
    and any client that doesn't know about streaming)."""
    return "application/x-ndjson" in (request.headers.get("accept") or "").lower()


def _log_query_completion(
    body: "QueryRequest",
    partner_key: str,
    result: dict,
    started: float,
    input_validation_ms: int = 0,
) -> None:
    """Shared post-query logging — same output for both streaming and
    non-streaming paths so the queries.jsonl file has a uniform schema."""
    elapsed_ms = int((time.time() - started) * 1000)
    rewrite = result.get("rewrite") or {}
    timing = dict(result.get("timing") or {})
    timing["total_ms"] = elapsed_ms
    if input_validation_ms:
        timing.setdefault("input_validation_ms", input_validation_ms)

    log_event(
        "query.ok",
        partner_key=partner_key,
        session_id=body.session_id,
        turn_id=body.turn_id,
        user=body.user,
        citations=len(result.get("citations", [])),
        answer_chars=len(result.get("answer", "")),
        history_turns_used=result.get("history_turns_used", 0),
        context_utilization=result.get("context_utilization"),
        rewrite_skipped=rewrite.get("skipped"),
        rewrite_reason=rewrite.get("reason"),
        timing=timing,
        streamed=result.get("_streamed", False),
    )
    sources_payload = [
        {
            "file_path": c.get("file_path", ""),
            "page_or_slide": c.get("page_or_slide"),
            "citation_index": c.get("n"),
        }
        for c in result.get("citations", [])
    ]
    log_query_record(
        session_id=body.session_id,
        turn_id=body.turn_id,
        user=body.user,
        query_original=body.query,
        query_rewritten=(rewrite.get("rewritten") if not rewrite.get("skipped") else None),
        sources=sources_payload,
        response=result.get("answer", ""),
        timing=timing,
        context_utilization_pct=result.get("context_utilization"),
        partner_key=partner_key,
    )


def _rejected_payload(body: "QueryRequest", v, elapsed_ms: int) -> dict:
    """Shape a canned rejection into the same payload shape generate() returns,
    so the streaming + non-streaming exits can share logging + response code."""
    return {
        "answer": canned_response(v.reject_reason),
        "citations": [],
        "context_chunks": [],
        "history_turns_used": 0,
        "context_utilization": None,
        "rewrite": None,
        "timing": {
            "input_validation_ms": v.elapsed_ms,
            "total_ms": elapsed_ms,
        },
        "rejected": True,
        "reject_reason": v.reject_reason,
    }


@app.post("/query")
async def query(
    body: QueryRequest,
    request: Request,
    partner_key: str = Depends(require_partner_key),
):
    check_rate_limit(partner_key)

    started = time.time()
    history_payload = [h.model_dump() for h in body.history] if body.history else None

    log_event(
        "query.received",
        partner_key=partner_key,
        session_id=body.session_id,
        turn_id=body.turn_id,
        user=body.user,
        query_chars=len(body.query),
        history_length=len(history_payload) if history_payload else 0,
        client=request.client.host if request.client else None,
        streaming=_wants_stream(request),
    )

    # Input validation gate — Haiku classifier + in-memory rate-abuse check.
    # Runs before retrieval/generation so rejected queries don't burn Pinecone
    # or Claude generation costs. Keyed on session_id (preferred) or the
    # partner_key sentinel so rate-abuse attribution works in anonymous mode.
    caller_key = body.session_id or partner_key or "anonymous"
    validation = get_validator().validate(body.query, caller_key=caller_key)

    if not validation.on_topic:
        elapsed_ms = int((time.time() - started) * 1000)
        log_event(
            "query.rejected",
            partner_key=partner_key,
            session_id=body.session_id,
            turn_id=body.turn_id,
            user=body.user,
            reject_reason=validation.reject_reason,
            input_validation_ms=validation.elapsed_ms,
            elapsed_ms=elapsed_ms,
        )
        log_reject_record(
            session_id=body.session_id,
            turn_id=body.turn_id,
            user=body.user,
            query=body.query,
            reject_reason=validation.reject_reason,
            elapsed_ms=elapsed_ms,
            partner_key=partner_key,
        )
        canned = _rejected_payload(body, validation, elapsed_ms)
        if _wants_stream(request):
            # Emit the rejection as a single streaming "final" event so the
            # client's existing stream-reader path handles it uniformly.
            def _reject_stream():
                final = {
                    "type": "final",
                    **canned,
                }
                yield (json.dumps(final, default=str, ensure_ascii=False) + "\n").encode("utf-8")
            return StreamingResponse(
                _reject_stream(),
                media_type="application/x-ndjson",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return QueryResponse(
            answer=canned["answer"],
            citations=[],
            query_time_ms=elapsed_ms,
            history_turns_used=0,
            context_utilization=None,
            rewrite=None,
        )

    # Streaming path — client asked for NDJSON deltas.
    if _wants_stream(request):
        return StreamingResponse(
            _stream_query(body, partner_key, history_payload, started, validation.elapsed_ms),
            media_type="application/x-ndjson",
            # Discourage buffering at intermediate proxies so the client sees
            # the first delta immediately rather than after the whole stream.
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Non-streaming path — backward-compatible with eval harness, curl, etc.
    try:
        result = get_generator().generate(body.query, history=history_payload)
    except Exception as exc:
        log_event(
            "query.error",
            partner_key=partner_key,
            session_id=body.session_id,
            turn_id=body.turn_id,
            user=body.user,
            error_type=type(exc).__name__,
            error=str(exc)[:500],
            elapsed_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail="Internal error generating answer.") from exc

    _log_query_completion(body, partner_key, result, started,
                          input_validation_ms=validation.elapsed_ms)
    elapsed_ms = int((time.time() - started) * 1000)
    rewrite = result.get("rewrite") or {}

    return QueryResponse(
        answer=result["answer"],
        citations=result["citations"],
        query_time_ms=elapsed_ms,
        history_turns_used=result.get("history_turns_used", 0),
        context_utilization=result.get("context_utilization"),
        rewrite=QueryRewriteInfo(**rewrite) if rewrite else None,
    )


def _stream_query(
    body: QueryRequest,
    partner_key: str,
    history_payload,
    started: float,
    input_validation_ms: int = 0,
):
    """Generator that yields NDJSON lines for the streaming /query path."""
    final_payload: dict = {}
    try:
        for event in get_generator().generate_stream(body.query, history=history_payload):
            # Fold the validator timing into the final event so clients and
            # logs see the full breakdown.
            if event.get("type") == "final" and input_validation_ms:
                timing = dict(event.get("timing") or {})
                timing.setdefault("input_validation_ms", input_validation_ms)
                event = {**event, "timing": timing}
            yield (json.dumps(event, default=str, ensure_ascii=False) + "\n").encode("utf-8")
            if event.get("type") == "final":
                # Capture for logging after the stream closes.
                final_payload = {
                    "answer": event.get("answer", ""),
                    "citations": event.get("citations", []),
                    "history_turns_used": event.get("history_turns_used", 0),
                    "context_utilization": event.get("context_utilization"),
                    "rewrite": event.get("rewrite"),
                    "timing": event.get("timing") or {},
                    "_streamed": True,
                }
    except Exception as exc:
        err = {"type": "error", "message": str(exc)[:500]}
        yield (json.dumps(err) + "\n").encode("utf-8")
        log_event(
            "query.error",
            partner_key=partner_key,
            session_id=body.session_id,
            turn_id=body.turn_id,
            user=body.user,
            error_type=type(exc).__name__,
            error=str(exc)[:500],
            elapsed_ms=int((time.time() - started) * 1000),
            streaming=True,
        )
        return

    if final_payload:
        _log_query_completion(body, partner_key, final_payload, started,
                              input_validation_ms=input_validation_ms)


@app.post("/feedback", response_model=FeedbackResponse)
async def feedback(
    body: FeedbackRequest,
    partner_key: str = Depends(require_partner_key),
) -> FeedbackResponse:
    """Per-turn feedback submission — 👍/👎 + optional free text. Log-only for
    Alpha/Beta; corpus updates from feedback are a separate governance step.
    """
    check_rate_limit(partner_key)
    log_event(
        "feedback.received",
        partner_key=partner_key,
        session_id=body.session_id,
        turn_id=body.turn_id,
        user=body.user,
        rating=body.rating,
        feedback_chars=len(body.feedback_text or ""),
    )
    log_feedback_record(
        session_id=body.session_id,
        turn_id=body.turn_id,
        user=body.user,
        query=body.query,
        answer=body.answer,
        citations=[c.model_dump() for c in body.citations],
        rating=body.rating,
        feedback_text=body.feedback_text,
        partner_key=partner_key,
    )
    return FeedbackResponse(ok=True)


# Browser-based UI for testing and demos. Mounted last so /health and /query
# routes take precedence. Serves api/static/index.html at /ui.
if _STATIC_DIR.is_dir():
    app.mount("/ui", StaticFiles(directory=str(_STATIC_DIR), html=True), name="ui")
