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
import re
import time
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from api.answer import ANTHROPIC_MODEL, get_generator
from api.input_validator import canned_response, get_validator
from api.intros import get_intros, refresh_intros_background
from api.github_logger import append_feedback as _github_append_feedback
from api.logger import (
    _LOG_DIR,
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
    _partner_vertical,
    _partner_verticals,
    require_partner_key,
)


_STATIC_DIR = Path(__file__).resolve().parent / "static"

_WS_RE = re.compile(r"\s+")

# ---------------------------------------------------------------------------
# Session-level response cache
# ---------------------------------------------------------------------------
# Keyed on (session_id, normalised_query). TTL = 30 minutes.
# Scope is deliberately per-session: a 30-minute window is short enough that
# corpus updates between sessions are not a concern, and the same rep
# re-running the same question in a session (common during alpha testing)
# gets an instant response. Cross-session caching is intentionally avoided
# so stale answers don't persist after a corpus ingest.
#
# Cache is busted globally by POST /cache/clear (called by the ingest
# pipeline after any Pinecone upsert or delete).
# ---------------------------------------------------------------------------

class _SessionCache:
    TTL = 30 * 60  # seconds

    def __init__(self) -> None:
        self._store: dict[tuple, tuple[float, dict]] = {}

    @staticmethod
    def _norm(query: str) -> str:
        return _WS_RE.sub(" ", (query or "").strip().lower())

    def _key(self, session_id: str, query: str) -> tuple:
        return (session_id or "", self._norm(query))

    def get(self, session_id: str, query: str) -> "dict | None":
        key = self._key(session_id, query)
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, result = entry
        if time.time() - ts > self.TTL:
            del self._store[key]
            return None
        return result

    def set(self, session_id: str, query: str, result: dict) -> None:
        self._evict_expired()
        self._store[self._key(session_id, query)] = (time.time(), result)

    def clear(self) -> int:
        n = len(self._store)
        self._store.clear()
        return n

    def _evict_expired(self) -> None:
        now = time.time()
        stale = [k for k, (ts, _) in self._store.items() if now - ts > self.TTL]
        for k in stale:
            del self._store[k]


# Set ENABLE_SESSION_CACHE=false on Render to disable without a redeploy.
_CACHE_ENABLED = os.getenv("ENABLE_SESSION_CACHE", "true").lower() not in ("false", "0", "no")
_cache = _SessionCache()


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

    # Session cache check — only on first turns (no history) or short history
    # where stale context is unlikely. Rejected queries are never cached (handled
    # above). Cache key: (session_id, normalised query).
    cached = _cache.get(body.session_id or "", body.query) if _CACHE_ENABLED else None
    if cached is not None:
        log_event(
            "query.cache_hit",
            partner_key=partner_key,
            session_id=body.session_id,
            turn_id=body.turn_id,
            user=body.user,
        )
        elapsed_ms = int((time.time() - started) * 1000)
        rewrite = cached.get("rewrite") or {}
        if _wants_stream(request):
            def _cache_stream():
                final = {"type": "final", **cached, "timing": {**(cached.get("timing") or {}), "total_ms": elapsed_ms}}
                yield (json.dumps(final, default=str, ensure_ascii=False) + "\n").encode("utf-8")
            return StreamingResponse(
                _cache_stream(),
                media_type="application/x-ndjson",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return QueryResponse(
            answer=cached["answer"],
            citations=cached["citations"],
            query_time_ms=elapsed_ms,
            history_turns_used=cached.get("history_turns_used", 0),
            context_utilization=cached.get("context_utilization"),
            rewrite=QueryRewriteInfo(**rewrite) if rewrite else None,
        )

    # Streaming path — client asked for NDJSON deltas.
    if _wants_stream(request):
        return StreamingResponse(
            _stream_query(body, partner_key, history_payload, started, validation.elapsed_ms,
                          session_id=body.session_id or ""),
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

    # Store in session cache for repeat queries within this session.
    if _CACHE_ENABLED:
        _cache.set(body.session_id or "", body.query, result)

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
    session_id: str = "",
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
        # Store in session cache so a repeat query in the same session is instant.
        if _CACHE_ENABLED:
            _cache.set(session_id or body.session_id or "", body.query, final_payload)


@app.get("/intros")
async def intros_get() -> dict:
    """Return cached vertical intro sentences for the extension.

    Response shape:
        {
          "intros": {"Healthcare": "...", "Animal Health": "...", ...},
          "generated_at": <epoch float>,
          "stale": <bool>
        }

    If the cache is missing or stale this call blocks while generating (first
    cold-start only — in normal operation the cache is always warm).
    The extension caches the result locally for 12 hours, so this endpoint
    is hit at most a few times a day per device.

    No partner-key auth required — intros contain no proprietary data and the
    generation cost is negligible (Haiku).
    """
    return get_intros()


@app.post("/intros/refresh")
async def intros_refresh(
    background_tasks: BackgroundTasks,
    partner_key: str = Depends(require_partner_key),
) -> dict:
    """Kick off a background regeneration of vertical intros.

    Returns 202 immediately; generation runs in a background thread (~10-15 s).
    Called automatically by the --delta pipeline after a SharePoint sync so
    intros stay grounded in the latest corpus content.
    """
    background_tasks.add_task(refresh_intros_background)
    log_event("intros.refresh_requested", partner_key=partner_key)
    return {"ok": True, "message": "Intro refresh started in background"}


@app.get("/config")
async def config(partner_key: str = Depends(require_partner_key)) -> dict:
    """Return per-partner configuration for the extension.

    Response shape::

        {
          "default_vertical":  "Healthcare",      # first vertical, or null (backward compat)
          "default_verticals": ["Healthcare"]      # full list, or null
        }

    The extension calls this once at startup (cached locally for 24 h) to
    decide whether to show the focused partner picker or the full industry
    picker.  ``default_verticals`` is the canonical field going forward;
    ``default_vertical`` is kept for older cached responses.
    To add a second vertical for a partner (e.g. Poultry), update the
    PARTNER_VERTICALS env var list and bump PARTNER_CONFIG_KEY in the extension
    so the 24 h cache is busted on next reload.
    """
    verticals = _partner_verticals(partner_key)
    return {
        "default_vertical":  verticals[0] if verticals else None,
        "default_verticals": verticals if verticals else None,
    }


@app.post("/feedback", response_model=FeedbackResponse)
async def feedback(
    body: FeedbackRequest,
    background_tasks: BackgroundTasks,
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
    record = dict(
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
    log_feedback_record(**record)
    # Persist to GitHub so feedback survives Render redeploys.
    background_tasks.add_task(_github_append_feedback, record)
    return FeedbackResponse(ok=True)


@app.get("/logs/recent")
async def logs_recent(
    partner_key: str = Depends(require_partner_key),
    event_type: Optional[str] = None,
    limit: int = 100,
) -> dict:
    """Return the most recent log entries from queries.jsonl.

    Query params:
      event_type  — filter to "query", "feedback", or "rejected" (omit for all)
      limit       — max number of entries to return (default 100, max 500)

    Auth: any valid partner key.
    """
    limit = min(limit, 500)
    log_path = _LOG_DIR / "queries.jsonl"
    if not log_path.exists():
        return {"ok": True, "count": 0, "records": []}

    records = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event_type and obj.get("event_type") != event_type:
                    continue
                records.append(obj)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Could not read log file: {exc}")

    # Return most-recent first
    records = records[-limit:][::-1]
    return {"ok": True, "count": len(records), "records": records}


@app.post("/cache/clear")
async def cache_clear(
    partner_key: str = Depends(require_partner_key),
) -> dict:
    """Bust the session cache. Called by the ingest pipeline after any Pinecone
    upsert or delete so reps don't receive stale cached answers post-update.
    Returns the number of entries cleared."""
    n = _cache.clear()
    log_event("cache.cleared", partner_key=partner_key, entries_cleared=n)
    return {"ok": True, "entries_cleared": n}


@app.post("/graph/notifications")
async def graph_notifications(
    request: Request,
    background_tasks: BackgroundTasks,
) -> Response:
    """Microsoft Graph change-notification webhook endpoint.

    Graph requires two things from this endpoint:
      1. Validation handshake — on subscription creation Graph sends a GET (or POST)
         with ?validationToken=<token>. Respond within 10 seconds with 200 + the token
         as plain text, Content-Type: text/plain.
      2. Normal notifications — POST with JSON payload. Respond 202 immediately,
         then process asynchronously in a background task so Graph doesn't time out.

    Auth: Graph validates via clientState in the payload body (checked in
    pipeline.sharepoint_sync.process_notification). No partner key required here
    because the caller is Microsoft's infrastructure, not a rep client.
    """
    # ── Validation handshake ────────────────────────────────────────────────
    validation_token = request.query_params.get("validationToken")
    if validation_token:
        log_event("graph.subscription_validation", token_len=len(validation_token))
        return Response(
            content=validation_token,
            media_type="text/plain",
            status_code=200,
        )

    # ── Normal notification ─────────────────────────────────────────────────
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    notification_count = len(payload.get("value", []))
    log_event("graph.notification_received", count=notification_count)

    # Process asynchronously — Graph requires 202 within a few seconds.
    background_tasks.add_task(_run_notification_processing, payload)

    return Response(status_code=202)


def _run_notification_processing(payload: dict) -> None:
    """Background task: process Graph notifications after 202 is sent."""
    try:
        from pipeline.sharepoint_sync import process_notification
        process_notification(payload)
    except Exception as exc:
        log_event("graph.notification_error", error=str(exc)[:500])


# Browser-based UI for testing and demos. Mounted last so /health and /query
# routes take precedence. Serves api/static/index.html at /ui.
if _STATIC_DIR.is_dir():
    app.mount("/ui", StaticFiles(directory=str(_STATIC_DIR), html=True), name="ui")
