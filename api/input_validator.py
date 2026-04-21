"""Haiku-backed input validator.

Sits at the top of the /query pipeline, BEFORE retrieval and generation. If the
query is off-topic for a Synexis sales agent, asks for medical advice, or
contains PII, the validator rejects and the caller returns a canned response
without burning Pinecone + Claude generation costs.

Rate-abuse detection is a separate in-memory window check keyed on
(session_id or partner_key) + normalized query hash. Claude isn't in the loop
for that — it's a pattern check, not a content classification.

Validator output shape:
    ValidationResult(
        on_topic: bool,
        reject_reason: Optional[str],   # "off_topic" | "medical_advice" | "pii" | "rate_abuse" | None
        elapsed_ms: int,
    )

Failure mode:
    Any Anthropic exception or JSON parse failure → fail OPEN (on_topic=True).
    We'd rather let a legitimate question through than block one on a hiccup.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential


load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
VALIDATOR_MODEL = os.getenv("ANTHROPIC_VALIDATOR_MODEL", "claude-haiku-4-5")
VALIDATOR_MAX_TOKENS = int(os.getenv("VALIDATOR_MAX_TOKENS", "80"))

RATE_ABUSE_WINDOW_SECONDS = int(os.getenv("VALIDATOR_RATE_WINDOW_SEC", "60"))
RATE_ABUSE_THRESHOLD = int(os.getenv("VALIDATOR_RATE_THRESHOLD", "3"))


REJECT_REASONS = {"off_topic", "medical_advice", "pii", "rate_abuse"}

# Canned responses the caller can surface when the validator rejects. Phrasing
# keeps the existing brand voice — no generic chatbot language.
CANNED_RESPONSES = {
    "off_topic": (
        "I'm focused on Synexis products and DHP® technology. "
        "I can't help with that. Is there a Synexis product or efficacy "
        "question I can answer instead?"
    ),
    "medical_advice": (
        "I can't provide medical advice — please direct clinical questions to "
        "a qualified healthcare professional."
    ),
    "pii": (
        "Please remove personal details (names, emails, phone numbers, "
        "account info) from your question and try again."
    ),
    "rate_abuse": (
        "You've submitted this query repeatedly in the last minute. "
        "Wait a moment before retrying."
    ),
}


VALIDATOR_SYSTEM_PROMPT = """\
You classify queries sent to the Synexis Rep Agent — an internal knowledge
agent for sales reps of Synexis Dry Hydrogen Peroxide (DHP) technology.

Classify the incoming query into exactly one category:

- "on_topic" — a question a Synexis sales rep would legitimately ask: DHP
  technology, products (Sphere, Sentry XL, Blade), deployment, efficacy,
  studies, competitors, verticals (healthcare, food, education, etc.),
  compliance, pricing (will be declined downstream but is still on-topic),
  and anything ABOUT Synexis the company — revenue, leadership, history,
  acquisitions, employee info. Those are on_topic even when we can't answer
  them; downstream rules route to Synexis support with a canonical phrase.

- "off_topic" — general-knowledge questions, trivia, coding help, unrelated
  product questions, anything not tied to Synexis, DHP, or infection/microbial
  control. "Write me a Python function" is off_topic. "What's Synexis's
  revenue?" is NOT — that's on_topic (company-level question).

- "medical_advice" — requests for individualized patient treatment guidance,
  clinical recommendations, or diagnosis. Note: questions ABOUT DHP safety or
  efficacy in medical settings are on_topic; requests for clinical decisions
  FOR a specific patient are medical_advice.

- "pii" — query contains a real person's name + contact info, credit card,
  social security / government ID, or similar personal data that shouldn't be
  in logs. Corporate names (e.g. "Cleveland Clinic") are not PII. Role titles
  are not PII.

Respond with ONLY a single JSON object on one line, no prose, no code fences:

  {"category":"on_topic","reject_reason":null}
  {"category":"off_topic","reject_reason":"off_topic"}
  {"category":"medical_advice","reject_reason":"medical_advice"}
  {"category":"pii","reject_reason":"pii"}

If unsure, default to "on_topic"."""


@dataclass
class ValidationResult:
    on_topic: bool
    reject_reason: Optional[str]  # None when on_topic; else one of REJECT_REASONS
    elapsed_ms: int


_NORMALIZE_RE = re.compile(r"\s+")


def _normalize(q: str) -> str:
    return _NORMALIZE_RE.sub(" ", (q or "").strip().lower())


def _hash_query(q: str) -> str:
    return hashlib.sha1(_normalize(q).encode("utf-8")).hexdigest()[:16]


class _RateAbuseTracker:
    """In-memory sliding-window duplicate-query counter.

    Keyed on the caller (session_id or partner_key). For each key we keep the
    most recent timestamps of each query hash; if a hash has >= THRESHOLD
    entries within WINDOW seconds, we flag rate_abuse.
    """

    def __init__(self,
                 threshold: int = RATE_ABUSE_THRESHOLD,
                 window_sec: int = RATE_ABUSE_WINDOW_SECONDS) -> None:
        self.threshold = threshold
        self.window = window_sec
        self._buckets: Dict[Tuple[str, str], Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check_and_record(self, caller_key: str, query: str) -> bool:
        """Return True if this query counts as rate abuse."""
        now = time.time()
        qh = _hash_query(query)
        key = (caller_key, qh)
        with self._lock:
            dq = self._buckets[key]
            cutoff = now - self.window
            while dq and dq[0] < cutoff:
                dq.popleft()
            dq.append(now)
            return len(dq) >= self.threshold


_default_rate_tracker = _RateAbuseTracker()


class InputValidator:
    def __init__(self) -> None:
        self._client = None

    def _ensure_client(self) -> None:
        if self._client is None:
            import anthropic
            if not ANTHROPIC_API_KEY:
                raise RuntimeError("ANTHROPIC_API_KEY is not set in the environment.")
            self._client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=4))
    def _call(self, query: str) -> str:
        self._ensure_client()
        resp = self._client.messages.create(
            model=VALIDATOR_MODEL,
            max_tokens=VALIDATOR_MAX_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": VALIDATOR_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": query[:2000]}],
        )
        parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        return "".join(parts).strip()

    def validate(
        self,
        query: str,
        caller_key: Optional[str] = None,
    ) -> ValidationResult:
        started = time.time()

        # 1. Rate-abuse check up front — cheap, no network. If a caller sends
        #    the same query repeatedly in a short window, flag without burning
        #    a Haiku call.
        if caller_key and _default_rate_tracker.check_and_record(caller_key, query):
            return ValidationResult(
                on_topic=False,
                reject_reason="rate_abuse",
                elapsed_ms=int((time.time() - started) * 1000),
            )

        # 2. Haiku content classification.
        try:
            raw = self._call(query)
        except Exception:
            # Fail open — a validator hiccup shouldn't block a legit query.
            return ValidationResult(
                on_topic=True,
                reject_reason=None,
                elapsed_ms=int((time.time() - started) * 1000),
            )

        payload = _extract_json(raw)
        if not isinstance(payload, dict):
            return ValidationResult(on_topic=True, reject_reason=None,
                                    elapsed_ms=int((time.time() - started) * 1000))
        category = str(payload.get("category") or "").strip().lower()
        reason = payload.get("reject_reason")
        if category == "on_topic":
            return ValidationResult(on_topic=True, reject_reason=None,
                                    elapsed_ms=int((time.time() - started) * 1000))
        if isinstance(reason, str) and reason in REJECT_REASONS:
            return ValidationResult(on_topic=False, reject_reason=reason,
                                    elapsed_ms=int((time.time() - started) * 1000))
        if category in REJECT_REASONS:
            return ValidationResult(on_topic=False, reject_reason=category,
                                    elapsed_ms=int((time.time() - started) * 1000))
        # Unrecognized classifier output — fail open.
        return ValidationResult(on_topic=True, reject_reason=None,
                                elapsed_ms=int((time.time() - started) * 1000))


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(raw: str) -> Optional[dict]:
    """Parse the first JSON object from the model's output; tolerate stray prose."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        pass
    m = _JSON_RE.search(raw)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


_default_validator: Optional[InputValidator] = None


def get_validator() -> InputValidator:
    global _default_validator
    if _default_validator is None:
        _default_validator = InputValidator()
    return _default_validator


def canned_response(reject_reason: str) -> str:
    return CANNED_RESPONSES.get(reject_reason, CANNED_RESPONSES["off_topic"])
