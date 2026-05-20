"""Vertical intro generation and caching.

Generates short, corpus-grounded intro sentences for each industry vertical.
These are shown instantly in the extension while the LLM generates the full answer.

Cache file: logs/intros_cache.json
TTL: 24 hours (configurable via INTROS_TTL_HOURS env var)

Refresh triggers:
  - GET /intros called when cache is stale
  - POST /intros/refresh (explicit — called by --delta pipeline after a sync)

Generation: Haiku retrieves top corpus chunks per vertical, then writes a
2-3 sentence intro ending with a colon. Runs ~5 Haiku calls in sequence.
Takes ~10-15 seconds total; callers always get stale-while-revalidate behaviour.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential


load_dotenv()

log = logging.getLogger(__name__)

ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")
INTROS_MODEL       = os.getenv("INTROS_MODEL", "claude-haiku-4-5")
INTROS_TTL_HOURS   = float(os.getenv("INTROS_TTL_HOURS", "24"))
INTROS_RETRIEVE_N  = int(os.getenv("INTROS_RETRIEVE_N", "6"))

REPO_ROOT   = Path(__file__).resolve().parent.parent
CACHE_FILE  = REPO_ROOT / "logs" / "intros_cache.json"

# ---------------------------------------------------------------------------
# Vertical retrieval queries + display labels
# ---------------------------------------------------------------------------

VERTICALS: dict[str, dict] = {
    "Healthcare": {
        "query": "Synexis DHP technology healthcare hospital infection control patient rooms HAI",
        "label": "healthcare",
    },
    "Animal Health": {
        "query": "Synexis DHP technology animal health veterinary clinic livestock kennel",
        "label": "animal health",
    },
    "Food Safety": {
        "query": "Synexis DHP technology food safety food processing plant pathogen control HACCP",
        "label": "food safety",
    },
    "Higher Education": {
        "query": "Synexis DHP technology higher education campus dorms dining halls classrooms IAQ",
        "label": "higher education",
    },
    "": {
        "query": "Synexis DHP technology overview benefits applications all industries",
        "label": "your industry",
    },
}

# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

_SYSTEM = (
    "You are a concise sales enablement writer for Synexis, a company that makes "
    "DHP® (Dry Hydrogen Peroxide) technology for continuous, touchless pathogen control. "
    "You write short, accurate intro sentences for sales reps. "
    "Never mention pricing. Always use the registered trademark DHP®. "
    "Never name competitor products or brands."
)

_USER_TMPL = """\
Using ONLY the knowledge-base excerpts below, write a 2-3 sentence intro paragraph \
for a Synexis sales rep speaking with a prospect in the {label} vertical.

Requirements:
- Start with "Synexis DHP® technology"
- Be specific to challenges and environments common in {label}
- End the final sentence with a colon followed by a bridging phrase such as \
"Here's the full picture for {label}:" or "Here's how it helps {label}:"
- 2-3 sentences maximum — no headers, no bullets, plain prose only

Knowledge-base context:
{context}

Write ONLY the intro paragraph. Nothing else."""


def _format_context(hits) -> str:
    lines = []
    for i, h in enumerate(hits, 1):
        md = h.metadata or {}
        lines.append(f"[{i}] {md.get('file_path', '')} p{md.get('page_or_slide', '')}")
        lines.append((h.text or "")[:400])
        lines.append("")
    return "\n".join(lines).strip()


class _IntroGenerator:
    def __init__(self) -> None:
        self._client = None
        self._lock = threading.Lock()

    def _ensure_client(self) -> None:
        if self._client is None:
            import anthropic
            if not ANTHROPIC_API_KEY:
                raise RuntimeError("ANTHROPIC_API_KEY is not set.")
            self._client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _call(self, prompt: str) -> str:
        self._ensure_client()
        resp = self._client.messages.create(
            model=INTROS_MODEL,
            max_tokens=220,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        return "".join(parts).strip()

    def generate_all(self) -> dict[str, str]:
        """Retrieve + generate intros for every vertical. Returns {key: intro_text}."""
        from api.retrieval import get_retriever

        retriever = get_retriever()
        intros: dict[str, str] = {}

        for key, cfg in VERTICALS.items():
            label = cfg["label"]
            try:
                hits = retriever.retrieve(cfg["query"], top_n=INTROS_RETRIEVE_N)
                if not hits:
                    log.warning("intros: no hits for vertical %r — skipping", key or "generic")
                    continue
                context = _format_context(hits)
                prompt = _USER_TMPL.format(label=label, context=context)
                text = self._call(prompt)
                intros[key] = text
                log.info("intros: generated for %r (%d chars)", key or "generic", len(text))
            except Exception as exc:
                log.warning("intros: failed for vertical %r: %s", key or "generic", exc)

        return intros


_generator = _IntroGenerator()

# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------

def _load_cache() -> Optional[dict]:
    try:
        if CACHE_FILE.exists():
            return json.loads(CACHE_FILE.read_text())
    except Exception as exc:
        log.warning("intros: cache read failed: %s", exc)
    return None


def _save_cache(intros: dict[str, str]) -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {"generated_at": time.time(), "intros": intros}
        CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        log.info("intros: cache saved (%d verticals)", len(intros))
    except Exception as exc:
        log.warning("intros: cache write failed: %s", exc)


def _is_stale(cache: dict) -> bool:
    generated_at = cache.get("generated_at", 0)
    return (time.time() - generated_at) > (INTROS_TTL_HOURS * 3600)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_refresh_lock = threading.Lock()
_refresh_in_progress = False


def get_intros() -> dict:
    """Return cached intros. If cache is missing or stale, regenerate synchronously.

    Returns a dict with keys: ``intros`` (map), ``generated_at`` (epoch float),
    ``stale`` (bool — True if cache was stale and regeneration was triggered).
    """
    cache = _load_cache()

    if cache is None or _is_stale(cache):
        # Synchronous generation — only used when the cache is completely absent.
        # In normal operation the cache always exists and refresh is async.
        try:
            fresh = _generator.generate_all()
            if fresh:
                _save_cache(fresh)
                return {"intros": fresh, "generated_at": time.time(), "stale": False}
        except Exception as exc:
            log.error("intros: synchronous generation failed: %s", exc)
        # If generation fails and we have stale cache, return it anyway.
        if cache:
            return {**cache, "stale": True}
        # Last resort: empty dict — extension falls back to hardcoded VERTICAL_INTROS.
        return {"intros": {}, "generated_at": 0, "stale": True}

    return {**cache, "stale": False}


def refresh_intros_background() -> None:
    """Kick off a background thread to regenerate intros.

    Safe to call from a FastAPI background task or the CLI.
    No-ops if a refresh is already running.
    """
    global _refresh_in_progress

    with _refresh_lock:
        if _refresh_in_progress:
            log.info("intros: refresh already in progress — skipping")
            return
        _refresh_in_progress = True

    def _run():
        global _refresh_in_progress
        try:
            log.info("intros: background refresh started")
            fresh = _generator.generate_all()
            if fresh:
                _save_cache(fresh)
                log.info("intros: background refresh complete")
            else:
                log.warning("intros: background refresh produced no output")
        except Exception as exc:
            log.error("intros: background refresh failed: %s", exc)
        finally:
            with _refresh_lock:
                _refresh_in_progress = False

    t = threading.Thread(target=_run, daemon=True, name="intros-refresh")
    t.start()
