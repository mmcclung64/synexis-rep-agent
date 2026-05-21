"""One-shot web ingestion for synexis.com.

Reads all URLs from logs/synexis_web_state.json, fetches each page,
chunks with the standard pipeline, embeds with Voyage, and upserts to Pinecone.

Tier 1  (source_category "Website - Core")           → intake_mode "web-ingest-approved"
Tier 2/3 (testimonials, press releases, blog, etc.)  → intake_mode "web-ingest-pending"

Two extra metadata fields are added to every Pinecone vector:
  governance_status : "approved" | "pending-review"
  source_url        : the canonical page URL (enables future gap-check in bootstrap)

Usage (from repo root):
    python3 -m pipeline.ingest_web --fetch-only          # phase 1: fetch + chunk → work/web_chunks.jsonl
    python3 -m pipeline.ingest_web --embed-only --confirm # phase 2: embed + upsert from cache
    python3 -m pipeline.ingest_web --confirm             # full run (both phases)
    python3 -m pipeline.ingest_web --url-list            # just print classified URL list and exit

Fetch uses a thread pool (--workers, default 8) for speed.

Output:
    work/web_chunks.jsonl              chunk cache from phase 1
    logs/web_ingest_YYYY-MM-DD.jsonl   per-URL ingest log
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from tqdm import tqdm

from pipeline.chunk import chunk_doc, EFFICACY_RE
from pipeline.embed_load import (
    _embed_batch,
    _upsert_batch,
    _build_token_batches,
    _voyage_client,
    _pinecone_index,
    EMBED_INTER_CALL_SECONDS,
    MAX_METADATA_TEXT_CHARS,
)

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = REPO_ROOT / "logs" / "synexis_web_state.json"
LOG_DIR = REPO_ROOT / "logs"
WORK_DIR = REPO_ROOT / "work"
WEB_CHUNKS_PATH = WORK_DIR / "web_chunks.jsonl"
LOG_DIR.mkdir(parents=True, exist_ok=True)
WORK_DIR.mkdir(parents=True, exist_ok=True)

REQUEST_TIMEOUT = 20
MIN_CHARS = 200          # skip pages with less extractable text than this
DEFAULT_WORKERS = 8      # parallel fetch threads

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SynexisRepAgent-Ingest/1.0; "
        "+https://www.synexis.com)"
    )
}

# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------

# Tier 1 — core product/science content, approved for immediate ingest
TIER1_SLUGS = {
    "faqs",
    "pathogens",
    "continuous-pathogen-control",
    "continuous-pathogen-control-2",
    "part-1-what-is-dry-hydrogen-peroxide-dhp",
    "part-2-why-is-dhp-more-reactive-than-aqueous-hydrogen-peroxide",
    "part-3-how-do-we-produce-dhp-vs-wet-aqueous-hydrogen-peroxide",
    "part-4-how-do-we-measure-dhp",
}

# Skip these entirely — pure navigation / index / legal pages with no KB value
SKIP_SLUGS = {
    "",
    "privacy-policy",
    "purchase-terms-conditions",
    "terms-conditions",
    "resources",
    "webinar-archive",
    "instructional-videos",
    "about/awards",
    "welcome-to-the-new-synexis-blog",
}

# Keywords that identify press-release / deployment-announcement pages
PRESS_RELEASE_KWS = (
    "synexis-and-",
    "synexis-llc-",
    "synexis-names-",
    "synexis-introduces-",
    "synexis-sentry-xl-expands",
    "synexis-presenting-",
    "synexis-sponsored-",
    "synexis-dhp-featured-",
    "synexis-dhp-in-a-long-term",
    "synexis-technology-makes-naples",
    "new-data-shows-",
    "data-shows-synexis",
    "new-air-cleaning-system-",
    "new-air-sanitizers-",
    "birch-ridge-inn-",
    "city-of-des-peres-",
    "colorado-school-district-",
    "events-dc-implements-",
    "fla-live-arena-",
    "pinewood-atlanta-",
    "quip-labs-collaborates-",
    "stratus-strengthens-",
    "university-of-oklahoma-",
    "dry-hydrogen-peroxide-dhp-expands-",
    "san-felipe-del-rio-",
    "texas-school-district-",
    "kansas-school-district-",
    "office-furniture-firm-",
    "synexis-and-trane-",
    "synexis-and-diversey-",
)


def _slug(url: str) -> str:
    return (
        url.replace("https://synexis.com/", "")
        .replace("https://www.synexis.com/", "")
        .rstrip("/")
    )


def _classify(url: str) -> Optional[Tuple[str, str]]:
    """Return (source_category, governance_status) or None to skip this URL."""
    s = _slug(url)

    # Skip /industry/ — identical to /industries/, avoid duplicate vectors
    if s.startswith("industry/"):
        return None

    # Skip low-value nav / legal / index pages
    if s in SKIP_SLUGS or s.startswith("category/"):
        return None

    # Tier 1 — approved core content
    if s in TIER1_SLUGS or s.startswith("industries/"):
        return "Website - Core", "approved"

    # Product device pages
    if s.startswith("device/"):
        return "Website - Product Pages", "pending-review"

    # Client testimonials
    if s.startswith("client-success-stories") or s == "mwvs-customer-tcs":
        return "Website - Testimonials", "pending-review"

    # Press releases / deployment announcements
    if any(s.startswith(kw) or kw in s for kw in PRESS_RELEASE_KWS):
        return "Website - Press Release", "pending-review"

    # Everything else (blog articles, study landing pages, webinar pages, etc.)
    return "Website - Blog", "pending-review"


# ---------------------------------------------------------------------------
# HTML fetch + text extraction
# ---------------------------------------------------------------------------

_STRIP_TAGS = frozenset({
    "script", "style", "nav", "footer", "header",
    "noscript", "meta", "link", "svg", "form", "button", "iframe",
})


def _fetch_text(url: str) -> Optional[Tuple[str, str]]:
    """Fetch a page and return (title, body_text), or None on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except (requests.RequestException, UnicodeError, ValueError) as exc:
        print(f"  [WARN] {url} — {exc}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url

    for tag in soup.find_all(_STRIP_TAGS):
        tag.decompose()

    container = soup.find("main") or soup.find("article") or soup.find("body")
    if container is None:
        return None

    text = container.get_text(separator=" ", strip=True)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return title, text


# ---------------------------------------------------------------------------
# Chunk + embed helpers
# ---------------------------------------------------------------------------

def _doc_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def _build_web_vector(chunk: dict, embedding: List[float]) -> dict:
    """Build a Pinecone vector dict, adding governance_status and source_url."""
    text = chunk["text"]
    if len(text) > MAX_METADATA_TEXT_CHARS:
        text = text[:MAX_METADATA_TEXT_CHARS]
    metadata = {
        "source":               chunk["source"],
        "file_path":            chunk["file_path"],
        "source_url":           chunk["file_path"],   # explicit URL for gap-check
        "doc_id":               chunk["doc_id"],
        "chunk_index":          chunk["chunk_index"],
        "source_category":      chunk["source_category"],
        "intake_mode":          chunk["intake_mode"],
        "tier":                 4,
        "governance_status":    chunk.get("governance_status", "pending-review"),
        "page_or_slide":        chunk["page_or_slide"],
        "has_efficacy_claim":   chunk["has_efficacy_claim"],
        "extension":            chunk["extension"],
        "extractor_used":       chunk["extractor_used"],
        "token_count":          chunk["token_count"],
        "text":                 text,
    }
    return {"id": chunk["chunk_id"], "values": embedding, "metadata": metadata}


# ---------------------------------------------------------------------------
# Phase 1: parallel fetch + chunk → cache
# ---------------------------------------------------------------------------

def _fetch_and_chunk(url: str) -> Optional[Tuple[dict, List[dict]]]:
    """Fetch one URL, extract text, chunk. Returns (log_entry, chunks) or None."""
    classification = _classify(url)
    if classification is None:
        return {"url": url, "status": "skipped"}, []

    source_category, governance_status = classification
    result = _fetch_text(url)
    if result is None:
        return {"url": url, "status": "fetch_error"}, []

    title, text = result
    if len(text) < MIN_CHARS:
        return {"url": url, "status": "too_short", "chars": len(text)}, []

    doc_json = {
        "doc_id":          _doc_id(url),
        "file_path":       url,
        "source_category": source_category,
        "intake_mode":     "web-ingest-approved" if governance_status == "approved" else "web-ingest-pending",
        "extension":       "html",
        "extractor_used":  "beautifulsoup",
        "pages":           [{"number": 1, "text": f"{title}\n\n{text}"}],
    }
    chunks = chunk_doc(doc_json)
    enriched: List[dict] = []
    for c in chunks:
        d = asdict(c)
        d["governance_status"] = governance_status
        enriched.append(d)

    log_entry = {
        "url":               url,
        "status":            "chunked",
        "title":             title,
        "chars":             len(text),
        "chunks":            len(chunks),
        "source_category":   source_category,
        "governance_status": governance_status,
    }
    return log_entry, enriched


def fetch_phase(workers: int = DEFAULT_WORKERS) -> Tuple[List[dict], List[dict]]:
    """Fetch all URLs in parallel. Returns (log_entries, all_chunks)."""
    state = json.loads(STATE_PATH.read_text())
    urls = sorted(state.keys())

    print(f"[ingest_web] Fetching {len(urls)} URLs with {workers} workers …")
    log_entries: List[dict] = []
    all_chunks: List[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_and_chunk, url): url for url in urls}
        for future in tqdm(as_completed(futures), total=len(futures), desc="fetch"):
            try:
                log_entry, chunks = future.result()
                log_entries.append(log_entry)
                all_chunks.extend(chunks)
            except Exception as exc:
                url = futures[future]
                log_entries.append({"url": url, "status": "exception", "error": str(exc)})

    n_chunked = sum(1 for e in log_entries if e["status"] == "chunked")
    n_err = sum(1 for e in log_entries if e["status"] not in ("chunked", "skipped", "too_short"))
    print(f"[ingest_web] Fetched+chunked: {n_chunked}  Errors: {n_err}  Chunks: {len(all_chunks)}")

    # Write chunk cache
    WEB_CHUNKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with WEB_CHUNKS_PATH.open("w", encoding="utf-8") as f:
        for c in all_chunks:
            f.write(json.dumps(c) + "\n")
    print(f"[ingest_web] Chunk cache: {WEB_CHUNKS_PATH} ({len(all_chunks)} chunks)")

    return log_entries, all_chunks


# ---------------------------------------------------------------------------
# Phase 2: embed + upsert from cache
# ---------------------------------------------------------------------------

def embed_phase(all_chunks: Optional[List[dict]] = None, confirm: bool = False) -> int:
    """Embed chunks and upsert to Pinecone. Returns total vectors upserted."""
    VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
    PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
    if not VOYAGE_API_KEY:
        raise SystemExit("VOYAGE_API_KEY missing from .env")
    if confirm and not PINECONE_API_KEY:
        raise SystemExit("PINECONE_API_KEY missing from .env")

    if all_chunks is None:
        if not WEB_CHUNKS_PATH.exists():
            raise SystemExit(f"No chunk cache found at {WEB_CHUNKS_PATH}. Run --fetch-only first.")
        with WEB_CHUNKS_PATH.open(encoding="utf-8") as f:
            all_chunks = [json.loads(line) for line in f if line.strip()]

    if not all_chunks:
        print("[ingest_web] No chunks to embed.")
        return 0

    voyage = _voyage_client()
    index = _pinecone_index() if confirm else None

    batches = _build_token_batches(all_chunks)
    total_upserted = 0
    pending_vecs: List[tuple] = []
    last_embed_at = 0.0

    print(f"[ingest_web] Embedding {len(all_chunks)} chunks in {len(batches)} batches "
          f"(confirm={confirm}) …")
    for batch in tqdm(batches, desc="embed"):
        texts = [c["text"] for c in batch]
        elapsed = time.time() - last_embed_at
        if last_embed_at and elapsed < EMBED_INTER_CALL_SECONDS:
            time.sleep(EMBED_INTER_CALL_SECONDS - elapsed)
        embeddings = _embed_batch(voyage, texts)
        last_embed_at = time.time()
        for c, emb in zip(batch, embeddings):
            pending_vecs.append((c, emb))

        while len(pending_vecs) >= 100:
            take, pending_vecs = pending_vecs[:100], pending_vecs[100:]
            vectors = [_build_web_vector(c, e) for c, e in take]
            if confirm:
                _upsert_batch(index, vectors)
            total_upserted += len(vectors)

    if pending_vecs:
        vectors = [_build_web_vector(c, e) for c, e in pending_vecs]
        if confirm:
            _upsert_batch(index, vectors)
        total_upserted += len(vectors)

    return total_upserted


# ---------------------------------------------------------------------------
# Orchestrated run
# ---------------------------------------------------------------------------

def run(confirm: bool = False, workers: int = DEFAULT_WORKERS) -> None:
    log_entries, all_chunks = fetch_phase(workers=workers)
    total_upserted = embed_phase(all_chunks=all_chunks, confirm=confirm)

    ingest_log_path = LOG_DIR / f"web_ingest_{time.strftime('%Y-%m-%d')}.jsonl"
    with ingest_log_path.open("w", encoding="utf-8") as f:
        for e in log_entries:
            f.write(json.dumps(e) + "\n")

    n_fetched = sum(1 for e in log_entries if e["status"] == "chunked")
    n_skipped = sum(1 for e in log_entries if e["status"] == "skipped")
    summary = {
        "urls_fetched":     n_fetched,
        "urls_skipped":     n_skipped,
        "chunks_built":     len(all_chunks),
        "vectors_upserted": total_upserted if confirm else 0,
        "dry_run":          not confirm,
        "log":              str(ingest_log_path),
    }
    print("\n" + json.dumps(summary, indent=2))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def list_urls() -> None:
    state = json.loads(STATE_PATH.read_text())
    rows = []
    for url in sorted(state.keys()):
        result = _classify(url)
        if result is None:
            rows.append(f"SKIP    {url}")
        else:
            cat, gov = result
            rows.append(f"{gov.upper():<15} {cat:<28} {url}")
    for r in rows:
        print(r)
    approved = sum(1 for u in state if _classify(u) and _classify(u)[1] == "approved")
    pending  = sum(1 for u in state if _classify(u) and _classify(u)[1] == "pending-review")
    skipped  = sum(1 for u in state if _classify(u) is None)
    print(f"\nApproved: {approved}  Pending: {pending}  Skipped: {skipped}")


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Ingest synexis.com web pages into Pinecone.")
    ap.add_argument("--confirm", action="store_true",
                    help="Actually upsert to Pinecone (default: dry run).")
    ap.add_argument("--fetch-only", action="store_true",
                    help="Phase 1 only: fetch + chunk → work/web_chunks.jsonl. No embedding.")
    ap.add_argument("--embed-only", action="store_true",
                    help="Phase 2 only: embed + upsert from work/web_chunks.jsonl cache.")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"Parallel fetch threads (default: {DEFAULT_WORKERS}).")
    ap.add_argument("--url-list", action="store_true",
                    help="Print classified URL list and exit.")
    args = ap.parse_args(argv)

    if args.url_list:
        list_urls()
        return 0
    if args.fetch_only:
        fetch_phase(workers=args.workers)
        return 0
    if args.embed_only:
        total = embed_phase(confirm=args.confirm)
        print(f"Upserted {total} vectors (confirm={args.confirm})")
        return 0
    run(confirm=args.confirm, workers=args.workers)
    return 0


if __name__ == "__main__":
    sys.exit(main())
