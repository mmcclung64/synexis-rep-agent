"""Feed 1 — synexis.com change monitor.

Two modes:

  Bootstrap (--bootstrap):
    Crawls the entire synexis.com site, compares discovered pages against the
    Pinecone corpus by source URL, and drops any unrepresented pages to staging
    for governance review. Builds the initial state store so Day 1 monitoring
    has a clean baseline. Run once before enabling daily monitoring.

  Monitor (default / called by orchestrator):
    Crawls the full URL inventory, hashes each page's text, and diffs against
    the state store. Changed pages go through a Haiku semantic filter. If Haiku
    says the change is substantive (product claims, specs, regulatory content),
    two outputs are triggered:
      A) Staging drop — extracted text + metadata JSON in
         pipeline/monitoring/staging/synexis_web/YYYY-MM-DD/
      B) Email notification to NOTIFY_EMAIL summarising what changed.
    Non-substantive changes update the hash silently. Pages with no change log
    a heartbeat entry.

Usage:
    python3 -m pipeline.monitoring.feed_synexis_web              # monitor dry run
    python3 -m pipeline.monitoring.feed_synexis_web --confirm    # monitor full run
    python3 -m pipeline.monitoring.feed_synexis_web --bootstrap  # bootstrap dry run
    python3 -m pipeline.monitoring.feed_synexis_web --bootstrap --confirm  # bootstrap full run

Environment:
    ANTHROPIC_API_KEY  — required (Haiku semantic filter)
    PINECONE_API_KEY   — required for bootstrap corpus comparison
    PINECONE_INDEX_NAME — defaults to "sra"
    NOTIFY_EMAIL       — recipient for change notifications (default: mmcclung@synexis.com)
    SMTP_HOST/PORT/USER/PASSWORD — required to actually send email (see utils.py)
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import anthropic
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from pipeline.monitoring.utils import append_monitoring_log, send_email

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MONITORING_DIR = REPO_ROOT / "pipeline" / "monitoring"
STAGING_ROOT = MONITORING_DIR / "staging" / "synexis_web"
STATE_PATH = REPO_ROOT / "logs" / "synexis_web_state.json"
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.synexis.com"
SITEMAP_URL = "https://www.synexis.com/sitemap.xml"
CRAWL_DELAY_SECONDS = 1.5          # be polite
REQUEST_TIMEOUT = 20
MIN_CONTENT_CHARS = 100            # ignore pages with almost no extractable text

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "sra")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "mmcclung@synexis.com")

# Haiku classifier prompt
HAIKU_SYSTEM = (
    "You are a content change classifier for a B2B sales AI system. "
    "Your job is to decide whether a website content change is substantive — "
    "meaning it affects product claims, technical specifications, application guidance, "
    "pricing, regulatory content, or efficacy data. "
    "Navigation tweaks, footer updates, typo fixes, formatting changes, and marketing "
    "copy rephrasings are NOT substantive. "
    "Reply with a JSON object and nothing else: "
    '{"substantive": true/false, "rationale": "one sentence"}'
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SynexisRepAgent-Monitor/1.0; "
        "+https://www.synexis.com)"
    )
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(url: str) -> Optional[requests.Response]:
    """Fetch a URL, returning None on error."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except (requests.RequestException, UnicodeError, ValueError) as exc:
        # UnicodeError: IDNA label-too-long on redirect target hostnames.
        # ValueError: malformed URL components from stray redirects.
        print(f"  [fetch] WARN: {url} — {exc}")
        return None


# ---------------------------------------------------------------------------
# Sitemap + spider
# ---------------------------------------------------------------------------

def _parse_sitemap(sitemap_url: str) -> List[str]:
    """Return all <loc> URLs from a sitemap (handles sitemap index files)."""
    resp = _get(sitemap_url)
    if resp is None:
        return []
    soup = BeautifulSoup(resp.text, "xml")
    # Sitemap index — recurse into child sitemaps
    child_maps = soup.find_all("sitemap")
    if child_maps:
        urls: List[str] = []
        for sm in child_maps:
            loc = sm.find("loc")
            if loc:
                urls.extend(_parse_sitemap(loc.text.strip()))
        return urls
    # Regular sitemap
    return [loc.text.strip() for loc in soup.find_all("loc")]


def _spider(start_url: str, max_pages: int = 500) -> List[str]:
    """Simple BFS spider scoped to the same domain as start_url."""
    visited: set[str] = set()
    queue = [start_url]
    found: List[str] = []

    while queue and len(found) < max_pages:
        url = queue.pop(0)
        url = url.rstrip("/")
        if url in visited:
            continue
        visited.add(url)

        resp = _get(url)
        if resp is None:
            continue
        ct = resp.headers.get("content-type", "")
        if "text/html" not in ct:
            continue

        found.append(url)
        time.sleep(CRAWL_DELAY_SECONDS)

        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            abs_url = urljoin(url, href).split("#")[0].rstrip("/")
            parsed = urlparse(abs_url)
            if _same_domain(abs_url) and parsed.scheme in ("http", "https"):
                if abs_url not in visited and abs_url not in queue:
                    queue.append(abs_url)

    return found


def _same_domain(url: str) -> bool:
    """Return True if url belongs to synexis.com (with or without www)."""
    netloc = urlparse(url).netloc.lower().lstrip("www.")
    base_netloc = urlparse(BASE_URL).netloc.lower().lstrip("www.")
    return netloc == base_netloc


def _discover_urls() -> List[str]:
    """Return deduplicated URL list from sitemap (primary) + spider fallback."""
    print("[discover] Fetching sitemap …")
    sitemap_urls = _parse_sitemap(SITEMAP_URL)
    if sitemap_urls:
        print(f"[discover] Sitemap returned {len(sitemap_urls)} URLs.")
        urls = [
            u for u in sitemap_urls
            if _same_domain(u)
            and not u.endswith((".xml", ".pdf", ".zip", ".jpg", ".png", ".svg"))
        ]
        print(f"[discover] {len(urls)} HTML-like URLs after filtering.")
        return list(dict.fromkeys(urls))  # deduplicate preserving order
    else:
        print("[discover] Sitemap empty or unreachable — falling back to spider.")
        urls = _spider(BASE_URL)
        print(f"[discover] Spider found {len(urls)} URLs.")
        return urls


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------

_STRIP_TAGS = {"script", "style", "nav", "footer", "header", "noscript", "meta",
               "link", "svg", "form", "button", "iframe"}


def _extract_text(html: str, url: str) -> Tuple[str, str]:
    """Extract visible body text and page title from HTML.

    Returns (title, text).
    """
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url

    for tag in soup.find_all(_STRIP_TAGS):
        tag.decompose()

    # Prefer <main> or <article> if present; fall back to <body>
    container = soup.find("main") or soup.find("article") or soup.find("body")
    if container is None:
        return title, ""

    text = container.get_text(separator=" ", strip=True)
    # Collapse whitespace runs
    text = re.sub(r"\s{2,}", " ", text).strip()
    return title, text


def _slug(url: str) -> str:
    """Convert a URL to a safe filename slug."""
    parsed = urlparse(url)
    path = parsed.netloc + parsed.path
    slug = re.sub(r"[^\w\-]", "_", path).strip("_")
    slug = re.sub(r"_+", "_", slug)
    return slug[:120]  # cap length


# ---------------------------------------------------------------------------
# State store
# ---------------------------------------------------------------------------

def _load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        with STATE_PATH.open(encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def _save_state(state: Dict[str, Any]) -> None:
    with STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Haiku semantic filter
# ---------------------------------------------------------------------------

def _is_substantive_change(old_text: str, new_text: str) -> Tuple[bool, str]:
    """Ask Haiku whether this diff represents a substantive content change.

    Returns (substantive: bool, rationale: str).
    Falls back to True (conservative) if the API call fails.
    """
    if not ANTHROPIC_API_KEY:
        print("  [haiku] ANTHROPIC_API_KEY not set — defaulting to substantive=True")
        return True, "API key not configured; defaulting conservative."

    # Build a compact diff summary for the prompt
    old_words = set(old_text.split())
    new_words = set(new_text.split())
    added = " ".join(list(new_words - old_words)[:80])
    removed = " ".join(list(old_words - new_words)[:80])
    diff_summary = f"ADDED: {added}\nREMOVED: {removed}"

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=128,
            system=HAIKU_SYSTEM,
            messages=[{"role": "user", "content": diff_summary}],
        )
        raw = msg.content[0].text.strip()
        # Extract JSON even if Haiku adds surrounding prose
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            return bool(parsed.get("substantive", True)), parsed.get("rationale", "")
        return True, f"Could not parse Haiku response: {raw[:120]}"
    except Exception as exc:  # noqa: BLE001
        print(f"  [haiku] API error: {exc} — defaulting substantive=True")
        return True, f"Haiku error: {exc}"


# ---------------------------------------------------------------------------
# Staging drop
# ---------------------------------------------------------------------------

def _drop_to_staging(url: str, title: str, text: str, rationale: str, dry_run: bool) -> Path:
    """Write extracted text + metadata to today's staging folder."""
    today = _dt.date.today().isoformat()
    staging_dir = STAGING_ROOT / today
    slug = _slug(url)

    if dry_run:
        print(f"  [staging] DRY RUN — would write {staging_dir}/{slug}.{{txt,meta.json}}")
        return staging_dir / f"{slug}.txt"

    staging_dir.mkdir(parents=True, exist_ok=True)
    txt_path = staging_dir / f"{slug}.txt"
    meta_path = staging_dir / f"{slug}.meta.json"

    txt_path.write_text(text, encoding="utf-8")
    meta = {
        "url": url,
        "title": title,
        "change_summary": rationale,
        "detected_at": _dt.datetime.utcnow().isoformat() + "Z",
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"  [staging] Dropped: {txt_path.name}")
    return txt_path


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def _run_bootstrap(confirm: bool, dry_run: bool, gap_check: bool = False) -> Dict[str, Any]:
    """Full-site crawl, initial state store build, and optional corpus gap check.

    gap_check is False by default because the existing corpus is ingested from
    local files and carries no source_url metadata — every page would show as a
    gap, which produces noise rather than signal. Enable it with --gap-check once
    the ingestion pipeline tags chunks with source_url.
    """
    print("\n=== BOOTSTRAP MODE ===")
    if not gap_check:
        print("[bootstrap] Gap check disabled (pass --gap-check to enable once corpus is URL-keyed).")
    urls = _discover_urls()
    print(f"[bootstrap] {len(urls)} pages to process.")

    # Load Pinecone URL inventory (only if gap check requested)
    corpus_urls: set[str] = set()
    if gap_check:
        if PINECONE_API_KEY:
            try:
                from pinecone import Pinecone
                pc = Pinecone(api_key=PINECONE_API_KEY)
                index = pc.Index(PINECONE_INDEX_NAME)
                result = index.query(
                    vector=[0.0] * 1024,
                    top_k=10000,
                    include_metadata=True,
                )
                for match in result.get("matches", []):
                    su = match.get("metadata", {}).get("source_url", "")
                    if su:
                        corpus_urls.add(su.rstrip("/"))
                print(f"[bootstrap] {len(corpus_urls)} source URLs found in Pinecone corpus.")
                if not corpus_urls:
                    print(
                        "[bootstrap] WARN: No source_url metadata in corpus — gap check will "
                        "flag every page. Consider skipping --gap-check until the ingestion "
                        "pipeline is updated to tag chunks with source_url."
                    )
            except Exception as exc:  # noqa: BLE001
                print(f"[bootstrap] WARN: Could not query Pinecone — {exc}. Gap check skipped.")
        else:
            print("[bootstrap] WARN: PINECONE_API_KEY not set — gap check skipped.")

    state: Dict[str, Any] = _load_state()
    gaps: List[str] = []
    now_iso = _dt.datetime.utcnow().isoformat() + "Z"

    for url in urls:
        print(f"  [bootstrap] Fetching: {url}")
        resp = _get(url)
        if resp is None:
            continue
        title, text = _extract_text(resp.text, url)
        if len(text) < MIN_CONTENT_CHARS:
            print(f"  [bootstrap] Skipping (too little text): {url}")
            continue

        h = _content_hash(text)
        state[url] = {
            "hash": h,
            "last_checked": now_iso,
            "last_changed": now_iso,
            "title": title,
        }

        # Gap check — only when corpus is URL-keyed and gap_check is enabled
        if gap_check and corpus_urls:
            url_norm = url.rstrip("/")
            if url_norm not in corpus_urls:
                gaps.append(url)
                print(f"  [bootstrap] GAP — not in corpus: {url}")
                _drop_to_staging(url, title, text, "Bootstrap gap — page not yet in corpus.", dry_run)

        time.sleep(CRAWL_DELAY_SECONDS)

    if not dry_run:
        _save_state(state)
        print(f"[bootstrap] State store written: {STATE_PATH}")
    else:
        print(f"[bootstrap] DRY RUN — state store not written.")

    result = {
        "mode": "bootstrap",
        "pages_crawled": len(urls),
        "corpus_gaps": len(gaps) if gap_check else "skipped",
        "gap_urls": gaps,
    }
    append_monitoring_log({"event": "feed_synexis_web_bootstrap", **result})
    gap_msg = f"{len(gaps)} corpus gaps found" if gap_check else "gap check skipped"
    print(f"\n[bootstrap] Done. {len(urls)} pages crawled, {gap_msg}.")
    return result


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

def _run_monitor(confirm: bool, dry_run: bool) -> Dict[str, Any]:
    """Daily monitor pass — diff, filter, drop, notify."""
    print("\n=== MONITOR MODE ===")
    urls = _discover_urls()
    print(f"[monitor] {len(urls)} pages to check.")

    state = _load_state()
    if not state:
        print(
            "[monitor] WARN: State store is empty. Run --bootstrap first to build a baseline.\n"
            "          Continuing — all pages will be treated as new (no diff available)."
        )

    now_iso = _dt.datetime.utcnow().isoformat() + "Z"
    changed_substantive: List[Dict[str, str]] = []
    changed_noise = 0
    unchanged = 0
    new_pages = 0
    errors = 0

    for url in urls:
        resp = _get(url)
        if resp is None:
            errors += 1
            continue
        title, text = _extract_text(resp.text, url)
        if len(text) < MIN_CONTENT_CHARS:
            continue

        h = _content_hash(text)
        prior = state.get(url)

        if prior is None:
            # New page discovered since last run
            new_pages += 1
            print(f"  [monitor] NEW PAGE: {url}")
            substantive, rationale = True, "New page — not in prior state store."
            state[url] = {"hash": h, "last_checked": now_iso, "last_changed": now_iso, "title": title}
            _drop_to_staging(url, title, text, rationale, dry_run)
            changed_substantive.append({"url": url, "title": title, "rationale": rationale})

        elif prior["hash"] != h:
            # Content changed — run Haiku filter
            old_text = prior.get("_cached_text", "")  # not stored; diff from word sets
            print(f"  [monitor] CHANGED — running Haiku filter: {url}")
            substantive, rationale = _is_substantive_change(old_text, text)
            state[url]["hash"] = h
            state[url]["last_checked"] = now_iso
            state[url]["title"] = title

            if substantive:
                state[url]["last_changed"] = now_iso
                print(f"  [monitor] SUBSTANTIVE — {rationale}")
                _drop_to_staging(url, title, text, rationale, dry_run)
                changed_substantive.append({"url": url, "title": title, "rationale": rationale})
            else:
                changed_noise += 1
                print(f"  [monitor] noise — {rationale}")

        else:
            # No change
            unchanged += 1
            state[url]["last_checked"] = now_iso
            append_monitoring_log({
                "event": "feed_synexis_web_heartbeat",
                "url": url,
                "status": "no_change",
            })

        time.sleep(CRAWL_DELAY_SECONDS)

    # Persist updated state
    if not dry_run:
        _save_state(state)

    # Email notification
    if changed_substantive:
        _send_change_notification(changed_substantive, dry_run)

    result = {
        "mode": "monitor",
        "pages_checked": len(urls),
        "unchanged": unchanged,
        "new_pages": new_pages,
        "changed_substantive": len(changed_substantive),
        "changed_noise": changed_noise,
        "errors": errors,
    }
    append_monitoring_log({"event": "feed_synexis_web_monitor_run", **result})
    print(
        f"\n[monitor] Done. {unchanged} unchanged, {len(changed_substantive)} substantive changes, "
        f"{changed_noise} noise, {new_pages} new pages, {errors} errors."
    )
    return result


def _send_change_notification(changes: List[Dict[str, str]], dry_run: bool) -> None:
    today = _dt.date.today().isoformat()
    n = len(changes)
    subject = f"Synexis.com changes detected — {n} page{'s' if n != 1 else ''} [{today}]"

    lines = [
        f"The following {n} page{'s' if n != 1 else ''} had substantive content changes:",
        "",
    ]
    for c in changes:
        lines.append(f"  • {c['title']}")
        lines.append(f"    {c['url']}")
        lines.append(f"    {c['rationale']}")
        lines.append("")

    lines += [
        "Changed content has been dropped to staging for governance review:",
        f"  pipeline/monitoring/staging/synexis_web/{today}/",
        "",
        "Review and approve before corpus ingest.",
    ]
    body = "\n".join(lines)
    send_email(subject, body, to=NOTIFY_EMAIL, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Public entry point (called by orchestrator)
# ---------------------------------------------------------------------------

def run(confirm: bool = False, dry_run: bool = True) -> Dict[str, Any]:
    """Entry point for the orchestrator. Returns a result summary dict."""
    return _run_monitor(confirm=confirm, dry_run=dry_run)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="synexis.com change monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build state store baseline (writes nothing without --confirm):
  python3 -m pipeline.monitoring.feed_synexis_web --bootstrap
  python3 -m pipeline.monitoring.feed_synexis_web --bootstrap --confirm

  # Daily monitor dry run (requires state store from bootstrap):
  python3 -m pipeline.monitoring.feed_synexis_web

  # Daily monitor full run:
  python3 -m pipeline.monitoring.feed_synexis_web --confirm

  # Bootstrap + corpus gap check (only useful once corpus chunks carry source_url metadata):
  python3 -m pipeline.monitoring.feed_synexis_web --bootstrap --gap-check --confirm
""",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Crawl full site and build initial state store. Run once before daily monitoring.",
    )
    parser.add_argument(
        "--gap-check",
        action="store_true",
        dest="gap_check",
        help=(
            "During bootstrap: compare discovered pages against Pinecone corpus and stage gaps. "
            "Only useful once the ingestion pipeline tags corpus chunks with source_url metadata."
        ),
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Execute full run with writes and notifications (default: dry run).",
    )
    args = parser.parse_args()
    dry_run = not args.confirm

    if args.bootstrap:
        _run_bootstrap(confirm=args.confirm, dry_run=dry_run, gap_check=args.gap_check)
    else:
        if args.gap_check:
            parser.error("--gap-check only applies to --bootstrap mode.")
        _run_monitor(confirm=args.confirm, dry_run=dry_run)


if __name__ == "__main__":
    main()
