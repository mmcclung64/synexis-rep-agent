"""Feed 2 — Outbreak intelligence monitor.

Polls two outbreak sources and produces three outputs per qualifying item:

Sources:
  - CDC Food Safety RSS (primary; US foodborne outbreaks + recalls).
    (Replaces the briefing's ProMED RSS — ProMED deprecated public RSS in 2024.)
  - FDA outbreak investigations page (secondary; structured, food/healthcare).

Per-item pipeline:
  1. Fetch new items since last run (state store: logs/outbreaks_state.json).
  2. Haiku extracts structured fields: pathogen, affected_vertical, geography,
     severity, summary, source_url.
  3. Relevance filter: skip if vertical not in Synexis markets, or non-US
     geography (unless severity == "outbreak" — cross-border relevance).

Outputs per qualifying item:

  A. HubSpot tasks (only if HUBSPOT_ACCESS_TOKEN is set):
     - Pass 1: named company in item → search HubSpot by name.
     - Pass 2: no named company → search by state + industry.
     - For each match: fetch contacts + owner, create task under the owner.

  B. Corpus drop:
     Markdown file under source_content/Outbreak Intelligence/
     (YYYY-MM-DD_{pathogen}_{state}.md) with status pending-governance.
     Governance owner approves before ingest.

  C. Marketing digest email:
     One email per run aggregating all qualifying items, sent to MARKETING_EMAIL.
     If no new items: no email.

Usage:
    python3 -m pipeline.monitoring.feed_outbreaks            # dry run
    python3 -m pipeline.monitoring.feed_outbreaks --confirm  # full run

Environment:
    ANTHROPIC_API_KEY       — required (Haiku extraction)
    HUBSPOT_ACCESS_TOKEN    — optional; HubSpot output skipped if absent
    MARKETING_EMAIL         — recipient for digest (no digest sent if absent)
    NOTIFY_EMAIL            — fallback for operator alerts
    SMTP_HOST/PORT/USER/PASSWORD — required for digest delivery
    SOURCE_CONTENT_PATH     — base path for corpus drops
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

import anthropic
import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from pipeline.monitoring.utils import append_monitoring_log, send_email

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
STATE_PATH = REPO_ROOT / "logs" / "outbreaks_state.json"
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Where governance-pending corpus drops live. Falls back to a repo-local path
# if SOURCE_CONTENT_PATH is not set.
_default_source_content = REPO_ROOT.parent / "source_content"
SOURCE_CONTENT_PATH = Path(
    os.path.expanduser(os.getenv("SOURCE_CONTENT_PATH") or str(_default_source_content))
)
OUTBREAK_CORPUS_DIR = SOURCE_CONTENT_PATH / "Outbreak Intelligence"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# CDC Food Safety feed (active, ~20 current recalls/outbreaks).
# ProMED's public RSS at promedmail.org/feed/ returns 404 since their 2024
# platform migration — kept out of config to avoid silent retries.
CDC_FOOD_SAFETY_RSS_URL = "https://tools.cdc.gov/api/v2/resources/media/316422.rss"
FDA_INVESTIGATIONS_URL = (
    "https://www.fda.gov/food/outbreaks-foodborne-illness/"
    "investigations-foodborne-illness-outbreaks"
)
REQUEST_TIMEOUT = 20
HAIKU_MODEL = os.getenv("ANTHROPIC_VALIDATOR_MODEL", "claude-haiku-4-5")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN", "").strip()
MARKETING_EMAIL = os.getenv("MARKETING_EMAIL", "").strip()
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "mmcclung@synexis.com")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SynexisRepAgent-OutbreakMonitor/1.0; "
        "+https://www.synexis.com)"
    )
}

# Verticals Synexis sells into. Haiku-extracted vertical is matched against this
# set (case-insensitive substring). Anything else is filtered out.
RELEVANT_VERTICALS = {
    "healthcare",
    "food processing",
    "food production",
    "food safety",
    "poultry",
    "animal health",
    "education",
    "government",
    "hospitality",
}

US_STATES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
    "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
    "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia",
    "Washington", "West Virginia", "Wisconsin", "Wyoming",
}

HAIKU_EXTRACTION_PROMPT = """\
You extract structured outbreak intelligence from news items for a B2B sales \
team selling DHP (dry hydrogen peroxide) air/surface pathogen control systems.

Given the item below, return a single JSON object with these fields:

  "pathogen": string — the pathogen or disease name, as commonly written \
(e.g. "Salmonella", "E. coli O157:H7", "Candida auris", "norovirus"). \
Empty string if none is identifiable.

  "affected_vertical": string — one of: "healthcare", "food processing", \
"food production", "poultry", "animal health", "education", "government", \
"hospitality", "residential", or "other". Pick the best single fit.

  "geography": array of strings — US states named or strongly implied \
(full name, e.g. ["Texas", "Oklahoma"]). Empty array if non-US or unspecified.

  "named_company": string — any specific company, facility, or brand named \
as affected (e.g. "Boar's Head", "Cleveland Clinic"). Empty string if none.

  "severity": string — one of: "outbreak", "investigation", "advisory". \
Use "outbreak" for confirmed illnesses, "investigation" for ongoing, \
"advisory" for warnings or recalls.

  "summary": string — 2–3 plain-English sentences. Lead with what happened, \
then scope (cases, geography). No speculation beyond the source text.

  "source_url": string — the URL of the source item (pass through unchanged).

Return ONLY the JSON object. No prose, no code fences."""


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
def _load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        with STATE_PATH.open(encoding="utf-8") as fh:
            return json.load(fh)
    return {"seen_ids": {}, "last_run": None}


def _save_state(state: Dict[str, Any]) -> None:
    with STATE_PATH.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


def _item_id(source: str, key: str) -> str:
    return f"{source}:{hashlib.sha1(key.encode('utf-8')).hexdigest()[:16]}"


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def _get(url: str) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp
    except (requests.RequestException, UnicodeError, ValueError) as exc:
        print(f"  [fetch] WARN: {url} — {exc}")
        return None


# ---------------------------------------------------------------------------
# CDC Food Safety RSS
# ---------------------------------------------------------------------------
def _fetch_cdc_food_safety() -> List[Dict[str, Any]]:
    """Return list of CDC Food Safety RSS items as dicts."""
    print(f"[cdc]   Fetching {CDC_FOOD_SAFETY_RSS_URL} …")
    parsed = feedparser.parse(CDC_FOOD_SAFETY_RSS_URL)
    if parsed.bozo:
        print(f"[cdc]   WARN: feed parse issue — {parsed.bozo_exception}")
    items: List[Dict[str, Any]] = []
    for entry in parsed.entries:
        guid = entry.get("id") or entry.get("guid") or entry.get("link", "")
        items.append({
            "source": "cdc",
            "id": _item_id("cdc", guid),
            "title": entry.get("title", "").strip(),
            "summary": BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(" ").strip(),
            "link": entry.get("link", "").strip(),
            "published": entry.get("published", ""),
        })
    print(f"[cdc]   {len(items)} entries in feed.")
    return items


# ---------------------------------------------------------------------------
# FDA outbreak investigations
# ---------------------------------------------------------------------------
def _fetch_fda() -> List[Dict[str, Any]]:
    """Scrape the FDA investigations table into item dicts."""
    print(f"[fda]   Fetching {FDA_INVESTIGATIONS_URL} …")
    resp = _get(FDA_INVESTIGATIONS_URL)
    if resp is None:
        return []
    soup = BeautifulSoup(resp.text, "lxml")

    # FDA page uses a main table; each row = an investigation. We grab rows with a
    # data-href or a <a> pointing to an investigation page. The page layout has
    # shifted historically — keep the selector permissive.
    items: List[Dict[str, Any]] = []
    for row in soup.select("table tbody tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        if not cells or len(cells) < 2:
            continue
        link_tag = row.find("a", href=True)
        link = link_tag["href"] if link_tag else ""
        if link and not link.startswith("http"):
            link = "https://www.fda.gov" + link
        title = " — ".join(c for c in cells if c)
        if not title:
            continue
        items.append({
            "source": "fda",
            "id": _item_id("fda", link or title),
            "title": title,
            "summary": title,
            "link": link or FDA_INVESTIGATIONS_URL,
            "published": cells[0] if cells else "",
        })
    print(f"[fda]   {len(items)} rows parsed.")
    return items


# ---------------------------------------------------------------------------
# Haiku extraction
# ---------------------------------------------------------------------------
_anthropic_client: Optional[anthropic.Anthropic] = None


def _anthropic() -> anthropic.Anthropic:
    global _anthropic_client
    if _anthropic_client is None:
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY is required for outbreak extraction.")
        _anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _anthropic_client


def _extract_structured(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Call Haiku to extract structured outbreak fields. Returns None on failure."""
    user_block = (
        f"Source: {item['source']}\n"
        f"Title: {item['title']}\n"
        f"Link: {item['link']}\n"
        f"Body:\n{item['summary'][:4000]}"
    )
    try:
        resp = _anthropic().messages.create(
            model=HAIKU_MODEL,
            max_tokens=500,
            system=[{"type": "text", "text": HAIKU_EXTRACTION_PROMPT,
                     "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_block}],
        )
        raw = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text").strip()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if not m:
                print(f"  [haiku] Could not parse extraction: {raw[:120]}")
                return None
            payload = json.loads(m.group())
        # Pass-through source_url from original if Haiku dropped it
        payload.setdefault("source_url", item["link"])
        return payload
    except Exception as exc:  # noqa: BLE001
        print(f"  [haiku] Extraction error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Relevance
# ---------------------------------------------------------------------------
def _is_relevant(ext: Dict[str, Any]) -> Tuple[bool, str]:
    """Return (keep, reason)."""
    vertical = (ext.get("affected_vertical") or "").strip().lower()
    if not any(v in vertical for v in RELEVANT_VERTICALS):
        return False, f"vertical '{vertical}' not in Synexis markets"

    geo = ext.get("geography") or []
    us_states = [g for g in geo if g in US_STATES]
    severity = (ext.get("severity") or "").lower()
    if not us_states and severity != "outbreak":
        return False, "non-US geography and severity below outbreak threshold"
    return True, "relevant"


# ---------------------------------------------------------------------------
# Output A — HubSpot
# ---------------------------------------------------------------------------
_HUBSPOT_BASE = "https://api.hubapi.com"


def _hs_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _hs_search_companies_by_name(name: str) -> List[Dict[str, Any]]:
    if not HUBSPOT_ACCESS_TOKEN or not name:
        return []
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "name", "operator": "CONTAINS_TOKEN", "value": name}
        ]}],
        "properties": ["name", "state", "industry", "hubspot_owner_id"],
        "limit": 5,
    }
    try:
        r = requests.post(f"{_HUBSPOT_BASE}/crm/v3/objects/companies/search",
                          headers=_hs_headers(), json=body, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("results", [])
    except requests.RequestException as exc:
        print(f"  [hubspot] company name search failed: {exc}")
        return []


def _hs_search_companies_by_state_industry(states: List[str], vertical: str) -> List[Dict[str, Any]]:
    if not HUBSPOT_ACCESS_TOKEN or not states:
        return []
    # HubSpot filterGroups are OR'd; within a group, filters are AND'd. We want
    # (state IN states) AND (industry ~ vertical). HubSpot lacks a native IN, so
    # build one OR-group per state.
    groups = []
    for state in states:
        groups.append({"filters": [
            {"propertyName": "state", "operator": "EQ", "value": state},
            {"propertyName": "industry", "operator": "CONTAINS_TOKEN", "value": vertical},
        ]})
    body = {
        "filterGroups": groups,
        "properties": ["name", "state", "industry", "hubspot_owner_id"],
        "limit": 10,
    }
    try:
        r = requests.post(f"{_HUBSPOT_BASE}/crm/v3/objects/companies/search",
                          headers=_hs_headers(), json=body, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("results", [])
    except requests.RequestException as exc:
        print(f"  [hubspot] state/industry search failed: {exc}")
        return []


def _hs_create_task(subject: str, body: str, owner_id: Optional[str],
                    company_id: str, dry_run: bool) -> bool:
    if dry_run:
        print(f"  [hubspot] DRY RUN — would create task for company {company_id} "
              f"(owner={owner_id}): {subject}")
        return True
    if not HUBSPOT_ACCESS_TOKEN:
        return False
    ts = int(time.time() * 1000) + 24 * 3600 * 1000  # due tomorrow
    payload = {
        "properties": {
            "hs_task_subject": subject,
            "hs_task_body": body,
            "hs_task_status": "NOT_STARTED",
            "hs_task_priority": "HIGH",
            "hs_timestamp": ts,
            **({"hubspot_owner_id": owner_id} if owner_id else {}),
        },
        "associations": [{
            "to": {"id": company_id},
            "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 192}],
        }],
    }
    try:
        r = requests.post(f"{_HUBSPOT_BASE}/crm/v3/objects/tasks",
                          headers=_hs_headers(), json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        print(f"  [hubspot] Task created for company {company_id}: {subject}")
        return True
    except requests.RequestException as exc:
        print(f"  [hubspot] Task create failed for company {company_id}: {exc}")
        return False


def _dispatch_hubspot(ext: Dict[str, Any], dry_run: bool) -> List[str]:
    """Create HubSpot tasks for matching companies. Returns list of company IDs reached."""
    if not HUBSPOT_ACCESS_TOKEN:
        print("  [hubspot] HUBSPOT_ACCESS_TOKEN not set — skipping HubSpot output.")
        return []

    named = (ext.get("named_company") or "").strip()
    companies: List[Dict[str, Any]] = []
    if named:
        companies = _hs_search_companies_by_name(named)
        if companies:
            print(f"  [hubspot] Named company '{named}' → {len(companies)} HubSpot matches.")
    if not companies:
        states = [s for s in (ext.get("geography") or []) if s in US_STATES]
        vertical = ext.get("affected_vertical", "")
        companies = _hs_search_companies_by_state_industry(states, vertical)
        if companies:
            print(f"  [hubspot] Geo/industry fallback → {len(companies)} HubSpot matches.")

    reached: List[str] = []
    subject = (
        f"[Outbreak Alert] {ext.get('pathogen') or 'Unspecified pathogen'} — "
        f"{', '.join(ext.get('geography') or ['US'])}"
    )
    body = (
        f"Pathogen: {ext.get('pathogen') or 'N/A'}\n"
        f"Affected area: {', '.join(ext.get('geography') or []) or 'Unspecified'}\n"
        f"Vertical: {ext.get('affected_vertical') or 'N/A'}\n"
        f"Severity: {ext.get('severity') or 'N/A'}\n\n"
        f"{ext.get('summary') or ''}\n\n"
        f"Talking point: DHP has demonstrated efficacy against multiple airborne "
        f"and surface pathogens — a timely reason to reach out.\n\n"
        f"Source: {ext.get('source_url') or ''}"
    )
    for c in companies:
        cid = c.get("id")
        owner_id = (c.get("properties") or {}).get("hubspot_owner_id")
        if _hs_create_task(subject, body, owner_id, cid, dry_run):
            reached.append(cid)
    return reached


# ---------------------------------------------------------------------------
# Output B — Corpus drop
# ---------------------------------------------------------------------------
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slug(s: str) -> str:
    return _SLUG_RE.sub("-", (s or "item").lower()).strip("-") or "item"


def _drop_corpus_markdown(ext: Dict[str, Any], dry_run: bool) -> Optional[Path]:
    date = _dt.date.today().isoformat()
    pathogen_slug = _slug(ext.get("pathogen") or "unknown")
    geo = ext.get("geography") or []
    state_slug = _slug(geo[0]) if geo else "us"
    filename = f"{date}_{pathogen_slug}_{state_slug}.md"
    target = OUTBREAK_CORPUS_DIR / filename

    md = (
        f"---\n"
        f"status: pending-governance\n"
        f"source: {ext.get('source_url') or ''}\n"
        f"pathogen: {ext.get('pathogen') or ''}\n"
        f"affected_vertical: {ext.get('affected_vertical') or ''}\n"
        f"geography: {', '.join(geo) if geo else ''}\n"
        f"severity: {ext.get('severity') or ''}\n"
        f"named_company: {ext.get('named_company') or ''}\n"
        f"detected_at: {_dt.datetime.utcnow().isoformat()}Z\n"
        f"---\n\n"
        f"# {ext.get('pathogen') or 'Outbreak'} — {', '.join(geo) or 'US'}\n\n"
        f"{ext.get('summary') or ''}\n\n"
        f"**Source:** {ext.get('source_url') or ''}\n"
    )

    if dry_run:
        print(f"  [corpus] DRY RUN — would write {target}")
        return target
    OUTBREAK_CORPUS_DIR.mkdir(parents=True, exist_ok=True)
    target.write_text(md, encoding="utf-8")
    print(f"  [corpus] Dropped: {target.relative_to(SOURCE_CONTENT_PATH)}")
    return target


# ---------------------------------------------------------------------------
# Output C — Marketing digest
# ---------------------------------------------------------------------------
def _send_digest(items: List[Dict[str, Any]], dry_run: bool) -> bool:
    if not items:
        return False
    recipient = MARKETING_EMAIL or NOTIFY_EMAIL
    today = _dt.date.today().isoformat()
    subject = f"Outbreak Intelligence Digest — {today}"

    lines = [
        f"Synexis Rep Agent — Outbreak Intelligence Digest",
        f"Date: {today}",
        f"Items: {len(items)}",
        "",
    ]
    for i, ext in enumerate(items, 1):
        geo = ", ".join(ext.get("geography") or []) or "Unspecified"
        lines += [
            f"{i}. {ext.get('pathogen') or 'Unspecified pathogen'} — {geo}",
            f"   Vertical: {ext.get('affected_vertical') or 'N/A'}",
            f"   Severity: {ext.get('severity') or 'N/A'}",
            f"   {ext.get('summary') or ''}",
            f"   Campaign angle: DHP efficacy against {ext.get('pathogen') or 'airborne/surface pathogens'} "
            f"— outreach relevance for {ext.get('affected_vertical') or 'affected vertical'} accounts.",
            f"   Source: {ext.get('source_url') or ''}",
            "",
        ]
    body = "\n".join(lines)
    return send_email(subject, body, to=recipient, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------
def run(confirm: bool = False, dry_run: bool = True) -> Dict[str, Any]:
    """Orchestrator entry point — always monitor mode, never bootstrap."""
    return _run_feed(confirm=confirm, dry_run=dry_run, bootstrap=False)


def _run_bootstrap(confirm: bool, dry_run: bool) -> Dict[str, Any]:
    """Bootstrap: mark every current item as seen without running the pipeline.

    Prevents the cold-start flood — FDA's investigations page carries years of
    historical outbreaks which would otherwise all fire through Haiku, HubSpot,
    corpus drop, and the marketing digest on the first real run.
    """
    print("\n=== OUTBREAK FEED — BOOTSTRAP ===")
    raw_items = _fetch_cdc_food_safety() + _fetch_fda()
    state = _load_state()
    seen: Dict[str, Any] = state.get("seen_ids", {})
    now = _dt.datetime.utcnow().isoformat() + "Z"
    for it in raw_items:
        seen[it["id"]] = {"title": it["title"], "ts": now, "bootstrapped": True}
    state["seen_ids"] = seen
    state["last_run"] = now
    if dry_run:
        print(f"[bootstrap] DRY RUN — would mark {len(raw_items)} items seen, state not persisted.")
    else:
        _save_state(state)
        print(f"[bootstrap] Marked {len(raw_items)} items seen. State: {STATE_PATH}")
    result = {"mode": "outbreaks_bootstrap", "items_seeded": len(raw_items)}
    append_monitoring_log({"event": "feed_outbreaks_bootstrap", **result})
    return result


def _run_feed(confirm: bool, dry_run: bool, bootstrap: bool = False) -> Dict[str, Any]:
    if bootstrap:
        return _run_bootstrap(confirm=confirm, dry_run=dry_run)
    print("\n=== OUTBREAK FEED ===")
    if dry_run:
        print("[feed_outbreaks] DRY RUN — no writes, no API calls beyond Haiku.")
    if not HUBSPOT_ACCESS_TOKEN:
        print("[feed_outbreaks] HUBSPOT_ACCESS_TOKEN not set — HubSpot tasks disabled.")
    if not MARKETING_EMAIL:
        print(f"[feed_outbreaks] MARKETING_EMAIL not set — digest (if any) will go to {NOTIFY_EMAIL}.")

    state = _load_state()
    seen: Dict[str, Any] = state.get("seen_ids", {})

    raw_items = _fetch_cdc_food_safety() + _fetch_fda()
    new_items = [it for it in raw_items if it["id"] not in seen]
    print(f"[feed_outbreaks] {len(raw_items)} items fetched, {len(new_items)} new since last run.")

    qualifying: List[Dict[str, Any]] = []
    filtered = 0
    hubspot_tasks = 0

    for it in new_items:
        print(f"\n  [item] {it['source']}: {it['title'][:90]}")
        ext = _extract_structured(it)
        if not ext:
            continue
        keep, reason = _is_relevant(ext)
        if not keep:
            filtered += 1
            print(f"  [filter] Skipped — {reason}")
            seen[it["id"]] = {"title": it["title"], "skipped": reason,
                              "ts": _dt.datetime.utcnow().isoformat() + "Z"}
            continue

        # Three outputs
        reached = _dispatch_hubspot(ext, dry_run)
        hubspot_tasks += len(reached)
        _drop_corpus_markdown(ext, dry_run)
        qualifying.append(ext)

        seen[it["id"]] = {
            "title": it["title"],
            "pathogen": ext.get("pathogen"),
            "geography": ext.get("geography"),
            "hubspot_companies": reached,
            "ts": _dt.datetime.utcnow().isoformat() + "Z",
        }

    # Output C — single digest for the whole run
    digest_sent = _send_digest(qualifying, dry_run=dry_run)

    state["seen_ids"] = seen
    state["last_run"] = _dt.datetime.utcnow().isoformat() + "Z"
    if not dry_run:
        _save_state(state)
        print(f"\n[feed_outbreaks] State updated: {STATE_PATH}")
    else:
        print(f"\n[feed_outbreaks] DRY RUN — state not persisted.")

    result = {
        "mode": "outbreaks",
        "items_fetched": len(raw_items),
        "items_new": len(new_items),
        "items_qualifying": len(qualifying),
        "items_filtered_out": filtered,
        "hubspot_tasks_created": hubspot_tasks,
        "digest_sent": digest_sent,
    }
    append_monitoring_log({"event": "feed_outbreaks_run", **result})
    print(f"\n[feed_outbreaks] Done — {result}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Outbreak intelligence monitor (CDC Food Safety + FDA)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Seed state store without firing outputs (run once before first --confirm):
  python3 -m pipeline.monitoring.feed_outbreaks --bootstrap --confirm

  # Dry run — parse, extract, preview outputs, no writes:
  python3 -m pipeline.monitoring.feed_outbreaks

  # Full run — creates HubSpot tasks, writes corpus drops, sends digest:
  python3 -m pipeline.monitoring.feed_outbreaks --confirm
""",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help=(
            "Seed the state store with every currently-listed item so only "
            "future additions fire outputs. Run once on first install."
        ),
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Execute full run with writes and notifications (default: dry run).",
    )
    args = parser.parse_args()
    _run_feed(confirm=args.confirm, dry_run=not args.confirm, bootstrap=args.bootstrap)


if __name__ == "__main__":
    main()
