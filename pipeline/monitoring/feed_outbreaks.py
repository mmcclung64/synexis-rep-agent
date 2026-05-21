"""Feed 2 — Outbreak intelligence monitor.

Polls five outbreak sources and produces three outputs per qualifying item:

Sources:
  - CDC Food Safety RSS (primary; US foodborne outbreaks + recalls).
    (Replaces the briefing's ProMED RSS — ProMED deprecated public RSS in 2024.)
  - FDA outbreak investigations page (secondary; structured, food/healthcare).
  - WHO Disease Outbreak News RSS (global outbreaks; cross-border relevance filter).
  - USDA FSIS recalls RSS (meat/poultry/egg recalls; direct food-processing relevance).
  - Google News via Serper (pathogen keyword + VOC/chemical incident queries;
    catches local news faster than federal feeds; covers NRC/EPA incident signals).

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
    SERPER_API_KEY          — optional; Serper/Google News queries skipped if absent
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
# Rolling log of every HubSpot task created — read by the weekly brief generator.
HUBSPOT_TASKS_LOG = REPO_ROOT / "logs" / "hubspot_tasks_log.jsonl"
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
WHO_DON_RSS_URL = "https://www.who.int/feeds/entity/csr/don/en/rss.xml"  # 404 as of May 2026; kept for fallback
FSIS_RECALLS_API_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"  # official JSON API (replaces 403 RSS)
SERPER_NEWS_URL = "https://google.serper.dev/news"

# Serper: keyword queries run each cycle.
# Pathogen queries target the highest-signal Tier 1 clusters.
# VOC queries cover chemical/indoor air incidents (NRC/EPA signal proxy).
SERPER_PATHOGEN_QUERIES = [
    "Salmonella OR Listeria OR \"E. coli\" outbreak 2026",
    "MRSA OR norovirus OR Legionella outbreak hospital 2026",
    "avian influenza H5N1 outbreak 2026",
]
SERPER_VOC_QUERIES = [
    '"VOC contamination" building OR facility OR workplace',
    '"indoor air quality" outbreak OR incident OR evacuation',
    '"chemical contamination" school OR hospital OR facility',
]

REQUEST_TIMEOUT = 20
HAIKU_MODEL = os.getenv("ANTHROPIC_VALIDATOR_MODEL", "claude-haiku-4-5")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN", "").strip()
MARKETING_EMAIL = os.getenv("MARKETING_EMAIL", "").strip()
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "mmcclung@synexis.com")
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "").strip()
# Alpha mode: when set, all HubSpot tasks are assigned to this owner ID
# regardless of territory routing. Unset to enable full team routing.
ALPHA_OWNER_ID = os.getenv("ALPHA_OWNER_ID", "").strip() or None

# Comma-separated list of digest recipients. Falls back to MARKETING_EMAIL / NOTIFY_EMAIL
# if not set. Set DIGEST_RECIPIENTS in .env to override.
_digest_recipients_raw = os.getenv("DIGEST_RECIPIENTS", "").strip()
DIGEST_RECIPIENTS: list[str] = (
    [r.strip() for r in _digest_recipients_raw.split(",") if r.strip()]
    if _digest_recipients_raw
    else ([MARKETING_EMAIL] if MARKETING_EMAIL else [NOTIFY_EMAIL])
)

# Directory where weekly brief PDFs are written by generate_brief_pdf.py.
# Defaults to ~/Desktop/Claude/outbreak-watcher/ — override via BRIEF_OUTPUT_DIR in .env.
BRIEF_OUTPUT_DIR = Path(
    os.path.expanduser(
        os.getenv("BRIEF_OUTPUT_DIR", "~/Desktop/Claude/outbreak-watcher")
    )
)

# Vertical → default owner mapping.
# Used when a matched company has no hubspot_owner_id set.
# Healthcare is territory-based — company records already carry the correct owner
# from the DHC import, so it is intentionally omitted here.
VERTICAL_OWNER_MAP: Dict[str, str] = {
    "education":       "88106519",   # Larry Shapiro — Higher Ed
    "animal health":   "82257890",   # Denise Bucari — Animal Health
    "food processing": "82067944",   # Tyler Mattson — Food Safety
    "food production": "82067944",   # Tyler Mattson — Food Safety
    "food safety":     "82067944",   # Tyler Mattson — Food Safety
    "poultry":         "162416134",  # Federico Sanchez — Poultry
}

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
    # VOC/chemical incidents can affect any commercial facility
    "commercial",
    "residential",
    "workplace",
    "industrial",
}

# ---------------------------------------------------------------------------
# Pathogen tier classification
# ---------------------------------------------------------------------------
# Tier 1 — DHP efficacy confirmed: use standard campaign angle.
# Tier 2 — Possible relevance, efficacy not yet established: soften language.
# If pathogen is empty (allergen recall, mislabeling, etc.) item is filtered out.

TIER_1_PATHOGENS = {
    "salmonella",
    "listeria",
    "e. coli", "e.coli", "escherichia coli",
    "mrsa", "methicillin-resistant staphylococcus",
    "candida auris", "c. auris",
    "vre", "vancomycin-resistant enterococcus",
    "norovirus",
    "influenza", "avian influenza", "h5n1", "bird flu",
    "candida",
    "mold", "aspergillus",
    "rsv", "respiratory syncytial virus",
    "covid-19", "covid", "sars-cov-2", "coronavirus",
    "staphylococcus", "staph aureus",
    "streptococcus",
    "clostridium difficile", "c. diff", "cdiff",
    "legionella",
    "campylobacter",
    "hepatitis a",
    "cryptosporidium",
}

TIER_2_PATHOGENS = {
    "cereulide",
    "bacillus cereus",
    "hantavirus",
    # VOC / chemical — DHP® has demonstrated VOC reduction in controlled environments
    "voc", "volatile organic compound", "chemical contamination",
    "indoor air quality", "chemical spill", "toxic chemical",
}

# Allergens DHP® cannot address — filter these out entirely.
# Covers the US "Big 9" and common mislabeling recall triggers.
ALLERGEN_BLOCKLIST = {
    "peanut", "tree nut", "almond", "cashew", "walnut", "pecan", "pistachio",
    "hazelnut", "brazil nut",
    "milk", "dairy", "lactose",
    "egg",
    "wheat", "gluten",
    "soy", "soybean",
    "fish", "salmon", "tuna", "pollock", "cod", "halibut", "tilapia",
    "shellfish", "shrimp", "crab", "lobster", "clam", "oyster", "scallop",
    "sesame",
    "mustard", "celery", "lupin", "molluscs",
    "sulfite", "sulphite",
    "undeclared allergen", "allergen",
}


def _pathogen_tier(pathogen: str) -> Optional[int]:
    """Return 1, 2, or None.

    1 — confirmed DHP efficacy
    2 — possible / efficacy not yet established (novel or edge-case pathogen)
    None — no pathogen, or allergen DHP® cannot address → filter out
    """
    if not pathogen or not pathogen.strip():
        return None
    p = pathogen.strip().lower()
    # Tier 1 and Tier 2 take priority — check pathogens before allergens
    # (avoids "salmon" in ALLERGEN_BLOCKLIST matching "Salmonella")
    if any(t in p for t in TIER_1_PATHOGENS):
        return 1
    if any(t in p for t in TIER_2_PATHOGENS):
        return 2
    # Allergen recalls — DHP® has no efficacy here, drop entirely
    if any(a in p for a in ALLERGEN_BLOCKLIST):
        return None
    # Named pathogen not on either list — treat as Tier 2 catch-all
    return 2

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

  "pathogen": string — the pathogen, disease, or hazard name, as commonly \
written (e.g. "Salmonella", "E. coli O157:H7", "Candida auris", "norovirus"). \
For chemical/air-quality incidents use "VOC", "chemical contamination", or the \
specific chemical name. Empty string only if no pathogen or hazard is identifiable.

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
def _fetch_fda_detail(url: str) -> str:
    """Fetch the full body text of an FDA advisory page.

    FDA table rows contain only sparse cell text — serotype names like
    "Salmonella Newport" get misread as geography, and no state is extracted.
    Fetching the advisory page gives Haiku the full context it needs.
    Returns empty string on failure (caller falls back to table-row summary).
    """
    if not url or url == FDA_INVESTIGATIONS_URL:
        return ""
    resp = _get(url)
    if resp is None:
        return ""
    soup = BeautifulSoup(resp.text, "lxml")
    # FDA advisory pages wrap main content in .content-body, <article>, or <main>
    main = (
        soup.find("div", class_="content-body")
        or soup.find("article")
        or soup.find("main")
    )
    text = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)
    return text[:6000]  # cap at 6k chars — well within Haiku's context window


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
# WHO Disease Outbreak News RSS
# ---------------------------------------------------------------------------
def _fetch_who_don() -> List[Dict[str, Any]]:
    """Return WHO Disease Outbreak News items.

    WHO's RSS (https://www.who.int/feeds/entity/csr/don/en/rss.xml) returned
    404 as of May 2026. Primary path: targeted Serper query for WHO DON pages.
    Falls back to the RSS attempt if Serper is not configured.
    """
    # Primary: Serper search targeting who.int DON item pages directly
    if SERPER_API_KEY:
        print("[who]   Fetching WHO DONs via Serper …")
        results = _serper_query('site:who.int "disease-outbreak-news/item" 2026', num=10)
        items: List[Dict[str, Any]] = []
        for r in results:
            link = r.get("link", "")
            if not link or "disease-outbreak-news/item" not in link:
                continue
            items.append({
                "source": "who",
                "id": _item_id("who", link),
                "title": r.get("title", "").strip(),
                "summary": r.get("snippet", "").strip(),
                "link": link,
                "published": r.get("date", ""),
            })
        print(f"[who]   {len(items)} entries via Serper.")
        return items

    # Fallback: try the RSS (likely dead but worth one attempt)
    print(f"[who]   Fetching {WHO_DON_RSS_URL} (fallback — may be 404) …")
    parsed = feedparser.parse(WHO_DON_RSS_URL)
    entries = parsed.entries

    if parsed.bozo and not entries:
        resp = _get(WHO_DON_RSS_URL)
        if resp:
            soup = BeautifulSoup(resp.content, "lxml-xml")
            entries_raw = soup.find_all("item")
            items = []
            for e in entries_raw:
                link = (e.find("link") or e.find("guid") or e)
                link_text = (link.get_text(" ") if link else "").strip()
                title = (e.find("title") or e).get_text(" ").strip()
                desc = (e.find("description") or e).get_text(" ").strip()
                guid = link_text or title
                items.append({
                    "source": "who",
                    "id": _item_id("who", guid),
                    "title": title,
                    "summary": desc,
                    "link": link_text,
                    "published": (e.find("pubDate") or e).get_text(" ").strip(),
                })
            print(f"[who]   {len(items)} entries via fallback parser.")
            return items
        print(f"[who]   WARN: feed unavailable and Serper not configured.")
        return []

    items = []
    for entry in entries:
        guid = entry.get("id") or entry.get("link", "")
        items.append({
            "source": "who",
            "id": _item_id("who", guid),
            "title": entry.get("title", "").strip(),
            "summary": BeautifulSoup(
                entry.get("summary", ""), "html.parser"
            ).get_text(" ").strip(),
            "link": entry.get("link", "").strip(),
            "published": entry.get("published", ""),
        })
    print(f"[who]   {len(items)} entries in feed.")
    return items


# ---------------------------------------------------------------------------
# USDA FSIS recalls RSS
# ---------------------------------------------------------------------------
def _fetch_fsis() -> List[Dict[str, Any]]:
    """Return USDA FSIS recall items from the official JSON API.

    FSIS launched a Recall and Public Health Alert API in Sept 2023.
    The legacy RSS (https://www.fsis.usda.gov/rss/recalls.xml) returns 403
    as of May 2026 — replaced by the JSON API endpoint.
    API docs: https://www.fsis.usda.gov/science-data/developer-resources/recall-api
    """
    print(f"[fsis]  Fetching {FSIS_RECALLS_API_URL} …")
    try:
        r = requests.get(
            FSIS_RECALLS_API_URL,
            params={"pageSize": 50},
            headers={**HEADERS, "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as exc:
        print(f"[fsis]  WARN: API fetch failed — {exc}")
        return []
    except (ValueError, KeyError) as exc:
        print(f"[fsis]  WARN: API response parse failed — {exc}")
        return []

    # API may return a list directly or wrapped under a key
    records = data if isinstance(data, list) else data.get("results", data.get("data", []))
    items: List[Dict[str, Any]] = []
    for rec in records:
        product = rec.get("productDescription") or rec.get("name") or ""
        reason = rec.get("reasonForRecall") or ""
        recall_num = rec.get("recallNumber") or rec.get("id") or ""
        title_parts = [p for p in [recall_num, product, reason] if p]
        title = " — ".join(title_parts) or "FSIS Recall"
        raw_link = rec.get("url") or rec.get("link") or ""
        link = (
            raw_link if raw_link.startswith("http")
            else f"https://www.fsis.usda.gov{raw_link}" if raw_link
            else FSIS_RECALLS_API_URL
        )
        guid = recall_num or link or title
        summary = " | ".join([
            f"Product: {product}",
            f"Reason: {reason}",
            f"Distribution: {rec.get('distributionList') or ''}",
        ])
        items.append({
            "source": "fsis",
            "id": _item_id("fsis", str(guid)),
            "title": title,
            "summary": summary,
            "link": link,
            "published": rec.get("recallInitiationDate") or rec.get("publicHealthAlertDate") or "",
        })
    print(f"[fsis]  {len(items)} records from API.")
    return items


# ---------------------------------------------------------------------------
# Serper (Google News) — pathogen + VOC queries
# ---------------------------------------------------------------------------
def _serper_query(query: str, num: int = 10) -> List[Dict[str, Any]]:
    """Run a single Serper news query. Returns raw result list."""
    if not SERPER_API_KEY:
        return []
    try:
        r = requests.post(
            SERPER_NEWS_URL,
            headers={
                "X-API-KEY": SERPER_API_KEY,
                "Content-Type": "application/json",
            },
            json={"q": query, "num": num},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("news", [])
    except requests.RequestException as exc:
        print(f"  [serper] WARN: query '{query[:60]}' failed — {exc}")
        return []


def _fetch_serper() -> List[Dict[str, Any]]:
    """Run all Serper pathogen + VOC queries and return normalised item dicts."""
    if not SERPER_API_KEY:
        print("[serper] SERPER_API_KEY not set — skipping Google News queries.")
        return []

    items: List[Dict[str, Any]] = []

    # Pathogen queries
    print(f"[serper] Running {len(SERPER_PATHOGEN_QUERIES)} pathogen queries …")
    for query in SERPER_PATHOGEN_QUERIES:
        for result in _serper_query(query):
            link = result.get("link", "")
            if not link:
                continue
            items.append({
                "source": "serper",
                "id": _item_id("serper", link),
                "title": result.get("title", "").strip(),
                "summary": result.get("snippet", "").strip(),
                "link": link,
                "published": result.get("date", ""),
                "voc_related": False,
            })

    # VOC / chemical incident queries
    print(f"[serper] Running {len(SERPER_VOC_QUERIES)} VOC queries …")
    for query in SERPER_VOC_QUERIES:
        for result in _serper_query(query):
            link = result.get("link", "")
            if not link:
                continue
            items.append({
                "source": "serper",
                "id": _item_id("serper_voc", link),
                "title": result.get("title", "").strip(),
                "summary": result.get("snippet", "").strip(),
                "link": link,
                "published": result.get("date", ""),
                "voc_related": True,
            })

    # Deduplicate by item ID (same article can appear in multiple query results)
    seen_ids: set = set()
    deduped: List[Dict[str, Any]] = []
    for it in items:
        if it["id"] not in seen_ids:
            seen_ids.add(it["id"])
            deduped.append(it)

    print(f"[serper] {len(deduped)} unique items after dedup ({len(items)} raw).")
    return deduped


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
def _is_relevant(ext: Dict[str, Any]) -> Tuple[bool, str, Optional[int]]:
    """Return (keep, reason, tier).

    tier is 1 (confirmed efficacy), 2 (possible efficacy), or None (filtered).
    """
    vertical = (ext.get("affected_vertical") or "").strip().lower()
    if not any(v in vertical for v in RELEVANT_VERTICALS):
        return False, f"vertical '{vertical}' not in Synexis markets", None

    tier = _pathogen_tier(ext.get("pathogen") or "")
    if tier is None:
        return False, "no pathogen identified (allergen recall or mislabeling — skipped)", None

    geo = ext.get("geography") or []
    us_states = [g for g in geo if g in US_STATES]
    severity = (ext.get("severity") or "").lower()
    if not us_states and severity != "outbreak":
        return False, "non-US geography and severity below outbreak threshold", None
    return True, "relevant", tier


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


def _log_hubspot_task(company_id: str, company_name: str, pathogen: str,
                      geography: List[str], vertical: str, tier: Optional[int],
                      voc_related: bool, source_url: str) -> None:
    """Append one record to the rolling HubSpot tasks log (JSONL)."""
    record = {
        "ts": _dt.datetime.utcnow().isoformat() + "Z",
        "company_id": company_id,
        "company_name": company_name,
        "pathogen": pathogen,
        "geography": geography,
        "vertical": vertical,
        "tier": tier,
        "voc_related": voc_related,
        "source_url": source_url,
    }
    with HUBSPOT_TASKS_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


FALLBACK_OWNER_ID = "162416133"  # Connor Harrison — catches unmatched verticals

def _resolve_owner(owner_id: Optional[str], vertical: str) -> Optional[str]:
    """Return the effective HubSpot owner ID for a task.

    Priority order:
      1. ALPHA_OWNER_ID (alpha mode — routes everything to Michael for review)
      2. company-level hubspot_owner_id (already assigned, e.g. DHC territory reps)
      3. VERTICAL_OWNER_MAP fallback (new verticals with no company-level owner)
      4. FALLBACK_OWNER_ID — Connor Harrison (catches anything not matched above)
    """
    if ALPHA_OWNER_ID:
        return ALPHA_OWNER_ID
    if owner_id:
        return owner_id
    v = (vertical or "").strip().lower()
    return VERTICAL_OWNER_MAP.get(v) or FALLBACK_OWNER_ID


def _hs_create_task(subject: str, body: str, owner_id: Optional[str],
                    company_id: str, dry_run: bool, vertical: str = "") -> bool:
    # Resolve owner via priority chain (alpha → company → vertical map)
    effective_owner = _resolve_owner(owner_id, vertical)
    if dry_run:
        route_note = (
            " [ALPHA — routed to Michael]" if ALPHA_OWNER_ID
            else f" [vertical map: {vertical}]" if not owner_id and vertical
            else ""
        )
        print(f"  [hubspot] DRY RUN — would create task for company {company_id} "
              f"(owner={effective_owner}{route_note}): {subject}")
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
            **({"hubspot_owner_id": effective_owner} if effective_owner else {}),
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


def _dispatch_hubspot(ext: Dict[str, Any], dry_run: bool,
                      hs_dispatched: Optional[set] = None) -> List[str]:
    """Create HubSpot tasks for matching companies. Returns list of company IDs reached.

    hs_dispatched: a set of (company_id, pathogen_lower) tuples already actioned
    this run — used to suppress duplicate tasks when the same outbreak appears
    in multiple news items (e.g. 4 Legionella stories all match Kaiser Santa Clara).
    """
    if not HUBSPOT_ACCESS_TOKEN:
        print("  [hubspot] HUBSPOT_ACCESS_TOKEN not set — skipping HubSpot output.")
        return []

    named = (ext.get("named_company") or "").strip()

    # Single-word names (city names, generic terms) produce too-broad matches.
    # Require at least 2 words before using the named-company search path.
    if named and len(named.split()) < 2:
        print(f"  [hubspot] Skipping single-word name '{named}' — ambiguous, using geo/industry fallback.")
        named = ""

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

    if hs_dispatched is None:
        hs_dispatched = set()

    reached: List[str] = []
    pathogen = ext.get("pathogen") or "Unspecified pathogen"
    pathogen_key = pathogen.strip().lower()
    tier = ext.get("_tier", 2)
    voc_related = ext.get("voc_related", False)
    facility_type = ext.get("affected_vertical") or "affected facility"
    subject = (
        f"[{'VOC Incident' if voc_related else 'Outbreak'} Alert] {pathogen} — "
        f"{', '.join(ext.get('geography') or ['US'])}"
    )
    if voc_related:
        talking_point = (
            f"DHP® has demonstrated VOC reduction in controlled environments — "
            f"relevant outreach opportunity for {facility_type} accounts."
        )
    elif tier == 1:
        talking_point = (
            f"DHP® has demonstrated efficacy against {pathogen} — "
            f"a timely reason to reach out."
        )
    else:
        talking_point = (
            f"DHP® may be relevant to {pathogen} — efficacy not yet formally "
            f"established. Use your judgment on outreach."
        )
    tier_label = (
        "VOC / Chemical Incident"
        if voc_related
        else ("Tier 1 — Confirmed efficacy" if tier == 1 else "Tier 2 — Possible (not yet established)")
    )
    body = (
        f"Pathogen / Hazard: {pathogen}\n"
        f"Classification: {tier_label}\n"
        f"Affected area: {', '.join(ext.get('geography') or []) or 'Unspecified'}\n"
        f"Vertical: {ext.get('affected_vertical') or 'N/A'}\n"
        f"Severity: {ext.get('severity') or 'N/A'}\n\n"
        f"{ext.get('summary') or ''}\n\n"
        f"Talking point: {talking_point}\n\n"
        f"Source: {ext.get('source_url') or ''}"
    )
    vertical = ext.get("affected_vertical", "")
    for c in companies:
        cid = c.get("id")
        props = c.get("properties") or {}
        owner_id = props.get("hubspot_owner_id")
        company_name = props.get("name") or cid

        # Dedup: skip if this (company, pathogen) pair already got a task this run
        dedup_key = (cid, pathogen_key)
        if dedup_key in hs_dispatched:
            print(f"  [hubspot] Skipping duplicate task for company {cid} ({pathogen}) — already actioned this run.")
            continue
        hs_dispatched.add(dedup_key)

        if _hs_create_task(subject, body, owner_id, cid, dry_run, vertical=vertical):
            reached.append(cid)
            if not dry_run:
                _log_hubspot_task(
                    company_id=cid,
                    company_name=company_name,
                    pathogen=pathogen,
                    geography=ext.get("geography") or [],
                    vertical=vertical,
                    tier=ext.get("_tier"),
                    voc_related=voc_related,
                    source_url=ext.get("source_url") or "",
                )
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
def _find_latest_brief_pdf() -> Optional[Path]:
    """Return the most recently modified Pathogen_Outbreak_Brief_*.pdf, or None."""
    if not BRIEF_OUTPUT_DIR.exists():
        return None
    pdfs = sorted(
        BRIEF_OUTPUT_DIR.glob("Pathogen_Outbreak_Brief_*.pdf"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return pdfs[0] if pdfs else None


def _tier_badge(tier: int, voc_related: bool) -> str:
    """Return an HTML badge span for the tier label."""
    if voc_related:
        return (
            '<span style="background:#5A3E8A;color:#fff;padding:2px 8px;border-radius:3px;'
            'font-size:11px;font-weight:bold;">VOC / Chemical Incident</span>'
        )
    if tier == 1:
        return (
            '<span style="background:#1B7A3B;color:#fff;padding:2px 8px;border-radius:3px;'
            'font-size:11px;font-weight:bold;">Tier 1 — Confirmed Efficacy</span>'
        )
    return (
        '<span style="background:#7A5A1B;color:#fff;padding:2px 8px;border-radius:3px;'
        'font-size:11px;font-weight:bold;">Tier 2 — Possible Efficacy</span>'
    )


def _build_digest_html(items: List[Dict[str, Any]], today: str) -> str:
    """Build a styled HTML email body for the digest."""
    BLUE   = "#1B3A6B"
    ORANGE = "#E8541A"
    LGRAY  = "#F5F5F5"
    MGRAY  = "#555555"

    item_blocks = ""
    for i, ext in enumerate(items, 1):
        geo        = ", ".join(ext.get("geography") or []) or "Unspecified"
        pathogen   = ext.get("pathogen") or "Unspecified pathogen"
        tier       = ext.get("_tier", 2)
        voc_related = ext.get("voc_related", False)
        vertical   = ext.get("affected_vertical") or "N/A"
        severity   = ext.get("severity") or "N/A"
        summary    = ext.get("summary") or ""
        source_url = ext.get("source_url") or ""

        if voc_related:
            campaign_angle = (
                f"DHP® has demonstrated VOC reduction in controlled environments — "
                f"relevant outreach opportunity for {vertical} accounts."
            )
        elif tier == 1:
            campaign_angle = (
                f"DHP® has demonstrated efficacy against {pathogen} — "
                f"a timely reason to reach out to {vertical} accounts."
            )
        else:
            campaign_angle = (
                f"DHP® may be relevant to {pathogen} — efficacy not yet formally established. "
                f"Use judgment on outreach to {vertical} accounts."
            )

        source_link = (
            f'<a href="{source_url}" style="color:{ORANGE};">View source</a>'
            if source_url else ""
        )
        badge = _tier_badge(tier, voc_related)
        bg = "#fff" if i % 2 else LGRAY

        item_blocks += f"""
        <tr>
          <td style="padding:18px 24px;background:{bg};border-bottom:1px solid #e0e0e0;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <span style="font-size:13px;font-weight:bold;color:{BLUE};">
                    {i}. {pathogen}
                  </span>
                  &nbsp;&nbsp;{badge}
                </td>
              </tr>
              <tr><td style="padding-top:8px;">
                <table cellpadding="0" cellspacing="0" style="font-size:12px;color:{MGRAY};">
                  <tr>
                    <td style="padding-right:20px;padding-bottom:4px;">
                      <b>Geography:</b> {geo}
                    </td>
                    <td style="padding-right:20px;padding-bottom:4px;">
                      <b>Vertical:</b> {vertical}
                    </td>
                    <td style="padding-bottom:4px;">
                      <b>Severity:</b> {severity.capitalize()}
                    </td>
                  </tr>
                </table>
              </td></tr>
              <tr><td style="padding-top:6px;font-size:12.5px;color:#333;line-height:1.5;">
                {summary}
              </td></tr>
              <tr><td style="padding-top:10px;">
                <table width="100%" cellpadding="10" cellspacing="0"
                       style="background:#FFF8F5;border-left:3px solid {ORANGE};border-radius:2px;">
                  <tr><td style="font-size:12px;color:#333;">
                    <b style="color:{ORANGE};">Talking point:</b> {campaign_angle}
                  </td></tr>
                </table>
              </td></tr>
              {"<tr><td style='padding-top:8px;font-size:11px;'>" + source_link + "</td></tr>" if source_link else ""}
            </table>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5;padding:24px 0;">
    <tr><td align="center">
      <table width="620" cellpadding="0" cellspacing="0"
             style="background:#fff;border-radius:6px;overflow:hidden;
                    box-shadow:0 1px 4px rgba(0,0,0,0.1);">

        <!-- Header -->
        <tr>
          <td style="background:{BLUE};padding:20px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <div style="font-size:11px;font-weight:bold;color:{ORANGE};
                              letter-spacing:1px;text-transform:uppercase;">SYNEXIS</div>
                  <div style="font-size:18px;font-weight:bold;color:#fff;margin-top:4px;">
                    Outbreak Intelligence Digest
                  </div>
                  <div style="font-size:12px;color:#aac4e8;margin-top:2px;">{today}</div>
                </td>
                <td align="right" style="vertical-align:bottom;">
                  <div style="font-size:11px;color:#aac4e8;font-style:italic;">
                    {len(items)} qualifying item{"s" if len(items) != 1 else ""} this run
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Orange rule -->
        <tr><td style="background:{ORANGE};height:3px;"></td></tr>

        <!-- Items -->
        <tr><td style="padding:0;">
          <table width="100%" cellpadding="0" cellspacing="0">
            {item_blocks}
          </table>
        </td></tr>

        <!-- Footer -->
        <tr>
          <td style="background:{BLUE};padding:14px 24px;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td style="font-size:11px;color:#aac4e8;">
                  Synexis &nbsp;|&nbsp; Touchless, Continuous Pathogen Control
                  &nbsp;|&nbsp; synexis.com
                </td>
                <td align="right" style="font-size:11px;color:#aac4e8;font-style:italic;">
                  Weekly brief attached
                </td>
              </tr>
            </table>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _send_digest(items: List[Dict[str, Any]], dry_run: bool) -> bool:
    if not items:
        return False
    today = _dt.date.today().isoformat()
    subject = f"Outbreak Intelligence Digest — {today}"

    # Plain-text fallback
    lines = [f"Synexis Outbreak Intelligence Digest — {today}", f"{len(items)} items", ""]
    for i, ext in enumerate(items, 1):
        geo      = ", ".join(ext.get("geography") or []) or "Unspecified"
        pathogen = ext.get("pathogen") or "Unspecified pathogen"
        tier     = ext.get("_tier", 2)
        voc      = ext.get("voc_related", False)
        vertical = ext.get("affected_vertical") or "N/A"
        tier_label = (
            "VOC / Chemical Incident" if voc
            else "Tier 1 — Confirmed efficacy" if tier == 1
            else "Tier 2 — Possible efficacy"
        )
        lines += [
            f"{i}. {pathogen} — {geo}  [{tier_label}]",
            f"   Vertical: {vertical} | Severity: {ext.get('severity') or 'N/A'}",
            f"   {ext.get('summary') or ''}",
            f"   Source: {ext.get('source_url') or ''}",
            "",
        ]
    plain_body = "\n".join(lines)
    html_body  = _build_digest_html(items, today)

    # Attach the most recent weekly brief PDF if available
    brief_pdf = _find_latest_brief_pdf()
    if brief_pdf:
        print(f"[digest] Attaching brief PDF: {brief_pdf.name}")
    else:
        print(f"[digest] No brief PDF found in {BRIEF_OUTPUT_DIR} — sending without attachment.")

    return send_email(
        subject, plain_body,
        to=DIGEST_RECIPIENTS,
        dry_run=dry_run,
        attachments=[brief_pdf] if brief_pdf else None,
        html_body=html_body,
    )


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
    raw_items = (
        _fetch_cdc_food_safety()
        + _fetch_fda()
        + _fetch_who_don()
        + _fetch_fsis()
        + _fetch_serper()
    )
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

    raw_items = (
        _fetch_cdc_food_safety()
        + _fetch_fda()
        + _fetch_who_don()
        + _fetch_fsis()
        + _fetch_serper()
    )
    new_items = [it for it in raw_items if it["id"] not in seen]
    print(f"[feed_outbreaks] {len(raw_items)} items fetched, {len(new_items)} new since last run.")

    qualifying: List[Dict[str, Any]] = []
    filtered = 0
    hubspot_tasks = 0
    # Dedup set: (company_id, pathogen_lower) — prevents repeat tasks when the
    # same outbreak appears across multiple news items in a single run.
    hs_dispatched: set = set()

    for it in new_items:
        print(f"\n  [item] {it['source']}: {it['title'][:90]}")
        # FDA table rows are sparse — fetch the advisory page for full context
        # so Haiku gets real geography, pathogen detail, and scope.
        if it["source"] == "fda" and it["link"] != FDA_INVESTIGATIONS_URL:
            detail = _fetch_fda_detail(it["link"])
            if detail:
                it["summary"] = detail
                print(f"  [fda]   Detail page fetched ({len(detail)} chars).")
        ext = _extract_structured(it)
        if not ext:
            continue
        keep, reason, tier = _is_relevant(ext)
        if not keep:
            filtered += 1
            print(f"  [filter] Skipped — {reason}")
            seen[it["id"]] = {"title": it["title"], "skipped": reason,
                              "ts": _dt.datetime.utcnow().isoformat() + "Z"}
            continue

        ext["_tier"] = tier  # attach tier for downstream outputs

        # Three outputs
        reached = _dispatch_hubspot(ext, dry_run, hs_dispatched=hs_dispatched)
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
