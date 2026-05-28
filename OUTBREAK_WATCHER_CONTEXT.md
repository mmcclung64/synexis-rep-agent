# Synexis Outbreak Watcher — Project Context

This file is the source of truth for any tool or agent working inside this repository.
It covers architecture, design decisions, current status, and known gotchas.

---

## What This Pipeline Does

Monitors public health sources for pathogen outbreaks relevant to Synexis' sales verticals.
Per qualifying item, it:
1. Creates a HubSpot task for the appropriate sales rep
2. Drops a governance-pending markdown file into `source_content/Outbreak Intelligence/`
3. Sends a single aggregate digest email per run

Run command (native Mac only — see constraints):
```
bash run_outbreak_watcher.sh --confirm   # full run
bash run_outbreak_watcher.sh             # dry run (default)
```

---

## Product Context

**Synexis** sells **DHP® (Dry Hydrogen Peroxide)** — continuous, touchless pathogen control.

**Tier 1 pathogens** (confirmed DHP® efficacy): Salmonella, Listeria, E. coli, MRSA, C. auris, VRE, norovirus, influenza / avian influenza / H5N1, Candida, mold/Aspergillus, RSV, COVID-19/SARS-CoV-2, Staphylococcus, Streptococcus, C. diff, Legionella, Campylobacter, Hepatitis A, Cryptosporidium

**Tier 2 pathogens** (possible relevance): Cereulide, Bacillus cereus, Hantavirus, any named pathogen not on Tier 1 list

**VOC / chemical incidents**: DHP® has demonstrated VOC reduction in controlled environments — treated as Tier 2 outreach opportunities

**Key verticals**: Healthcare (hospitals), Higher Education (campus health), Food Safety / Food Processing, Animal Health, Poultry

---

## File Structure

```
synexis-rep-agent/
├── pipeline/
│   └── monitoring/
│       ├── feed_outbreaks.py     ← main pipeline (edit here)
│       └── utils.py              ← email (Graph API) + monitoring log
├── logs/
│   ├── outbreaks_state.json      ← seen item IDs; persists between runs
│   ├── hubspot_tasks_log.jsonl   ← rolling log of HubSpot tasks created
│   ├── confirm_run.log           ← stdout of last --confirm run
│   └── monitoring.jsonl          ← structured event log
├── source_content/
│   └── Outbreak Intelligence/   ← governance-pending corpus drops
│       └── YYYY-MM-DD_pathogen_state.md
├── .env                          ← secrets (never commit)
├── run_outbreak_watcher.sh       ← entry point
└── OUTBREAK_WATCHER_CONTEXT.md  ← this file
```

---

## Data Sources

| Source | Method | Status |
|---|---|---|
| CDC Food Safety RSS | feedparser | ✅ Active |
| FDA Outbreak Investigations page | BeautifulSoup HTML scrape | ✅ Active |
| WHO Disease Outbreak News | Serper (`site:who.int "disease-outbreak-news/item" 2026`) | ✅ Active — RSS was 404 as of May 2026 |
| USDA FSIS Recalls API | `GET /fsis/api/recall/v/1` | ⚠️ Returns 403 even on native Mac; CDC covers same content; graceful no-op |
| Serper / Google News | 3 pathogen queries + 3 VOC queries | ✅ Active |

**Serper pathogen queries:**
```python
"Salmonella OR Listeria OR \"E. coli\" outbreak 2026"
"MRSA OR norovirus OR Legionella outbreak hospital 2026"
"avian influenza H5N1 outbreak 2026"
```

**Serper VOC queries:**
```python
'"VOC contamination" building OR facility OR workplace'
'"indoor air quality" outbreak OR incident OR evacuation'
'"chemical contamination" school OR hospital OR facility'
```

---

## Pipeline Logic (feed_outbreaks.py)

### Per-item flow

1. Fetch all sources → deduplicate by item ID → filter to new items (not in `outbreaks_state.json`)
2. For FDA items: fetch detail page for full context before Haiku extraction
3. **Haiku extraction** → structured JSON: `pathogen`, `affected_vertical`, `geography` (list of US states), `severity`, `named_company`, `summary`, `source_url`
4. **Relevance filter** → drop if vertical not in RELEVANT_VERTICALS, or non-US geography + severity != "outbreak"
5. **Pathogen tier** → Tier 1/2/None. Allergen blocklist checked AFTER Tier 1/2 (critical: prevents "salmon" matching "Salmonella")
6. **HubSpot dispatch** → create task(s)
7. **Corpus drop** → write governance-pending markdown
8. After all items: **send digest email**

### Pathogen tier priority (do not reorder)
```python
if any(t in p for t in TIER_1_PATHOGENS): return 1
if any(t in p for t in TIER_2_PATHOGENS): return 2
if any(a in p for a in ALLERGEN_BLOCKLIST): return None
return 2  # named pathogen not on either list
```

### HubSpot dispatch rules

**Company search:**
- If `named_company` has ≥ 2 words → `CONTAINS_TOKEN` name search (max 5 results)
- Single-word company names are skipped (too ambiguous — city names, generic terms)
- If no named company or no name matches → state + industry geo search (max 10 results)

**Task dedup (within a single run):**
- `hs_dispatched: set` of `(company_id, pathogen_lower)` tuples
- Same company + same pathogen won't get two tasks in one run, even if 4 news articles all cover the same outbreak

**Owner resolution priority:**
1. `ALPHA_OWNER_ID` (env var) — overrides everything; routes all tasks to Michael during alpha
2. `hubspot_owner_id` on the matched company record (set by DHC import territory assignment)
3. `VERTICAL_OWNER_MAP` fallback (for verticals with no company-level owner)

### VERTICAL_OWNER_MAP

| Vertical key | Owner | HubSpot Owner ID |
|---|---|---|
| `education` | Larry Shapiro | 88106519 |
| `animal health` | Denise Bucari | 82257890 |
| `food processing` / `food production` / `food safety` | Tyler Mattson | 82067944 |
| `poultry` | Federico Sanchez | 162416134 |
| healthcare | (not in map — DHC import assigns territory reps at company level) | — |

**Healthcare territory reps:**
| Rep | HubSpot Owner ID | Territory |
|---|---|---|
| Matt Howarth | 163856334 | HC Northeast |
| Brian Reina | 163856335 | HC West |
| Jeff Popick | 163856336 | HC Southeast |

---

## Email Delivery (utils.py)

Microsoft Graph API client credentials flow. SMTP is disabled at the Synexis tenant level.

```python
# Token endpoint
https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token

# Send endpoint
https://graph.microsoft.com/v1.0/users/{GRAPH_SENDER_EMAIL}/sendMail
```

Required `.env` vars: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `GRAPH_SENDER_EMAIL`

The `send_email(subject, body, to, dry_run)` function in `utils.py` is the only interface callers need — token caching is internal.

---

## Environment Variables (.env)

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Required — Haiku extraction |
| `HUBSPOT_ACCESS_TOKEN` | HubSpot tasks (skipped if absent) |
| `MARKETING_EMAIL` | Digest recipient (falls back to NOTIFY_EMAIL) |
| `NOTIFY_EMAIL` | Operator alert fallback |
| `SERPER_API_KEY` | Google News queries (WHO DON + pathogen + VOC) |
| `ALPHA_OWNER_ID` | Routes all tasks to this owner ID (set to 82345912 = Michael in alpha) |
| `AZURE_TENANT_ID` | Graph API auth |
| `AZURE_CLIENT_ID` | Graph API auth |
| `AZURE_CLIENT_SECRET` | Graph API auth |
| `GRAPH_SENDER_EMAIL` | Graph API sending mailbox |
| `SOURCE_CONTENT_PATH` | Base path for corpus drops |
| `PINECONE_API_KEY` | Vector store (used by other pipeline modules) |
| `VOYAGE_API_KEY` | Embeddings (used by other pipeline modules) |

---

## Scheduled Tasks (Cowork)

These run automatically via Cowork's scheduled task system. Task files live in
`~/Documents/Claude/Scheduled/`.

### weekly-pathogen-brief (Mon 8:08am)
Generates the internal weekly brief. Steps:
1. Search CIDRAP, YLE Substack, WHO DON, HubSpot tasks log
2. Write `~/Desktop/Claude/outbreak-watcher/weekly-pathogen-brief-YYYY-MM-DD.md`
3. Update `~/Desktop/Claude/outbreak-watcher/pathogen-tracker.md`
4. Run `cd ~/Desktop/Claude/outbreak-watcher && python3 generate_brief_pdf.py weekly-pathogen-brief-YYYY-MM-DD.md`
5. Send digest email, post PDF link in chat

### synexis-source-content-check (every 6h)
Checks `source_content/` for new unclassified files and updates the manifest.

---

## Weekly Brief PDF Generator

`~/Desktop/Claude/outbreak-watcher/generate_brief_pdf.py`

- Input: path to a `weekly-pathogen-brief-YYYY-MM-DD.md` file
- Output: `Pathogen_Outbreak_Brief_YYYY-MM-DD.pdf` in same directory
- Uses reportlab Platypus; Synexis brand colors BLUE=#1B3A6B, ORANGE=#E8541A
- Requires: `pip install reportlab --break-system-packages`
- Usage: `python3 generate_brief_pdf.py weekly-pathogen-brief-2026-05-14.md`

---

## HubSpot Tasks Log (hubspot_tasks_log.jsonl)

One JSON record per task created. Read by the weekly brief generator to populate the
"HubSpot Activity This Week" table in the brief.

```json
{
  "ts": "2026-05-14T18:23:01Z",
  "company_id": "293877060326",
  "company_name": "Kaiser Santa Clara Medical Center",
  "pathogen": "Legionella",
  "geography": ["California"],
  "vertical": "healthcare",
  "tier": 1,
  "voc_related": false,
  "source_url": "https://..."
}
```

---

## Known Issues & Gotchas

**Run environment**
- Full `--confirm` run must be done natively on Mac — Cowork sandbox times out at 45s (5 sources + Haiku calls exceed limit)
- `SOURCE_CONTENT_PATH` in `.env` is an absolute Mac path; sandbox overrides with `SOURCE_CONTENT_PATH=/tmp/...`

**Feed availability (as of May 2026)**
- WHO RSS `https://www.who.int/feeds/entity/csr/don/en/rss.xml` → 404; replaced with Serper
- FSIS RSS `https://www.fsis.usda.gov/rss/recalls.xml` → 403; FSIS JSON API also → 403; CDC covers same content

**HubSpot**
- Single-word company names skipped (e.g. "Hartford" matched 5 unrelated companies in first test run)
- Task dedup (`hs_dispatched` set) prevents the same company/pathogen pair getting multiple tasks when multiple news articles cover the same outbreak in one run
- Company search uses `CONTAINS_TOKEN` — partial matches are intentional but can still be loose; review tasks in alpha mode before beta flip

**Alpha mode**
- `ALPHA_OWNER_ID=82345912` routes all tasks to Michael (mmcclung@synexis.com) for review
- To flip to beta: remove `ALPHA_OWNER_ID` from `.env`, change `MARKETING_EMAIL` to `marketing@synexis.com`

**Allergen / pathogen priority**
- Allergen blocklist is checked AFTER Tier 1/2 pathogen lists — do not reorder
- This prevents "salmon" in the allergen blocklist from matching "Salmonella"

---

## Current Status (as of May 14, 2026)

| Component | Status |
|---|---|
| CDC / FDA feeds | ✅ Working |
| WHO DON via Serper | ✅ Working (10 DONs per run) |
| FSIS | ⚠️ 403 — graceful no-op, CDC covers content |
| Serper pathogen + VOC queries | ✅ Working |
| Haiku extraction | ✅ Working |
| HubSpot task creation | ✅ Working — alpha mode (all → Michael) |
| Task dedup | ✅ Working |
| Name ambiguity threshold | ✅ Working (2-word minimum) |
| Graph API email | ✅ Working — confirmed delivered |
| Corpus drops | ✅ Working |
| Weekly brief (Cowork scheduled task) | ✅ Scheduled Mon 8:08am — first live run May 18 |
| PDF generation | ✅ Working — `generate_brief_pdf.py` tested May 14 |
| Beta flip (remove alpha owner) | ⏳ Pending — run 2-3 more weeks in alpha first |
