# Outbreak Watcher — Git Convention

## Why this repo

The Outbreak Watcher pipeline lives inside `synexis-rep-agent` because it shares
core infrastructure: `.env`, `pipeline/monitoring/utils.py`, and `orchestrator.py`.
Extracting it into a separate repo would require duplicating or cross-linking that
infrastructure — not worth it at current scale.

## Commit prefix

All Outbreak Watcher changes use the prefix `outbreak:` to keep them scannable
in the log and clearly separate from extension/backend work.

```
outbreak: <short description>

- detail
- detail
```

**Extension/backend prefix:** no prefix (or `fix:` / `feat:` as usual)
**Outbreak watcher prefix:** `outbreak:`

## Files that belong to Outbreak Watcher

| File | Notes |
|---|---|
| `pipeline/monitoring/feed_outbreaks.py` | Main pipeline — all sources, HubSpot dispatch, digest |
| `pipeline/monitoring/utils.py` | Shared — Graph API email used by both projects |
| `pipeline/monitoring/orchestrator.py` | Shared — runs all feeds including outbreak |

## Files that live outside this repo (not version-controlled)

| Location | What |
|---|---|
| `~/Desktop/Claude/outbreak-watcher/` | Outputs: briefs, PDFs, tracker, HANDOFF.md |
| `~/Desktop/Claude/outbreak-watcher/generate_brief_pdf.py` | PDF generator |
| `synexis-rep-agent/.env` | All secrets — never committed |
