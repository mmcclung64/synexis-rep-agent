# Code Briefing — Synexis Rep Agent

Ongoing notes and refinements from live testing. Share with Code at appropriate intervals.

---

## System Prompt Refinements

### Don't expose retrieval mechanics
Phrases like "here's what the context supports", "based on the available context", "my context shows", or "from the documents I have" expose the model's internal process and sound robotic. The agent should answer directly as a knowledgeable resource, not narrate where it found the information. Add to system prompt: never reference "context", "documents", or retrieval process in the response body.

### Table formatting — implement now for internal Beta exposure [Alpha]

Alpha testing identified a consistent pattern where Copilot's tabular output outperformed Rep Agent's default prose: questions where the response enumerates multiple items that each share the same set of parallel attributes. Three examples observed:

- Dock doors: conditions (closed / open with screens / mitigation) × outcome per condition
- Low-moisture food processing: field data broken out by use case (meat processing / egg hatchery / egg processing)
- Oil-heavy particulates: components (MERV 11 / Carbon Filter / Catalyst Sail / UV-A Lamp) × standard life / demanding-environment interval / impact of buildup

The trigger is specific: use a table when the response enumerates **three or more items that each share two or more parallel attributes**. Prose remains the default everywhere else — single-topic answers, conversational follow-ups, and narrative explanations should not become tables.

**Implement now** so Nick, Richelle, and Jimmy are exposed to the behavior during internal Beta rather than approving it in the abstract. Their usage will validate or refine the trigger condition — if tables appear where prose would serve better, that's the signal to tighten the rule. Add the table trigger to the system prompt alongside the existing header guidance.

**Extension rendering — must ship alongside the system prompt change:**

`sidebar.js` currently only parses bold (`**text**`) and URLs. Markdown tables (pipe syntax) will render as raw characters without explicit handling. Add a markdown table parser to `renderAnswer()` that converts pipe-delimited rows to `<table>` HTML.

**CSS requirements (informed by live Alpha observation):**

A 4-column table (Device / Coverage / Deployment Type / Ceiling Height Requirement) was tested in the sidebar and revealed two issues: cell content line-wraps awkwardly at sidebar width, and repeating identical values across rows wastes column space. Apply the following:

- Navy (`#0F2D69`) header row, white text, `font-weight: 600`
- Alternating row stripes: `#f9fafb` / white
- `1px solid #e5e7eb` borders, `border-radius: 6px` on outer table
- **`font-size: 12px`** in all cells — one step below body text; reduces wrapping pressure
- `overflow-x: auto` wrapper div — confirmed needed; wide tables were compressing columns rather than scrolling
- `Device` column: `white-space: nowrap` and fixed narrow `min-width` (e.g. `80px`) so "Sentry XL" never wraps to two lines
- Other columns: allow natural wrapping, generous `padding: 8px 10px`
- `width: 100%` on the table itself within the scrollable wrapper

**System prompt addition — column economy:**

When a column value would be identical across all rows (e.g. "Standard application assumed*" repeating three times), collapse it to a footnote below the table instead of a column. This keeps tables to 3 columns max where possible, which fits the sidebar width cleanly. The footnote pattern is already used naturally in generation (the agent produced the asterisk caveat as prose below the table unprompted in Alpha testing).

### Below-threshold fallback: bold inline labels, not headers

When a response contains supplementary context that doesn't meet the table trigger threshold — qualifications, caveats, notes on a specific device, operational nuance — the agent should use **bold inline labels** followed by an em-dash and prose. Never use `##` or `###` headers for sub-points within a single-topic answer.

Observed failure mode: the agent used `## Salmonella enterica`, `## Listeria monocytogenes`, `## E. coli` as section headers for a pathogen efficacy response — table threshold not met (asymmetric data across organisms), but the headers imposed artificial document structure on what should have been prose. Similarly, `## Photocatalytic DHP Generation` and `## Electrocatalytic DHP Generation` as headers for a 2-item comparison — too heavy for the content weight.

**Correct pattern:** `**High-demand environments** — Complex or high-VOC environments may require more frequent sail and filter changes.`

**Incorrect pattern:** `## High-demand environments` followed by a paragraph.

Rule of thumb: if removing the header wouldn't lose any navigational value — i.e., the content is one coherent topic, not a document with sections a reader might jump between — use bold inline instead.

### Use headers purposefully, not reflexively
Headers and bullets help reps consume responses quickly — especially on mobile between calls. Use them when there are genuinely distinct sections a rep might want to jump to, or when listing multiple parallel items. Do not use headers to label single-topic content or create artificial section breaks within a short response (e.g., "Justification:" as a header for two bullets, or splitting one topic into "Current Data" and "Cronobacter Specific Data" as separate sections). Rule of thumb: if removing the header wouldn't lose any information, remove it.

### Tone: consultative, not call-center
The agent should sound like a knowledgeable senior rep, not a help desk bot. Conversational language is fine and encouraged — but generic closing lines ("Please let me know if you need more details!", "Was this answer helpful?", "Feel free to ask follow-up questions!") undermine credibility. A consultative rep either ends with a contextually specific next step ("If you want the full study methodology, it's in the DHP/Formaldehyde manuscript") or stops when the answer is complete. Add to system prompt: never append generic feedback solicitations or open-ended follow-up offers. If a natural next step exists in the context, surface it — otherwise, stop.

### Never surface named internal contacts or personal email addresses

Observed in Alpha testing (Q20 — raw meat case studies): the agent escalated to "Tyler Mattson (tmattson@synexis.com), food safety business development lead" — retrieved from a training document that contained the contact. This violates the intended escalation design and creates several problems: the reference goes stale when roles change, external partners receive a direct internal email address outside any controlled channel, and the corpus entry will persist until manually deprecated.

Add to system prompt: never include named individuals or personal/work email addresses in responses, regardless of what appears in the retrieved context. Use only the defined escalation phrases ("contact Synexis support" / "loop in your Account Representative"). If the retrieved context contains a name or email, discard it and use the standard pattern.

This is also a corpus governance flag: any training documents containing named employee contacts should be reviewed. Email addresses in ingested content should be stripped or redacted at the pipeline level before chunking.

### Consistent escalation routing language
When the agent reaches the boundary of its knowledge — no data on a specific question, can't confirm a compatibility, can't make a claim — it should always escalate with a consistent phrase pattern. Observed in testing: "contact Synexis support" (foaming cleaners response) is the right direction. The system prompt should specify the exact escalation phrase to use so responses don't vary between "talk to your Account Representative", "contact Synexis support", "reach out to the Synexis team", etc. Proposed standard: **"contact Synexis support"** for technical/product questions, **"loop in your Account Representative"** when the question involves pricing, contracts, or deal-specific detail. Add both patterns to the system prompt with clear triggers.

### Response framing: lead with positive, caveat the negative
When a response contains both a positive finding and a gap (e.g., "DHP is effective against a broad range of pathogens" + "however, we have no data on Strep specifically"), lead with the positive and frame the gap as a caveat — not the other way around. Pattern: affirm what is true first, then "however, we don't have data on [X specifically]." This keeps the rep in a confident posture going into the conversation rather than opening with what they can't claim.

---

## Bug Fixes

### Duplicate citations in sources list
The sources list at the bottom of responses can show the same file + page combination more than once (observed: nejm Point Prevalence of HAI.pdf page 8 appearing as both [2] and [5]). The dedup-by-text-hash step correctly collapses duplicate chunk content before generation, but the citation list is not deduplicated before rendering. Fix: deduplicate the sources list by (file_path, page_or_slide) before returning in the API response, keeping the lowest citation index for each unique source.

### Sources list should sort by citation number
The sources list at the bottom renders in retrieval order, not citation number order. Fix: sort sources list by citation index before rendering so [1] always appears first.

### Table word-break breaking mid-character in narrow columns [FIXED]

In 4-column tables at sidebar width, `word-break: break-word` on non-first `th` and `td` elements was breaking words mid-character — observed as "Coverag e Area" in a header and "6 mont hs" in a data cell. Fixed by replacing `word-break: break-word` with `overflow-wrap: break-word; word-break: normal` on those selectors. `overflow-wrap` breaks within words only as a last resort; the `overflow-x: auto` wrapper on `.table-wrap` handles wide tables by scrolling rather than compressing.

### Italic markdown not rendering [FIXED]

`*italic*` markdown was rendering as raw asterisks — `inlineTransforms()` in `sidebar.js` only handled `**bold**`. Fixed by adding `html.replace(/\*([^*\n]+)\*/g, "<em>$1</em>")` immediately after the bold rule, so bold markers are fully consumed before the single-asterisk pattern runs.

### Citation numbering gaps after dedup
After the dedup step removes chunks, citation numbers in the response body can be non-sequential (e.g., [1], [3], [4] with no [2]). This looks like an error to the reader. Fix: after dedup, renumber citations sequentially so the response always shows [1], [2], [3] with no gaps. Requires rewriting citation markers in both the answer text and the sources list.

---

## Retrieval Tuning

### Chunking failure: Application Guide PDF table (page 13) — maintenance intervals by environment

`Manuals and Guides/Synexis Application Guide 20250501.pdf` page 13 contains a structured table of maintenance intervals by environment type (Retail Stores, Spa, Theaters, Utility Closets, etc.) with multiple numeric columns. PDF extraction flattened the table into a token stream without preserving row/column relationships, producing chunks like: "6 6 24 or 60 Retail Stores 6 6 6 24 or 60 Spa 6 6 6 24 or 60 Theaters 6 6 6 24 or 60 Utility Closets..." The numbers (likely filter/bulb replacement intervals in months) are unanchored from their column headers and the chunk is semantically opaque. Observed live in a citation tooltip during Alpha testing — visible to users and looks broken.

Impact: (1) chunk won't retrieve reliably on natural language maintenance schedule queries, (2) if it does retrieve, it provides garbled context that could produce wrong answers, (3) tooltip exposure is unprofessional.

Fix: re-extract page 13 with table-aware parsing (pdfplumber or camelot) to recover row/column structure, then re-chunk as one row per chunk with column headers prepended (e.g. "Retail Stores — Filter: 6 months, Bulb: 24 months, ..."). Re-embed and upsert to replace the current broken chunks. Check adjacent pages of the same PDF for the same issue — tables in the Application Guide may be affected throughout.

### Retrieval miss: equipment material compatibility queries
Query "how does it do on various equipment materials (stainless steel, belts, plastics)" returned 0 relevant chunks despite the corpus containing applicable content — SDS non-corrosive data, device construction materials from device manuals (18-gauge stainless steel housing, ABS/polycarbonate, metal mesh catalyst components). The same corpus correctly answered the metals question ("will DHP react with any metals?"), so the chunks exist. The issue is semantic distance between "equipment materials" and the relevant chunk content. Possible fixes: (1) add "material compatibility" as a BM25-boosted keyword cluster in the hybrid retrieval config, (2) add a query expansion step that maps material-type queries to SDS + device manual source filters, or (3) ensure device manual chunks include equipment material compatibility as explicit metadata tags for filtered retrieval.

---

## Latency & Performance

Independent review identified gaps in the original latency analysis. Items are tiered: implement now, implement before Beta, or deferred pending measurement.

### ① Profile the pipeline first — gate on this before any optimization work [NOW]

Before implementing any latency optimization, instrument the pipeline to measure each step independently: input validation, embedding, retrieval, reranking, generation. Log timing per step on every query (structured JSON). Without a baseline breakdown, optimization efforts will be aimed at the wrong bottleneck. Current observed wall time is 8–18s — the likely dominant cost is Claude generation, but measure before assuming.

### ② Streaming — implement now, design citations carefully [NOW]

Switch FastAPI to `StreamingResponse` and the extension to a `ReadableStream` reader. First token appears in ~1–2 seconds; full response completes in the same 15–18s wall time but perceived latency drops dramatically. This is the single highest-impact improvement available.

**Citation architecture caveat (must be resolved before implementation):** Citations are currently embedded in the response body. With streaming, inline citation markers (`[1]`, `[2]`) will appear mid-stream but the sources list won't arrive until the end — disorienting UX. Design decision required: either (a) stream answer text only and append the sources list after the stream closes, or (b) emit a structured preamble (citations metadata) before streaming begins so the extension can resolve badges in real time. Option (a) is simpler. Resolve this before Code touches streaming.

### ③ Haiku for input validator [NOW, after profiling]

Switch the input validator from Sonnet to Haiku. The classification task (is this query on-topic?) doesn't need Sonnet's capability. **Revised savings estimate: ~80–130ms** — not the 400–700ms originally stated. Short prompts on fast models have low variance; the original estimate was 2–3x too high. Still worth doing for cost reduction at scale; just set the right expectations.

### ④ Load test Render before internal Beta [BETA gate]

The Render Starter tier (0.5 CPU, 512MB RAM) has not been tested under concurrent load. Under 5–10 simultaneous users, CPU contention may add 500ms–2s of queued latency that doesn't appear in single-query testing. Run a concurrency test (5–10 parallel requests) before internal Beta. If p95 latency spikes >20% vs. single-query baseline, upgrade to the next tier ($12/month) — the cost delta is negligible.

### ⑤ Session-level response caching [BETA]

Cache responses per session with a 30-minute TTL. Keyed on normalized query string within a session. **Do not implement cross-session caching** — if the corpus is evolving (new docs ingested, old ones deprecated), a cross-session cache will silently serve stale answers. Session-level cache is safe (TTL is short, corpus changes between sessions are expected) and still provides meaningful speed improvement for Alpha testing where the same questions are re-run repeatedly. Cache-bust strategy: invalidate all session caches on any corpus ingest event.

### ⑥ Parallelize validation and embedding [DEFERRED — measure first]

Originally proposed: fire Voyage AI embedding and input validator in parallel. **Problem:** validation gates whether retrieval proceeds — if it fails, embedding was wasted. This only saves time if validation is slow (>150ms). After profiling in ①, revisit. If validator is <100ms (likely with Haiku), parallelization saves nothing and adds code complexity. Skip unless profiling proves otherwise.

### ⑦ Query rewriting (Haiku) [ALPHA — elevated from deferred]

**Priority elevated after two consecutive multi-turn retrieval failures observed in Alpha testing:**
- Q13 "What about poultry hatcheries?" (in context of dusty environments question) — both agents missed; Copilot marginally better
- Q14 "What about the door?" (in context of walk-in cooler deployment question) — Copilot correctly resolved to walk-in cooler door deployment guidance; Rep Agent retrieved door handle bioburden data from a pediatric ICU study

The trigger condition from the original deferral has been met. Copilot's multi-turn context resolution is demonstrably better on short ambiguous follow-ups. Without query rewriting, Rep Agent loses these exchanges.

**Implementation:** Before hitting Pinecone, make a Haiku API call with the conversation history + current query to produce a standalone, contextualized query. The rewriter should do two things: (1) resolve context ("What about the door?" → "How do walk-in cooler doors affect Synexis device placement and DHP concentration?") and (2) normalize non-standard terminology to canonical corpus terms ("walking cooler" → "walk-in cooler", "caustic" → "caustic cleaning agents", etc.). Copilot's Turn 2 response on the walk-in cooler question correctly resolved the context but carried the user's non-standard term "walking coolers" through verbatim — suggesting it threaded the prior topic mechanically without normalizing terminology. Use the rewritten query for retrieval only; pass original history to generation unchanged.

**Risks to manage (from independent review):**
- Rewriting may normalize away domain-specific terms Pinecone needs — monitor retrieval quality on technical queries post-implementation
- Adds ~670–890ms latency (measured in Alpha — higher than the 150–300ms originally estimated; re-measure once Latency ① pipeline profiling logs are in to attribute time accurately) — acceptable within overall 15–18s wall time
- Skip rewrite when history is empty or fewer than 2 turns — avoid wasting a Haiku call on effectively stateless queries
- Log both original and rewritten query on every turn — required for debugging and measuring whether rewriting actually improved retrieval

### ⑧ Voyage AI call batching [POST-BETA]

If embedding and reranking are running as separate sequential API calls, combining or pipelining them could save 50–100ms. Low priority; measure first.

---

## Planned Capabilities (Pre-Partner-Beta)

### Multi-turn conversation — simplified implementation, no rewrite step [Alpha — accelerated from Beta]

The agent is currently stateless — each `/query` is independent with no conversation history. **Priority elevated to Alpha:** the competing Copilot bot already supports multi-turn conversation, making the head-to-head comparison structurally uneven without it. Reps evaluating both tools will notice immediately. The simplified implementation (below) is low-risk and backward-compatible — a few hours of Code work with no pipeline changes.

Note: the remaining Alpha eval questions (webinar question bank) are single-turn by design and are not affected by this change. A separate multi-turn testing pass with the internal team (Nick, Richelle, Jimmy) should follow implementation before the Beta handoff.

**Implementation — history-in-generation only (no query rewriting):**

Pass the full conversation history to Claude generation as a messages array (alternating user/assistant turns). Use the **original unmodified last user turn** for retrieval — do not rewrite. This is simpler, avoids adding latency, and sidesteps the rewriter's known failure mode of normalizing away domain-specific terms. If retrieval quality demonstrably suffers on pronoun-heavy follow-ups during Beta testing, add the rewrite step then (see Latency ⑦).

**Extension changes:**
- Accumulate turns in `localStorage` (not just in-memory) — history must survive sidebar close/reopen
- Send history array with each `/query` POST; skip sending history on the first turn (empty array)
- Add a "New conversation" button, prominently placed — required to prevent context bleed between different customer calls
- When context is truncated (see below), show a subtle UI indicator: "Earlier context summarized"

**API changes:**
- `QueryRequest`: add optional `history: list[dict]` field (empty or absent = stateless, backward-compatible)
- `answer.py`: update generation call to use messages array when history is present
- Add structured logging: `session_id`, `turn_id`, retrieved chunk scores, per-step timing — required for debugging multi-turn failures
- API response: add `context_utilization` field (% of context window used) so extension can warn user when approaching limits

**Truncation strategy — simple last-N-turns for Alpha, sliding window summarization for Beta:**

For Alpha: implement simple last-8-turns truncation. This is sufficient for early multi-turn testing where conversations will be short and controlled. When truncation kicks in, show a subtle UI indicator: "Earlier context summarized."

For Beta (before Partner Beta): replace with sliding window summarization. FIFO truncation (dropping oldest turns) is wrong for sustained conversations — it loses the setup context from turn 1 that later turns depend on. The correct approach: every 4–5 turns, summarize prior exchanges into a compact system-injected context block and keep the last 4 turns in full. This preserves intent without ballooning token count.

**Smoke test results (Alpha — passed):**

3-turn conversation (`work/smoke_multiturn.py`): Turn 1 stateless (feline calicivirus surface efficacy). Turn 2 pronoun follow-up ("what's the contact time on that?") — `history_turns_used=2`, model correctly resolved the referent. Turn 3 topic switch (USP 797 pharmacy) — `history_turns_used=4`, fresh retrieval from pharmacy/USP sources with no calicivirus drift. Regression eval: 18/18 pass, stateless path unchanged.

**Known caveat — citation hedging on follow-up turns [RESOLVED by query rewriting]:**

Turn 2 of the smoke test produced a mildly hedged answer: "contact times in my previous answer... that context isn't included in the current retrieval." Root cause: `[N]` citation markers are stripped from conversation history before it's passed to Claude. With query rewriting now implemented, this is resolved — the rewriter produces specific enough queries (e.g. "What is the contact time required for DHP to achieve 99.8% reduction of feline calicivirus on non-porous surfaces?") that retrieval returns the right chunks confidently, and the agent cites directly. Verified in Alpha eval: agent now answers "Six hours" verbatim on the calicivirus contact time follow-up, no hedging. No further action required.

**Risks to monitor during internal Beta:**
- Retrieval quality: does omitting rewriting cause retrieval misses on follow-ups like "what about there?" or "does it work with that?" — monitor query logs; add rewrite step if yes
- Conversation drift: user shifts topic across turns; verify retrieval doesn't over-anchor on earlier turns — test with topic-switch fixtures
- Context window pressure: long Beta sessions may push toward Sonnet's limit; `context_utilization` field enables the extension to warn before it's a problem
- Citation hedging on follow-up turns: see known caveat above — watch for it during internal Beta sessions

**Multi-turn eval benchmark — required before Partner Beta [Beta gate, not Alpha]:**
Build 10–15 hand-crafted multi-turn conversation fixtures with known-good answers at turns 3, 5, and 7. Test cases must include: explicit follow-ups, pronoun/reference follow-ups, topic switches, and long conversations (8+ turns). Score on retrieval quality, answer coherence, and context preservation. Current eval harness is single-turn only — this gap must be closed before Partner Beta. The internal team's usage during Alpha will inform which conversation patterns matter most for the fixture set.

### Feedback mechanism — thumbs up/down + free text [Alpha]

Add inline feedback controls to each answer turn in the extension. Appears after the answer renders — not before, and not blocking.

**Extension changes:**
- After `finalizeTurn`, append thumbs up / thumbs down buttons to the turn element
- Thumbs down reveals a collapsible free-text field ("Where did the agent miss?") with a Submit button
- On submit (or thumbs up), POST to `/feedback` with: `query`, `answer`, `citations`, `rating` (`up`/`down`), `feedback_text` (optional)
- Disable feedback controls after submission — one rating per turn

**API changes:**
- New `POST /feedback` endpoint: accepts the payload above, logs it (structured JSON, same logger as query events)
- No pipeline changes required — feedback is logged only; corpus updates from feedback are a manual governance step

**Why this matters for internal Beta:** Nick, Richelle, and Jimmy flagging bad answers inline — with context on what was wrong — is the primary mechanism for identifying corpus gaps and nuance failures before Partner Beta. Free text is where the value is; thumbs down alone isn't actionable.

### Feedback-to-corpus pipeline — auto-generate candidate content from SME corrections [Beta]

When a thumbs-down with free text is submitted, the `/feedback` endpoint should do two things: log the event (as above) and automatically generate a candidate Q&A document for corpus review. The candidate document contains the original question, the agent's answer, and the SME's correction formatted as an approved-answer entry. It is dropped into `source_content/` with status `pending-governance` and triggers a notification (email or Slack) summarizing what was flagged. Nothing is ingested until a governance owner explicitly approves it — the existing manifest status system handles this without any new infrastructure.

**Approval UI — `/admin` route [Beta]:**

A minimal admin panel alongside the existing `/ui` route. Displays all `pending-governance` items from the feedback pipeline with the candidate content visible. Governance owners (Nick for regulatory/efficacy, Richelle/Jimmy for sales guidance) can approve or reject directly — approve flips the manifest status to `approved` and queues ingest; reject archives the item with a note. This removes Michael from the critical path for corpus updates once the internal team is actively flagging things.

**Why this matters:** Without this pipeline, SME feedback accumulates in a log that requires manual review, document drafting, and governance coordination before anything improves. With it, a correction Nick writes in 30 seconds becomes a candidate corpus entry that's one approval click away from being live. The drop-folder + manifest architecture already supports `pending-governance` status — this is wiring the feedback endpoint into a workflow that's already designed for it.

---

## Planned Capabilities (Post-Beta)

### Nightly trade press sweep
Add a nightly scheduled task that searches a defined list of trade publications (*Infection Prevention Today*, *APIC*, *Food Safety News*, etc.) for articles matching relevant keywords (DHP, dry hydrogen peroxide, infection prevention, HAI, food safety, bioburden, etc.). Fetches full text of any hits, drops them into `source_content/` with status `pending-governance`, and sends a notification (email or Slack) summarizing what was found. Governance review (Nick / Richelle / Jimmy) approves items before they're ingested into the corpus. Requires a news/web search API — Google News API, NewsAPI.org, or Exa are candidate options. Fits cleanly into the existing drop-folder pipeline architecture. No web access added to the agent itself — sweep is a separate upstream process.

---

## Extension UX Refinements

### Standalone web UI for testing and demos
Add a `/ui` route serving a static HTML page that replicates the extension sidebar interface and calls `/query` directly. This allows browser-based testing without the Chrome side panel limitation, and doubles as a demo surface for Beta. No API changes required — same `/query` endpoint, same response format.

### Citation tooltip vertical clipping [FIXED]

Hover tooltips on citation badges were clipping at the top of the viewport when a badge appeared near the top of the scrollable `main` container. Root cause: tooltip is `position: absolute` inside a badge inside `main` which has `overflow-y: auto` — the scroll container clips the tooltip regardless of `z-index`.

Initial fix attempt (`scrollIntoView` on `mouseover`) was incorrect — scrolling the badge out from under the pointer fires `mouseleave` and collapses the tooltip before it renders.

Final fix: switched `.cite-tooltip` to `position: fixed` with JS-calculated viewport coordinates. The tooltip is now positioned relative to the viewport, completely bypassing scroll container overflow clipping. Logic: on `mouseover`, show tooltip invisible to measure its dimensions, then calculate `top`/`left` in fixed coordinates. If the tooltip would clip at the top (`fitsAbove` check), flip it below the badge instead (`.below::after` CSS variant flips the arrow direction). Horizontal clamping with arrow offset adjustment retained. `mouseout` hides on pointer leave (with child-element guard). `focusin`/`focusout` handlers mirror the behavior for keyboard navigation.

### Citation anchor `href` jumps to first instance in chat history [FIXED]

Clicking an inline citation badge (`href="#src-N"`) was jumping to the first occurrence of `#src-N` in the DOM — always the oldest turn's source entry, not the current one. Root cause: every turn rendered `id="src-1"`, `id="src-2"`, etc. with no turn scope, so duplicate IDs resolved to the first match.

Fixed by threading a `turnKey` integer through the rendering chain (`renderBadge`, `inlineTransforms`, `renderTableBlock`, `renderAnswer`, `renderCitations`, `finalizeTurnEl`). IDs are now `src-{turnKey}-{n}` — e.g. `#src-0-1`, `#src-1-1`. The key is `userTurnCount(session)` at submit time; history restore uses a matching incrementing counter. IDs stay consistent whether a turn was just submitted or reloaded from a prior session.

### Citation tooltip positioning — left edge clipping
Hover tooltips on inline citation badges are clipping at the left edge of the panel when the badge appears near the left margin. Fix: constrain tooltip `max-width` to panel width minus padding (e.g., `max-width: calc(100% - 24px)`), set `left: 0` with a small offset rather than centering on the badge, and ensure `position: absolute` parent has `overflow: visible` so the tooltip doesn't get clipped by a parent container. A `clamp()` or `Math.max()` check on the computed left position in JS will prevent the tooltip from rendering outside the sidebar bounds.

### Citation display — inline superscript badges with hover tooltips
Currently citations render as `[N]` inline markers with a flat sources list at the bottom. The target UX (observed in Copilot bot) is: inline superscript number badges (styled circles) with a hover tooltip showing a short snippet of the source text. The full sources list can remain at the bottom, but the inline markers should be visually distinct and interactive. This is a CSS/JS change in sidebar.html and sidebar.js — no API changes required. The API should also return a `snippet` field per citation (first ~150 chars of the chunk text) to populate the tooltip.

### Citation badge visual weight — reduce prominence, apply brand color
Current badge styling is too visually prominent — the badges compete with the response text rather than supporting it. Target: support element, not focal point. Changes: (1) reduce badge size slightly (font-size and circle diameter one step down from current), (2) replace current background color with Synexis brand navy `#0F2D69`. The dark navy limits brightness and avoids the eye-catching contrast of the current color while maintaining legibility. White text on `#0F2D69` passes WCAG AA. Copilot's treatment is too subtle in the other direction — this should land between the two.

### Sources list — match citation number color to badge color
The numbered citation markers in the sources list at the bottom of the response should use the same `#0F2D69` color as the inline superscript badges. This creates a unified citation system where the inline reference and the corresponding source entry are visually connected. Apply `color: #0F2D69` (or the equivalent styled class) to the number/index portion of each source list item.

---
