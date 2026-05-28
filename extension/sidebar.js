// Synexis Rep Agent — side panel controller.
// Talks to the Synexis Rep Agent HTTP API. Multi-turn: the session (sessionId +
// turns array) is persisted in chrome.storage.local so history survives the
// sidebar closing and reopening. Only the last MAX_HISTORY_TURNS turns are sent
// on each /query request; the server applies its own safety truncation on top.

const DEFAULT_API_URL = "https://synexis-rep-agent.onrender.com";
const SETTINGS_KEY = "sra.settings";
const SESSION_KEY = "sra.session";
const MAX_HISTORY_TURNS = 8;

// Set to true during corpus rebuilds / Pinecone wipes to block all queries.
const MAINTENANCE_MODE = false;
const MAINTENANCE_MESSAGE = "I am currently undergoing maintenance. Please try back later.";

// Pre-cached intro text shown instantly when a query is submitted, before the
// first token arrives from the API. Keyed by the industry-picker value.
// If the LLM generates a real preamble sentence it streams over this naturally;
// if it jumps straight to ## sections this text stays visible as the lead-in.
// Edit these strings to tune tone/depth — no redeployment required, just reload the extension.
const VERTICAL_INTROS = {
  "Healthcare": "Synexis DHP® technology delivers continuous, touchless pathogen reduction across healthcare environments — patient rooms, waiting areas, and hallways — without chemicals, room downtime, or staff intervention.\nHere's how it works for healthcare:",

  "Animal Health": "Synexis DHP® technology provides continuous, chemical-free pathogen control across veterinary clinics, livestock facilities, and animal care spaces — without harming animals, disrupting workflows, or requiring handler intervention.\nHere's the full picture for animal health:",

  "Food Safety": "Synexis DHP® technology is purpose-built to complement food safety programs with continuous, touchless pathogen control across air and surfaces — without chemical residue, production disruption, or staff intervention.\nHere's the full picture for food safety:",

  "Higher Education": "Synexis DHP® technology addresses pathogen control challenges across dense campus environments — dorms, dining halls, health clinics, and classrooms — with continuous, touchless protection that runs in occupied spaces.\nHere's the full picture for higher education:",

  // Fallback for "Other", "I'm interested in everything", or no industry selected
  "": "Synexis DHP® technology delivers continuous, touchless pathogen control across air and surfaces — without chemicals, downtime, or staff intervention.\nHere's what I found:",
};

const $ = (id) => document.getElementById(id);

// ---------- settings ----------

async function loadSettings() {
  const { [SETTINGS_KEY]: s = {} } = await chrome.storage.local.get(SETTINGS_KEY);
  return {
    apiUrl: (s.apiUrl || DEFAULT_API_URL).replace(/\/$/, ""),
    apiKey: s.apiKey || "",
    userName: s.userName || "",
    returnToSend: s.returnToSend !== false,
  };
}

async function saveSettings(s) {
  await chrome.storage.local.set({ [SETTINGS_KEY]: s });
}

// ---------- vertical intros (API-fetched, locally cached) ----------
// The extension fetches fresh intros from GET /intros on startup and caches
// them in chrome.storage.local for INTROS_TTL_MS. Falls back to the hardcoded
// VERTICAL_INTROS constant when the API is unreachable or the cache is empty.

const INTROS_CACHE_KEY = "sra.intros.v2"; // bump to bust old cache on reload
const INTROS_TTL_MS    = 12 * 60 * 60 * 1000; // 12 hours

// activeIntros starts as the hardcoded defaults and is swapped out once the
// API fetch resolves. submit() reads this variable at call time, so any query
// submitted after the fetch resolves automatically gets the corpus-fresh intros.
let activeIntros = { ...VERTICAL_INTROS };

async function loadVerticalIntros(settings) {
  // 1. Check local cache first — avoids a network call if still fresh.
  try {
    const { [INTROS_CACHE_KEY]: cached } = await chrome.storage.local.get(INTROS_CACHE_KEY);
    if (cached && cached.ts && (Date.now() - cached.ts) < INTROS_TTL_MS && cached.data) {
      activeIntros = { ...VERTICAL_INTROS, ...cached.data };
      console.debug("[sra] vertical intros: loaded from local cache");
      return;
    }
  } catch (e) {
    console.debug("[sra] vertical intros: local cache read failed:", e.message);
  }

  // 2. Fetch from the API.
  try {
    const res = await fetch(`${settings.apiUrl}/intros`, {
      headers: { "Accept": "application/json" },
    });
    if (res.ok) {
      const body = await res.json();
      const fresh = body.intros || {};
      if (Object.keys(fresh).length > 0) {
        activeIntros = { ...VERTICAL_INTROS, ...fresh };
        await chrome.storage.local.set({ [INTROS_CACHE_KEY]: { ts: Date.now(), data: fresh } });
        console.debug("[sra] vertical intros: refreshed from API");
      }
    } else {
      console.debug("[sra] vertical intros: API returned", res.status, "— keeping defaults");
    }
  } catch (e) {
    console.debug("[sra] vertical intros: fetch failed (offline?):", e.message);
  }
}

// ---------- partner config (API-fetched, locally cached) ----------
// loadPartnerConfig() fetches GET /config once at startup and caches the
// result for 24 h.  The response { default_vertical } tells the extension
// whether to show the focused 3-chip partner picker or the full industry
// picker.  Falls back to null (full picker) if the key is unset, the API
// is unreachable, or the partner is not mapped to a vertical.

const PARTNER_CONFIG_KEY     = "sra.partnerConfig.v2"; // v2: default_verticals array
const PARTNER_CONFIG_TTL_MS  = 24 * 60 * 60 * 1000; // 24 hours

async function loadPartnerConfig(settings) {
  // Only attempt if a partner key is configured — anonymous partners always
  // get null, so we can skip the round-trip.
  if (!settings.apiKey) return null;

  // 1. Check local cache first.
  try {
    const { [PARTNER_CONFIG_KEY]: cached } = await chrome.storage.local.get(PARTNER_CONFIG_KEY);
    if (
      cached &&
      cached.ts &&
      cached.apiKey === settings.apiKey &&
      (Date.now() - cached.ts) < PARTNER_CONFIG_TTL_MS &&
      cached.data
    ) {
      console.debug("[sra] partner config: loaded from local cache");
      return cached.data;
    }
  } catch (e) {
    console.debug("[sra] partner config: local cache read failed:", e.message);
  }

  // 2. Fetch from API (3-second timeout so a slow cold-start doesn't block init).
  try {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 3000);
    const res = await fetch(`${settings.apiUrl}/config`, {
      headers: headersFor(settings),
      signal: controller.signal,
    });
    clearTimeout(tid);
    if (res.ok) {
      const data = await res.json();
      await chrome.storage.local.set({
        [PARTNER_CONFIG_KEY]: { ts: Date.now(), apiKey: settings.apiKey, data },
      });
      console.debug("[sra] partner config: fetched from API:", data);
      return data;
    }
    console.debug("[sra] partner config: API returned", res.status, "— no partner vertical");
  } catch (e) {
    console.debug("[sra] partner config: fetch failed:", e.message);
  }
  return null;
}

// ---------- session (multi-turn) ----------

function newSessionId() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return "sra-" + Math.random().toString(36).slice(2) + Date.now().toString(36);
}

async function loadSession() {
  const { [SESSION_KEY]: s } = await chrome.storage.local.get(SESSION_KEY);
  if (s && s.sessionId && Array.isArray(s.turns)) return s;
  const fresh = { sessionId: newSessionId(), turns: [] };
  await chrome.storage.local.set({ [SESSION_KEY]: fresh });
  return fresh;
}

async function saveSession(session) {
  await chrome.storage.local.set({ [SESSION_KEY]: session });
}

async function resetSession() {
  const fresh = { sessionId: newSessionId(), turns: [] };
  await chrome.storage.local.set({ [SESSION_KEY]: fresh });
  return fresh;
}

function historyForSend(session) {
  // Map to the minimal {role, content} shape the API expects, and send only
  // the last MAX_HISTORY_TURNS turns. The server truncates again as a safety.
  const trimmed = session.turns.slice(-MAX_HISTORY_TURNS);
  return trimmed.map((t) => ({ role: t.role, content: t.content }));
}

function userTurnCount(session) {
  return session.turns.filter((t) => t.role === "user").length;
}

// ---------- API ----------

function headersFor(settings) {
  const h = { "Content-Type": "application/json", Accept: "application/json" };
  if (settings.apiKey) h["X-Partner-Key"] = settings.apiKey;
  return h;
}

async function health(settings) {
  const res = await fetch(`${settings.apiUrl}/health`, { headers: headersFor(settings) });
  if (!res.ok) throw new Error(`health ${res.status}`);
  return res.json();
}

async function postQuery(settings, payload) {
  const res = await fetch(`${settings.apiUrl}/query`, {
    method: "POST",
    headers: headersFor(settings),
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { detail: text }; }
  if (!res.ok) {
    const retryAfter = res.headers.get("Retry-After");
    const msg = body?.detail || `HTTP ${res.status}`;
    const e = new Error(msg);
    e.status = res.status;
    if (retryAfter) e.retryAfter = retryAfter;
    throw e;
  }
  return body;
}

async function streamQuery(settings, payload, onDelta) {
  // NDJSON streaming path. Server emits one JSON object per line:
  //   {"type":"delta","text":"..."}  while Claude is generating
  //   {"type":"final","answer":...,"citations":[...],...}  once complete
  //   {"type":"error","message":"..."}  on server-side failure
  const res = await fetch(`${settings.apiUrl}/query`, {
    method: "POST",
    headers: { ...headersFor(settings), Accept: "application/x-ndjson" },
    body: JSON.stringify(payload),
  });
  if (!res.ok || !res.body) {
    const text = await res.text().catch(() => "");
    let parsed;
    try { parsed = JSON.parse(text); } catch { parsed = { detail: text }; }
    const retryAfter = res.headers.get("Retry-After");
    const e = new Error(parsed?.detail || `HTTP ${res.status}`);
    e.status = res.status;
    if (retryAfter) e.retryAfter = retryAfter;
    throw e;
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let finalEvent = null;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      let ev;
      try { ev = JSON.parse(line); } catch { continue; }
      if (ev.type === "delta") {
        onDelta(ev.text || "");
      } else if (ev.type === "final") {
        finalEvent = ev;
      } else if (ev.type === "error") {
        throw new Error(ev.message || "stream error");
      }
    }
  }
  if (!finalEvent) throw new Error("stream ended without final event");
  return finalEvent;
}

async function postFeedback(settings, payload) {
  const res = await fetch(`${settings.apiUrl}/feedback`, {
    method: "POST",
    headers: headersFor(settings),
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(body || `feedback ${res.status}`);
  }
  return res.json().catch(() => ({ ok: true }));
}

// ---------- rendering ----------

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

// Known all-caps abbreviations to preserve when title-casing URL path slugs.
// Extend this list as Synexis expands into new terminology or markets.
const SLUG_ACRONYMS = new Set([
  "DHP", "PPB", "HAI", "RTE", "FDA", "EPA", "CDC", "WHO", "USDA",
  "HACCP", "HVAC", "UV", "AC", "UK", "US",
]);

// Title-case a slug that has already had hyphens/underscores replaced with spaces.
// Preserves known all-caps abbreviations; capitalises the first letter of everything else.
function slugToTitle(slug) {
  return slug
    .split(" ")
    .map((w) => {
      if (!w) return w;
      return SLUG_ACRONYMS.has(w.toUpperCase())
        ? w.toUpperCase()
        : w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join(" ");
}

// Clean up raw file_path values for display.
// Handles two formats from the corpus:
//   - SharePoint files:  "sharepoint/{folder}/{filename.ext}"
//   - Web pages:         full URLs like "https://synexis.com/products/dhp-devices/"
// Extend the locale map and SLUG_ACRONYMS as new markets / terminology are added.
function prettyPath(filePath) {
  if (!filePath) return "?";

  // Web URL: use URL() to handle trailing slashes, then build a readable breadcrumb
  // from the last 1–2 non-empty path segments (e.g. "Products › DHP Devices").
  if (/^https?:\/\//.test(filePath)) {
    try {
      const url = new URL(filePath);
      const segments = url.pathname.split("/").filter(Boolean); // filter removes empty from trailing slash
      if (segments.length === 0) {
        return url.hostname.replace(/^www\./, ""); // root URL → just the domain
      }
      const crumbs = segments
        .slice(-2)                                      // up to last 2 segments for context
        .map((s) => slugToTitle(s.replace(/[-_]/g, " ").replace(/\s{2,}/g, " ").trim()));
      return crumbs.join(" › ") || url.hostname.replace(/^www\./, "");
    } catch {
      // Malformed URL — fall through to basename logic below
    }
  }

  // File / SharePoint path ("sharepoint/{folder}/{filename.ext}" or plain path).
  // Use filter(Boolean) so a trailing slash never produces an empty last segment.
  const parts = filePath.split("/").filter(Boolean);
  const basename = parts.pop() || filePath;
  let name = basename.replace(/\.[^.]+$/, ""); // strip extension

  // Locale suffixes → readable labels (add new entries here as markets expand)
  name = name.replace(/_ESP\b/gi, " (Spanish)");
  name = name.replace(/_ENG\b/gi, ""); // strip redundant English suffix
  // Strip trailing production suffixes (FINAL, PROOF) and anything that follows.
  // Requires a preceding underscore/space so "semifinal" is not affected.
  name = name.replace(/[_ ]+(?:FINAL|PROOF)\b.*$/i, "");
  // Strip trailing 4-digit date codes (_MMYY)
  name = name.replace(/_\d{4}$/, "");
  // Underscores → spaces, collapse whitespace, trim
  const result = name.replace(/_/g, " ").replace(/\s{2,}/g, " ").trim();

  return result || basename.replace(/\.[^.]+$/, "").trim() || filePath || "?";
}

// Return the best linkable URL for a citation: prefer share_url (HubSpot CDN),
// fall back to file_path when it is itself a web URL (web-crawl chunks).
function citationUrl(citation) {
  if (!citation) return "";
  if (citation.share_url) return citation.share_url;
  const fp = citation.file_path || "";
  return /^https?:\/\//.test(fp) ? fp : "";
}

function renderBadge(n, citation, turnKey) {
  if (!citation) return `[${n}]`;
  const path = escapeHtml(prettyPath(citation.file_path || ""));
  const page = citation.page_or_slide;
  const pageStr = page !== undefined && page !== null && page !== ""
    ? ` — page/slide ${escapeHtml(String(page))}`
    : "";
  const snippet = escapeHtml(citation.snippet || "");
  const shareUrl = citationUrl(citation);
  // Use <span> not <a> here — cite-badge is already an <a>, so nesting anchors is invalid HTML.
  const pathEl = shareUrl
    ? `<span class="tt-path tt-path-link" role="link" tabindex="0" data-href="${escapeHtml(shareUrl)}">[${n}] ${path}${pageStr}</span>`
    : `<span class="tt-path">[${n}] ${path}${pageStr}</span>`;
  return (
    `<a class="cite-badge" href="#src-${turnKey}-${n}" data-n="${n}" tabindex="0">${n}` +
      `<span class="cite-tooltip">` +
        pathEl +
        (snippet ? `<span class="tt-snippet">${snippet}</span>` : "") +
        (shareUrl ? `<a class="tt-link" href="${escapeHtml(shareUrl)}" target="_blank" rel="noopener">View ↗</a>` : "") +
      `</span>` +
    `</a>`
  );
}

function inlineTransforms(html, citeMap, turnKey) {
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/\*([^*\n]+)\*/g, "<em>$1</em>");
  html = html.replace(/(https?:\/\/[^\s<]+)/g, '<a href="$1" target="_blank" rel="noopener">$1</a>');
  html = html.replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, (match, inner) => {
    const nums = inner.split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => !isNaN(n));
    if (nums.length === 0) return match;
    return nums.map((n) => renderBadge(n, citeMap.get(n), turnKey)).join('<span class="cite-sep">, </span>');
  });
  return html;
}

function isTableSeparatorRow(line) {
  const t = line.trim();
  if (!t || !t.includes("-") || !t.includes("|")) return false;
  return /^[\s|\-:]+$/.test(t);
}

function parseTableRow(line) {
  // "| a | b | c |" → ["a", "b", "c"]. Also tolerates the no-leading/trailing-pipe form.
  return line.trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim());
}

function renderTableBlock(headerLine, bodyLines, citeMap, turnKey) {
  const headers = parseTableRow(headerLine);
  const th = headers.map((h) => `<th>${inlineTransforms(h, citeMap, turnKey)}</th>`).join("");
  const trs = bodyLines
    .map(parseTableRow)
    .map((cells) => {
      // Pad/trim so every row has the same column count as the header.
      while (cells.length < headers.length) cells.push("");
      cells.length = headers.length;
      return `<tr>${cells.map((c) => `<td>${inlineTransforms(c, citeMap, turnKey)}</td>`).join("")}</tr>`;
    })
    .join("");
  return `<div class="table-wrap"><table><thead><tr>${th}</tr></thead><tbody>${trs}</tbody></table></div>`;
}

// Render an array of pre-escaped lines to HTML, handling tables and inline transforms.
function renderLines(lines, citeMap, turnKey) {
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const next = lines[i + 1];
    // ### sub-headings inside section bodies — styled as H3.
    // (## headings are consumed by renderAnswer/renderProgressiveAccordion above this level.)
    if (/^###\s/.test(line)) {
      const heading = line.replace(/^###\s+/, "");
      out.push(`<h3>${inlineTransforms(heading, citeMap, turnKey)}</h3>`);
      i++;
      continue;
    }
    if (/\|/.test(line) && next !== undefined && isTableSeparatorRow(next)) {
      const header = line;
      i += 2;
      const body = [];
      while (i < lines.length && /\|/.test(lines[i]) && !isTableSeparatorRow(lines[i])) {
        body.push(lines[i]);
        i++;
      }
      out.push(renderTableBlock(header, body, citeMap, turnKey));
      continue;
    }
    out.push(inlineTransforms(line, citeMap, turnKey));
    i++;
  }
  return out.join("\n");
}


function renderAnswer(answer, citations, turnKey, fallbackPreamble) {
  const citeMap = new Map();
  for (const c of citations || []) citeMap.set(c.n, c);

  // Escape once up front.
  const lines = escapeHtml(answer).split("\n");

  // If no ## headings, render flat as before.
  if (!lines.some(l => /^##\s/.test(l))) {
    return renderLines(lines, citeMap, turnKey);
  }

  // Split into preamble (before first ##) and sections.
  const firstHd = lines.findIndex(l => /^##\s/.test(l));
  const preamble = lines.slice(0, firstHd);
  const sections = [];
  let cur = null;
  for (const line of lines.slice(firstHd)) {
    if (/^##\s/.test(line)) {
      if (cur) sections.push(cur);
      cur = { heading: line.replace(/^##\s+/, ""), lines: [] };
    } else {
      if (cur) cur.lines.push(line);
    }
  }
  if (cur) sections.push(cur);

  const parts = [];

  // Preamble — prefer the LLM-generated preamble; fall back to the placeholder
  // intro text (the same text shown during streaming) so it's never lost when
  // the model skips straight to ## sections.
  if (preamble.some(l => l.trim())) {
    parts.push(`<div class="ans-preamble">${renderLines(preamble, citeMap, turnKey)}</div>`);
  } else if (fallbackPreamble) {
    const fbLines = escapeHtml(fallbackPreamble).split("\n");
    parts.push(`<div class="ans-preamble">${renderLines(fbLines, citeMap, turnKey)}</div>`);
  }

  // Expand all bar — shown when there are multiple sections
  if (sections.length > 1) {
    parts.push(`<div class="ans-expand-bar"><button class="ans-expand-all">Expand all</button></div>`);
  }

  // Sections — all collapsed by default
  sections.forEach((s, idx) => {
    const bodyId = `sec-${turnKey}-${idx}`;
    parts.push(
      `<div class="ans-section">` +
        `<button class="ans-section-hd" aria-expanded="false" data-body="${bodyId}">` +
          `<span class="ans-chevron">&#9654;</span>` +
          `<span>${inlineTransforms(s.heading, citeMap, turnKey)}</span>` +
        `</button>` +
        `<div class="ans-section-body" id="${bodyId}" hidden>` +
          renderLines(s.lines, citeMap, turnKey) +
        `</div>` +
      `</div>`
    );
  });

  return parts.join("");
}

function renderCitations(citations, turnKey) {
  if (!citations || citations.length === 0) return "";
  const bodyId = `cit-${turnKey}`;
  const items = citations
    .map((c) => {
      const pageVal = c.page_or_slide;
      const page = pageVal !== undefined && pageVal !== null && pageVal !== ""
        ? ` — page/slide ${escapeHtml(String(pageVal))}`
        : "";
      const link = citationUrl(c)
        ? ` <span class="cite-link" role="link" tabindex="0" data-href="${escapeHtml(citationUrl(c))}">View ↗</span>`
        : "";
      return `<div class="cite" id="src-${turnKey}-${c.n}"><span class="n">[${c.n}]</span> <span class="path">${escapeHtml(prettyPath(c.file_path || ""))}</span><span class="page">${page}</span>${link}</div>`;
    })
    .join("");
  // Render as a collapsed accordion section, consistent with answer sections.
  return (
    `<div class="ans-section ans-section-sources">` +
      `<button class="ans-section-hd" aria-expanded="false" data-body="${bodyId}">` +
        `<span class="ans-chevron">&#9654;</span>` +
        `<span>Sources</span>` +
      `</button>` +
      `<div class="ans-section-body" id="${bodyId}" hidden>${items}</div>` +
    `</div>`
  );
}

// Show and position tooltip using fixed coordinates to escape scroll container clipping.
document.addEventListener("mouseover", (ev) => {
  const badge = ev.target && ev.target.closest && ev.target.closest(".cite-badge");
  if (!badge) return;
  const tooltip = badge.querySelector(".cite-tooltip");
  if (!tooltip) return;

  // Show invisible so we can measure natural dimensions before positioning.
  tooltip.style.visibility = "hidden";
  tooltip.style.display = "block";
  tooltip.classList.remove("above", "below");

  const br = badge.getBoundingClientRect();
  const tr = tooltip.getBoundingClientRect();
  const margin = 8;
  const gap = 6;
  const vw = document.documentElement.clientWidth;

  // Decide whether to show above or below the badge.
  const fitsAbove = (br.top - tr.height - gap) >= margin;
  let top;
  if (fitsAbove) {
    top = br.top - tr.height - gap;
    tooltip.classList.add("above");
  } else {
    top = br.bottom + gap;
    tooltip.classList.add("below");
  }

  // Centre horizontally on badge, clamped to viewport edges.
  let left = br.left + br.width / 2 - tr.width / 2;
  left = Math.max(margin, Math.min(left, vw - tr.width - margin));

  // Shift the arrow to keep it pointing at the badge even after horizontal clamping.
  const idealLeft = br.left + br.width / 2 - tr.width / 2;
  const shift = left - idealLeft;
  tooltip.style.setProperty("--arrow-offset", `${-shift}px`);

  tooltip.style.top = top + "px";
  tooltip.style.left = left + "px";
  tooltip.style.transform = "none";
  tooltip.style.visibility = "";
});

// Hide tooltip when pointer leaves the badge.
document.addEventListener("mouseout", (ev) => {
  const badge = ev.target && ev.target.closest && ev.target.closest(".cite-badge");
  if (!badge) return;
  // Only hide if leaving the badge entirely (not moving to a child element).
  if (badge.contains(ev.relatedTarget)) return;
  const tooltip = badge.querySelector(".cite-tooltip");
  if (!tooltip) return;
  tooltip.style.display = "none";
});

// Handle keyboard focus/blur for keyboard navigation support.
document.addEventListener("focusin", (ev) => {
  const badge = ev.target && ev.target.closest && ev.target.closest(".cite-badge");
  if (!badge) return;
  badge.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
});

document.addEventListener("focusout", (ev) => {
  const badge = ev.target && ev.target.closest && ev.target.closest(".cite-badge");
  if (!badge) return;
  const tooltip = badge.querySelector(".cite-tooltip");
  if (!tooltip) return;
  tooltip.style.display = "none";
});

// Open document link when clicking/entering on tt-path-link or cite-link spans.
// Both use data-href + window.open so behaviour is consistent across tooltip and Sources.
document.addEventListener("click", (ev) => {
  const el = ev.target && ev.target.closest && (
    ev.target.closest(".tt-path-link") || ev.target.closest(".cite-link")
  );
  if (!el) return;
  ev.preventDefault(); ev.stopPropagation();
  const href = el.dataset.href;
  if (href) chrome.runtime.sendMessage({ type: "open_tab", url: href });
});
document.addEventListener("keydown", (ev) => {
  if (ev.key !== "Enter" && ev.key !== " ") return;
  const el = ev.target && ev.target.closest && (
    ev.target.closest(".tt-path-link") || ev.target.closest(".cite-link")
  );
  if (!el) return;
  ev.preventDefault();
  const href = el.dataset.href;
  if (href) chrome.runtime.sendMessage({ type: "open_tab", url: href });
});

// --- Accordion: section toggle ---
document.addEventListener("click", (ev) => {
  const hd = ev.target && ev.target.closest && ev.target.closest(".ans-section-hd");
  if (!hd) return;
  const body = document.getElementById(hd.dataset.body);
  if (!body) return;
  const expanding = hd.getAttribute("aria-expanded") !== "true";
  hd.setAttribute("aria-expanded", String(expanding));
  body.hidden = !expanding;
  // Keep expand-all label in sync
  const bar = hd.closest(".turn, .ans-section")?.closest(".turn");
  syncExpandAll(bar);
});

// --- Accordion: expand all / collapse all ---
document.addEventListener("click", (ev) => {
  const btn = ev.target && ev.target.closest && ev.target.closest(".ans-expand-all");
  if (!btn) return;
  const turn = btn.closest(".turn");
  if (!turn) return;
  const collapse = btn.textContent.trim() === "Collapse all";
  turn.querySelectorAll(".ans-section-hd").forEach(hd => {
    const body = document.getElementById(hd.dataset.body);
    if (!body) return;
    hd.setAttribute("aria-expanded", String(!collapse));
    body.hidden = collapse;
  });
  btn.textContent = collapse ? "Expand all" : "Collapse all";
});

function syncExpandAll(turnEl) {
  if (!turnEl) return;
  const btn = turnEl.querySelector(".ans-expand-all");
  if (!btn) return;
  const hds = turnEl.querySelectorAll(".ans-section-hd");
  const allOpen = Array.from(hds).every(h => h.getAttribute("aria-expanded") === "true");
  btn.textContent = allOpen ? "Collapse all" : "Expand all";
}

function addTurnEl(query, state) {
  const history = $("history");
  $("empty").style.display = "none";
  const div = document.createElement("div");
  div.className = "turn";
  div.innerHTML = `<div class="q">${escapeHtml(query)}</div><div class="a"></div><div class="meta"></div>`;
  if (state) div.querySelector(".meta").textContent = state;
  history.appendChild(div);
  return div;
}

function finalizeTurnEl(turnEl, answer, citations, meta, turnKey, fallbackPreamble) {
  // Citations accordion is appended inside .a so it integrates naturally with
  // the answer sections and the expand-all / toggle handlers.
  const cits = renderCitations(citations || [], turnKey);
  turnEl.querySelector(".a").innerHTML =
    renderAnswer(answer || "", citations || [], turnKey, fallbackPreamble) + cits;
  turnEl.querySelector(".meta").textContent = meta || "";
}

function attachFeedbackControls(turnEl, ctx) {
  // Two-click flow: pick a rating (👍 or 👎) → optionally add free text →
  // Submit. Either rating shows the textarea with a rating-specific prompt.
  // Additive feedback ("could also have mentioned X") is as useful for corpus
  // gap ID as corrections are, so we collect it on both ratings.
  const fb = document.createElement("div");
  fb.className = "feedback";
  fb.innerHTML =
    `<button class="fb-btn fb-up" title="Helpful" type="button">👍</button>` +
    `<button class="fb-btn fb-down" title="Not helpful" type="button">👎</button>` +
    `<span class="fb-label"></span>` +
    `<span class="fb-status"></span>`;
  const form = document.createElement("div");
  form.className = "feedback-form";
  form.innerHTML =
    `<textarea placeholder=""></textarea>` +
    `<button class="fb-submit" type="button">Submit</button>`;

  turnEl.appendChild(fb);
  turnEl.appendChild(form);

  const up = fb.querySelector(".fb-up");
  const down = fb.querySelector(".fb-down");
  const status = fb.querySelector(".fb-status");
  const textarea = form.querySelector("textarea");
  const submit = form.querySelector(".fb-submit");

  let selectedRating = null;   // "up" | "down" | null
  let submitted = false;

  function selectRating(rating) {
    if (submitted) return;
    selectedRating = rating;
    // Visual state: selected button gets .active; the other loses it.
    up.classList.toggle("active", rating === "up");
    down.classList.toggle("active", rating === "down");
    // Rating-specific placeholder, per briefing.
    textarea.placeholder = rating === "up"
      ? "Anything to add? (optional)"
      : "Where did it miss? (optional)";
    form.classList.add("open");
    textarea.focus();
  }

  async function sendRating() {
    if (submitted || !selectedRating) return;
    submitted = true;
    up.disabled = true;
    down.disabled = true;
    submit.disabled = true;
    const freeText = textarea.value.trim();
    status.textContent = "sending…";
    const settingsNow = await loadSettings();
    try {
      await postFeedback(settingsNow, {
        rating: selectedRating,
        query: ctx.query,
        answer: ctx.answer,
        citations: ctx.citations,
        feedback_text: freeText || null,
        session_id: ctx.sessionId,
        turn_id: ctx.turnId,
        user: settingsNow.userName || null,
      });
      status.textContent = selectedRating === "up" ? "thanks!" : "thanks — noted.";
      // Selected state persists on the chosen button after submit.
      form.classList.remove("open");
    } catch (e) {
      status.textContent = `failed — ${e.message}`;
      submitted = false;
      up.disabled = false;
      down.disabled = false;
      submit.disabled = false;
    }
  }

  up.addEventListener("click", () => selectRating("up"));
  down.addEventListener("click", () => selectRating("down"));
  submit.addEventListener("click", () => sendRating());
}

function failTurnEl(turnEl, err) {
  const retry = err.retryAfter ? ` (retry after ${err.retryAfter}s)` : "";
  const msg = `Error: ${err.message}${retry}`;
  turnEl.querySelector(".a").innerHTML = `<div class="error">${msg}</div>`;
  turnEl.querySelector(".meta").textContent = err.status ? `HTTP ${err.status}` : "";
}

function formatMeta(serverMs, wallMs, nCitations, ctxUtil) {
  const timing = typeof serverMs === "number" && !Number.isNaN(serverMs)
    ? `server ${serverMs} ms  ·  wall ${wallMs} ms`
    : `${wallMs} ms`;
  const citeLine = `${nCitations} citation${nCitations === 1 ? "" : "s"}`;
  const util = typeof ctxUtil === "number"
    ? `  ·  ctx ${ctxUtil.toFixed(1)}%`
    : "";
  return `${timing}  ·  ${citeLine}${util}`;
}

function renderHistoryFromSession(session) {
  const historyEl = $("history");
  historyEl.innerHTML = "";
  if (session.turns.length === 0) {
    $("empty").style.display = "";
    updateTruncationIndicator(session);
    return;
  }
  $("empty").style.display = "none";
  // Walk turns in pairs (user → assistant). If the last user has no assistant
  // reply (mid-flight), skip rendering it here; the submit flow handles it.
  let turnKey = 0;
  for (let i = 0; i < session.turns.length; i++) {
    const t = session.turns[i];
    if (t.role !== "user") continue;
    const userTurn = t;
    const assistantTurn = session.turns[i + 1];
    if (!assistantTurn || assistantTurn.role !== "assistant") continue;
    const turnEl = addTurnEl(userTurn.content, "");
    finalizeTurnEl(
      turnEl,
      assistantTurn.content,
      assistantTurn.citations || [],
      formatMeta(
        assistantTurn.query_time_ms,
        assistantTurn.query_time_ms || 0,
        (assistantTurn.citations || []).length,
        assistantTurn.context_utilization,
      ),
      turnKey,
    );
    turnKey++;
    i += 1;
  }
  updateTruncationIndicator(session);
}

function updateTruncationIndicator(session) {
  const el = $("truncationNote");
  if (!el) return;
  const total = session.turns.length;
  if (total > MAX_HISTORY_TURNS) {
    const dropped = total - MAX_HISTORY_TURNS;
    el.textContent = `Earlier context summarized — ${dropped} prior turn${dropped === 1 ? "" : "s"} not sent with this query.`;
    el.style.display = "block";
  } else {
    el.style.display = "none";
    el.textContent = "";
  }
}

// ---------- status dot ----------

async function refreshStatus(settings) {
  const dot = $("statusDot");
  try {
    const h = await health(settings);
    dot.style.background = h.auth_configured ? "#0b5fff" : "#22c55e";
    dot.title = `API OK · model=${h.model} · index=${h.index} · auth=${h.auth_configured}`;
  } catch {
    dot.style.background = "#dc2626";
    dot.title = "API unreachable";
  }
}

// ---------- wiring ----------

async function ensureUserName(settings) {
  // First-launch prompt: if no name yet, gate usage on entering one.
  if (settings.userName) return settings;
  const dlg = $("namePromptDialog");
  const input = $("namePromptInput");
  if (!dlg || !input) return settings;
  input.value = "";
  try { dlg.showModal(); } catch { /* older browsers */ }
  const name = await new Promise((resolve) => {
    const onSubmit = (ev) => {
      ev.preventDefault();
      const v = input.value.trim();
      if (!v) return;
      dlg.close();
      resolve(v);
    };
    dlg.querySelector("form").addEventListener("submit", onSubmit, { once: true });
  });
  const next = { ...settings, userName: name };
  await saveSettings(next);
  return next;
}

function updatePlaceholder(returnToSend) {
  $("queryInput").placeholder = returnToSend
    ? "Ask a question…  (Shift+Enter for new line)"
    : "Ask a question…  (Cmd/Ctrl+Enter to send)";
}

async function init() {
  let settings = await loadSettings();
  settings = await ensureUserName(settings);
  let session = await loadSession();

  $("userName").value = settings.userName;
  $("apiUrl").value = settings.apiUrl;
  $("apiKey").value = settings.apiKey;
  $("returnToSend").checked = settings.returnToSend;
  updatePlaceholder(settings.returnToSend);

  // Non-blocking: fetch corpus-fresh vertical intros from the API.
  // activeIntros is updated in the background; any submit() call after this
  // resolves automatically picks up the latest intros.
  loadVerticalIntros(settings);

  // Blocking: fetch partner config so we know which picker to show before rendering.
  const partnerConfig = await loadPartnerConfig(settings);
  // Prefer the array field (default_verticals); fall back to single-string field for
  // older cached responses.  partnerVerticals is null when no vertical is configured.
  const partnerVerticals = (partnerConfig && partnerConfig.default_verticals && partnerConfig.default_verticals.length > 0)
    ? partnerConfig.default_verticals
    : (partnerConfig && partnerConfig.default_vertical)
      ? [partnerConfig.default_vertical]
      : null;
  const partnerVertical = partnerVerticals ? partnerVerticals[0] : null;  // backward-compat alias

  $("returnToSend").addEventListener("change", () => {
    updatePlaceholder($("returnToSend").checked);
  });

  renderHistoryFromSession(session);

  // Default to the partner vertical so the placeholder is correct for partner sessions.
  let _selectedIndustry = partnerVertical || "";

  function showIndustryPicker() {
    $("empty").style.display = "none";
    $("partner-picker").style.display = "none";
    $("intent-picker").style.display = "none";
    $("industry-picker").style.display = "";
  }

  function showIntentPicker(industry) {
    _selectedIndustry = industry;
    $("industry-picker").style.display = "none";
    $("partner-picker").style.display = "none";
    $("intent-prompt").textContent = industry
      ? `Great. What can I help you with for ${industry}?`
      : "Great. What can I help you with?";
    $("intent-picker").style.display = "";
  }

  // Partner picker — focused chip flow when the partner key maps to one or more verticals.
  // Chips: one [<Vertical> overview] per vertical + [Specific question] + [Explore another industry]
  // Adding a new vertical (e.g. Poultry) requires only an env-var update on the backend —
  // chips are built dynamically from the verticals array returned by /config.
  function showPartnerPicker(verticals) {
    const vList = Array.isArray(verticals) ? verticals : [verticals];
    const primaryVertical = vList[0] || "";
    _selectedIndustry = primaryVertical;
    $("empty").style.display = "none";
    $("industry-picker").style.display = "none";
    $("intent-picker").style.display = "none";
    $("partner-prompt").textContent = vList.length > 1
      ? "What can I help you with today?"
      : `What can I help you with for ${primaryVertical} today?`;
    // One overview chip per vertical, then the shared action chips.
    const overviewChips = vList
      .map(v => `<button class="chip" data-partner-intent="overview" data-industry="${escapeHtml(v)}">${escapeHtml(v)} overview</button>`)
      .join("");
    $("partner-chips").innerHTML =
      overviewChips +
      `<button class="chip" data-partner-intent="question" data-industry="${escapeHtml(primaryVertical)}">I have a specific question</button>` +
      `<button class="chip chip-dismiss" data-partner-intent="other">Explore another industry</button>`;
    $("partner-picker").style.display = "";
  }

  // Show whichever picker is appropriate given whether a partner vertical is set.
  function showInitialPicker() {
    if (partnerVerticals) {
      showPartnerPicker(partnerVerticals);
    } else {
      showIndustryPicker();
    }
  }

  function hidePickers() {
    $("industry-picker").style.display = "none";
    $("intent-picker").style.display = "none";
    $("partner-picker").style.display = "none";
  }

  if (session.turns.length === 0) showInitialPicker();

  document.querySelectorAll("#industry-picker .chip").forEach((btn) => {
    btn.addEventListener("click", () => {
      const industry = btn.dataset.industry || "";
      if (industry) {
        showIntentPicker(industry);
      } else {
        hidePickers();
        $("queryInput").focus();
      }
    });
  });

  $("intent-overview").addEventListener("click", () => {
    hidePickers();
    const q = _selectedIndustry
      ? `Give me an overview of Synexis products and solutions for ${_selectedIndustry}.`
      : "Give me a general overview of Synexis products and solutions.";
    $("queryInput").value = q;
    submit();
  });

  $("intent-question").addEventListener("click", () => {
    hidePickers();
    $("queryInput").focus();
  });

  // Partner picker chip handler — event-delegated because innerHTML is rebuilt
  // each time showPartnerPicker() is called.
  $("partner-chips").addEventListener("click", (ev) => {
    const btn = ev.target.closest("[data-partner-intent]");
    if (!btn) return;
    const intent   = btn.dataset.partnerIntent;
    const industry = btn.dataset.industry || _selectedIndustry;
    if (intent === "overview") {
      _selectedIndustry = industry;
      hidePickers();
      const q = `Give me an overview of Synexis products and solutions for ${industry}.`;
      $("queryInput").value = q;
      submit();
    } else if (intent === "question") {
      _selectedIndustry = industry;
      hidePickers();
      $("queryInput").focus();
    } else if (intent === "other") {
      // Let them pick from the full industry list; their choice updates _selectedIndustry.
      showIndustryPicker();
    }
  });

  $("toggleSettings").addEventListener("click", () => {
    $("settings").classList.toggle("open");
  });

  $("newConversation").addEventListener("click", async () => {
    session = await resetSession();
    $("history").innerHTML = "";
    $("empty").style.display = "none";
    _selectedIndustry = partnerVertical || "";
    showInitialPicker();
    updateTruncationIndicator(session);
  });

  $("saveSettings").addEventListener("click", async () => {
    const next = {
      apiUrl: $("apiUrl").value.trim().replace(/\/$/, "") || DEFAULT_API_URL,
      apiKey: $("apiKey").value.trim(),
      userName: $("userName").value.trim(),
      returnToSend: $("returnToSend").checked,
    };
    await saveSettings(next);
    $("settingsStatus").textContent = "Saved.";
    refreshStatus(next);
  });

  $("testConnection").addEventListener("click", async () => {
    const s = {
      apiUrl: $("apiUrl").value.trim().replace(/\/$/, "") || DEFAULT_API_URL,
      apiKey: $("apiKey").value.trim(),
    };
    $("settingsStatus").textContent = "Testing…";
    try {
      const h = await health(s);
      $("settingsStatus").textContent =
        `OK — model=${h.model}, index=${h.index}, auth_configured=${h.auth_configured}`;
    } catch (e) {
      $("settingsStatus").textContent = `FAILED — ${e.message}`;
    }
  });

  $("ask").addEventListener("click", submit);
  $("queryInput").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter" && !ev.shiftKey) {
      if ($("returnToSend").checked || ev.metaKey || ev.ctrlKey) {
        ev.preventDefault();
        submit();
      }
    }
  });

  refreshStatus(settings);

  async function submit() {
    const q = $("queryInput").value.trim();
    if (!q) return;

    if (MAINTENANCE_MODE) {
      const turnEl = addTurnEl(q, "");
      turnEl.querySelector(".a").textContent = MAINTENANCE_MESSAGE;
      $("queryInput").value = "";
      return;
    }

    const settingsNow = await loadSettings();
    const turnEl = addTurnEl(q, "…");
    turnEl.scrollIntoView({ behavior: "smooth", block: "start" });
    $("queryInput").value = "";
    $("ask").disabled = true;
    $("spinner").classList.add("on");
    $("hint").textContent = "";
    const started = Date.now();

    const turnId = userTurnCount(session);
    const turnKey = turnId;
    const history = historyForSend(session);

    const payload = {
      query: q,
      history,
      session_id: session.sessionId,
      turn_id: turnId,
      user: settingsNow.userName || null,
    };

    const answerEl = turnEl.querySelector(".a");
    // Show the short, controlled VERTICAL_INTROS placeholder instantly.
    // This stays visible for the full loading phase — we don't stream the LLM
    // preamble into the DOM for accordion responses, which eliminates the flash
    // caused by replacing streamed plain text with the final HTML layout.
    answerEl.textContent =
      VERTICAL_INTROS[_selectedIndustry] ?? VERTICAL_INTROS[""];

    let streamedText = "";
    let headingsDetected = false; // true once the first ## heading appears in the stream

    try {
      const result = await streamQuery(settingsNow, payload, (chunk) => {
        streamedText += chunk;

        if (headingsDetected) return; // accordion confirmed — placeholder stays until final render

        // Detect first ## heading in the stream.
        if (streamedText.includes("\n## ") || streamedText.startsWith("## ")) {
          headingsDetected = true;
          return; // placeholder stays — finalizeTurnEl handles all rendering
        }

        // No ## detected yet. Once we have enough text to be confident this is a
        // conversational (non-accordion) response, start streaming it to the DOM.
        // 150 chars without a heading almost certainly means no accordion is coming.
        if (streamedText.trim().length > 150) {
          answerEl.textContent = streamedText;
        }
      });
      const wallMs = Date.now() - started;
      const serverMs = (result.timing && result.timing.total_ms) || null;
      finalizeTurnEl(
        turnEl,
        result.answer || streamedText,
        result.citations || [],
        formatMeta(
          serverMs,
          wallMs,
          (result.citations || []).length,
          result.context_utilization,
        ),
        turnKey,
        VERTICAL_INTROS[_selectedIndustry] ?? VERTICAL_INTROS[""],
      );
      attachFeedbackControls(turnEl, {
        query: q,
        answer: result.answer || "",
        citations: result.citations || [],
        sessionId: session.sessionId,
        turnId: turnId,
      });
      // Pin the question at the top of the viewport now that rendering is
      // settled — streaming growth + final swap can have shifted the scroll.
      // Smooth scroll per briefing; fall back to CSS scroll-behavior on the
      // container if long histories feel sluggish (not observed yet).
      const qEl = turnEl.querySelector(".q");
      if (qEl) qEl.scrollIntoView({ behavior: "smooth", block: "start" });
      session.turns.push({ role: "user", content: q });
      session.turns.push({
        role: "assistant",
        content: result.answer || "",
        citations: result.citations || [],
        query_time_ms: serverMs,
        context_utilization: result.context_utilization,
      });
      await saveSession(session);
      updateTruncationIndicator(session);
    } catch (e) {
      failTurnEl(turnEl, e);
    } finally {
      $("ask").disabled = false;
      $("spinner").classList.remove("on");
    }
  }
}

init();
