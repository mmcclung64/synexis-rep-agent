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

// Clean up raw file_path values for display.
// Rules: basename only → locale suffixes → strip FINAL/PROOF → strip 4-digit date codes → underscores→spaces.
// Extend the locale map as global markets are added.
function prettyPath(filePath) {
  // 1. Filename only — drop folder segments and extension
  let name = (filePath || "").split("/").pop().replace(/\.[^.]+$/, "");
  // 2. Locale suffixes → readable labels  (add new entries here as markets expand)
  name = name.replace(/_ESP\b/gi, " (Spanish)");
  // 3. Strip trailing production suffixes and anything after (FINAL, Final, PROOF, etc.)
  name = name.replace(/[_ ]+(?:FINAL|Final|PROOF)\b.*$/i, "");
  // 4. Strip trailing 4-digit date codes (_MMYY)
  name = name.replace(/_\d{4}$/, "");
  // 5. Underscores → spaces, collapse whitespace, trim
  return name.replace(/_/g, " ").replace(/\s{2,}/g, " ").trim();
}

function renderBadge(n, citation, turnKey) {
  if (!citation) return `[${n}]`;
  const path = escapeHtml(prettyPath(citation.file_path || ""));
  const page = citation.page_or_slide;
  const pageStr = page !== undefined && page !== null && page !== ""
    ? ` — page/slide ${escapeHtml(String(page))}`
    : "";
  const snippet = escapeHtml(citation.snippet || "");
  const shareUrl = citation.share_url || "";
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

// Progressive accordion rendered during streaming (no citations yet).
// Returns null if no ## headings detected yet — caller falls back to textContent.
// Completed sections (those before the currently-streaming one) are rendered
// collapsed so the user can start expanding them immediately.
// The in-progress section is shown open with a streaming indicator.
function renderProgressiveAccordion(text, turnKey) {
  // Detect headings without escaping first so we can bail early cheaply.
  if (!text.includes("\n## ") && !text.startsWith("## ")) return null;

  const lines = escapeHtml(text).split("\n");
  if (!lines.some(l => /^##\s/.test(l))) return null;

  const firstHd = lines.findIndex(l => /^##\s/.test(l));
  const preamble = lines.slice(0, firstHd);
  const sections = [];
  let cur = null;
  for (const line of lines.slice(firstHd)) {
    if (/^##\s/.test(line)) {
      if (cur) sections.push({ ...cur, complete: true });
      cur = { heading: line.replace(/^##\s+/, ""), lines: [] };
    } else {
      if (cur) cur.lines.push(line);
    }
  }
  if (cur) sections.push({ ...cur, complete: false }); // last section still streaming

  const citeMap = new Map(); // empty during stream — citations arrive with final event
  const parts = [];

  if (preamble.some(l => l.trim())) {
    parts.push(`<div class="ans-preamble">${renderLines(preamble, citeMap, turnKey)}</div>`);
  }

  if (sections.length > 1) {
    parts.push(`<div class="ans-expand-bar"><button class="ans-expand-all">Expand all</button></div>`);
  }

  sections.forEach((s, idx) => {
    const bodyId = `sec-${turnKey}-${idx}`;
    if (s.complete) {
      parts.push(
        `<div class="ans-section">` +
          `<button class="ans-section-hd" aria-expanded="false" data-body="${bodyId}">` +
            `<span class="ans-chevron">&#9654;</span>` +
            `<span>${inlineTransforms(s.heading, citeMap, turnKey)}</span>` +
          `</button>` +
          `<div class="ans-section-body" id="${bodyId}" hidden>${renderLines(s.lines, citeMap, turnKey)}</div>` +
        `</div>`
      );
    } else {
      // In-progress: visible, no toggle button, cursor blinking to signal streaming
      parts.push(
        `<div class="ans-section ans-section-live">` +
          `<div class="ans-section-hd-static">` +
            `<span>${inlineTransforms(s.heading, citeMap, turnKey)}</span>` +
          `</div>` +
          `<div class="ans-section-body ans-section-body-live">${renderLines(s.lines, citeMap, turnKey)}<span class="ans-cursor"></span></div>` +
        `</div>`
      );
    }
  });

  return parts.join("");
}

function renderAnswer(answer, citations, turnKey) {
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

  // Preamble (if non-empty)
  if (preamble.some(l => l.trim())) {
    parts.push(`<div class="ans-preamble">${renderLines(preamble, citeMap, turnKey)}</div>`);
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
  const items = citations
    .map((c) => {
      const pageVal = c.page_or_slide;
      const page = pageVal !== undefined && pageVal !== null && pageVal !== ""
        ? ` — page/slide ${escapeHtml(String(pageVal))}`
        : "";
      const link = c.share_url
        ? ` <a class="cite-link" href="${escapeHtml(c.share_url)}" target="_blank" rel="noopener">View ↗</a>`
        : "";
      return `<div class="cite" id="src-${turnKey}-${c.n}"><span class="n">[${c.n}]</span> <span class="path">${escapeHtml(prettyPath(c.file_path || ""))}</span><span class="page">${page}</span>${link}</div>`;
    })
    .join("");
  return `<div class="citations"><div class="head">Sources</div>${items}</div>`;
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

// Open document link when clicking/entering on tt-path-link spans (can't nest <a> inside cite-badge <a>).
document.addEventListener("click", (ev) => {
  const el = ev.target && ev.target.closest && ev.target.closest(".tt-path-link");
  if (!el) return;
  ev.preventDefault(); ev.stopPropagation();
  const href = el.dataset.href;
  if (href) window.open(href, "_blank", "noopener");
});
document.addEventListener("keydown", (ev) => {
  if (ev.key !== "Enter" && ev.key !== " ") return;
  const el = ev.target && ev.target.closest && ev.target.closest(".tt-path-link");
  if (!el) return;
  ev.preventDefault();
  const href = el.dataset.href;
  if (href) window.open(href, "_blank", "noopener");
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

function finalizeTurnEl(turnEl, answer, citations, meta, turnKey) {
  turnEl.querySelector(".a").innerHTML = renderAnswer(answer || "", citations || [], turnKey);
  const cits = renderCitations(citations || [], turnKey);
  if (cits) turnEl.querySelector(".a").insertAdjacentHTML("afterend", cits);
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

  $("returnToSend").addEventListener("change", () => {
    updatePlaceholder($("returnToSend").checked);
  });

  renderHistoryFromSession(session);

  let _selectedIndustry = "";

  function showIndustryPicker() {
    $("empty").style.display = "none";
    $("intent-picker").style.display = "none";
    $("industry-picker").style.display = "";
  }

  function showIntentPicker(industry) {
    _selectedIndustry = industry;
    $("industry-picker").style.display = "none";
    $("intent-prompt").textContent = industry
      ? `Great. What can I help you with for ${industry}?`
      : "Great. What can I help you with?";
    $("intent-picker").style.display = "";
  }

  function hidePickers() {
    $("industry-picker").style.display = "none";
    $("intent-picker").style.display = "none";
  }

  if (session.turns.length === 0) showIndustryPicker();

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

  $("toggleSettings").addEventListener("click", () => {
    $("settings").classList.toggle("open");
  });

  $("newConversation").addEventListener("click", async () => {
    session = await resetSession();
    $("history").innerHTML = "";
    $("empty").style.display = "none";
    _selectedIndustry = "";
    showIndustryPicker();
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
    let streamedText = "";
    let inAccordionMode = false;

    try {
      const result = await streamQuery(settingsNow, payload, (chunk) => {
        streamedText += chunk;

        // Only attempt accordion rendering at line boundaries (cheaper) and
        // when we see or already know there are ## headings.
        const hasHeadings = streamedText.includes("\n## ") || streamedText.startsWith("## ");
        if (hasHeadings && (chunk.includes("\n") || !inAccordionMode)) {
          const progressive = renderProgressiveAccordion(streamedText, turnKey);
          if (progressive !== null) {
            // Snapshot which sections the user already expanded so we can restore after re-render.
            const expandedIds = new Set();
            answerEl.querySelectorAll(".ans-section-hd[aria-expanded='true']").forEach(hd => expandedIds.add(hd.dataset.body));

            inAccordionMode = true;
            answerEl.innerHTML = progressive;

            // Restore expanded state.
            if (expandedIds.size > 0) {
              answerEl.querySelectorAll(".ans-section-hd").forEach(hd => {
                if (expandedIds.has(hd.dataset.body)) {
                  hd.setAttribute("aria-expanded", "true");
                  const body = document.getElementById(hd.dataset.body);
                  if (body) body.hidden = false;
                }
              });
              syncExpandAll(turnEl);
            }
            return;
          }
        }

        // Fall back to plain text while waiting for the first ## heading.
        if (!inAccordionMode) answerEl.textContent = streamedText;
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
