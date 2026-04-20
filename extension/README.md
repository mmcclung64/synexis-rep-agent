# Synexis Rep Agent — Chrome Extension (side panel)

Vanilla-JS Chrome MV3 extension. Opens a side panel that POSTs queries to the Synexis Rep Agent HTTP API and renders the grounded answer + citations.

## Install (unpacked, for Alpha)

1. Make sure the API is running. Locally:
   ```
   python3 -m uvicorn api.main:app --host 127.0.0.1 --port 8000
   ```
2. Open `chrome://extensions` in Chrome.
3. Toggle **Developer mode** on (top right).
4. Click **Load unpacked** and select this `extension/` folder.
5. Pin the extension so its icon is visible on the toolbar.
6. Click the icon → side panel opens on the right.
7. Click **Settings** in the side panel header:
   - **API URL**: `http://127.0.0.1:8000` (default) or your Render URL.
   - **Partner key**: leave blank during Alpha (anonymous fallback is intentional). Populate only once `PARTNER_KEYS` is set on the server.
   - Click **Test connection** → expect `OK — model=..., index=sra, auth_configured=false` for local dev.
   - Click **Save**.

The status dot in the header:
- green = API reachable, auth not configured (Alpha default)
- blue  = API reachable, auth configured
- red   = API unreachable

## Use

- Type a question in the textarea, **Cmd/Ctrl+Enter** or click **Ask**.
- Each turn shows the answer, inline `[N]` citation markers, and a **Sources** list below with file path and page/slide.

## Requirements

- Chrome 114+ for the `chrome.sidePanel` API.
- Host permissions are baked into `manifest.json` for:
  - `http://127.0.0.1:8000/*` and `http://localhost:8000/*` (local dev)
  - `https://*.onrender.com/*` (Render deploy)
  If you deploy the API to a different host, add it under `host_permissions` in `manifest.json` and reload the extension.

## Troubleshooting

- **Side panel doesn't open**: Chrome < 114. Update Chrome.
- **Red status dot, "Failed to fetch"**: API isn't running at the configured URL, or CORS is blocking. `api/main.py` sets `allow_origins=["*"]` during Alpha; if you've tightened CORS, add the extension origin (`chrome-extension://<your-extension-id>`) to the allowlist.
- **401 on every query after setting `PARTNER_KEYS`**: the key in Settings must match one of the comma-separated keys on the server.
- **429 "Rate limit exceeded"**: you've hit `RATE_LIMIT_PER_HOUR`. Wait for the `Retry-After` window or raise the limit in Render env vars.
