# Infrastructure Setup — Before First Code Session

Complete all five steps before opening Claude Code. These are one-time account
and configuration tasks, not code. Estimated time: 1.5–2 hours total.

---

## 1. Pinecone — Create the Vector Index

**Where:** https://app.pinecone.io

1. Sign up or log in. Free tier is sufficient for Alpha.
2. In the left sidebar, click **Indexes → Create index**.
3. Configure the index:
   - **Index name:** `synexis-rep-agent`
   - **Dimensions:** `1024` (matches Voyage AI `voyage-3` output — do not change)
   - **Metric:** `Cosine`
   - **Index type:** `Serverless` (NOT pod-based — pod-based starts at $50/month)
   - **Cloud / Region:** AWS us-east-1 (default is fine)
4. Click **Create index**. It will be ready in ~30 seconds.
5. Once created, click into the index and note the **Host** URL — it looks like:
   `synexis-rep-agent-abc123.svc.aped-4627-b74a.pinecone.io`
   You'll need this for the API client config.
6. Go to **API Keys** (left sidebar) → copy your API key.
7. Add to `.env`:
   ```
   PINECONE_API_KEY=<your key>
   PINECONE_INDEX_NAME=synexis-rep-agent
   ```

**Verify:** On the index detail page, Status should show **Ready**.

---

## 2. Voyage AI — Confirm Free Tier and Grab API Key

**Where:** https://dash.voyageai.com

1. Sign up or log in using your Google or GitHub account.
2. Free tier gives you **200M tokens/month** — more than enough for Alpha and Beta.
   No credit card required at this stage.
3. Go to **API Keys** in the left sidebar → click **Create new key**.
4. Name it `synexis-rep-agent`, copy the key immediately (it won't be shown again).
5. Add to `.env`:
   ```
   VOYAGE_API_KEY=<your key>
   VOYAGE_EMBEDDING_MODEL=voyage-3
   VOYAGE_RERANK_MODEL=rerank-2.5-lite
   ```

**Verify:** In the dashboard, confirm the free tier monthly usage resets are shown.
Note: `rerank-2` is Voyage's current reranking model — confirm the name hasn't
changed at https://docs.voyageai.com/docs/reranker.

---

## 3. Render — Create the Web Service

**Where:** https://render.com

1. Sign up or log in.
2. From the dashboard, click **New → Web Service**.
3. Choose **Deploy from a Git repository**.
   - Connect your GitHub account if not already connected.
   - Select the `synexis-rep-agent` repo (create it first — see Step 5 below).
   - If the repo doesn't exist yet, you can create a placeholder service manually
     and connect it later.
4. Configure the service:
   - **Name:** `synexis-rep-agent`
   - **Region:** Oregon (US West) or Ohio (US East) — pick closest to your users
   - **Branch:** `main`
   - **Runtime:** Python 3
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `uvicorn api.main:app --host 0.0.0.0 --port $PORT`
   - **Instance type:** `Starter ($7/month)` — do NOT use Free tier (it spins down
     after inactivity, adding 30–60s cold start to first query)
5. Under **Environment Variables**, scroll down to the **Environment Variables**
   section on the same "Create Web Service" page (before you click Create).
   Add each variable one at a time using the **Add Environment Variable** button.
   Each row has a **Key** field and a **Value** field.

   Add all of the following (skip `SOURCE_CONTENT_PATH` — that's local only):

   | Key | Value |
   |---|---|
   | `PINECONE_API_KEY` | your Pinecone API key |
   | `PINECONE_INDEX_NAME` | `synexis-rep-agent` |
   | `PINECONE_ENVIRONMENT` | `us-east-1-aws` (confirm in Pinecone dashboard) |
   | `VOYAGE_API_KEY` | your Voyage AI API key |
   | `VOYAGE_EMBEDDING_MODEL` | `voyage-3` |
   | `VOYAGE_RERANK_MODEL` | `rerank-2` |
   | `ANTHROPIC_API_KEY` | your Anthropic API key |
   | `PARTNER_KEYS` | leave blank for now — add before Beta |
   | `RATE_LIMIT_PER_HOUR` | `100` |
   | `LOG_LEVEL` | `INFO` |

   **Tip:** If you've already clicked Create and need to add or edit variables
   later, go to your service → **Environment** tab → **Add Environment Variable**.
   Changes take effect on the next deploy (click **Manual Deploy** to trigger one).

   **Do not** paste the contents of your `.env` file directly — Render's env var
   UI expects one key-value pair per row, not a file format.

6. Click **Create Web Service**. The first deploy will fail on the placeholder
   scaffold — that's expected. It will succeed once real code is pushed.
7. Note the service URL: `https://synexis-rep-agent.onrender.com`

**Verify:** After first real deploy, `GET https://synexis-rep-agent.onrender.com/health`
should return `{"status": "ok"}`.

---

## 4. Anthropic Console — Set the Spend Cap

**Where:** https://console.anthropic.com

This must be done before Beta goes live with outside access. Do it now so it's
not forgotten.

1. Log in to the Anthropic console.
2. Go to **Settings → Billing**.
3. Under **Usage limits**, set a **Monthly spend limit**.
   - For Alpha: $10 is sufficient (internal testing only)
   - For Beta: $25–50 is reasonable for Synexis-team-only access
   - For Partner Beta: revisit based on projected query volume
4. Enable budget alert notifications at **50%** and **80%** of the cap.
5. Copy your API key from **API Keys** if you don't already have it.
6. Add to `.env`:
   ```
   ANTHROPIC_API_KEY=<your key>
   ```

**Why this matters:** The spend cap is a zero-code kill switch. If anything goes
wrong — runaway script, prompt injection, unexpected traffic — it stops Claude
API spend dead without requiring a deploy or manual intervention.

---

## 5. Git Repo — Create and Initialize

1. Go to https://github.com/new
2. Create a new **private** repository named `synexis-rep-agent`.
3. Do NOT initialize with a README (the scaffold already has one).
4. Copy the repo URL.
5. In your terminal, navigate to the `synexis-rep-agent/` folder from this
   workspace and run:
   ```bash
   git init
   git remote add origin <your repo URL>
   cp .env.example .env          # then fill in your keys
   echo ".env" >> .gitignore
   echo "__pycache__/" >> .gitignore
   echo "*.pyc" >> .gitignore
   git add .
   git commit -m "Initial scaffold"
   git push -u origin main
   ```
6. Connect the repo to Render (see Step 3) — every push to `main` will trigger
   a new deploy automatically.

**Security check:** Confirm `.env` appears in `.gitignore` before the first push.
Never commit the actual `.env` file.

---

## After All Five Steps

Copy the completed `.env` (with all keys filled in) to wherever Claude Code will
run the pipeline — it needs `SOURCE_CONTENT_PATH` pointing at the local
`source_content/` folder.

Then open Claude Code and hand off with:

> The infrastructure is live. Repo is scaffolded at `synexis-rep-agent/`.
> All keys are in `.env`. Source files are at `[SOURCE_CONTENT_PATH]`.
> Manifest is at `source_content_manifest.md` — files with status `ingested`
> are the ones to process. Start with the content pipeline: extract, chunk
> (400 tokens, efficacy claims atomic), embed with Voyage AI voyage-3,
> upsert to Pinecone with full metadata. Gate before moving on: spot-check
> 10 chunks for text integrity and metadata completeness.
