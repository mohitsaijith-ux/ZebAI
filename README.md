═══════════════════════════════════════════════════════════════════════
  ZebAI — An Agentic Framework
  v1.0.0 · Free for personal use
═══════════════════════════════════════════════════════════════════════

ZebAI is an agentic AI framework built on Google Gemini. The model emits
bare XML tags, the backend runs them, and the frontend renders every
step as it happens — thinking, tool calls, sources, charts, and answer,
all streamed live.


───────────────────────────────────────────────────────────────────────
  WHAT'S IN THIS ZIP
───────────────────────────────────────────────────────────────────────

  frontend.txt     → rename to index.html after editing
  backend.txt      → paste into Cloudflare Worker (or rename to worker.js)
  readme.txt       → this file
  licence.txt      → PolyForm Noncommercial 1.0.0
  logo-dark.png    → app logo for dark theme
  logo-light.png   → app logo for light theme


───────────────────────────────────────────────────────────────────────
  REQUIREMENTS
───────────────────────────────────────────────────────────────────────

  · Google AI Studio account — free tier
  · Cloudflare account — free tier (Workers, D1, KV)
  · Six API keys total (see step 2 below)
  · A text editor (Notepad, TextEdit, VS Code)
  · A modern web browser

  No terminal. No command line. No npm install.


───────────────────────────────────────────────────────────────────────
  SETUP — 9 STEPS
───────────────────────────────────────────────────────────────────────

  ─────────────────────────────────────────────────────────────────
  STEP 1 — EXTRACT THE ZIP
  ─────────────────────────────────────────────────────────────────

  Extract this zip into a folder. Keep it open — you'll be copying
  text out of it in the next few steps.

  The only file you'll edit is frontend.txt (one line). When you're
  done, you'll rename it to index.html and rename backend.txt to
  worker.js.


  ─────────────────────────────────────────────────────────────────
  STEP 2 — GET SIX API KEYS
  ─────────────────────────────────────────────────────────────────

  Each tool talks to a different service. Sign up for all six —
  skip one and that tool fails silently.

  ┌─────────────────┬──────────────┬─────────────────────────────────┐
  │ Secret name     │ Free tier    │ What it powers                  │
  ├─────────────────┼──────────────┼─────────────────────────────────┤
  │ GOOGLE_KEYS     │ 500/day      │ The model itself                │
  │                 │              │ aistudio.google.com/app/apikey  │
  ├─────────────────┼──────────────┼─────────────────────────────────┤
  │ TAVILY_KEYS     │ 1,000/mo     │ The <search> tool               │
  │                 │              │ tavily.com                      │
  ├─────────────────┼──────────────┼─────────────────────────────────┤
  │ FIRECRAWL_KEYS  │ 500 credits  │ The <analyse> tool              │
  │                 │              │ firecrawl.dev                   │
  ├─────────────────┼──────────────┼─────────────────────────────────┤
  │ WA_KEYS         │ 1M/mo        │ The <weather> tool              │
  │                 │              │ weatherapi.com                  │
  ├─────────────────┼──────────────┼─────────────────────────────────┤
  │ ONETWO_KEYS     │ 800/day      │ The <finance> tool              │
  │                 │              │ twelvedata.com                  │
  ├─────────────────┼──────────────┼─────────────────────────────────┤
  │ ERA_KEYS        │ 1,500/mo     │ Currency conversion             │
  │                 │              │ exchangerate-api.com            │
  └─────────────────┴──────────────┴─────────────────────────────────┘

  No credit card required for any of them.

  TIP: Every provider accepts a comma-separated list of keys. If one
  hits its limit, ZebAI rotates to the next automatically. Example:

      AIzaSyAbc123...,AIzaSyXyz789...


  ─────────────────────────────────────────────────────────────────
  STEP 3 — CREATE THE D1 DATABASE
  ─────────────────────────────────────────────────────────────────

  1. Open dash.cloudflare.com and log in.
  2. Sidebar → Workers & Pages → D1 SQL Database.
  3. Click "Create database", name it exactly: zebai-db
  4. Click Create.
  5. Click the "Console" tab.
  6. Copy the SQL below (between the ==== lines) and paste it into the
     console.
  7. Click Execute.

  ─────────────── D1 SCHEMA — COPY EVERYTHING BELOW ───────────────

CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
    token TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chats (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    title TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    chat_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    mode TEXT,
    timestamp INTEGER NOT NULL,
    blocks TEXT
);

CREATE TABLE IF NOT EXISTS rate_limits (
    user_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    mime TEXT NOT NULL,
    name TEXT,
    size INTEGER,
    data TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rate_limits_user_time
    ON rate_limits (user_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_tokens_username
    ON tokens (username);
CREATE INDEX IF NOT EXISTS idx_chats_username
    ON chats (username);
CREATE INDEX IF NOT EXISTS idx_messages_chat_id
    ON messages (chat_id);
CREATE INDEX IF NOT EXISTS idx_idempotency_created
    ON idempotency_keys (created_at);
CREATE INDEX IF NOT EXISTS idx_blobs_username
    ON blobs (username);
CREATE INDEX IF NOT EXISTS idx_blobs_created
    ON blobs (created_at);

  ─────────────── END OF SCHEMA ───────────────

  VERIFY: click the "Tables" tab. You should see seven tables:
  users, tokens, chats, messages, rate_limits, idempotency_keys, blobs.


  ─────────────────────────────────────────────────────────────────
  STEP 4 — CREATE TWO KV NAMESPACES
  ─────────────────────────────────────────────────────────────────

  1. Sidebar → Workers & Pages → KV.
  2. Click "Create namespace", name it exactly: CHATS
  3. Click "Create namespace" again, name it exactly: MODELTRACKER

  CAPITALISATION MATTERS. If you type "chats" instead of "CHATS", the
  Worker won't find it and every file upload will fail silently.


  ─────────────────────────────────────────────────────────────────
  STEP 5 — CREATE THE WORKER
  ─────────────────────────────────────────────────────────────────

  1. Sidebar → Workers & Pages → Overview.
  2. Click "Create application" → "Create Worker".
  3. Name it something like: zebai-backend
  4. Click "Deploy". Deploy the default "Hello World" first.
  5. Once deployed, click "Edit code".
  6. Select everything in the left editor pane and delete it.
  7. Open backend.txt from this zip. Copy all of it. Paste into the
     editor.
  8. Click "Deploy" in the top right.

  COPY YOUR WORKER URL NOW. It looks like:
      https://zebai-backend.your-name.workers.dev

  You'll need it in step 8.


  ─────────────────────────────────────────────────────────────────
  STEP 6 — ADD THE SIX SECRETS
  ─────────────────────────────────────────────────────────────────

  1. On your Worker page, click the "Settings" tab.
  2. Left menu → "Variables and Secrets" → click Add.
  3. For each of these six names, set the type to "Secret", paste the
     API key from step 2, and click Save:

         GOOGLE_KEYS
         TAVILY_KEYS
         FIRECRAWL_KEYS
         WA_KEYS
         ONETWO_KEYS
         ERA_KEYS

  CASE-SENSITIVE. If you type "google_keys" or add a stray space, the
  key won't be found and that tool fails without an obvious error.


  ─────────────────────────────────────────────────────────────────
  STEP 7 — BIND DATABASE AND KV
  ─────────────────────────────────────────────────────────────────

  Still on the Worker page, in Settings:

  1. Left menu → "Bindings" → Add → "D1 database".
       Variable name:  DB
       D1 database:    zebai-db
       Click Save.

  2. Add → "KV namespace".
       Variable name:  CHATS
       KV namespace:   CHATS
       Click Save.

  3. Add → "KV namespace".
       Variable name:  MODELTRACKER
       KV namespace:   MODELTRACKER
       Click Save.

  REDEPLOY after adding bindings. Cloudflare doesn't apply them to the
  running Worker until you redeploy. Go to Deployments → Redeploy, or
  open Edit code and click Deploy again.


  ─────────────────────────────────────────────────────────────────
  STEP 8 — POINT THE FRONTEND AT YOUR WORKER
  ─────────────────────────────────────────────────────────────────

  This is the only code change in the entire setup.

  1. Open frontend.txt in any text editor.
  2. Press Ctrl+F (or Cmd+F on Mac) and search for:  API_BASE
  3. Replace the placeholder URL with your Worker URL from step 5.
  4. Save the file. Rename it from frontend.txt to index.html.

  BEFORE:
      const API_BASE = 'https://zebai-backend.YOUR-NAME.workers.dev';

  AFTER:
      const API_BASE = 'https://zebai-backend.your-actual-name.workers.dev';

  Keep the single quotes. No trailing slash. No spaces.

  Double-click the renamed index.html — you should see the login screen.


  ─────────────────────────────────────────────────────────────────
  STEP 9 — VERIFY EVERYTHING WORKS
  ─────────────────────────────────────────────────────────────────

  Create an account in the app, then send this exact message. It fires
  all six tools in one round.

      What's the weather in Tokyo, the AAPL stock price, and the
      latest AI news? Also convert 100 USD to EUR.

  You should see:
    · A "Thought" block opening with streaming reasoning
    · A search card with favicons and 3–5 sources
    · A weather card showing Tokyo's current temperature
    · A finance card with the AAPL quote and currency conversion
    · A streaming final answer

  If a tool fails, the error tells you which one. <search> failing
  means TAVILY_KEYS is wrong. <weather> means WA_KEYS. Go back to
  step 6 and check the spelling.


───────────────────────────────────────────────────────────────────────
  RENAME YOUR FILES (final step)
───────────────────────────────────────────────────────────────────────

  Once you've pasted the backend into Cloudflare and edited the
  frontend, rename the two files so your folder structure is clean:

      frontend.txt  →  index.html
      backend.txt   →  worker.js     (keep this as a local copy)

  Your final folder should look like:

      zebai/
      ├── index.html
      ├── worker.js
      ├── readme.txt
      ├── licence.txt
      ├── logo-dark.png
      └── logo-light.png


───────────────────────────────────────────────────────────────────────
  FILE STRUCTURE AT RUNTIME
───────────────────────────────────────────────────────────────────────

  frontend (index.html)
    Single file. Contains markup, CSS, and JS. Talks to the Worker
    over SSE. Renders every event the Worker emits.

  backend (worker.js)
    Runs on Cloudflare Workers. Holds the tool protocol, key router,
    and sandboxed JS interpreter. Talks to Gemini and the six tool
    providers.

  D1 database (zebai-db)
    Stores users, tokens, chats, messages, rate limits, idempotency
    keys, and blob metadata.

  KV namespaces (CHATS, MODELTRACKER)
    CHATS holds file blobs. MODELTRACKER holds per-model health and
    API key rotation state.


───────────────────────────────────────────────────────────────────────
  TOOL PROTOCOL
───────────────────────────────────────────────────────────────────────

  The model emits bare XML tags. A tool call reply contains ONLY the
  tag — no prose, no punctuation, no whitespace around it.

  CORRECT:
      <search>latest AI news</search><finance>{"type":"stock","symbol":"AAPL"}</finance>

  WRONG (trailing period, parser drops the tool):
      <search>latest AI news</search>.

  WRONG (comma between tags):
      <search>latest AI news</search>, <weather>Tokyo</weather>

  WRONG (prose wrapper):
      Let me search for that. <search>latest AI news</search>

  The seven tools:

    <search>query</search>
        Live web search. Use for current events and live facts.

    <analyse>https://exact-url</analyse>
        Reads a specific URL in full. Fires after <search> when
        snippets are too thin.

    <weather>City</weather>
        Current weather for a named city.

    <finance>{"type":"stock","symbol":"AAPL"}</finance>
    <finance>{"type":"forex","base":"USD","target":"INR"}</finance>
        Live market data.

    <run>javascript</run>
        Executes JS in a sandboxed interpreter. Math, dates, data
        transforms. No network, no DOM, no timers.

    <analysing>filename.ext</analysing>
        Re-attaches a file from the conversation as native input.

    <chart>{...}</chart>
        A chart inside the final answer. Always the first block.


───────────────────────────────────────────────────────────────────────
  CONFIG REFERENCE
───────────────────────────────────────────────────────────────────────

  All secrets are set in Cloudflare → Worker → Settings → Variables
  and Secrets. Nothing is hardcoded.

  Required:
      GOOGLE_KEYS      — one or more Gemini API keys, comma-separated
      TAVILY_KEYS      — Tavily API keys
      FIRECRAWL_KEYS   — Firecrawl API keys
      WA_KEYS          — WeatherAPI keys
      ONETWO_KEYS      — Twelve Data API keys
      ERA_KEYS         — ExchangeRate-API keys

  Bindings (Worker → Settings → Bindings):
      DB               — D1 database: zebai-db
      CHATS            — KV namespace: CHATS
      MODELTRACKER     — KV namespace: MODELTRACKER


───────────────────────────────────────────────────────────────────────
  TROUBLESHOOTING
───────────────────────────────────────────────────────────────────────

  "All Gemini models failed this round"
    Google's free tier is capacity-limited. Wait a minute and retry.
    If it happens often, add a second Google key (from a different
    Google account) to GOOGLE_KEYS separated by a comma.

  "<search> failed" / "Can't search the web right now"
    TAVILY_KEYS is wrong or empty. Check the spelling in Cloudflare.

  "File uploads fail silently"
    You probably named the KV namespace "chats" instead of "CHATS".
    Names are case-sensitive.

  "Worker can't find DB / CHATS / MODELTRACKER"
    You added bindings but forgot to redeploy. Go to Deployments →
    Redeploy.

  "Blank page / nothing happens"
    Open the browser console (F12). If you see a CORS error, your
    API_BASE doesn't match the Worker URL. Check for a trailing slash
    or extra space.

  "Login screen never appears"
    The API_BASE line in index.html wasn't edited, or the Worker URL
    is wrong. It should look like:
        https://zebai-backend.your-name.workers.dev


───────────────────────────────────────────────────────────────────────
  FAQ
───────────────────────────────────────────────────────────────────────

  Is this really free?
    Yes. Every service has a permanent free tier. For personal use
    you won't come close to any limit.

  Can I use it at my company?
    Not under this license. PolyForm Noncommercial forbids commercial
    use — including internal tools at a for-profit company.

  What if I hit an API limit?
    Sign up for a second account with the same provider and add both
    keys to the same secret, comma-separated.

  Can I use Claude or GPT instead of Gemini?
    The core is model-agnostic. You'd write an adapter that converts
    the message format and streams the response. Gemini is the
    reference because it's the only free-tier model that reliably
    handles the tag protocol without looping.

  Do I need to install anything?
    No. Everything happens in the browser. No terminal, no command
    line, no npm install.

  How do I update?
    Download the new zip, replace the backend.txt content in the
    Cloudflare editor with the new one, redeploy. Database and
    secrets stay untouched. Chats are preserved.


───────────────────────────────────────────────────────────────────────
  LICENSE
───────────────────────────────────────────────────────────────────────

  ZebAI is licensed under PolyForm Noncommercial 1.0.0.

  Free for personal use. Free for research, study, hobby projects,
  nonprofits, educational institutions, and government bodies.

  Commercial use is not permitted. You can't sell ZebAI or a product
  built on it, run a paid SaaS on it, or use it inside a for-profit
  business. For commercial licensing, reach out.

  Full text is in licence.txt.


───────────────────────────────────────────────────────────────────────
  CREDITS
───────────────────────────────────────────────────────────────────────

  Built by MohitTheFounder.
  Framework design and implementation: MCOS Private Limited.

  Powered by Google Gemini. Tool integrations with Tavily, Firecrawl,
  WeatherAPI, Twelve Data, and ExchangeRate-API.

═══════════════════════════════════════════════════════════════════════
