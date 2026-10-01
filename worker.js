// ============================================================================
// ZEBAI WORKER – v106.27.0
//   • Google Gemini only. Flash-Lite family + 2.5 fallbacks.
//   • Chat: gemini-3.5-flash-lite, gemini-3.1-flash-lite,
//           gemini-2.5-flash-lite, gemini-2.5-flash.
//   • Embeddings: gemini-embedding-2/001.
//   • Key router rotates across API keys, tracks RPM vs RPD cooldowns.
//   • Native CoT streamed as thinking events. No prompted thinking.
//   • Blobs in KV CHATS, D1 spill. Ambient file index with COMPLETE/PARTIAL.
//   • Exponential backoff on 5xx/429. Aborts classified as transient 503.
//   • Structured error fingerprint: E=M1-code/M2-code/M3-code/M4-code
//   • Search: Tavily discovery-only. Analyse: Firecrawl.
//   • Gemini Files API native upload (cached 47h) for all attachments.
//   • v106.27.0:
//      - <vid> YouTube embedding tool removed end to end. The
//        search-first requirement meant it only fired when Tavily
//        happened to return a YouTube URL in the top 5 results,
//        which was rare and produced broken cards more often than
//        useful embeds. Removed:
//          • sanitizeVideoSpec() helper
//          • vid from StatefulXMLParser.SOFT_TOOLS
//          • video branch in _closeTool
//          • videos[] from parser + pipeStream return shape
//          • video_render SSE event
//          • hasVideo gate in handleMessages
//          • the entire # Video section in the system prompt
//          • <vid> references in tools list, checklist, formatting
//            safety, anti-patterns, and reply contract
//      - All v106.26.5 fixes retained: chats.mode column in D1,
//        isTruncatedStop trusts provider, pipeStream flushes
//        residual SSE, exact-URL citation rules for prose links.
// ============================================================================

const DEBUG = true;
const WORKER_VERSION = '106.27.0';
const ASSISTANT_NAME = 'ZebAI';
const ASSISTANT_CREATOR = 'MCOS Private Limited';

const TTFT_TEXT_MS   = 35000;
const TTFT_CODE_MS   = 45000;
const TTFT_VISION_MS = 40000;
const CHUNK_WATCHDOG_MS = 120000;

const DISABLE_FAILURE_COOLDOWN = false;
const MAX_HISTORY_MESSAGES = 6;
const MAX_TOOL_RESULT_CHARS = 1200;
const MAX_SEARCH_SOURCES = 5;
const MAX_SEARCH_SNIPPET_CHARS = 500;
const MAX_SEARCH_RAW_CHARS = 6000;
const FAILURE_COOLDOWN_SECONDS = 30;
const TOKEN_TTL = 30 * 24 * 60 * 60;
const MAX_MSG = 50;
const RATE_LIMIT_WINDOW = 60;
const RATE_LIMIT_MAX = 20;
const MAX_ATTACHMENTS_PER_MESSAGE = 5;
const MAX_ATTACHMENT_BYTES = 15 * 1024 * 1024;
const MAX_TOTAL_ATTACHMENT_BYTES = 20 * 1024 * 1024;
const TOOL_FETCH_TIMEOUT_MS = 15000;
const SEARCH_TIMEOUT_MS = 8000;
const ANALYSE_TIMEOUT_MS = 15000;
const MAX_TOOL_ROUNDS = 40;
const SOFT_TOOL_ROUND_LIMIT = 10;
const MAX_PARALLEL_TOOLS = 4;
const TOOL_BATCH_TIMEOUT_MS = 45000;
const MAX_READ_CHARS = 4000;
const MAX_CONTINUATIONS = 25;
const FASTEST_TTL = 86400;
const LLM_MAX_ATTEMPTS = 3;
const LLM_MAX_ATTEMPTS_CAPACITY = 4;
const MAX_THINKING_CHARS = 6000;
const TURN_DEADLINE_MS = 900000;
const STREAM_HEARTBEAT_MS = 10000;
const MAX_HISTORY_FOR_TOOLS = 20;
const LLM_SILENCE_MS = 3000;
const MAX_OUTPUT_TOKENS_PER_ROUND = 65535;

const MAX_EMBED_TEXTS_PER_CALL = 32;
const MAX_EMBED_CHARS = 8000;
const EMBED_TIMEOUT_MS = 15000;

const FILE_PREVIEW_LINES = 10;
const FILE_PREVIEW_CHARS = 500;
const FILE_INDEX_CHARS = 2500;

const FREE_TIER_TPM_LIMIT = 250000;
const ESTIMATED_TOKENS_PER_CHAR = 0.25;

const GEMINI_FILES_UPLOAD_URL = 'https://generativelanguage.googleapis.com/upload/v1beta/files';
const GEMINI_FILES_BASE       = 'https://generativelanguage.googleapis.com/v1beta';
const GEMINI_FILE_CACHE_PREFIX = 'gfile:';
const GEMINI_FILE_CACHE_TTL    = 47 * 60 * 60;
const GEMINI_FILE_UPLOAD_TIMEOUT_MS = 60000;
const GEMINI_FILE_UPLOAD_BUCKET = 'gfile_rl';
const GEMINI_FILE_UPLOAD_MAX_PER_MIN = 2;

const STICKY_MODEL_KEY = 'sticky:model';
const STICKY_MODEL_TTL = 5 * 60;

const SEARCH_CACHE_PREFIX = 'scache:';
const SEARCH_CACHE_TTL = 10 * 60;

const CONTINUATION_PROMPT =
  'Resume exactly where the previous message stopped. Output only the continuation — do not repeat any earlier text, do not add a prefix, do not acknowledge this instruction. Start mid-sentence as if the previous message and this one are one continuous reply.';

const RECITATION_CONTINUATION_PROMPT =
  'Resume the analysis from exactly where the previous message stopped. Rewrite every idea in completely different sentence structures and word choices — paraphrase aggressively. Do NOT quote, do NOT reproduce section headings or list items verbatim, do NOT copy any sentence from the source document. Output only the continuation, mid-sentence, with no prefix and no acknowledgment.';

const SUPPORTED_MIMES = new Set([
  'image/png','image/jpeg','image/jpg','image/webp','image/heic','image/heif',
  'image/gif','image/bmp','image/tiff','image/svg+xml',
  'application/pdf',
  'audio/mpeg','audio/mp3','audio/wav','audio/x-wav','audio/aiff','audio/aac',
  'audio/ogg','audio/flac','audio/x-flac',
  'video/mp4','video/mpeg','video/mov','video/quicktime','video/avi','video/x-msvideo',
  'video/webm','video/x-ms-wmv','video/3gpp','video/x-flv','video/mpg',
  'text/plain','text/markdown','text/csv','text/tsv','text/tab-separated-values',
  'text/html','text/css','text/xml','text/yaml','text/x-yaml','text/x-log',
  'text/x-python','text/x-java','text/x-c','text/x-cpp','text/x-rust',
  'application/json','application/jsonl','application/x-ndjson','application/ld+json',
  'application/xml','application/xhtml+xml','application/atom+xml','application/rss+xml',
  'application/javascript','application/x-javascript','application/ecmascript',
  'application/x-yaml','application/yaml','application/toml',
  'application/x-sh','application/x-shellscript','application/sql','application/graphql',
  'application/x-python','application/x-ruby','application/x-rust','application/x-go','application/x-java',
]);

function isSupportedMime(m) {
  if (!m) return false;
  const lower = String(m).toLowerCase().split(';')[0].trim();
  if (SUPPORTED_MIMES.has(lower)) return true;
  if (lower.startsWith('image/'))  return true;
  if (lower.startsWith('audio/'))  return true;
  if (lower.startsWith('video/'))  return true;
  if (lower.startsWith('text/'))   return true;
  if (lower.startsWith('application/')
      && /(json|ndjson|jsonl|xml|yaml|toml|csv|javascript|ecmascript|sql|graphql|sh\b|shell)/.test(lower))
    return true;
  return false;
}
function isImageMime(m) { return !!m && String(m).toLowerCase().startsWith('image/'); }

function safeStr(x) {
  if (x == null) return 'Unknown error';
  if (typeof x === 'string') return x;
  if (x instanceof Error) return x.message || 'Error';
  if (typeof x === 'object') { try { return JSON.stringify(x); } catch { return String(x); } }
  return String(x);
}
function log(...args) { if (DEBUG) console.log(`[zebai ${WORKER_VERSION}]`, ...args); }

function extractStatus(e) {
  const errStr = String(e);
  const httpMatch = errStr.match(/\b(\d{3})\b/);
  if (e && e.status) return e.status;
  if (httpMatch) { const n = parseInt(httpMatch[1]); if (n >= 100 && n < 600) return n; }
  if (/abort|timeout|network|fetch failed|ECONNRESET|ETIMEDOUT/i.test(errStr)) return 503;
  return 500;
}
function isCapacityError(status, msg) {
  const m = String(msg || '').toLowerCase();
  return status === 503 || status === 502 || status === 504
    || /high demand|overloaded|capacity|unavailable/i.test(m);
}
function parseRetryAfterMs(response, errorBody) {
  try {
    const h = response && response.headers && typeof response.headers.get === 'function'
            ? response.headers.get('Retry-After') : null;
    if (h) {
      const n = parseFloat(h);
      if (!isNaN(n) && n > 0) return Math.min(n * 1000, 60000);
      const d = Date.parse(h);
      if (!isNaN(d)) return Math.min(Math.max(0, d - Date.now()), 60000);
    }
  } catch (e) {}
  try {
    const parsed = typeof errorBody === 'string' ? JSON.parse(errorBody) : errorBody;
    const details = parsed?.error?.details || [];
    for (const d of details) {
      if (d['@type'] === 'type.googleapis.com/google.rpc.RetryInfo' && d.retryDelay) {
        const m = String(d.retryDelay).match(/^([\d.]+)s$/);
        if (m) return Math.min(parseFloat(m[1]) * 1000, 60000);
      }
    }
  } catch (e) {}
  return null;
}

const ERROR_CODES = {
  OK:'success',429:'rate limit',503:'capacity',500:'server error',
  502:'bad gateway',504:'gateway timeout',404:'model not found',
  401:'unauthorized',403:'forbidden',C:'worker cooldown',K:'no keys',
  T:'timeout',N:'network error',S:'stream failed',X:'unknown',
};
function codeForStatus(status, msg) {
  const m = String(msg || '').toLowerCase();
  if (status === 0 || status === 200) return 'OK';
  if (status === 429) return '429';
  if (status === 503) return '503';
  if (status === 500) return '500';
  if (status === 502) return '502';
  if (status === 504) return '504';
  if (status === 404) return '404';
  if (status === 401) return '401';
  if (status === 403) return '403';
  if (/abort|timeout/i.test(m)) return 'T';
  if (/network|econnreset|etimedout|fetch failed/i.test(m)) return 'N';
  return 'X';
}
function formatErrorFingerprint(slotCodes) {
  const parts = Object.keys(slotCodes)
    .sort((a, b) => parseInt(a.slice(1), 10) - parseInt(b.slice(1), 10))
    .map(slot => `${slot}-${slotCodes[slot] || 'X'}`);
  return `E=${parts.join('/')}`;
}

async function fetchWithTimeout(url, opts, timeout, label = '', retries = 0, externalSignal = null) {
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    let signal = controller.signal;
    if (externalSignal) {
      if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.any === 'function') {
        signal = AbortSignal.any([controller.signal, externalSignal]);
      } else {
        if (externalSignal.aborted) controller.abort();
        else externalSignal.addEventListener('abort', () => controller.abort(), { once: true });
      }
    }
    const timer = setTimeout(() => controller.abort(), timeout);
    const t0 = Date.now();
    try {
      const res = await fetch(url, { ...opts, signal });
      clearTimeout(timer);
      if (DEBUG) log(`[fetch${label ? ' ' + label : ''}] ${res.status} (${Date.now() - t0}ms)${attempt > 0 ? ` [retry ${attempt}]` : ''}`);
      if (res.status >= 500 && attempt < retries) {
        lastErr = new Error(`HTTP ${res.status}`);
        await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 500));
        continue;
      }
      return res;
    } catch (e) {
      clearTimeout(timer);
      lastErr = e;
      log(`[fetch${label ? ' ' + label : ''}] FAILED (${Date.now() - t0}ms)${attempt > 0 ? ` [retry ${attempt}]` : ''} — ${safeStr(e)}`);
      if (externalSignal && externalSignal.aborted) throw e;
      if (attempt < retries) { await new Promise(r => setTimeout(r, Math.pow(2, attempt) * 500)); continue; }
      throw e;
    }
  }
  throw lastErr;
}

function capToolResult(text, max = MAX_TOOL_RESULT_CHARS) {
  if (typeof text !== 'string') return text;
  if (text.length <= max) return text;
  return text.slice(0, max) + `\n…[truncated ${text.length - max} chars]`;
}

function sanitizeAssistantContent(content) {
  let text = String(content || '');
  text = text.replace(/<thinking>[\s\S]*?<\/thinking>/g, '');
  text = text.replace(/<think>[\s\S]*?<\/think>/g, '');
  text = text.replace(/<thought>[\s\S]*?<\/thought>/g, '');
  text = text.replace(/<\/?(thinking|think|thought)>/g, '');
  text = text.replace(/<(weather|search|finance|chart|analyse|run|analysing|vid)>[\s\S]*?<\/\1>/g, '');
  text = text.replace(/Tool execution result:[\s\S]*?(?=\n\n|$)/g, '');
  return text.replace(/\n{3,}/g, '\n\n').trim();
}
function escapeUserToolTags(text) {
  return String(text || '').replace(
    /<\/?(search|analyse|weather|finance|chart|vid|run|analysing|open_url|fullscreen|thinking|think|thought|title|tool)>/gi,
    (m) => m.replace(/</g, '&lt;').replace(/>/g, '&gt;')
  );
}
function buildHistoryForLLM(chatMessages) {
  const out = [];
  for (const m of chatMessages) {
    if (m.role === 'assistant') {
      const clean = sanitizeAssistantContent(m.content);
      if (clean) out.push({ role: 'assistant', content: clean });
    } else if (m.role === 'user' && m.content) {
      const stripped = String(m.content)
        .replace(/!\[.*?\]\((?:data|blob):[^)]+\)\n?/g, '')
        .replace(/\[Attached:\s*([^\]]+?)\s*\(([^)]+)\)(?:\s*blob:[a-zA-Z0-9-]+)?\]/g, '[Attached: $1 ($2)]')
        .trim();
      const finalContent = stripped || '[User sent a file with no accompanying text.]';
      out.push({ role: 'user', content: escapeUserToolTags(finalContent) });
    }
  }
  return out;
}

function sanitizeChartSpec(raw) {
  let t = String(raw || '').trim();
  t = t.replace(/^```[a-zA-Z0-9_-]*\s*/, '').replace(/\s*```\s*$/, '').trim();
  if (!t.startsWith('{') && !t.startsWith('[')) {
    const startObj = t.indexOf('{');
    const startArr = t.indexOf('[');
    let start = -1;
    if (startObj === -1) start = startArr;
    else if (startArr === -1) start = startObj;
    else start = Math.min(startObj, startArr);
    if (start !== -1) {
      const openCh  = t[start];
      const closeCh = openCh === '{' ? '}' : ']';
      let depth = 0, inStr = false, esc = false, end = -1;
      for (let i = start; i < t.length; i++) {
        const c = t[i];
        if (esc) { esc = false; continue; }
        if (c === '\\') { esc = true; continue; }
        if (inStr) { if (c === '"') inStr = false; continue; }
        if (c === '"') { inStr = true; continue; }
        if (c === openCh) depth++;
        else if (c === closeCh) { depth--; if (depth === 0) { end = i; break; } }
      }
      if (end !== -1) t = t.slice(start, end + 1);
    }
  }
  return t;
}

// ---------------------------------------------------------------------------
// isTruncatedStop — v106.27.0
//
// Google AI Studio does NOT send a finishReason when it cuts a stream short.
// A genuinely truncated reply arrives as finishReason === null and is caught
// upstream by missingFinishReason. STOP is authoritative: the model finished
// its turn. The only structural signal we still trust is an unbalanced
// code fence.
// ---------------------------------------------------------------------------
function isTruncatedStop(result) {
  if (result.finishReason !== 'STOP') return false;
  if (!result.sawText) return false;
  const text = String(result.text || '').trimEnd();
  if (!text) return false;
  const fenceCount = (text.match(/```/g) || []).length;
  return fenceCount % 2 !== 0;
}

// ---------------------------------------------------------------------------
// 1. STATEFUL XML PARSER
// ---------------------------------------------------------------------------
class StatefulXMLParser {
  static FETCH_TOOLS = new Set(['weather', 'search', 'finance', 'analyse', 'run', 'analysing']);
  static SOFT_TOOLS = new Set(['chart']);
  static ALL_TOOLS = new Set([...StatefulXMLParser.FETCH_TOOLS, ...StatefulXMLParser.SOFT_TOOLS]);
  static THINK_TAGS = new Set(['think', 'thinking', 'thought']);
  constructor(onEvent, options = {}) {
    this.onEvent = onEvent;
    this.allowTools = options.allowTools !== false;
    this.allowThinking = options.allowThinking === true;
    this.state = 'TEXT';
    this.tagBuf = '';
    this.mode = 'NORMAL';
    this.thinkTag = null;
    this.toolName = null;
    this.toolContent = '';
    this.textBatch = '';
    this.thinkBatch = '';
    this.tools = [];
    this.charts = [];
    this.sawText = false;
    this.toolDetected = false;
    this.dropText = false;
  }
  feed(chunk) {
    if (typeof chunk !== 'string' || !chunk) return;
    for (let i = 0; i < chunk.length; i++) this._step(chunk[i]);
    this._flushBatches();
  }
  flush() {
    if (this.mode === 'THINKING') this._closeThinking();
    if (this.mode === 'TOOL') {
      if (this.toolName === 'chart' && this.toolContent.trim()) this._closeTool();
      else { this.toolName = null; this.toolContent = ''; this.mode = 'NORMAL'; }
    }
    if (this.state === 'TAG_OPEN' && this.tagBuf) {
      this._appendToCurrent(this.tagBuf); this.tagBuf = ''; this.state = 'TEXT';
    }
    this._flushBatches();
    return { tools: this.tools, charts: this.charts, sawText: this.sawText, toolDetected: this.toolDetected };
  }
  _flushBatches() {
    if (this.textBatch) { this.onEvent({ type: 'text', content: this.textBatch }); this.textBatch = ''; }
    if (this.thinkBatch) { this.onEvent({ type: 'thinking', content: this.thinkBatch }); this.thinkBatch = ''; }
  }
  _appendToCurrent(s) {
    if (!s) return;
    if (this.mode === 'THINKING') this.thinkBatch += s;
    else if (this.mode === 'TOOL') this.toolContent += s;
    else { if (this.dropText) return; this.textBatch += s; this.sawText = true; }
  }
  _step(ch) {
    if (this.state === 'TEXT') {
      if (ch === '<') { this.state = 'TAG_OPEN'; this.tagBuf = '<'; return; }
      this._appendToCurrent(ch); return;
    }
    if (ch === '>') { this.tagBuf += '>'; this._processTag(); return; }
    if (ch === '<') { this._appendToCurrent(this.tagBuf); this.tagBuf = '<'; return; }
    this.tagBuf += ch;
  }
  _processTag() {
    const raw = this.tagBuf;
    this.tagBuf = ''; this.state = 'TEXT';
    if (raw.length < 3 || raw[0] !== '<' || raw[raw.length - 1] !== '>') { this._appendToCurrent(raw); return; }
    const inner = raw.slice(1, -1);
    const isClosing = inner.startsWith('/');
    const body = isClosing ? inner.slice(1) : inner;
    const trimmed = body.trim();
    const hasAttrs = /\s/.test(trimmed);
    const name = (hasAttrs ? trimmed.split(/\s+/)[0] : trimmed).toLowerCase();
    if (this.mode === 'THINKING') {
      if (isClosing && !hasAttrs && name === this.thinkTag) this._closeThinking();
      else this.thinkBatch += raw;
      return;
    }
    if (this.mode === 'TOOL') {
      if (isClosing && !hasAttrs && name === this.toolName) this._closeTool();
      else this.toolContent += raw;
      return;
    }
    if (this.allowThinking && !isClosing && !hasAttrs && StatefulXMLParser.THINK_TAGS.has(name)) {
      this.textBatch = ''; this.thinkTag = name; this.mode = 'THINKING';
      this.onEvent({ type: 'thinking_start' });
      return;
    }
    if (this.allowTools && !isClosing && !hasAttrs && StatefulXMLParser.ALL_TOOLS.has(name)) {
      this.textBatch = ''; this.toolName = name; this.toolContent = ''; this.mode = 'TOOL';
      if (StatefulXMLParser.FETCH_TOOLS.has(name)) this.toolDetected = true;
      this.onEvent({ type: 'tool_start', name });
      return;
    }
    if (!this.allowThinking && !hasAttrs && StatefulXMLParser.THINK_TAGS.has(name)) return;
    this._appendToCurrent(raw);
  }
  _closeThinking() {
    this._flushBatches();
    this.mode = 'NORMAL'; this.thinkTag = null;
    this.onEvent({ type: 'thinking_end' });
  }
  _closeTool() {
    let content = this.toolContent.trim();
    const name = this.toolName;
    if (name === 'chart') {
      content = sanitizeChartSpec(content);
      this.charts.push({ name, content });
      this.onEvent({ type: 'chart', content });
    } else {
      this.tools.push({ name, content });
      this.onEvent({ type: 'tool_end', name, content });
      this.dropText = true;
    }
    this.mode = 'NORMAL'; this.toolName = null; this.toolContent = '';
  }
}

// ---------------------------------------------------------------------------
// 2. SYSTEM PROMPT
// ---------------------------------------------------------------------------
function getSystemPrompt(mode, date, { hasImage = false, hasFile = false, fileIndex = '' } = {}) {
  const fileSection = fileIndex ? `\n\n# Files in this conversation\n\n${fileIndex}` : '';
  const vision = mode === 'vision' || mode === 'vision-agent';

  const base = `You are ZebAI. Today is ${date}.

# Reply contract

Every reply is EXACTLY ONE of:
1. **A tool call** — nothing but the tool tag(s). No prose.
2. **A final answer** — detailed Markdown, optionally starting with one \`<chart>\`.

Never both. When you emit a fetch tool tag, the reply ends there.

# Tool tag purity

A tool call reply contains ONLY the tags. No period, comma, space, newline, or prose around them. If a reply has any non-tag character while trying to call a tool, the parser drops your tool call and treats the reply as a final answer.

    Correct:   <search>Tokyo weather</search><weather>Tokyo</weather>
    Wrong:     Let me check. <search>Tokyo weather</search>.
    Wrong:     <search>Tokyo weather</search>, <weather>Tokyo</weather>
    Wrong:     <search>Tokyo</search>\n<weather>Tokyo</weather>

# Parallel tool calls — same tool OR independent tools

Fire independent calls together in ONE reply. This is the single biggest speed lever.

**Same tool, multiple arguments — always parallel:**

    Correct:   <weather>Tokyo</weather><weather>London</weather><weather>NYC</weather>
    Correct:   <finance>{"type":"stock","symbol":"AAPL"}</finance><finance>{"type":"stock","symbol":"MSFT"}</finance>
    Wrong:     one tag per round

**Different tools with independent jobs — also parallel:**

    Correct:   <weather>NYC</weather><finance>{"type":"stock","symbol":"AAPL"}</finance><run>231331311/233</run>

    These are three separate questions. None needs the output of the others. Batch them.

**Sequential when one tool's output feeds another:**

    Correct:   Round 1:  <finance>{"type":"stock","symbol":"AAPL"}</finance>
               Round 2:  <run>price / 3</run>

    Wrong:     <finance>{"type":"stock","symbol":"AAPL"}</finance><run>price / 3</run>
               (the <run> can't see the price yet — it doesn't exist)

**The test before you batch:** does call B need output from call A? If no, batch them. If yes, sequence them across rounds.

Cap: 4 tags per reply. Mixing families is fine — the round just can't exceed 4.

# Tools

    <search>query</search>                      Live web search.
    <analyse>https://exact-url</analyse>        Read a specific URL in full.
    <weather>City</weather>                     Current weather.
    <finance>{"type":"stock","symbol":"AAPL"}</finance>
    <finance>{"type":"forex","base":"USD","target":"INR"}</finance>
    <run>javascript</run>                       Execute JS in the sandbox — see below.
    <analysing>filename.ext</analysing>         Re-attach a file from an EARLIER turn.
    <chart>{...}</chart>                        Chart inside the final answer.

# Choosing a tool — run this checklist before every reply

- **Stable fact in training data?** → answer directly. No tool.
- **Live / current / changes over time?** → \`<search>\`, \`<weather>\`, or \`<finance>\`.
- **Any math at all, even "15% of 82"?** → \`<run>\`. Never compute in your head.
- **A specific URL the user gave you, or one a search snippet pointed at?** → \`<analyse>\`.
- **A file from an EARLIER turn the user is referring to?** → \`<analysing>\`.
- **A file on the CURRENT message?** → ALREADY in your context. Answer directly. NEVER call \`<analysing>\` on it.

Never search for what you know. Never duplicate a call. Never fire a tool "just to be safe".

Before you answer, ask: which of these calls can go in parallel, and which depend on each other? Batch the independent ones. Sequence the dependent ones.

# Search → analyse — highly recommended

Search gives you headlines. \`<analyse>\` gives you the source. The difference between a thin answer and a real one is usually one round of \`<analyse>\`.

**Highly recommended after every search round:**
- The question is a research question, comparison, or "what's the latest".
- A snippet says "according to" / "reported that" / "sources said" — the detail is one click away.
- The snippets disagree with each other and you need to see which is right.
- You need quotes, numbers, or specific facts to write a confident answer.

**Skip only when:**
- A single value is already in the snippet ("Bitcoin price" → "$62,000").
- The user asked a yes/no or one-line lookup and the search answered it.

**Shape:** search round → analyse round → final answer. Two rounds. Three is rare.

**Example:**
    Round 1:  <search>latest AI news this week</search><search>OpenAI announcements October 2026</search>
    Round 2:  <analyse>https://techcrunch.com/...</analyse><analyse>https://theverge.com/...</analyse>
    Round 3:  final answer, with quotes and dates from both articles

You decide. But when in doubt, analyse.

# Chart — first block of the final answer

One \`<chart>\` per reply. Raw JSON, no fences, no prose. Must be the **first** thing in the reply.

Emit when it genuinely helps — 3+ comparisons, trends, distributions. Not for a single number.

## Schema per type

**bar, hbar, line, area, stackedBar, stackedArea** — labels + values, or labels + datasets:

    <chart>{"type":"bar","title":"Revenue","labels":["Q1","Q2","Q3"],"values":[120,135,98]}</chart>

    <chart>{"type":"line","title":"Growth","labels":["2022","2023","2024"],"datasets":[{"label":"Users","data":[10,40,120]},{"label":"Revenue","data":[5,20,80]}]}</chart>

**pie, doughnut, polarArea** — labels + values, same length:

    <chart>{"type":"pie","title":"Market Share","labels":["Chrome","Safari","Firefox"],"values":[65,18,3]}</chart>

**gauge** — single value with min/max:

    <chart>{"type":"gauge","title":"CPU Load","values":[72],"min":0,"max":100}</chart>

**scatter, bubble** — datasets with point objects:

    <chart>{"type":"scatter","title":"Height vs Weight","datasets":[{"label":"People","data":[{"x":170,"y":65},{"x":180,"y":78}]}]}</chart>

    <chart>{"type":"bubble","title":"Cities","datasets":[{"label":"Population","data":[{"x":100,"y":200,"r":30}]}]}</chart>

**radar** — labels + datasets:

    <chart>{"type":"radar","title":"Skill Profile","labels":["Speed","Power","Accuracy"],"datasets":[{"label":"Player A","data":[8,6,9]}]}</chart>

## Rules

- Keys in double quotes. Numbers as numbers, not strings.
- \`labels.length\` must equal \`values.length\` (or each dataset's \`data.length\`).
- \`title\` is a short string, plain text, no formatting.
- Never wrap in \`\`\` fences.
- Never write prose inside the \`<chart>\` tag.
- Never emit two charts in one reply.
- If unsure of the type, use \`bar\` with labels + values.

# URLs — hard rule

Every URL in your answer must be one that **literally appeared in a tool result this turn**, or one **the user typed in their message**. Nothing else.

**You may not invent, guess, shorten, lengthen, or modify a URL.** You may not use a URL from training data. You may not construct a plausible-looking URL like "openai.com/blog/gpt-5" — even if you are certain such a page exists, you don't have a verified URL for it this turn.

When you want to cite a source:

- **If a tool result this turn contains the URL** → use it exactly, character for character. Do not strip tracking parameters. Do not add or remove a trailing slash.
- **If no tool result contains the URL** → mention the source by name in plain text. No link. No URL.

    Bad:  [OpenAI's announcement](https://openai.com/blog/gpt-5)     ← invented
    Bad:  [source](https://example.com)                              ← generic
    Bad:  [Verge](https://theverge.com)                              ← bare domain
    Good: [OpenAI's announcement](https://openai.com/index/gpt-5/)   ← exact match from search
    Good: The Verge reported that…                                   ← plain text, no link

This applies to every link in every reply: prose, bullet lists, tables, follow-ups. No exceptions.

# Run — use it for everything it can do

The \`<run>\` sandbox is a full JavaScript interpreter. It's exact. Your head is not. If the answer involves any of the following, use \`<run>\`:

**Always use \`<run>\` for:**
- Any arithmetic — even "15% of 82". Never compute in your head.
- Any date arithmetic — days between dates, "what date is 30 days from now".
- Any unit conversion the sandbox can express.
- Any string manipulation — split, join, regex, padding, case.
- Any array or object transform — sort, filter, map, aggregate.
- Any percentage, compound interest, growth rate, average.
- Any comparison of numbers you need to decide on.
- Any encoding, base conversion, or hash.
- Any data shape change — CSV → JSON, flattening, grouping.

**Sandbox has:** expression grammar, \`let/const/var\`, destructuring, functions, closures, classes, loops, \`try/catch\`, template literals, arrays, objects, \`Map\`, \`Set\`, \`RegExp\`, all \`Math.*\`, \`JSON.parse/stringify\`, \`Date.now/parse/UTC\`, \`console.log\`. A bare expression at the end prints its value.

**Sandbox does NOT have:** network, timers, DOM, files, \`eval\`, \`new Date()\` (use \`Date.now()\`), async.

**Patterns:**

    Simple math:     <run>15/100 * 82</run>                                    → 12.3
    Compound:        <run>const p=1000,r=.05,n=12; console.log(p*Math.pow(1+r/n,n*10))</run>
    Date diff:       <run>(Date.UTC(2026,8,27) - Date.UTC(2024,0,15)) / 86400000</run>
    Data transform:  <run>console.log([3,1,2].sort((a,b)=>a-b).join(","))</run>

When in doubt, run it. The sandbox is instant and exact.

# Answer depth

Detailed by default. Lead with the answer.

    Fact / definition      3–5 sentences with context + example.
    Calculation            result + working + interpretation.
    Comparison             table or facing paragraphs (3–5 points each).
    News / roundup         bullets with source + interpretation.
    Data lookup            headline number, fields, trend note.
    Code                   block + what it does + edge cases + usage.
    Research / analysis    ## sections: conclusion, evidence, caveats.

Include specifics: numbers, names, dates. One line of interpretation at the end.

# Closing rule

Every substantive answer ends with exactly ONE short follow-up — a natural next question or offer, 5–15 words, on-topic, no filler.

This is a completion signal. If the answer ends mid-sentence or without a follow-up, the user assumes it was cut off.

    Good: "Want me to dig into the token-cost side?"
    Good: "Curious how this compares to the Anthropic SDK?"
    Good: "Shall I keep going, or is this enough?"

    Bad:  "Anything else?"
    Bad:  "Hope this helps!"
    Bad:  "Feel free to ask!"
    Bad:  ending on a bare period with no follow-up

Skip the follow-up ONLY for: one-word replies ("Yo.", "Anytime."), pure math results, and single-value lookups where a follow-up would be absurd.

# Voice

Smart friend texting. Concrete over abstract. Em-dashes for asides. Vary sentence length. No "Sure!", no "Great question!", no hedging, no corporate voice.

# Casual conversation

For "hi", "hey", "hello", "yo", "thanks", "bye", "good morning" — reply like a person. One or two sentences. Match their energy.

**Do NOT:** list tools, explain ZebAI, describe capabilities, offer a menu, ask "how can I assist", add follow-up suggestions, use emojis.

**Do:** say hi back. "Hey. What's up?" is complete. Match short with short.

    "hi"       → "Hey. What's up?"
    "thanks!"  → "Anytime."
    "yo"       → "Yo."

The single failure mode to avoid: "Hello! I'm ZebAI, an AI assistant with seven tools..." — never write that. Just say hi back.

**One exception:** if the user asks "what can you do" or "what tools do you have", answer in plain prose (no tags). Name the tools in a short list, no pitch.

# Markdown

- Headings: \`##\` (never \`#\`), \`###\` for sub-sections.
- Bullets: always \`-\`. Never \`*\` or \`+\`.
- Bold \`**key term**\` sparingly. Italic \`*word*\` for a foreign term or emphasis.
- Inline code \`code\` for filenames, commands, functions.
- Code blocks: triple backticks with the language tag.
- Tables: 3+ items × 2+ attributes. Header separator row required.
- Links: \`[label](url)\`. Never bare URLs.

# LaTeX

Use ONLY for real math, physics, chemistry notation.

    Use:      $E = mc^2$, $\\int_0^1 x\\,dx$, $\\text{2H}_2 + \\text{O}_2$
    NEVER:    prices ($49.99 plain), dates, temperatures, percentages,
              distances, chemical names in prose.

Rules:
- No Markdown inside math — \`$x = 5$\`, never \`$**x** = 5$\`.
- No unclosed \`$\` — an open dollar swallows the rest of the paragraph.
- Display math (\`$$...$$\`) on its own line, alone.
- Never write raw LaTeX commands outside math delimiters.

# Formatting safety

- Never nest code fences. Use \`~~~\` if you must show a fenced block inside a fence.
- Close every fence, every \`**\`, every \`*\`.
- Never write raw HTML — DOMPurify strips it. Use Markdown instead.
- Never write inline SVG or MathML. Charts go through \`<chart>\`, math through LaTeX.
- One \`<chart>\` per reply, always first.
- To show HTML as an example, wrap it in \`\`\`html.

# Anti-patterns

Never write: "What I looked up:", "Specific values:", "Interpretation:", a tool tag wrapped in prose, a trailing period after a tool tag, an invented tool result, a <chart> tag anywhere except the first position, a capabilities pitch in response to a greeting, a long preamble or "in conclusion" summary, a URL that didn't appear in a tool result this turn, a URL from training data presented as if it came from a search.`;

  if (vision) {
    const attachmentLine = hasImage && hasFile
      ? 'The user attached images and files.'
      : hasFile ? 'The user attached files.' : 'The user attached images.';
    return `${base}

# Files on the current message — do not call a tool

${attachmentLine}

**The file(s) attached to this exact message are ALREADY loaded as native input to this turn. They are in your context right now.**

Do NOT call \`<analysing>\` on them. Do NOT call \`<analyse>\` on them. Do NOT call any file tool. Just read them and answer.

    WRONG:  <analysing>report.pdf</analysing>       ← already loaded, this re-fetches nothing
    WRONG:  <analyse>report.pdf</analyse>           ← wrong tag entirely, this is for URLs
    RIGHT:  (no tag) — answer directly from the file content

**Tag reference — do not mix these up:**

    <analyse>https://example.com</analyse>          reads a live WEB PAGE from a URL
    <analysing>report.pdf</analysing>               re-attaches a FILE from an EARLIER turn

Neither applies to a file on the current message. Both apply only when the user is asking you to look at something that isn't already in your context.

**When you should use \`<analysing>\`:** only if the user refers to a file from a *previous* turn — "that PDF I sent earlier", "the CSV from before", "re-read report.pdf". In that case, the file is no longer in your context and you must re-attach it.

**When you should use \`<analyse>\`:** only if the user gives you a URL or you're reading a specific page you found via search. Never for a file.

Read the attachment(s). Describe specific values, labels, names. Be detailed.

**Paraphrase rule — CRITICAL.** When summarising, analysing, or extracting from a document, rewrite every idea in your own words. Do not quote sentences verbatim. Do not reproduce section headings, definitions, or list items as they appear in the source. Gemini's recitation filter terminates the stream silently when your output too closely matches the input. Paraphrase aggressively — new sentence structures, new word choices.${fileSection}

---

Your turn. Read the attached file(s) and answer. No tool tag needed.`;
  }

  return `${base}${fileSection}

---

Your turn. Emit a tool tag, or write the answer.`;
}

// ---------------------------------------------------------------------------
// 3. AI TITLE
// ---------------------------------------------------------------------------
const cleanTitleString = t => {
  const str = String(t || '').trim();
  const match = str.match(/<title>([\s\S]*?)<\/title>/i);
  if (match) return match[1].trim().replace(/^["'`]+|["'`.]+$/g, '').replace(/\s+/g, ' ').split('\n')[0].trim();
  const openOnly = str.match(/<title>([\s\S]*)/i);
  if (openOnly) return openOnly[1].replace(/<\/?[^>]+>/g, '').replace(/^["'`]+|["'`.]+$/g, '').replace(/\s+/g, ' ').split('\n')[0].trim();
  return str.replace(/^["'`]+|["'`.]+$/g, '').replace(/\s+/g, ' ').split('\n')[0].trim();
};

const TITLE_PROVIDERS = [
  { name: 'Google', model: 'gemini-3.5-flash-lite', keyEnv: 'GOOGLE_KEYS' },
  { name: 'Google', model: 'gemini-3.1-flash-lite', keyEnv: 'GOOGLE_KEYS' },
];

function titleThinkingConfig(model) {
  const m = String(model || '').toLowerCase();
  if (/gemini-3/.test(m)) return { thinkingLevel: 'minimal', includeThoughts: false };
  if (/flash-lite-latest/.test(m)) return { thinkingLevel: 'minimal', includeThoughts: false };
  if (/gemini-2\.5-flash-lite/.test(m)) return { thinkingBudget: 0, includeThoughts: false };
  if (/gemini-2\.5-flash/.test(m)) return { thinkingBudget: 0, includeThoughts: false };
  return null;
}

async function tryTitleFromGemini(provider, key, prompt) {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(provider.model)}:generateContent?key=${key}`;
  const res = await fetchWithTimeout(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      contents: [{ role: 'user', parts: [{ text: prompt }] }],
      generationConfig: (() => {
        const cfg = { maxOutputTokens: 32, temperature: 0.4, topP: 0.9 };
        const tc = titleThinkingConfig(provider.model);
        if (tc) cfg.thinkingConfig = tc;
        return cfg;
      })(),
    }),
  }, 5000, `title-${provider.model}`, 1);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const j = await res.json();
  const parts = j?.candidates?.[0]?.content?.parts || [];
  return parts.filter(p => p.text && p.thought !== true).map(p => p.text).join('') || '';
}

async function generateAITitle(env, userContent, assistantContent) {
  const cleanUser = String(userContent || '').replace(/!\[.*?\]\(.*?\)/g, '').replace(/\[Attached:[^\]]+\]/g, '').replace(/\[File:[^\]]*\][\s\S]*$/m, '').trim();
  const cleanAssistant = String(assistantContent || '').replace(/<thinking>[\s\S]*?<\/thinking>/g, '').replace(/<(?:weather|search|finance|chart|vid|analyse|run|analysing)>[\s\S]*?<\/(?:weather|search|finance|chart|vid|analyse|run|analysing)>/g, '').replace(/\s+/g, ' ').trim();
  let userSeed = cleanUser; if (!userSeed) userSeed = cleanAssistant.slice(0, 300);
  if (!userSeed) return null;
  const prompt = `You are a chat-title generator. Output a SHORT, SPECIFIC title describing WHAT THIS CONVERSATION IS ABOUT.

RULES:
- 3 to 6 words, Title Case
- No quotes, no trailing punctuation, no emoji
- Output ONLY the title

EXAMPLES:
"hi" → Casual Greeting
"what's the weather in tokyo" → Tokyo Weather Check
"AAPL stock price" → Apple Stock Price

USER: ${userSeed.slice(0, 400)}
ASSISTANT: ${cleanAssistant.slice(0, 300)}

Title:`;
  for (const provider of TITLE_PROVIDERS) {
    const available = await getAvailableKeys(env, provider.keyEnv);
    if (!available.length) continue;
    for (const key of available) {
      try {
        const raw = await tryTitleFromGemini(provider, key, prompt);
        let title = cleanTitleString(raw);
        if (!title) continue;
        if (/^(title|output|assistant|user)[:\s]/i.test(title)) title = title.replace(/^[^:]+:\s*/i, '');
        if (title.length < 3) continue;
        if (title.length > 60) {
          const cut = title.slice(0, 60); const lastSpace = cut.lastIndexOf(' ');
          title = (lastSpace > 30 ? cut.slice(0, lastSpace) : cut).trim() + '…';
        }
        return title;
      } catch (e) { continue; }
    }
  }
  const words = userSeed.split(/\s+/).filter(Boolean).slice(0, 4);
  if (!words.length) return null;
  let fallback = words.map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  if (fallback.length > 48) fallback = fallback.slice(0, 48).trim();
  return fallback;
}

// ---------------------------------------------------------------------------
// 4. PROVIDER REGISTRY
// ---------------------------------------------------------------------------
const GEMINI_TEXT_TIMEOUT   = 45000;
const GEMINI_CODE_TIMEOUT   = 60000;
const GEMINI_VISION_TIMEOUT = 50000;
function googleModel(model, timeout, maxTokens = MAX_OUTPUT_TOKENS_PER_ROUND) {
  return { name: 'Google', model, keyEnv: 'GOOGLE_KEYS', type: 'gemini-native', maxTokens, timeout };
}

const PROVIDERS = {
  text: [
    googleModel('gemini-3.5-flash-lite', GEMINI_TEXT_TIMEOUT),
    googleModel('gemini-3.1-flash-lite', GEMINI_TEXT_TIMEOUT),
    googleModel('gemini-2.5-flash-lite', GEMINI_TEXT_TIMEOUT),
    googleModel('gemini-2.5-flash', GEMINI_TEXT_TIMEOUT),
  ],
  code: [
    googleModel('gemini-3.5-flash-lite', GEMINI_CODE_TIMEOUT),
    googleModel('gemini-3.1-flash-lite', GEMINI_CODE_TIMEOUT),
    googleModel('gemini-2.5-flash-lite', GEMINI_CODE_TIMEOUT),
    googleModel('gemini-2.5-flash', GEMINI_CODE_TIMEOUT),
  ],
  vision: [
    googleModel('gemini-3.5-flash-lite', GEMINI_VISION_TIMEOUT),
    googleModel('gemini-3.1-flash-lite', GEMINI_VISION_TIMEOUT),
    googleModel('gemini-2.5-flash-lite', GEMINI_VISION_TIMEOUT),
    googleModel('gemini-2.5-flash', GEMINI_VISION_TIMEOUT),
  ],
};
PROVIDERS['vision-agent'] = [
  googleModel('gemini-3.5-flash-lite', GEMINI_CODE_TIMEOUT),
  googleModel('gemini-3.1-flash-lite', GEMINI_CODE_TIMEOUT),
  googleModel('gemini-2.5-flash-lite', GEMINI_CODE_TIMEOUT),
  googleModel('gemini-2.5-flash', GEMINI_CODE_TIMEOUT),
];

const EMBEDDING_PROVIDERS = [
  { name: 'Google', model: 'gemini-embedding-2',   keyEnv: 'GOOGLE_KEYS', dim: 3072 },
  { name: 'Google', model: 'gemini-embedding-001', keyEnv: 'GOOGLE_KEYS', dim: 3072 },
];
async function getGlobalFastestModel(env) { return await mtGet(env, 'fastest:global'); }

function buildGeminiThinkingConfig(model, mode) {
  const m = String(model || '').toLowerCase();
  const isExpert = mode === 'code' || mode === 'vision-agent';

  if (!isExpert) {
    if (/gemini-3/.test(m))               return { thinkingLevel: 'minimal', includeThoughts: false };
    if (/flash-lite-latest/.test(m))      return { thinkingLevel: 'minimal', includeThoughts: false };
    if (/gemini-2\.5-flash-lite/.test(m)) return { thinkingBudget: 0, includeThoughts: false };
    if (/gemini-2\.5-flash/.test(m))      return { thinkingBudget: 0, includeThoughts: false };
    return null;
  }

  if (/gemini-3\.5-flash-lite/.test(m)) return { thinkingLevel: 'high', includeThoughts: true };
  if (/gemini-3\.1-flash-lite/.test(m)) return { thinkingLevel: 'high', includeThoughts: true };
  if (/gemini-3/.test(m))               return { thinkingLevel: 'high', includeThoughts: true };
  if (/gemini-2\.5-flash-lite/.test(m)) return { thinkingBudget: 8192, includeThoughts: true };
  if (/gemini-2\.5-flash/.test(m))      return { thinkingBudget: 16384, includeThoughts: true };
  return null;
}

async function getStickyModel(env) { return await mtGet(env, STICKY_MODEL_KEY); }
async function setStickyModel(env, providerName, model) {
  await mtPut(env, STICKY_MODEL_KEY, { provider: providerName, model, ts: Date.now() }, STICKY_MODEL_TTL);
}
async function orderPipelineByQuota(env, mode) {
  const base = (PROVIDERS[mode] || PROVIDERS.text).slice();
  const sticky = await getStickyModel(env);
  let ordered = base;
  if (sticky && sticky.model) {
    const idx = base.findIndex(p => p.model === sticky.model);
    if (idx > 0) ordered = [base[idx], ...base.slice(0, idx), ...base.slice(idx + 1)];
  }
  return ordered;
}
async function clearFastest(env) {
  await mtDelete(env, 'fastest:global');
  for (const m of ['text', 'code', 'vision', 'vision-agent']) await mtDelete(env, `fastest:${m}`);
}

// ---------------------------------------------------------------------------
// 5. SANDBOXED JS RUNNER
// ---------------------------------------------------------------------------
const JS_LIMITS = { MAX_CODE_CHARS: 20000, MAX_OUTPUT_CHARS: 6000, MAX_STEPS: 2_000_000, MAX_CALL_DEPTH: 400, MAX_RUN_MS: 4000 };
class JSError extends Error { constructor(m){ super(m); this.name = 'JSError'; } }
const BLOCKED_PROPS = new Set(['constructor', '__proto__', 'prototype', 'caller', 'callee', 'arguments', 'call', 'apply', 'bind']);
const KW = new Set(['let','const','var','function','return','if','else','for','while','do','break','continue','true','false','null','undefined','typeof','instanceof','new','try','catch','finally','throw','of','in','this','void','delete','switch','case','default','class','extends','super','static','get','set','async','await','yield','export','import','from','as']);

function jsLex(src){
  const toks = []; let i = 0, line = 1; let prevKind = 'start';
  const isValueEnd = () => prevKind === 'value';
  const push = (t, v) => { toks.push({ t, v, line }); prevKind = (t === 'id' || t === 'num' || t === 'str' || t === 'regex' || t === 'tmpl') ? 'value' : 'op'; };
  const readStr = q => {
    i++; let s = '';
    while (i < src.length && src[i] !== q) {
      if (src[i] === '\\') {
        const e = src[i+1];
        if (e === 'u') { if (src[i+2] === '{') { const end = src.indexOf('}', i+3); s += String.fromCodePoint(parseInt(src.slice(i+3,end),16)); i = end+1; continue; } s += String.fromCharCode(parseInt(src.slice(i+2,i+6),16)); i += 6; continue; }
        if (e === 'x') { s += String.fromCharCode(parseInt(src.slice(i+2,i+4),16)); i += 4; continue; }
        s += ({n:'\n',t:'\t',r:'\r',b:'\b',f:'\f',v:'\v',0:'\0','\\':'\\',"'":"'",'"':'"','`':'`'}[(e)]) ?? e;
        i += 2;
      } else { if (src[i] === '\n') line++; s += src[i++]; }
    }
    i++; return s;
  };
  const readTmpl = () => {
    i++; const parts = []; let buf = '';
    while (i < src.length && src[i] !== '`') {
      if (src[i] === '\\') { const e = src[i+1]; buf += ({n:'\n',t:'\t',r:'\r','\\':'\\','`':'`','$':'$'}[(e)]) ?? e; i += 2; }
      else if (src[i] === '$' && src[i+1] === '{') { if (buf) { parts.push({ k:'s', v:buf }); buf = ''; } i += 2; let depth = 1, expr = ''; while (i < src.length && depth > 0) { if (src[i] === '{') depth++; else if (src[i] === '}') { depth--; if (depth === 0) break; } expr += src[i++]; } i++; parts.push({ k:'e', src: expr }); }
      else { if (src[i] === '\n') line++; buf += src[i++]; }
    }
    i++; if (buf) parts.push({ k:'s', v:buf }); return parts;
  };
  const readRegex = () => {
    i++; let pattern = ''; let inClass = false;
    while (i < src.length) { const c = src[i]; if (c === '\\') { pattern += c + (src[i+1]||''); i += 2; continue; } if (c === '[') inClass = true; else if (c === ']') inClass = false; else if (c === '/' && !inClass) break; else if (c === '\n') throw new JSError(`Unterminated regex at line ${line}`); pattern += c; i++; }
    if (src[i] !== '/') throw new JSError(`Unterminated regex at line ${line}`);
    i++; let flags = ''; while (i < src.length && /[dgimsuy]/.test(src[i])) flags += src[i++];
    return { pattern, flags };
  };
  while (i < src.length) {
    const c = src[i];
    if (c === '\n') { line++; i++; continue; }
    if (/\s/.test(c)) { i++; continue; }
    if (c === '/' && src[i+1] === '/') { i += 2; while (i < src.length && src[i] !== '\n') i++; continue; }
    if (c === '/' && src[i+1] === '*') { i += 2; while (i < src.length && !(src[i] === '*' && src[i+1] === '/')) { if (src[i]==='\n') line++; i++; } i += 2; continue; }
    if (c === '/' && !isValueEnd()) { push('regex', readRegex()); continue; }
    if (/[0-9]/.test(c) || (c === '.' && /[0-9]/.test(src[i+1]))) {
      let n = '';
      if (c === '0' && (src[i+1] === 'x' || src[i+1] === 'X')) { i += 2; while (i < src.length && /[0-9a-fA-F_]/.test(src[i])) n += src[i++]; push('num', parseInt(n.replace(/_/g,''), 16)); continue; }
      if (c === '0' && (src[i+1] === 'b' || src[i+1] === 'B')) { i += 2; while (i < src.length && /[01_]/.test(src[i])) n += src[i++]; push('num', parseInt(n.replace(/_/g,''), 2)); continue; }
      if (c === '0' && (src[i+1] === 'o' || src[i+1] === 'O')) { i += 2; while (i < src.length && /[0-7_]/.test(src[i])) n += src[i++]; push('num', parseInt(n.replace(/_/g,''), 8)); continue; }
      while (i < src.length && /[0-9_.]/.test(src[i])) n += src[i++];
      if (i < src.length && /[eE]/.test(src[i])) { n += src[i++]; if (/[+-]/.test(src[i])) n += src[i++]; while (i < src.length && /[0-9]/.test(src[i])) n += src[i++]; }
      push('num', parseFloat(n.replace(/_/g,''))); continue;
    }
    if (c === '"' || c === "'") { push('str', readStr(c)); continue; }
    if (c === '`') { push('tmpl', readTmpl()); continue; }
    if (/[a-zA-Z_$]/.test(c)) { let n = ''; while (i < src.length && /[a-zA-Z0-9_$]/.test(src[i])) n += src[i++]; push(KW.has(n) ? 'kw' : 'id', n); continue; }
    let done = false;
    const ops3 = ['>>>=','===','!==','**=','...','<<=','>>=','&&=','||=','??='];
    for (const p of ops3) if (src.startsWith(p, i)) { push('op', p); i += p.length; done = true; break; }
    if (done) continue;
    const ops2 = ['==','!=','<=','>=','&&','||','??','**','+=','-=','*=','/=','%=','&=','|=','^=','<<','>>','++','--','=>','?.'];
    for (const p of ops2) if (src.startsWith(p, i)) { push('op', p); i += p.length; done = true; break; }
    if (done) continue;
    if ('+-*/%<>=!&|^~?:;,.(){}[]'.includes(c)) { push('op', c); i++; continue; }
    throw new JSError(`Unexpected character '${c}' at line ${line}`);
  }
  push('eof',''); return toks;
}

class JSParser {
  constructor(toks){ this.toks = toks; this.pos = 0; }
  peek(o = 0){ return this.toks[this.pos+o] || this.toks[this.toks.length-1]; }
  next(){ return this.toks[this.pos++]; }
  isOp(v){ const t = this.peek(); return t.t === 'op' && t.v === v; }
  isKw(v){ const t = this.peek(); return t.t === 'kw' && t.v === v; }
  isKwAny(vs){ const t = this.peek(); return t.t === 'kw' && vs.includes(t.v); }
  eat(v){ if (this.isOp(v) || this.isKw(v)) { this.pos++; return true; } return false; }
  expect(v){ if (this.isOp(v) || this.isKw(v)) return this.next(); const t = this.peek(); throw new JSError(`Expected '${v}', got '${t.v || t.t}' at line ${t.line}`); }
  expectId(){ const t = this.peek(); if (t.t !== 'id') throw new JSError(`Expected identifier at line ${t.line}`); this.pos++; return t.v; }
  parseProgram(){ const body = []; while (this.peek().t !== 'eof') body.push(this.parseStatement()); return { type:'Program', body }; }
  parseStatement(){
    const t = this.peek();
    if (t.t === 'op' && t.v === ';') { this.next(); return { type:'Empty' }; }
    if (t.t === 'op' && t.v === '{') return this.parseBlock();
    if (t.t === 'kw') {
      if (t.v === 'let' || t.v === 'const' || t.v === 'var') return this.parseVarDecl();
      if (t.v === 'function') return this.parseFnDecl();
      if (t.v === 'return') return this.parseReturn();
      if (t.v === 'if') return this.parseIf();
      if (t.v === 'for') return this.parseFor();
      if (t.v === 'while') return this.parseWhile();
      if (t.v === 'do') return this.parseDo();
      if (t.v === 'break') { this.next(); let label = null; if (this.peek().t === 'id') label = this.next().v; this.eat(';'); return { type:'Break', label }; }
      if (t.v === 'continue') { this.next(); let label = null; if (this.peek().t === 'id') label = this.next().v; this.eat(';'); return { type:'Continue', label }; }
      if (t.v === 'throw') { this.next(); const arg = this.parseExpr(); this.eat(';'); return { type:'Throw', arg }; }
      if (t.v === 'try') return this.parseTry();
      if (t.v === 'switch') return this.parseSwitch();
      if (t.v === 'class') return this.parseClass(true);
      if (t.v === 'async' && this.peek(1).t === 'kw' && this.peek(1).v === 'function') { this.next(); return this.parseFnDecl(); }
    }
    if (t.t === 'id' && this.peek(1).t === 'op' && this.peek(1).v === ':') { const label = this.next().v; this.next(); return { type:'Labeled', label, body: this.parseStatement() }; }
    const expr = this.parseExpr(); this.eat(';'); return { type:'ExprStmt', expr };
  }
  parseBlock(){ this.expect('{'); const body = []; while (!this.isOp('}') && this.peek().t !== 'eof') body.push(this.parseStatement()); this.expect('}'); return { type:'Block', body }; }
  parseVarDecl(){ const kind = this.next().v; const decls = []; while (true) { const target = this.parseBindingTarget(); let init = null; if (this.eat('=')) init = this.parseAssign(); decls.push({ target, init }); if (!this.eat(',')) break; } this.eat(';'); return { type:'VarDecl', kind, decls }; }
  parseBindingTarget(){ const t = this.peek(); if (t.t === 'id') { this.next(); return { type:'Ident', name: t.v }; } if (t.t === 'op' && t.v === '[') return this.parseArrayPattern(); if (t.t === 'op' && t.v === '{') return this.parseObjectPattern(); throw new JSError(`Expected binding target at line ${t.line}`); }
  parseArrayPattern(){ this.expect('['); const elements = []; while (!this.isOp(']') && this.peek().t !== 'eof') { if (this.isOp(',')) { elements.push(null); this.next(); continue; } if (this.isOp('...')) { this.next(); const rest = this.parseBindingTarget(); elements.push({ type:'Rest', target: rest }); break; } const target = this.parseBindingTarget(); let def = null; if (this.eat('=')) def = this.parseAssign(); elements.push({ target, default: def }); if (!this.eat(',')) break; } this.expect(']'); return { type:'ArrayPattern', elements }; }
  parseObjectPattern(){
    this.expect('{'); const props = [];
    while (!this.isOp('}') && this.peek().t !== 'eof') {
      if (this.isOp('...')) { this.next(); const rest = this.parseBindingTarget(); props.push({ type:'Rest', target: rest }); break; }
      const kt = this.peek(); let key, computed = false;
      if (kt.t === 'op' && kt.v === '[') { this.next(); key = this.parseAssign(); this.expect(']'); computed = true; }
      else if (kt.t === 'str') { key = { type:'Str', value: kt.v }; this.next(); }
      else if (kt.t === 'num') { key = { type:'Str', value: String(kt.v) }; this.next(); }
      else if (kt.t === 'id' || kt.t === 'kw') { key = { type:'Str', value: kt.v }; this.next(); }
      else throw new JSError(`Bad object pattern key at line ${kt.line}`);
      let target; if (this.eat(':')) target = this.parseBindingTarget(); else if (!computed && key.type === 'Str') target = { type:'Ident', name: key.value }; else throw new JSError('Computed key needs : in destructuring');
      let def = null; if (this.eat('=')) def = this.parseAssign();
      props.push({ key, computed, target, default: def }); if (!this.eat(',')) break;
    }
    this.expect('}'); return { type:'ObjectPattern', props };
  }
  parseFnDecl(){ this.expect('function'); const id = this.expectId(); const params = this.parseParams(); const body = this.parseBlock(); return { type:'FnDecl', id, params, body }; }
  parseParams(){ this.expect('('); const params = []; while (!this.isOp(')') && this.peek().t !== 'eof') { if (this.isOp('...')) { this.next(); const target = this.parseBindingTarget(); params.push({ type:'Rest', target }); break; } const target = this.parseBindingTarget(); let def = null; if (this.eat('=')) def = this.parseAssign(); params.push({ target, default: def }); if (!this.eat(',')) break; } this.expect(')'); return params; }
  parseReturn(){ this.expect('return'); let arg = null; if (!this.isOp(';') && !this.isOp('}') && this.peek().t !== 'eof') arg = this.parseExpr(); this.eat(';'); return { type:'Return', arg }; }
  parseIf(){ this.expect('if'); this.expect('('); const test = this.parseExpr(); this.expect(')'); const cons = this.parseStatement(); let alt = null; if (this.isKw('else')) { this.next(); alt = this.parseStatement(); } return { type:'If', test, cons, alt }; }
  parseFor(){
    this.expect('for'); if (this.isKw('await')) this.next(); this.expect('(');
    if (this.isKwAny(['let','const','var'])) {
      const save = this.pos; const kind = this.next().v; const target = this.parseBindingTarget();
      if (this.isKw('of')) { this.next(); const iter = this.parseExpr(); this.expect(')'); const body = this.parseStatement(); return { type:'ForOf', kind, target, iter, body }; }
      if (this.isKw('in')) { this.next(); const iter = this.parseExpr(); this.expect(')'); const body = this.parseStatement(); return { type:'ForIn', kind, target, iter, body }; }
      this.pos = save;
    }
    let init = null;
    if (!this.isOp(';')) { if (this.isKwAny(['let','const','var'])) init = this.parseVarDecl(); else { init = { type:'ExprStmt', expr: this.parseExpr() }; this.eat(';'); } } else this.eat(';');
    let test = null; if (!this.isOp(';')) test = this.parseExpr(); this.expect(';');
    let update = null; if (!this.isOp(')')) update = this.parseExpr(); this.expect(')');
    const body = this.parseStatement(); return { type:'For', init, test, update, body };
  }
  parseWhile(){ this.expect('while'); this.expect('('); const test = this.parseExpr(); this.expect(')'); const body = this.parseStatement(); return { type:'While', test, body }; }
  parseDo(){ this.expect('do'); const body = this.parseStatement(); this.expect('while'); this.expect('('); const test = this.parseExpr(); this.expect(')'); this.eat(';'); return { type:'Do', body, test }; }
  parseTry(){
    this.expect('try'); const block = this.parseBlock(); let handler = null, finalizer = null;
    if (this.isKw('catch')) { this.next(); let param = null; if (this.eat('(')) { param = this.parseBindingTarget(); this.expect(')'); } handler = { param, body: this.parseBlock() }; }
    if (this.isKw('finally')) { this.next(); finalizer = this.parseBlock(); }
    return { type:'Try', block, handler, finalizer };
  }
  parseSwitch(){
    this.expect('switch'); this.expect('('); const disc = this.parseExpr(); this.expect(')'); this.expect('{'); const cases = [];
    while (!this.isOp('}') && this.peek().t !== 'eof') {
      let test = null;
      if (this.isKw('case')) { this.next(); test = this.parseExpr(); } else if (this.isKw('default')) { this.next(); } else throw new JSError(`Expected case/default`);
      this.expect(':');
      const body = [];
      while (!this.isKw('case') && !this.isKw('default') && !this.isOp('}') && this.peek().t !== 'eof') body.push(this.parseStatement());
      cases.push({ test, body });
    }
    this.expect('}'); return { type:'Switch', disc, cases };
  }
  parseClass(isStatement){
    this.expect('class'); let id = null; if (this.peek().t === 'id') id = this.next().v; let parent = null; if (this.isKw('extends')) { this.next(); parent = this.parseMember(); } this.expect('{');
    const methods = [];
    while (!this.isOp('}') && this.peek().t !== 'eof') {
      let kind = 'method', isStatic = false;
      if (this.isKw('static')) { this.next(); isStatic = true; }
      if (this.isKw('get')) { kind = 'get'; this.next(); } else if (this.isKw('set')) { kind = 'set'; this.next(); } else if (this.isKw('async')) { this.next(); }
      let key, computed = false; const kt = this.peek();
      if (kt.t === 'op' && kt.v === '[') { this.next(); key = this.parseAssign(); this.expect(']'); computed = true; }
      else if (kt.t === 'str') { key = { type:'Str', value: kt.v }; this.next(); }
      else if (kt.t === 'num') { key = { type:'Str', value: String(kt.v) }; this.next(); }
      else if (kt.t === 'id' || kt.t === 'kw') { key = { type:'Str', value: kt.v }; this.next(); }
      else throw new JSError(`Bad class member at line ${kt.line}`);
      if (this.isOp('(')) { const params = this.parseParams(); const body = this.parseBlock(); methods.push({ isStatic, kind, key, computed, params, body }); }
      else { if (this.eat('=')) this.parseAssign(); this.eat(';'); }
    }
    this.expect('}'); return isStatement ? { type:'ClassDecl', id, parent, methods } : { type:'ClassExpr', id, parent, methods };
  }
  parseExpr(){ let e = this.parseAssign(); while (this.isOp(',')) { this.next(); const right = this.parseAssign(); e = { type:'Seq', left: e, right }; } return e; }
  parseAssign(){
    const left = this.parseConditional(); const t = this.peek();
    if (t.t === 'op' && ['=','+=','-=','*=','/=','%=','**=','&=','|=','^=','<<=','>>=','>>>=','&&=','||=','??='].includes(t.v)) { this.next(); return { type:'Assign', op:t.v, target:left, value: this.parseAssign() }; }
    return left;
  }
  parseConditional(){ const test = this.parseNullish(); if (this.eat('?')) { const cons = this.parseAssign(); this.expect(':'); const alt = this.parseAssign(); return { type:'Cond', test, cons, alt }; } return test; }
  parseNullish(){ let l = this.parseOr(); while (this.isOp('??')) { this.next(); l = { type:'Logical', op:'??', left:l, right: this.parseOr() }; } return l; }
  parseOr(){ let l = this.parseAnd(); while (this.isOp('||')) { this.next(); l = { type:'Logical', op:'||', left:l, right: this.parseAnd() }; } return l; }
  parseAnd(){ let l = this.parseBitOr(); while (this.isOp('&&')) { this.next(); l = { type:'Logical', op:'&&', left:l, right: this.parseBitOr() }; } return l; }
  parseBitOr(){ let l = this.parseBitXor(); while (this.isOp('|')) { this.next(); l = { type:'Binary', op:'|', left:l, right: this.parseBitXor() }; } return l; }
  parseBitXor(){ let l = this.parseBitAnd(); while (this.isOp('^')) { this.next(); l = { type:'Binary', op:'^', left:l, right: this.parseBitAnd() }; } return l; }
  parseBitAnd(){ let l = this.parseEquality(); while (this.isOp('&')) { this.next(); l = { type:'Binary', op:'&', left:l, right: this.parseEquality() }; } return l; }
  parseEquality(){ let l = this.parseRelational(); while (true) { const t = this.peek(); if (t.t === 'op' && ['==','!=','===','!=='].includes(t.v)) { this.next(); l = { type:'Binary', op:t.v, left:l, right: this.parseRelational() }; } else break; } return l; }
  parseRelational(){ let l = this.parseShift(); while (true) { const t = this.peek(); if (t.t === 'op' && ['<','>','<=','>='].includes(t.v)) { this.next(); l = { type:'Binary', op:t.v, left:l, right: this.parseShift() }; } else if (t.t === 'kw' && (t.v === 'in' || t.v === 'instanceof')) { this.next(); l = { type:'Binary', op:t.v, left:l, right: this.parseShift() }; } else break; } return l; }
  parseShift(){ let l = this.parseAdd(); while (true) { const t = this.peek(); if (t.t === 'op' && ['<<','>>','>>>'].includes(t.v)) { this.next(); l = { type:'Binary', op:t.v, left:l, right: this.parseAdd() }; } else break; } return l; }
  parseAdd(){ let l = this.parseMul(); while (true) { const t = this.peek(); if (t.t === 'op' && (t.v === '+' || t.v === '-')) { this.next(); l = { type:'Binary', op:t.v, left:l, right: this.parseMul() }; } else break; } return l; }
  parseMul(){ let l = this.parseExp(); while (true) { const t = this.peek(); if (t.t === 'op' && ['*','/','%'].includes(t.v)) { this.next(); l = { type:'Binary', op:t.v, left:l, right: this.parseExp() }; } else break; } return l; }
  parseExp(){ const base = this.parseUnary(); if (this.isOp('**')) { this.next(); return { type:'Binary', op:'**', left:base, right: this.parseExp() }; } return base; }
  parseUnary(){
    const t = this.peek();
    if (t.t === 'op' && ['!','-','+','~'].includes(t.v)) { this.next(); return { type:'Unary', op:t.v, arg: this.parseUnary() }; }
    if (t.t === 'kw' && ['typeof','void','delete'].includes(t.v)) { this.next(); return { type:'Unary', op:t.v, arg: this.parseUnary() }; }
    if (t.t === 'op' && ['++','--'].includes(t.v)) { this.next(); return { type:'Update', op:t.v, arg: this.parseUnary(), prefix:true }; }
    if (t.t === 'kw' && t.v === 'await') { this.next(); return { type:'Await', arg: this.parseUnary() }; }
    return this.parsePostfix();
  }
  parsePostfix(){ let e = this.parsePrimary(); while (true) { const t = this.peek(); if (t.t === 'op' && ['++','--'].includes(t.v)) { this.next(); e = { type:'Update', op:t.v, arg:e, prefix:false }; continue; } break; } return e; }
  parsePrimary(){
    const t = this.peek();
    if (t.t === 'num') { this.next(); return { type:'Num', value:t.v }; }
    if (t.t === 'str') { this.next(); return { type:'Str', value:t.v }; }
    if (t.t === 'regex') { this.next(); return { type:'Regex', pattern: t.v.pattern, flags: t.v.flags }; }
    if (t.t === 'tmpl') { this.next(); const parts = t.v.map(p => p.k === 's' ? { k:'s', v:p.v } : { k:'e', expr: new JSParser(jsLex(p.src)).parseExpr() }); return { type:'Tmpl', parts }; }
    if (t.t === 'kw') {
      if (t.v === 'true') { this.next(); return { type:'Bool', value:true }; }
      if (t.v === 'false') { this.next(); return { type:'Bool', value:false }; }
      if (t.v === 'null') { this.next(); return { type:'Null' }; }
      if (t.v === 'undefined') { this.next(); return { type:'Undefined' }; }
      if (t.v === 'this') { this.next(); return { type:'This' }; }
      if (t.v === 'function') return this.parseFnExpr();
      if (t.v === 'new') return this.parseNew();
      if (t.v === 'class') return this.parseClass(false);
      if (t.v === 'super') { this.next(); return { type:'Ident', name:'super' }; }
    }
    if (t.t === 'op' && t.v === '(') { const save = this.pos; try { this.next(); const params = this.parseParams(); if (this.isOp('=>')) { this.next(); return this.parseArrowBody(params); } } catch (e) {} this.pos = save; this.next(); const e = this.parseExpr(); this.expect(')'); return e; }
    if (t.t === 'id') { const save = this.pos; const name = this.next().v; if (this.isOp('=>')) { this.next(); return this.parseArrowBody([{ target: { type:'Ident', name } }]); } this.pos = save; }
    return this.parseMember();
  }
  parseArrowBody(params){ if (this.isOp('{')) return { type:'Arrow', params, body: this.parseBlock(), exprBody:false }; return { type:'Arrow', params, body: this.parseAssign(), exprBody:true }; }
  parseFnExpr(){ this.expect('function'); let id = null; if (this.peek().t === 'id') id = this.next().v; const params = this.parseParams(); const body = this.parseBlock(); return { type:'FnExpr', id, params, body }; }
  parseNew(){ this.expect('new'); const callee = this.parseMember(); let args = []; if (this.isOp('(')) { this.next(); args = this.parseArgs(); this.expect(')'); } return { type:'New', callee, args }; }
  parseArgs(){ const args = []; while (!this.isOp(')') && this.peek().t !== 'eof') { if (this.isOp('...')) { this.next(); args.push({ type:'Spread', arg: this.parseAssign() }); } else args.push(this.parseAssign()); if (!this.eat(',')) break; } return args; }
  parseMember(){
    let obj = this.parseAtom();
    while (true) {
      const t = this.peek();
      if (t.t === 'op' && t.v === '.') { this.next(); const p = this.peek(); if (p.t !== 'id' && p.t !== 'kw') throw new JSError(`Expected property name at line ${p.line}`); this.next(); obj = { type:'Member', obj, prop:{ type:'Str', value:p.v }, computed:false, optional:false }; }
      else if (t.t === 'op' && t.v === '?.') { this.next(); if (this.isOp('[')) { this.next(); const prop = this.parseExpr(); this.expect(']'); obj = { type:'Member', obj, prop, computed:true, optional:true }; } else if (this.isOp('(')) { this.next(); const args = this.parseArgs(); this.expect(')'); obj = { type:'Call', callee: obj, args, optional:true }; } else { const p = this.peek(); if (p.t !== 'id' && p.t !== 'kw') throw new JSError(`Expected property name at line ${p.line}`); this.next(); obj = { type:'Member', obj, prop:{ type:'Str', value:p.v }, computed:false, optional:true }; } }
      else if (t.t === 'op' && t.v === '[') { this.next(); const prop = this.parseExpr(); this.expect(']'); obj = { type:'Member', obj, prop, computed:true, optional:false }; }
      else if (t.t === 'op' && t.v === '(') { this.next(); const args = this.parseArgs(); this.expect(')'); obj = { type:'Call', callee:obj, args, optional:false }; }
      else break;
    }
    return obj;
  }
  parseAtom(){
    const t = this.peek();
    if (t.t === 'id') { this.next(); return { type:'Ident', name:t.v }; }
    if (t.t === 'op' && t.v === '(') { this.next(); const e = this.parseExpr(); this.expect(')'); return e; }
    if (t.t === 'op' && t.v === '[') {
      this.next(); const el = [];
      while (!this.isOp(']') && this.peek().t !== 'eof') {
        if (this.isOp(',')) { el.push(null); this.next(); continue; }
        if (this.isOp('...')) { this.next(); el.push({ type:'Spread', arg: this.parseAssign() }); } else el.push(this.parseAssign());
        if (!this.eat(',')) break;
      }
      this.expect(']'); return { type:'Array', elements:el };
    }
    if (t.t === 'op' && t.v === '{') {
      this.next(); const props = [];
      while (!this.isOp('}') && this.peek().t !== 'eof') {
        if (this.isOp('...')) { this.next(); props.push({ spread: this.parseAssign() }); if (!this.eat(',')) break; continue; }
        const kt = this.peek(); let key, computed = false;
        if (kt.t === 'op' && kt.v === '[') { this.next(); key = this.parseAssign(); this.expect(']'); computed = true; }
        else if (kt.t === 'str') { key = { type:'Str', value: kt.v }; this.next(); }
        else if (kt.t === 'num') { key = { type:'Str', value: String(kt.v) }; this.next(); }
        else if (kt.t === 'id' || kt.t === 'kw') { key = { type:'Str', value: kt.v }; this.next(); }
        else throw new JSError(`Bad object key at line ${kt.line}`);
        if (this.isOp('(')) { const params = this.parseParams(); const body = this.parseBlock(); props.push({ key, computed, value: { type:'FnExpr', id:null, params, body } }); }
        else if (this.eat(':')) { props.push({ key, computed, value: this.parseAssign() }); }
        else { props.push({ key, computed, value: { type:'Ident', name:key.value } }); }
        if (!this.eat(',')) break;
      }
      this.expect('}'); return { type:'Object', props };
    }
    throw new JSError(`Unexpected token '${t.v || t.t}' at line ${t.line}`);
  }
}

class JSScope {
  constructor(parent = null, kind = 'block'){ this.vars = Object.create(null); this.consts = new Set(); this.parent = parent; this.kind = kind; this.thisValue = undefined; }
  declare(name, val, isConst = false){ this.vars[name] = val; if (isConst) this.consts.add(name); }
  declareVar(name, val){ let s = this; while (s.parent && s.kind !== 'function' && s.kind !== 'global') s = s.parent; s.vars[name] = val; }
  lookup(name){ let s = this; while (s) { if (name in s.vars) return s; s = s.parent; } return null; }
  get(name){ const s = this.lookup(name); if (!s) throw new JSError(`'${name}' is not defined`); return s.vars[name]; }
  set(name, val){ const s = this.lookup(name); if (!s) throw new JSError(`'${name}' is not defined`); if (s.consts.has(name)) throw new JSError(`Assignment to constant '${name}'`); s.vars[name] = val; return val; }
  getThis(){ let s = this; while (s) { if (s.kind === 'function' || s.kind === 'global') return s.thisValue; s = s.parent; } return undefined; }
}
class ReturnSignal { constructor(v){ this.value = v; } }
class BreakSignal { constructor(label){ this.label = label; } }
class ContinueSignal { constructor(label){ this.label = label; } }

class JSInterpreter {
  constructor(){ this.output = []; this.outputLen = 0; this.steps = 0; this.callDepth = 0; this.startTime = Date.now(); this.truncated = false; this.lastValue = undefined; this.global = this.makeGlobalScope(); }
  tick(){ this.steps++; if (this.steps > JS_LIMITS.MAX_STEPS) throw new JSError('Step limit exceeded'); if ((this.steps & 0x1FFF) === 0 && Date.now() - this.startTime > JS_LIMITS.MAX_RUN_MS) throw new JSError('Time limit exceeded'); }
  write(s){ const str = String(s); if (this.outputLen + str.length + 1 > JS_LIMITS.MAX_OUTPUT_CHARS) { if (!this.truncated) { this.truncated = true; this.output.push('[output truncated]'); } return; } this.output.push(str); this.outputLen += str.length + 1; }
  getOutput(){ if (!this.output.length) return '(no output)'; return this.output.join('\n'); }
  display(v){
    if (v === null) return 'null'; if (v === undefined) return 'undefined'; if (typeof v === 'string') return v;
    if (typeof v === 'number' || typeof v === 'boolean' || typeof v === 'bigint') return String(v);
    if (typeof v === 'function') return '[Function]';
    if (Array.isArray(v)) { try { return JSON.stringify(v, (k, val) => typeof val === 'function' ? '[Function]' : val); } catch { return String(v); } }
    if (v instanceof RegExp) return String(v);
    if (v instanceof Map) return `Map(${v.size}) ${JSON.stringify([...v])}`;
    if (v instanceof Set) return `Set(${v.size}) ${JSON.stringify([...v])}`;
    if (typeof v === 'object') { if (v.__fn) return '[Function]'; try { return JSON.stringify(v, (k, val) => typeof val === 'function' ? '[Function]' : val); } catch { return '[Object]'; } }
    return String(v);
  }
  makeGlobalScope(){
    const g = new JSScope(null, 'global'); g.thisValue = undefined; const self = this;
    g.declare('console', { log: (...a) => self.write(a.map(x => self.display(x)).join(' ')), error: (...a) => self.write(a.map(x => self.display(x)).join(' ')), warn: (...a) => self.write(a.map(x => self.display(x)).join(' ')), info: (...a) => self.write(a.map(x => self.display(x)).join(' ')), debug: (...a) => self.write(a.map(x => self.display(x)).join(' ')), table: (a) => self.write(self.display(a)), dir: (a) => self.write(self.display(a)), group: () => {}, groupEnd: () => {} }, true);
    g.declare('Math', this.makeMath(), true);
    g.declare('JSON', { stringify: (v, r, s) => JSON.stringify(v, r, s), parse: s => JSON.parse(s) }, true);
    g.declare('Object', { keys: o => Object.keys(o), values: o => Object.values(o), entries: o => Object.entries(o), assign: (t, ...s) => Object.assign(t, ...s), freeze: o => Object.freeze(o), fromEntries: e => Object.fromEntries(e), hasOwnProperty: (o, k) => Object.prototype.hasOwnProperty.call(o, k), create: p => Object.create(p) }, true);
    g.declare('Number', { isInteger: n => Number.isInteger(n), isFinite: n => Number.isFinite(n), isNaN: n => Number.isNaN(n), isSafeInteger: n => Number.isSafeInteger(n), parseFloat: s => parseFloat(s), parseInt: (s, r) => parseInt(s, r), MAX_SAFE_INTEGER: Number.MAX_SAFE_INTEGER, MIN_SAFE_INTEGER: Number.MIN_SAFE_INTEGER, MAX_VALUE: Number.MAX_VALUE, MIN_VALUE: Number.MIN_VALUE, EPSILON: Number.EPSILON, POSITIVE_INFINITY: Infinity, NEGATIVE_INFINITY: -Infinity, NaN: NaN }, true);
    g.declare('String', { fromCharCode: (...c) => String.fromCharCode(...c), fromCodePoint: (...c) => String.fromCodePoint(...c) }, true);
    g.declare('Array', {
      isArray: v => Array.isArray(v),
      from: (v, f) => f ? Array.from(v, self.wrapCallback(f)) : Array.from(v),
      of: (...a) => a,
    }, true);
    g.declare('Boolean', v => !!v, true);
    g.declare('parseInt', (s, r = 10) => parseInt(s, r), true);
    g.declare('parseFloat', s => parseFloat(s), true);
    g.declare('isNaN', v => Number.isNaN(Number(v)), true);
    g.declare('isFinite', v => Number.isFinite(Number(v)), true);
    g.declare('NaN', NaN, true); g.declare('Infinity', Infinity, true);
    g.declare('encodeURIComponent', s => encodeURIComponent(s), true);
    g.declare('decodeURIComponent', s => decodeURIComponent(s), true);
    g.declare('encodeURI', s => encodeURI(s), true);
    g.declare('decodeURI', s => decodeURI(s), true);
    g.declare('Date', { now: () => Date.now(), parse: s => Date.parse(s), UTC: (...a) => Date.UTC(...a) }, true);
    g.declare('Map', Map, true); g.declare('Set', Set, true); g.declare('RegExp', RegExp, true);
    g.declare('Error', Error, true); g.declare('TypeError', TypeError, true); g.declare('RangeError', RangeError, true);
    g.declare('ReferenceError', ReferenceError, true); g.declare('SyntaxError', SyntaxError, true);
    g.declare('Symbol', { iterator: Symbol.iterator, for: Symbol.for, keyFor: Symbol.keyFor }, true);
    const M = Math;
    const bareFns = { abs:M.abs, sign:M.sign, sqrt:M.sqrt, cbrt:M.cbrt, pow:M.pow, exp:M.exp, log:M.log, log2:M.log2, log10:M.log10, sin:M.sin, cos:M.cos, tan:M.tan, asin:M.asin, acos:M.acos, atan:M.atan, atan2:M.atan2, sinh:M.sinh, cosh:M.cosh, tanh:M.tanh, floor:M.floor, ceil:M.ceil, round:M.round, trunc:M.trunc, min:M.min, max:M.max, hypot:M.hypot };
    for (const [k, v] of Object.entries(bareFns)) g.declare(k, v, true);
    g.declare('PI', Math.PI, true); g.declare('pi', Math.PI, true);
    return g;
  }
  makeMath(){ const M = {}; for (const k of ['abs','sign','sqrt','cbrt','pow','exp','log','log2','log10','sin','cos','tan','asin','acos','atan','atan2','sinh','cosh','tanh','floor','ceil','round','trunc','min','max','hypot','random','clz32','imul','fround']) M[k] = Math[k]; M.PI = Math.PI; M.E = Math.E; M.LN2 = Math.LN2; M.LN10 = Math.LN10; M.SQRT2 = Math.SQRT2; M.SQRT1_2 = Math.SQRT1_2; return M; }
  run(ast){ try { this.execBlock(ast.body, this.global); } catch (e) { if (!(e instanceof ReturnSignal)) throw e; } if (this.output.length === 0 && this.lastValue !== undefined) this.write(this.display(this.lastValue)); return { ok: true, output: this.getOutput(), truncated: this.truncated }; }
  execBlock(stmts, scope){ for (const s of stmts) if (s.type === 'FnDecl') scope.declare(s.id, this.makeFunction(s.params, s.body, scope, scope.getThis()), false); for (const s of stmts) { if (s.type === 'FnDecl') continue; this.execStmt(s, scope); } }
  execStmt(s, scope){
    this.tick();
    switch (s.type) {
      case 'Empty': return;
      case 'ExprStmt': this.lastValue = this.evalExpr(s.expr, scope); return;
      case 'VarDecl': { for (const d of s.decls) { const val = d.init ? this.evalExpr(d.init, scope) : undefined; this.bindPattern(d.target, val, scope, s.kind); } return; }
      case 'Block': { const inner = new JSScope(scope, 'block'); this.execBlock(s.body, inner); return; }
      case 'If': { const t = this.evalExpr(s.test, scope); if (this.truthy(t)) this.execStmt(s.cons, scope); else if (s.alt) this.execStmt(s.alt, scope); return; }
      case 'While': { while (this.truthy(this.evalExpr(s.test, scope))) { this.tick(); try { this.execStmt(s.body, scope); } catch (e) { if (e instanceof BreakSignal) break; if (e instanceof ContinueSignal) continue; throw e; } } return; }
      case 'Do': { do { this.tick(); try { this.execStmt(s.body, scope); } catch (e) { if (e instanceof BreakSignal) break; if (e instanceof ContinueSignal) continue; throw e; } } while (this.truthy(this.evalExpr(s.test, scope))); return; }
      case 'For': { const forScope = new JSScope(scope, 'block'); if (s.init) this.execStmt(s.init, forScope); while (true) { this.tick(); if (s.test) { if (!this.truthy(this.evalExpr(s.test, forScope))) break; } try { this.execStmt(s.body, forScope); } catch (e) { if (e instanceof BreakSignal) break; if (e instanceof ContinueSignal) { } else throw e; } if (s.update) this.evalExpr(s.update, forScope); } return; }
      case 'ForOf': { const iter = this.evalExpr(s.iter, scope); if (iter == null || typeof iter[Symbol.iterator] !== 'function') throw new JSError('for-of requires iterable'); const forScope = new JSScope(scope, 'block'); for (const v of iter) { this.tick(); this.rebindPattern(s.target, v, forScope, s.kind); try { this.execStmt(s.body, forScope); } catch (e) { if (e instanceof BreakSignal) break; if (e instanceof ContinueSignal) continue; throw e; } } return; }
      case 'ForIn': { const obj = this.evalExpr(s.iter, scope); if (obj == null) return; const forScope = new JSScope(scope, 'block'); const keys = []; for (const k in obj) keys.push(k); for (const k of keys) { this.tick(); this.rebindPattern(s.target, k, forScope, s.kind); try { this.execStmt(s.body, forScope); } catch (e) { if (e instanceof BreakSignal) break; if (e instanceof ContinueSignal) continue; throw e; } } return; }
      case 'Return': throw new ReturnSignal(s.arg ? this.evalExpr(s.arg, scope) : undefined);
      case 'Break': throw new BreakSignal(s.label);
      case 'Continue': throw new ContinueSignal(s.label);
      case 'Throw': throw this.evalExpr(s.arg, scope);
      case 'Labeled': { try { this.execStmt(s.body, scope); } catch (e) { if (e instanceof BreakSignal && e.label === s.label) return; if (e instanceof ContinueSignal && e.label === s.label) return; throw e; } return; }
      case 'Switch': {
        const d = this.evalExpr(s.disc, scope); let matched = false; let defaultIdx = -1;
        for (let i = 0; i < s.cases.length; i++) { const c = s.cases[i]; if (c.test === null) defaultIdx = i; else if (!matched && (this.evalExpr(c.test, scope) === d)) matched = true; if (matched) { for (let j = i; j < s.cases.length; j++) { try { for (const stmt of s.cases[j].body) this.execStmt(stmt, scope); } catch (e) { if (e instanceof BreakSignal) return; throw e; } } return; } }
        if (!matched && defaultIdx >= 0) { for (let j = defaultIdx; j < s.cases.length; j++) { try { for (const stmt of s.cases[j].body) this.execStmt(stmt, scope); } catch (e) { if (e instanceof BreakSignal) return; throw e; } } }
        return;
      }
      case 'Try': { try { this.execStmt(s.block, scope); } catch (e) { if (e instanceof ReturnSignal || e instanceof BreakSignal || e instanceof ContinueSignal) { if (s.finalizer) this.execStmt(s.finalizer, scope); throw e; } if (s.handler) { const inner = new JSScope(scope, 'block'); if (s.handler.param) this.bindPattern(s.handler.param, e, inner, 'let'); this.execStmt(s.handler.body, inner); } else if (s.finalizer) { this.execStmt(s.finalizer, scope); throw e; } else throw e; return; } finally { if (s.finalizer) this.execStmt(s.finalizer, scope); } return; }
      case 'ClassDecl': { const cls = this.evalClass(s, scope); scope.declare(s.id, cls, false); return; }
      case 'FnDecl': return;
      default: throw new JSError(`Unknown statement: ${s.type}`);
    }
  }
  evalClass(node, scope){
    const self = this; const parent = node.parent ? this.evalExpr(node.parent, scope) : null;
    const ctorMethod = node.methods.find(m => !m.isStatic && !m.computed && m.key.value === 'constructor');
    const ctor = function(...args){ if (self.callDepth >= JS_LIMITS.MAX_CALL_DEPTH) throw new JSError('Call depth exceeded'); self.callDepth++; const local = new JSScope(scope, 'function'); local.thisValue = this; try { if (ctorMethod) { self.bindParams(ctorMethod.params, args, local); self.execBlock(ctorMethod.body.body, local); } } finally { self.callDepth--; } };
    ctor.__fn = true; ctor.__classCtor = true; ctor.prototype = Object.create(parent ? parent.prototype : Object.prototype);
    for (const m of node.methods) { if (!m.isStatic && m.kind === 'method' && !m.computed && m.key.value === 'constructor') continue; const fn = this.makeFunction(m.params, m.body, scope, undefined); const k = m.computed ? this.evalExpr(m.key, scope) : m.key.value; if (m.isStatic) ctor[k] = fn; else { if (m.kind === 'get') Object.defineProperty(ctor.prototype, k, { get: fn, configurable: true }); else if (m.kind === 'set') Object.defineProperty(ctor.prototype, k, { set: fn, configurable: true }); else ctor.prototype[k] = fn; } }
    return ctor;
  }
  bindPattern(target, value, scope, kind){
    if (target.type === 'Ident') { if (kind === 'var') scope.declareVar(target.name, value); else scope.declare(target.name, value, kind === 'const'); return; }
    if (target.type === 'ArrayPattern') { const arr = value == null ? [] : value; let i = 0; for (const el of target.elements) { if (el === null) { i++; continue; } if (el.type === 'Rest') { this.bindPattern(el.target, Array.from(arr).slice(i), scope, kind); i = arr.length; break; } let v = arr[i]; if (v === undefined && el.default) v = this.evalExpr(el.default, scope); this.bindPattern(el.target, v, scope, kind); i++; } return; }
    if (target.type === 'ObjectPattern') { const obj = value == null ? {} : Object(value); for (const p of target.props) { if (p.type === 'Rest') { const rest = {}; const used = new Set(target.props.filter(x => x.key && !x.computed && x.key.type === 'Str').map(x => x.key.value)); for (const k in obj) if (!used.has(k)) rest[k] = obj[k]; this.bindPattern(p.target, rest, scope, kind); continue; } const k = p.computed ? this.evalExpr(p.key, scope) : p.key.value; let v = obj[k]; if (v === undefined && p.default) v = this.evalExpr(p.default, scope); this.bindPattern(p.target, v, scope, kind); } return; }
    throw new JSError('Invalid binding target');
  }
  rebindPattern(target, value, scope, kind){ if (target.type === 'Ident') { if (kind === 'var') scope.declareVar(target.name, value); else { if (scope.lookup(target.name) === scope) scope.vars[target.name] = value; else scope.declare(target.name, value, kind === 'const'); } return; } this.bindPattern(target, value, scope, kind); }
  makeFunction(params, body, closure, capturedThis){
    const self = this;
    const fn = function(boundThis, ...args){
      if (self.callDepth >= JS_LIMITS.MAX_CALL_DEPTH) throw new JSError('Call depth exceeded');
      self.callDepth++; const local = new JSScope(closure, 'function'); local.thisValue = boundThis;
      try { self.bindParams(params, args, local); if (body.type === 'Block') self.execBlock(body.body, local); return undefined; } catch (e) { if (e instanceof ReturnSignal) return e.value; throw e; } finally { self.callDepth--; }
    };
    fn.__fn = true; fn.__arrow = false; fn.__capturedThis = capturedThis; fn.__params = params; return fn;
  }
  bindParams(params, args, local){ let i = 0; for (const p of params) { if (p.type === 'Rest') { this.bindPattern(p.target, args.slice(i), local, 'let'); i = args.length; break; } let v = args[i]; if (v === undefined && p.default) v = this.evalExpr(p.default, local); this.bindPattern(p.target, v, local, 'let'); i++; } }
  makeArrow(params, body, exprBody, closure, capturedThis){
    const self = this;
    const fn = function(boundThis, ...args){
      if (self.callDepth >= JS_LIMITS.MAX_CALL_DEPTH) throw new JSError('Call depth exceeded');
      self.callDepth++; const local = new JSScope(closure, 'function'); local.thisValue = capturedThis;
      try { self.bindParams(params, args, local); if (exprBody) return self.evalExpr(body, local); self.execBlock(body.body, local); return undefined; } catch (e) { if (e instanceof ReturnSignal) return e.value; throw e; } finally { self.callDepth--; }
    };
    fn.__fn = true; fn.__arrow = true; fn.__capturedThis = capturedThis; return fn;
  }
  evalExpr(e, scope){
    this.tick();
    switch (e.type) {
      case 'Num': return e.value;
      case 'Str': return e.value;
      case 'Bool': return e.value;
      case 'Null': return null;
      case 'Undefined': return undefined;
      case 'This': return scope.getThis();
      case 'Regex': return new RegExp(e.pattern, e.flags);
      case 'Ident': { if (e.name === 'super') return scope.lookup('super') ? scope.get('super') : undefined; const s = scope.lookup(e.name); if (!s) throw new JSError(`'${e.name}' is not defined`); return s.vars[e.name]; }
      case 'Tmpl': { let s = ''; for (const p of e.parts) { if (p.k === 's') s += p.v; else s += this.display(this.evalExpr(p.expr, scope)); } return s; }
      case 'Array': { const arr = []; for (const el of e.elements) { if (el === null) { arr.push(undefined); continue; } if (el.type === 'Spread') { const inner = this.evalExpr(el.arg, scope); for (const v of inner) arr.push(v); } else arr.push(this.evalExpr(el, scope)); } return arr; }
      case 'Object': { const obj = {}; for (const p of e.props) { if (p.spread) { const src = this.evalExpr(p.spread, scope); if (src != null) for (const k of Object.keys(src)) obj[k] = src[k]; continue; } const k = p.computed ? this.evalExpr(p.key, scope) : p.key.value; obj[k] = this.evalExpr(p.value, scope); } return obj; }
      case 'Member': return this.evalMember(e, scope);
      case 'Call': return this.evalCall(e, scope);
      case 'New': return this.evalNew(e, scope);
      case 'Unary': return this.evalUnary(e, scope);
      case 'Update': return this.evalUpdate(e, scope);
      case 'Binary': return this.evalBinary(e, scope);
      case 'Logical': { const l = this.evalExpr(e.left, scope); if (e.op === '&&') return this.truthy(l) ? this.evalExpr(e.right, scope) : l; if (e.op === '||') return this.truthy(l) ? l : this.evalExpr(e.right, scope); if (e.op === '??') return (l === null || l === undefined) ? this.evalExpr(e.right, scope) : l; throw new JSError(`Unknown logical: ${e.op}`); }
      case 'Assign': return this.evalAssign(e, scope);
      case 'Cond': return this.truthy(this.evalExpr(e.test, scope)) ? this.evalExpr(e.cons, scope) : this.evalExpr(e.alt, scope);
      case 'Seq': { this.evalExpr(e.left, scope); return this.evalExpr(e.right, scope); }
      case 'Arrow': return this.makeArrow(e.params, e.body, e.exprBody, scope, scope.getThis());
      case 'FnExpr': return this.makeFunction(e.params, e.body, scope, scope.getThis());
      case 'ClassExpr': return this.evalClass(e, scope);
      case 'Await': return this.evalExpr(e.arg, scope);
      default: throw new JSError(`Unknown expression: ${e.type}`);
    }
  }
  evalArgs(args, scope){ const out = []; for (const a of args) { if (a.type === 'Spread') { const arr = this.evalExpr(a.arg, scope); for (const v of arr) out.push(v); } else out.push(this.evalExpr(a, scope)); } return out; }
  evalMember(e, scope){ let obj; try { obj = this.evalExpr(e.obj, scope); } catch (err) { if (e.optional) return undefined; throw err; } if (obj === null || obj === undefined) { if (e.optional) return undefined; throw new JSError(`Cannot read property of ${obj === null ? 'null' : 'undefined'}`); } const key = e.computed ? this.evalExpr(e.prop, scope) : e.prop.value; return this.getProp(obj, key); }
  evalCall(e, scope){
    if (e.callee.type === 'Member') {
      let recv; try { recv = this.evalExpr(e.callee.obj, scope); } catch (err) { if (e.optional || e.callee.optional) return undefined; throw err; }
      if (recv === null || recv === undefined) { if (e.optional || e.callee.optional) return undefined; throw new JSError(`Cannot read property of ${recv === null ? 'null' : 'undefined'}`); }
      const key = e.callee.computed ? this.evalExpr(e.callee.prop, scope) : e.callee.prop.value;
      let fn; try { fn = this.getProp(recv, key); } catch (err) { if (e.optional) return undefined; throw err; }
      if (fn === undefined || fn === null) { if (e.optional) return undefined; throw new JSError(`'${String(key)}' is not a function`); }
      if (typeof fn !== 'function') throw new JSError(`'${String(key)}' is not a function`);
      const args = this.evalArgs(e.args, scope);
      if (fn.__fn) return fn.call(null, recv, ...args);
      return fn.apply(recv, args);
    }
    const fn = this.evalExpr(e.callee, scope);
    if (typeof fn !== 'function') throw new JSError('Not a function');
    const args = this.evalArgs(e.args, scope);
    if (fn.__fn) return fn.call(null, undefined, ...args);
    return fn(...args);
  }
  evalNew(e, scope){
    const ctor = this.evalExpr(e.callee, scope);
    if (typeof ctor !== 'function') throw new JSError('new requires a constructor');
    const args = this.evalArgs(e.args, scope);
    const proto = ctor.prototype && typeof ctor.prototype === 'object' ? ctor.prototype : Object.prototype;
    const obj = Object.create(proto);
    if (ctor.__fn) { const ret = ctor.call(null, obj, ...args); if (ret !== undefined && ret !== null && (typeof ret === 'object' || typeof ret === 'function')) return ret; return obj; }
    const ret = ctor.apply(obj, args);
    if (ret !== undefined && ret !== null && (typeof ret === 'object' || typeof ret === 'function')) return ret;
    return obj;
  }
  evalUnary(e, scope){
    if (e.op === 'typeof') { try { return this.jsTypeof(this.evalExpr(e.arg, scope)); } catch (err) { return 'undefined'; } }
    if (e.op === 'delete') { if (e.arg.type === 'Member') { const obj = this.evalExpr(e.arg.obj, scope); const key = e.arg.computed ? this.evalExpr(e.arg.prop, scope) : e.arg.prop.value; if (obj != null) { try { delete obj[key]; } catch {} } return true; } return true; }
    const v = this.evalExpr(e.arg, scope);
    switch (e.op) { case '!': return !this.truthy(v); case '-': return -Number(v); case '+': return +Number(v); case '~': return ~v; case 'void': return undefined; }
    throw new JSError(`Unknown unary: ${e.op}`);
  }
  evalUpdate(e, scope){
    if (e.arg.type === 'Ident') { const cur = Number(scope.get(e.arg.name)); const nv = e.op === '++' ? cur + 1 : cur - 1; scope.set(e.arg.name, nv); return e.prefix ? nv : cur; }
    if (e.arg.type === 'Member') { const obj = this.evalExpr(e.arg.obj, scope); const key = e.arg.computed ? this.evalExpr(e.arg.prop, scope) : e.arg.prop.value; const cur = Number(obj[key]); const nv = e.op === '++' ? cur + 1 : cur - 1; obj[key] = nv; return e.prefix ? nv : cur; }
    throw new JSError('Invalid update target');
  }
  evalAssign(e, scope){
    const RHS = () => this.evalExpr(e.value, scope);
    const apply = (cur, rhs) => {
      switch (e.op) {
        case '=': return rhs;
        case '+=': return (typeof cur === 'string' || typeof rhs === 'string') ? String(cur) + String(rhs) : Number(cur) + Number(rhs);
        case '-=': return Number(cur) - Number(rhs);
        case '*=': return Number(cur) * Number(rhs);
        case '/=': return Number(cur) / Number(rhs);
        case '%=': return Number(cur) % Number(rhs);
        case '**=': return Number(cur) ** Number(rhs);
        case '&=': return cur & rhs;
        case '|=': return cur | rhs;
        case '^=': return cur ^ rhs;
        case '<<=': return cur << rhs;
        case '>>=': return cur >> rhs;
        case '>>>=': return cur >>> rhs;
        case '&&=': return this.truthy(cur) ? rhs : cur;
        case '||=': return this.truthy(cur) ? cur : rhs;
        case '??=': return (cur === null || cur === undefined) ? rhs : cur;
      }
      throw new JSError(`Unknown assignment: ${e.op}`);
    };
    if (e.target.type === 'Ident') { const cur = scope.get(e.target.name); const nv = apply(cur, RHS()); return scope.set(e.target.name, nv); }
    if (e.target.type === 'Member') { const obj = this.evalExpr(e.target.obj, scope); const key = e.target.computed ? this.evalExpr(e.target.prop, scope) : e.target.prop.value; const cur = obj[key]; const nv = apply(cur, RHS()); obj[key] = nv; return nv; }
    throw new JSError('Invalid assignment target');
  }
  evalBinary(e, scope){
    const op = e.op; const l = this.evalExpr(e.left, scope);
    if (op === 'in') { const r = this.evalExpr(e.right, scope); return (r != null && (typeof r === 'object' || typeof r === 'function')) ? (l in r) : false; }
    if (op === 'instanceof') { const r = this.evalExpr(e.right, scope); if (typeof r !== 'function') return false; if (r.__classCtor && r.prototype) { let p = l && l.__proto__; while (p) { if (p === r.prototype) return true; p = p.__proto__; } return false; } try { return l instanceof r; } catch { return false; } }
    const r = this.evalExpr(e.right, scope);
    switch (op) {
      case '+': return (typeof l === 'string' || typeof r === 'string') ? String(l) + String(r) : Number(l) + Number(r);
      case '-': return Number(l) - Number(r);
      case '*': return Number(l) * Number(r);
      case '/': return Number(l) / Number(r);
      case '%': return Number(l) % Number(r);
      case '**': return Number(l) ** Number(r);
      case '==': return this.looseEq(l, r);
      case '!=': return !this.looseEq(l, r);
      case '===': return l === r;
      case '!==': return l !== r;
      case '<': return l < r;
      case '>': return l > r;
      case '<=': return l <= r;
      case '>=': return l >= r;
      case '&': return l & r;
      case '|': return l | r;
      case '^': return l ^ r;
      case '<<': return l << r;
      case '>>': return l >> r;
      case '>>>': return l >>> r;
    }
    throw new JSError(`Unknown binary: ${op}`);
  }
  looseEq(a, b){ if (a === b) return true; if (a === null && b === undefined) return true; if (a === undefined && b === null) return true; if (typeof a === typeof b) return a === b; if (typeof a === 'number' && typeof b === 'string') return a === Number(b); if (typeof a === 'string' && typeof b === 'number') return Number(a) === b; if (typeof a === 'boolean') return this.looseEq(Number(a), b); if (typeof b === 'boolean') return this.looseEq(a, Number(b)); return false; }
  truthy(v){ if (v === null || v === undefined || v === false) return false; if (typeof v === 'number') return v !== 0 && !Number.isNaN(v); if (typeof v === 'string') return v.length > 0; return true; }
  jsTypeof(v){ if (v === null) return 'object'; if (v === undefined) return 'undefined'; if (typeof v === 'function') return 'function'; if (Array.isArray(v)) return 'object'; return typeof v; }
  wrapCallback(fn) {
    if (typeof fn !== 'function' || !fn.__fn) return fn;
    return function(...callbackArgs) {
      return fn.call(null, undefined, ...callbackArgs);
    };
  }
  getProp(obj, key){
    const k = String(key);
    if (BLOCKED_PROPS.has(k)) return undefined;
    if (obj === null || obj === undefined) throw new JSError(`Cannot read '${k}'`);
    const t = typeof obj;
    if (t === 'string' || t === 'number' || t === 'boolean') { if (k === 'length' && t === 'string') return obj.length; const boxed = this.getPrimitiveMethod(t, obj, k); if (boxed !== undefined) return boxed; }
    if (Array.isArray(obj)) {
      if (k === 'length') return obj.length;
      const m = Array.prototype[k];
      if (typeof m === 'function') return (...args) => {
        this.tick();
        const wrapped = args.map(a => this.wrapCallback(a));
        return m.apply(obj, wrapped);
      };
      if (k in obj) return obj[k];
      return undefined;
    }
    if (obj instanceof Map) {
      if (k === 'size') return obj.size;
      const m = Map.prototype[k];
      if (typeof m === 'function') return (...args) => {
        this.tick();
        const wrapped = args.map(a => this.wrapCallback(a));
        return m.apply(obj, wrapped);
      };
    }
    if (obj instanceof Set) {
      if (k === 'size') return obj.size;
      const m = Set.prototype[k];
      if (typeof m === 'function') return (...args) => {
        this.tick();
        const wrapped = args.map(a => this.wrapCallback(a));
        return m.apply(obj, wrapped);
      };
    }
    if (obj instanceof RegExp) { if (['source','flags','global','ignoreCase','multiline','lastIndex'].includes(k)) return obj[k]; const m = RegExp.prototype[k]; if (typeof m === 'function') return (...args) => { this.tick(); return m.apply(obj, args); }; }
    if (obj instanceof Date) { const m = Date.prototype[k]; if (typeof m === 'function') return (...args) => { this.tick(); return m.apply(obj, args); }; }
    if (obj instanceof Error) { if (k === 'message' || k === 'name' || k === 'stack') return obj[k]; const m = Error.prototype[k]; if (typeof m === 'function') return (...args) => { this.tick(); return m.apply(obj, args); }; }
    if (t === 'object' || t === 'function') { if (k === 'length' && typeof obj === 'function') return obj.length; if (k === 'name' && typeof obj === 'function') return obj.name || ''; if (k in obj) { const v = obj[k]; if (typeof v === 'function') return (...args) => { this.tick(); return v.apply(obj, args); }; return v; } if (k === 'hasOwnProperty') return (p) => Object.prototype.hasOwnProperty.call(obj, p); if (k === 'toString') return () => this.display(obj); if (k === 'valueOf') return () => obj; }
    return undefined;
  }
  getPrimitiveMethod(type, obj, k){ let proto; if (type === 'string') proto = String.prototype; else if (type === 'number') proto = Number.prototype; else if (type === 'boolean') proto = Boolean.prototype; if (!proto) return undefined; const fn = proto[k]; if (typeof fn !== 'function') return undefined; return (...args) => { this.tick(); return fn.apply(obj, args); }; }
}

function evaluateJSSandboxed(src){
  let code = String(src || '').trim();
  if (!code) return 'Error: Empty code';
  if (code.length > JS_LIMITS.MAX_CODE_CHARS) return `Error: Code exceeds ${JS_LIMITS.MAX_CODE_CHARS} characters`;
  code = code.replace(/×/g, '*').replace(/÷/g, '/').replace(/−/g, '-').replace(/(\d+(?:\.\d+)?)\s*%\s*of\s*/gi, '($1/100)*');
  if (!/\*\*/.test(code)) code = code.replace(/\^/g, '**');
  try { const toks = jsLex(code); const ast = new JSParser(toks).parseProgram(); const interp = new JSInterpreter(); const res = interp.run(ast); return res.output; }
  catch (e) { return 'Error: ' + (e && e.message ? e.message : String(e)); }
}

// ---------------------------------------------------------------------------
// 6. CORS / RESPONSE HELPERS
// ---------------------------------------------------------------------------
function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
}
function json(data, status = 200) { return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json', ...corsHeaders() } }); }
function errorResponse(message, status = 400) { return json({ error: message }, status); }
function getAllKeys(env, keyEnv) { if (!keyEnv) return []; const raw = env[keyEnv]; if (!raw) return []; return raw.split(',').map(s => s.trim()).filter(Boolean); }
function hasAnyKey(env, mode) {
  const providers = PROVIDERS[mode] || [];
  for (const p of providers) if (getAllKeys(env, p.keyEnv).length) return true;
  return false;
}
function pickExternalKey(env, keyEnv) {
  const keys = getAllKeys(env, keyEnv);
  if (!keys.length) return null;
  if (keys.length === 1) return keys[0];
  return keys[Math.floor(Math.random() * keys.length)];
}

// ---------------------------------------------------------------------------
// 7. BLOB STORE
// ---------------------------------------------------------------------------
const BLOB_KV_PREFIX = 'blob:';
const PREVIEW_KV_PREFIX = 'preview:';
const KV_MAX_VALUE_BYTES = 24 * 1024 * 1024;
const D1_MAX_BLOB_BYTES  = 1_800_000;

async function blobPut(env, blobId, blob) {
  const payload = JSON.stringify(blob);
  const byteLen = new TextEncoder().encode(payload).length;
  if (env.CHATS && byteLen < KV_MAX_VALUE_BYTES) {
    try { await env.CHATS.put(BLOB_KV_PREFIX + blobId, payload); return { where: 'kv', id: blobId }; }
    catch (e) { log(`[blobPut] KV failed: ${safeStr(e).slice(0, 160)}`); }
  }
  if (byteLen > D1_MAX_BLOB_BYTES) return { error: `Blob too large (${Math.round(byteLen / 1024)} KB)` };
  try { await env.DB.prepare('INSERT INTO blobs (id, username, mime, name, size, data, created_at) VALUES (?,?,?,?,?,?,?)').bind(blobId, blob.username || '', blob.mime || 'application/octet-stream', blob.name || '', blob.size || byteLen, payload, Math.floor(Date.now() / 1000)).run(); return { where: 'd1', id: blobId }; }
  catch (e) { return { error: safeStr(e) }; }
}
async function blobGet(env, blobId) {
  if (env.CHATS) { try { const v = await env.CHATS.get(BLOB_KV_PREFIX + blobId); if (v) return JSON.parse(v); } catch (e) {} }
  try { const row = await env.DB.prepare('SELECT data FROM blobs WHERE id = ?').bind(blobId).first(); if (row && row.data) return JSON.parse(row.data); } catch (e) {}
  return null;
}
async function blobDelete(env, blobId) {
  try { if (env.CHATS) await env.CHATS.delete(BLOB_KV_PREFIX + blobId); } catch (e) {}
  try { if (env.CHATS) await env.CHATS.delete(PREVIEW_KV_PREFIX + blobId); } catch (e) {}
  try { await env.DB.prepare('DELETE FROM blobs WHERE id = ?').bind(blobId).run(); } catch (e) {}
}
function extractBlobIds(text) { const ids = new Set(); const re = /blob:([a-zA-Z0-9-]+)/g; let m; while ((m = re.exec(String(text || ''))) !== null) ids.add(m[1]); return Array.from(ids); }
function isTextMimeForPreview(mime) {
  const m = String(mime || '').toLowerCase();
  if (m.startsWith('text/')) return true;
  if (/(json|jsonl|ndjson|xml|javascript|ecmascript|yaml|toml|markdown|md|sh\b|shell|sql|graphql|python|ruby|rust|go|java|csv|tsv)/.test(m)) return true;
  return false;
}
function decodeDataUrl(dataUrl) { const m = String(dataUrl || '').match(/^data:([^;]+);base64,(.+)$/); if (!m) return null; try { const bin = atob(m[2]); const bytes = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i); return { mime: m[1], bytes }; } catch (e) { return null; } }
function computePreviewFromBlob(blob) {
  const mime = blob.mime || '';
  if (!isTextMimeForPreview(mime)) { const kb = Math.max(1, Math.round((blob.size || 0) / 1024)); return { text: `[${mime || 'binary'} — ${kb} KB]`, totalLines: 0, previewComplete: true, isBinary: true }; }
  const decoded = decodeDataUrl(blob.data);
  if (!decoded) return { text: '[unreadable]', totalLines: 0, previewComplete: false, isBinary: false };
  let text;
  try { text = new TextDecoder('utf-8', { fatal: false }).decode(decoded.bytes); } catch (e) { return { text: '[decode failed]', totalLines: 0, previewComplete: false, isBinary: false }; }
  const lines = text.split(/\r?\n/);
  const totalLines = lines.length;
  const previewComplete = totalLines <= FILE_PREVIEW_LINES;
  const head = lines.slice(0, FILE_PREVIEW_LINES).join('\n');
  const more = Math.max(0, totalLines - FILE_PREVIEW_LINES);
  let body = previewComplete ? head : `${head}\n… (${more} more line${more === 1 ? '' : 's'})`;
  if (body.length > FILE_PREVIEW_CHARS) body = body.slice(0, FILE_PREVIEW_CHARS) + '…';
  return { text: body, totalLines, previewComplete, isBinary: false };
}
async function getBlobPreview(env, blob) {
  if (env.CHATS) { try { const cached = await env.CHATS.get(PREVIEW_KV_PREFIX + blob.id); if (cached) { try { return JSON.parse(cached); } catch {} } } catch (e) {} }
  const preview = computePreviewFromBlob(blob);
  if (env.CHATS) { try { await env.CHATS.put(PREVIEW_KV_PREFIX + blob.id, JSON.stringify(preview), { expirationTtl: 30 * 24 * 60 * 60 }); } catch (e) {} }
  return preview;
}

async function buildFileIndex(env, chat, currentImageBlobIds = null) {
  if (!chat || !Array.isArray(chat.messages)) return '';
  const seen = new Set();
  const orderedIds = [];
  for (let i = chat.messages.length - 1; i >= 0; i--) {
    const m = chat.messages[i];
    if (m.role !== 'user' || !m.content) continue;
    for (const id of extractBlobIds(String(m.content))) {
      if (seen.has(id)) continue;
      seen.add(id);
      orderedIds.push({ id, isCurrent: !!(currentImageBlobIds && currentImageBlobIds.has(id)) });
    }
  }
  if (!orderedIds.length) return '';

  const metas = [];
  for (const { id: bid, isCurrent } of orderedIds) {
    const blob = await blobGet(env, bid);
    if (!blob) continue;
    const isImg = isImageMime(blob.mime);
    const preview = isImg ? null : await getBlobPreview(env, { ...blob, id: bid });
    metas.push({ bid, blob, isImg, isCurrent, preview });
  }
  if (!metas.length) return '';

  const roster = metas.map((m, i) => {
    const nm = m.blob.name || 'file';
    const kb = m.blob.size ? Math.max(1, Math.round(m.blob.size / 1024)) : 0;
    const sizeTag = kb ? `, ${kb} KB` : '';
    const curTag = m.isCurrent ? ' ← CURRENT TURN' : '';
    return `${i + 1}. ${nm} (${m.blob.mime || 'unknown'}${sizeTag})${curTag}`;
  }).join('\n');

  const detailParts = [];
  let used = 0;
  let previewsOmitted = 0;

  for (const m of metas) {
    const nm = m.blob.name || 'file';

    if (m.isCurrent && m.isImg) {
      detailParts.push(`### ${nm} — IMAGE — ALREADY ATTACHED to the CURRENT message as native input. Do NOT call <analysing>. Read the pixels directly.`);
      used += 200;
      continue;
    }
    if (m.isImg) {
      detailParts.push(`### ${nm} — IMAGE (earlier turn). Call <analysing>${nm}</analysing> to load natively.`);
      used += 150;
      continue;
    }
    if (!m.preview) continue;

    const lines = m.preview.totalLines || 0;
    const curSuffix = m.isCurrent ? ' ← CURRENT — read it and answer.' : '';
    const status = m.preview.isBinary
      ? `BINARY — call <analysing>${nm}</analysing> to attach.`
      : m.preview.previewComplete
        ? `COMPLETE — all ${lines} line${lines === 1 ? '' : 's'} shown.${curSuffix}`
        : `PARTIAL — first ${FILE_PREVIEW_LINES} of ${lines} lines. Full file via <analysing>${nm}</analysing>.${curSuffix}`;

    const block = `### ${nm} — ${status}\n${m.preview.text}`;
    if (used + block.length > FILE_INDEX_CHARS) {
      previewsOmitted++;
      detailParts.push(`### ${nm} — preview omitted (index size limit). Full via <analysing>${nm}</analysing>.`);
      used += 120;
      continue;
    }
    detailParts.push(block);
    used += block.length + 2;
  }

  let out = `## Files in this conversation (${metas.length})\n${roster}\n`;
  if (detailParts.length) {
    out += `\n## Previews\n${detailParts.join('\n\n')}`;
  }
  if (previewsOmitted > 0) {
    out += `\n\n_(${previewsOmitted} preview${previewsOmitted === 1 ? '' : 's'} omitted above — the full names are still listed. Use <analysing>filename.ext</analysing> to load any file's content.)_`;
  }
  return out;
}

// ---------------------------------------------------------------------------
// 8. MODELTRACKER + KEY ROUTER
// ---------------------------------------------------------------------------
async function mtGet(env, key) { if (!env.MODELTRACKER) return null; try { const v = await env.MODELTRACKER.get(key); if (v) return JSON.parse(v); } catch (e) {} return null; }
async function mtPut(env, key, value, ttlSeconds) { if (!env.MODELTRACKER) return false; try { const opts = ttlSeconds ? { expirationTtl: Math.max(60, ttlSeconds) } : undefined; await env.MODELTRACKER.put(key, JSON.stringify(value), opts); return true; } catch (e) { return false; } }
async function mtDelete(env, key) { try { if (env.MODELTRACKER) await env.MODELTRACKER.delete(key); } catch (e) {} }

function hashKey(key) { let h = 0; for (let i = 0; i < key.length; i++) h = ((h << 5) - h + key.charCodeAt(i)) | 0; return (h >>> 0).toString(36); }

function secondsUntilMidnightPacific() {
  const now = Date.now();
  try {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Los_Angeles',
      hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
    }).formatToParts(new Date(now));
    const get = (t) => parseInt(parts.find(p => p.type === t)?.value || '0', 10);
    const h = get('hour') % 24;
    const mi = get('minute');
    const s = get('second');
    const secsToday = h * 3600 + mi * 60 + s;
    return Math.max(300, 86400 - secsToday);
  } catch {
    return Math.max(300, 86400);
  }
}

async function keyStateGet(env, key) { return await mtGet(env, `kstate:${hashKey(key)}`); }
async function keyStateMarkRPM(env, key) { await mtPut(env, `kstate:${hashKey(key)}`, { reason: 'rpm', ts: Date.now() }, 90); }
async function keyStateMarkRPD(env, key) {
  const ttl = secondsUntilMidnightPacific();
  await mtPut(env, `kstate:${hashKey(key)}`, { reason: 'rpd', ts: Date.now() }, ttl);
  await mtDelete(env, `kstate:rpm:${hashKey(key)}`);
}
async function keyStateClear(env, key) { await mtDelete(env, `kstate:${hashKey(key)}`); }

async function getAvailableKeys(env, keyEnv) {
  const all = getAllKeys(env, keyEnv);
  if (!all.length) return [];
  const available = [];
  for (const k of all) {
    const st = await keyStateGet(env, k);
    if (!st) { available.push(k); continue; }
    const ageMs = Date.now() - (st.ts || 0);
    if (st.reason === 'rpm' && ageMs > 90_000) { await keyStateClear(env, k); available.push(k); }
  }
  return available;
}

function cooldownKey(providerName, model) { return `cooldown:${providerName}:${model}`; }
async function recordFailure(env, providerName, model, status) {
  const now = Math.floor(Date.now() / 1000);
  const cooldownUntil = now + FAILURE_COOLDOWN_SECONDS;
  await mtPut(env, cooldownKey(providerName, model), { cooldownUntil, ts: now, status }, FAILURE_COOLDOWN_SECONDS + 300);
}
async function isOnCooldown(env, providerName, model) {
  if (DISABLE_FAILURE_COOLDOWN) return false;
  const now = Math.floor(Date.now() / 1000);
  const key = cooldownKey(providerName, model);
  const kv = await mtGet(env, key);
  if (!kv) return false;
  if (now < (kv.cooldownUntil || 0)) return true;
  await mtDelete(env, key);
  return false;
}
async function recordSuccess(env, mode, providerName, model, latencyMs) {
  const now = Math.floor(Date.now() / 1000);
  await mtDelete(env, cooldownKey(providerName, model));
  if (typeof latencyMs === 'number' && latencyMs > 0) {
    const fastest = await mtGet(env, `fastest:${mode}`);
    if (!fastest || !fastest.latency || latencyMs < fastest.latency) {
      await mtPut(env, `fastest:${mode}`, { provider: providerName, model, latency: latencyMs, ts: now, mode }, FASTEST_TTL);
    }
    const globalFastest = await mtGet(env, 'fastest:global');
    if (!globalFastest || !globalFastest.latency || latencyMs < globalFastest.latency) {
      await mtPut(env, 'fastest:global', { provider: providerName, model, latency: latencyMs, ts: now, mode }, FASTEST_TTL);
    }
  }
  await setStickyModel(env, providerName, model);
}
async function getFastestModel(env, mode) { return await mtGet(env, `fastest:${mode}`) || null; }

// ---------------------------------------------------------------------------
// 9. AUTH
// ---------------------------------------------------------------------------
async function hashPassword(password) {
  const encoder = new TextEncoder();
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const km = await crypto.subtle.importKey('raw', encoder.encode(password), 'PBKDF2', false, ['deriveBits']);
  const hb = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' }, km, 256);
  return Array.from(salt).map(b => b.toString(16).padStart(2, '0')).join('') + ':' + Array.from(new Uint8Array(hb)).map(b => b.toString(16).padStart(2, '0')).join('');
}
async function verifyPassword(password, stored) {
  const [saltHex, hashHex] = stored.split(':');
  if (!saltHex || !hashHex) return false;
  const salt = new Uint8Array(saltHex.match(/.{1,2}/g).map(b => parseInt(b, 16)));
  const encoder = new TextEncoder();
  const km = await crypto.subtle.importKey('raw', encoder.encode(password), 'PBKDF2', false, ['deriveBits']);
  const hb = await crypto.subtle.deriveBits({ name: 'PBKDF2', salt, iterations: 100000, hash: 'SHA-256' }, km, 256);
  return Array.from(new Uint8Array(hb)).map(b => b.toString(16).padStart(2, '0')).join('') === hashHex;
}
async function getUserFromToken(token, env) {
  try { const r = await env.DB.prepare("SELECT username FROM tokens WHERE token = ? AND (strftime('%s', 'now') - created_at) < ?").bind(token, TOKEN_TTL).first(); return r?.username || null; } catch { return null; }
}
async function requireAuth(req, env) {
  const h = req.headers.get('Authorization');
  if (h && h.startsWith('Bearer ')) { const u = await getUserFromToken(h.slice(7), env); if (u) return u; }
  try { const url = new URL(req.url); const qt = url.searchParams.get('t'); if (qt) return getUserFromToken(qt, env); } catch (e) {}
  return null;
}

// ---------------------------------------------------------------------------
// 10. DB INIT
// ---------------------------------------------------------------------------
async function initDatabase(db) {
  await db.prepare(`CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT NOT NULL)`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS tokens (token TEXT PRIMARY KEY, username TEXT NOT NULL, created_at INTEGER NOT NULL)`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS chats (id TEXT PRIMARY KEY, username TEXT NOT NULL, title TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, mode TEXT DEFAULT 'quick')`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, chat_id TEXT NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL, mode TEXT, timestamp INTEGER NOT NULL, blocks TEXT)`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS rate_limits (user_id TEXT NOT NULL, timestamp INTEGER NOT NULL)`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS idempotency_keys (key TEXT PRIMARY KEY, message_id TEXT NOT NULL, created_at INTEGER NOT NULL)`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS blobs (id TEXT PRIMARY KEY, username TEXT NOT NULL, mime TEXT NOT NULL, name TEXT, size INTEGER, data TEXT NOT NULL, created_at INTEGER NOT NULL)`).run();
  await db.prepare(`CREATE TABLE IF NOT EXISTS embeddings (id TEXT PRIMARY KEY, username TEXT NOT NULL, chat_id TEXT, blob_id TEXT, source TEXT, mime TEXT, model TEXT, dim INTEGER, vector TEXT NOT NULL, meta TEXT, created_at INTEGER NOT NULL)`).run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_rate_limits_user_time ON rate_limits (user_id, timestamp)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_tokens_username ON tokens(username)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_chats_username ON chats(username)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_idempotency_created ON idempotency_keys (created_at)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_blobs_username ON blobs (username)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_blobs_created ON blobs (created_at)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_embeddings_user ON embeddings (username)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_embeddings_chat ON embeddings (chat_id)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_embeddings_blob ON embeddings (blob_id)').run();
  try { const t = await db.prepare('PRAGMA table_info(chats)').all(); if (!t.results.some(c => c.name === 'updated_at')) { await db.prepare('ALTER TABLE chats ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0').run(); await db.prepare('UPDATE chats SET updated_at = created_at WHERE updated_at = 0').run(); } } catch {}
  try { const t = await db.prepare('PRAGMA table_info(chats)').all(); if (!t.results.some(c => c.name === 'mode')) await db.prepare("ALTER TABLE chats ADD COLUMN mode TEXT DEFAULT 'quick'").run(); } catch (e) {}
  try { const t = await db.prepare('PRAGMA table_info(messages)').all(); if (!t.results.some(c => c.name === 'blocks')) await db.prepare('ALTER TABLE messages ADD COLUMN blocks TEXT').run(); } catch (e) {}
}
let __dbReadyPromise = null, __dbReadyAt = 0;
const __DB_READY_TTL_MS = 5 * 60 * 1000;
async function ensureDatabase(env) {
  const now = Date.now();
  if (!__dbReadyPromise || now - __dbReadyAt > __DB_READY_TTL_MS) {
    __dbReadyAt = now;
    __dbReadyPromise = initDatabase(env.DB).catch((e) => { __dbReadyPromise = null; throw e; });
  }
  return __dbReadyPromise;
}

// ---------------------------------------------------------------------------
// 11. RATE LIMIT + HOUSEKEEPING
// ---------------------------------------------------------------------------
async function checkRateLimit(env, userId) {
  const now = Math.floor(Date.now() / 1000);
  const ws = now - RATE_LIMIT_WINDOW;
  try {
    await env.DB.prepare('DELETE FROM rate_limits WHERE user_id = ? AND timestamp < ?').bind(userId, ws).run();
    const c = await env.DB.prepare('SELECT COUNT(*) as cnt FROM rate_limits WHERE user_id = ? AND timestamp >= ?').bind(userId, ws).first();
    if ((c?.cnt || 0) >= RATE_LIMIT_MAX) return false;
    await env.DB.prepare('INSERT INTO rate_limits (user_id, timestamp) VALUES (?, ?)').bind(userId, now).run();
    return true;
  } catch (e) { return true; }
}
async function saveIdempotency(env, key, messageId) {
  const now = Math.floor(Date.now() / 1000);
  try { await env.DB.prepare('INSERT INTO idempotency_keys (key, message_id, created_at) VALUES (?, ?, ?) ON CONFLICT (key) DO NOTHING').bind(key, messageId, now).run(); } catch (e) {}
}
async function cleanupIdempotencyKeys(env) { const cutoff = Math.floor(Date.now() / 1000) - 86400; try { await env.DB.prepare('DELETE FROM idempotency_keys WHERE created_at < ?').bind(cutoff).run(); } catch (e) {} }

// ---------------------------------------------------------------------------
// 12. EXTERNAL TOOLS
// ---------------------------------------------------------------------------
async function performWeatherLookup(env, query) {
  const apiKey = pickExternalKey(env, 'WA_KEYS');
  if (!apiKey) return { error: 'Weather service not configured' };
  let location = query, highlightDate = null;
  if (query.includes('|')) { const p = query.split('|'); location = p[0].trim(); const pa = p[1].trim(); if (/^\d{4}-\d{2}-\d{2}$/.test(pa)) highlightDate = pa; }
  const fmt = d => d.toISOString().split('T')[0];
  const yest = new Date(); yest.setDate(yest.getDate() - 1);
  const startDt = new Date(); startDt.setDate(startDt.getDate() - 3);
  try {
    const [cr, hr] = await Promise.all([
      fetchWithTimeout(`https://api.weatherapi.com/v1/current.json?key=${apiKey}&q=${encodeURIComponent(location)}&aqi=no`, {}, TOOL_FETCH_TIMEOUT_MS, 'weather-current', 1),
      fetchWithTimeout(`https://api.weatherapi.com/v1/history.json?key=${apiKey}&q=${encodeURIComponent(location)}&dt=${fmt(startDt)}&end_dt=${fmt(yest)}`, {}, TOOL_FETCH_TIMEOUT_MS, 'weather-history', 1),
    ]);
    const cd = await cr.json().catch(() => ({}));
    const hd = await hr.json().catch(() => ({}));
    if (cd.error) return { error: safeStr(cd.error.message) };
    if (hd.error) return { error: safeStr(hd.error.message) };
    const history = (hd.forecast?.forecastday || []).map(d => ({
      date: d.date, condition: d.day.condition.text,
      avgTemp: `${Math.round(d.day.avgtemp_c)}°C`, maxTemp: `${Math.round(d.day.maxtemp_c)}°C`, minTemp: `${Math.round(d.day.mintemp_c)}°C`,
      humidity: `${d.day.avghumidity}%`, wind: `${Math.round(d.day.maxwind_kph)} km/h`,
      rainChance: `${d.day.daily_chance_of_rain ?? 0}%`,
    }));
    return {
      header: cd.location.name,
      now: { text: cd.current.condition.text, temp: `${cd.current.temp_c}°C`, feels: `${cd.current.feelslike_c}°C`, humidity: `${cd.current.humidity}%`, wind: `${cd.current.wind_kph} km/h` },
      history, highlightDate,
    };
  } catch (e) { return { error: `Weather lookup failed: ${safeStr(e)}` }; }
}

async function performWebSearch(env, query) {
  const tv = pickExternalKey(env, 'TAVILY_KEYS');
  if (!tv) return { error: 'Search service not configured' };
  const isNews = /\b(news|latest|breaking|today|this week|recent|just|now|current|new)\b/i.test(query);
  const depth = 'fast';
  const body = {
    api_key: tv, query, search_depth: depth,
    include_answer: false, include_raw_content: false,
    max_results: MAX_SEARCH_SOURCES, chunks_per_source: 1,
    ...(isNews ? { topic: 'news', days: 1, include_published_date: true } : {}),
  };
  try {
    const r = await fetchWithTimeout('https://api.tavily.com/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }, SEARCH_TIMEOUT_MS, 'tavily', 1);
    const d = await r.json().catch(() => ({}));
    if (!r.ok) return { error: `Search failed (HTTP ${r.status})` };
    const sources = (d.results || []).slice(0, MAX_SEARCH_SOURCES).map(x => ({
      title: x.title, url: x.url, content: x.content,
      publishedDate: x.published_date || null, score: x.score,
      sourceName: (() => { try { return new URL(x.url).hostname; } catch { return 'Web'; } })(),
    }));
    if (!sources.length) return { error: 'No search results found' };
    return { rawText: sources.map(s => s.content).join('\n\n'), sources, isNews };
  } catch (e) { return { error: `Search failed: ${safeStr(e)}` }; }
}

async function performFinanceLookup(env, finance) {
  if (finance.type === 'stock') {
    const apiKey = pickExternalKey(env, 'ONETWO_KEYS');
    if (!apiKey) return { error: 'Stock service not configured' };
    try {
      const symbol = String(finance.symbol || '').toUpperCase().trim();
      if (!symbol) return { error: 'No stock symbol provided' };
      const r = await fetchWithTimeout(`https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbol)}&apikey=${apiKey}`, {}, TOOL_FETCH_TIMEOUT_MS, 'twelvedata', 1);
      const d = await r.json().catch(() => ({}));
      if (d && (d.code || d.status === 'error')) {
        const errMsg = safeStr(d.message || d.code);
        if (d.code === 429 || /limit/i.test(errMsg)) return { error: 'Stock service rate limit reached' };
        return { error: `No stock data found for "${symbol}"` };
      }
      const price = parseFloat(d.close);
      if (!isFinite(price) || price <= 0) return { error: `No stock data found for "${symbol}"` };
      return { type: 'stock', symbol: d.symbol || symbol, price, high: d.high ? parseFloat(d.high) : null, low: d.low ? parseFloat(d.low) : null, open: d.open ? parseFloat(d.open) : null, previousClose: d.previous_close ? parseFloat(d.previous_close) : null, currency: d.currency || 'USD', exchange: d.exchange || null, source: 'twelvedata' };
    } catch (e) { return { error: `Stock lookup failed: ${safeStr(e)}` }; }
  }
  if (finance.type === 'forex') {
    const apiKey = pickExternalKey(env, 'ERA_KEYS');
    if (!apiKey) return { error: 'Currency service not configured' };
    try {
      const base = String(finance.base || '').toUpperCase().trim();
      const target = String(finance.target || '').toUpperCase().trim();
      if (!base || !target) return { error: 'Currency pair missing base or target' };
      const r = await fetchWithTimeout(`https://v6.exchangerate-api.com/v6/${apiKey}/latest/${base}`, {}, TOOL_FETCH_TIMEOUT_MS, 'exchangerate', 1);
      const d = await r.json().catch(() => ({}));
      if (d.result !== 'success') return { error: `Currency lookup failed: ${d['error-type'] || 'unknown'}` };
      const rate = d.conversion_rates?.[target];
      if (rate === undefined) return { error: `Currency pair ${base}/${target} not found` };
      return { type: 'forex', base, target, rate, source: 'exchangerate' };
    } catch (e) { return { error: `Currency lookup failed: ${safeStr(e)}` }; }
  }
  return { error: 'Invalid finance request' };
}

async function performAnalyseLookup(env, rawUrl) {
  const urlStr = String(rawUrl || '').trim();
  if (!urlStr) return { error: 'No URL provided' };
  let parsedUrl;
  try { parsedUrl = new URL(urlStr); } catch { return { error: 'Invalid URL' }; }
  if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') return { error: 'Only http(s) URLs can be read' };
  const sourceName = parsedUrl.hostname.replace(/^www\./, '');
  const fc = pickExternalKey(env, 'FIRECRAWL_KEYS');
  if (fc) {
    try {
      const r = await fetchWithTimeout('https://api.firecrawl.dev/v1/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fc}` },
        body: JSON.stringify({ url: urlStr, formats: ['markdown'], onlyMainContent: true, timeout: 12000 }),
      }, ANALYSE_TIMEOUT_MS, 'firecrawl-scrape', 1);
      const d = await r.json().catch(() => ({}));
      if (r.ok && d.success !== false && d.data) {
        const content = String(d.data.markdown || d.data.content || '').trim();
        const title = String(d.data.metadata?.title || parsedUrl.hostname);
        if (content) return { type: 'page', url: urlStr, title: title.slice(0, 200), sourceName, content: content.slice(0, MAX_READ_CHARS), truncated: content.length > MAX_READ_CHARS, charCount: content.length };
      }
      return { error: `Firecrawl couldn't extract content from ${sourceName}` };
    } catch (e) { return { error: `Firecrawl failed: ${safeStr(e)}` }; }
  }
  try {
    const r = await fetchWithTimeout(urlStr, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ZebAI/1.0)', 'Accept': 'text/html,application/xhtml+xml' } }, TOOL_FETCH_TIMEOUT_MS, 'analyse-raw', 1);
    if (!r.ok) return { error: `Page returned HTTP ${r.status}` };
    const html = await r.text();
    const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
    const title = titleMatch ? titleMatch[1].trim() : parsedUrl.hostname;
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<noscript[\s\S]*?<\/noscript>/gi, '').replace(/<nav[\s\S]*?<\/nav>/gi, '')
      .replace(/<footer[\s\S]*?<\/footer>/gi, '').replace(/<header[\s\S]*?<\/header>/gi, '')
      .replace(/<aside[\s\S]*?<\/aside>/gi, '').replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\s+/g, ' ').trim();
    if (!text) return { error: 'Page contained no readable text' };
    return { type: 'page', url: urlStr, title: String(title).slice(0, 200), sourceName, content: text.slice(0, MAX_READ_CHARS), truncated: text.length > MAX_READ_CHARS, charCount: text.length };
  } catch (e) { return { error: `Failed to read page: ${safeStr(e)}` }; }
}

async function collectConversationFiles(env, chat, currentAttachments) {
  const ids = new Set();
  if (chat && Array.isArray(chat.messages)) {
    for (const m of chat.messages) if (m.role === 'user' && m.content) for (const bid of extractBlobIds(String(m.content))) ids.add(bid);
  }
  if (Array.isArray(currentAttachments)) {
    for (const a of currentAttachments) if (a && a.blobId) ids.add(a.blobId);
  }
  const files = [];
  for (const bid of ids) {
    const blob = await blobGet(env, bid);
    if (!blob || !blob.data) continue;
    files.push({ blobId: bid, name: blob.name || 'file', mime: blob.mime || 'application/octet-stream', size: blob.size || 0, data: blob.data });
  }
  return files;
}

// ---------------------------------------------------------------------------
// 12.5. GEMINI FILES API
// ---------------------------------------------------------------------------
async function hashFileContent(mime, base64) {
  const sample =
    String(mime || '') + '::' +
    String(base64 || '').length + '::' +
    String(base64 || '').slice(0, 256) + '::' +
    String(base64 || '').slice(-256);
  const buf = new TextEncoder().encode(sample);
  const digest = await crypto.subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 32);
}

async function geminiFileCacheGet(env, key) { return await mtGet(env, GEMINI_FILE_CACHE_PREFIX + key); }
async function geminiFileCacheSet(env, key, value) { await mtPut(env, GEMINI_FILE_CACHE_PREFIX + key, value, GEMINI_FILE_CACHE_TTL); }

function base64ToBytes(base64) {
  const bin = atob(base64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function uploadToGeminiFiles(env, key, mime, base64Data, displayName) {
  const bytes = base64ToBytes(base64Data);

  const startRes = await fetchWithTimeout(`${GEMINI_FILES_UPLOAD_URL}?key=${key}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Upload-Protocol': 'resumable',
      'X-Goog-Upload-Command': 'start',
      'X-Goog-Upload-Header-Content-Length': String(bytes.length),
      'X-Goog-Upload-Header-Content-Type': mime,
    },
    body: JSON.stringify({ file: { displayName: displayName || 'attachment' } }),
  }, 20000, 'gemini-files-start', 1);

  if (!startRes.ok) {
    const t = await startRes.text().catch(() => '');
    throw new Error(`Files start ${startRes.status}: ${t.slice(0, 200)}`);
  }
  const uploadUrl = startRes.headers.get('X-Goog-Upload-URL')
                 || startRes.headers.get('x-goog-upload-url');
  if (!uploadUrl) throw new Error('No upload URL returned from Files API');

  const upRes = await fetchWithTimeout(uploadUrl, {
    method: 'POST',
    headers: {
      'Content-Length': String(bytes.length),
      'X-Goog-Upload-Offset': '0',
      'X-Goog-Upload-Command': 'upload, finalize',
    },
    body: bytes,
  }, GEMINI_FILE_UPLOAD_TIMEOUT_MS, 'gemini-files-upload', 1);

  if (!upRes.ok) {
    const t = await upRes.text().catch(() => '');
    throw new Error(`Files upload ${upRes.status}: ${t.slice(0, 200)}`);
  }
  const j = await upRes.json();
  const file = j?.file;
  if (!file?.uri) throw new Error('No file URI returned');

  return {
    uri: file.uri,
    name: file.name || '',
    mimeType: file.mimeType || mime,
    state: file.state || 'ACTIVE',
  };
}

async function waitForFileActive(env, key, fileName, maxWaitMs = 15000) {
  const t0 = Date.now();
  while (Date.now() - t0 < maxWaitMs) {
    try {
      const r = await fetchWithTimeout(
        `${GEMINI_FILES_BASE}/${fileName}?key=${key}`, {},
        8000, 'gemini-files-state', 0
      );
      if (r.ok) {
        const j = await r.json();
        if (j.state === 'ACTIVE') return true;
        if (j.state === 'FAILED') throw new Error('File processing FAILED');
      }
    } catch (e) {
      if (/FAILED/.test(safeStr(e))) throw e;
    }
    await new Promise(res => setTimeout(res, 1000));
  }
  return false;
}

async function ensureGeminiFile(env, key, mime, base64Data, displayName) {
  if (!key || !mime || !base64Data) return null;
  try {
    const hash = await hashFileContent(mime, base64Data);
    const cached = await geminiFileCacheGet(env, hash);
    if (cached && cached.uri && cached.uploadKey === key) {
      return { uri: cached.uri, uploadKey: key, cached: true };
    }

    const now = Math.floor(Date.now() / 1000);
    try {
      await env.DB.prepare('DELETE FROM rate_limits WHERE user_id = ? AND timestamp < ?')
        .bind(GEMINI_FILE_UPLOAD_BUCKET, now - 60).run();
      const c = await env.DB.prepare('SELECT COUNT(*) as cnt FROM rate_limits WHERE user_id = ? AND timestamp >= ?')
        .bind(GEMINI_FILE_UPLOAD_BUCKET, now - 60).first();
      if ((c?.cnt || 0) >= GEMINI_FILE_UPLOAD_MAX_PER_MIN) {
        log('[gemini-file] rate limit hit — falling back to inline for', mime);
        return null;
      }
      await env.DB.prepare('INSERT INTO rate_limits (user_id, timestamp) VALUES (?, ?)')
        .bind(GEMINI_FILE_UPLOAD_BUCKET, now).run();
    } catch (e) { /* non-fatal */ }

    const up = await uploadToGeminiFiles(env, key, mime, base64Data, displayName);
    if (up.state && up.state !== 'ACTIVE' && up.name) {
      try { await waitForFileActive(env, key, up.name); } catch (e) {
        log('[gemini-file] wait failed:', safeStr(e));
      }
    }
    await geminiFileCacheSet(env, hash, { uri: up.uri, name: up.name, uploadKey: key });
    return { uri: up.uri, uploadKey: key, cached: false };
  } catch (e) {
    log('[gemini-file] upload failed:', safeStr(e));
    return null;
  }
}

// ---------------------------------------------------------------------------
// 13. GEMINI NATIVE
// ---------------------------------------------------------------------------
function convertToGeminiContents(messages, currentKey) {
  const contents = [];
  let systemInstruction = null;
  for (const m of messages) {
    if (m.role === 'system') { systemInstruction = { parts: [{ text: m.content || '' }] }; continue; }
    const role = m.role === 'assistant' ? 'model' : 'user';
    if (Array.isArray(m.attachments) && m.attachments.length) {
      const parts = m.attachments.map(a => {
        if (a.fileUri) {
          return { fileData: { mimeType: a.mime, fileUri: a.fileUri } };
        }
        return { inlineData: { mimeType: a.mime, data: a.base64 } };
      });
      parts.push({ text: m.content || 'Read the attached file(s) and answer the user.' });
      contents.push({ role, parts });
      continue;
    }
    contents.push({ role, parts: [{ text: m.content || '' }] });
  }
  return { contents, systemInstruction };
}

async function tryGeminiNativeStream(provider, key, messages, isVision, mode, env, temperatureOverride = null, hasAttachments = false) {
  if (!key) throw new Error('Gemini native route requires API key');
  const model = provider.model;
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse&key=${key}`;
  const { contents, systemInstruction } = convertToGeminiContents(messages, key);

  const temp = hasAttachments
    ? 0.6
    : (temperatureOverride !== null ? temperatureOverride : 0);

  const generationConfig = {
    maxOutputTokens: provider.maxTokens || MAX_OUTPUT_TOKENS_PER_ROUND,
    temperature: temp,
    topP: 0.9,
  };
  const tc = buildGeminiThinkingConfig(model, mode);
  if (tc) generationConfig.thinkingConfig = tc;
  const body = { contents, generationConfig };
  if (systemInstruction) body.systemInstruction = systemInstruction;

  const inlineCount = contents.reduce((n, c) => n + c.parts.filter(p => p.inlineData).length, 0);
  const fileCount = contents.reduce((n, c) => n + c.parts.filter(p => p.fileData).length, 0);
  log(`[gemini-req] model=${model} mode=${mode} temp=${temp} parts: fileData=${fileCount} inlineData=${inlineCount} bodySize=${JSON.stringify(body).length}`);

  const timeout = provider.timeout || 45000;
  const response = await fetchWithTimeout(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }, timeout, `Google-native-${model}`, 2);
  if (!response.ok) {
    const errorText = await response.text().catch(() => '');
    const err = new Error(`Google ${model} ${response.status}: ${errorText.slice(0, 300)}`);
    err.status = response.status;
    err.retryAfterMs = parseRetryAfterMs(response, errorText);
    throw err;
  }
  return response;
}

// ---------------------------------------------------------------------------
// 14. PIPE STREAM
// ---------------------------------------------------------------------------
function handleGeminiPart(part, parser, sendEventRaw, thinkingState, cotEnabled, acc) {
  const text = part.text;
  if (typeof text !== 'string' || !text) return;
  if (part.thought === true) {
    if (!cotEnabled) return;
    if (thinkingState.charCount >= MAX_THINKING_CHARS) return;
    if (!thinkingState.visible) {
      thinkingState.visible = true;
      thinkingState.sawThinking = true;
      sendEventRaw({ done: false, type: 'thinking_start' });
    }
    const remain = MAX_THINKING_CHARS - thinkingState.charCount;
    const chunk = text.length > remain ? text.slice(0, remain) : text;
    thinkingState.charCount += chunk.length;
    sendEventRaw({ done: false, type: 'thinking', content: chunk });
  } else {
    if (acc) { acc.raw = (acc.raw || '') + text; acc.text += text; }
    parser.feed(text);
  }
}

async function pipeStream(providerResponse, mode, sendEventRaw, env, provider, options = {}) {
  const t0Stream = Date.now();
  const { allowTools = true } = options;
  const cotEnabled = mode === 'code' || mode === 'vision-agent';

  const rawStream = (providerResponse && typeof providerResponse.getReader === 'function') ? providerResponse
    : (providerResponse && providerResponse.body) ? providerResponse.body : null;
  if (!rawStream) return { tools: [], charts: [], sawText: false, sawThinking: false, toolDetected: false, error: 'no_stream', isNetworkError: false };

  const thinkingState = { visible: false, sawThinking: false, charCount: 0, truncated: false };
  const sendEvent = (ev) => {
    if (ev.type === 'thinking_start') {
      thinkingState.sawThinking = true;
      if (!cotEnabled || thinkingState.visible) return;
      thinkingState.visible = true;
      sendEventRaw({ done: false, type: 'thinking_start' });
      return;
    }
    if (ev.type === 'thinking_end') return;
    if (ev.type === 'thinking') {
      if (!cotEnabled) return;
      thinkingState.charCount += (ev.content || '').length;
      sendEventRaw({ done: false, type: 'thinking', content: ev.content || '' });
      return;
    }
    if (ev.type === 'tool_end') return;
    if (ev.type === 'tool_start') { sendEventRaw({ done: false, type: 'tool_tag', name: ev.name }); return; }
    if (ev.type === 'chart') { sendEventRaw({ done: false, type: 'chart_render', blockId: genBlockId(), spec: ev.content || '' }); return; }
    if (ev.type === 'text') { sendEventRaw({ done: false, type: 'text', content: ev.content || '' }); return; }
    sendEventRaw({ done: false, ...ev });
  };
  const closeRoundThinking = () => {
    if (!thinkingState.visible) return;
    thinkingState.visible = false;
    sendEventRaw({ done: false, type: 'thinking_end' });
  };

  const reader = rawStream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  const parser = new StatefulXMLParser(sendEvent, { allowTools, allowThinking: false });
  const acc = { raw: '', text: '' };
  let lastFinishReason = null;
  let sawFirstChunk = false;
  let cancelReason = null;

  let silenceTimer = null;
  const armSilence = () => {
    if (silenceTimer) clearTimeout(silenceTimer);
    silenceTimer = setTimeout(() => {
      sendEventRaw({ done: false, type: 'llm_still_working' });
      armSilence();
    }, LLM_SILENCE_MS);
  };
  armSilence();

  let ttftTimer = null;
  const ttftPromise = new Promise((_, reject) => {
    ttftTimer = setTimeout(() => {
      if (!sawFirstChunk) {
        cancelReason = 'ttft-timeout';
        try { reader.cancel('ttft-timeout'); } catch {}
        reject(new Error('TTFT timeout'));
      }
    }, mode === 'code' || mode === 'vision-agent' ? TTFT_CODE_MS : TTFT_TEXT_MS);
  });

  let chunkTimer = null;
  const readWithWatchdog = () => Promise.race([
    reader.read(),
    new Promise((_, reject) => {
      chunkTimer = setTimeout(() => {
        cancelReason = 'chunk-timeout';
        try { reader.cancel('chunk-timeout'); } catch {}
        reject(new Error('Chunk timeout'));
      }, CHUNK_WATCHDOG_MS);
    }),
  ]);

  try {
    while (true) {
      const chunkPromise = sawFirstChunk ? readWithWatchdog() : Promise.race([reader.read(), ttftPromise]);
      const { done, value } = await chunkPromise;
      if (chunkTimer) { clearTimeout(chunkTimer); chunkTimer = null; }
      if (done) break;
      if (!sawFirstChunk) { sawFirstChunk = true; clearTimeout(ttftTimer); ttftTimer = null; }
      armSilence();
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const dataContent = line.slice(6).trim();
        if (!dataContent || dataContent === '[DONE]') continue;
        let parsed; try { parsed = JSON.parse(dataContent); } catch { continue; }
        const candidate = parsed?.candidates?.[0];
        if (candidate?.finishReason) lastFinishReason = candidate.finishReason;
        const parts = candidate?.content?.parts;
        if (Array.isArray(parts)) {
          for (const part of parts) handleGeminiPart(part, parser, sendEventRaw, thinkingState, cotEnabled, acc);
        }
      }
    }

    // Flush residual SSE buffer — Gemini's final finishReason chunk
    // sometimes arrives without a trailing newline, leaving it in `buffer`.
    if (buffer) {
      buffer += '\n\n';
      const leftover = buffer.split('\n');
      for (const line of leftover) {
        if (!line.startsWith('data: ')) continue;
        const dataContent = line.slice(6).trim();
        if (!dataContent || dataContent === '[DONE]') continue;
        let parsed; try { parsed = JSON.parse(dataContent); } catch { continue; }
        const candidate = parsed?.candidates?.[0];
        if (candidate?.finishReason) lastFinishReason = candidate.finishReason;
        const parts = candidate?.content?.parts;
        if (Array.isArray(parts)) {
          for (const part of parts) handleGeminiPart(part, parser, sendEventRaw, thinkingState, cotEnabled, acc);
        }
      }
      buffer = '';
    }
  } catch (e) {
    const msg = safeStr(e);
    const isTimeout = cancelReason === 'ttft-timeout' || cancelReason === 'chunk-timeout';
    const isExternalAbort = !isTimeout && /abort|cancel/i.test(msg);
    try { reader.cancel(); } catch {}
    closeRoundThinking();
    parser.flush();
    log(`[pipeStream] ERR mode=${mode} cancelReason=${cancelReason || 'n/a'} msg="${msg}" sawFirstChunk=${sawFirstChunk} textLen=${acc.text.length}`);
    return {
      tools: parser.tools, charts: parser.charts, sawText: parser.sawText, sawThinking: thinkingState.sawThinking,
      toolDetected: parser.toolDetected, text: acc.text, raw: acc.raw, finishReason: lastFinishReason,
      error: isTimeout ? msg : (isExternalAbort ? 'external-abort' : msg),
      isNetworkError: isTimeout, isExternalAbort,
      partialAnswer: acc.text,
    };
  } finally {
    if (ttftTimer) clearTimeout(ttftTimer);
    if (chunkTimer) clearTimeout(chunkTimer);
    if (silenceTimer) clearTimeout(silenceTimer);
    if (!sawFirstChunk) { try { await reader.cancel(); } catch {} }
    try { reader.releaseLock(); } catch {}
  }

  parser.flush();
  closeRoundThinking();

  log(`[pipeStream] DONE mode=${mode} finishReason=${lastFinishReason === null ? 'NULL' : lastFinishReason} textLen=${acc.text.length} sawText=${parser.sawText} tools=${parser.tools.length} charts=${parser.charts.length} duration=${Date.now() - t0Stream}ms`);

  return {
    tools: parser.tools, charts: parser.charts, sawText: parser.sawText, sawThinking: thinkingState.sawThinking,
    toolDetected: parser.toolDetected, text: acc.text, raw: acc.raw, finishReason: lastFinishReason,
  };
}

// ---------------------------------------------------------------------------
// 15. TOOL EXECUTION
// ---------------------------------------------------------------------------
function genBlockId() { return 'blk-' + crypto.randomUUID().slice(0, 12); }

function parseFinanceArgument(arg) {
  try {
    const obj = JSON.parse(arg);
    if (obj.type && (obj.type === 'stock' || obj.type === 'forex')) return obj;
    if (obj.symbol) return { type: 'stock', symbol: String(obj.symbol).toUpperCase() };
    if (obj.base && obj.target) return { type: 'forex', base: String(obj.base).toUpperCase(), target: String(obj.target).toUpperCase() };
  } catch (_) {}
  if (/^[A-Z][A-Z0-9.\-]{0,9}$/.test(arg)) return { type: 'stock', symbol: arg.toUpperCase() };
  const forexMatch = arg.match(/^([A-Z]{3})\s*[\/\-]\s*([A-Z]{3})$/);
  if (forexMatch) return { type: 'forex', base: forexMatch[1].toUpperCase(), target: forexMatch[2].toUpperCase() };
  throw new Error('Invalid finance format');
}

async function executeToolAndEmit(tool, env, sendEvent, context = {}) {
  const blockId = genBlockId();
  const name = tool.name;
  const raw = (tool.content || '').trim();

  if (name === 'weather') {
    sendEvent({ done: false, type: 'weather_start', query: raw, blockId });
    try {
      const data = await performWeatherLookup(env, raw);
      if (data && !data.error) { sendEvent({ done: false, type: 'weather_results', data, blockId }); return { ok: true, tool: 'weather', query: raw, data }; }
      const err = safeStr(data?.error || 'Weather lookup failed');
      sendEvent({ done: false, type: 'weather_error', error: err, blockId });
      return { ok: false, tool: 'weather', query: raw, error: err };
    } catch (e) { const err = safeStr(e); sendEvent({ done: false, type: 'weather_error', error: err, blockId }); return { ok: false, tool: 'weather', query: raw, error: err }; }
  }
  if (name === 'search') {
    sendEvent({ done: false, type: 'search_start', query: raw, blockId });
    try {
      const data = await performWebSearch(env, raw);
      if (data && !data.error) { sendEvent({ done: false, type: 'search_results', results: data.rawText, sources: data.sources, blockId }); return { ok: true, tool: 'search', query: raw, data }; }
      const err = safeStr(data?.error || 'Search failed');
      sendEvent({ done: false, type: 'search_error', error: err, blockId });
      return { ok: false, tool: 'search', query: raw, error: err };
    } catch (e) { const err = safeStr(e); sendEvent({ done: false, type: 'search_error', error: err, blockId }); return { ok: false, tool: 'search', query: raw, error: err }; }
  }
  if (name === 'run') {
    sendEvent({ done: false, type: 'run_start', code: raw, blockId });
    const result = evaluateJSSandboxed(raw);
    if (typeof result === 'string' && result.startsWith('Error:')) { sendEvent({ done: false, type: 'run_error', error: result, blockId }); return { ok: false, tool: 'run', query: raw, error: result }; }
    sendEvent({ done: false, type: 'run_results', result, blockId });
    return { ok: true, tool: 'run', query: raw, result };
  }
  if (name === 'analysing') {
    const query = raw;
    sendEvent({ done: false, type: 'analysing_start', query, blockId, source: 'tool_call' });
    if (!query) { const err = 'Filename required.'; sendEvent({ done: false, type: 'analysing_error', error: err, query: '', blockId, source: 'tool_call' }); return { ok: false, tool: 'analysing', query: '', error: err }; }
    try {
      const files = await collectConversationFiles(env, context.chat, context.currentAttachments);
      if (!files.length) { const err = 'No files found in this conversation.'; sendEvent({ done: false, type: 'analysing_error', error: err, query, blockId, source: 'tool_call' }); return { ok: false, tool: 'analysing', query, error: err }; }
      const q = query.toLowerCase();
      const match = files.find(f => f.name === query) || files.find(f => f.name.toLowerCase() === q) || files.find(f => f.name.toLowerCase().includes(q)) || files.find(f => f.name.toLowerCase().startsWith(q));
      if (!match) { const err = `File "${query}" not found. Available: ${files.map(f => f.name).join(', ')}`; sendEvent({ done: false, type: 'analysing_error', error: err, query, blockId, source: 'tool_call' }); return { ok: false, tool: 'analysing', query, error: err }; }
      const m = String(match.data).match(/^data:([^;]+);base64,(.+)$/);
      if (!m) { const err = 'File data is malformed.'; sendEvent({ done: false, type: 'analysing_error', error: err, query, blockId, source: 'tool_call' }); return { ok: false, tool: 'analysing', query, error: err }; }
      const realMime = m[1] || match.mime || 'application/octet-stream';
      sendEvent({ done: false, type: 'analysing_results', query: match.name, data: { name: match.name, mime: realMime, size: match.size }, blockId, source: 'tool_call' });
      let fileUri = null, uploadKey = null;
      try {
        const uploadKeys = await getAvailableKeys(env, 'GOOGLE_KEYS');
        if (uploadKeys.length) {
          const res = await ensureGeminiFile(env, uploadKeys[0], realMime, m[2], match.name);
          if (res && res.uri) { fileUri = res.uri; uploadKey = res.uploadKey; }
        }
      } catch (e) { log('[analysing] native upload failed:', safeStr(e)); }
      return {
        ok: true, tool: 'analysing', query: match.name,
        result: `File "${match.name}" (${realMime}, ${match.size} bytes) attached as native input.`,
        nativeAttachment: { mime: realMime, base64: m[2], name: match.name, fileUri, uploadKey },
      };
    } catch (e) { const err = safeStr(e); sendEvent({ done: false, type: 'analysing_error', error: err, query, blockId, source: 'tool_call' }); return { ok: false, tool: 'analysing', query, error: err }; }
  }
  if (name === 'finance') {
    let financeObj;
    try { financeObj = parseFinanceArgument(raw); }
    catch { sendEvent({ done: false, type: 'finance_error', error: 'Invalid finance data', blockId }); return { ok: false, tool: 'finance', query: raw, error: 'Invalid finance data' }; }
    sendEvent({ done: false, type: 'finance_start', query: financeObj, blockId });
    try {
      const data = await performFinanceLookup(env, financeObj);
      if (data && !data.error) { sendEvent({ done: false, type: 'finance_results', data, blockId }); return { ok: true, tool: 'finance', query: financeObj, data }; }
      const err = safeStr(data?.error || 'Finance lookup failed');
      sendEvent({ done: false, type: 'finance_error', error: err, blockId });
      return { ok: false, tool: 'finance', query: financeObj, error: err };
    } catch (e) { const err = safeStr(e); sendEvent({ done: false, type: 'finance_error', error: err, blockId }); return { ok: false, tool: 'finance', query: financeObj, error: err }; }
  }
  if (name === 'analyse') {
    sendEvent({ done: false, type: 'analyse_start', url: raw, blockId });
    try {
      const data = await performAnalyseLookup(env, raw);
      if (data && !data.error) { sendEvent({ done: false, type: 'analyse_results', data, blockId }); return { ok: true, tool: 'analyse', query: raw, data }; }
      const err = safeStr(data?.error || 'Page read failed');
      sendEvent({ done: false, type: 'analyse_error', error: err, blockId });
      return { ok: false, tool: 'analyse', query: raw, error: err };
    } catch (e) { const err = safeStr(e); sendEvent({ done: false, type: 'analyse_error', error: err, blockId }); return { ok: false, tool: 'analyse', query: raw, error: err }; }
  }
  return { ok: false, tool: name, error: 'Unknown tool' };
}

async function runToolsInParallel(tools, env, sendEvent, failedTools, context = {}, maxConcurrency = MAX_PARALLEL_TOOLS, batchTimeoutMs = TOOL_BATCH_TIMEOUT_MS) {
  const results = new Array(tools.length);
  let cursor = 0;
  const batchStart = Date.now();

  const runOne = async (idx) => {
    const tool = tools[idx];

    if (Date.now() - batchStart > batchTimeoutMs) {
      results[idx] = { ok: false, tool: tool.name, query: tool.content, error: 'Tool batch exceeded time budget', skipped: true, skipReason: 'batch-timeout' };
      return;
    }
    if (failedTools.includes(tool.name)) {
      results[idx] = { ok: false, tool: tool.name, query: tool.content, error: 'Already failed', skipped: true };
      return;
    }
    try { results[idx] = await executeToolAndEmit(tool, env, sendEvent, context); }
    catch (e) { results[idx] = { ok: false, tool: tool.name, query: tool.content, error: safeStr(e) }; }
  };

  const worker = async () => { while (true) { const idx = cursor++; if (idx >= tools.length) return; await runOne(idx); } };
  await Promise.all(Array.from({ length: Math.min(maxConcurrency, tools.length) }, () => worker()));
  return results;
}

function formatToolResultForLLM(result, round = 0) {
  const GROUNDING = `\nGrounding: the values above are truth. Do not substitute training data.`;
  const isSoftLimit = round >= SOFT_TOOL_ROUND_LIMIT && round < MAX_TOOL_ROUNDS - 2;
  const isFinalRound = round >= MAX_TOOL_ROUNDS - 2;
  let nextHint;
  if (isFinalRound) nextHint = `⚠ FINAL ROUND — write the final answer now.`;
  else if (isSoftLimit) nextHint = `⚠ You've used ${round + 1} tool rounds. Wrap up soon if possible.`;
  else nextHint = `Either call the next tool, or write the final answer.`;
  if (result.ok) {
    if (result.tool === 'run') {
      const out = capToolResult(String(result.result || ''), MAX_TOOL_RESULT_CHARS);
      return `${nextHint}\n\nTool execution result:\nrun →\n${out}${GROUNDING}`;
    }
    if (result.tool === 'analysing') return `${nextHint}\n\nTool execution result:\nanalysing → ${String(result.result || '')}${GROUNDING}`;
    const isReadTool = result.tool === 'analyse';
    const cap = isReadTool ? MAX_READ_CHARS : MAX_TOOL_RESULT_CHARS;
    const payload = JSON.stringify(result.data ?? result.result);
    return `${nextHint}\n\nTool execution result:\n${result.tool} → ${capToolResult(payload, cap)}${GROUNDING}`;
  }
  return `${nextHint}\n\nTool execution result:\n${result.tool} → ERROR: ${result.error}\n\nPick ONE: try a different tool, answer from training, or say the tool failed. Do NOT invent data.`;
}

// ---------------------------------------------------------------------------
// 16. FINAL-ANSWER ROUTE
// ---------------------------------------------------------------------------
async function streamFinalFromProviders(env, messages, mode = 'text') {
  const temperature = (mode === 'code' || mode === 'vision-agent') ? 0.4 : 0.2;
  const providers = await orderPipelineByQuota(env, mode);
  for (const provider of providers) {
    const keys = await getAvailableKeys(env, provider.keyEnv);
    if (!keys.length) continue;
    if (await isOnCooldown(env, provider.name, provider.model)) continue;
    for (const key of keys) {
      try {
        const res = await tryGeminiNativeStream(provider, key, messages, false, mode, env, temperature, false);
        if (res) return { response: res, provider };
      } catch (e) {
        const status = extractStatus(e);
        if (status === 429) {
          const msg = safeStr(e);
          const isRPD = /per day|daily|quotaExceeded/i.test(msg);
          if (isRPD) await keyStateMarkRPD(env, key);
          else await keyStateMarkRPM(env, key);
        } else if (status === 401 || status === 403) {
          await keyStateMarkRPD(env, key);
        } else if (isCapacityError(status, safeStr(e))) {
          await keyStateMarkRPM(env, key);
        }
      }
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// 17. ATTACHMENT NORMALIZATION
// ---------------------------------------------------------------------------
function normalizeAttachments({ attachments, imageBase64, imageBase64s }) {
  const out = [];
  let totalBytes = 0;
  if (Array.isArray(attachments) && attachments.length) {
    for (const a of attachments) {
      if (!a || typeof a !== 'object') continue;
      const dataUrl = String(a.data || '');
      const m = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
      if (!m) { log(`[normalizeAttachments] dropping malformed data URL for ${a.name || '?'}`); continue; }
      const mime = m[1];
      if (!isSupportedMime(mime)) return { error: `Unsupported file type: ${mime}` };
      const decodedBytes = Math.round((m[2].length * 3) / 4);
      if (decodedBytes > MAX_ATTACHMENT_BYTES) return { error: `File "${a.name || 'attachment'}" exceeds ${Math.round(MAX_ATTACHMENT_BYTES/1024/1024)}MB limit.` };
      totalBytes += decodedBytes;
      if (totalBytes > MAX_TOTAL_ATTACHMENT_BYTES) return { error: `Total attachment size exceeds ${Math.round(MAX_TOTAL_ATTACHMENT_BYTES/1024/1024)}MB.` };
      out.push({ mime, base64: m[2], name: a.name || '' });
      if (out.length > MAX_ATTACHMENTS_PER_MESSAGE) return { error: `Maximum ${MAX_ATTACHMENTS_PER_MESSAGE} attachments per message.` };
    }
    if (out.length) return finalizeAttachments(out);
  }
  const imageList = Array.isArray(imageBase64s) && imageBase64s.length ? imageBase64s : (imageBase64 ? [imageBase64] : []);
  for (const img of imageList) {
    const m = String(img || '').match(/^data:([^;]+);base64,(.+)$/);
    if (!m) continue;
    const mime = m[1];
    if (!isImageMime(mime)) continue;
    const decodedBytes = Math.round((m[2].length * 3) / 4);
    if (decodedBytes > MAX_ATTACHMENT_BYTES) return { error: `Image exceeds ${Math.round(MAX_ATTACHMENT_BYTES/1024/1024)}MB limit.` };
    totalBytes += decodedBytes;
    if (totalBytes > MAX_TOTAL_ATTACHMENT_BYTES) return { error: `Total attachment size exceeds ${Math.round(MAX_TOTAL_ATTACHMENT_BYTES/1024/1024)}MB.` };
    out.push({ mime, base64: m[2], name: '' });
    if (out.length > MAX_ATTACHMENTS_PER_MESSAGE) return { error: `Maximum ${MAX_ATTACHMENTS_PER_MESSAGE} attachments per message.` };
  }
  if (!out.length) return { attachments: [], hasImage: false, hasFile: false };
  return finalizeAttachments(out);
}
function finalizeAttachments(list) {
  let hasImage = false, hasFile = false;
  for (const a of list) { if (isImageMime(a.mime)) hasImage = true; else hasFile = true; }
  return { attachments: list, hasImage, hasFile };
}

function estimateRequestTokens(messages, attachments) {
  let chars = 0;
  for (const m of messages) {
    if (typeof m.content === 'string') chars += m.content.length;
    if (Array.isArray(m.attachments)) {
      for (const a of m.attachments) chars += Math.round((a.base64 || '').length * 0.75);
    }
  }
  for (const a of (attachments || [])) {
    chars += Math.round((a.base64 || '').length * 0.75);
  }
  return Math.round(chars * ESTIMATED_TOKENS_PER_CHAR);
}

// ---------------------------------------------------------------------------
// 18. EMBEDDING HELPERS
// ---------------------------------------------------------------------------
async function callGeminiEmbed(env, provider, key, parts, opts = {}) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(provider.model)}:embedContent?key=${key}`;
  const body = { content: { parts } };
  if (opts.taskType) body.taskType = opts.taskType;
  if (opts.outputDimensionality) body.outputDimensionality = opts.outputDimensionality;
  const res = await fetchWithTimeout(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, EMBED_TIMEOUT_MS, `embed-${provider.model}`, 1);
  if (!res.ok) { const errText = await res.text().catch(() => ''); throw new Error(`Gemini ${provider.model} ${res.status}: ${errText.slice(0, 200)}`); }
  const j = await res.json();
  return j?.embedding?.values || null;
}
async function callGeminiBatchEmbed(env, provider, key, requests) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(provider.model)}:batchEmbedContents?key=${key}`;
  const body = { requests };
  const res = await fetchWithTimeout(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, EMBED_TIMEOUT_MS, `batch-embed-${provider.model}`, 1);
  if (!res.ok) { const errText = await res.text().catch(() => ''); throw new Error(`Gemini batch ${provider.model} ${res.status}: ${errText.slice(0, 200)}`); }
  const j = await res.json();
  return (j?.embeddings || []).map(e => e?.values || null);
}
function cosineSimilarity(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return 0;
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

// ---------------------------------------------------------------------------
// 19. MAIN MESSAGE HANDLER
// ---------------------------------------------------------------------------
async function handleMessages(chat, mode, attachmentsOrLegacy, env, username, opts = {}) {
  if (!hasAnyKey(env, mode)) return buildErrorStream('No Google API keys configured.');

  let normalized;
  if (opts && opts.attachmentsInput) normalized = normalizeAttachments(opts.attachmentsInput);
  else normalized = normalizeAttachments({ imageBase64s: Array.isArray(attachmentsOrLegacy) ? attachmentsOrLegacy : (attachmentsOrLegacy ? [attachmentsOrLegacy] : []) });
  if (normalized.error) return buildErrorStream(normalized.error);

  const { attachments, hasImage, hasFile } = normalized;
  const isVisionInput = mode === 'vision' || mode === 'vision-agent';
  const anyAttachment = attachments.length > 0;
  const actualMode = (isVisionInput && !anyAttachment) ? 'text' : mode;
  const isVision = actualMode === 'vision' || actualMode === 'vision-agent';
  const isVisionAgent = actualMode === 'vision-agent';
  const allowTools = opts.allowTools !== false;
  const hasAttachments = attachments.length > 0;

  const today = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });

  const currentImageBlobIds = new Set();
  const lastMsg = chat.messages && chat.messages[chat.messages.length - 1];
  if (lastMsg && lastMsg.role === 'user' && lastMsg.content) {
    for (const bid of extractBlobIds(String(lastMsg.content))) {
      const blob = await blobGet(env, bid);
      if (blob && isImageMime(blob.mime)) currentImageBlobIds.add(bid);
    }
  }

  let fileIndex = '';
  try { fileIndex = await buildFileIndex(env, chat, currentImageBlobIds); } catch (e) { log('[fileIndex] failed:', safeStr(e)); }

  let initialMessages;
  if (isVision) {
    const rawLastUser = chat.messages[chat.messages.length - 1]?.content
      ?.replace(/!\[.*?\]\((?:data|blob):[^)]+\)/g, '').replace(/\[Attached:[^\]]+\]/g, '').trim()
      || (attachments.length > 1 ? 'Read the attached files and answer the user.' : 'Read the attached file and answer the user.');
    const lastUser = escapeUserToolTags(rawLastUser);
    const systemPrompt = getSystemPrompt(actualMode, today, { hasImage, hasFile, fileIndex });
    const history = buildHistoryForLLM(chat.messages.slice(0, -1)).slice(-MAX_HISTORY_MESSAGES);
    initialMessages = [{ role: 'system', content: systemPrompt }, ...history, { role: 'user', content: lastUser, attachments }];
  } else {
    const useAttachments = attachments.length > 0;
    const historySource = useAttachments ? chat.messages.slice(0, -1) : chat.messages;
    const history = buildHistoryForLLM(historySource).slice(-MAX_HISTORY_MESSAGES);
    const systemPrompt = getSystemPrompt(actualMode, today, { fileIndex });
    if (useAttachments) {
      const rawLastUser = chat.messages[chat.messages.length - 1]?.content
        ?.replace(/!\[.*?\]\((?:data|blob):[^)]+\)/g, '')
        .replace(/\[Attached:[^\]]+\]/g, '')
        .trim() || 'Please read the attached file(s) and answer my question.';
      const lastUser = escapeUserToolTags(rawLastUser);
      initialMessages = [{ role: 'system', content: systemPrompt }, ...history, { role: 'user', content: lastUser, attachments }];
    } else {
      initialMessages = [{ role: 'system', content: systemPrompt }, ...history];
    }
  }

  const estimatedTokens = estimateRequestTokens(initialMessages, attachments);
  const skipNativeUpload = estimatedTokens > FREE_TIER_TPM_LIMIT * 0.8;

  if (!(await checkRateLimit(env, username))) return buildErrorStream('Rate limit exceeded.');
  const pipeline = await orderPipelineByQuota(env, actualMode);
  if (pipeline.length === 0) return buildErrorStream('No providers configured.');

  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream({
      async start(controller) {
        let closed = false;
        const safeEnqueue = (bytes) => {
          if (closed) return false;
          try { controller.enqueue(bytes); return true; }
          catch (e) { closed = true; return false; }
        };
        const sendEvent = (data) => {
          safeEnqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
        };

        const heartbeat = setInterval(() => {
          safeEnqueue(encoder.encode(`: keepalive\n\n`));
        }, STREAM_HEARTBEAT_MS);

        const cleanup = () => {
          clearInterval(heartbeat);
          if (!closed) {
            closed = true;
            try { controller.close(); } catch {}
          }
        };

        try {
          safeEnqueue(encoder.encode(`: ready\n\n`));

          if (attachments.length > 0 && !skipNativeUpload) {
            const uploadKeys = await getAvailableKeys(env, 'GOOGLE_KEYS');
            const uploadKey = uploadKeys[0] || null;
            if (uploadKey) {
              for (let i = 0; i < attachments.length; i++) {
                const att = attachments[i];
                if (!att.base64 || !att.mime) continue;
                const blockId = genBlockId();
                const attName = att.name || `attachment-${i + 1}`;
                sendEvent({ done: false, type: 'analysing_start', query: attName, blockId, source: 'attachment', index: i + 1, total: attachments.length });
                try {
                  const res = await ensureGeminiFile(env, uploadKey, att.mime, att.base64, attName);
                  if (res && res.uri) {
                    att.fileUri = res.uri;
                    att.uploadKey = res.uploadKey;
                    log(`[auto-attach] ${res.cached ? 'cache-hit' : 'uploaded'} ${att.mime} → ${res.uri}`);
                  } else {
                    log(`[auto-attach] fell back to inline for ${att.mime}`);
                  }
                  sendEvent({ done: false, type: 'analysing_results', query: attName, blockId, source: 'attachment', data: { name: attName, mime: att.mime, size: att.size || 0 } });
                } catch (e) {
                  log('[auto-attach] error:', safeStr(e));
                  sendEvent({ done: false, type: 'analysing_error', query: attName, blockId, source: 'attachment', error: safeStr(e) });
                }
              }
            }
          }

          const messages = [...initialMessages];
          let answerSent = false;
          const failedTools = [];
          const attemptedCalls = new Set();
          let continuationCount = 0;
          const t0 = Date.now();

          const pushContinuation = (text, promptOverride = null) => {
            const prompt = promptOverride || CONTINUATION_PROMPT;
            const lastIdx = messages.length - 1;
            const prev = lastIdx >= 1 ? messages[lastIdx - 1] : null;
            const last = messages[lastIdx];
            if (prev && prev.role === 'assistant' && prev._partial &&
                last && last.role === 'user' && last.content === prompt) {
              prev.content += text;
              return;
            }
            messages.push({ role: 'assistant', content: text, _partial: true });
            messages.push({ role: 'user', content: prompt });
          };

          try {
            for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
              if (Date.now() - t0 > TURN_DEADLINE_MS) {
                log(`[turn] deadline hit at round ${round}, forcing final answer`);
                break;
              }
              if (messages.length > 1 + MAX_HISTORY_FOR_TOOLS) {
                const system = messages[0];
                const head = messages.slice(1, 3);
                const tail = messages.slice(-(MAX_HISTORY_FOR_TOOLS - head.length));
                const seen = new Set();
                const merged = [system];
                for (const m of [...head, ...tail]) {
                  const k = `${m.role}|${typeof m.content === 'string' ? m.content.slice(0, 64) : ''}`;
                  if (seen.has(k)) continue;
                  seen.add(k);
                  merged.push(m);
                }
                messages.length = 0;
                messages.push(...merged);
              }

              let providerResponse = null;
              let usedProvider = null;
              const slotCodes = {};
              const roundErrors = [];

              for (let pi = 0; pi < pipeline.length; pi++) {
                const provider = pipeline[pi];
                const slot = `M${pi + 1}`;

                if (await isOnCooldown(env, provider.name, provider.model)) {
                  slotCodes[slot] = 'C'; continue;
                }
                const keys = await getAvailableKeys(env, provider.keyEnv);
                if (!keys.length) { slotCodes[slot] = 'K'; continue; }
                let resolved = false;

                for (const key of keys) {
                  let attempt = 0;
                  let success = false;
                  while (attempt < LLM_MAX_ATTEMPTS_CAPACITY) {
                    try {
                      sendEvent({ done: false, type: 'llm_start', round, model: provider.model });
                      const startT = Date.now();
                      const resp = await tryGeminiNativeStream(provider, key, messages, isVision, actualMode, env, null, hasAttachments);
                      if (resp) {
                        providerResponse = resp; usedProvider = provider;
                        const latency = Date.now() - startT;
                        await recordSuccess(env, actualMode, provider.name, provider.model, latency);
                        slotCodes[slot] = 'OK'; resolved = true; success = true;
                        break;
                      }
                    } catch (e) {
                      const status = extractStatus(e);
                      const msg = safeStr(e).slice(0, 300);
                      const isCapacity = isCapacityError(status, msg);
                      const isTransient = isCapacity || status === 429;
                      const maxAttempts = isCapacity ? LLM_MAX_ATTEMPTS_CAPACITY : LLM_MAX_ATTEMPTS;

                      roundErrors.push(`${provider.model}: ${msg.slice(0, 120)}`);

                      if (isTransient && attempt < maxAttempts - 1) {
                        const hinted = (e && e.retryAfterMs) ? e.retryAfterMs : null;
                        const base = hinted || (Math.pow(2, attempt) * 1500);
                        const jitter = Math.floor(Math.random() * 800);
                        const backoffMs = Math.min(base + jitter, 30000);
                        await new Promise(r => setTimeout(r, backoffMs));
                        attempt++;
                        continue;
                      }

                      log(`[round ${round}] ${provider.model} failed (${status}): ${msg}`);
                      slotCodes[slot] = codeForStatus(status, msg);

                      if (status === 429) {
                        const isRPD = /per day|per_day|daily|quotaExceeded/i.test(msg);
                        if (isRPD) await keyStateMarkRPD(env, key);
                        else await keyStateMarkRPM(env, key);
                        break;
                      }
                      if (status === 404) { await recordFailure(env, provider.name, provider.model, status); break; }
                      if (status === 401 || status === 403) { await keyStateMarkRPD(env, key); break; }
                      if (isCapacity) { break; }
                      await recordFailure(env, provider.name, provider.model, status);
                      break;
                    }
                  }
                  if (success || resolved) break;
                }
                if (resolved) break;
              }

              if (!providerResponse) {
                for (let i = 0; i < pipeline.length; i++) { const s = `M${i + 1}`; if (!slotCodes[s]) slotCodes[s] = 'X'; }
                const fingerprint = formatErrorFingerprint(slotCodes);
                log(`[round ${round}] all models failed: ${fingerprint} — ${roundErrors.join(' | ')}`);
                sendEvent({ done: false, type: 'text', content: `**All Gemini models failed this round.**\n\n\`${fingerprint}\`` });
                sendEvent({ done: true, error: fingerprint, code: fingerprint });
                cleanup();
                return;
              }

              const result = await pipeStream(providerResponse, actualMode, sendEvent, env, usedProvider, { allowTools });

              if (result.error && usedProvider && !result.isExternalAbort) {
                await recordFailure(env, usedProvider.name, usedProvider.model, 500);
              }

              const hasFetchTool = result.tools.length > 0;

              if (!hasFetchTool) {
                const hasText = result.sawText;
                const hasChart = result.charts.length > 0;
                const hasAnything = hasText || hasChart;

                const isSafetyBlock =
                  result.finishReason === 'SAFETY' ||
                  result.finishReason === 'PROHIBITED_CONTENT' ||
                  result.finishReason === 'SPII';

                if (isSafetyBlock) {
                  log(`[round ${round}] safety block: ${result.finishReason}`);
                  sendEvent({
                    done: false,
                    type: 'text',
                    content: `**Response blocked by Gemini's safety filter** (\`${result.finishReason}\`). Rephrase the request, or remove any content that might have triggered it.`,
                  });
                  answerSent = true;
                  break;
                }

                const missingFinishReason =
                  hasAnything && (result.finishReason === null || result.finishReason === undefined);
                const otherFinishReason =
                  hasAnything && result.finishReason === 'OTHER';
                const recitationStop =
                  hasAnything && result.finishReason === 'RECITATION';
                const truncatedStop =
                  isTruncatedStop(result);

                if ((missingFinishReason || otherFinishReason || recitationStop || truncatedStop)
                    && continuationCount < MAX_CONTINUATIONS) {
                  continuationCount++;
                  const reason = missingFinishReason ? 'no-finish-reason'
                               : otherFinishReason  ? 'finish-reason-OTHER'
                               : recitationStop     ? 'recitation'
                               :                     'truncated-STOP';
                  log(`[round ${round}] silent continuation (${reason}) after ${result.text.length} chars — ${continuationCount}/${MAX_CONTINUATIONS}`);
                  pushContinuation(
                    result.raw || result.text,
                    recitationStop ? RECITATION_CONTINUATION_PROMPT : null
                  );
                  continue;
                }

                if (result.isNetworkError && continuationCount < MAX_CONTINUATIONS) {
                  continuationCount++;
                  log(`[round ${round}] silent continuation (network) — ${continuationCount}/${MAX_CONTINUATIONS}`);
                  pushContinuation(result.raw || result.text);
                  continue;
                }

                if (!hasAnything) {
                  const nudgeCount = messages.filter(m => m.role === 'user' && typeof m.content === 'string' && m.content.startsWith('Please write your final answer')).length;
                  if (nudgeCount < 2) {
                    messages.push({ role: 'assistant', content: '' });
                    messages.push({ role: 'user', content: 'Please write your final answer now.' });
                    continue;
                  }
                  break;
                }

                if (result.finishReason === 'MAX_TOKENS' && continuationCount < MAX_CONTINUATIONS) {
                  continuationCount++;
                  log(`[round ${round}] silent continuation (MAX_TOKENS) — ${continuationCount}/${MAX_CONTINUATIONS}`);
                  pushContinuation(result.raw || result.text);
                  continue;
                }

                answerSent = true;
                break;
              }

              const filteredTools = result.tools;
              const toolsToRun = filteredTools.slice(0, MAX_PARALLEL_TOOLS);
              if (toolsToRun.length === 0) { answerSent = true; break; }
              if (Date.now() - t0 > TURN_DEADLINE_MS * 0.75) {
                log(`[turn] near deadline at round ${round}, skipping tools`);
                break;
              }

              const blocked = new Set();
              for (const t of toolsToRun) if (failedTools.includes(t.name)) blocked.add(t);

              const runnable = toolsToRun.filter(t => {
                if (blocked.has(t)) return false;
                const fp = `${t.name}::${t.content}`;
                if (attemptedCalls.has(fp)) return false;
                attemptedCalls.add(fp);
                return true;
              });

              let toolResults = [];
              if (runnable.length > 0) {
                const toolContext = { chat, currentAttachments: attachments };
                toolResults = await runToolsInParallel(runnable, env, sendEvent, failedTools, toolContext, MAX_PARALLEL_TOOLS, TOOL_BATCH_TIMEOUT_MS);
              }

              let runnableIdx = 0;
              const ordered = toolsToRun.map(t => {
                if (blocked.has(t)) return { ok: false, tool: t.name, query: t.content, error: 'Already failed', skipped: true, skipReason: 'already-failed' };
                if (!runnable.includes(t)) return { ok: false, tool: t.name, query: t.content, error: 'Duplicate call', skipped: true, skipReason: 'duplicate' };
                return toolResults[runnableIdx++];
              });

              for (let i = 0; i < toolsToRun.length; i++) {
                const tool = toolsToRun[i];
                const r = ordered[i];
                messages.push({ role: 'assistant', content: `<${tool.name}>${tool.content}</${tool.name}>` });
                if (r.skipped) {
                  const reason = r.skipReason === 'duplicate'
                    ? `You already ran this exact ${tool.name} call. Do NOT repeat it.`
                    : r.skipReason === 'batch-timeout'
                    ? `The ${tool.name} batch exceeded its time budget. Do not retry the same call in this turn.`
                    : `You already tried the ${tool.name} tool and it FAILED. Do not use it again.`;
                  messages.push({ role: 'user', content: reason });
                } else {
                  if (!r.ok) failedTools.push(r.tool);
                  if (r.nativeAttachment) {
                    const projected = estimateRequestTokens(messages, []) +
                                     Math.round((r.nativeAttachment.base64 || '').length * 0.75 * ESTIMATED_TOKENS_PER_CHAR);
                    if (projected > FREE_TIER_TPM_LIMIT * 0.9) {
                      log(`[tpm] native attachment projected ${projected} — falling back to text notice`);
                      messages.push({ role: 'user', content: `Tool execution result:\n${r.tool} → ${r.result}\n\n(file contents omitted — token budget).` });
                    } else {
                      messages.push({
                        role: 'user',
                        content: `Tool execution result:\n${r.tool} → ${r.result}\n\nGrounding rule: contents of the file attached below are the source of truth.`,
                        attachments: [{
                          mime: r.nativeAttachment.mime,
                          base64: r.nativeAttachment.base64,
                          name: r.nativeAttachment.name,
                          fileUri: r.nativeAttachment.fileUri || null,
                          uploadKey: r.nativeAttachment.uploadKey || null,
                        }],
                      });
                    }
                  } else {
                    messages.push({ role: 'user', content: formatToolResultForLLM(r, round) });
                  }
                }
              }
            }

            if (!answerSent) {
              const convoOnly = messages.filter(m => m.role !== 'system').map(m => {
                const out = { role: m.role, content: m.content };
                if (Array.isArray(m.attachments) && m.attachments.length) out.attachments = m.attachments;
                return out;
              });
              const forceMessages = [
                messages[0],
                ...convoOnly,
                { role: 'user', content: `## FINAL ROUND\nWrite the detailed final answer now. No tool tag (except a leading <chart>). If tools failed and you lack live data, say so — never invent numbers.` },
              ];
              let sent = false;
              try {
                const forceResult = await streamFinalFromProviders(env, forceMessages, 'text');
                if (forceResult) { const r = await pipeStream(forceResult.response, 'text', sendEvent, env, null, { allowTools: false }); if (r.sawText) sent = true; }
              } catch (e) { log('forced final failed:', safeStr(e)); }

              if (!sent && isVision && !isVisionAgent) {
                try {
                  const simpleSystem = `You are ${ASSISTANT_NAME}, created by ${ASSISTANT_CREATOR}. Describe the attached file(s) in 3–5 detailed sentences. No tools.`;
                  const simpleMessages = [{ role: 'system', content: simpleSystem }, ...initialMessages.filter(m => m.role !== 'system')];
                  const retryResult = await streamFinalFromProviders(env, simpleMessages, 'text');
                  if (retryResult) { const r2 = await pipeStream(retryResult.response, 'text', sendEvent, env, null, { allowTools: false }); if (r2.sawText) sent = true; }
                } catch (e) { log('vision fallback failed:', safeStr(e)); }
              }

              if (!sent) sendEvent({ done: false, type: 'text', content: `I wasn't able to complete that request. Please try again.` });
            }

            log(`[turn done] total ${Date.now() - t0}ms`);
            sendEvent({ done: true, completed: true });
          } catch (e) {
            const raw = safeStr(e);
            log('handleMessages inner error:', raw);
            sendEvent({
              done: false,
              type: 'text',
              content: `**Something went wrong on the server.** Please try again. If it keeps happening, check the Cloudflare Worker logs.`
            });
            sendEvent({ done: true, error: 'internal-error' });
          }
        } catch (e) {
          const raw = safeStr(e);
          log('handleMessages outer error:', raw);
          sendEvent({
            done: false,
            type: 'text',
            content: `**Something went wrong on the server.** Please try again. If it keeps happening, check the Cloudflare Worker logs.`
          });
          sendEvent({ done: true, error: 'internal-error' });
        } finally {
          cleanup();
        }
      },
    }),
    { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', ...corsHeaders() } }
  );
}

function buildErrorStream(msg) {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream({
      start(c) {
        try { c.enqueue(encoder.encode(`: ready\n\n`)); } catch {}
        c.enqueue(encoder.encode(`data: ${JSON.stringify({ done: false, type: 'text', content: `**Error:** ${msg}` })}\n\n`));
        c.enqueue(encoder.encode(`data: ${JSON.stringify({ done: true, error: msg })}\n\n`));
        c.close();
      },
    }),
    { headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', ...corsHeaders() } }
  );
}

// ---------------------------------------------------------------------------
// 20. FETCH HANDLER
// ---------------------------------------------------------------------------
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\/api/, '') || '/';
    const method = request.method;
    if (method === 'OPTIONS') return new Response(null, { headers: corsHeaders() });
    await ensureDatabase(env);
    if (Math.random() < 0.05) cleanupIdempotencyKeys(env).catch(() => {});
    try {
      if (path === '/' || path === '/health') return json({ status: 'ok', version: WORKER_VERSION, assistant: ASSISTANT_NAME, creator: ASSISTANT_CREATOR });

      if (path === '/auth/signup' && method === 'POST') {
        const { username, password } = await request.json();
        if (!username || !password) return errorResponse('Missing credentials');
        if (await env.DB.prepare('SELECT username FROM users WHERE username=?').bind(username).first()) return errorResponse('Username taken', 409);
        await env.DB.prepare('INSERT INTO users (username, password) VALUES (?, ?)').bind(username, await hashPassword(password)).run();
        const token = crypto.randomUUID();
        await env.DB.prepare('INSERT INTO tokens (token, username, created_at) VALUES (?, ?, ?)').bind(token, username, Math.floor(Date.now() / 1000)).run();
        return json({ token, username });
      }
      if (path === '/auth/login' && method === 'POST') {
        const { username, password } = await request.json();
        if (!username || !password) return errorResponse('Missing credentials');
        const user = await env.DB.prepare('SELECT password FROM users WHERE username=?').bind(username).first();
        if (!user || !(await verifyPassword(password, user.password))) return errorResponse('Invalid credentials', 401);
        const token = crypto.randomUUID();
        await env.DB.prepare('INSERT INTO tokens (token, username, created_at) VALUES (?, ?, ?)').bind(token, username, Math.floor(Date.now() / 1000)).run();
        return json({ token, username });
      }
      const username = await requireAuth(request, env);
      if (!username) return errorResponse('Authentication required', 401);

      if (path === '/debug' && method === 'GET') {
        return json({
          version: WORKER_VERSION, provider: 'Google Gemini only',
          modes: ['text', 'code', 'vision', 'vision-agent'],
          chat: { text: PROVIDERS.text.map(p => p.model), code: PROVIDERS.code.map(p => p.model), vision: PROVIDERS.vision.map(p => p.model) },
          embeddings: EMBEDDING_PROVIDERS.map(p => ({ model: p.model, dim: p.dim })),
          cooldownSeconds: FAILURE_COOLDOWN_SECONDS, errorCodeLegend: ERROR_CODES, you: username,
        });
      }
      if (path === '/debug/keys' && method === 'GET') {
        const keys = getAllKeys(env, 'GOOGLE_KEYS');
        const states = [];
        for (const k of keys) {
          const st = await keyStateGet(env, k);
          states.push({ hash: hashKey(k).slice(0, 6), state: st ? st.reason : 'available', ageSeconds: st ? Math.round((Date.now() - st.ts) / 1000) : null });
        }
        return json({ totalKeys: keys.length, availableKeys: states.filter(s => s.state === 'available').length, keys: states, resetInSeconds: secondsUntilMidnightPacific() });
      }
      if (path === '/debug-run' && method === 'GET') { const q = url.searchParams.get('q') || 'sqrt(144) + 2**10'; return json({ code: q, output: evaluateJSSandboxed(q) }); }

      if (path === '/embed' && method === 'POST') {
        const body = await request.json();
        const { texts, text, image, video, audio, pdf, taskType, outputDimensionality, model: modelOverride, chatId, blobId, store } = body || {};
        const isBatch = Array.isArray(texts) && texts.length > 0;
        if (!isBatch && !text && !image && !video && !audio && !pdf) return errorResponse('Provide text, texts, image, video, audio, or pdf', 400);
        let provider = EMBEDDING_PROVIDERS[0];
        if (modelOverride) { const found = EMBEDDING_PROVIDERS.find(p => p.model === modelOverride); if (found) provider = found; }
        if (isBatch) {
          if (texts.length > MAX_EMBED_TEXTS_PER_CALL) return errorResponse(`Batch too large. Max ${MAX_EMBED_TEXTS_PER_CALL}.`, 400);
          const truncated = texts.map(t => String(t || '').slice(0, MAX_EMBED_CHARS));
          const requests = truncated.map(t => {
            const req = { model: `models/${provider.model}`, content: { parts: [{ text: t }] } };
            if (taskType) req.taskType = taskType;
            if (outputDimensionality) req.outputDimensionality = outputDimensionality;
            return req;
          });
          const keys = await getAvailableKeys(env, provider.keyEnv);
          if (!keys.length) return errorResponse('No API key available', 503);
          const key = keys[0];
          try {
            const vectors = await callGeminiBatchEmbed(env, provider, key, requests);
            if (store === true) {
              const now = Date.now();
              for (let i = 0; i < vectors.length; i++) {
                if (!vectors[i]) continue;
                const id = crypto.randomUUID();
                try { await env.DB.prepare('INSERT INTO embeddings (id, username, chat_id, blob_id, source, mime, model, dim, vector, meta, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)').bind(id, username, chatId || null, blobId || null, 'text', 'text/plain', provider.model, vectors[i].length, JSON.stringify(vectors[i]), JSON.stringify({ preview: truncated[i].slice(0, 120) }), now).run(); } catch (e) {}
              }
            }
            return json({ model: provider.model, dim: vectors[0]?.length || 0, count: vectors.length, embeddings: vectors });
          } catch (e) { return errorResponse(`Embedding failed: ${safeStr(e)}`, 502); }
        }
        let parts; let source = 'text'; let mimeForStore = 'text/plain';
        if (text) { parts = [{ text: String(text).slice(0, MAX_EMBED_CHARS) }]; source = 'text'; }
        else {
          const binInput = image || video || audio || pdf;
          const m = String(binInput).match(/^data:([^;]+);base64,(.+)$/);
          if (!m) return errorResponse('Binary input must be a data URL', 400);
          parts = [{ inlineData: { mimeType: m[1], data: m[2] } }];
          mimeForStore = m[1];
          source = m[1].startsWith('image/') ? 'image' : m[1].startsWith('video/') ? 'video' : m[1].startsWith('audio/') ? 'audio' : m[1] === 'application/pdf' ? 'pdf' : 'binary';
        }
        const keys = await getAvailableKeys(env, provider.keyEnv);
        if (!keys.length) return errorResponse('No API key available', 503);
        const key = keys[0];
        try {
          const vec = await callGeminiEmbed(env, provider, key, parts, { taskType, outputDimensionality });
          if (!vec) return errorResponse('Empty embedding returned', 502);
          if (store === true) {
            const id = crypto.randomUUID();
            try { await env.DB.prepare('INSERT INTO embeddings (id, username, chat_id, blob_id, source, mime, model, dim, vector, meta, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)').bind(id, username, chatId || null, blobId || null, source, mimeForStore, provider.model, vec.length, JSON.stringify(vec), null, Date.now()).run(); } catch (e) {}
          }
          return json({ model: provider.model, dim: vec.length, source, embedding: vec });
        } catch (e) { return errorResponse(`Embedding failed: ${safeStr(e)}`, 502); }
      }

      if (path === '/auth/delete-account' && method === 'POST') {
        try {
          const rows = await env.DB.prepare('SELECT content FROM messages WHERE chat_id IN (SELECT id FROM chats WHERE username=?)').bind(username).all();
          const ids = new Set();
          for (const r of (rows.results || [])) for (const bid of extractBlobIds(r.content)) ids.add(bid);
          for (const bid of ids) await blobDelete(env, bid);
        } catch (e) {}
        await env.DB.prepare('DELETE FROM embeddings WHERE username=?').bind(username).run();
        await env.DB.prepare('DELETE FROM blobs WHERE username=?').bind(username).run();
        await env.DB.prepare('DELETE FROM tokens WHERE username=?').bind(username).run();
        await env.DB.prepare('DELETE FROM messages WHERE chat_id IN (SELECT id FROM chats WHERE username=?)').bind(username).run();
        await env.DB.prepare('DELETE FROM chats WHERE username=?').bind(username).run();
        await env.DB.prepare('DELETE FROM users WHERE username=?').bind(username).run();
        return json({ success: true });
      }

      if (path === '/chats' && method === 'GET') {
        const { results } = await env.DB.prepare(`SELECT c.id, c.title, c.mode, c.created_at, c.updated_at, (SELECT COUNT(*) FROM messages WHERE chat_id = c.id) AS messageCount FROM chats c WHERE c.username = ? ORDER BY COALESCE(c.updated_at, c.created_at) DESC`).bind(username).all();
        return json(results);
      }
      if (path === '/chats' && method === 'POST') {
        const id = crypto.randomUUID(); const now = Date.now();
        let mode = 'quick';
        try { const body = await request.json(); if (body && (body.mode === 'quick' || body.mode === 'expert')) mode = body.mode; } catch (e) {}
        await env.DB.prepare('INSERT INTO chats (id, username, title, created_at, updated_at, mode) VALUES (?, ?, ?, ?, ?, ?)').bind(id, username, 'New Chat', now, now, mode).run();
        return json({ id, title: 'New Chat', mode, createdAt: now, messages: [] }, 201);
      }

      const chatMatch = path.match(/^\/chats\/([a-zA-Z0-9-]+)$/);
      if (chatMatch) {
        const chatId = chatMatch[1];
        const chat = await env.DB.prepare('SELECT id, title, mode FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
        if (!chat) return errorResponse('Chat not found', 404);
        if (method === 'GET') {
          const messages = await env.DB.prepare('SELECT id, role, content, mode, timestamp, blocks FROM messages WHERE chat_id = ? ORDER BY timestamp ASC').bind(chatId).all();
          const enriched = messages.results.map((m) => {
            const base = { ...m, content: String(m.content) };
            if (m.role === 'assistant' && m.blocks) { try { base.blocks = JSON.parse(m.blocks); } catch (e) {} }
            return base;
          });
          return json({ id: chat.id, title: chat.title, mode: chat.mode || 'quick', messages: enriched });
        }
        if (method === 'DELETE') {
          try {
            const rows = await env.DB.prepare('SELECT content FROM messages WHERE chat_id = ?').bind(chatId).all();
            const ids = new Set();
            for (const r of (rows.results || [])) for (const bid of extractBlobIds(r.content)) ids.add(bid);
            for (const bid of ids) await blobDelete(env, bid);
          } catch (e) {}
          try { await env.DB.prepare('DELETE FROM embeddings WHERE chat_id = ? AND username = ?').bind(chatId, username).run(); } catch (e) {}
          await env.DB.prepare('DELETE FROM messages WHERE chat_id = ?').bind(chatId).run();
          await env.DB.prepare('DELETE FROM chats WHERE id = ?').bind(chatId).run();
          return json({ success: true });
        }
        if (method === 'PATCH') {
          const body = await request.json();
          const sets = [];
          const binds = [];
          if (typeof body.title === 'string' && body.title.trim()) { sets.push('title = ?'); binds.push(body.title.trim()); }
          if (body.mode === 'quick' || body.mode === 'expert') { sets.push('mode = ?'); binds.push(body.mode); }
          if (sets.length) {
            sets.push('updated_at = ?');
            binds.push(Date.now());
            binds.push(chatId);
            binds.push(username);
            await env.DB.prepare(`UPDATE chats SET ${sets.join(', ')} WHERE id = ? AND username = ?`).bind(...binds).run();
          }
          const fresh = await env.DB.prepare('SELECT id, title, mode FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
          return json({ id: fresh.id, title: fresh.title, mode: fresh.mode || 'quick' });
        }
      }

      const msgDeleteMatch = path.match(/^\/chats\/([a-zA-Z0-9-]+)\/messages\/([a-zA-Z0-9-]+)$/);
      if (msgDeleteMatch && method === 'DELETE') {
        const chatId = msgDeleteMatch[1]; const messageId = msgDeleteMatch[2];
        const chat = await env.DB.prepare('SELECT id FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
        if (!chat) return errorResponse('Chat not found', 404);
        try {
          const row = await env.DB.prepare('SELECT content FROM messages WHERE id = ? AND chat_id = ?').bind(messageId, chatId).first();
          if (row && row.content) for (const bid of extractBlobIds(row.content)) await blobDelete(env, bid);
        } catch (e) {}
        const result = await env.DB.prepare('DELETE FROM messages WHERE id = ? AND chat_id = ?').bind(messageId, chatId).run();
        return json({ success: true, deleted: result.meta?.changes || 0 });
      }

      const completeMatch = path.match(/^\/chats\/([a-zA-Z0-9-]+)\/complete$/);
      if (completeMatch && method === 'POST') {
        const chatId = completeMatch[1];
        const chat = await env.DB.prepare('SELECT id FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
        if (!chat) return errorResponse('Chat not found', 404);
        const body = await request.json();
        const { content, client_message_id, message_id, mode, blocks } = body || {};
        if (!content || typeof content !== 'string') return errorResponse('Missing content', 400);
        const blocksJson = (Array.isArray(blocks) && blocks.length) ? JSON.stringify(blocks) : null;
        let messageId;
        if (message_id) {
          const existing = await env.DB.prepare('SELECT id FROM messages WHERE id = ? AND chat_id = ?').bind(message_id, chatId).first();
          if (!existing) return errorResponse('Message not found', 404);
          await env.DB.prepare('UPDATE messages SET content = ?, mode = ?, blocks = ? WHERE id = ?').bind(content, mode || 'text', blocksJson, message_id).run();
          messageId = message_id;
        } else {
          messageId = crypto.randomUUID();
          await env.DB.prepare('INSERT INTO messages (id, chat_id, role, content, mode, timestamp, blocks) VALUES (?, ?, ?, ?, ?, ?, ?)').bind(messageId, chatId, 'assistant', content, mode || 'text', Date.now(), blocksJson).run();
        }
        await env.DB.prepare('UPDATE chats SET updated_at = ? WHERE id = ?').bind(Date.now(), chatId).run();
        if (client_message_id) await saveIdempotency(env, `complete:${chatId}:${client_message_id}`, messageId);
        return json({ success: true, id: messageId });
      }

      const regenMatch = path.match(/^\/chats\/([a-zA-Z0-9-]+)\/regenerate$/);
      if (regenMatch && method === 'POST') {
        const chatId = regenMatch[1];
        const body = await request.json();
        const { mode = 'text', websearch = true } = body || {};
        const chat = await env.DB.prepare('SELECT id, title FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
        if (!chat) return errorResponse('Chat not found', 404);
        const count = await env.DB.prepare('SELECT COUNT(*) as c FROM messages WHERE chat_id = ? AND role = ?').bind(chatId, 'user').first();
        if ((count?.c || 0) >= MAX_MSG) return errorResponse('Chat limit reached', 400);
        const allMessages = await env.DB.prepare('SELECT id, role, content, mode, timestamp FROM messages WHERE chat_id = ? ORDER BY timestamp ASC').bind(chatId).all();
        const msgs = allMessages.results || [];
        let lastUserIdx = -1;
        for (let i = msgs.length - 1; i >= 0; i--) { if (msgs[i].role === 'user') { lastUserIdx = i; break; } }
        const trimmed = lastUserIdx >= 0 ? msgs.slice(0, lastUserIdx + 1) : msgs;
        const attachmentsInput = { attachments: [] };
        if (lastUserIdx >= 0) {
          const content = String(msgs[lastUserIdx].content);
          const seen = new Set();
          const blobIds = extractBlobIds(content);
          for (const bid of blobIds) {
            if (seen.has(bid)) continue;
            seen.add(bid);
            const blob = await blobGet(env, bid);
            if (blob && blob.data && (!blob.username || blob.username === username)) attachmentsInput.attachments.push({ name: blob.name || '', mime: blob.mime || 'application/octet-stream', data: blob.data });
          }
        }
        return handleMessages({ id: chatId, title: chat.title, messages: trimmed }, mode, null, env, username, { allowTools: websearch, ...(attachmentsInput.attachments.length ? { attachmentsInput } : {}) });
      }

      if (path === '/blobs' && method === 'POST') {
        const body = await request.json();
        const { name = '', mime = '', data = '' } = body || {};
        if (!data || typeof data !== 'string') return errorResponse('Missing attachment data', 400);
        const m = data.match(/^data:([^;]+);base64,(.+)$/);
        if (!m) return errorResponse('Malformed attachment', 400);
        const realMime = m[1] || mime;
        if (!isSupportedMime(realMime)) return errorResponse(`Unsupported file type: ${realMime}`, 400);
        const decodedBytes = Math.round((m[2].length * 3) / 4);
        if (decodedBytes > MAX_ATTACHMENT_BYTES) return errorResponse(`File exceeds ${Math.round(MAX_ATTACHMENT_BYTES/1024/1024)}MB limit.`, 413);
        const blobBucket = `blobrl:${username}`;
        const now = Math.floor(Date.now() / 1000);
        try {
          await env.DB.prepare('DELETE FROM rate_limits WHERE user_id = ? AND timestamp < ?').bind(blobBucket, now - 60).run();
          const c = await env.DB.prepare('SELECT COUNT(*) as cnt FROM rate_limits WHERE user_id = ? AND timestamp >= ?').bind(blobBucket, now - 60).first();
          if ((c?.cnt || 0) >= 60) return errorResponse('Too many uploads — slow down a moment.', 429);
          await env.DB.prepare('INSERT INTO rate_limits (user_id, timestamp) VALUES (?, ?)').bind(blobBucket, now).run();
        } catch (e) {}
        const blobId = crypto.randomUUID();
        const put = await blobPut(env, blobId, { username, mime: realMime, name, size: decodedBytes, data });
        if (put.error) return errorResponse(`Storage failed: ${put.error}`, 500);
        try { await getBlobPreview(env, { id: blobId, name, mime: realMime, size: decodedBytes, data }); } catch (e) {}
        return json({ id: blobId, name, mime: realMime, size: decodedBytes, where: put.where }, 201);
      }

      const blobMatch = path.match(/^\/blobs\/([a-zA-Z0-9-]+)$/);
      if (blobMatch && method === 'GET') {
        const blobId = blobMatch[1];
        const blob = await blobGet(env, blobId);
        if (!blob || !blob.data) return errorResponse('Blob not found', 404);
        if (blob.username && blob.username !== username) return errorResponse('Blob not found', 404);
        const m = String(blob.data).match(/^data:([^;]+);base64,(.+)$/);
        if (!m) return errorResponse('Malformed blob', 500);
        let bytes;
        try { const bin = atob(m[2]); bytes = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i); } catch (e) { return errorResponse('Corrupt blob', 500); }
        return new Response(bytes, { status: 200, headers: { 'Content-Type': m[1], 'Content-Length': String(bytes.length), 'Cache-Control': 'private, max-age=31536000, immutable', 'Access-Control-Allow-Origin': '*' } });
      }

      const msgMatch = path.match(/^\/chats\/([a-zA-Z0-9-]+)\/messages$/);
      if (msgMatch && method === 'POST') {
        const chatId = msgMatch[1];
        const body = await request.json();
        const { content = '', attachments, attachmentIds, imageBase64, imageBase64s, mode = 'text', websearch = true } = body || {};
        if (!['text', 'code', 'vision', 'vision-agent'].includes(mode)) return errorResponse('Invalid mode', 400);

        let preparedAttachments = [];
        const storeMarkers = [];
        const preUploadedIds = Array.isArray(attachmentIds) ? attachmentIds : [];
        const addedBlobIds = new Set();

        const ingestAttachment = async (a) => {
          const dataUrl = String(a.data || '');
          const m = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
          if (!m) return;
          const mime = m[1];
          if (!isSupportedMime(mime)) return;
          const decodedBytes = Math.round((m[2].length * 3) / 4);
          preparedAttachments.push({ mime, data: dataUrl, name: a.name || '' });
          const blobId = crypto.randomUUID();
          const put = await blobPut(env, blobId, { username, mime, name: a.name || '', size: decodedBytes, data: dataUrl });
          storeMarkers.push({ name: a.name || '', mime, isImg: isImageMime(mime), blobId: put && put.id ? put.id : null, dataUrl: put && put.id ? null : dataUrl });
        };

        if (preUploadedIds.length) {
          for (const blobId of preUploadedIds.slice(0, MAX_ATTACHMENTS_PER_MESSAGE)) {
            if (typeof blobId !== 'string') continue;
            const blob = await blobGet(env, blobId);
            if (!blob || !blob.data) continue;
            if (blob.username && blob.username !== username) continue;
            preparedAttachments.push({ mime: blob.mime, data: blob.data, name: blob.name || '' });
            storeMarkers.push({ name: blob.name || '', mime: blob.mime, isImg: isImageMime(blob.mime), blobId, dataUrl: null });
            addedBlobIds.add(blobId);
          }
        }
        if (Array.isArray(attachments) && attachments.length) {
          for (const a of attachments) {
            if (!a || typeof a !== 'object') continue;
            if (a.blobId && addedBlobIds.has(a.blobId)) continue;
            const dataUrl = String(a.data || '');
            const m = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
            if (!m) continue;
            const mime = m[1];
            if (!isSupportedMime(mime)) return errorResponse(`Unsupported file type: ${mime}`, 400);
            await ingestAttachment(a);
          }
        }
        if (!preUploadedIds.length && !(Array.isArray(attachments) && attachments.length)) {
          const legacyImages = Array.isArray(imageBase64s) && imageBase64s.length ? imageBase64s : (imageBase64 ? [imageBase64] : []);
          for (const img of legacyImages) { if (typeof img !== 'string') continue; await ingestAttachment({ data: img, name: '' }); }
        }

        if (preparedAttachments.length > MAX_ATTACHMENTS_PER_MESSAGE) return errorResponse(`Too many attachments. Maximum ${MAX_ATTACHMENTS_PER_MESSAGE}.`, 400);

        const chat = await env.DB.prepare('SELECT id, title FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
        if (!chat) return errorResponse('Chat not found', 404);
        const count = await env.DB.prepare('SELECT COUNT(*) as c FROM messages WHERE chat_id = ? AND role = ?').bind(chatId, 'user').first();
        if ((count?.c || 0) >= MAX_MSG) return errorResponse('Chat limit reached', 400);

        const messageId = crypto.randomUUID();
        const imgLines = storeMarkers.filter(s => s.isImg).map(s => `![uploaded](${s.blobId ? 'blob:' + s.blobId : s.dataUrl})`).join('\n');
        const fileLines = storeMarkers.filter(s => !s.isImg).map(s => {
          if (s.blobId) return `[Attached: ${s.name || 'file'} (${s.mime}) blob:${s.blobId}]`;
          if (s.dataUrl) return `[Attached: ${s.name || 'file'} (${s.mime}) inline:${s.dataUrl}]`;
          return `[Attached: ${s.name || 'file'} (${s.mime})]`;
        }).join('\n');
        const userContent = [imgLines, fileLines, content].filter(Boolean).join('\n');

        await env.DB.batch([
          env.DB.prepare('INSERT INTO messages (id, chat_id, role, content, mode, timestamp, blocks) VALUES (?, ?, ?, ?, ?, ?, ?)').bind(messageId, chatId, 'user', userContent, mode, Date.now(), null),
          env.DB.prepare('UPDATE chats SET updated_at = ? WHERE id = ?').bind(Date.now(), chatId),
        ]);

        const allMessages = await env.DB.prepare('SELECT role, content, mode FROM messages WHERE chat_id = ? ORDER BY timestamp ASC').bind(chatId).all();
        const attachmentsInput = { attachments: preparedAttachments.map(a => ({ name: a.name, data: a.data })) };
        return handleMessages({ id: chatId, title: chat.title, messages: allMessages.results }, mode, null, env, username, { allowTools: websearch, attachmentsInput });
      }

      const titleMatch = path.match(/^\/chats\/([a-zA-Z0-9-]+)\/generate-title$/);
      if (titleMatch && method === 'POST') {
        const chatId = titleMatch[1];
        const chat = await env.DB.prepare('SELECT id, title FROM chats WHERE id = ? AND username = ?').bind(chatId, username).first();
        if (!chat) return errorResponse('Chat not found', 404);
        let userContent = '', assistantContent = '';
        try { const body = await request.json(); userContent = body?.userContent || ''; assistantContent = body?.assistantContent || ''; } catch {}
        if (!userContent && !assistantContent) return json({ title: chat.title });
        const aiTitle = await generateAITitle(env, userContent, assistantContent);
        if (!aiTitle) return json({ title: chat.title });
        await env.DB.prepare('UPDATE chats SET title = ?, updated_at = ? WHERE id = ?').bind(aiTitle, Date.now(), chatId).run();
        return json({ title: aiTitle, generated: true });
      }

      return errorResponse('Not found', 404);
    } catch (e) {
      console.error(e);
      if (path.includes('/messages') && !path.includes('/generate-title')) return buildErrorStream(`Internal error: ${safeStr(e)}`);
      return errorResponse(`Internal server error: ${safeStr(e)}`, 500);
    }
  },
};
