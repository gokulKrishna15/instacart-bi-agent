"""
BI Agent — Instacart Market Basket Analysis (IMPROVED v2)
Built for Hireathon 2026 | i2e Consulting AI Labs

DEPLOYMENT MODES:
  🖥  LOCAL:             set GROQ_API_KEY in .env, place CSVs in ./data/
  ☁️  STREAMLIT CLOUD:  set secrets in dashboard, data auto-downloaded from Kaggle

IMPROVEMENTS over v1:
  ✅ FIX 1:  Exact numbers shown (not just rounded)
  ✅ FIX 2:  No artificial DataFrame index in table output
  ✅ FIX 3:  Removed hidden product_stats dependency — always raw SQL from fact tables
  ✅ FIX 4:  Auto HAVING COUNT(*) >= 500 for ratio/rate/percentage queries
  ✅ FIX 5:  Never AVG(pre-aggregated_col) — always compute from raw reordered flag
  ✅ FIX 6:  Scatter plot auto-selected for correlation / two-continuous-variable queries
  ✅ FIX 7:  ID-based joins (not string-based)
  ✅ FIX 8:  Raw data table always shown alongside chart
  ✅ FIX 9:  No LIMIT on correlation queries (scatter needs all points)
  ✅ FIX 10: Explainability layer — always show step-by-step SQL reasoning
  ✅ FIX 11: Auto Kaggle download on Streamlit Cloud — no manual CSV upload needed
  ✅ FIX 12: Works locally with ./data/ folder OR auto-downloads from Kaggle
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import re
import time
from pathlib import Path
from groq import Groq
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────
# ENVIRONMENT DETECTION
# ─────────────────────────────────────────────────────
load_dotenv()  # loads .env locally — no-op on Streamlit Cloud

def get_secret(key: str) -> str:
    """
    Get secret from Streamlit secrets (cloud) or environment variable (local).
    Streamlit secrets take priority so cloud always works even if .env exists.
    """
    try:
        val = st.secrets.get(key)
        if val:
            return val
    except Exception:
        pass
    return os.getenv(key, "")

def is_cloud() -> bool:
    """Detect if running on Streamlit Cloud (no local ./data folder)."""
    return not Path("./data").exists()

# ─────────────────────────────────────────────────────
# SETUP — API clients
# ─────────────────────────────────────────────────────
GROQ_API_KEY = get_secret("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY)

st.set_page_config(
    page_title="Instacart BI Agent v2",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────
# STYLING
# ─────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #0d0f12; color: #e2e8f0; }

.user-msg {
    background: #1a1f2e; border: 1px solid #2d3748;
    border-left: 3px solid #6366f1; border-radius: 8px;
    padding: 12px 16px; margin: 8px 0; font-size: 0.92rem; color: #e2e8f0;
}
.agent-msg {
    background: #111827; border: 1px solid #1f2937;
    border-left: 3px solid #10b981; border-radius: 8px;
    padding: 12px 16px; margin: 8px 0; font-size: 0.92rem; color: #d1fae5;
}
.sql-block {
    background: #0a0c10; border: 1px solid #374151; border-radius: 6px;
    padding: 12px 16px; font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem; color: #a5f3fc; overflow-x: auto; margin: 8px 0;
}
.insight-card {
    background: linear-gradient(135deg, #064e3b 0%, #065f46 100%);
    border: 1px solid #059669; border-radius: 8px;
    padding: 12px 16px; color: #d1fae5; font-size: 0.88rem; margin: 8px 0;
}
.thought-card {
    background: #1e1b4b; border: 1px solid #4338ca; border-radius: 6px;
    padding: 8px 14px; color: #c7d2fe; font-size: 0.82rem;
    margin: 6px 0; font-style: italic;
}
.warn-card {
    background: #451a03; border: 1px solid #b45309; border-radius: 6px;
    padding: 8px 14px; color: #fcd34d; font-size: 0.82rem; margin: 6px 0;
}
.stat-pill {
    display: inline-block; background: #1f2937; border: 1px solid #374151;
    border-radius: 20px; padding: 4px 12px; font-size: 0.75rem; color: #9ca3af;
    margin: 2px 4px; font-family: 'JetBrains Mono', monospace;
}
.metric-exact {
    font-family: 'JetBrains Mono', monospace; font-size: 1.1rem;
    color: #10b981; font-weight: 600;
}
.metric-rounded {
    font-size: 0.78rem; color: #6b7280; margin-left: 8px;
}
.agent-header {
    font-family: 'JetBrains Mono', monospace; font-size: 1.6rem;
    font-weight: 600; color: #f9fafb; letter-spacing: -0.5px;
}
.agent-subheader {
    font-size: 0.8rem; color: #6b7280;
    font-family: 'JetBrains Mono', monospace; margin-top: -4px;
}
.retry-badge {
    background: #451a03; border: 1px solid #b45309; color: #fcd34d;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.72rem; font-family: 'JetBrains Mono', monospace;
}
.fix-badge {
    background: #022c22; border: 1px solid #059669; color: #6ee7b7;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.72rem; font-family: 'JetBrains Mono', monospace;
    margin-right: 4px;
}
.stButton > button {
    background: #1f2937 !important; color: #9ca3af !important;
    border: 1px solid #374151 !important; border-radius: 20px !important;
    font-size: 0.78rem !important; padding: 4px 14px !important;
    font-family: 'JetBrains Mono', monospace !important;
}
.stButton > button:hover {
    background: #374151 !important; color: #e2e8f0 !important;
    border-color: #6366f1 !important;
}
section[data-testid="stSidebar"] {
    background: #0a0c10 !important; border-right: 1px solid #1f2937;
}
.stChatInput textarea {
    background: #111827 !important; border: 1px solid #374151 !important;
    color: #e2e8f0 !important; border-radius: 8px !important;
}
.mode-badge {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-size: 0.72rem; font-family: 'JetBrains Mono', monospace;
    margin-bottom: 8px;
}
.mode-local { background: #1e3a5f; border: 1px solid #3b82f6; color: #93c5fd; }
.mode-cloud { background: #1a2e1a; border: 1px solid #22c55e; color: #86efac; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────
# DATA LOADING — smart: local CSV OR Kaggle download
# ─────────────────────────────────────────────────────

FILE_MAP = {
    "orders":               "orders.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv",
    "products":             "products.csv",
    "aisles":               "aisles.csv",
    "departments":          "departments.csv",
}

def download_from_kaggle(download_dir: Path) -> None:
    """
    Download Instacart dataset from Kaggle.
    Requires KAGGLE_USERNAME + KAGGLE_KEY in secrets or env.
    """
    import kaggle  # imported here so local installs without kaggle still work

    os.environ["KAGGLE_USERNAME"] = get_secret("KAGGLE_USERNAME")
    os.environ["KAGGLE_KEY"]      = get_secret("KAGGLE_KEY")

    kaggle.api.authenticate()

    progress_msg = st.empty()
    progress_msg.info("⬇️ Downloading Instacart dataset from Kaggle (~200MB). First load takes ~60s…")

    kaggle.api.dataset_download_files(
        "psparks/instacart-market-basket-analysis",
        path=str(download_dir),
        unzip=True,
        quiet=False,
    )
    progress_msg.empty()


@st.cache_resource(show_spinner=False)
def init_db(data_dir: str = "") -> tuple[duckdb.DuckDBPyConnection, dict, str]:
    """
    Load all CSVs into DuckDB.
    - LOCAL:  reads from data_dir (./data by default)
    - CLOUD:  downloads from Kaggle to /tmp if not already present

    Returns (connection, stats_dict, mode_string)
    """
    # ── Determine data source ──────────────────────
    local_dir  = Path(data_dir) if data_dir else Path("./data")
    cloud_dir  = Path("/tmp/instacart_data")
    mode       = "local"

    if local_dir.exists() and (local_dir / "orders.csv").exists():
        # Local mode — CSVs already present
        resolved_dir = local_dir
        mode = "local"
    else:
        # Cloud mode — download from Kaggle to /tmp
        cloud_dir.mkdir(parents=True, exist_ok=True)
        if not (cloud_dir / "orders.csv").exists():
            download_from_kaggle(cloud_dir)
        resolved_dir = cloud_dir
        mode = "cloud"

    # ── Load into DuckDB ───────────────────────────
    con = duckdb.connect(":memory:")
    con.execute("SET threads = 4")
    con.execute("SET memory_limit = '2GB'")

    stats = {}
    missing = []

    for table, fname in FILE_MAP.items():
        path = resolved_dir / fname
        if path.exists():
            con.execute(
                f"CREATE TABLE {table} AS "
                f"SELECT * FROM read_csv_auto('{path}', ignore_errors=true)"
            )
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
        else:
            missing.append(fname)

    if missing:
        raise FileNotFoundError(
            f"Missing CSV files: {missing}\n"
            f"Looked in: {resolved_dir}\n"
            "Local: place all 6 CSVs in ./data/\n"
            "Cloud: set KAGGLE_USERNAME + KAGGLE_KEY in Streamlit secrets."
        )

    # Combined VIEW — transparent, not a hidden materialised table
    con.execute("""
        CREATE VIEW order_products AS
        SELECT * FROM order_products_prior
        UNION ALL
        SELECT * FROM order_products_train
    """)
    stats["order_products (view)"] = (
        stats.get("order_products_prior", 0) + stats.get("order_products_train", 0)
    )

    return con, stats, mode


# ─────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────
def get_schema() -> str:
    return """
=== DATABASE SCHEMA (DuckDB in-memory — RAW TABLES ONLY) ===

TABLE: orders  (~3.4M rows)
  order_id               INT    -- unique order identifier
  user_id                INT    -- customer identifier
  eval_set               TEXT   -- 'prior' | 'train' | 'test'
  order_number           INT    -- nth order for this user (1 = first ever)
  order_dow              INT    -- 0=Saturday, 1=Sunday, 2=Monday, 3=Tuesday,
                                --  4=Wednesday, 5=Thursday, 6=Friday
  order_hour_of_day      INT    -- 0-23
  days_since_prior_order FLOAT  -- NULL for first-ever order (order_number=1)
                                -- ALWAYS filter: WHERE days_since_prior_order IS NOT NULL

TABLE: order_products_prior  (~32M rows)
TABLE: order_products_train  (~1.4M rows)
TABLE: order_products  ← VIEW = prior UNION ALL train  (~33.5M rows)
  order_id          INT
  product_id        INT
  add_to_cart_order INT    -- 1 = added first; higher = added later
  reordered         INT    -- BINARY: 1 = bought before, 0 = first time
                           -- reorder_rate = AVG(reordered), NOT SUM/COUNT

TABLE: products  (~50K rows)
  product_id    INT
  product_name  TEXT
  aisle_id      INT    -- FK → aisles.aisle_id
  department_id INT    -- FK → departments.department_id

TABLE: aisles  (134 rows)
  aisle_id INT
  aisle    TEXT

TABLE: departments  (21 rows)
  department_id INT
  department    TEXT

=== CORRECT JOIN PATHS (always use integer IDs, NEVER string columns) ===
  order_products.order_id      = orders.order_id
  order_products.product_id    = products.product_id
  products.aisle_id            = aisles.aisle_id
  products.department_id       = departments.department_id

=== CRITICAL RULES ===

RULE 1 — REORDER RATE:
  ✅ AVG(op.reordered) → gives rate 0.0–1.0
  ❌ NEVER SUM(reordered)/COUNT(*) manually
  ❌ NEVER use a pre-computed reorder_rate column

RULE 2 — SAMPLE BIAS (MANDATORY for rate/ratio/percentage):
  ✅ ALWAYS add: HAVING COUNT(*) >= 500

RULE 3 — NEVER AGGREGATE AGGREGATES:
  ✅ ALWAYS compute AVG(reordered) directly from raw rows

RULE 4 — CORRELATION QUERIES:
  ✅ chart_type = "scatter", NO LIMIT, return ALL rows

RULE 5 — JOINS on IDs only:
  ✅ products.aisle_id = aisles.aisle_id
  ❌ NEVER join on string columns

RULE 6 — NULL HANDLING:
  ✅ WHERE days_since_prior_order IS NOT NULL for frequency analysis

RULE 7 — LIMITS:
  ✅ Top-N: LIMIT 10
  ✅ Scatter/correlation: NO LIMIT

=== SQL PATTERNS ===

-- Reorder rate:
SELECT p.product_name, COUNT(*) AS total_orders, AVG(op.reordered) AS reorder_rate
FROM order_products op
JOIN products p ON op.product_id = p.product_id
GROUP BY p.product_name
HAVING COUNT(*) >= 500
ORDER BY reorder_rate DESC LIMIT 10;

-- Department totals:
SELECT d.department, COUNT(*) AS total_orders
FROM order_products op
JOIN products p    ON op.product_id   = p.product_id
JOIN departments d ON p.department_id = d.department_id
GROUP BY d.department ORDER BY total_orders DESC;

-- Correlation (no LIMIT):
SELECT a.aisle, AVG(op.reordered) AS reorder_rate, AVG(op.add_to_cart_order) AS avg_cart_position
FROM order_products op
JOIN products p ON op.product_id = p.product_id
JOIN aisles a   ON p.aisle_id    = a.aisle_id
GROUP BY a.aisle;
"""


# ─────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a senior BI analyst AI specializing in the Instacart grocery dataset.

CRITICAL: Respond with ONLY valid JSON. No preamble, no markdown fences, no explanation.

HARD RULES:
RULE A — reorder_rate = AVG(reordered). NEVER SUM, NEVER pre-computed column.
RULE B — Any rate/ratio/percentage query MUST have HAVING COUNT(*) >= 500.
RULE C — Correlation query → chart_type="scatter", NO LIMIT.
RULE D — Joins always on integer ID columns, NEVER string columns.
RULE E — days_since_prior_order queries MUST have WHERE days_since_prior_order IS NOT NULL.
RULE F — Only use: orders, order_products_prior, order_products_train, order_products, products, aisles, departments.
RULE G — Return raw numbers, UI will format them.

CHART SELECTION:
  ranking/top-N                         → bar
  time/sequence                         → line
  category breakdown %                  → pie (≤8) or bar (>8)
  correlation/relationship 2 metrics    → scatter
  single scalar (1 row, 1 col only)     → number
  "which single X has highest Y"        → bar (NOT number — has 2 cols)
  raw exploration                       → table

OUTPUT FORMAT (JSON only):
{
  "thought": "what user wants, tables used, join path, caveats",
  "sql": "valid DuckDB SQL",
  "chart_type": "bar|line|pie|scatter|table|number",
  "x_col": "column name",
  "y_col": "column name",
  "color_col": "",
  "title": "chart title",
  "insight": "1-2 sentence business insight",
  "applied_fixes": ["list of rules applied"]
}
"""


# ─────────────────────────────────────────────────────
# QUERY SEMANTICS DETECTOR
# ─────────────────────────────────────────────────────
RATE_KEYWORDS = {
    "reorder rate", "reorder_rate", "repurchase", "retention",
    "rate", "ratio", "percentage", "%", "percent", "frequency rate",
}
CORRELATION_KEYWORDS = {
    "correlat", "relationship between", "compare.*and", "vs", "versus",
    "how does.*affect", "show.*and.*how", "relate", "scatter",
}
TEMPORAL_KEYWORDS = {
    "days since", "days between", "frequency", "how often",
    "time between", "average days",
}


def detect_query_semantics(user_input: str) -> dict:
    t = user_input.lower()
    return {
        "needs_having_filter": any(kw in t for kw in RATE_KEYWORDS),
        "is_correlation":      any(re.search(kw, t) for kw in CORRELATION_KEYWORDS),
        "needs_null_filter":   any(kw in t for kw in TEMPORAL_KEYWORDS),
        "force_scatter":       any(re.search(kw, t) for kw in CORRELATION_KEYWORDS),
    }


# ─────────────────────────────────────────────────────
# INTENT CLASSIFICATION
# ─────────────────────────────────────────────────────
GREETINGS    = {"hi", "hello", "hey", "sup", "yo", "hiya", "howdy", "good morning", "good afternoon"}
META_QUERIES = {"schema", "tables", "what tables", "what data", "show schema", "show tables"}

CHITCHAT_PATTERNS = [
    r"^what('s| is) your name", r"^who are you", r"^what are you", r"^who made you",
    r"^are you (an? )?ai", r"^how are you", r"^what can you do",
    r"^help$", r"^thank", r"^thanks",
    r"^ok$", r"^okay$", r"^cool$", r"^nice$", r"^great$", r"^awesome$",
]
CHITCHAT_REPLIES = {
    "name":    "I'm the Instacart BI Agent v2 — ask me anything about 3.4M orders, 200K customers, and 50K products.",
    "help":    "Try: *Top 10 reordered products*, *Busiest hour*, *Reorder rate by department*, *Correlation between reorder rate and cart position*.",
    "thanks":  "Happy to help. Ask another question.",
    "default": "I answer questions about the Instacart dataset. Try asking about products, departments, order patterns, or reorder behaviour.",
}


def _is_gibberish(text: str) -> bool:
    words = text.split()
    if not words: return True
    vowels = set("aeiou")
    real_words = [w for w in words if len(w) >= 2 and any(c in vowels for c in w.lower())]
    if not real_words: return True
    if len(words) >= 3 and len(real_words) / len(words) < 0.25: return True
    stripped = text.lower().replace(" ", "")
    if len(stripped) >= 6:
        for seg_len in range(2, 5):
            seg = stripped[:seg_len]
            if stripped == seg * (len(stripped) // seg_len) + stripped[:len(stripped) % seg_len]:
                return True
    return False


def classify_intent(text: str) -> str:
    t = text.lower().strip().rstrip("?!.")
    if not t: return "unknown"
    if t in GREETINGS or t.startswith("good "): return "greeting"
    if t in META_QUERIES or "what tables" in t or "show me the schema" in t: return "schema"
    for pattern in CHITCHAT_PATTERNS:
        if re.search(pattern, t): return "chitchat"
    if len(t) < 4 or _is_gibberish(t): return "unknown"
    return "query"


def chitchat_reply(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["name", "who are", "what are", "who made"]): return CHITCHAT_REPLIES["name"]
    if "help" in t or "can you do" in t: return CHITCHAT_REPLIES["help"]
    if "thank" in t: return CHITCHAT_REPLIES["thanks"]
    return CHITCHAT_REPLIES["default"]


# ─────────────────────────────────────────────────────
# LLM CALL
# ─────────────────────────────────────────────────────
def call_llm(messages: list, temperature: float = 0.0) -> dict:
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=messages,
        temperature=temperature,
        max_tokens=1500,
    )
    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def build_context_prompt(user_input: str, history: list, flags: dict) -> list:
    schema = get_schema()
    context_lines = []
    for turn in history[-4:]:
        role = turn["role"]
        if role == "user":
            context_lines.append(f"USER: {turn['content']}")
        elif role == "assistant" and turn.get("sql"):
            context_lines.append(f"ASSISTANT SQL: {turn['sql'][:150]}...")
            context_lines.append(f"ASSISTANT insight: {turn.get('insight', '')}")

    hints = []
    if flags["needs_having_filter"]: hints.append("⚠ RATE/RATIO QUERY: MUST include HAVING COUNT(*) >= 500")
    if flags["is_correlation"]:      hints.append("⚠ CORRELATION QUERY: use scatter chart, NO LIMIT, return all rows")
    if flags["needs_null_filter"]:   hints.append("⚠ TEMPORAL QUERY: add WHERE days_since_prior_order IS NOT NULL")

    full_prompt = (
        f"{schema}\n\n"
        f"CONVERSATION HISTORY:\n{chr(10).join(context_lines)}\n\n"
        f"SEMANTIC HINTS:\n{chr(10).join(hints)}\n\n"
        f"CURRENT QUESTION: {user_input}"
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": full_prompt},
    ]


# ─────────────────────────────────────────────────────
# SQL POST-PROCESSOR
# ─────────────────────────────────────────────────────
def post_process_sql(sql: str, flags: dict, result: dict) -> tuple[str, list]:
    fixes = []
    sql_upper = sql.upper()

    if flags["needs_having_filter"] and "HAVING" not in sql_upper:
        if "ORDER BY" in sql_upper:
            sql = re.sub(r"(ORDER BY)", r"HAVING COUNT(*) >= 500 \1", sql, flags=re.IGNORECASE, count=1)
        else:
            sql = sql.rstrip(";") + "\nHAVING COUNT(*) >= 500"
        fixes.append("Auto-added HAVING COUNT(*) >= 500 (sample bias prevention)")

    if flags["force_scatter"] and result.get("chart_type") != "scatter":
        result["chart_type"] = "scatter"
        fixes.append("Chart type overridden to scatter (correlation query)")

    if flags["is_correlation"] and "LIMIT" in sql_upper:
        sql = re.sub(r"\bLIMIT\s+\d+\b", "", sql, flags=re.IGNORECASE).strip()
        fixes.append("Removed LIMIT (correlation needs all data points)")

    for ft in ["product_stats", "product_summary", "aisle_stats"]:
        if ft.lower() in sql.lower():
            fixes.append(f"⚠ WARNING: SQL references hidden table '{ft}'")

    return sql, fixes


# ─────────────────────────────────────────────────────
# SQL SAFETY VALIDATOR
# ─────────────────────────────────────────────────────
FORBIDDEN_KEYWORDS = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]

def validate_sql(sql: str) -> tuple[bool, str]:
    upper = sql.upper()
    for kw in FORBIDDEN_KEYWORDS:
        if kw in upper:
            return False, f"Forbidden keyword: {kw}"
    return True, ""

def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    return con.execute(sql).df()


# ─────────────────────────────────────────────────────
# RETRY / REPAIR LOOP
# ─────────────────────────────────────────────────────
def generate_and_execute(con, user_input: str, history: list, max_retries: int = 2):
    flags     = detect_query_semantics(user_input)
    messages  = build_context_prompt(user_input, history, flags)
    result    = call_llm(messages)
    error_log = []

    if not result.get("sql", "").strip():
        raise ValueError(
            "I can only answer questions about the Instacart dataset. "
            "Try asking about products, orders, departments, or customer behaviour."
        )

    fixed_sql, auto_fixes = post_process_sql(result["sql"], flags, result)
    result["sql"]        = fixed_sql
    result["auto_fixes"] = auto_fixes

    for attempt in range(max_retries + 1):
        sql = result["sql"].strip()
        ok, reason = validate_sql(sql)
        if not ok:
            raise ValueError(f"SQL safety check failed: {reason}")
        try:
            df = run_query(con, sql)
            return result, df, attempt, error_log, flags
        except Exception as e:
            error_msg = str(e)
            error_log.append({"attempt": attempt + 1, "sql": sql, "error": error_msg})
            if attempt < max_retries:
                repair_messages = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": (
                        f"SQL failed:\n{error_msg}\n\nFailed SQL:\n{sql}\n\n"
                        f"Schema:\n{get_schema()}\n\nQuestion: {user_input}\n\nFix it. JSON only."
                    )},
                ]
                result = call_llm(repair_messages, temperature=0.1)
                fixed_sql, extra = post_process_sql(result["sql"], flags, result)
                result["sql"] = fixed_sql
                result.setdefault("auto_fixes", []).extend(extra)
            else:
                raise RuntimeError(f"Query failed after {max_retries+1} attempts. Last error: {error_msg}")


# ─────────────────────────────────────────────────────
# NUMBER FORMATTING
# ─────────────────────────────────────────────────────
def fmt_exact(n) -> str:
    try:    return f"{int(n):,}"
    except: return str(n)

def fmt_human(n) -> str:
    try:
        n = float(n)
        if n >= 1_000_000: return f"{n/1_000_000:.2f}M"
        if n >= 1_000:     return f"{n/1_000:.1f}K"
        return str(int(n))
    except: return str(n)


# ─────────────────────────────────────────────────────
# CHART BUILDER
# ─────────────────────────────────────────────────────
DARK_TEMPLATE = "plotly_dark"
CHART_COLORS  = px.colors.qualitative.Bold


def build_chart(df: pd.DataFrame, result: dict) -> go.Figure | None:
    ct    = result.get("chart_type", "table")
    x     = result.get("x_col", "")
    y     = result.get("y_col", "")
    color = result.get("color_col", "") or None
    title = result.get("title", "")
    cols  = df.columns.tolist()

    if x not in cols: x = cols[0] if cols else None
    if y not in cols and ct not in ("number", "table", "pie"):
        y = cols[1] if len(cols) > 1 else (cols[0] if cols else None)

    fig = None
    try:
        if ct == "bar":
            fig = px.bar(df, x=x, y=y, color=color, title=title,
                         color_discrete_sequence=CHART_COLORS, template=DARK_TEMPLATE, text=y)
            if y in df.columns:
                col_vals = df[y].dropna()
                is_rate  = (col_vals.max() <= 1.0 and col_vals.min() >= 0)
                if is_rate:
                    df = df.copy()
                    df["_label"] = df[y].apply(lambda v: f"{v*100:.1f}%")
                    fig = px.bar(df, x=x, y=y, color=color, title=title,
                                 color_discrete_sequence=CHART_COLORS, template=DARK_TEMPLATE, text="_label")
                    fig.update_traces(textposition="outside")
                elif df[y].dtype in [float] and (df[y] % 1 != 0).any():
                    fig.update_traces(texttemplate="%{text:,.2f}", textposition="outside")
                else:
                    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig.update_layout(xaxis_tickangle=-35)

        elif ct == "line":
            fig = px.line(df, x=x, y=y, color=color, title=title,
                          template=DARK_TEMPLATE, markers=True,
                          color_discrete_sequence=CHART_COLORS)

        elif ct == "pie":
            fig = px.pie(df.head(12), names=x, values=y, title=title,
                         template=DARK_TEMPLATE, color_discrete_sequence=CHART_COLORS)
            fig.update_traces(textposition="inside", textinfo="percent+label")

        elif ct == "scatter":
            hover_cols = [c for c in cols if c not in (x, y)][:2]
            fig = px.scatter(
                df, x=x, y=y, color=color, title=title,
                template=DARK_TEMPLATE, color_discrete_sequence=CHART_COLORS,
                hover_data=[cols[0]] + hover_cols if cols[0] not in (x, y) else hover_cols,
                trendline="ols",
                labels={x: x.replace("_", " ").title(), y: y.replace("_", " ").title()},
            )
            fig.update_traces(marker=dict(size=7, opacity=0.75))

        elif ct == "number":
            if len(df) > 0:
                numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                string_cols  = df.select_dtypes(include=["object"]).columns.tolist()
                if not numeric_cols:
                    ct = "table"
                else:
                    val       = df[numeric_cols[0]].iloc[0]
                    label_str = str(df[string_cols[0]].iloc[0]) if string_cols else ""
                    is_rate   = (0.0 <= float(val) <= 1.0)
                    if is_rate:
                        display_val, value_suffix, fmt = float(val)*100, "%", ".1f"
                    else:
                        display_val  = float(val)
                        value_suffix = ""
                        fmt = ",d" if float(val) == int(float(val)) else ",.2f"

                    display_title = title + (
                        f"<br><span style='font-size:0.85rem;color:#6ee7b7'>{label_str}</span>"
                        if label_str else ""
                    )
                    fig = go.Figure(go.Indicator(
                        mode="number", value=display_val,
                        title={"text": display_title, "font": {"color": "#e2e8f0", "size": 14}},
                        number={"font": {"color": "#10b981", "size": 64},
                                "suffix": value_suffix, "valueformat": fmt},
                    ))
                    fig.update_layout(template=DARK_TEMPLATE, height=240)

    except Exception as e:
        st.warning(f"Chart error: {e}")
        return None

    if fig and ct != "number":
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(17,24,39,0.8)",
            font_color="#e2e8f0", title_font_size=15, title_font_color="#f9fafb",
            margin=dict(t=60, b=50, l=50, r=30),
            legend=dict(bgcolor="rgba(31,41,55,0.8)", bordercolor="#374151", borderwidth=1),
        )
    return fig


# ─────────────────────────────────────────────────────
# TABLE RENDERER
# ─────────────────────────────────────────────────────
def render_clean_table(df: pd.DataFrame):
    display_df = df.copy()
    display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]

    for col in display_df.select_dtypes(include=["float64", "float32"]).columns:
        if display_df[col].max() <= 1.0 and display_df[col].min() >= 0:
            display_df[col] = display_df[col].apply(lambda v: f"{v:.1%}" if pd.notna(v) else "")
        else:
            display_df[col] = display_df[col].apply(lambda v: f"{v:,.2f}" if pd.notna(v) else "")

    for col in display_df.select_dtypes(include=["int64", "int32"]).columns:
        display_df[col] = display_df[col].apply(lambda v: f"{v:,}" if pd.notna(v) else "")

    st.dataframe(display_df, use_container_width=True,
                 height=min(420, 48 + 36 * len(display_df)), hide_index=True)


# ─────────────────────────────────────────────────────
# SUGGESTED QUERIES
# ─────────────────────────────────────────────────────
SUGGESTED_QUERIES = [
    "Top 10 most reordered products (min 1000 orders)",
    "Which hour of day gets most orders?",
    "Reorder rate by department",
    "Correlation between reorder rate and cart position per aisle",
    "How many unique customers order on each day of week?",
    "Top 5 departments by number of products",
    "Average days between orders (excluding first orders)",
    "Top 10 departments by total orders",
]


# ─────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="agent-header">⬡ BI AGENT v2</div>', unsafe_allow_html=True)
    st.markdown('<div class="agent-subheader">instacart · groq · duckdb · llama3.3</div>', unsafe_allow_html=True)
    st.divider()

    # Show deployment mode badge
    if is_cloud():
        st.markdown(
            '<span class="mode-badge mode-cloud">☁️ Cloud Mode — Kaggle auto-download</span>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<span class="mode-badge mode-local">🖥 Local Mode — ./data/ folder</span>',
            unsafe_allow_html=True,
        )

    # Local mode: let user override data directory
    if not is_cloud():
        data_dir_input = st.text_input(
            "📁 Data directory", "./data",
            help="Folder containing the 6 CSV files"
        )
    else:
        data_dir_input = ""

    load_btn = st.button("⚡ Load Database", use_container_width=True)

    if load_btn:
        with st.spinner("Loading dataset…"):
            t0 = time.time()
            try:
                con, stats, mode = init_db(data_dir_input)
                elapsed = time.time() - t0
                st.session_state["con"]   = con
                st.session_state["stats"] = stats
                st.session_state["mode"]  = mode
                st.session_state["ready"] = True
                st.success(f"✅ Loaded in {elapsed:.1f}s  ({mode} mode)")
            except FileNotFoundError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"Load failed: {e}")

    if st.session_state.get("ready"):
        st.divider()
        st.markdown("**📊 Table Sizes**")
        for tname, cnt in st.session_state.get("stats", {}).items():
            st.markdown(
                f'<span class="stat-pill">{tname}: {fmt_exact(cnt)} ({fmt_human(cnt)})</span>',
                unsafe_allow_html=True,
            )

        st.divider()
        st.markdown("**⚙️ Settings**")
        st.session_state["show_sql"]    = st.toggle("Show SQL",             value=True)
        st.session_state["show_thought"]= st.toggle("Show agent reasoning", value=True)
        st.session_state["show_fixes"]  = st.toggle("Show auto-fixes",      value=True)

        st.divider()
        if st.button("🗑️ Clear chat", use_container_width=True):
            st.session_state["history"] = []
            st.rerun()

    st.divider()
    st.markdown("""
    <div style="font-size:0.7rem; color:#4b5563; font-family:'JetBrains Mono',monospace;">
    v2 · local + cloud ready<br>
    · exact numbers shown<br>
    · HAVING filter auto-added<br>
    · scatter for correlation<br>
    · no aggregation of aggregates<br>
    · ID-based joins only<br>
    · Kaggle auto-download on cloud
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────
if not st.session_state.get("ready"):
    cloud_note = (
        "Auto-downloads from Kaggle on first load (~60s)."
        if is_cloud() else
        "Place your 6 CSVs in <code>./data/</code> then click Load."
    )
    st.markdown(f"""
    <div style="text-align:center; padding: 80px 40px;">
        <div style="font-size:3rem">🛒</div>
        <h2 style="color:#f9fafb; font-family:'JetBrains Mono',monospace;">Instacart BI Agent v2</h2>
        <p style="color:#6b7280; max-width:520px; margin:0 auto; font-size:0.9rem;">
            Production-grade BI agent with statistical safeguards.<br>
            {cloud_note}
        </p>
        <div style="margin-top:24px; display:flex; gap:12px; justify-content:center; flex-wrap:wrap;">
            <span class="stat-pill">DuckDB columnar engine</span>
            <span class="stat-pill">Groq LLaMA 3.3 70B</span>
            <span class="stat-pill">HAVING sample bias guard</span>
            <span class="stat-pill">Scatter for correlations</span>
            <span class="stat-pill">Auto SQL repair</span>
            <span class="stat-pill">Local + Cloud ready</span>
        </div>
        <div style="margin-top:32px; color:#4b5563; font-size:0.8rem; font-family:'JetBrains Mono',monospace;">
            ← Click "Load Database" in the sidebar to begin
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# Init history
if "history" not in st.session_state:
    st.session_state["history"] = []

# Render past turns
for turn in st.session_state.get("history", []):
    if turn["role"] == "user":
        st.markdown(f'<div class="user-msg">🙋 {turn["content"]}</div>', unsafe_allow_html=True)
    elif turn["role"] == "assistant":
        st.markdown(f'<div class="agent-msg">✦ {turn.get("insight", "")}</div>', unsafe_allow_html=True)

# Suggested query chips
if not st.session_state["history"]:
    st.markdown("**💡 Try these queries:**")
    cols_ui = st.columns(4)
    for i, q in enumerate(SUGGESTED_QUERIES):
        if cols_ui[i % 4].button(q, key=f"chip_{i}"):
            st.session_state["pending_query"] = q
            st.rerun()

# Chat input
user_input = st.chat_input("Ask anything about the Instacart data…")
if "pending_query" in st.session_state:
    user_input = st.session_state.pop("pending_query")
if not user_input:
    st.stop()

st.markdown(f'<div class="user-msg">🙋 {user_input}</div>', unsafe_allow_html=True)

# Intent handling
intent = classify_intent(user_input)

if intent == "greeting":
    st.markdown('<div class="agent-msg">✦ Ready. Ask about products, customers, departments, reorder behaviour, or correlations.</div>', unsafe_allow_html=True)
    st.stop()
if intent == "chitchat":
    st.markdown(f'<div class="agent-msg">✦ {chitchat_reply(user_input)}</div>', unsafe_allow_html=True)
    st.stop()
if intent == "unknown":
    st.markdown('<div class="agent-msg">✦ I didn\'t catch that. Try: "Top 10 reordered products" or "Which hour has most orders?"</div>', unsafe_allow_html=True)
    st.stop()
if intent == "schema":
    with st.expander("Database Schema", expanded=True):
        st.code(get_schema(), language="sql")
    st.stop()

# Query execution
con          = st.session_state["con"]
show_sql     = st.session_state.get("show_sql", True)
show_thought = st.session_state.get("show_thought", True)
show_fixes   = st.session_state.get("show_fixes", True)

with st.spinner("⟳ Generating query…"):
    try:
        result, df, retries, error_log, flags = generate_and_execute(
            con, user_input, st.session_state["history"]
        )
    except ValueError as e:
        st.markdown(f'<div class="agent-msg">✦ {e}</div>', unsafe_allow_html=True)
        st.stop()
    except Exception as e:
        st.markdown(f'<div class="agent-msg">✦ Query failed: <code>{e}</code></div>', unsafe_allow_html=True)
        st.stop()

# Agent thought
if show_thought and result.get("thought"):
    st.markdown(f'<div class="thought-card">💭 {result["thought"]}</div>', unsafe_allow_html=True)

# Auto-fixes
auto_fixes = result.get("auto_fixes", []) + result.get("applied_fixes", [])
if show_fixes and auto_fixes:
    fix_html  = " ".join(f'<span class="fix-badge">✓ {f}</span>' for f in auto_fixes if not f.startswith("⚠"))
    warn_html = " ".join(f'<span class="retry-badge">{f}</span>' for f in auto_fixes if f.startswith("⚠"))
    if fix_html:  st.markdown(fix_html, unsafe_allow_html=True)
    if warn_html: st.markdown(warn_html, unsafe_allow_html=True)

# Retry badge
if retries > 0:
    st.markdown(f'<span class="retry-badge">⚠ Auto-repaired after {retries} failed attempt(s)</span>', unsafe_allow_html=True)
    with st.expander("Error history", expanded=False):
        for e in error_log:
            st.code(f"Attempt {e['attempt']}: {e['error']}", language="text")

# SQL display
if show_sql:
    sql_to_show = result.get("sql", "").strip()
    if not sql_to_show:
        for key in ("query", "generated_sql", "duckdb_sql", "statement"):
            if result.get(key, "").strip():
                sql_to_show = result[key].strip()
                break
    with st.expander("🔍 Generated SQL", expanded=False):
        if sql_to_show:
            st.code(sql_to_show, language="sql")
        else:
            st.warning("⚠ LLM returned empty SQL.")

# Results
st.markdown(f"### {result.get('title', 'Results')}")
n = len(df)
st.markdown(
    f'<span class="stat-pill">{fmt_exact(n)} rows returned ({fmt_human(n)})</span>',
    unsafe_allow_html=True,
)

# Single-value display
if n == 1 and len(df.columns) == 1:
    val = df.iloc[0, 0]
    st.markdown(
        f'<div style="padding:20px 0">'
        f'<span class="metric-exact">{fmt_exact(val)}</span>'
        f'<span class="metric-rounded">({fmt_human(val)})</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

# Chart
fig = build_chart(df, result)
if fig:
    st.plotly_chart(fig, use_container_width=True)

# Raw data table
if result.get("chart_type") != "number":
    with st.expander(f"📋 Raw data ({fmt_exact(n)} rows)", expanded=(fig is None or n <= 20)):
        render_clean_table(df)

# Insight
if result.get("insight"):
    st.markdown(f'<div class="insight-card">✦ {result["insight"]}</div>', unsafe_allow_html=True)

# Save to history
st.session_state["history"].append({"role": "user", "content": user_input})
st.session_state["history"].append({
    "role": "assistant",
    "content": result.get("insight", ""),
    "sql":     result.get("sql", ""),
    "insight": result.get("insight", ""),
})
