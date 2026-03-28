# 🛒 Instacart BI Agent v2 — Production-Grade Conversational Analytics

> **Natural Language → SQL → Insights → Visualization**
> Built for Hireathon 2026 | i2e Consulting AI Labs

---

## 🚀 What Makes This Different?

Most NL→SQL apps are **demo-level**.
This is a **production-grade BI agent** with:

* ✅ **Statistical correctness (no sample bias)**
* ✅ **Auto SQL repair + validation**
* ✅ **Deterministic JSON contract (no hallucinated UI)**
* ✅ **Explainability layer (thought + fixes + SQL)**
* ✅ **Correct chart selection (scatter for correlation, etc.)**
* ✅ **Handles 33M+ rows efficiently with DuckDB**

---

## 🧠 System Architecture

```
User Query (Natural Language)
        │
        ▼
┌────────────────────────────────────────────┐
│  LLM Layer (Groq / LLaMA 3.3 70B)          │
│  - SQL generation                          │
│  - Chart selection                         │
│  - Structured JSON output                  │
│  - Context-aware (multi-turn memory)       │
└──────────────┬─────────────────────────────┘
               │ JSON (sql + metadata)
               ▼
┌────────────────────────────────────────────┐
│  Guardrail Layer (CRITICAL)                │
│  - HAVING filter injection (bias fix)      │
│  - Correlation → scatter enforcement       │
│  - LIMIT removal for analytics             │
│  - SQL validation (no DROP/DELETE)         │
│  - Auto-repair loop (retry on failure)     │
└──────────────┬─────────────────────────────┘
               │ Safe SQL
               ▼
┌────────────────────────────────────────────┐
│  DuckDB Engine (In-Memory OLAP)            │
│  - 33M+ rows                              │
│  - Columnar execution                      │
│  - Vectorized joins                        │
│  - order_products VIEW (prior + train)     │
└──────────────┬─────────────────────────────┘
               │ DataFrame
               ▼
┌────────────────────────────────────────────┐
│  Visualization Layer (Plotly)              │
│  - bar / line / pie / scatter              │
│  - auto-selection based on intent          │
│  - regression line for correlation         │
└──────────────┬─────────────────────────────┘
               ▼
┌────────────────────────────────────────────┐
│  Streamlit UI                              │
│  - Chat interface                          │
│  - SQL transparency                        │
│  - Raw data table                          │
│  - Insights + reasoning                    │
└────────────────────────────────────────────┘
```

---

## ⚙️ Core Innovations (This is where you WIN interviews)

### 1. 🛑 Statistical Guardrails (BIG differentiator)

* Auto-injects:

```sql
HAVING COUNT(*) >= 500
```

👉 Prevents:

* Fake 100% reorder rates from small samples
* Misleading BI insights

---

### 2. 🧠 Semantic Query Detection

Detects:

* Correlation queries → forces scatter plot
* Rate queries → enforces HAVING filter
* Temporal queries → handles NULLs

---

### 3. 🔁 Auto SQL Repair Loop

If SQL fails:

1. Capture error
2. Send back to LLM
3. Regenerate fixed SQL
4. Retry execution

👉 Zero manual debugging

---

### 4. 📊 Correct BI Logic (No Rookie Mistakes)

Enforced rules:

* ✅ `AVG(reordered)` → correct reorder rate
* ❌ Never `AVG(precomputed_rate)`
* ❌ Never string joins
* ❌ Never hidden aggregates

---

### 5. 🔍 Explainability Layer

Every response includes:

* Thought process
* SQL query
* Auto-fixes applied
* Business insight

👉 This is **enterprise-grade transparency**

---

## 🗄️ Data Model

* Orders: ~3.4M
* Order Products: ~33M
* Products: ~50K

### Key Tables:

* `orders`
* `order_products_prior`
* `order_products_train`
* `products`
* `aisles`
* `departments`

### Optimization:

```sql
CREATE VIEW order_products AS
SELECT * FROM order_products_prior
UNION ALL
SELECT * FROM order_products_train
```

---

## ⚡ Why DuckDB?

| Problem           | Solution             |
| ----------------- | -------------------- |
| 32M+ rows         | Columnar engine      |
| Slow joins        | Vectorized execution |
| Memory issues     | Efficient scanning   |
| Pandas limitation | Replaced entirely    |

👉 Runs complex queries in seconds.

---

## 🧪 Example Queries

* “Top 10 reordered products (min 1000 orders)”
* “Which hour has most orders?”
* “Reorder rate by department”
* “Correlation between reorder rate and cart position”
* “Average days between orders”

---

## 🚀 Setup

### 1. Install

```bash
pip install -r requirements.txt
```

### 2. Download Dataset

From Kaggle → unzip into `/data`

### 3. Add API Key

```bash
export GROQ_API_KEY=your_key_here
```

### 4. Run

```bash
streamlit run app.py
```

---

## ⚠️ Known Limitations

* No real timestamps (only relative days)
* Very complex basket queries may be slower
* LLM can still misinterpret vague questions
* Long chats may require summarization (future work)

---

## 🔮 Future Improvements

* Query caching layer
* Role-based BI dashboards
* Semantic layer (metrics catalog)
* Real-time data ingestion
* Cost-aware LLM routing

---

## 🏆 Why This Project Stands Out

This is NOT just:

> “Ask question → get chart”

This is:

> **A reliable BI system with correctness guarantees**

You solved:

* LLM hallucination
* Statistical bias
* Wrong aggregations
* Chart misuse
* SQL failures

👉 That’s exactly what companies struggle with.

---

## 💡 Interview One-Liner

> “I didn’t just build an NL→SQL app — I built a production-grade BI agent with statistical guardrails, auto-repairing SQL, and explainable analytics over 33M rows.”

---

## 📌 Tech Stack

* **LLM**: Groq (LLaMA 3.3 70B)
* **DB**: DuckDB (in-memory OLAP)
* **UI**: Streamlit
* **Charts**: Plotly
* **Language**: Python

---

## 👨‍💻 Author

Built by S Gokul Krishna
Hireathon 2026 Candidate 🚀
