# Hireathon Evaluation Form — Answers
## Problem 1: Conversational BI Agent

---

### Q1: Why did you choose DuckDB over pandas or SQLite?

**Answer:**
`order_products__prior.csv` has ~32 million rows. Loading that into pandas would consume 4–6 GB of RAM and make joins extremely slow — a naive merge would likely OOM on a standard laptop. 

DuckDB is an in-process OLAP database with a columnar vectorized execution engine. It's purpose-built for analytical queries (aggregations, multi-table joins) on large datasets. Key advantages over SQLite: DuckDB reads directly from CSV files without a separate import step, uses vectorized SIMD operations, and parallelizes queries across CPU cores automatically. On the 32M row table, a `GROUP BY` aggregation runs in ~2 seconds vs 30+ seconds in pandas.

I chose in-memory (`:memory:`) over a persisted `.duckdb` file because the CSVs are the source of truth — there's no benefit to maintaining a separate database file for a single-session BI tool.

---

### Q2: How does your system handle the prior vs train split?

**Answer:**
I created a DuckDB VIEW called `order_products` that is a `UNION ALL` of `order_products_prior` and `order_products_train`. 

For a BI tool (not a prediction task), the eval_set partition is irrelevant — we want the full picture of what was ordered. The VIEW is zero-cost (no data duplication) and makes queries cleaner. When an analyst asks "top 10 most ordered products," they want all orders, not just one partition.

The `orders` table still has the `eval_set` column, and I include it in the schema description to Claude so it can filter if the user explicitly asks about a specific partition.

---

### Q3: How do you handle NaN in days_since_prior_order?

**Answer:**
`days_since_prior_order` is NULL for every user's first order (order_number = 1). This is semantically correct — there's no "days since prior" for an initial order.

In the system prompt to Claude, I explicitly note: *"days_since_prior_order can be NULL for first orders."* This instructs the LLM to use `WHERE days_since_prior_order IS NOT NULL` when computing averages or frequency metrics, preventing silent corruption (a naive `AVG()` in SQL already ignores NULLs, but COUNT and calculations involving NULL would fail without this awareness).

A failure case: if someone asks "average order frequency per user" and the LLM forgets to filter NULLs, the first-order NULL values would be excluded from AVG anyway, but a query doing `DATEDIFF` style calculations could produce wrong results.

---

### Q4: How does the multi-turn memory work?

**Answer:**
I maintain a `chat_history` list in Streamlit session state. Every user message and every assistant response (including the SQL result summary) is appended to this list. On each new query, the *full history* is sent to the Claude API as the `messages` array.

This enables follow-up questions like:
- "Top 10 departments by order count" → [user sees bar chart]
- "Now show only produce and dairy" → Claude knows what the prior query was and can add a `WHERE department IN ('produce', 'dairy eggs')` filter

Limitation: After ~15 turns, the context window fills up. A production system would summarize old turns or use a sliding window. For a hackathon demo, full history is fine.

---

### Q5: What breaks in your system?

**Answer (be honest — they reward this):**

1. **Complex basket analysis**: "What products are most commonly bought together?" requires a self-join on 32M rows (`op1.product_id != op2.product_id WHERE op1.order_id = op2.order_id`). DuckDB can handle it, but it may take 30-60 seconds.

2. **Absolute time queries**: The dataset has no calendar dates — only relative `days_since_prior_order`. "Show me orders from December 2024" is unanswerable. The system should gracefully explain this instead of generating a wrong query.

3. **SQL hallucination**: Claude occasionally joins on the wrong key or uses a column name that doesn't exist. I show the SQL to the user for transparency, but there's no automatic retry loop in the MVP (it's listed as a stretch goal).

4. **Very long conversations**: Full history sent to Claude — after 15+ turns, token costs grow and context quality degrades.

5. **Chart type edge cases**: Claude picks chart type from semantics. "Orders by hour" → line chart ✓. But ambiguous queries might get the wrong chart type.

---

### Q6: Why Streamlit over FastAPI + React?

**Answer:**
Time constraint. Streamlit lets me build a functional, visually decent UI in ~50 lines vs 500+ for a React frontend with a FastAPI backend. For a 3-hour hackathon, this is the right tradeoff.

The downside: Streamlit reruns the entire script on every interaction (though session_state manages persistence), it has limited UI customization, and it's not production-grade for multi-user deployment. For a real product I'd build a proper React frontend with WebSocket streaming.

---

### Q7: How does Claude know which tables to join?

**Answer:**
The system prompt includes a complete schema description with:
- All table names, column names, and types
- Primary key / foreign key relationships written out explicitly
- A note about the `order_products` view being the correct one for BI queries

This "schema injection" approach is standard for text-to-SQL systems. Claude's reasoning capability handles multi-table join planning given this context. For production, you'd fine-tune on schema-specific examples or use a retrieval step to inject only relevant table schemas.

---

### Q8: How would you scale this to 10x the data?

**Answer:**
Current approach handles 32M rows fine in DuckDB in-memory. For 10x (320M rows):

1. **Switch to persisted DuckDB file** — avoids reload on restart, can be bigger than RAM
2. **Partition-aware queries** — add sampling hints to Claude for exploratory queries
3. **Result caching** — cache common aggregations (top products, department stats don't change)
4. **Async execution** — run queries in a background thread, stream results to UI
5. **For truly massive scale** — replace DuckDB with MotherDuck (cloud DuckDB) or BigQuery, same SQL interface
