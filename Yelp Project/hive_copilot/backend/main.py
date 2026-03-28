# ============================================================
# RAHAT — Backend (FastAPI + Hive Connection)
# File: hive_copilot/backend/main.py
# ============================================================
# Install dependencies:
# pip install fastapi uvicorn impyla requests pandas
# ============================================================

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from impala.dbapi import connect
import pandas as pd
import requests
import json
import re
import os

app = FastAPI()

# Allow Streamlit frontend to call this backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Hive connection settings ──
HIVE_HOST = "192.168.56.101"
HIVE_PORT = 10000
HIVE_DB   = "default"

# ── DeepSeek API settings ──
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-8d12c866e35b48b0920277923a961ff9")
DEEPSEEK_URL     = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"

# ── Yelp schema injected into every LLM prompt ──
YELP_SCHEMA = """
You are a SQL expert for a Yelp dataset stored in Apache Hive.
Database: default

Tables and columns:
1. business
   - business_id   STRING  (unique ID)
   - name          STRING  (business name)
   - city          STRING  (e.g. 'Philadelphia')
   - state         STRING  (e.g. 'PA')
   - stars         FLOAT   (avg star rating 1.0-5.0)
   - review_count  INT     (total reviews)
   - categories    STRING  (comma-separated, e.g. 'Restaurants, Chinese, Delivery')
   - is_open       INT     (1=open, 0=closed)

2. review
   - review_id       STRING
   - rev_business_id STRING  (links to business.business_id)
   - rev_user_id     STRING  (links to users.user_id)
   - rev_stars       INT     (1-5)
   - rev_text        STRING  (review content)
   - rev_date        DATE    (format: YYYY-MM-DD)
   - rev_useful      INT
   - rev_funny       INT
   - rev_cool        INT

3. users
   - user_id           STRING
   - user_name         STRING
   - user_review_count INT
   - user_fans         INT
   - user_elite        STRING  (comma-separated years e.g. '2015,2016')
   - user_average_stars FLOAT
   - user_yelping_since STRING (format: YYYY-MM-DD)

4. checkin
   - business_id    STRING
   - checkin_dates  STRING  (comma-separated datetimes)

Rules:
- Always use HiveQL syntax (not MySQL or PostgreSQL)
- Use LIKE with % for partial string matches in categories
- For categories use: categories LIKE '%Restaurants%'
- Never use LIMIT more than 100
- Always end SQL with a semicolon
- Only return the SQL query, nothing else, no markdown, no explanation
"""


class QueryRequest(BaseModel):
    question: str
    history:  list = []   # for multi-turn conversation


def get_hive_connection():
    return connect(
        host="192.168.56.101",
        port=10000,
        database="default",
        auth_mechanism="PLAIN",    # ← change from NOSASL to PLAIN
        user="jack",               # ← add your username
    )


def generate_sql(question: str, history: list, error_feedback: str = None) -> str:
    """Send question to DeepSeek and get back a SQL query"""
    messages = [{"role": "system", "content": YELP_SCHEMA}]

    # Add conversation history for multi-turn support
    for turn in history:
        messages.append({"role": "user",      "content": turn["question"]})
        messages.append({"role": "assistant", "content": turn["sql"]})

    # If there was a previous SQL error, include it so LLM can self-correct
    if error_feedback:
        user_msg = f"""Question: {question}

The previous SQL failed with this error:
{error_feedback}

Please fix the SQL and return only the corrected SQL query."""
    else:
        user_msg = f"Question: {question}"

    messages.append({"role": "user", "content": user_msg})

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type":  "application/json"
    }

    payload = {
        "model":       DEEPSEEK_MODEL,
        "messages":    messages,
        "max_tokens":  500,
        "temperature": 0.1   # low temp = more deterministic SQL
    }

    response = requests.post(DEEPSEEK_URL, headers=headers, json=payload, timeout=30)
    response.raise_for_status()

    raw_sql = response.json()["choices"][0]["message"]["content"]

    # Strip markdown code blocks if LLM adds them
    raw_sql = re.sub(r"```sql|```", "", raw_sql).strip()
    return raw_sql


def execute_sql(sql: str):
    # Remove semicolon — Hive via impyla doesn't accept it
    sql = sql.strip().rstrip(";")
    conn   = get_hive_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows    = cursor.fetchall()
        return columns, [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()


@app.get("/")
def health_check():
    return {"status": "Backend is running", "hive": HIVE_HOST}


@app.post("/query")
def query(request: QueryRequest):
    """
    Main endpoint:
    1. Send question to DeepSeek → get SQL
    2. Execute SQL on Hive
    3. If error → send error back to DeepSeek → retry once
    4. Return SQL + results
    """
    sql = None
    try:
        # Step 1: Generate SQL
        sql = generate_sql(request.question, request.history)

        # Step 2: Execute SQL
        try:
            columns, results = execute_sql(sql)
            return {
                "status":   "success",
                "question": request.question,
                "sql":      sql,
                "columns":  columns,
                "results":  results,
                "row_count": len(results)
            }

        except Exception as db_error:
            # Step 3: Self-correction — send error back to LLM
            corrected_sql = generate_sql(
                request.question,
                request.history,
                error_feedback=str(db_error)
            )

            try:
                columns, results = execute_sql(corrected_sql)
                return {
                    "status":        "corrected",
                    "question":      request.question,
                    "sql":           corrected_sql,
                    "original_sql":  sql,
                    "error":         str(db_error),
                    "columns":       columns,
                    "results":       results,
                    "row_count":     len(results)
                }
            except Exception as retry_error:
                return {
                    "status":   "error",
                    "question": request.question,
                    "sql":      corrected_sql,
                    "error":    str(retry_error)
                }

    except Exception as e:
        return {
            "status":   "error",
            "question": request.question,
            "sql":      sql,
            "error":    str(e)
        }


@app.get("/validate")
def validate_connection():
    """Test Hive connection"""
    try:
        columns, results = execute_sql("SHOW TABLES")
        return {"status": "connected", "tables": [r[list(r.keys())[0]] for r in results]}
    except Exception as e:
        return {"status": "failed", "error": str(e)}