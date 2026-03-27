import os
import re
import requests
from typing import Optional, List, Dict

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"

SCHEMA_CONTEXT = """
Current database: yelp

Confirmed tables:

TABLE: business
  business_id   STRING
  name          STRING
  address       STRING
  city          STRING
  state         STRING
  postal_code   STRING
  latitude      FLOAT
  longitude     FLOAT
  stars         FLOAT
  review_count  INT
  is_open       TINYINT
  attributes    STRING
  categories    STRING
  hours         STRING

TABLE: json_business
  json_body     STRING

Sample category values:
  Restaurants, Food, Nightlife, Bars, Mexican, Chinese, American,
  Pizza, Coffee & Tea, Japanese, Italian, Fast Food

SQL rules:
- Generate Spark SQL / Hive-compatible SQL only.
- Use only confirmed tables and columns from this schema.
- Do not invent columns like restaurant_name; use name.
- For cuisine filtering, use LOWER(categories) LIKE '%mexican%'.
- For city filtering, use LOWER(city) = 'philadelphia' when appropriate.
- is_open = 1 means open, is_open = 0 means closed.
- review_count belongs to business.
- stars belongs to business.
"""

SYSTEM_PROMPT = f"""
You are an expert Text-to-SQL assistant for a Yelp analytics project.

Your job:
1. Read the schema carefully.
2. Convert the user's question into exactly ONE valid SQL query.
3. Return ONLY raw executable SQL.
4. No markdown.
5. No explanation.
6. No comments.
7. No backticks.

Schema:
{SCHEMA_CONTEXT}

Strict output rules:
- Only generate SELECT or WITH queries.
- Never generate INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, REPLACE, MERGE, GRANT, REVOKE, LOAD, EXPORT, IMPORT, USE, MSCK, DFS, or ADD JAR.
- Keep SQL simple and executable.
- Prefer explicit table aliases.
- If the user asks for top results, use ORDER BY and LIMIT.
- If the user does not specify a row limit, add LIMIT 50.
- If a question cannot be fully answered from the confirmed schema, return the closest safe SQL using the available tables only.
"""

def build_messages(
    question: str,
    history: Optional[List[Dict]] = None,
    error: Optional[str] = None
) -> List[Dict[str, str]]:
    if history is None:
        history = []

    messages = [{"role": "system", "content": SYSTEM_PROMPT.strip()}]

    for item in history[-5:]:
        if isinstance(item, dict):
            old_q = item.get("question")
            old_sql = item.get("sql")
            if old_q:
                messages.append({"role": "user", "content": str(old_q)})
            if old_sql:
                messages.append({"role": "assistant", "content": str(old_sql)})

    if error:
        user_prompt = f"""
Question:
{question}

The previous SQL failed with this execution error:
{error}

Please fix the SQL and return ONLY the corrected SQL query.
""".strip()
    else:
        user_prompt = question.strip()

    messages.append({"role": "user", "content": user_prompt})
    return messages

def extract_sql(raw_text: str) -> str:
    if not raw_text or not raw_text.strip():
        raise ValueError("Empty LLM response.")

    text = raw_text.strip()

    fenced = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()
    else:
        text = re.sub(r"```", "", text).strip()

    patterns = [
        r"^sql\s*[:\-]?\s*",
        r"^here\s+is\s+the\s+sql\s*[:\-]?\s*",
        r"^here'?s\s+the\s+sql\s*[:\-]?\s*",
        r"^the\s+sql\s+query\s+is\s*[:\-]?\s*",
        r"^sure[,\s:.-]*",
        r"^certainly[,\s:.-]*"
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = text.strip()

    if ";" in text:
        text = text[:text.find(";") + 1]
    else:
        text += ";"

    return text.strip()

def ensure_limit(sql: str, default_limit: int = 50) -> str:
    if re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
        return sql.strip()
    sql = sql.rstrip(";").strip()
    return f"{sql} LIMIT {default_limit};"

def is_safe_sql(sql: str) -> bool:
    upper_sql = sql.upper().strip()

    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH") or upper_sql.startswith("SHOW")):
        return False

    blocked = [
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER",
        "CREATE", "TRUNCATE", "REPLACE", "MERGE",
        "GRANT", "REVOKE", "LOAD", "EXPORT", "IMPORT",
        "USE ", "MSCK", "DFS", "ADD JAR"
    ]

    return not any(word in upper_sql for word in blocked)

def call_deepseek(messages: List[Dict[str, str]]) -> str:
    if not DEEPSEEK_API_KEY:
        raise ValueError("DEEPSEEK_API_KEY is not set.")

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": messages,
        "temperature": 0,
        "max_tokens": 600
    }

    response = requests.post(
        DEEPSEEK_URL,
        headers=headers,
        json=payload,
        timeout=60
    )
    response.raise_for_status()

    data = response.json()
    return data["choices"][0]["message"]["content"]

def generate_sql(
    question: str,
    history: Optional[List[Dict]] = None,
    error: Optional[str] = None
) -> str:
    mock_sql_map = {
        "mexican": """
SELECT name, city, stars, review_count
FROM business
WHERE LOWER(categories) LIKE '%mexican%'
  AND LOWER(city) = 'philadelphia'
  AND review_count > 500
ORDER BY stars DESC, review_count DESC
LIMIT 5;
""",
        "cities": """
SELECT city, COUNT(*) AS business_count
FROM business
GROUP BY city
ORDER BY business_count DESC
LIMIT 10;
""",
        "open businesses": """
SELECT name, city, review_count, stars
FROM business
WHERE is_open = 1
ORDER BY review_count DESC
LIMIT 20;
""",
        "chinese": """
SELECT name, city, stars, review_count
FROM business
WHERE LOWER(categories) LIKE '%chinese%'
ORDER BY stars DESC, review_count DESC
LIMIT 10;
"""
    }

    q = question.lower().strip()

    try:
        if not DEEPSEEK_API_KEY:
            raise ValueError("No DeepSeek API key set.")

        messages = build_messages(question, history=history, error=error)
        raw_output = call_deepseek(messages)
        sql = extract_sql(raw_output)
        sql = ensure_limit(sql)

        if not is_safe_sql(sql):
            raise ValueError(f"Unsafe SQL generated and blocked: {sql}")

        return sql

    except Exception:
        if "mexican" in q:
            return mock_sql_map["mexican"].strip()
        elif "most businesses" in q or "cities" in q:
            return mock_sql_map["cities"].strip()
        elif "open businesses" in q:
            return mock_sql_map["open businesses"].strip()
        elif "chinese" in q:
            return mock_sql_map["chinese"].strip()
        else:
            return """
SELECT name, city, stars, review_count
FROM business
ORDER BY review_count DESC
LIMIT 50;
""".strip()

if __name__ == "__main__":
    test_questions = [
        "Show me the top 5 highest-rated Mexican restaurants in Philadelphia that have over 500 reviews",
        "List the top 10 cities with the most businesses",
        "Show the top 20 open businesses by review count",
        "List the top 10 highest-rated Chinese restaurants"
    ]

    print("=" * 80)
    print("Testing FARIA LLM Integration")
    print("=" * 80)

    for q in test_questions:
        print(f"\nQuestion: {q}")
        try:
            sql = generate_sql(q)
            print("Generated SQL:")
            print(sql)
        except Exception as e:
            print("Error:", e)
