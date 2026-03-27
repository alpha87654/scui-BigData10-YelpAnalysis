from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Optional
import pandas as pd
from pyhive import hive

# Import your LLM SQL generator
from faria_llm_integration import generate_sql

app = FastAPI(title="Yelp Text-to-SQL Backend")

HIVE_HOST = "node-master"
HIVE_PORT = 10000
HIVE_USERNAME = "faria"
HIVE_DATABASE = "yelp"


def get_hive_connection():
    return hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        username=HIVE_USERNAME,
        database=HIVE_DATABASE,
    )


class QueryRequest(BaseModel):
    question: str
    history: Optional[List[Dict]] = []


@app.get("/")
def root():
    return {"message": "Yelp backend is running"}


@app.get("/validate")
def validate():
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        rows = cursor.fetchall()
        tables = [r[0] for r in rows]
        cursor.close()
        conn.close()
        return {"status": "connected", "tables": tables}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def run_sql(sql: str):
    conn = get_hive_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    cursor.close()
    conn.close()

    df = pd.DataFrame(rows, columns=columns)
    return df


@app.post("/query")
def query_data(request: QueryRequest):
    question = request.question.strip()
    history = request.history or []

    try:
        sql = generate_sql(question=question, history=history)
        df = run_sql(sql)

        return {
            "status": "success",
            "question": question,
            "sql": sql,
            "row_count": len(df),
            "results": df.fillna("").to_dict(orient="records"),
        }

    except Exception as first_error:
        try:
            sql = generate_sql(question=question, history=history, error=str(first_error))
            df = run_sql(sql)
            return {
                "status": "corrected",
                "question": question,
                "sql": sql,
                "row_count": len(df),
                "results": df.fillna("").to_dict(orient="records"),
                "error": str(first_error),
            }
        except Exception as second_error:
            return {
                "status": "error",
                "question": question,
                "sql": sql if 'sql' in locals() else "",
                "error": str(second_error),
            }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
