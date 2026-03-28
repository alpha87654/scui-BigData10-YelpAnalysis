# ============================================================
# run_backend.py — Start the FastAPI backend
# File: hive_copilot/backend/run_backend.py
# ============================================================
# Run this file in PyCharm by right-clicking → Run
# OR in terminal: python run_backend.py
# ============================================================

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )