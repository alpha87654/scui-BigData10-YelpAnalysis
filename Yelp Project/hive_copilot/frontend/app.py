# ============================================================
# MUGDHA — Requirement 3: FRONTEND
# Role: Chat UI, show SQL, tables, charts
# ============================================================

# ════════════════════════════════════════════════════════════
# STEP 1 — Install dependencies
# ════════════════════════════════════════════════════════════
# pip install streamlit requests pandas plotly

# ════════════════════════════════════════════════════════════
# STEP 2 — Create folder
# ════════════════════════════════════════════════════════════
# mkdir -p ~/hive_copilot/frontend
# cd ~/hive_copilot/frontend
# Save this file as: app.py

# ════════════════════════════════════════════════════════════
# STEP 3 — Run the frontend
# ════════════════════════════════════════════════════════════
# streamlit run app.py --server.port 8501 --server.address 0.0.0.0

# ════════════════════════════════════════════════════════════
# STEP 4 — Open in browser
# ════════════════════════════════════════════════════════════
# http://node-master:8501

# ════════════════════════════════════════════════════════════
# IMPORTANT: Rahat's backend must be running first on port 8000
# Ask Rahat to confirm: http://node-master:8000 is live
# ════════════════════════════════════════════════════════════

# ============================================================
# app.py — PASTE EVERYTHING BELOW INTO app.py
# ============================================================

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import base64
from pathlib import Path

# ── Point to Rahat's backend ──
BACKEND_URL = "http://localhost:8000"

def set_bg_from_local(image_name: str):
    img_path = Path(__file__).parent / image_name
    with open(img_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode()

    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                linear-gradient(rgba(0,0,0,0.72), rgba(0,0,0,0.78)),
                url("data:image/jpg;base64,{encoded}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
        }}

        section[data-testid="stSidebar"] {{
            background: rgba(12, 12, 18, 0.88);
            backdrop-filter: blur(6px);
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

st.set_page_config(
    page_title="Yelp Genie",
    page_icon="🧞",
    layout="wide"
)
set_bg_from_local("bg_dark.jpg")

# ── Chat bubble styles ──
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@600;700;800&display=swap');
.user-bubble {
    background: #DCF8C6;
    padding: 12px 16px;
    border-radius: 18px 18px 4px 18px;
    margin: 8px 0;
    margin-left: 20%;
    color: #FFFFFF;
    font-size: 15px;
}
.bot-bubble {
    background: #F0F0F0;
    padding: 12px 16px;
    border-radius: 18px 18px 18px 4px;
    margin: 8px 0;
    margin-right: 20%;
    color: #FFFFFF;
    font-size: 15px;
}
.sql-block {
    background: #1E1E1E;
    color: #D4D4D4;
    padding: 12px;
    border-radius: 8px;
    font-family: monospace;
    font-size: 13px;
    margin: 8px 0;
    white-space: pre-wrap;
}

/* Glass effect for content blocks */
.user-bubble, .bot-bubble, .sql-block {
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    background: rgba(255, 255, 255, 0.08) !important;
    border: 1px solid rgba(255, 255, 255, 0.14);
    box-shadow: 0 8px 30px rgba(0, 0, 0, 0.25);
    color: #FFFFFF !important;
}

/* Make the main input look cleaner */
div[data-testid="stTextInputRootElement"] input {
    background: rgba(255, 255, 255, 0.08) !important;
    color: white !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 14px !important;
}

/* Nice hover effect for all buttons */
.stButton > button {
    border-radius: 14px !important;
    transition: all 0.25s ease !important;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.20);
}

.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 10px 24px rgba(0, 0, 0, 0.28);
}

.hero-title {
    font-family: 'Orbitron', sans-serif;
    font-size: 58px;
    font-weight: 800;
    color: white;
    margin-top: 10px;
    margin-bottom: 8px;
    text-shadow: 0 0 18px rgba(255,255,255,0.12);
    animation: floatTitle 3s ease-in-out infinite;
}

.hero-subtitle {
    font-size: 20px;
    color: rgba(255,255,255,0.78);
    margin-bottom: 30px;
}

@keyframes floatTitle {
    0%   { transform: translateY(0px); }
    50%  { transform: translateY(-4px); }
    100% { transform: translateY(0px); }
}

.credit-badge {
    position: fixed;
    top: 88px;
    right: 72px;
    padding: 0;
    background: transparent;
    border: none;
    box-shadow: none;
    backdrop-filter: none;
    -webkit-backdrop-filter: none;
    color: rgba(255, 255, 255, 0.72);
    font-size: 13px;
    font-weight: 500;
    letter-spacing: 0.5px;
    z-index: 9999;
    pointer-events: none;
}

.credit-badge span {
    color: rgba(255, 90, 95, 0.92);
    font-weight: 700;
}
.thinking-bubble {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: white !important;
    font-size: 15px;
    margin-right: 20%;
}

.dot-anim {
    display: inline-flex;
    align-items: center;
}

.dot-anim span {
    display: inline-block;
    margin-left: 2px;
    animation: thinkingDots 1.4s infinite;
    opacity: 0.25;
}

.dot-anim span:nth-child(2) {
    animation-delay: 0.2s;
}

.dot-anim span:nth-child(3) {
    animation-delay: 0.4s;
}

@keyframes thinkingDots {
    0%, 80%, 100% {
        opacity: 0.25;
        transform: translateY(0);
    }
    40% {
        opacity: 1;
        transform: translateY(-2px);
    }
}
</style>
""", unsafe_allow_html=True)

# ── Session state ──
if "messages" not in st.session_state:
    st.session_state.messages = []
if "history"  not in st.session_state:
    st.session_state.history  = []
if "last_df"  not in st.session_state:
    st.session_state.last_df  = None


def call_backend(question: str, history: list) -> dict:
    """Send question to Rahat's FastAPI backend"""
    try:
        resp = requests.post(
            f"{BACKEND_URL}/query",
            json={"question": question, "history": history},
            timeout=120
        )
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {
            "status": "error",
            "error":  "Cannot reach backend. Ask Rahat to start the server!"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def auto_chart(df: pd.DataFrame, question: str):
    """Pick best chart type based on the question and data"""
    if df is None or df.empty or len(df.columns) < 2:
        return

    num_cols = df.select_dtypes(include="number").columns.tolist()
    str_cols = df.select_dtypes(include="object").columns.tolist()

    if not num_cols:
        return

    x = str_cols[0] if str_cols else df.columns[0]
    y = num_cols[0]
    q = question.lower()

    try:
        if any(w in q for w in ["distribution", "proportion", "percentage", "ratio"]):
            fig = px.pie(df.head(10), names=x, values=y,
                         title=f"Distribution of {y}")
        elif any(w in q for w in ["year", "month", "trend", "over time", "growth"]):
            fig = px.line(df, x=x, y=y,
                          title=f"{y} over {x}", markers=True)
        else:
            fig = px.bar(df.head(20), x=x, y=y,
                         title=f"{y} by {x}",
                         color=y, color_continuous_scale="Blues")
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        pass


# ── Header ──
st.markdown("""
<div class="hero-title">🧞 Yelp Genie</div>
<div class="hero-subtitle">Ask your Yelp data wishes, we'll do the magic✨</div>
""", unsafe_allow_html=True)

st.markdown(
    '<div class="credit-badge">Brought to you by <span>SCUIB10</span></div>',
    unsafe_allow_html=True
)

# ── Sidebar ──
with st.sidebar:
    st.header("Quick Info")
    st.markdown("""
    **Example questions to try:**
    - Top 5 Mexican restaurants in Philadelphia
    - Which city has the most businesses?
    - Show reviews per year
    - Top 10 users with most fans
    - Most popular check-in cities
    - Rating distribution 1 to 5 stars
    """)
    st.divider()

    # Test backend connection
    if st.button("Test Backend Connection"):
        with st.spinner("Checking..."):
            try:
                resp = requests.get(f"{BACKEND_URL}/validate", timeout=10)
                data = resp.json()
                if data["status"] == "connected":
                    st.success(f"Connected! Tables: {', '.join(data['tables'])}")
                else:
                    st.error(f"Failed: {data['error']}")
            except Exception as e:
                st.error(f"Backend unreachable: {e}")

    st.divider()

    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.session_state.history  = []
        st.session_state.last_df  = None
        st.rerun()

    if st.session_state.last_df is not None:
        csv = st.session_state.last_df.to_csv(index=False)
        st.download_button(
            "Download Last Result (CSV)",
            csv,
            "yelp_result.csv",
            "text/csv"
        )

# ── Chat history ──
for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(
            f'<div class="user-bubble">👤 {msg["content"]}</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f'<div class="bot-bubble">🤖 {msg["content"]}</div>',
            unsafe_allow_html=True
        )
        if "sql" in msg:
            st.markdown(
                f'<div class="sql-block">📋 Generated SQL:\n\n{msg["sql"]}</div>',
                unsafe_allow_html=True
            )
        if "df" in msg and msg["df"] is not None:
            st.dataframe(msg["df"], use_container_width=True)
            auto_chart(msg["df"], msg.get("question", ""))

# ── Input ──
st.divider()
col1, col2 = st.columns([5, 1])
with col1:
    user_input = st.text_input(
        "Ask about Yelp data...",
        placeholder="e.g. Top 10 cities with most restaurants",
        label_visibility="collapsed",
        key="input"
    )
with col2:
    send = st.button("Send", type="primary", use_container_width=True)

# ── Example buttons ──
st.markdown("**Quick examples:**")
c1, c2, c3 = st.columns(3)
examples = [
    "Top 5 Mexican restaurants in Philadelphia",
    "Which 10 cities have the most businesses?",
    "Show rating distribution 1 to 5 stars",
    "Top 10 users with most reviews",
    "Most popular cities for check-ins",
    "How many reviews were written each year?"
]
for i, ex in enumerate(examples):
    if [c1, c2, c3][i % 3].button(ex, use_container_width=True):
        user_input = ex
        send = True

# ── Handle send ──
if send and user_input.strip():
    question = user_input.strip()
    st.session_state.messages.append({"role": "user", "content": question})

    thinking_placeholder = st.empty()
    thinking_placeholder.markdown(
        """
        <div class="bot-bubble thinking-bubble">
            🍜 Yelp Genie is thinking
            <span class="dot-anim"><span>.</span><span>.</span><span>.</span></span>
        </div>
        """,
        unsafe_allow_html=True
    )

    try:
        with st.spinner("Analyzing Yelp data..."):
            result = call_backend(question, st.session_state.history)

        thinking_placeholder.empty()

        if result["status"] in ("success", "corrected"):
            df       = pd.DataFrame(result["results"])
            sql      = result["sql"]
            rows     = result["row_count"]
            note     = " *(auto-corrected)*" if result["status"] == "corrected" else ""
            response = f"Found **{rows}** results{note}"

            st.session_state.last_df = df
            st.session_state.history.append({"question": question, "sql": sql})
            st.session_state.messages.append({
                "role":     "assistant",
                "content":  response,
                "sql":      sql,
                "df":       df,
                "question": question
            })
        else:
            st.session_state.messages.append({
                "role":    "assistant",
                "content": f"Error: {result.get('error', 'Unknown error')}",
                "sql":     result.get("sql", "")
            })

    except Exception as e:
        thinking_placeholder.empty()
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"Error: {e}",
            "sql": ""
        })

    st.rerun()