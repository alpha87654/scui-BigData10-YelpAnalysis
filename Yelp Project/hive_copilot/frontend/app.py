import streamlit as st
import base64
from pathlib import Path

# ---------- helpers ----------
BASE_DIR = Path(__file__).parent

def get_base64_image(image_path: Path):
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def set_bg_from_local(image_name: str):
    img_path = BASE_DIR / image_name
    encoded = get_base64_image(img_path)

    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                linear-gradient(rgba(0,0,0,0.74), rgba(0,0,0,0.82)),
                url("data:image/jpg;base64,{encoded}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
        }}

        section[data-testid="stSidebar"] {{
            background: rgba(12, 12, 18, 0.88);
            backdrop-filter: blur(6px);
        }}

        header[data-testid="stHeader"] {{
            background: rgba(0,0,0,0);
        }}

        .block-container {{
            padding-top: 1.8rem !important;
            padding-bottom: 2rem !important;
            max-width: 1400px;
        }}

        .top-line {{
            height: 22px;
            border-radius: 999px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.06);
            margin-bottom: 26px;
        }}

        .topbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 14px;
            margin-bottom: 22px;
            flex-wrap: wrap;
        }}

        .brand {{
            display: flex;
            align-items: center;
            gap: 12px;
            color: white;
            font-size: 26px;
            font-weight: 700;
        }}

        .nav-wrap {{
            display: flex;
            justify-content: center;
            gap: 14px;
            flex-wrap: wrap;
        }}

        .nav-btn {{
            display: inline-block;
            padding: 8px 18px;
            border-radius: 14px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.12);
            color: white;
            text-decoration: none;
            font-size: 16px;
            font-weight: 500;
            text-align: center;
            backdrop-filter: blur(10px);
            transition: all 0.25s ease;
        }}

        .nav-btn:hover {{
            background: rgba(255,255,255,0.10);
            transform: translateY(-1px);
        }}

        .credit-badge {{
            color: rgba(255,255,255,0.78);
            font-size: 15px;
            font-weight: 500;
            white-space: nowrap;
        }}

        .credit-badge span {{
            color: rgba(255, 90, 95, 0.95);
            font-weight: 700;
        }}

        .hero-box {{
            padding: 34px 26px 30px 26px;
            border-radius: 28px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.12);
            backdrop-filter: blur(12px);
            text-align: center;
            margin-bottom: 22px;
        }}

        .hero-title {{
            font-size: 64px;
            font-weight: 800;
            color: white;
            margin-bottom: 10px;
            line-height: 1.05;
        }}

        .hero-subtitle {{
            font-size: 20px;
            color: rgba(255,255,255,0.84);
            margin-bottom: 0;
        }}

        .card {{
            padding: 24px 24px;
            border-radius: 24px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.12);
            backdrop-filter: blur(12px);
            color: white;
            min-height: 260px;
        }}

        .card-icon {{
            width: 58px;
            height: 58px;
            object-fit: contain;
            margin-bottom: 14px;
        }}

        .card h3 {{
            margin-top: 0;
            margin-bottom: 14px;
            color: white;
            font-size: 24px;
        }}

        .card p, .card li {{
            color: rgba(255,255,255,0.84);
            font-size: 15px;
            line-height: 1.65;
        }}

        .card ul {{
            padding-left: 20px;
            margin-top: 14px;
            margin-bottom: 0;
        }}

        .open-btn {{
            display: inline-block;
            margin-top: 16px;
            padding: 9px 16px;
            border-radius: 12px;
            background: rgba(255,255,255,0.07);
            border: 1px solid rgba(255,255,255,0.14);
            color: white;
            text-decoration: none;
            font-weight: 600;
            font-size: 15px;
            transition: all 0.25s ease;
        }}

        .open-btn:hover {{
            background: rgba(255,255,255,0.12);
            transform: translateY(-1px);
        }}

        .home-card {{
            min-height: 260px;
        }}

        @media (max-width: 900px) {{
            .hero-title {{
                font-size: 52px;
            }}

            .hero-subtitle {{
                font-size: 18px;
            }}

            .topbar {{
                justify-content: center;
            }}

            .credit-badge {{
                text-align: center;
                width: 100%;
            }}
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# ---------- page config ----------
st.set_page_config(
    page_title="Yelp Genie",
    page_icon="🧞",
    layout="wide"
)

set_bg_from_local("bg_dark.jpg")

analysis_icon = get_base64_image(BASE_DIR / "analysis.png")
chat_icon = get_base64_image(BASE_DIR / "chat.png")

# ---------- top area ----------
st.markdown('<div class="top-line"></div>', unsafe_allow_html=True)

st.markdown(
    """
    <div class="topbar">
        <div class="brand">🧞 Yelp Genie</div>
        <div class="nav-wrap">
            <a class="nav-btn" href="/">Home</a>
            <a class="nav-btn" href="/Analytics" target="_self">Analytics</a>
            <a class="nav-btn" href="/Chatbot" target="_self">Chatbot</a>
        </div>
        <div class="credit-badge">Brought to you by <span>SCUIB10</span></div>
    </div>
    """,
    unsafe_allow_html=True
)

# ---------- hero ----------
st.markdown(
    """
    <div class="hero-box">
        <div class="hero-title">🧞 Yelp Genie</div>
        <div class="hero-subtitle">
            Explore Yelp insights, analytics, and conversational querying in one place.
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# ---------- cards ----------
col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown(
        f"""
        <div class="card home-card">
            <img class="card-icon" src="data:image/png;base64,{analysis_icon}" />
            <h3>Analytics</h3>
            <p>Open the analytics page to access:</p>
            <ul>
                <li>Data Analysis and Visualization</li>
                <li>Data Enrichment and Exploration</li>
            </ul>
            <a class="open-btn" href="/Analytics" target="_self">Open Analytics</a>
        </div>
        """,
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        f"""
        <div class="card home-card">
            <img class="card-icon" src="data:image/png;base64,{chat_icon}" />
            <h3>Chatbot</h3>
            <p>
                Ask questions in natural language and get SQL, tables, and charts
                from the Yelp dataset.
            </p>
            <p>
                Interact with your project like a real conversational assistant.
            </p>
            <a class="open-btn" href="/Chatbot" target="_self">Open Chatbot</a>
        </div>
        """,
        unsafe_allow_html=True
    )