import streamlit as st
import base64
from pathlib import Path

def set_bg_from_local(image_name: str):
    img_path = Path(__file__).parent.parent / image_name
    with open(img_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode()

    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                linear-gradient(rgba(0,0,0,0.72), rgba(0,0,0,0.80)),
                url("data:image/jpg;base64,{encoded}");
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
        }}

        section[data-testid="stSidebar"] {{
            background: rgba(12, 12, 18, 0.88);
            backdrop-filter: blur(6px);
        }}

        .page-title {{
            font-size: 48px;
            font-weight: 800;
            color: white;
            margin-bottom: 10px;
        }}

        .page-subtitle {{
            font-size: 18px;
            color: rgba(255,255,255,0.80);
            margin-bottom: 28px;
        }}

        .option-card {{
            padding: 28px;
            border-radius: 20px;
            background: rgba(255,255,255,0.07);
            border: 1px solid rgba(255,255,255,0.12);
            backdrop-filter: blur(10px);
            color: white;
            min-height: 220px;
        }}

        .option-card h3 {{
            margin-top: 0;
            color: white;
        }}

        .option-card p {{
            color: rgba(255,255,255,0.82);
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

st.set_page_config(page_title="Analytics | Yelp Genie", page_icon="📊", layout="wide")
set_bg_from_local("bg_dark.jpg")

st.markdown('<div class="page-title">📊 Analytics</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="page-subtitle">Choose which analytics section you want to explore.</div>',
    unsafe_allow_html=True
)

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="option-card">
        <h3>📈 Data Analysis and Visualization</h3>
        <p>Explore business insights, trends, distributions, comparisons, and visual summaries from the Yelp dataset.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Open Data Analysis and Visualization", use_container_width=True):
        st.session_state["analytics_section"] = "analysis"

with col2:
    st.markdown("""
    <div class="option-card">
        <h3>🧪 Data Enrichment and Exploration</h3>
        <p>Explore enriched business information, cursed storefronts, golden locations, attribute patterns, and external validation insights.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Open Data Enrichment and Exploration", use_container_width=True):
        st.session_state["analytics_section"] = "enrichment"

st.markdown("<br>", unsafe_allow_html=True)

section = st.session_state.get("analytics_section", None)

if section == "analysis":
    st.success("You selected: Data Analysis and Visualization")
    st.markdown("""
    ### Suggested content for this section
    - Top categories
    - Top cities
    - Rating distributions
    - Review trends over time
    - Check-in popularity
    - User activity analysis
    """)

elif section == "enrichment":
    st.success("You selected: Data Enrichment and Exploration")
    st.markdown("""
    ### Suggested content for this section
    - Cursed storefronts
    - Golden locations
    - Attribute comparison
    - Complaint keyword analysis
    - External validation summary
    - Multi-dimensional attribution results
    """)