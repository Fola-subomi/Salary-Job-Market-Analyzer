import os
import pickle
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────
# Page config
# ─────────────────────────────────────────

st.set_page_config(
    page_title="Nigerian Job Market Analyzer",
    page_icon="🇳🇬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────
# Theme
# ─────────────────────────────────────────

st.markdown("""
<style>
/* 1. Added Nunito to the import list */
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@700;800&family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0a2e1a;
    border-right: 1px solid #1a4a2a;
}
[data-testid="stSidebar"] * {
    color: #e8f5e9 !important;
}

/* Headers - Keeping Syne for that artistic punch */
h1, h2, h3 {
    font-family: 'Syne', sans-serif !important;
    color: #0a2e1a !important;
}

/* Metric cards - Swapping to Nunito (Nudo-style) for the digits */
[data-testid="stMetricValue"] {
    color: #0a2e1a !important;
    font-family: 'Nunito', sans-serif !important; /* Updated font */
    font-size: 32px !important; /* Slightly larger for geometric impact */
    font-weight: 800 !important;
}

[data-testid="stMetricLabel"] {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 13px !important;
    color: #5a7a62 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* Metric container styling */
[data-testid="stMetric"] {
    background: white;
    border: 1px solid #e0ebe2;
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 2px 8px rgba(10,46,26,0.06);
}

/* Update the band-label for your Salary Predictor results */
.band-label {
    font-family: 'Nunito', sans-serif; /* Updated font */
    font-size: 36px;
    font-weight: 800;
    color: #1a6b35;
    margin: 12px 0;
}

/* Button */
.stButton > button {
    background: #1a6b35 !important;
    color: white !important;              /* This forces the text to white */
    border: none !important;
    border-radius: 8px !important;
    padding: 12px 32px !important;
    font-family: 'Syne', sans-serif !important;
    font-weight: 600 !important;
    font-size: 15px !important;
    width: 100% !important;
    transition: background 0.2s !important;
}

.stButton > button:hover {
    background: #0a2e1a !important;
    color: white !important;              /* Ensures it stays white on hover */
}
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Data & model loading
# ─────────────────────────────────────────

@st.cache_data
def load_data():
    df = pd.read_csv("data/clean/jobs_clean.csv")
    return df


@st.cache_resource
def load_model():
    model_path   = "model/salary_model.pkl"
    encoder_path = "model/encoders.pkl"
    meta_path    = "model/model_meta.pkl"

    if not all(os.path.exists(p) for p in [model_path, encoder_path, meta_path]):
        return None, None, None

    with open(model_path,   "rb") as f: model    = pickle.load(f)
    with open(encoder_path, "rb") as f: encoders = pickle.load(f)
    with open(meta_path,    "rb") as f: meta     = pickle.load(f)
    return model, encoders, meta


def predict_band(model, encoders, role_group, city, job_type, sector):
    row = {}
    for col, value in zip(
        ["role_group", "city", "job_type", "sector"],
        [role_group, city, job_type, sector]
    ):
        le = encoders[col]
        row[col] = le.transform([value])[0] if value in le.classes_ else 0

    X = pd.DataFrame([row])
    pred_encoded = model.predict(X)[0]
    pred_proba   = model.predict_proba(X)[0]
    band_label   = encoders["salary_band"].inverse_transform([pred_encoded])[0]
    confidence   = pred_proba.max()
    return band_label, confidence


ROLE_GROUPS = [
    "Engineering & Tech", "Sales & Marketing", "Finance & Accounting",
    "Management", "Human Resources", "Operations & Logistics",
    "Professional Services", "Education & Training", "Customer Service",
    "Admin & Secretarial", "Creative & Design", "NGO & Humanitarian",
    "Construction & Estate", "Other"
]

CITIES     = ["Lagos", "Abuja", "Port Harcourt", "Kano", "Ibadan", "Remote", "Other States"]
JOB_TYPES  = ["Full Time", "Part Time", "Contract", "Other"]
GREEN      = "#1a6b35"
LIGHT_GREEN = "#e8f5e9"
COLORS     = ["#1a6b35", "#2d9e56", "#52c47a", "#86dba0", "#b8f0c8"]


# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='padding: 8px 0 24px'>
        <div style='font-family:Syne,sans-serif;font-size:20px;font-weight:800;
                    color:#52c47a;letter-spacing:-0.5px'>🇳🇬 JobMarket.ng</div>
        <div style='font-size:12px;color:#7aaa82;margin-top:4px'>
            Nigerian Job Market Analyzer
        </div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["📊  Dashboard", "💰  Salary Predictor"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("""
    <div style='font-size:12px;color:#7aaa82;line-height:1.6'>
        <b style='color:#b8f0c8'>Data sources</b><br>
        Jobberman · MyJobMag<br>
        HotNigerianJobs · NgoJobSite<br><br>
        <b style='color:#b8f0c8'>Model</b><br>
        Random Forest Classifier<br>
        Accuracy: 67%
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────
# Load
# ─────────────────────────────────────────

df = load_data()
model, encoders, meta = load_model()


# ─────────────────────────────────────────
# Page 1 — Dashboard
# ─────────────────────────────────────────

if page == "📊  Dashboard":

    st.markdown("<h1 style='margin-bottom:4px'>Nigerian Job Market</h1>", unsafe_allow_html=True)
    st.markdown("<p style='color:#7a9a82;font-size:15px;margin-bottom:28px'>Real-time insights from Nigerian job boards</p>", unsafe_allow_html=True)

    # ── Filters ──
    col1, col2, col3 = st.columns(3)
    with col1:
        loc_filter = st.selectbox("Location", ["All"] + sorted(df["location"].dropna().unique().tolist()))
    with col2:
        src_filter = st.selectbox("Source", ["All"] + sorted(df["source"].dropna().unique().tolist()))
    with col3:
        sec_filter = st.selectbox("Sector", ["All"] + sorted(df["sector"].dropna().unique().tolist()))

    # Apply filters
    filtered = df.copy()
    if loc_filter != "All":
        filtered = filtered[filtered["location"] == loc_filter]
    if src_filter != "All":
        filtered = filtered[filtered["source"] == src_filter]
    if sec_filter != "All":
        filtered = filtered[filtered["sector"] == sec_filter]

    st.markdown("---")

    # ── Metrics ──
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Listings", f"{len(filtered):,}")
    with m2:
        st.metric("Unique Companies", f"{filtered['company'].nunique():,}")
    with m3:
        has_salary = filtered["avg_salary"].notna().sum()
        st.metric("With Salary Data", f"{has_salary:,}")
    with m4:
        avg = filtered["avg_salary"].mean()
        st.metric("Avg Salary", f"₦{avg:,.0f}" if pd.notna(avg) else "N/A")

    st.markdown("---")

    # ── Charts row 1 ──
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="section-title">Top Sectors</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Most active hiring sectors</div>', unsafe_allow_html=True)
        sector_counts = (
            filtered["sector"]
            .dropna()
            .value_counts()
            .head(10)
            .reset_index()
        )
        sector_counts.columns = ["sector", "count"]
        fig = px.bar(
            sector_counts, x="count", y="sector",
            orientation="h",
            color="count",
            color_continuous_scale=["#b8f0c8", "#1a6b35"],
        )
        fig.update_layout(
            showlegend=False, coloraxis_showscale=False,
            plot_bgcolor="white", paper_bgcolor="white",
            margin=dict(l=0, r=0, t=0, b=0),
            xaxis_title="", yaxis_title="",
            font=dict(family="DM Sans"),
            height=340,
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('<div class="section-title">Jobs by Location</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Distribution across Nigerian cities</div>', unsafe_allow_html=True)
        loc_counts = (
            filtered["location"]
            .dropna()
            .value_counts()
            .head(8)
            .reset_index()
        )
        loc_counts.columns = ["location", "count"]
        fig2 = px.pie(
            loc_counts, values="count", names="location",
            color_discrete_sequence=COLORS,
            hole=0.45,
        )
        fig2.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            margin=dict(l=0, r=0, t=0, b=0),
            font=dict(family="DM Sans"),
            height=340,
            legend=dict(orientation="v", x=1, y=0.5),
        )
        fig2.update_traces(textposition="inside", textinfo="percent")
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ── Charts row 2 ──
    c3, c4 = st.columns(2)

    with c3:
        st.markdown('<div class="section-title">Salary Distribution</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Monthly salary ranges (Jobberman data)</div>', unsafe_allow_html=True)
        sal_df = filtered[filtered["avg_salary"].notna()]
        if len(sal_df) > 0:
            fig3 = px.histogram(
                sal_df, x="avg_salary",
                nbins=30,
                color_discrete_sequence=[GREEN],
            )
            fig3.update_layout(
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=0, t=0, b=0),
                xaxis_title="Monthly Salary (₦)",
                yaxis_title="Number of Jobs",
                font=dict(family="DM Sans"),
                height=300,
            )
            fig3.update_xaxes(tickformat=",.0f")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No salary data for this filter combination.")

    with c4:
        st.markdown('<div class="section-title">Avg Salary by Sector</div>', unsafe_allow_html=True)
        st.markdown('<div class="section-sub">Where the money is</div>', unsafe_allow_html=True)
        sal_sector = (
            filtered[filtered["avg_salary"].notna()]
            .groupby("sector")["avg_salary"]
            .mean()
            .sort_values(ascending=False)
            .head(8)
            .reset_index()
        )
        sal_sector.columns = ["sector", "avg_salary"]
        if len(sal_sector) > 0:
            fig4 = px.bar(
                sal_sector, x="avg_salary", y="sector",
                orientation="h",
                color="avg_salary",
                color_continuous_scale=["#b8f0c8", "#1a6b35"],
            )
            fig4.update_layout(
                showlegend=False, coloraxis_showscale=False,
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=0, t=0, b=0),
                xaxis_title="Avg Monthly Salary (₦)",
                yaxis_title="",
                font=dict(family="DM Sans"),
                height=300,
            )
            fig4.update_xaxes(tickformat=",.0f")
            fig4.update_yaxes(autorange="reversed")
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No salary data for this filter combination.")

    st.markdown("---")

    # ── Data table ──
    st.markdown('<div class="section-title">Browse Listings</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Raw job data from all sources</div>', unsafe_allow_html=True)
    display_cols = ["title", "company", "location", "sector", "avg_salary", "source"]
    table = filtered[display_cols].copy()
    table["avg_salary"] = table["avg_salary"].apply(
        lambda x: f"₦{x:,.0f}" if pd.notna(x) else "—"
    )
    table.columns = ["Title", "Company", "Location", "Sector", "Avg Salary", "Source"]
    st.dataframe(table, use_container_width=True, height=320)


# ─────────────────────────────────────────
# Page 2 — Salary Predictor
# ─────────────────────────────────────────

elif page == "💰  Salary Predictor":

    st.markdown("<h1 style='margin-bottom:4px'>Salary Predictor</h1>", unsafe_allow_html=True)
    st.markdown("<p style='color:#7a9a82;font-size:15px;margin-bottom:28px'>Find out what salary band your role falls into in the Nigerian market</p>", unsafe_allow_html=True)

    if model is None:
        st.warning("Model not found. Run `python model/train.py` first.")
    else:
        col_form, col_result = st.columns([1, 1], gap="large")

        with col_form:
            st.markdown('<div class="section-title">Your Details</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Fill in your role information</div>', unsafe_allow_html=True)

            role_group = st.selectbox("Role / Function", ROLE_GROUPS)
            city       = st.selectbox("City", CITIES)
            job_type   = st.selectbox("Job Type", JOB_TYPES)

            # Get sectors from data
            sectors = sorted(df["sector"].dropna().unique().tolist())
            sector  = st.selectbox("Sector", sectors)

            st.markdown("<br>", unsafe_allow_html=True)
            predict_btn = st.button("Predict My Salary Band")

        with col_result:
            st.markdown('<div class="section-title">Result</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-sub">Based on real Nigerian job market data</div>', unsafe_allow_html=True)

            if predict_btn:
                band, confidence = predict_band(model, encoders, role_group, city, job_type, sector)

                # Band to naira range map
                band_ranges = {
                    "Entry (≤₦150k)":    "Up to ₦150,000 / month",
                    "Mid (₦150k–300k)":  "₦150,000 – ₦300,000 / month",
                    "Senior (₦300k+)":   "₦300,000+ / month",
                }
                naira_range = band_ranges.get(band, "")

                st.markdown(f"""
                <div class="result-card">
                    <div style='font-size:13px;color:#7a9a82;text-transform:uppercase;
                                letter-spacing:0.08em'>Predicted Salary Band</div>
                    <div class="band-label">{band}</div>
                    <div style='font-size:20px;color:#0a2e1a;font-weight:500;
                                margin:8px 0'>{naira_range}</div>
                    <hr style='border-color:#e0ebe2;margin:16px 0'>
                    <div class="confidence-label">
                        Model confidence: <b style='color:#1a6b35'>{confidence:.0%}</b>
                    </div>
                    <div style='font-size:12px;color:#aaa;margin-top:8px'>
                        Based on {role_group} roles in {city}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

                # Show similar jobs from data
                st.markdown('<div class="section-title" style="font-size:16px">Similar Jobs in Dataset</div>', unsafe_allow_html=True)
                similar = df[
                    (df["location"].str.contains(city, case=False, na=False)) &
                    (df["avg_salary"].notna())
                ][["title", "company", "location", "avg_salary"]].head(5)

                if len(similar) > 0:
                    similar["avg_salary"] = similar["avg_salary"].apply(lambda x: f"₦{x:,.0f}")
                    similar.columns = ["Title", "Company", "Location", "Avg Salary"]
                    st.dataframe(similar, use_container_width=True, hide_index=True)
                else:
                    st.info("No similar jobs found for this location.")
            else:
                st.markdown("""
                <div style='background:white;border:1px dashed #b8d4be;border-radius:16px;
                            padding:48px 32px;text-align:center;color:#7a9a82'>
                    <div style='font-size:40px;margin-bottom:12px'>💼</div>
                    <div style='font-family:Syne,sans-serif;font-size:16px;font-weight:600;
                                color:#0a2e1a'>Fill in your details</div>
                    <div style='font-size:13px;margin-top:6px'>
                        Select your role, city and sector<br>then click Predict
                    </div>
                </div>
                """, unsafe_allow_html=True)