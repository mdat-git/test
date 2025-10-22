# Create a ready-to-run Streamlit dashboard app for OMS analysis
# It includes modern rounded cards, tabs, filters, and placeholder data.
# Save as /mnt/data/oms_dashboard.py so the user can download it.

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import textwrap

app_code = r'''
# oms_dashboard.py
# Streamlit OMS Reliability Dashboard (rounded, modern, accessible)
# Run with: streamlit run oms_dashboard.py

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# -----------------------------
# Page config & CSS (rounded cards aesthetic)
# -----------------------------
st.set_page_config(
    page_title="NOVA · OMS Reliability Dashboard",
    page_icon="⚡",
    layout="wide",
)

CARD_CSS = """
<style>
:root {
  --card-radius: 16px;
  --card-bg: rgb(255 255 255 / 0.65);
  --card-shadow: 0 2px 16px rgba(0,0,0,0.08);
}
/* Dark mode safe bg */
[data-theme="dark"] :root, [data-theme="dark"] body, body.dark {
  --card-bg: rgb(30 30 30 / 0.45);
}

.block-container { padding-top: 1.2rem; }
.card {
  border-radius: var(--card-radius);
  padding: 14px 16px;
  background: var(--card-bg);
  box-shadow: var(--card-shadow);
  border: 1px solid rgba(120,120,120,0.15);
}
.kpi-value { font-weight: 700; font-size: 1.7rem; line-height: 1; }
.kpi-label { font-size: 0.8rem; color: rgba(120,120,120,0.9); margin-top: 4px; }
.kpi-suffix { font-weight: 600; opacity: 0.8; }
.section-title {
  font-weight: 700; font-size: 1.05rem; margin: 0 0 8px 0;
  display:flex; align-items:center; gap:8px;
}
.tag {
  display:inline-block; padding:4px 10px; border-radius:999px;
  border:1px solid rgba(120,120,120,0.25); font-size:0.8rem;
}
hr.soft { border:none; height:1px; background:rgba(120,120,120,0.2); margin:12px 0; }
</style>
"""
st.markdown(CARD_CSS, unsafe_allow_html=True)

# -----------------------------
# Mock data generators (replace with Snowflake/HANA views later)
# -----------------------------
np.random.seed(42)

weeks = pd.date_range(datetime.today() - timedelta(weeks=25), periods=26, freq="W-MON")
ts = pd.DataFrame({
    "date": weeks.date,
    "SAIDI": 20 + np.random.rand(len(weeks)) * 40,
    "SAIFI": 0.5 + np.random.rand(len(weeks)) * 0.8,
    "CMI_M": 2.5 + np.random.rand(len(weeks)) * 4.0,
    "Validations": 200 + np.random.randint(0, 250, len(weeks)),
    "ETR_MAE": 15 + np.random.rand(len(weeks)) * 35,  # minutes
})

districts = [f"D{i}" for i in range(1, 13)]
district_perf = pd.DataFrame({
    "District": districts,
    "SAIDI": np.random.randint(20, 80, len(districts)),
    "SAIFI": np.round(0.3 + np.random.rand(len(districts)), 2),
    "CMI_Saved_pct": np.round(5 + np.random.rand(len(districts)) * 20, 1),
})

cause_mix = pd.DataFrame({
    "Cause": ["OH Equipment", "UG Equipment", "Vegetation", "Weather", "Animals", "3rd Party", "Other"],
    "Percent": [32, 18, 22, 9, 7, 6, 6],
})

# Top-level KPIs
kpi = {
    "SAIDI": round(42.1, 1),
    "SAIFI": round(0.87, 2),
    "CAIDI": round(48.6, 1),
    "CMI Saved %": 12.3,
    "Validations/day": 386,
    "Dangerous Miss %": 0.3,
    "ETR MAE (min)": 28.4,
}

# -----------------------------
# Header
# -----------------------------
left, mid, right = st.columns([2,6,3])
with left:
    st.markdown("### ⚡ NOVA · OMS Reliability Dashboard")
with mid:
    st.write("")
with right:
    c1, c2 = st.columns([1,1])
    with c1:
        st.button("Refresh", use_container_width=True)
    with c2:
        st.download_button("Export CSV", ts.to_csv(index=False).encode(), "oms_timeseries.csv", use_container_width=True)

# -----------------------------
# Filters Card
# -----------------------------
with st.container():
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🔎 Filters <span class="tag">Last 12 months</span></div>', unsafe_allow_html=True)
    f1, f2, f3, f4, f5 = st.columns([1,1,1,2,1])
    with f1:
        d = st.selectbox("District", ["All"] + districts, index=0)
    with f2:
        feeder = st.selectbox("Feeder", ["All"] + [f"FDR-{i}" for i in range(1, 9)], index=0)
    with f3:
        klass = st.selectbox("Incident Class", ["All", "Single-Line", "Multi-Step", "Planned", "Storm", "Major Event"], index=0)
    with f4:
        query = st.text_input("Search incident/device/note…", "")
    with f5:
        st.write("")
        st.button("Advanced", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# -----------------------------
# KPI Cards
# -----------------------------
def kpi_card(title, value, suffix=""):
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi-value">{value}<span class="kpi-suffix">{suffix}</span></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi-label">{title}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1: kpi_card("SAIDI (min)", kpi["SAIDI"])
with c2: kpi_card("SAIFI", kpi["SAIFI"])
with c3: kpi_card("CAIDI (min)", kpi["CAIDI"])
with c4: kpi_card("CMI Saved vs Baseline", kpi["CMI Saved %"], "%")

c5, c6, c7 = st.columns(3)
with c5: kpi_card("Validations / Day", kpi["Validations/day"])
with c6: kpi_card("Dangerous Miss Rate", kpi["Dangerous Miss %"], "%")
with c7: kpi_card("ETR MAE (min)", kpi["ETR MAE (min)"])

# -----------------------------
# Tabs
# -----------------------------
tab_exec, tab_ops, tab_etr, tab_cause = st.tabs(["Executive", "Validation Ops", "ETR Quality", "Cause & Classification"])

# --- Executive tab ---
with tab_exec:
    # Reliability Trends
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📈 Reliability Trends</div>', unsafe_allow_html=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ts["date"], y=ts["SAIDI"], name="SAIDI (min)", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["date"], y=ts["SAIFI"], name="SAIFI", mode="lines"))
    fig.add_trace(go.Scatter(x=ts["date"], y=ts["ETR_MAE"], name="ETR MAE (min)", mode="lines", line=dict(dash="dash")))
    fig.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=380, legend_orientation="h", legend_y=-0.2)
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # District Performance & Cause Mix
    colA, colB = st.columns(2)
    with colA:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🗺️ District Performance</div>', unsafe_allow_html=True)
        fig_bar = px.bar(district_perf, x="District", y=["SAIDI", "SAIFI", "CMI_Saved_pct"], barmode="group")
        fig_bar.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=380, legend_orientation="h", legend_y=-0.2)
        st.plotly_chart(fig_bar, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with colB:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🧰 Cause Mix</div>', unsafe_allow_html=True)
        fig_pie = px.pie(cause_mix, names="Cause", values="Percent", hole=0.45)
        fig_pie.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=380, legend_orientation="h", legend_y=-0.2)
        st.plotly_chart(fig_pie, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# --- Validation Ops tab ---
with tab_ops:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">👥 Validation Throughput</div>', unsafe_allow_html=True)
    fig_val = px.bar(ts, x="date", y="Validations")
    fig_val.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=360)
    st.plotly_chart(fig_val, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Work Mix & Savings + threshold slider
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🎚️ Work Mix & Savings</div>', unsafe_allow_html=True)
    col1, col2 = st.columns([1,1])
    with col1:
        thr = st.slider("Auto-approve threshold (classifier probability)", 50, 99, 80, step=1)
        # Simple demo curve: higher threshold -> lower workload saved, lower dangerous miss
        workload_saved = max(0, 100 - (thr - 50) * 2)  # fake curve
        dangerous_miss = max(0.1, (100 - thr) * 0.02)  # fake curve
        st.metric("Estimated workload saved", f"{workload_saved:.0f}%")
        st.metric("Estimated dangerous miss rate", f"{dangerous_miss:.1f}%")
        st.caption("Tune threshold to balance workload saved vs. dangerous-miss risk.")
    with col2:
        fig_cmi = px.line(ts, x="date", y="CMI_M", markers=False)
        fig_cmi.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=260, yaxis_title="CMI (Millions)")
        st.plotly_chart(fig_cmi, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# --- ETR Quality tab ---
with tab_etr:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">⏱️ ETR Error Over Time</div>', unsafe_allow_html=True)
    fig_etr = px.line(ts, x="date", y="ETR_MAE")
    fig_etr.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=360, yaxis_title="MAE (minutes)")
    st.plotly_chart(fig_etr, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🧩 Top ETR Failure Patterns (NLP)</div>', unsafe_allow_html=True)
    patterns = [
        "Disable ETR recalculation @ <place>",
        "Remove ETR for <place> (MAN/SYS)",
        "Inspection ETR set then cleared",
        "Conflicting Followup: CREW_ACTION vs FOLLOWUP",
        "CGI_HISMGR archived → reopened (contiguous block)",
    ]
    for p in patterns:
        st.markdown(f"- {p}")
    st.markdown('</div>', unsafe_allow_html=True)

# --- Cause & Classification tab ---
with tab_cause:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🏷️ Causes by District</div>', unsafe_allow_html=True)
    fig_cause_bar = px.bar(district_perf, x="District", y="CMI_Saved_pct", labels={"CMI_Saved_pct":"% Low-Impact Auto-Validated"})
    fig_cause_bar.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=380)
    st.plotly_chart(fig_cause_bar, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🧮 Cause Distribution</div>', unsafe_allow_html=True)
    fig_cause_pie = px.pie(cause_mix, names="Cause", values="Percent", hole=0.45)
    fig_cause_pie.update_layout(margin=dict(l=10,r=10,t=10,b=10), height=380, legend_orientation="h", legend_y=-0.2)
    st.plotly_chart(fig_cause_pie, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# -----------------------------
# Notes for integration
# -----------------------------
with st.expander("Integration notes (replace mocks with live data)"):
    st.markdown("""
**Data joins you’ll likely need**
- **Incident → Device → Location** mapping from OMS HIS tables
- **AMI meters ↔ Incident** link (for CI/CMI deltas)
- **FOLLOWUP_DESC & CREW_ACTION** parsed events for ETR set/clear, cause tagging
- **District/Feeder** lookups for rollups

**Key measures**
- SAIDI/SAIFI/CAIDI (IEEE 1366)
- CMI saved vs baseline (define baseline period or counterfactual model)
- Validations/day, auto-approve %, dangerous-miss %
- ETR quality: MAE/MedianAE/P90, bias by cause/storm/district

**How to wire**
- Replace `ts`, `district_perf`, `cause_mix`, `kpi` dict with Snowflake/HANA queries via `snowflake-connector-python` or `sqlalchemy`.
- Cache heavy queries with `@st.cache_data(ttl=900)`.
- Use a feature flag in `st.sidebar` to switch environments (DEV/UAT/PROD).
""")
'''
with open('/mnt/data/oms_dashboard.py', 'w', encoding='utf-8') as f:
    f.write(app_code)

'/mnt/data/oms_dashboard.py'
