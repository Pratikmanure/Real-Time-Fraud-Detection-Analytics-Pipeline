from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from analytics import fetch_heatmap_data, fetch_recent_transactions, fetch_summary
from database import DB_PATH, init_db, insert_transactions
from simulator import _build_cards, generate_transactions


CITY_COORDS = {
    "New York": (40.7128, -74.0060),
    "San Francisco": (37.7749, -122.4194),
    "Chicago": (41.8781, -87.6298),
    "Miami": (25.7617, -80.1918),
    "Austin": (30.2672, -97.7431),
    "London": (51.5072, -0.1276),
    "Toronto": (43.6532, -79.3832),
    "Singapore": (1.3521, 103.8198),
}


st.set_page_config(page_title="FraudStream Command Center", layout="wide")


def apply_theme() -> None:
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap');
            :root {
                --panel: rgba(255, 251, 245, 0.96);
                --panel-2: #fffdf9;
                --text: #20150f;
                --muted: #6d584d;
                --accent: #c25b2a;
                --accent-2: #1e6a63;
                --danger: #972d20;
                --line: rgba(104, 74, 61, 0.14);
                --shadow: 0 18px 40px rgba(86, 50, 35, 0.14);
                --rule-bg: rgba(194,91,42,0.08);
                --note-bg: rgba(30,106,99,0.05);
                --bg-1: #f7f0e8;
                --bg-2: #efe6da;
                --sidebar-1: #fff8ee;
                --sidebar-2: #f8ecde;
            }

            @media (prefers-color-scheme: dark) {
                :root {
                    --panel: rgba(18, 18, 20, 0.96);
                    --panel-2: #1a1a1e;
                    --text: #fdfdfd;
                    --muted: #9e9ea6;
                    --accent: #00f0ff; /* Neon cyan */
                    --accent-2: #ff4b4b; /* Neon red */
                    --danger: #ff3333;
                    --line: rgba(255, 255, 255, 0.1);
                    --shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
                    --rule-bg: rgba(0,240,255,0.08);
                    --note-bg: rgba(255,75,75,0.05);
                    --bg-1: #0a0a0c;
                    --bg-2: #050506;
                    --sidebar-1: #101014;
                    --sidebar-2: #0a0a0c;
                }
            }

            .stApp {
                background:
                    radial-gradient(circle at top left, var(--note-bg), transparent 28%),
                    radial-gradient(circle at top right, var(--rule-bg), transparent 22%),
                    linear-gradient(180deg, var(--bg-1) 0%, var(--bg-2) 100%);
                color: var(--text);
                font-family: 'IBM Plex Sans', sans-serif;
            }
            .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
            #MainMenu, footer, header {visibility: hidden;}
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, var(--sidebar-1) 0%, var(--sidebar-2) 100%);
                border-right: 1px solid var(--line);
            }
            .hero, .panel {
                background: var(--panel);
                border: 1px solid var(--line);
                box-shadow: var(--shadow);
                border-radius: 24px;
            }
            .hero {padding: 1.5rem 1.65rem; margin-bottom: 1rem;}
            .panel {padding: 1rem 1.1rem; margin-bottom: 1rem;}
            .eyebrow {
                text-transform: uppercase;
                letter-spacing: 0.18em;
                color: var(--accent);
                font-size: 0.72rem;
                font-weight: 700;
            }
            .hero-title, h2, h3 {font-family: 'Space Grotesk', sans-serif; color: var(--text) !important;}
            .hero-title {font-size: 2.25rem; margin: 0.35rem 0;}
            .hero-copy {color: var(--muted); max-width: 900px;}
            .metric-shell {
                background: var(--panel-2);
                border: 1px solid var(--line);
                border-radius: 18px;
                padding: 0.9rem 1rem;
                box-shadow: var(--shadow);
            }
            .rule-chip {
                display: inline-block;
                padding: 0.38rem 0.7rem;
                margin: 0.2rem 0.4rem 0 0;
                border-radius: 999px;
                border: 1px solid var(--line);
                background: var(--rule-bg);
                font-size: 0.8rem;
                color: var(--text);
            }
            .note-card {
                border: 1px solid var(--line);
                background: var(--note-bg);
                border-radius: 18px;
                padding: 0.9rem 1rem;
                color: var(--text);
            }
            .stButton > button {
                border-radius: 14px;
                min-height: 2.8rem;
                background: linear-gradient(135deg, var(--accent), var(--accent-2));
                color: white;
                border: none;
                font-weight: 700;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _seed_demo_batch(batch_size: int) -> None:
    cards = _build_cards()
    insert_transactions(generate_transactions(cards, batch_size=batch_size))


def _render_header() -> None:
    st.markdown(
        f"""
        <div class="hero">
            <div class="eyebrow">Real-Time Fraud Detection Analytics Pipeline</div>
            <div class="hero-title">FraudStream Command Center</div>
            <div class="hero-copy">
                A streaming analytics demo that ingests synthetic card transactions, stores them in SQLite, applies advanced SQL window-function anomaly detection,
                then scores and visualizes suspicious behavior. Database file: <strong>{Path(DB_PATH).name}</strong>.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metrics(metrics: dict) -> None:
    cols = st.columns(5)
    metric_map = [
        ("Transactions", f"{metrics['total_transactions']:,}", "All ingested swipes and digital payments."),
        ("Flagged", f"{metrics['flagged_transactions']:,}", "Rows matching at least one fraud rule."),
        ("Volume", f"${metrics['volume_usd']:,.0f}", "Total processed transaction value."),
        ("Flagged Volume", f"${metrics['flagged_volume']:,.0f}", "Suspicious dollar exposure."),
        ("Max Flagged", f"${metrics['max_flagged_amount']:,.0f}", "Largest suspicious spend."),
    ]
    for col, (label, value, note) in zip(cols, metric_map):
        with col:
            st.markdown("<div class='metric-shell'>", unsafe_allow_html=True)
            st.metric(label, value)
            st.caption(note)
            st.markdown("</div>", unsafe_allow_html=True)


def _prepare_heatmap(heatmap: pd.DataFrame) -> pd.DataFrame:
    if heatmap.empty:
        return heatmap
    shaped = heatmap.copy()
    shaped["lat"] = shaped["merchant_city"].map(lambda city: CITY_COORDS.get(city, (None, None))[0])
    shaped["lon"] = shaped["merchant_city"].map(lambda city: CITY_COORDS.get(city, (None, None))[1])
    return shaped.dropna(subset=["lat", "lon"])


def _render_rule_panel() -> None:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("### SQL Detection Rules")
    st.markdown(
        """
        <span class="rule-chip">`LAG()` for city-to-city jumps inside 1 hour</span>
        <span class="rule-chip">`COUNT() OVER RANGE 900 PRECEDING` for 15-minute velocity</span>
        <span class="rule-chip">`AVG() OVER ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING` for personal baselines</span>
        <span class="rule-chip">`COUNT() OVER` per merchant for burst detection</span>
        <span class="rule-chip">`LAG()` + rolling device counts for takeover detection</span>
        <span class="rule-chip">`ROW_NUMBER()` for daily top-spend ranking</span>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_alerts(flagged: pd.DataFrame) -> None:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("### High-Risk Alerts & AI Insights")
    if flagged.empty:
        st.info("No anomalies yet. Start the simulator or seed a batch from the sidebar.")
    else:
        display = flagged[
            [
                "event_timestamp",
                "card_id",
                "amount",
                "fraud_type",
                "risk_score",
                "ai_insight",
            ]
        ].copy()
        display["event_timestamp"] = pd.to_datetime(display["event_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        display["amount"] = display["amount"].map(lambda value: f"${value:,.2f}")
        st.dataframe(display.head(20), use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_typology(type_counts: pd.DataFrame, risk_counts: pd.DataFrame, rule_counts: pd.DataFrame) -> None:
    left, right = st.columns(2)
    with left:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("### Fraud Typology")
        if type_counts.empty:
            st.info("Fraud type distribution will appear once transactions are flagged.")
        else:
            fig = px.bar(
                type_counts,
                x="fraud_type",
                y="count",
                color="count",
                color_continuous_scale=["#f4d8c5", "#c25b2a", "#7f231c"],
            )
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=25, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
            fig.update_xaxes(title=None)
            fig.update_yaxes(title="Flag count")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("### Risk Bands")
        if risk_counts.empty:
            st.info("Risk bands will populate after alerts appear.")
        else:
            fig = px.pie(risk_counts, names="risk_band", values="count", color="risk_band", color_discrete_map={"monitor": "#e1cbb5", "review": "#d48a55", "critical": "#972d20"})
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=25, b=10), paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("### Rule Hit Rates")
    if rule_counts.empty:
        st.info("Rule usage appears after fraud rules trigger.")
    else:
        fig = px.bar(rule_counts, x="count", y="rule_name", orientation="h", color="count", color_continuous_scale=["#dbe7e3", "#1e6a63"])
        fig.update_layout(height=340, margin=dict(l=10, r=10, t=20, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
        fig.update_xaxes(title="Triggered rows")
        fig.update_yaxes(title=None)
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_geo_and_feed(heatmap: pd.DataFrame, recent: pd.DataFrame) -> None:
    left, right = st.columns([1.05, 0.95])
    with left:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("### Geographic Hotspots")
        geo = _prepare_heatmap(heatmap)
        if geo.empty:
            st.info("No geo anomalies to map yet.")
        else:
            fig = px.scatter_geo(
                geo,
                lat="lat",
                lon="lon",
                size="flagged_count",
                color="avg_amount",
                hover_name="merchant_city",
                hover_data={"merchant_country": True, "flagged_count": True, "avg_amount": ":.2f"},
                projection="natural earth",
                color_continuous_scale=["#f4d8c5", "#c25b2a", "#7f231c"],
            )
            fig.update_layout(height=390, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("### Live Transaction Feed")
        if recent.empty:
            st.info("Recent feed is empty.")
        else:
            display = recent.copy()
            display["event_timestamp"] = pd.to_datetime(display["event_timestamp"]).dt.strftime("%H:%M:%S")
            display["amount"] = display["amount"].map(lambda value: f"${value:,.2f}")
            st.dataframe(display, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)


def _render_interview_notes() -> None:
    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown("### Interview Talking Points")
    st.markdown(
        """
        <div class="note-card">
            <strong>Data Engineering:</strong> micro-batch simulator writes append-only transactions into SQLite with realistic event timestamps.<br><br>
            <strong>Machine Learning & AI:</strong> Ensemble scoring combines `Scikit-Learn` unsupervised Isolation Forest models for zero-day anomalies with strict SQL heuristics.<br><br>
            <strong>Advanced SQL:</strong> Window functions drive deterministic detection via `LAG`, rolling `COUNT`, and moving `AVG`.<br><br>
            <strong>Analytics:</strong> The AI Copilot generates dynamic natural-language insights interpreting risk probabilities instantly for analysts.
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)


def render_dashboard() -> None:
    summary = fetch_summary()
    recent = fetch_recent_transactions(limit=20)
    heatmap = fetch_heatmap_data()
    _render_header()
    _render_metrics(summary["metrics"])
    _render_rule_panel()
    _render_alerts(summary["flagged"])
    _render_typology(summary["type_counts"], summary["risk_counts"], summary["rule_counts"])
    _render_geo_and_feed(heatmap, recent)
    _render_interview_notes()


def main() -> None:
    apply_theme()
    init_db()
    with st.sidebar:
        st.markdown("## Controls")
        st.caption("Use the simulator script for continuous streaming, or seed a demo batch here.")
        seed_count = st.slider("Seed transactions", min_value=10, max_value=200, value=40, step=10)
        if st.button("Seed Demo Batch", use_container_width=True):
            _seed_demo_batch(seed_count)
            st.rerun()

        st.markdown("## Run")
        st.code("python -m fraud_pipeline.simulator --batch-size 10 --sleep-seconds 2")
        refresh_seconds = st.slider("Auto refresh every (seconds)", min_value=2, max_value=30, value=5, step=1)

    if hasattr(st, "fragment"):
        @st.fragment(run_every=f"{refresh_seconds}s")
        def _live_fragment() -> None:
            render_dashboard()

        _live_fragment()
    else:
        st.info("This Streamlit version does not support automatic fragment refresh. Use the rerun button after new data arrives.")
        render_dashboard()


if __name__ == "__main__":
    main()
