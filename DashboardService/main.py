import streamlit as st
import pandas as pd
from DashboardService.utils import get_basic_metrics, search_documents, get_weapons_list
from Shared.logger_config import get_logger

logger = get_logger("dashboard-app")

def render_dashboard():
    st.set_page_config(page_title="Writers Net - Investigation System", layout="wide")
    st.title("Intelligence Investigation System - Writers Net 🕸️")

    metrics = get_basic_metrics()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Documents", value=metrics["total_documents"])

    st.markdown("---")

    st.subheader("Data Search & Exploration")

    weapons_available = get_weapons_list()

    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        free_text = st.text_input("Search free text in documents:")
    with col_b:
        sentiment = st.selectbox("Filter by Sentiment:", ["All", "Positive", "Neutral", "Negative"])
    with col_c:
        weapon_filter = st.selectbox("Filter by Weapon:", ["All"] + weapons_available)

    if st.button("Search"):
        with st.spinner("Fetching data from Elasticsearch..."):
            results = search_documents(free_text, sentiment, weapon_filter)

            if results:
                st.success(f"Found {len(results)} matching documents.")

                for doc in results:
                    original_name = doc.get('original_filename', 'Unknown')
                    image_id_short = doc.get('image_id', 'N/A')[:8]

                    with st.expander(f"Document: {original_name} | ID: {image_id_short}"):
                        analysis = doc.get("results", {}).get("analysis", {})

                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Sentiment:**", analysis.get("sentiment", "N/A"))
                        with col2:
                            found_weapons = analysis.get("weapons_found", [])
                            st.write("**Weapons Found:**", ", ".join(found_weapons) if found_weapons else "None")

                        st.write("**Clean Text:**")
                        st.text_area("", doc.get("results", {}).get("clean_text", ""), height=150, disabled=True, key=f"text_{image_id_short}")
            else:
                st.warning("No results found for your search.")



if __name__ == "__main__":
    import os
    import subprocess
    from Shared.config import settings

    if "STREAMLIT_RUN" not in os.environ:
        logger.info(f"Starting Streamlit Dashboard on {settings.DASHBOARD_HOST}:{settings.DASHBOARD_PORT}")

        env = os.environ.copy()
        env["STREAMLIT_RUN"] = "true"

        subprocess.run([
            "streamlit", "run", __file__,
            "--server.port", str(settings.DASHBOARD_PORT),
            "--server.address", settings.DASHBOARD_HOST
        ], env=env)
    else:
        render_dashboard()
