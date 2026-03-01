import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from DashboardService.utils import (
    get_basic_metrics, 
    search_documents, 
    get_weapons_list,
    get_latest_10_documents,
    search_israel_docs,
    search_multiple_weapons,
    search_positive_palestinian,
    search_grenade_december,
    get_all_documents
)
from Shared.logger_config import get_logger
from typing import List, Dict, Any, Tuple, cast

logger = get_logger("dashboard-app")

def setup_page():
    st.set_page_config(page_title="Writers Net - Investigation System", layout="wide")
    st.title("Intelligence Investigation System - Writers Net 🕸️")

def display_metrics():
    metrics = get_basic_metrics()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Documents", value=metrics["total_documents"])
    st.markdown("---")

def display_word_cloud(results: List[Dict[str, Any]]):
    all_text = " ".join([doc.get("results", {}).get("clean_text", "") for doc in results])
    if not all_text.strip():
        st.info("No text available to generate word cloud.")
        return

    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_text)
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    st.pyplot(fig)

def display_weapon_stats(results: List[Dict[str, Any]]):
    all_weapons = []
    for doc in results:
        all_weapons.extend(doc.get("results", {}).get("analysis", {}).get("weapons_found", []))
    
    if not all_weapons:
        st.info("No weapons found in these results.")
        return

    df_weapons = pd.DataFrame(all_weapons, columns=["Weapon"])
    weapon_counts = df_weapons["Weapon"].value_counts().reset_index()
    weapon_counts.columns = ["Weapon", "Count"]

    fig = px.bar(weapon_counts, x="Weapon", y="Count", title="Total Weapons Found",
                 color="Count", color_continuous_scale="Reds")
    st.plotly_chart(fig, use_container_width=True)

def display_global_analytics(results: List[Dict[str, Any]]):
    if not results:
        return

    st.header("Global Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        sentiments = [doc.get("results", {}).get("analysis", {}).get("sentiment", "Neutral") for doc in results]
        df_sentiment = pd.DataFrame(sentiments, columns=["Sentiment"])
        sentiment_counts = df_sentiment["Sentiment"].value_counts().reset_index()
        sentiment_counts.columns = ["Sentiment", "Count"]

        fig = px.pie(sentiment_counts, values="Count", names="Sentiment", title="Sentiment Distribution",
                     color="Sentiment",
                     color_discrete_map={"Positive": "green", "Negative": "red", "Neutral": "gray"})
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        display_weapon_stats(results)

    st.subheader("Word Cloud")
    display_word_cloud(results)
    st.markdown("---")

def display_document_list(results: List[Dict[str, Any]], limit_text: bool = False):
    if not results:
        st.warning("No results found.")
        return

    for doc in results:
        original_name = doc.get('original_filename', 'Unknown')
        image_id_short = doc.get('image_id', 'N/A')[:8]

        with st.expander(f"Document: {original_name} | ID: {image_id_short}"):
            analysis = doc.get("results", {}).get("analysis", {})
            clean_text = doc.get("results", {}).get("clean_text", "")

            if limit_text:
                display_text = clean_text[:42] + "..." if len(clean_text) > 42 else clean_text
                st.write("**Snippet:**", display_text)
            else:
                col_text, col_chart = st.columns([1, 1])

                with col_text:
                    st.write("**Sentiment:**", analysis.get("sentiment", "N/A"))
                    found_weapons = analysis.get("weapons_found", [])
                    st.write("**Weapons Found:**", ", ".join(found_weapons) if found_weapons else "None")
                    st.write("**Clean Text:**")
                    st.text_area(
                        "", 
                        clean_text, 
                        height=200, 
                        disabled=True, 
                        key=f"text_{image_id_short}"
                    )

                with col_chart:
                    top_words = analysis.get("top_10_words", [])
                    if top_words:
                        df_words = pd.DataFrame(top_words)
                        fig = px.bar(df_words, x="count", y="word", orientation='h',
                                     title="Top 10 Words Frequency",
                                     labels={"count": "Frequency", "word": "Word"},
                                     color="count",
                                     color_continuous_scale="Viridis")
                        fig.update_layout(yaxis={'categoryorder':'total ascending'})
                        st.plotly_chart(fig, use_container_width=True)

def render_tab1():
    weapons_available = get_weapons_list()
    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        free_text = st.text_input("Search free text in documents:")
    with col_b:
        sentiment = st.selectbox("Filter by Sentiment:", ["All", "Positive", "Neutral", "Negative"])
    with col_c:
        weapon_filter = st.selectbox("Filter by Weapon:", ["All"] + weapons_available)

    if st.button("Run Interactive Search"):
        with cast(Any, st.spinner("Searching...")):
            results = search_documents(free_text, sentiment, weapon_filter)
            if results:
                st.success(f"Found {len(results)} matching documents.")
                display_global_analytics(results)
                display_document_list(results)
            else:
                st.warning("No results found.")

def render_tab2():
    st.header("Intelligence Queries")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Latest 10 Documents"):
            results = get_latest_10_documents()
            display_document_list(results, limit_text=True)

        if st.button("Documents containing 'Israel' (Max 5)"):
            results = search_israel_docs()
            display_document_list(results)

        if st.button("Documents with 3+ Weapons"):
            results = search_multiple_weapons()
            display_document_list(results)

    with col2:
        if st.button("Positive Palestinian Sentiment"):
            results = search_positive_palestinian()
            display_document_list(results)

        if st.button("Grenade in December"):
            results = search_grenade_december()
            display_document_list(results)

def render_dashboard():
    setup_page()
    display_metrics()
    
    tab1, tab2 = st.tabs(["Interactive Search & Global Analytics", "Pre-defined Intelligence Queries"])
    
    with tab1:
        render_tab1()
        
    with tab2:
        render_tab2()

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
