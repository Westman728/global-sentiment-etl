import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from pymongo import MongoClient
import yaml
from datetime import datetime, timedelta

# connecting to MongoDB
@st.cache_resource
def get_mongo_client():
    with open("config/settings.yaml", "r") as file:
        settings = yaml.safe_load(file)
    with open("config/credentials.yaml", "r") as cred_file:
        credentials = yaml.safe_load(cred_file)
    if "mongodb" in settings:
        settings["mongodb"] = credentials["mongodb"]
    return MongoClient(settings["mongodb"]["connection_string"]), settings

client, settings = get_mongo_client()
db = client[settings["mongodb"]["database"]]

# loading data
@st.cache_data(ttl=300)
def load_sentiment_data():
    collection = db["sentiment_analysis"]
    data = list(collection.find())
    return pd.DataFrame(data)

# title and description
st.title("Global Sentiment Analaysis")
st.write("Sentiment and topic analysis of Reddit/X/news headlines")

# loading data
with st.spinner("Loading data..."):
    df = load_sentiment_data()
    st.success("Data loaded successfully!")

# doublecheck data is loaded
if df.empty:
    st.error("No data found in the MongoDB collection. Did you run the data pipeline?")
else:
    # sidebar date filter
    st.sidebar.header("Filter by Date")
    if 'processed_at' in df.columns:
        min_date = df['processed_at'].min().date()
        max_date = df['processed_at'].max().date()

        default_start = max(min_date, (max_date - timedelta(days=7)))

        start_date = st.sidebar.date_input("Start Date", default_start, min_value=min_date, max_value=max_date)
        end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

        df_filtered = df[(df['processed_at'] >= start_datetime) & (df['processed_at'] <= end_datetime)]
    else:
        df_filtered = df.copy()
        st.sidebar.warning("No date column found in DF. Filtering by date is disabled.")

    # sidebar topic filter
    st.sidebar.subheader("Filter by Topic")
    topics = sorted(df["topic_id"].unique())
    selected_topics = st.sidebar.multiselect("Select Topics", topics, default=topics)
    if selected_topics:
        df_filtered = df_filtered[df_filtered["topic_id"].isin(selected_topics)]

    # show summary
    st.subheader("Data Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Posts", len(df_filtered))
    col2.metric("Sources", len(df_filtered["source"].unique()))
    col3.metric("Topics", len(df_filtered["topic_id"].unique()))

    # show sentiment distribution
    st.subheader("Sentiment Distribution")
    fig_sentiment = px.histogram(
        df_filtered,
        x="sentiment_compound",
        color="source",
        nbins=20,
        title="Sentiment Distribution by Source",
        labels={"sentiment_compound": "Sentiment Score", "count": "Number of Documents"},
    )
    st.plotly_chart(fig_sentiment, use_container_width=True)

    # topic distribution
    st.subheader("Topic Distribution")
    topic_counts = df_filtered.groupby(["topic_id", "source"]).size().reset_index(name="count")
    fig_topics = px.bar(
        topic_counts,
        x="topic_id",
        y="count",
        color="source",
        title="Topic Distribution by Source",
        labels={"topic_id": "Topic ID", "count": "Number of Documents"},
    )
    st.plotly_chart(fig_topics, use_container_width=True)

    # sentiment by topic
    st.subheader("Sentiment by Topic")
    sentiment_by_topic = df_filtered.groupby("topic_id")["sentiment_compound"].mean().reset_index()
    fig_sentiment_topic = px.bar(
        sentiment_by_topic,
        x="topic_id",
        y="sentiment_compound",
        title="Average Sentiment by Topic",
        labels={"topic_id": "Topic ID", "sentiment_compound": "Average Sentiment Score"},
    )
    st.plotly_chart(fig_sentiment_topic, use_container_width=True)

    # topic keywords
    st.subheader("Topic Keywords")
    topics_collection = db["topic_keywords"]
    topics_data = list(topics_collection.find())
    topics_df = pd.DataFrame(topics_data)

    if not topics_df.empty:
        for _, topic in topics_df.iterrows():
            topic_id = topic["topic_id"]
            keywords = ", ".join(topic["keywords"][:10])
            st.write(f"**Topic {topic_id}**: {keywords}")
        
    st.subheader("Recent content with sentiment")
    recent_df = df_filtered.sort_values(by="processed_at", ascending=False).head(10)
    for _, row in recent_df.iterrows():
        sentiment = row["sentiment_compound"]
        color = "green" if sentiment > 0.05 else "red" if sentiment < -0.05 else "gray"
        st.markdown(f"<div style='border-left: 5px solid {color}; padding-left: 10px;'>"
                    f"<p>{row['text'][:100]}...</p>"
                    f"<p><strong>Source:</strong> {row['source']} | "
                    f"<strong>Topic:</strong> {row['topic_id']} | "
                    f"<strong>Sentiment:</strong> {sentiment:.2f}</p></div>", 
                    unsafe_allow_html=True)