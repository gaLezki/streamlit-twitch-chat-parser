import streamlit as st
import pandas as pd
import time

from data_utils import load_csv, parse_vod_id, apply_filters
from processing import (
    compute_sliding_windows,
    format_vod_timestamp_url,
    format_seconds_to_ts,
    get_top_peaks
)
from charts import make_chart
from tables import render_top_table

st.set_page_config(page_title="Twitch VOD Chat Peaks Analyzer", layout="wide", page_icon="🔍")

st.title("🔍 VOD Chat Analyzer")

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file is not None:
    df = load_csv(uploaded_file)
    parser_vod_id = parse_vod_id(uploaded_file.name)
    st.success("✅ File loaded and cached!")

    vod_id = st.number_input("Enter VOD id here for URL purposes", step=1, value=parser_vod_id)

    # Rename Twitch export columns
    df.rename(columns={"time": "Time", "user_name": "User", "message": "Message"}, inplace=True)

    # Time filtering
    values = st.slider("Select a time range", df["Time"].min(), df["Time"].max(), (df["Time"].min(), df["Time"].max()))
    df = df[(df["Time"] > values[0]) & (df["Time"] < values[1])]

    # Apply filters
    filtered_df = apply_filters(df[["Time", "User", "Message"]])

    # Sliding window
    sliding_window = st.select_slider("Sliding window (s)", options=list(range(6, 16)), value=12)
    ignore_threshold = st.select_slider("Ignore moments with less unique users than", options=list(range(0, 11)), value=0)

    with st.spinner("Processing sliding windows..."):
        filtered_df = compute_sliding_windows(filtered_df, sliding_window)
        filtered_df = filtered_df[filtered_df["UUIW"] > ignore_threshold]
        filtered_df["timestamp_url"] = filtered_df["Time"].apply(lambda t: format_vod_timestamp_url(t, vod_id))

    # Chart
    chart_type = st.radio("Chart type", ["Line", "Bar"], index=0)
    fig = make_chart(filtered_df, chart_type)
    st.plotly_chart(fig, config={"scrollZoom": False})

    # Timestamp inspection
    timestamp = st.number_input(f"Show messages {sliding_window}s before this moment", step=1, value=None, placeholder="Enter a number")
    if timestamp is not None and vod_id is not None:
        vod_timestamp = time.strftime("%Hh%Mm%Ss", time.gmtime(timestamp - 30))
        st.page_link(f"https://www.twitch.tv/videos/{vod_id}?t={vod_timestamp}", label="Check VOD (-30s)")

        timestamp_df = filtered_df[
            (filtered_df["Time"] > (timestamp - sliding_window)) & (filtered_df["Time"] <= timestamp)
        ][["Message", "UUIW", "UUIW_msgs", "timestamp_url"]]
        st.dataframe(timestamp_df)

    # Top peaks table
    st.subheader("Get top broadcast moments")
    SLACK = st.selectbox("Time difference between peaks (s)", (30, 45, 60, 75, 90, 120), index=5)
    TOP_N = st.selectbox("TOP N", (10, 25, 50, 100))

    top_df = get_top_peaks(filtered_df, SLACK, TOP_N)
    render_top_table(top_df)
else:
    st.markdown("""
    1. Download Twitch VOD chat with [twitchchatdownloader.com](https://www.twitchchatdownloader.com/)  
    2. Upload the CSV here for analysis
    """)
