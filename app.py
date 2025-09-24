import streamlit as st
import pandas as pd
import polars as pl
import time

from data_utils import load_csv, parse_vod_id, apply_filters
from processing import (
    add_sliding_window_lazy,
    add_sliding_window_lazy_with_rolling,
    compute_sliding_windows,
    format_vod_timestamp_url,
    format_seconds_to_ts,
    get_top_peaks,
    format_messages
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

    vod_id = st.number_input("Enter VOD id here for URL purposes (if not fetched automatically from filename)", step=1, value=parser_vod_id)

    # Rename Twitch export columns
    df = df.rename({"time": "Time", "user_name": "User", "message": "Message"})

    # Time filtering
    values = st.slider("Select a time range", df["Time"].min(), df["Time"].max(), (df["Time"].min(), df["Time"].max()))
    df = df.filter(pl.col('Time') > values[0], pl.col('Time') < values[1])
    # df = df[(df["Time"] > values[0]) & (df["Time"] < values[1])]

    # Apply filters
    filtered_df = apply_filters(df)

    # Sliding window
    sliding_window = st.select_slider("Sliding window (s)", options=list(range(6, 16)), value=12)
    ignore_threshold = st.select_slider("Ignore moments with less unique users than", options=list(range(0, 11)), value=0)

    with st.spinner("Processing sliding windows..."):
        # Polars
        filtered_df = add_sliding_window_lazy_with_rolling(filtered_df, sliding_window, ignore_threshold).to_pandas()
        # filtered_df = filtered_df[filtered_df["UUIW"] > ignore_threshold]
        filtered_df["timestamp_url"] = filtered_df["Time"].apply(lambda t: format_vod_timestamp_url(t, vod_id))

    # Chart
    hover_info = 'In desktop, you can see chat some messages of that moment by hovering the chart. Zoom in by drawing a rectangle in any area you want. Zoom out with double-click.'
    chart_type = st.radio("Chart type", ["Bar", "Line"], index=0)
    hide_empty = False
    if chart_type == 'Bar':
        hide_empty = st.checkbox('Hide empty seconds to bring the useful data points closer to each other')
        hover_info = hover_info + ' After zooming in, you can open the VOD 30 seconds prior to that moment by clicking the URL in the bar.'
    fig = make_chart(filtered_df, chart_type, hide_empty)
    st.info(hover_info, icon="ℹ️")
    st.plotly_chart(fig, config={"scrollZoom": False}, key=1)

    # Timestamp inspection
    timestamp = st.number_input(f"Show messages {sliding_window}s before this moment (e.g. 12345)", step=1, value=None, placeholder="Enter a number")
    if timestamp is not None and vod_id is not None:
        vod_timestamp = time.strftime("%Hh%Mm%Ss", time.gmtime(timestamp - 30))
        st.page_link(f"https://www.twitch.tv/videos/{vod_id}?t={vod_timestamp}", label="Check VOD (-30s)")

        timestamp_df = filtered_df[
            (filtered_df["Time"] >= (timestamp - sliding_window)) & (filtered_df["Time"] <= (timestamp + sliding_window))
        ][["User","Time","Message", "UUIW", "UUIW_msgs", "timestamp_url"]]
        st.dataframe(timestamp_df)

    # Top peaks table
    st.subheader("Get top broadcast moments")
    SLACK = st.selectbox("Time difference between peaks (s)", (30, 45, 60, 75, 90, 120), index=5)
    TOP_N = st.selectbox("TOP N", (10, 25, 50, 100))

    top_df = get_top_peaks(filtered_df, SLACK, TOP_N)
    render_top_table(top_df)
else:
    st.markdown("""
    1. Download Twitch VOD chat with [twitchchatdownloader.com](https://www.twitchchatdownloader.com/) using Export chat feature
    2. Don't rename the downloaded CSV file, as the script identifies VOD id automatically            
    3. Upload the CSV file here for analysis and change settings as you wish
    4. If the VOD is still available, you'll get links to most chat-active parts of it
    """)
