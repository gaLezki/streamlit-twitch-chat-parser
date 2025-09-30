import streamlit as st
import pandas as pd
import polars as pl
import time

from data_utils import load_csv, parse_vod_id, apply_filters
from processing import (
    add_sliding_windows,
    add_tumbling_window,
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

    status_container = st.status("Processing csv with given parameters...", expanded=True)

    # Time filtering
    values = st.slider("Select a time range", df["Time"].min(), df["Time"].max(), (df["Time"].min(), df["Time"].max()))
    df = df.filter(pl.col('Time') > values[0], pl.col('Time') < values[1])
    # df = df[(df["Time"] > values[0]) & (df["Time"] < values[1])]

    # Apply filters
    filtered_df = apply_filters(df)

    window_type = st.radio(
        "Windowing function",
        ["Tumbling", "Sliding"],
        captions=[
            "Faster but less accurate: Doesn't necessarily get the best peaks if messages spread across two windows, as windows are static (e.g. 0-12s, 12-24s and so on).",
            "Slower, but more accurate: Window is calculated for every second there is a message."
        ], index=0
    )    
    window_size = st.select_slider("Sliding window (s)", options=list(range(6, 16)), value=12)
    ignore_threshold = st.select_slider("Ignore moments with less unique users than", options=list(range(0, 11)), value=0)

    
    status_container.update(state='running', label='Calculating unique nicknames within given time', expanded=True)
    perf_time_start = time.perf_counter()
    if window_type == 'Sliding':
        filtered_df = add_sliding_windows(filtered_df, window_size, ignore_threshold=ignore_threshold).to_pandas()
    else:
        filtered_df = add_tumbling_window(filtered_df, window_size, ignore_threshold=ignore_threshold).to_pandas()
    perf_time_end = time.perf_counter()
    status_container.update(state='running', label=f'Calculation of unique nicknames within given window took {round(perf_time_end - perf_time_start, 2)} seconds.', expanded=True)
    filtered_df["timestamp_url"] = filtered_df["Time"].apply(lambda t: format_vod_timestamp_url(t, vod_id))
    filtered_df['Timestamp'] = filtered_df["Time"].apply(lambda t: format_seconds_to_ts(t))


    hover_info = 'In desktop, you can see chat some messages of that moment by hovering the chart. Zoom in by drawing a rectangle in any area you want. Zoom out with double-click.'
    chart_type = st.radio("Chart type", ["Bar", "Line"], index=0)
    if chart_type == 'Bar':
        hover_info = hover_info + ' After zooming in, you can open the VOD 30 seconds prior to that moment by clicking the URL in the bar.'
    status_container.update(state='running', label=f'Drawing {chart_type} chart.', expanded=True)
    fig = make_chart(filtered_df, chart_type)
    st.info(hover_info, icon="ℹ️")
    st.plotly_chart(fig, config={"scrollZoom": False})

    # Top peaks table
    st.subheader("Get top broadcast moments")
    SLACK = st.selectbox("Time difference between peaks (s)", (30, 45, 60, 75, 90, 120), index=5)
    TOP_N = st.selectbox("TOP N", (10, 25, 50, 100))

    status_container.update(state='running', label=f'Building table for top {TOP_N} unique nickname peaks within {window_size} seconds, at least {SLACK} seconds apart from each other.', expanded=True)
    top_df = get_top_peaks(filtered_df, SLACK, TOP_N)
    render_top_table(top_df)
    status_container.update(state='complete', label='✅ All ready!', expanded=True)
else:
    st.markdown("""
    1. Download Twitch VOD chat with [twitchchatdownloader.com](https://www.twitchchatdownloader.com/) using Export chat feature
    2. Don't rename the downloaded CSV file, as the script identifies VOD id automatically            
    3. Upload the CSV file here for analysis and change settings as you wish
    4. If the VOD is still available, you'll get links to most chat-active parts of it
    """)
