import streamlit as st
import plotly.graph_objects as go
import html
import pandas as pd
import time
import os
import re

st.set_page_config(page_title="Twitch VOD Chat Peaks Analyzer", layout="wide", page_icon='🔍')

st.title("🔍 VOD Chat Analyzer")


# Apply filters to the DataFrame
def apply_filters(df, filter_replies=True):
    # Filter out rows with User "nightbot"
    df = df[df['User'] != 'nightbot']
    
    # Filter out rows with messages starting with an exclamation mark or "@"
    if filter_replies == True:
        df = df[~df['Message'].str.startswith(('!', '@'), na=False)]
    else:
        df = df[~df['Message'].str.startswith(('!'), na=False)]
    
    return df

def parse_vod_id(filename):
    basename, _ = os.path.splitext(filename)

    match = re.search(r"(\d+)", basename)
    if match:
        return int(match.group(1))
    return None

# Define a function to format the messages
def format_messages(messages):
    # Limit the messages to the first 30
    messages_array = messages.split(' || ')[:30]
    messages = '<br>- '.join(messages_array)
    return messages

def format_seconds_to_ts(seconds):
    return time.strftime('%Hh%Mm%Ss', time.gmtime(seconds))

def format_vod_timestamp_url(time_in_seconds):
    vod_timestamp = time.strftime('%Hh%Mm%Ss', time.gmtime(time_in_seconds-30))
    if vod_id is not None:
        return f'https://www.twitch.tv/videos/{vod_id}?t={vod_timestamp}'
    return None

@st.cache_data
def load_csv(file):
    return pd.read_csv(file, encoding='utf-8', on_bad_lines='warn')

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file is not None:
    df = load_csv(uploaded_file)
    parser_vod_id = parse_vod_id(uploaded_file.name)
    st.success("✅ File loaded and cached!")
    vod_id = st.number_input("Enter VOD id here for URL purposes", step=1, value=parser_vod_id, placeholder='VOD id')

    df.rename(columns={'time': 'Time', 'user_name': 'User', 'message': 'Message'}, inplace=True)
    values = st.slider("Select a time range for messages to be studied", df["Time"].min(), df["Time"].max(), (df["Time"].min(), df["Time"].max()))
    df = df[(df['Time'] > values[0]) & (df['Time'] < values[1])]

    filtered_df = apply_filters(df[['Time', 'User', 'Message']], filter_replies=True)
    sliding_window = st.select_slider(
        "Select size of sliding window (s)",options=[6,7,8,9,10,11,12,13,14,15],
        value=12
    )
    ignore_threshold = st.select_slider(
        "Ignore moments with less unique users than",options=list(range(0, 11)), value=0
    )
    with st.spinner('Processing amounts of unique users (and their messages) within given sliding window'):
        uuiw_counts = []
        uuiw_messages = []

        for t in filtered_df["Time"]:
            window = filtered_df.loc[
                (filtered_df["Time"] >= t - sliding_window) & (filtered_df["Time"] <= t)
            ]

            # Count unique users
            uuiw_counts.append(window["User"].nunique())

            if window.empty:
                msgs = ""
            else:
                # Drop missing values, take first message per user, ensure string
                msgs_list = (
                    window.groupby("User")["Message"]
                    .first()
                    .dropna()
                    .astype(str)
                    .tolist()
                )
                msgs = " || ".join(msgs_list) if msgs_list else ""

            uuiw_messages.append(msgs)

        filtered_df["UUIW"] = uuiw_counts
        filtered_df["UUIW_msgs"] = uuiw_messages

    # Filter moments with less uuiw than given ignore_threshold
    filtered_df = filtered_df[filtered_df['UUIW'] > ignore_threshold]

    filtered_df['MessagePeek'] = filtered_df['UUIW_msgs'].apply(format_messages)
    filtered_df['timestamp_url'] = filtered_df['Time'].apply(format_vod_timestamp_url)
    filtered_df['timestamp_url_md'] = '[🔗](' + filtered_df['timestamp_url'] + ')'
    chart_type = st.radio(
        "Chart type",
        ["Line", "Bar"],
        index=0,
    )
    if chart_type == 'Line':
        fig = go.Figure(data=[
            go.Scatter(name='Unique chatters during window', x=filtered_df['Time'], y=filtered_df['UUIW'],
                hovertemplate='Window: %{x}<br>Unique chatters: %{y}<br>Messages:<br>%{customdata}',
                customdata=filtered_df['MessagePeek'],
                mode='lines',
                line=dict(width=1),
                text=[f'<a href="{url}" target="_blank">{url}</a>' for url in filtered_df['timestamp_url']]
                )
        ])
    elif chart_type == 'Bar':
        fig = go.Figure(data=[
            go.Bar(name='Unique chatters during window', x=filtered_df['Time'], y=filtered_df['UUIW'],
                hovertemplate='Window: %{x}<br>Unique chatters: %{y}<br>Messages:<br>%{customdata}',
                customdata=filtered_df['MessagePeek'],
                text=[f'<a href="{url}" target="_blank">🔗 OPEN 🔗</a>' for url in filtered_df['timestamp_url']],
                textposition='auto',
                marker_color='purple'
                )
        ])

    st.plotly_chart(fig, config = {'scrollZoom': False})

    timestamp = st.number_input(
        f"Show messages {sliding_window} seconds before this moment (s)", value=None, placeholder="Enter a number", step=1
    )
    if timestamp is not None:
        vod_timestamp = time.strftime('%Hh%Mm%Ss', time.gmtime(timestamp-30))
        if vod_id is not None:
            st.page_link(f'https://www.twitch.tv/videos/{vod_id}?t={vod_timestamp}', label='Check time of VOD (-30s)')
        timestamp_df = filtered_df[(filtered_df['Time'] > (timestamp - sliding_window)) & (filtered_df['Time'] <= timestamp)].sort_values(by='Time', ascending=True).sort_index(level=0, kind="mergesort")
        timestamp_df = timestamp_df[['Message', 'UUIW', 'UUIW_msgs', 'timestamp_url']]
        st.dataframe(data=timestamp_df)

    st.subheader('Get top moments of broadcast to a table')
    SLACK = st.selectbox(
        "The time difference between top activity moments (s) at minimum to distinguish different situations from each other",
        (30, 45, 60, 75, 90, 120),
        index=5
    )
    TOP_N = st.selectbox(
        "TOP N",(10, 25, 50, 100)
    )

    candidates = filtered_df.sort_values("UUIW", ascending=False).reset_index(drop=True)

    chosen = []
    for _, row in candidates.iterrows():
        t = row["Time"]  # seconds

        # check if this time is too close to any already chosen
        if any(abs(t - prev["Time"]) <= SLACK for prev in chosen):
            continue

        chosen.append(row)

        if len(chosen) >= TOP_N:
            break
    
    top_peaks = pd.DataFrame(chosen)
    top_df = top_peaks[["Time", "UUIW", "UUIW_msgs", "timestamp_url"]].copy()

    # Turn the URL column into clickable links
    top_df['Time'] = top_df['Time'].apply(
        lambda x: format_seconds_to_ts(x)
    )
    top_df["timestamp_url"] = top_df["timestamp_url"].apply(
        lambda x: f"<a href='{x}' target='_blank'>🔗</a>"
    )
    # Escape all message text to be safe
    top_df["UUIW_msgs"] = top_df["UUIW_msgs"].apply(lambda x: html.escape(str(x)))
    # Rename columns for nicer display (optional)
    top_df = top_df.rename(columns={
        "Time": "⏱️",
        "UUIW": "Unique Users In Window",
        "UUIW_msgs": "💌",
        "timestamp_url": "🔗"
    })
    # Render as HTML so the <a> tags remain clickable
    st.markdown(top_df.to_html(escape=False, index=False), unsafe_allow_html=True)
else:
    st.markdown('''
            1. Download and export Twitch VOD chat log with https://www.twitchchatdownloader.com/ 
            2. Upload it here for analysis
            ''')