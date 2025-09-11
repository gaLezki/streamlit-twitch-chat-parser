import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import time

st.set_page_config(page_title="CSV Loader", layout="wide")

st.title("📂 VOD Chat Analyzer")
vod_id = st.number_input("Enter VOD id here for URL purposes", step=1, value=None, placeholder='VOD id')
SLIDING_WINDOW = st.select_slider(
    "Select size of sliding window (s)",
    options=[
       6,7,8,9,10,11,12,13,14,15
    ],
    value=12
)

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

@st.cache_data
def load_csv(file):
    return pd.read_csv(file, encoding='utf-8', on_bad_lines='warn')

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file is not None:
    df = load_csv(uploaded_file)
    st.success("✅ File loaded and cached!")

    df.rename(columns={'time': 'Time', 'user_name': 'User', 'message': 'Message'}, inplace=True)
    values = st.slider("Select a time range for messages to be studied", df["Time"].min(), df["Time"].max(), (df["Time"].min(), df["Time"].max()))
    df = df[(df['Time'] > values[0]) & (df['Time'] < values[1])]

    filtered_df = apply_filters(df[['Time', 'User', 'Message']], filter_replies=True)    
    with st.spinner('Processing amounts of unique users within given sliding window'):
        filtered_df["UUIW"] = [
            filtered_df.loc[(df["Time"] >= t - SLIDING_WINDOW) & (filtered_df["Time"] <= t), "User"].nunique()
            for t in filtered_df["Time"]
        ]

    fig = go.Figure(data=[
        go.Scatter(name='Unique chatters during window', x=filtered_df['Time'], y=filtered_df['UUIW'],
            hovertemplate='Window: %{x}<br>Unique chatters: %{y}',
            mode='lines',
            line=dict(width=1)  # thinner line (default is 2)
            )
    ])

    st.plotly_chart(fig, config = {'scrollZoom': False})

    timestamp = st.number_input(
        f"Show messages {SLIDING_WINDOW} seconds before this moment (s)", value=None, placeholder="Enter a number", step=1
    )
    if timestamp is not None:
        vod_timestamp = time.strftime('%Hh%Mm%Ss', time.gmtime(timestamp-30))
        if vod_id is not None:
            st.page_link(f'https://www.twitch.tv/videos/{vod_id}?t={vod_timestamp}', label='Check time of VOD (-30s)')
        st.dataframe(data=filtered_df[(filtered_df['Time'] > (timestamp - SLIDING_WINDOW)) & (filtered_df['Time'] <= timestamp)].sort_values(by='Time', ascending=True).sort_index(level=0, kind="mergesort"))