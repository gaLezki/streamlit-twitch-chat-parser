import streamlit as st
import html

from processing import format_seconds_to_ts

def render_top_table(df):
    df = df[["Time", "timestamp_url", "UUIW", "UUIW_msgs"]].copy()
    df["Time"] = df["Time"].apply(format_seconds_to_ts)
    df["timestamp_url"] = df["timestamp_url"].apply(lambda x: f"<a href='{x}' target='_blank'>🔗</a>")
    df["UUIW_msgs"] = df["UUIW_msgs"].apply(lambda x: html.escape(str(x)))

    df = df.rename(
        columns={
            "Time": "⏱️",
            "Timestamp": "⏱️ (s)",
            "timestamp_url": "🔗",
            "UUIW": "Unique Users",
            "UUIW_msgs": "💌"
        }
    )

    st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
