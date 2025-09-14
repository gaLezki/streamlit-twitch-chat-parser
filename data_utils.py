import pandas as pd
import streamlit as st
import os
import re

def apply_filters(df, filter_replies=True):
    df = df[df["User"] != "nightbot"]
    if filter_replies:
        df = df[~df["Message"].str.startswith(("!", "@"), na=False)]
    else:
        df = df[~df["Message"].str.startswith(("!"), na=False)]
    return df

def parse_vod_id(filename):
    basename, _ = os.path.splitext(filename)
    match = re.search(r"(\d+)", basename)
    return int(match.group(1)) if match else None

@st.cache_data
def load_csv(file):
    return pd.read_csv(file, encoding="utf-8", on_bad_lines="warn")
