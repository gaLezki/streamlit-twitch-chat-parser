import time
import html
import pandas as pd

def format_messages(messages):
    msgs = messages.split(" || ")[:30]
    return "<br>- ".join(msgs)

def format_seconds_to_ts(seconds):
    return time.strftime("%Hh%Mm%Ss", time.gmtime(seconds))

def format_vod_timestamp_url(time_in_seconds, vod_id):
    vod_timestamp = time.strftime("%Hh%Mm%Ss", time.gmtime(time_in_seconds - 30))
    if vod_id:
        return f"https://www.twitch.tv/videos/{vod_id}?t={vod_timestamp}"
    return None

def compute_sliding_windows(df, sliding_window):
    uuiw_counts, uuiw_messages = [], []

    for t in df["Time"]:
        window = df[(df["Time"] >= t - sliding_window) & (df["Time"] <= t)]

        uuiw_counts.append(window["User"].nunique())

        if window.empty:
            msgs = ""
        else:
            msgs_list = (
                window.groupby("User")["Message"]
                .first()
                .dropna()
                .astype(str)
                .tolist()
            )
            msgs = " || ".join(msgs_list) if msgs_list else ""

        uuiw_messages.append(msgs)

    df["UUIW"] = uuiw_counts
    df["UUIW_msgs"] = uuiw_messages
    df["MessagePeek"] = df["UUIW_msgs"].apply(format_messages)

    return df

def get_top_peaks(df, slack, n):
    candidates = df.sort_values("UUIW", ascending=False).reset_index(drop=True)
    chosen = []

    for _, row in candidates.iterrows():
        t = row["Time"]
        if any(abs(t - prev["Time"]) <= slack for prev in chosen):
            continue
        chosen.append(row)
        if len(chosen) >= n:
            break

    return pd.DataFrame(chosen)
