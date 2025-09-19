import time
import html
import pandas as pd
import polars as pl

def format_messages(messages):
    if messages is not None and len(messages) > 1:
        msgs = messages.split(" || ")[:30]
        return "<br>- ".join(msgs)
    else:
        return ''

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

def add_sliding_window_lazy(df: pl.DataFrame, window_size: int) -> pl.DataFrame:
    # Make a datetime column for rolling windows
    lf = df.lazy().with_columns(
        (pl.col("Time") * 1_000_000_000).cast(pl.Datetime("ns")).alias("Time_dt")
    )

    # Unique users per window
    uuiw = (
        lf.group_by_dynamic(
            index_column="Time_dt",
            every=f"{window_size}s",
            period=f"{window_size}s",
            closed="right",
        )
        .agg(pl.col("User").n_unique().alias("UUIW"))
    )

    # Concatenate one message per user per window
    msgs = (
        lf.group_by_dynamic(
            index_column="Time_dt",
            every=f"{window_size}s",
            period=f"{window_size}s",
            closed="right",
            by="User",
        )
        .agg(pl.col("Message").first().alias("Msg"))
        .group_by("Time_dt")
        .agg(pl.col("Msg").str.concat(" || ").alias("UUIW_msgs"))
    )

    lf.with_columns(
        pl.col("UUIW_msgs")
        .str.split(" || ")                      # split into list
        .list.slice(0, 30)                      # take first 30
        .list.eval(pl.element().str.slice(0, 30))  # truncate each element to 30 chars
        .list.join("<br>- ")                    # join with line breaks
        .alias("MessagePeek")
    )

    # Join everything back
    result = (
        lf.join(uuiw, on="Time_dt", how="left")
          .join(msgs, on="Time_dt", how="left")
          .drop("Time_dt")
          .collect()
    )

    return result


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
