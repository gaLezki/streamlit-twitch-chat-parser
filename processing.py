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

# Method using pandas, leaving this here just in case I want to create performance comparisons at some point
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

def add_sliding_windows(df: pl.DataFrame, window_size: int, ignore_threshold: int = 0) -> pl.DataFrame:
    """
    Use LazyFrame.rolling() for sliding windows matching pandas [t - window_size, t] behavior.
    Adds columns UUIW, UUIW_msgs, MessagePeek.
    """

    # Ensure df has needed cols
    assert {"Time", "User", "Message"}.issubset(set(df.columns)), "Need Time, User, Message"

    lf = df.lazy()
    
    rolled = (
        lf.rolling(index_column="Time", period=f"{window_size}i", closed="both")
        .agg([
            pl.col("User").n_unique().alias("UUIW"),
            pl.col("Message").str.concat(" || ").alias("UUIW_msgs")
        ])
    )
    
    # Collapse per-time duplicates (aggregate messages + keep UUIW)
    rolled = (
        rolled.group_by("Time")
        .agg([
            pl.col("UUIW").max(),  # just take max, they’re the same within each Time
            pl.col("UUIW_msgs").max()
        ])
    )

    
    out_lf = (
        lf.join(rolled, on="Time", how="left")
        .with_columns([
            pl.col("UUIW").fill_null(0).cast(pl.Int64),
            pl.col("UUIW_msgs").fill_null(""),
            pl.when(pl.col("UUIW_msgs") == "")
              .then(pl.lit(""))
              .otherwise(
                  pl.col("UUIW_msgs")
                    .str.split(" || ")
                    .list.slice(0, 30)
                    .list.eval(pl.element().str.slice(0, 30))
                    .list.join("<br>")
              )
              .alias("MessagePeek"),
        ])
    )
    out_lf = (out_lf.filter(pl.col('UUIW') >= ignore_threshold))
    out_lf = out_lf.select(['Time', 'UUIW', 'UUIW_msgs', 'MessagePeek'])
    return out_lf.collect()

def add_tumbling_window(df: pl.DataFrame, window_size: int, ignore_threshold: int = 0) -> pl.DataFrame:
    """
    Compute UUIW (unique users) and UUIW_msgs (concat first message per user)
    using fixed-length tumbling windows like 0–11s, 12–23s, etc.
    Adds a 'window_start_ts' column formatted as 00h00m00s style.
    """
    lf = df.lazy()
    # Assign each row to a window_id
    lf = lf.with_columns(
        (pl.col("Time") // window_size).alias("window_id")
    )

    # Aggregate per window
    uuiw = (
        lf.group_by("window_id")
        .agg(pl.col("User").n_unique().alias("UUIW"))
    )

    # One first message per user, then concat
    msgs = (
        lf.group_by(["window_id", "User"])
          .agg(pl.col("Message").first().alias("Msg"))
          .group_by("window_id")
          .agg(pl.col("Msg").str.concat(" || ").alias("UUIW_msgs"))
    )

    combined = uuiw.join(msgs, on="window_id")

    # Add numeric start and end seconds
    result = combined.with_columns([
        (pl.col("window_id") * window_size).alias("window_start"),
        ((pl.col("window_id") + 1) * window_size - 1).alias("window_end"),
    ])

    
    result = result.with_columns([
            pl.col("UUIW").fill_null(0).cast(pl.Int64),
            pl.col("UUIW_msgs").fill_null(""),
            pl.when(pl.col("UUIW_msgs") == "")
              .then(pl.lit(""))
              .otherwise(
                  pl.col("UUIW_msgs")
                    .str.split(" || ")
                    .list.slice(0, 30)
                    .list.eval(pl.element().str.slice(0, 30))
                    .list.join("<br>")
              )
              .alias("MessagePeek"),
        ])
    result = (result.filter(pl.col('UUIW') >= ignore_threshold))
    result = result.rename({'window_start': 'Time'}).select(['Time', 'UUIW', 'UUIW_msgs', 'MessagePeek'])
    
    return result.collect().sort("Time")


def get_top_peaks(df, slack, n):
    candidates = df.sort_values("UUIW", ascending=False).reset_index(drop=True)
    chosen = []

    for _, row in candidates.iterrows():
        t = int(row["Time"])
        if any(abs(t - int(prev["Time"])) <= slack for prev in chosen):
            continue
        chosen.append(row)
        if len(chosen) >= n:
            break

    return pd.DataFrame(chosen)
