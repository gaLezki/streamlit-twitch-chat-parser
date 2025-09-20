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

def add_sliding_window_lazy(df: pl.DataFrame, window_size: int, every: str = "1i") -> pl.DataFrame:
    """
    Returns a polars.DataFrame with added columns:
      - UUIW (unique users in window [t-window_size, t])
      - UUIW_msgs (one message per user joined with " || ")
      - MessagePeek (first up to 30 msgs, each truncated to 30 chars, joined with '<br>- ')
    """

    # ensure cols exist
    assert {"Time", "User", "Message"}.issubset(set(df.columns)), "Require Time, User, Message columns"

    lf = df.lazy()

    # unique users per sliding window (exclude null/empty users)
    uuiw = (
        lf.group_by_dynamic(
            index_column="Time",
            every=every,
            period=f"{window_size}i",
            closed="both",
        )
        .agg(
            # drop null/empty before counting
            pl.col("User").drop_nulls().filter(pl.col("User") != "").n_unique().alias("UUIW")
        )
    )
    # one message per user per window (first message for that user in that window), then concat per window
    msgs = (
        lf.group_by_dynamic(
            index_column="Time",
            every=every,
            period=f"{window_size}i",
            closed="both",
            by="User",
        )
        .agg(pl.col("Message").first().alias("Msg"))
        .group_by("Time")
        .agg(pl.col("Msg").str.concat(" || ").alias("UUIW_msgs"))
    )

    out_lf = (
        lf.join(uuiw, on="Time", how="left")
          .join(msgs, on="Time", how="left")
          .with_columns([
              pl.col("UUIW").fill_null(0).cast(pl.Int64),
              pl.col("UUIW_msgs").fill_null("").alias("UUIW_msgs"),
              # MessagePeek lazy construction
              pl.when(pl.col("UUIW_msgs") == "")
                .then(pl.lit(""))
                .otherwise(
                    pl.col("UUIW_msgs")
                      .str.split(" || ")
                      .list.slice(0, 30)                          # first 30 messages
                      .list.eval(pl.element().str.slice(0, 30))   # truncate each to 30 chars
                      .list.join("<br>- ")                        # join for tooltip
                ).alias("MessagePeek")
          ])
    )
    result_df = out_lf.collect()
    print('result_df: ', result_df.tail(50)) # debug
    #.drop("Time_dt")

    return result_df


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
