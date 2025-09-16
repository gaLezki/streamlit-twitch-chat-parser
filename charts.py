import plotly.graph_objects as go

def make_chart(df, chart_type, hide_empty=False):
    if chart_type == "Line":
        return go.Figure(
            data=[
                go.Scatter(
                    name="",
                    x=df["Time"],
                    y=df["UUIW"],
                    hovertemplate="Window: %{x}<br>Unique chatters: %{y}<br>Messages:<br>%{customdata}",
                    customdata=df["MessagePeek"],
                    mode="lines",
                    line=dict(width=1),
                )
            ]
        )
    elif chart_type == "Bar":
        fig = go.Figure(
            data=[
                go.Bar(
                    name="",
                    x=df["Time"],
                    y=df["UUIW"],
                    hovertemplate="Window: %{x}<br>Unique chatters: %{y}<br>Messages:<br>%{customdata}",
                    customdata=df["MessagePeek"],
                    text=[
                        f"<a href='{url}' target='_blank'>🔗 OPEN 🔗</a>"
                        for url in df["timestamp_url"]
                    ],
                    textposition="auto",
                    marker_color="purple",
                )
            ]
        )
        if hide_empty:
            fig.update_xaxes(
                type="category",  # treats x as discrete categories
                categoryorder="array",
                categoryarray=df["Time"]  # preserves order
            )
        return fig
