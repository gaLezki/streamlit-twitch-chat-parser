import plotly.graph_objects as go

def make_chart(df, chart_type):
    if chart_type == "Line":
        fig = go.Figure(
            data=[
                go.Scatter(
                    name="",
                    x=df["Time"],
                    y=df["UUIW"],
                    hovertemplate="Unique chatters: %{y}<br>Message preview:<br>%{customdata}",
                    customdata=df["MessagePeek"].str.split("<br>").str[:50].str.join("<br>"),
                    mode="lines",
                    line=dict(width=1)
                )
            ]
        )
    elif chart_type == "Bar":
        fig = go.Figure(
            go.Bar(
                name="",
                x=df["Timestamp"].astype('string'),
                y=df["UUIW"],
                hovertemplate="Unique chatters: %{y}<br>Message preview:<br>%{customdata}",
                customdata=df["MessagePeek"].str.split("<br>").str[:50].str.join("<br>"),
                text=[
                    f"<a href='{url}' target='_blank'>🔗 OPEN 🔗</a>"
                    for url in df["timestamp_url"]
                ],
                textposition="outside",
                marker_color="purple"
            )
        )
        fig.update_layout(barmode="overlay")
    fig.update_layout(hovermode="x unified", height=750)
    return fig
