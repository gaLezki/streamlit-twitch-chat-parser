import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from wordcloud import WordCloud
import plotly.offline as pyo
import sys
import os
import fetch_vod_chat

# Currently eats csv files created by https://www.twitchchatdownloader.com/
# Download VOD Chat export, rename it to [vodid].csv, i.e. 1828948529.csv and
# put it in import folder
# Run with python chat_parser.py [vod_id]

ADJUSTMENT = 15
WINDOW_SIZE = 15
TOP_ROWS_PCT = 5
MIN_UNIQUE_CHATTERS = 5

# Read CSV file into a DataFrame
def read_csv_file(filename):
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'import')
    file_path = os.path.join(folder_path, filename)
    df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='warn')
    return df

# Read CSV file into a DataFrame
def write_csv_file(df, vod_id, type):
    folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'export')
    file_path = os.path.join(folder_path, vod_id)
    df.to_csv(file_path + f'_{type}.csv', encoding='utf-8', sep=',', index=False)
    return df

# Apply filters to the DataFrame
def apply_filters(df, filter_replies=True):
    # Filter out rows with user_name "nightbot"
    df = df[df['user_name'] != 'nightbot']
    
    # Filter out rows with messages starting with an exclamation mark or "@"
    if filter_replies == True:
        df = df[~df['message'].str.startswith(('!', '@'), na=False)]
    else:
        df = df[~df['message'].str.startswith(('!'), na=False)]
    
    return df

def group_rows_into_windows(df):
    # Group rows into windows
    df['window'] = (df['time'] // WINDOW_SIZE) * WINDOW_SIZE

    # Fill na messages
    df.fillna(value={'message': ''},inplace=True)

    # Group by the window and aggregate the data
    grouped_df = df.groupby('window').agg({
        'message': lambda x: ' | '.join(x),
        'user_name': 'nunique',
        'message_count': 'count'
    }).reset_index() 

    # Sort by message_count in descending order
    grouped_df = grouped_df.sort_values(by='user_name', ascending=False)

    # Filter out smaller than minimum message windows
    grouped_df = grouped_df[grouped_df['user_name'] >= MIN_UNIQUE_CHATTERS]

    return grouped_df

def get_highscores(df):
    highscore_df = df.groupby('user_name').agg({
        'message_count': 'count',
        'message': 'nunique'
    }).reset_index().sort_values(by='message_count', ascending=False)
    return highscore_df

def get_wordcounts(df):
    wordcloud_df = df['message'].str.split()
    wordcloud_df = wordcloud_df.explode('message')
    wordcloud_df = wordcloud_df.str.replace('[\,\?\!\"]', '', regex=True)
    wordcloud_df = wordcloud_df.str.lower()
    # wordcloud_df = wordcloud_df.apply(leave_unique_words, axis=1) # TO-DO korjaa tää käyttökelposeks
    return wordcloud_df.value_counts().to_dict()

# Convert seconds to correct format for Twitch VOD timestamp, i.e. 00h01m00s
def format_vod_timestamp(seconds):
    if seconds > ADJUSTMENT:
        seconds = seconds - ADJUSTMENT
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f'{hours:02d}h{minutes:02d}m{seconds:02d}s'

def leave_unique_words(input_string):
    words_list = input_string.split()
    unique_words_list = []
    for word in words_list:
        if word not in unique_words_list:
            unique_words_list.append(word)
    output_string = ' '.join(unique_words_list)
    return output_string

def drawFigure(df, x_column, y_column, highscore_df, wordcloud_dict):
    # Sort by time
    df = df.sort_values(x_column)

    # Format message column
    df['formatted_message'] = df['message'].apply(format_messages)
 
    # Create a bar graph using Plotly
    bar = go.Figure(data=[
        go.Bar(name='Unique chatters during window', x=df[x_column], y=df[y_column],
            hovertemplate='Window: %{x}<br>Unique chatters: %{y}<br>Messages:<br>%{customdata}',
            customdata=df['formatted_message'],
            marker_color='steelblue',
            text=[f'<a href="{url}" target="_blank">{url}</a>' for url in df['timestamp_url']],
            textposition='auto')
        ])    

    # Create the table
    table = go.Figure(data=[
        go.Table(
        name='Top Chatters',
        header=dict(values=list(highscore_df.columns)),
        cells=dict(values=[highscore_df[col] for col in highscore_df.columns]),
        type='table'
    )])

    # Generate the word cloud
    wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(wordcloud_dict)
    # Convert the word cloud to a Plotly trace
    wordcloud_fig = go.Figure(data=[go.Image(z=wordcloud.to_array())])

    bar.update_layout(
        title='Unique chatters distribution',
        xaxis_title='Time window',
        yaxis_title=f'Unique chatters during window, showing minimum of {MIN_UNIQUE_CHATTERS}',
        font=dict(
        family='Arial',
        size=12,
        color='black'
        ),
    )

    # Display the graph
    # fig.show()
    pyo.plot(bar, filename='export/bar_chart.html')
    pyo.plot(table, filename='export/table.html')
    pyo.plot(wordcloud_fig, filename='export/wordcloud.html')

# Define a function to format the messages
def format_messages(messages):
    # Limit the messages to the first 10
    messages_array = messages.split(' | ')[:30]
    messages = '<br>- '.join(messages_array)
    return messages

# Main program
if __name__ == "__main__":
    vod_id = sys.argv[1]
    csv_filename = vod_id + '.csv'
    if os.path.isfile(f'{os.path.dirname(os.path.abspath(__file__))}/import/{csv_filename}'):
        df = read_csv_file(csv_filename)
    else:
        status = fetch_vod_chat.download_chat_log(vod_id)
        if status == True:
            df = read_csv_file(csv_filename)
        else:
            quit('Vod id csv not found and download failed')

    # Add a count column to track the number of messages per row
    df['message_count'] = 1

    # Apply filters to the DataFrame
    filtered_df = apply_filters(df, filter_replies=True)
    rolling_df = filtered_df
    rolling_df["unique_users_in_window"] = [
        rolling_df.loc[(filtered_df["time"] >= t - WINDOW_SIZE) & (rolling_df["time"] <= t), "user_name"].nunique()
        for t in rolling_df["time"]
    ]
    # TO-DO tee joku systeemi että tästä saa ulos sekä tään viivagraafin rullaavalla että palikkagraafin staattisilla
    replies_included_df = apply_filters(df, filter_replies=False)

    highscore_df = get_highscores(replies_included_df)
    wordcloud_dict = get_wordcounts(filtered_df)
    grouped_df = group_rows_into_windows(filtered_df)

    # Convert window column to %H:%M:%S format
    grouped_df['timestamp'] = grouped_df['window'].apply(format_vod_timestamp)

    # Format timestamp url and write csv
    # Extra space after timestamp allows using shift-click straight from VSCode without next column getting stuck to url
    grouped_df['timestamp_url'] = f'https://www.twitch.tv/videos/{vod_id}?t=' + grouped_df['timestamp']# + ' '
    write_csv_file(grouped_df[['user_name', 'message_count', 'timestamp_url', 'message']], vod_id, 'all')
    # print(grouped_df[['timestamp_url', 'message_count', 'message']]) # debug

    # Define amount of top rows to show sorted by timestamps instead of message_count  
    top_n_pct_row_amount = round(len(grouped_df.index) / (100 / TOP_ROWS_PCT))

    top_windows_df = grouped_df.nlargest(top_n_pct_row_amount, 'user_name').sort_values('window')
    write_csv_file(top_windows_df[['user_name', 'message_count', 'timestamp_url', 'message']], vod_id, 'top')
    # print('top_windows_df: ', top_windows_df) # debug

    grouped_df['timestamp_dt'] = pd.to_datetime(grouped_df['window'], unit='s').dt.time
    drawFigure(grouped_df, 'timestamp_dt', 'user_name', highscore_df, wordcloud_dict)

    