
# Import the external library
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use the Agg backend
import matplotlib.pyplot as plt
from PIL import Image
import urllib.request
import subprocess
import asyncio
import aiohttp
from datetime import datetime, timedelta, date
import time
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import nest_asyncio
import pickle
import json
import re
import requests
import hockey_rink
from hockey_rink import NHLRink, RinkImage
from PIL import Image

# Ensure Python can find the data module
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the function from season_data.py
from data.season_data import load_season_data

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

image = Image.open('logo.png')
st.image(image)
# Update pip to the latest version
subprocess.run(["pip", "install", "--upgrade", "pip"])

# List of pip commands to run
pip_commands = [
    ["pip", "install", "pytz"],
    ["pip", "install", "nest_asyncio"],
    ["pip", "install", "aiohttp"],
    ["pip", "install", "git+https://github.com/the-bucketless/hockey_rink.git"]
]

# Run each pip install command
for command in pip_commands:
    result = subprocess.run(command, capture_output=True, text=True)
# subprocess.run(["pip", "install", "pytz"])
# subprocess.run(["pip", "install", "nest_asyncio"])
# subprocess.run(["pip", "install", "aiohttp"])

# #Install the library from the GitHub repository using pip within your Streamlit app
# subprocess.run(["pip", "install", "git+https://github.com/the-bucketless/hockey_rink.git"])


primaryColor="#fafaff"
backgroundColor="#e1dee9"
secondaryBackgroundColor="#d5cfe1"
textColor="#262730"
font="Garamond"


# CSS for tables
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>   """

center_heading_text = """
    <style>
        .col_heading   {text-align: center !important}
    </style>          """
    
center_row_text = """
    <style>
        td  {text-align: center !important}
    </style>      """

# Inject CSS with Markdown

st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.markdown(center_heading_text, unsafe_allow_html=True) 
st.markdown(center_row_text, unsafe_allow_html=True) 

# More Table Styling

def color_surplusvalue(val):
    if str(val) == '0':
        color = 'azure'
    elif str(val)[0] == '-':
        color = 'lightpink'
    else:
        color = 'lightgreen'
    return 'background-color: %s' % color

heading_properties = [('font-size', '16px'),('text-align', 'center'),
                      ('color', 'black'),  ('font-weight', 'bold'),
                      ('background', 'e1dee9'),('border', '1.2px solid')]

cell_properties = [('font-size', '16px'),('text-align', 'center')]

dfstyle = [{"selector": "th", "props": heading_properties},
               {"selector": "td", "props": cell_properties}]

# Expander Styling

#st.markdown(
    #     """
    # <style>
    # .streamlit-expanderHeader {
    # #   font-weight: bold;
    #     background: #e1dee9;
    #     font-size: 180px;
    # }
    # </style>
    # """,
    #     unsafe_allow_html=True,
    # )
    
custom_css = """
<style>
    .streamlit-tabs-label {
        font-size: 120px;  /* You can adjust the font size as needed */
    }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)
  
##########################################
##  Title, Tabs, and Sidebar            ##
##########################################
#Explore NHL Advanced Stats, Simply
st.title("Check This Data")
st.markdown('''##### <span style="color: #aaaaaa">Explore NHL Advanced Stats, Simply</span>
            ''', unsafe_allow_html=True)
                
tab_bug, tab_player, tab_games = st.tabs(["Scores", "Goals", "Matchups"])
st.sidebar.markdown(" ## Make Selections")

##########################################
##  Matchup Sidebar                     ##
##########################################
# Function to fetch game data for a specific date
def fetch_game_data(start_date, end_date):
    base_url = "https://api-web.nhle.com/v1/schedule/"
    daily_games = pd.DataFrame()
    
    current_date = start_date
    seen_dates = set()

    while current_date <= end_date:
        formatted_date = current_date.strftime("%Y-%m-%d")
        api_url = f"{base_url}{formatted_date}"
        response = requests.get(api_url)
        
        if response.status_code == 200:
            response_text = response.text
            json_data = json.loads(response_text)

            if 'gameWeek' in json_data:
                game_week = json_data['gameWeek']
                game_week_df = pd.DataFrame(game_week)
                game_week_df = game_week_df[game_week_df['numberOfGames'] != 0]

                if formatted_date not in seen_dates:
                    seen_dates.add(formatted_date)
                    daily_games = pd.concat([daily_games, game_week_df], ignore_index=True)
            else:
                st.warning(f"No games found for {formatted_date}")
        else:
            st.error(f"Request failed with status code {response.status_code}")

        current_date += timedelta(weeks=1)

    return daily_games

# Function to process game data into a DataFrame
def process_game_data(daily_games):
    game_week_details = pd.json_normalize(daily_games['games'])
    dfs = {}

    for i in range(len(game_week_details.columns)):
        game_info = pd.json_normalize(game_week_details[i]) if game_week_details[i] is not None else pd.DataFrame()
        df_name = f'game_test{i}'
        dfs[df_name] = game_info

    combined_df = pd.concat(dfs.values(), ignore_index=True).dropna(how='all')
    combined_df = combined_df[['id', 'season', 'startTimeUTC', 'gameType', 'awayTeam.id', 'awayTeam.abbrev',
                                'homeTeam.id', 'homeTeam.abbrev', 'homeTeam.logo', 'awayTeam.logo',
                                'homeTeam.placeName.default', 'awayTeam.placeName.default',
                                'awayTeam.score', 'homeTeam.score', 'winningGoalScorer.playerId', 
                                'winningGoalie.playerId', 'gameState']].convert_dtypes()

    combined_df['link'] = 'https://api-web.nhle.com/v1/gamecenter/' + combined_df['id'].astype(str) + '/play-by-play'
    combined_df = combined_df.dropna(subset=['id']).query('gameState == "OFF"')
    combined_df['startTimeUTC'] = pd.to_datetime(combined_df['startTimeUTC'])
    combined_df = combined_df.rename(columns={'id': 'game_id'})
    
    # Convert 'startTimeUTC' to Eastern Time
    utc_timezone = pytz.utc
    eastern_timezone = pytz.timezone('America/New_York')
    combined_df['game_date'] = combined_df['startTimeUTC'].dt.tz_convert(eastern_timezone)
    combined_df['game_date'] = combined_df['game_date'].dt.strftime('%Y-%m-%d')
    combined_df.drop('startTimeUTC', axis=1, inplace=True)

    return combined_df

##########################################
## Scorebug Tab                         ##
##########################################

with tab_bug:
    st.title("NHL Scorebug")

    # Date range for fetching game data
    start_date = datetime.strptime("2024-10-08", "%Y-%m-%d")
    end_date = datetime.strptime("2025-04-17", "%Y-%m-%d")

    # Fetch game data
    daily_games = fetch_game_data(start_date, end_date)
    if daily_games.empty:
        st.warning("No games found in the specified date range.")
    else:
        combined_df = process_game_data(daily_games)

        # Create a list of matchups for the selectbox in the sidebar
        locations = combined_df[['game_id', 'awayTeam.abbrev', 'homeTeam.abbrev']]
        matchups = locations['homeTeam.abbrev'] + ' vs ' + locations['awayTeam.abbrev']

        # Sidebar selectbox for matchups
        selected_match = st.sidebar.selectbox('Select a match:', matchups)

        # Display selected matchup details
        selected_game_index = matchups.tolist().index(selected_match)  # Get index of the selected match
        selected_game = combined_df.iloc[selected_game_index]  # Get the corresponding game data

        # Display score bug information
        st.subheader(f"Score Bug for {selected_match}")
        st.write(f"Home Team: {selected_game['homeTeam.abbrev']}, Score: {selected_game['homeTeam.score']}")
        st.write(f"Away Team: {selected_game['awayTeam.abbrev']}, Score: {selected_game['awayTeam.score']}")
        st.write(f"Game State: {selected_game['gameState']}")
        st.write(f"Game Link: [Play by Play]({selected_game['link']})")

##########################################
## Player Tab                           ##
##########################################
with tab_player:
    @st.cache_data(show_spinner=True) 
    def season_data():
        try:
            # Call the function from season_data.py
            season_totals = load_season_data()

            return season_totals
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None
    def load_players(season_totals):
        # Create new columns based on the existing ones
        season_totals['Name'] = season_totals['player_name']
        season_totals['player_id'] = season_totals['player_id']
        season_totals['Goals'] = season_totals['g']
        season_totals['Points'] = season_totals['p']
        season_totals['Games Played'] = season_totals['gp']
        season_totals['Goals per Game'] = season_totals['gpg']
        season_totals['Sweater Number'] = season_totals['sweaterNumber']
        season_totals['Team'] =  season_totals['team_name'] 
        season_totals['Position'] =  season_totals['positionCode'] 
        # Select specific columns to return
        selected_columns = ['Name', 'player_id', 'Sweater Number', 'Team', 'Position', 'Goals', 'Points', 'Games Played', 'Goals per Game']
        players_df = season_totals[selected_columns]
        return players_df
    
    # players_df = load_players()
    # cols = ['Name','Position','Team','Goals']
    # Streamlit app
    season_totals = load_season_data()
    if season_totals is not None:
            # Extract player data from season totals
        players_df = load_players(season_totals)
        if players_df is not None:
            # Create a mapping of player names to IDs
            player_id_mapping = {row['Name']: row['player_id'] for index, row in players_df.iterrows()}
            # Player selection dropdown (display names, but keep player IDs hidden)
            selected_player_name = st.selectbox('Select a player:', list(player_id_mapping.keys()))
            # Retrieve the corresponding player ID
            selected_player_id = player_id_mapping[selected_player_name]
        else:
            st.error("Player data could not be loaded.")
    else:
        st.error("Season data could not be loaded.")

    # Get the player ID based on the selected player name
    selected_player_id = player_id_mapping[selected_player_name]

# @@ -328,6 +332,22 @@ def load_map():
#     ''', unsafe_allow_html=True)
#     st.markdown("<br>", unsafe_allow_html=True)



    # Select only the desired columns from the DataFrame
    selected_columns = ['Name', 'player_id', 'Games Played', 'Team', 'Position','Goals', 'Points', 'Games Played', 'Goals per Game']# Replace with your actual column names

    # Create an HTML table with desired styling
    st.write(f'''
    <table style="background: #d5cfe1; border: 1.2px solid; width: 100%">
    <tr>
        <td style="font-weight: bold;">Name</td>
        <td style="font-weight: bold;">Position</td>
        <td style="font-weight: bold;">Team</td>
        <td style="font-weight: bold;">Games Played</td>
        <td style="font-weight: bold;">Goals</td>
        <td style="font-weight: bold;">Goals per Game</td>
        <td style="font-weight: bold;">Points</td>
    </tr>
     <tr>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Name'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Position'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Team'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Games Played'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Goals'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Goals per Game'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == selected_player_name, 'Points'].values[0]}</td> 
    </tr>
    </table>
    ''', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # ## goal mapping
    # player_goals = goal_mapping[goal_mapping['Name'] == selected_player_name]

    # # Create an NHLRink object
    # rink = hockey_rink.NHLRink(rotation=270, net={"visible": False})

    # # Define the figure and axes for the rink map
    # fig, ax = plt.subplots(1, 1, figsize=(10, 16)) 

    # # Draw the rink on the single Axes object
    # rink.draw(display_range="half", ax=ax)

    # # Scatter plot for goals
    # rink.scatter(
    #     "x_adjusted", "y_adjusted", ax=ax,
    #     facecolor="white", edgecolor="black", s=500,
    #     data=player_goals
    # )

    # # Add text for goal numbers
    # rink.text(
    #     "x_adjusted", "y_adjusted", "goal_no", ax=ax,
    #     ha="center", va="center", fontsize=8, 
    #     data=player_goals
    # )

    # # Additional Test
    # location_texth = rink.text(
    #     0.5, 0.05, selected_player_name, ax=ax,
    #     use_rink_coordinates=False,
    #     ha="center", va="center", fontsize=20,
    # )

    # # Display the rink map
    # st.pyplot(fig)

    # # Rest of your code for player information and goals
    # st.write("Player Goals Detail:")
    # st.write(player_goals)

    # text = "Ice rink heat map package from [The Bucketless](https://github.com/the-bucketless/hockey_rink)"
    # st.markdown(text, unsafe_allow_html=True)

##########################################
## Explore Tab  League-Wide Stats       ##
##########################################
