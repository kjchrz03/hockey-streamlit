
# subprocess.run(["pip", "install", "pytz"])
# subprocess.run(["pip", "install", "nest_asyncio"])
# subprocess.run(["pip", "install", "aiohttp"])

# Install the library from the GitHub repository using pip within your Streamlit app
# subprocess.run(["pip", "install", "git+https://github.com/the-bucketless/hockey_rink.git"])

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
from data.season_data import get_season_data

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

image = Image.open('logo.png')
st.image(image)


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

    def load_season_data():
        try:

            season_data = get_season_data()
            return season_data
        
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None

    # Function to transform and extract player data
    def load_players(season_data):
        try:
            # Create new columns based on the existing ones
            season_data['Name'] = season_data['player_name']
            season_data['player_id'] = season_data['playerId']
            season_data['Goals'] = season_data['g']
            season_data['Points'] = season_data['p']
            season_data['Games Played'] = season_data['gp']
            season_data['Goals per Game'] = season_data['gpg']
            season_data['Team'] = season_data['team_name']
            season_data['Position'] = season_data['position']

            # Select specific columns to return
            selected_columns = ['Name', 'player_id', 'Goals', 'Points', 'Games Played', 'Goals per Game', 'Position']
            players_df = season_data[selected_columns]
            
            return players_df
        except KeyError as e:
            st.error(f"Column missing in data: {e}")
            return None
        
    players_df = load_players()
    cols = ['Name','Position','Team','Goals', 'Points', 'Goals per Game']

    # # Streamlit app structure
    # with st.container():  # Using a container to structure the app
    #     st.header('Explore Player Goals')

        # Load season data
    season_data = load_season_data()

    if season_data is not None:
            # Extract player data from season totals
        players_df = load_players(season_data)
        st.write(players_df)

        # if players_df is not None:
        # Create a mapping of player names to IDs
        player_id_mapping = {row['Name']: row['player_id'] for index, row in players_df.iterrows()}

        # Player selection dropdown (display names, but keep player IDs hidden)
        selected_player_name = st.selectbox("Choose a player (or click below and start typing):", list(player_id_mapping.keys()), index=0)

        # Retrieve the corresponding player ID
        selected_player_id = player_id_mapping[selected_player_name]
        player_goals = players_df[players_df.Name == selected_player_name].Goals.to_list()[0]

        st.write(f"Selected Player ID: {selected_player_id}")

        st.write(f'''
            ##### <div style="text-align: center"> This season  <span style="color:blue">{selected_player_name}</span> has scored <span style="color:green">{player_goals}</span> goals.</div>
    ''', unsafe_allow_html=True)

    
       # Select only the desired columns from the DataFrame
        selected_columns = ['Name', 'Position', 'Team', 'Goals', 'Points', 'Goals per Game']  # Replace with your actual column names

        # Create an HTML table with desired styling
        st.write(f'''
        <table style="background: #d5cfe1; border: 1.2px solid; width: 100%">
        <tr>
            <td style="font-weight: bold;">Name</td>
            <td style="font-weight: bold;">Positiontd>
            <td style="font-weight: bold;">Team</td>
            <td style="font-weight: bold;">Goals</td>
            <td style="font-weight: bold;">Points</td>
            <td style="font-weight: bold;">Goals per Game</td>
        </tr>
        <tr>
            <td>{players_df.loc[players_df.Name == selected_player_name, 'Name'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == selected_player_name, 'Position'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == selected_player_name, 'Team'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == selected_player_name, 'Goals'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == selected_player_name, 'Points'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == selected_player_name, 'Goals per Game'].values[0]}</td>
        </tr>
        </table>
        ''', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

#goal scoring data
#     def load_map():
#         github_ice_map_url = 'data/ice_map_data.csv'
#         goal_mapping = pd.read_csv(github_ice_map_url)
#         goal_mapping['Name'] = goal_mapping['player_name']
#         goal_mapping['ID'] = goal_mapping['player_id']
#         goal_mapping['Goal Number'] = goal_mapping['goal_no']
#         goal_mapping['Adjusted X'] = goal_mapping['x_adjusted']
#         goal_mapping['Adjusted Y'] = goal_mapping['y_adjusted']
#         return goal_mapping
#     goal_mapping = load_map()
#     cols = ['Name','Goal Number','Adjusted X', 'Adjusted Y']


    ## goal mapping
    player_goals = goal_mapping[goal_mapping['Name'] == selected_player_name]

    # Create an NHLRink object
    rink = hockey_rink.NHLRink(rotation=270, net={"visible": False})

    # Define the figure and axes for the rink map
    fig, ax = plt.subplots(1, 1, figsize=(10, 16)) 

    # Draw the rink on the single Axes object
    rink.draw(display_range="half", ax=ax)

    # Scatter plot for goals
    rink.scatter(
        "x_adjusted", "y_adjusted", ax=ax,
        facecolor="white", edgecolor="black", s=500,
        data=player_goals
    )

    # Add text for goal numbers
    rink.text(
        "x_adjusted", "y_adjusted", "goal_no", ax=ax,
        ha="center", va="center", fontsize=8, 
        data=player_goals
    )

    # Additional Test
    location_texth = rink.text(
        0.5, 0.05, selected_player_name, ax=ax,
        use_rink_coordinates=False,
        ha="center", va="center", fontsize=20,
    )

    # Display the rink map
    st.pyplot(fig)

    # Rest of your code for player information and goals
    st.write("Player Goals Detail:")
    st.write(player_goals)

    text = "Ice rink heat map package from [The Bucketless](https://github.com/the-bucketless/hockey_rink)"
    st.markdown(text, unsafe_allow_html=True)

# ##########################################
# ## Explore Matchups                     ##
# ##########################################    

# with tab_games:
#     # game matchup data
#     def load_matchups():
#         github_shots_url = 'data/game_matchups.csv'
#         shots = pd.read_csv(github_shots_url)
#         shots['Event'] = shots['event']
#         shots['Matchup'] = shots['matchup']
#         unique_matchups = shots[['game_id', 'Matchup']].drop_duplicates()
#         shots = pd.merge(shots, unique_matchups, on='game_id', how='inner')
#         shots['Home Team'] = shots['home_team_name']
#         shots['Away Team'] = shots['away_team_name']
#         return shots

#     shots = load_matchups()
#     cols = ['Event', 'Matchup']

#     #game matchup logos
#     def load_logos():
#         github_logos_url = 'data/logos.csv'
#         logos = pd.read_csv(github_logos_url)
#         logos['Tri Code'] = logos['tri_code']
#         logos['Team ID'] = logos['id']
#         logos['Logo'] = logos['logo']
#         return logos

#     logos = load_logos()
#     cols = ['Tri Code','Team ID','Logo']

#     st.header('Explore Matchups')

#     # Create a mapping of game matchups to their corresponding game IDs
#     game_id_mapping = {row['matchup']: row['game_id'] for index, row in shots.iterrows()}

#     # Display the game matchup dropdown with hidden game IDs
#     selected_matchup = st.selectbox("Choose a matchup (or click below and start typing - through 11/7/2023):", list(game_id_mapping.keys()), index=0)

#     # Get the selected game ID based on the chosen matchup
#     selected_game_id = game_id_mapping[selected_matchup]

#     # You can now use selected_game_id to filter your shots data based on the chosen matchup
#     selected_matchup_shots = shots[shots['game_id'] == selected_game_id]

#     # Extract home team and away team names
#     selected_home_team = shots.loc[shots['game_id'] == selected_game_id, 'home_team_name'].values[0]
#     selected_away_team = shots.loc[shots['game_id'] == selected_game_id, 'away_team_name'].values[0]

#     # Calculate the number of goals in the selected matchup
#     number_of_goals = selected_matchup_shots[selected_matchup_shots['event'] == 'Goal']['event'].count()

#     # Group and count goals by 'name'
#     goals = selected_matchup_shots[selected_matchup_shots['event'] == 'Goal']
#     goal_counts = goals.groupby('name')['event'].count()

#     # Display information about the selected matchup and the number of goals
#     if selected_game_id in game_id_mapping.values():
    
#         # Table Columns
#         selected_columns = ['Matchup', 'Total Shot Attempts', 'Total Goals']  # Replace with your actual column names

#         # HTML Table
#         st.write(f'''
#         <table style="background: azure; border: 1.2px solid; width: 100%">
#         <tr>
#             <td style="font-weight: bold;">Matchup</td>
#             <td style="font-weight: bold;">Total Shot Attempts</td>
#             <td style="font-weight: bold;">Total Goals</td>
#         </tr>
#         <tr>
#             <td>{selected_home_team} vs. {selected_away_team}</td>
#             <td>{len(selected_matchup_shots)}</td>
#             <td>{number_of_goals}</td>
#         </tr>
#         </table>
#         ''', unsafe_allow_html=True)
#         st.markdown("<br>", unsafe_allow_html=True)
        
#         # Display the goal counts for each 'name'
#         st.write("Final Score:")
#         for name, count in goal_counts.items():
#             st.write(f"{name}: {count}")
        
#         st.write("Home team shot attempts (🟠) 🏒 Away team shot attempts (🔵) 🏒 Goals are (🟢)")
#         st.write("Some home teams like to start on the right, others on the left. I am working on making arena adjustments.")
#     ## goal mapping
#         # The rest of your script goes here
#         for period in [1, 2, 3]:
#             period_data = shots.query("game_id == @selected_game_id and period == @period")
            
#             # Find the home team's ID and away team's ID for the current period
#             home_team_id = period_data['home_team'].values[0]
#             away_team_id = period_data['away_team'].values[0]

#             # Retrieve the logo links for the home and away teams from your logo_df
#             home_team_logo_link = logos.loc[logos['id'] == home_team_id, 'logo'].values[0]
#             away_team_logo_link = logos.loc[logos['id'] == away_team_id, 'logo'].values[0]

#             fig, ax = plt.subplots(figsize=(12, 8))

#             # Map the triCode values to colors
#             period_data.loc[:, 'color'] = 'blue'  # Assign blue as the default color
#             period_data.loc[period_data['id'] == home_team_id, 'color'] = 'orange'

#             # Update shots marked as "Goals" with green color
#             period_data.loc[period_data['event'] == 'Goal', 'color'] = 'green'

#             rink = NHLRink(
#                 home_team_logo={
#                     "feature_class": RinkImage,
#                     "image_path": home_team_logo_link,
#                     "x": 55, "length": 50, "width": 42,
#                     "zorder": 15, "alpha": 0.5,
#                 },
#                 away_team_logo={
#                     "feature_class": RinkImage,
#                     "image_path": away_team_logo_link,
#                     "x": -55, "length": 50, "width": 42,
#                     "zorder": 15, "alpha": 0.5,
#                 }
#             )

#             # Switch the logos' positions for the second period
#             if period == 2:
#                 rink = NHLRink(
#                     home_team_logo={
#                         "feature_class": RinkImage,
#                         "image_path": away_team_logo_link,
#                         "x": 55, "length": 50, "width": 42,
#                         "zorder": 15, "alpha": 0.5,
#                     },
#                     away_team_logo={
#                         "feature_class": RinkImage,
#                         "image_path": home_team_logo_link,
#                         "x": -55, "length": 50, "width": 42,
#                         "zorder": 15, "alpha": 0.5,
#                     }
#                 )

#             # Use the 'color' column for dot colors
#             rink.scatter("x", "y", s=100, c=period_data['color'], edgecolor="white", data=period_data, ax=ax)

#             ax.set_title(f"Period {period} Shot Locations")
#             st.pyplot(fig) 


##########################################
## Explore Tab  League-Wide Stats       ##
##########################################
