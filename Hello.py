# Import the external library
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use the Agg backend
import matplotlib.pyplot as plt
from PIL import Image
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import requests
from hockey_rink import NHLRink, RinkImage
from pprint import pprint 
import subprocess
import logging
import warnings# Suppress the specific warning

logging.getLogger("streamlit").setLevel(logging.ERROR)
subprocess.run(["pip", "install", "--upgrade", "pip"])
subprocess.run(["pip", "install", "--upgrade", "streamlit"])
# List of pip commands to run
pip_commands = [
    ["pip", "install", "pytz"],
    ["pip", "install", "nest_asyncio"],
    ["pip", "install", "aiohttp"],
    ["pip", "install", "git+https://github.com/the-bucketless/hockey_rink.git"]
]

import nest_asyncio


# Run each pip install command
for command in pip_commands:
    result = subprocess.run(command, capture_output=True, text=True)

import pytz

import aiohttp
import time
import pickle
import re
import asyncio
import urllib.request
import hockey_rink

nest_asyncio.apply()

# Ensure Python can find the data module
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# print(sys.path)

# Import the function from season_data.py
from data.season_data import load_season_data, get_daily_games, get_standings_data

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

st.markdown(
        """
    <style>
    .streamlit-expanderHeader {
    #   font-weight: bold;
        background: #e1dee9;
        font-size: 180px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )
    
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
                
tab_bug, tab_goals, tab_games = st.tabs(["Scores", "Goals", "Matchups"])
st.sidebar.markdown("League-Wide Standings")

##########################################
##  Standingsidebar                     ##
##########################################
@st.cache_data(show_spinner=True)

def display_standings():
    try:
        standings =  get_standings_data()
        return standings
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def load_standings(standings):
    standings['Conference'] = standings['conferenceName']
    standings['Conference Rank'] = standings['conferenceSequence']
    standings['Division'] = standings['divisionName']
    standings['Division Rank'] = standings['divisionSequence']
    standings['Team'] = standings['team']
    standings['League Rank'] = standings['leagueSequence']
    standings['Games Played'] = standings['gamesPlayed']
    standings['logo'] = standings['teamLogo']  # Assuming this contains the SVG URL
    standings['Win Pctg'] = standings['winPctg']
    standings['Date'] = standings['date']
    standings['Points'] = standings['points']

    # Select specific columns to return
    selected_columns = ['Conference', 'Conference Rank', 'Division', 'Division Rank', 'Team', 'League Rank', 'Games Played', 'logo', 'Win Pctg', 
                        'Date', 'Points']
    league_standings_df = standings[selected_columns]
    return league_standings_df

def todays_standings():
    try:
        standings = display_standings()  # Fetch the data
        if standings is None:
            return  # Exit if no standings were fetched
            
        league_standings_df = load_standings(standings)
        #         Define colors for each division
        division_colors = {
            'Atlantic': '#FF5733',  # Example color for Atlantic
            'Metropolitan': '#33FF57',  # Example color for Metropolitan
            'Central': '#3357FF',  # Example color for Central
            'Pacific': '#FF33A1',  # Example color for Pacific
            # Add more divisions and their respective colors as needed
        }
    
        # Get today's date in the required format
        today = datetime.now().strftime("%B %d, %Y")
        st.sidebar.markdown(f"##### Today's Date: {today}")

        # Dropdown for division selection
        divisions = league_standings_df['Division'].unique().tolist()
        divisions.sort()
        divisions.append("League-Wide")  # Add League-Wide option

        # Select division from the sidebar
        selected_division = st.sidebar.selectbox("Select Division:", divisions)
        
        # Filter standings based on selection
        if selected_division == "League-Wide":
            # Get top 8 teams from each conference
            conference_teams = league_standings_df.groupby('Conference').apply(lambda x: x.nlargest(8, 'Points')).reset_index(drop=True)
            filtered_standings = conference_teams
            ranking = conference_teams['League Rank']  # Use League Rank for league-wide
        else:
            filtered_standings = league_standings_df[league_standings_df['Division'] == selected_division]
            ranking = filtered_standings['Division Rank']

        
        def create_dataframe(n):
            div_standings = []

            for index, row in filtered_standings.iterrows():
                team = row['Team']
                points = row['Points']
                division = row['Division']
                games_played = row['Games Played']
                logo_url = row['logo']  # SVG logo link
                div_standings.append(row)

            # Create DataFrame from the collected data
            return pd.DataFrame(div_standings)

        # Call the function to create the DataFrame
        div_standings = create_dataframe(8)
        # points_dict = {}

        # #Create visual representation for each team
        team = div_standings['Team']
        points = div_standings['Points']
        division = div_standings['Division']
        logo_url = div_standings['logo']
        games_played = div_standings['Games Played']


        max_games_played = games_played.max()
        max_points = 2 * max_games_played
        scale_height = 500

        html_content = """
        <div style="display: flex; flex-direction: column; align-items: flex-start;position: relative; height: {}px; margin: 20px 0;">
            <div style="position: absolute; left: 10%; width: 4px; height: 100%; background-color: red;"></div> 
            <div style="position: absolute; left: 5%; right: 80%; width: 20%; top: 0%; border-top: 5px solid black;"></div>
            <div style="position: absolute; left: 5%; top: -20px; color: orange; font-size: 15px;">Max Possible Points to Date: {}</div>
        """.format(scale_height, max_points)


        standing_points = {}

        for i, point in enumerate(points):
            position = scale_height - (point / max_points) * scale_height
            logo_url = div_standings['logo'].iloc[i]
            team = div_standings['Team'].iloc[i]
            division = div_standings['Division'].iloc[i]

            if point in standing_points:
                standing_points[point].append({
                    'team': team,
                    'logo': logo_url,
                    'division': division,
                })
                print(f"Appending to {point}: {team}")
            else:
                standing_points[point] = [{
                    'team': team,
                    'logo': logo_url,
                    'division': division,
                }]
                print(f"Creating new entry for {point}: {team}")
            
        sorted_standing_points = {k: standing_points[k] for k in sorted(standing_points.keys(), reverse=True)}


        label_vertical_offset = 0
        dot_vertical_offset = 0   



        # Iterate through the sorted standing points
        for point, teams in sorted_standing_points.items():
            # Calculate the vertical position for this point value
            position = scale_height - (point / max_points) * scale_height

            # Create the HTML content for the point value with logos
            html_content += """
            <div style="position: absolute; left: 0%; top: {}px; color: white; font-size: 15px; line-height: 10px;">{}</div>
            <div style="position: absolute; left: 15%; top: {}px; display: flex; gap: 5px;">
            """.format(position - label_vertical_offset, point, position - dot_vertical_offset)

            # Add the team logos for each team with the same point value
            for team_info in teams:
                logo_url = team_info['logo']
                division = team_info['division']

                # Logo with division-colored border
                html_content += """
                <div style="background-color: white; border: 1px solid {}; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin-left: 10px;">
                    <img src="{}" alt="{} Logo" width="50" height="50" style="border-radius: 50%;">
                </div>
                """.format(division_colors.get(division, 'grey'), logo_url, team_info['team'])

            # Close the flex container for logos
            html_content += "</div>"  # Close the logo flex container

            # Optional: Add a small dot or indicator below the logos (if needed)
            html_content += """
            <div style="position: absolute; left: 9%; top: {}px; width: 10px; height: 10px; background-color: {}; border-radius: 50%;"></div>
            """.format(position - dot_vertical_offset, '#FF5733')


        # Close the outer div
        html_content += "</div>"
        with st.sidebar:
            components.html(html_content, height=600, scrolling=True)

    except Exception as e:
        st.error(f"Error loading standings data: {e}")
        return None

todays_standings()
##########################################
## Scorebug Tab                         ##
##########################################

with tab_bug:
    current_date = datetime.now().strftime("%B %d, %Y")
    #current_date = "October 19, 2024"
    st.title("Today's Games")
    st.markdown(f'''##### <span style="color: #aaaaaa">{current_date}</span>''', unsafe_allow_html=True)
    # @st.cache_data(show_spinner=True) 
   
    def score_bug():
        try:
            daily_games = get_daily_games()  # Fetch external data
            return daily_games
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None

    def load_games(daily_games):
        # Create new columns based on the existing ones
        daily_games['Away Team'] = daily_games['awayTeam.abbrev']
        daily_games['away_logo'] = daily_games['awayTeam.logo']
        daily_games['Home Team'] = daily_games['homeTeam.abbrev']
        daily_games['home_logo'] = daily_games['homeTeam.logo']
        daily_games['Away Score'] = daily_games['awayTeam.score']
        daily_games['Home Score'] = daily_games['homeTeam.score']
        daily_games['Winning Goal Scorer'] = daily_games['winningGoalScorer.playerId']
        daily_games['Game Date'] = daily_games['game_date']
        daily_games['game_type'] = daily_games['gameType']
        daily_games['game_state'] = daily_games['gameState']

        # Select specific columns to return
        selected_columns = ['Away Team', 'away_logo', 'Away Score', 'Home Team', 'home_logo', 'Home Score', 'Winning Goal Scorer', 'game_type', 'Game Date', 'game_state']
        score_bug_df = daily_games[selected_columns]
        print(score_bug_df)
        return score_bug_df

    def todays_games():
        try:
            daily_games = score_bug()  # Fetch the data
            score_bug_df = load_games(daily_games)
            # Get today's date in the required format
            today = datetime.now().strftime("%Y-%m-%d")

            print(today)
            # Convert game date to a comparable format and filter for today's games
            score_bug_df.loc[:, 'Game Date'] = pd.to_datetime(score_bug_df['Game Date']).dt.strftime("%Y-%m-%d")

            todays_games_df = score_bug_df[score_bug_df['Game Date'] == today]

            # Loop through each game and display it in a table format
            for index, row in todays_games_df.iterrows():
                # Create table with headers
                game_row = f"""
                <table style="border-collapse: separate; border: none; border-spacing: 0 10px;">  <!-- 10px gap -->
                    <thead>
                        <tr>
                            <th style="border: none;"></th>
                            <th style="text-align: center; border: none;" colspan="2.5">Home Team</th>
                            <th style="border: none;"></th> <!-- Empty for vs -->
                            <th style="border: none;"></th> <!-- Empty for vs -->
                            <th style="border: none;"></th> <!-- Empty for vs -->
                            <th style="text-align: center; border: none;" colspan="2.5">Away Team</th>
                            <th style="border: none;"></th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td style="border: none; text-align: center; width: 15%;"><img src="{row['home_logo']}" alt="Home Logo" width="50" height="50"></td>
                            <td style="border: none; text-align: center; width: 15%;">{row['Home Team']}</td>
                            <td style="border: none; text-align: right; width: 10%;">{row['Home Score']}</td>
                            <td style="border-left: none; border-right: none; text-align: center; width: 10%; vertical-align: middle;">vs</td>
                            <td style="border: none; text-align: center; width: 10%;">{row['Away Score']}</td>
                            <td style="border: none; text-align: center; width: 15%;">{row['Away Team']}</td>
                            <td style="border: none;text-align: center; width: 15%;"><img src="{row['away_logo']}" alt="Away Logo" width="50" height="50"></td>
                        </tr>
                    </tbody>
                </table>
                """
                # Display the game row in Streamlit
                st.markdown(game_row, unsafe_allow_html=True)

                # If the game is complete ('OFF'), show the winning goal scorer
                if row['game_type'] == 'OFF':
                    scorer_row = f"""
                    <table>
                        <tr>
                            <td colspan="7">Winning Goal Scorer ID: {row['Winning Goal Scorer']}</td>
                        </tr>
                    </table>
                    """
                    st.markdown(scorer_row, unsafe_allow_html=True)

            return todays_games_df

        except Exception as e:
            st.error(f"Error loading final data: {e}")
            return None

    # Run the function to display today's games
todays_games()


##########################################
## Goals Tab                           ##
##########################################

with tab_goals:
    # @st.cache_data(show_spinner=True) 
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
load_season_data()
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
## Player Tab                         ##
##########################################
