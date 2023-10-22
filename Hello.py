import streamlit as st
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use the Agg backend
import matplotlib.pyplot as plt

import subprocess

# Install the library from the GitHub repository using pip within your Streamlit app
subprocess.run(["pip", "install", "git+https://github.com/the-bucketless/hockey_rink.git"])


# Import the external library
import hockey_rink
from hockey_rink import NHLRink, RinkImage
from PIL import Image 
st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

image = Image.open('logo.png')
st.image(image)

#player goals info
def load_players():
    github_csv_url = 'data/goal_counts.csv'
    players_df = pd.read_csv(github_csv_url)
    players_df['Name'] = players_df['player_name']
    players_df['Player ID'] = players_df['player_id']
    players_df['Position'] = players_df['position']
    players_df['Team'] = players_df['team_name']
    players_df['Goals'] = players_df['goals']
    return players_df

players_df = load_players()

cols = ['Name','Position','Team','Goals']

#goal scoring data
def load_map():
    github_ice_map_url = 'data/ice_map_data.csv'
    goal_mapping = pd.read_csv(github_ice_map_url)
    goal_mapping['Name'] = goal_mapping['player_name']
    goal_mapping['ID'] = goal_mapping['player_id']
    goal_mapping['Goal Number'] = goal_mapping['goal_no']
    goal_mapping['Adjusted X'] = goal_mapping['x_adjusted']
    goal_mapping['Adjusted Y'] = goal_mapping['y_adjusted']
    return goal_mapping

goal_mapping = load_map()

cols = ['Name','Goal Number','Adjusted X', 'Adjusted Y']

# game matchup data
def load_matchups():
    github_shots_url = 'data/game_matchups.csv'
    shots = pd.read_csv(github_shots_url)
    shots['Event'] = shots['event']
    shots['Matchup'] = shots['matchup_date']
    return shots

shots = load_matchups()

cols = ['Event', 'Matchup']


#game matchup logos
#def load_logos():
#    github_logos_url = 'data/logos.csv'
#    logos = pd.read_csv(github_logos_url)
#    logos['Tri Code'] = logos['tri_code']
#    logos['Team ID'] = logos['id']
#    logos['Logo'] = logos['logo']
#    return logos

#logos = load_logos()

#cols = ['Tri Code','Team ID','Logo']

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
                      ('background', 'mediumturquoise'),('border', '1.2px solid')]

cell_properties = [('font-size', '16px'),('text-align', 'center')]

dfstyle = [{"selector": "th", "props": heading_properties},
               {"selector": "td", "props": cell_properties}]

# Expander Styling

st.markdown(
    """
<style>
.streamlit-expanderHeader {
 #   font-weight: bold;
    background: aliceblue;
    font-size: 18px;
}
</style>
""",
    unsafe_allow_html=True,
)
    
  
##########################################
##  Title, Tabs, and Sidebar            ##
##########################################

st.title("Check This Data")
st.markdown('''##### <span style="color:gray">Explore NHL Advanced Stats, Simply</span>
            ''', unsafe_allow_html=True)
                
tab_player, tab_games, tab_explore, tab_faq = st.tabs(["Player Goals", "Explore Games", "Explore", "FAQ"])


##########################################
## Player Tab                           ##
##########################################

#player id hidden and mapped to player name
with tab_player:
  player_id_mapping = {row['Name']: row['player_id'] for index, row in players_df.iterrows()}

# Display the player dropdown with hidden player IDs
selected_player_name = st.selectbox("Choose a player (or click below and start typing):", list(player_id_mapping.keys()), index=0)

# Get the player ID based on the selected player name
selected_player_id = player_id_mapping[selected_player_name]
player_position = players_df[players_df.Name == selected_player_name].Position.to_list()[0]
player_goals = players_df[players_df.Name == selected_player_name].Goals.to_list()[0]

st.write(f'''
        ##### <div style="text-align: center"> This season, <span style="color:blue">{selected_player_name}</span> has scored <span style="color:green">{player_goals}</span> goals.</div>
''', unsafe_allow_html=True)

# Select only the desired columns from the DataFrame
selected_columns = ['Name', 'Position', 'Team', 'Goals']  # Replace with your actual column names

# Create an HTML table with desired styling
st.write(f'''
<table style="background: azure; border: 1.2px solid; width: 100%">
<tr>
    <td style="font-weight: bold;">Name</td>
    <td style="font-weight: bold;">Position</td>
    <td style="font-weight: bold;">Team</td>
    <td style="font-weight: bold;">Goals</td>
</tr>
<tr>
    <td>{players_df.loc[players_df.Name == selected_player_name, 'Name'].values[0]}</td>
    <td>{players_df.loc[players_df.Name == selected_player_name, 'Position'].values[0]}</td>
    <td>{players_df.loc[players_df.Name == selected_player_name, 'Team'].values[0]}</td>
    <td>{players_df.loc[players_df.Name == selected_player_name, 'Goals'].values[0]}</td>
</tr>
</table>
''', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

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

text = "Ice rink heat map package from [The Bucketless](https://www.example.com)"
st.markdown(text, unsafe_allow_html=True)



##########################################
## Explore Games                             ##
##########################################    
   
with tab_games:
    game_id_mapping = {row['matchup_date']: row['game_id'] for index, row in shots.iterrows()}

    # Display the player dropdown with hidden player IDs
    selected_matchup = st.selectbox("Choose a matchup (or click below and start typing):", list(game_id_mapping.keys()), index=0)

    # Get the player ID based on the selected player name
    selected_game_id = game_id_mapping[selected_matchup]
    #player_position = players_df[players_df.Name == selected_player_name].Position.to_list()[0]
    #player_goals = players_df[players_df.Name == selected_player_name].Goals.to_list()[0]
  
    def create_matchup_mapping(data_frame):
        matchup_mapping = {f"{row['home_team_tri_code']} vs {row['away_team_tri_code']}, {row['gameDate']}": row['game_id'] for index, row in data_frame.iterrows()}
        return matchup_mapping

# Create the matchup mapping
    matchup_mapping = create_matchup_mapping(shots)

# Display the matchup dropdown with hidden game IDs
    selected_matchup = st.selectbox("Choose a matchup:", list(matchup_mapping.keys()), index=0)


# Get the game ID based on the selected matchup
    selected_game_id = matchup_mapping[selected_matchup]

# You can now use selected_game_id to filter your shots data based on the chosen matchup
    selected_matchup_shots = shots[shots['game_id'] == selected_game_id]

# Example usage: Display some information about the selected matchup
#if selected_game_id in matchup_mapping.values():
#    st.write(f"Selected Game ID: {selected_game_id}")
#    st.write(f"Selected Matchup: {selected_matchup}")
#    st.write(f"Number of Shots in this Matchup: {len(selected_matchup_shots)}")


## goal mapping
#shots = matchups

#for game_id in shots['game_id'].unique():
    # Loop through periods
#    for period in [1, 2, 3]:  # Assuming you have three periods in a game
#        period_data = shots.query("game_id == @game_id and period == @period")
#
#        # Find the home team's ID and away team's ID for the current period
#        home_team_id = period_data['home_team'].values[0]
#        away_team_id = period_data['away_team'].values[0]

#        # Retrieve the logo links for the home and away teams from your logo_df
#        home_team_logo_link = logos.loc[logos['id'] == home_team_id, 'logo'].values[0]
#        away_team_logo_link = logos.loc[logos['id'] == away_team_id, 'logo'].values[0]

 #       fig, ax = plt.subplots(figsize=(12, 8))

        # Map the triCode values to colors
 #       period_data.loc[:, 'color'] = 'blue'  # Assign blue as the default color
 #       period_data.loc[period_data['id'] == home_team_id, 'color'] = 'red'

#        rink = NHLRink(
 #           home_team_logo={
  #              "feature_class": RinkImage,
   #             "image_path": home_team_logo_link,
    #            "x": 55, "length": 50, "width": 42,
     #           "zorder": 15, "alpha": 0.5,
     #       },
     #       away_team_logo={
     #           "feature_class": RinkImage,
     #           "image_path": away_team_logo_link,
     #           "x": -55, "length": 50, "width": 29,
     #           "zorder": 15, "alpha": 0.5,
     #       }
     #   )

        # Switch the logos' positions for the second period
  #      if period == 2:
  #          rink = NHLRink(
  #              home_team_logo={
  #                  "feature_class": RinkImage,
  #                  "image_path": away_team_logo_link,
  #                  "x": 55, "length": 50, "width": 42,
  #                  "zorder": 15, "alpha": 0.5,
  #              },
  #              away_team_logo={
  #                  "feature_class": RinkImage,
  #                  "image_path": home_team_logo_link,
  #                  "x": -55, "length": 50, "width": 29,
  #                  "zorder": 15, "alpha": 0.5,
  #              }
  #          )

        # Use the 'color' column for dot colors
 #       rink.scatter("x", "y", s=100, c=period_data['color'], edgecolor="white", data=period_data, ax=ax)
#
 #       ax.set_title(f"Game ID: {game_id}, Period {period} Shot Locations")
#
 #   plt.show()

##########################################
## Explore Tab                          ##
##########################################
   
