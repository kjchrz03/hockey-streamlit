import streamlit as st
import pandas as pd
import numpy as np
#from hockey_rink import NHLRink, RinkImage
#import matplotlib.pyplot as plt
#import seaborn as sns

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

#@st.cache
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

#def load_teams():


def load_map():
    github_csv_url = 'data/ice_map_data.csv'
    goal_mapping = pd.read_csv(github_csv_url)
    goal_mapping['Name'] = goal_mapping['player_name']
    goal_mapping['ID'] = goal_mapping['player_id']
    goal_mapping['Goal Number'] = goal_mapping['goal_no']
    goal_mapping['Adjusted X'] = goal_mapping['x_adjusted']
    goal_mapping['Adjusted Y'] = goal_mapping['y_adjusted']
    return goal_mapping

goal_mapping = load_map()

cols = ['Name','Goal Number','Adjusted X', 'Adjusted Y']

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
                
tab_player, tab_team, tab_explore, tab_faq = st.tabs(["Player Lookup", "Team Lookup", "Explore", "FAQ"])



#st.sidebar.markdown(" ## About Check This Data")
#st.sidebar.markdown("Dip your toes into advanced hockey analytics with some of my favorite metrics"  )              
#st.sidebar.info("Read more about how the model works and see the code on my [Github](https://github.com/kjchrz03/hockey-streamlit).", icon="ℹ️")


##########################################
## Player Tab                           ##
##########################################


with tab_player:
  player_id_mapping = {row['Name']: row['Player_ID'] for index, row in players_df.iterrows()}

# Display the player dropdown with hidden player IDs
    selected_player_name = st.selectbox("Choose a player (or click below and start typing):", list(player_id_mapping.keys()), index=0)

# Get the player ID based on the selected player name
    selected_player_id = player_id_mapping[selected_player_name]
    player_position = players_df[players_df.Name == selected_player_name].Position.to_list()[0]
    player_goals = players_df[players_df.Name == selected_player_name].Goals.to_list()[0]

    st.write(f'''
         ##### <div style="text-align: center"> This season, <span style="color:blue">{player}</span> has scored <span style="color:green">{player_goals}</span> goals.</div>
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
        <td>{players_df.loc[players_df.Name == player, 'Name'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == player, 'Position'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == player, 'Team'].values[0]}</td>
        <td>{players_df.loc[players_df.Name == player, 'Goals'].values[0]}</td>
    </tr>
</table>
''', unsafe_allow_html=True)

## goal mapping
#player_goals = goal_counts.query('player_id == 8477426')
player_goals = ('player_name == @selected_player_name')
player_name = player_goals['player_name']

goals = (
    player_goals
    .assign(
        x=np.abs(player_goals.x_adjusted),
        y=player_goals.y_adjusted * np.sign(player_goals.x_adjusted),
    )
)

rink = NHLRink(rotation=270, net={"visible": False})

fig, ax = plt.subplots(1, 1, figsize=(10, 20))

# Draw the rink on the single Axes object
rink.draw(display_range="half", ax=ax)

# Scatter plot for goals
rink.scatter(
    "x_adjusted", "y_adjusted", ax=ax,
    facecolor="white", edgecolor="black", s=500,
    data=goals
)

# Add text for goal numbers
rink.text(
    "x_adjusted", "y_adjusted", "goal_no", ax=ax,
    ha="center", va="center", fontsize=8, 
    data=goals
)

#Additional Test
location_texth = rink.text(
    0.5, 0.05, player_name, ax=ax,
    use_rink_coordinates=False,
    ha="center", va="center", fontsize=20,
)









##########################################
## Team Tab                             ##
##########################################    
    
#with tab_team:
#    team = st.selectbox("Choose a team (or click below and start typing):", players_df.Team, index=1)
#   
#    styler_team = (players_df[players_df.Team == team_to_tm[team]][cols].style
#                          .set_properties(**{'background': 'azure', 'border': '1.2px solid'})
#                          .hide(axis='index')
#                          .set_table_styles(dfstyle))                                               
#    st.table(styler_team)
    
#    st.success('''**A Brief Note on Methods:**  

    
##########################################
## Explore Tab                          ##
##########################################
   
