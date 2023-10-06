import streamlit as st
import pandas as pd
import numpy as np
from hockey_rink import NHLRink, RinkImage
import matplotlib.pyplot as plt
import seaborn as sns

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

#@st.cache
def load_players():
    github_csv_url = 'data/final_game_data.csv'
    players_df = pd.read_csv(github_csv_url)
    players_df['Name'] = players_df['person.fullName']
    players_df['Position'] = players_df['position']
    players_df['Team'] = players_df['team_name']
    players_df['Goals'] = players_df['goals']
    return players_df

players_df = load_players()

cols = ['Name','Position','Team','Goals']

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
    player = st.selectbox("Choose a player (or click below and start typing):", players_df.Name, index=0)

    player_position = players_df[players_df.Name == player].Position.to_list()[0]
    player_goals = players_df[players_df.Name == player].Goals.to_list()[0]

    st.write(f'''
         ##### <div style="text-align: center"> This season, <span style="color:blue">{player}</span> has scored <span style="color:green">{player_goals}</span> goals.</div>
    ''', unsafe_allow_html=True)

    # Select only the desired columns from the DataFrame
    selected_columns = ['Name', 'Position', 'Team', 'Goals']  # Replace with your actual column names

    # Create an HTML table with desired styling
    html_table = f"""
    <table style="background: azure; border: 1.2px solid; width: 100%">
        <tr>
            <th>Name</th>
            <th>Position</th>
            <th>Team</th>
            <th>Goals</th>
        </tr>
        <tr>
            <td>{players_df.loc[players_df.Name == player, 'Name'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == player, 'Position'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == player, 'Team'].values[0]}</td>
            <td>{players_df.loc[players_df.Name == player, 'Goals'].values[0]}</td>
        </tr>
    </table>
    """

    # Display the HTML table in Streamlit
    st.write(html_table, unsafe_allow_html=True)
    # Convert the Styler object to HTML and display it without the index
    #st.write(styler_player.render(), unsafe_allow_html=True)


    
   # st.markdown('''#### Most Similar Players:''', unsafe_allow_html=True)

    #df_mostsimilar = (dfplayers[(dfplayers.Name != player) & (dfplayers.Sal_class_predict == player_sal_class_predict)
    #                          &  (dfplayers.Pos == player_pos) ]
    #                           .sort_values(by='Max_proba', key=lambda col: np.abs(col-player_max_proba))[cols][:10])

 #   styler_mostsimilar = (df_mostsimilar.style
 #                         .set_properties(**{'background': 'azure', 'border': '1.2px solid'})
 #                         .hide(axis='index')
 #                         .set_table_styles(dfstyle)
 #                         .applymap(color_surplusvalue, subset=pd.IndexSlice[:, ['Surplus Value ($M)']])
 #                        )                                                  
 #   st.table(styler_mostsimilar)
    
 #   st.success('''**A Brief Note on Methods:**  

#The machine learning model deployed in this app is a Random Forest 
#Classifier that uses the following information to predict a player's market value: Games Played, Games Started, 
#Minutes Per Game, Points Per Game, Usage Percentage, Offensive Box Plus/Minus (OBPM), Value Over Replacement Player (VORP), 
#and Win Shares (WS), all scraped from [Basketball Reference](http://www.basketball-reference.com).  

#The seven market value buckets used were:  \$0-5M, \$5-10M, \$10-15M, \$15-20M, \$20-25M, \$25-30M, and \$30M+.  In keeping with best data science practices, the model was trained and fine-tuned on player data from previous years and was not exposed to any data from the 2021-22 NBA season before generating these predictions.''')

##########################################
## Team Tab                             ##
##########################################    
    
#with tab_team:
#    team = st.selectbox("Choose a team (or click below and start typing):", dfteams.Team, index=1)
#   
#    styler_team = (dfplayers[dfplayers.Team == team_to_tm[team]][cols].style
#                          .set_properties(**{'background': 'azure', 'border': '1.2px solid'})
#                          .hide(axis='index')
#                          .set_table_styles(dfstyle)
#                          .applymap(color_surplusvalue, subset=pd.IndexSlice[:, ['Surplus Value ($M)']])                                                    )
#    st.table(styler_team)
#    
#    st.success('''**A Brief Note on Methods:**  

    
##########################################
## Explore Tab                          ##
##########################################
   
