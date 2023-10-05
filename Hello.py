import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="Check This Data", page_icon="🏒", initial_sidebar_state="expanded")

@st.cache
def load_players():
    github_csv_url = 'https://github.com/kjchrz03/hockey-streamlit/blob/main/data/goal_tracker.csv'
    players_df = pd.read_csv(github_csv_url)
    players_df['Name'] = players_df['person.fullName']
    players_df['Team'] = players_df['team_name']
    players_df['Goals'] = players_df['player_id']
    return players_df

players_df = load_players()

cols = ['Name','Team','Goals']

#hide_table_row_index = """
#            <style>
#            thead tr th:first-child {display:none}
#            tbody th {display:none}
#            </style>   """

#center_heading_text = """
#    <style>
#        .col_heading   {text-align: center !important}
#    </style>          """
    
#center_row_text = """
#    <style>
#        td  {text-align: center !important}
#    </style>      """

# Inject CSS with Markdown

st.markdown(hide_table_row_index, unsafe_allow_html=True)
st.markdown(center_heading_text, unsafe_allow_html=True) 
st.markdown(center_row_text, unsafe_allow_html=True) 

# More Table Styling

#def color_surplusvalue(val):
#    if str(val) == '0':
#        color = 'azure'
#    elif str(val)[0] == '-':
#        color = 'lightpink'
#    else:
#        color = 'lightgreen'
#    return 'background-color: %s' % color

#heading_properties = [('font-size', '16px'),('text-align', 'center'),
#                      ('color', 'black'),  ('font-weight', 'bold'),
#                      ('background', 'mediumturquoise'),('border', '1.2px solid')]

#cell_properties = [('font-size', '16px'),('text-align', 'center')]

#dfstyle = [{"selector": "th", "props": heading_properties},
#               {"selector": "td", "props": cell_properties}]

# Expander Styling

#st.markdown(
#    """
#<style>
#.streamlit-expanderHeader {
 #   font-weight: bold;
#    background: aliceblue;
#    font-size: 18px;
#}
#</style>
#""",
#    unsafe_allow_html=True,
#)
    
  
##########################################
##  Title, Tabs, and Sidebar            ##
##########################################

st.title("Check This Data")
st.markdown('''##### <span style="color:gray">Explore NHL Advanced Stats, Simply</span>
            ''', unsafe_allow_html=True)
                
tab_player, tab_team, tab_explore, tab_faq = st.tabs(["Player Lookup", "Team Lookup", "Explore", "FAQ"])



st.sidebar.markdown(" ## About Check This Data")
st.sidebar.markdown("Dip your toes into advanced hockey analytics with some of my favorite metrics"  )              
st.sidebar.info("Read more about how the model works and see the code on my [Github](https://github.com/kjchrz03/hockey-streamlit).", icon="ℹ️")

##########################################
## Player Tab                           ##
##########################################

#with tab_player:
#    player = st.selectbox("Choose a player (or click below and start typing):", players_df.Name, index =508)
    
    #player_pos = dfplayers[dfplayers.Name == player].Pos.to_list()[0]
    #player_sal_class_predict = dfplayers[dfplayers.Name == player].Sal_class_predict.to_list()[0]
    #player_max_proba = dfplayers[dfplayers.Name == player].Max_proba.to_list()[0]          
    #player_salary = dfplayers[dfplayers.Name == player]['Salary ($M)'].to_list()[0]
    #player_goals = players_df['player_id']
    #if player_salary == '<2':
    #    player_salary = '<$2M'
    #else:
    #    player_salary = '$' +  player_salary + 'M'   
    #player_marketvalue = dfplayers[dfplayers.Name == player]['Market Value ($M)'].to_list()[0]
    #if player_marketvalue == '30+':
    #    player_marketvalue = '$30M+'
    #else:
    #    player_marketvalue = '$' +  player_marketvalue + 'M'
    #player_url = 'https://www.basketball-reference.com' + dfplayers[dfplayers.Name == player]['ID'].to_list()[0]                   
    
    #st.write(f'''
         ##### <div style="text-align: center"> In the 2021-22 NBA season, <span style="color:blue">[{player}]({player_url})</span> earned a salary of <span style="color:blue"> {player_salary}   </span> </div>
         
          ##### <div style="text-align: center"> According to our model, his market value was <span style="color:blue">{player_marketvalue}</span> </div>
     #    ''', unsafe_allow_html=True)
    
    #styler_player = (players_df[players_df.Name == player][cols]
    #               .style.set_properties(**{'background': 'azure', 'border': '1.2px solid'})
    #               .hide(axis='index')
    #               .set_table_styles(dfstyle)
    #               .applymap(color_surplusvalue, subset=pd.IndexSlice[:, ['Surplus Value ($M)']]))
    #st.table(styler_player)
    
    
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

    
