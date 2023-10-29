#!/usr/bin/env python
# coding: utf-8

# In[44]:


import requests
import json
import pandas as pd
from datetime import datetime 
from pprint import pprint 
import pytz
import schedule
import time
from PIL import Image
from IPython.display import display
pd.options.mode.chained_assignment = None 
#def main_script_logic():

#season game results

api_url = "https://statsapi.web.nhl.com/api/v1/schedule?season=20232024"

response = requests.get(api_url )
content = json.loads(response.content)
type(content)


# In[45]:


# Send an HTTP GET request to the specified URL
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # The response content can be accessed using response.text
    response_text = response.text
    #pprint(response_text)
else:
    print(f"Request failed with status code {response.status_code}")


# ### Game Data (for base source)

# In[46]:


# Parse the JSON string into a Python dictionary - what are the keys
json_data = json.loads(response_text)
json_data.keys()


# In[47]:


#get the dates and game numbers for the season
game_dates = json_data['dates']

#convert the game info dictionary into a dataframe
df_game_info= pd.DataFrame(game_dates)
df_game_info = df_game_info.convert_dtypes()
#df_game_info.head()

#Normalize (separates the data in the curly brackets out into separate columns)
game_data =pd.json_normalize(df_game_info['games'])
game_data= pd.DataFrame(game_data)
#game_data

#test
game_test1 =pd.json_normalize(game_data[0])
game_test1= pd.DataFrame(game_test1)
game_test1.head()


# In[48]:


#create a loop to extract all of the data and put it in the dfs list

dfs = {}

# Loop through the iterations (30 times)
for i in range(0, 15):  # Loop from 1 to 30 (inclusive)
    # Make an API request and obtain the JSON response using the make_api_request function
    api_response = game_dates
    
    if api_response is not None:
        # Extract relevant data from the API response and normalize it
        game_info = pd.json_normalize(game_data[i])
        
        # Create a DataFrame for this iteration
        df_name = f'game_test{i}'  # Generate a unique variable name
        dfs[df_name] = pd.DataFrame(game_info)
    else:
        # Handle the case where the API request failed
        print(f"API request failed for index {i}")

# Now, you have 30 separate DataFrames stored in the 'dfs' dictionary
# You can access them using dfs['game_test1'], dfs['game_test2'], etc.
dfs['game_test1'].head()


# In[49]:


#Then I combine all of the dfs in the list by concatenation to create a single df
combined_df = pd.concat(dfs.values(), ignore_index=True)
combined_df = combined_df[['gamePk', 'gameType', 'gameDate', 'link','status.abstractGameState',
                           'teams.home.team.id', 'teams.home.team.name', 'teams.away.team.id', 'teams.away.team.name']]
combined_df=combined_df.convert_dtypes()
combined_df['gameDate'] = pd.to_datetime(combined_df['gameDate'])
combined_df = combined_df.query('gameType != "PR"')
combined_df.head()


# In[50]:


#Rename game_state column
state = 'game_state'
combined_df = combined_df.rename(columns={combined_df.columns[4]: state})
combined_df.head()


# In[51]:


combined_df['game_state'].value_counts()


# In[52]:


combined_df = combined_df.query('game_state == "Final" |  game_state == "Live"')
combined_df


# In[53]:


#add to the beginning of the link
prefix = 'https://statsapi.web.nhl.com'
combined_df['link'] = prefix + combined_df['link']
combined_df.head()


# ### Roster Data

# In[54]:


stats_url = "https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster&season=20232024"

response = requests.get(stats_url )
content = json.loads(response.content)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # The response content can be accessed using response.text
    response_text = response.text
    #pprint(response_text)
else:
    print(f"Request failed with status code {response.status_code}")

json_data = json.loads(response_text)
team_data = json_data['teams']

all_rosters = []

for team in json_data['teams']:
    team_id = team['id']  # Get the team ID
    team_name = team['name']  # Get the team name

    # Extract the roster for the current team
    roster_data = team['roster']['roster']

    # Create a DataFrame for the roster data
    roster_df = pd.json_normalize(roster_data)

    # Add a 'team_id' and 'team_name' column to identify the team
    roster_df['team_id'] = team_id
    roster_df['team_name'] = team_name

    # Append the roster data for the current team to the list
    all_rosters.append(roster_df)

# Concatenate all the roster DataFrames into one
final_roster_data = pd.concat(all_rosters, ignore_index=True)
final_roster_data = final_roster_data[['jerseyNumber', 'person.id', 'person.fullName', 'position.name', 'team_name']]
final_roster_data = final_roster_data.rename(columns = {'jerseyNumber':'jersey_number', 'person.id':'player_id', 'person.fullName':'player_name',
                                                      'position.name':'position' })
final_roster_data.head()


# ### Games to Date

# In[55]:


games = combined_df[['gamePk', 'gameType', 'gameDate', 'link']]
games = games.dropna(subset=['gamePk'])
games = games.sort_values(by='gamePk')

games = games.rename(columns={'gamePk': 'game_id'})

# games_to_date now contains games that have happened prior to today in UTC-4 timezone
games_to_date = games


# In[56]:


games_to_date


# In[57]:


# Assuming your DataFrame is named 'games_to_date'
games_to_date = games_to_date.sort_values(by='game_id')
games_to_date = games_to_date.reset_index(drop=True)
games_to_date.head()


# In[58]:


# Create an empty dictionary to store the results
game_data_dict = {}

    
    # Define your extraction script as a function
def extract_game_data(game_link):
    try:
        # Send an HTTP GET request to the game URL
        response = requests.get(game_link)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            response_text = response.text
            json_game_data = json.loads(response_text)
            
            #event data
            event_data = json_game_data['liveData']['plays']['allPlays']
            event_data = pd.DataFrame(event_data)
            
            #coordinates data
            coordinates = pd.json_normalize(event_data['coordinates'])
            coordinates = pd.DataFrame(coordinates)
            coordinates['x'].fillna(0.0, inplace=True)
            coordinates['y'].fillna(0.0, inplace=True)
            
            #game events data
            game_events = pd.json_normalize(event_data['result'])
            game_events = pd.DataFrame(game_events)
            game_events = game_events[['event', 'eventCode', 'eventTypeId', 'description']]
            game_events['eventId'] = game_events['eventCode'].str[3:].astype(int)
            
            #about data
            about_game = pd.json_normalize(event_data['about'])
            about_game = pd.DataFrame(about_game)
            about_game = about_game[['eventId', 'period', 'periodTime', 'periodTimeRemaining']]
            
            #join the dfs
            all_game_data = game_events.merge(about_game, on="eventId", how="left")
            all_game_data = all_game_data.merge(coordinates, left_index=True, right_index=True)
            
            
            #event players
            event_players = pd.DataFrame(event_data['players'])
            event_players['eventIdx'] = event_players.index
            
            #join event players
            all_game_data = all_game_data.merge(event_players, left_index=True, right_index=True)
     
            # Define a function to extract player IDs safely
            def extract_player_id(player_data):
                if isinstance(player_data, list) and len(player_data) > 0:
                    player = player_data[0]
                    if 'player' in player and 'id' in player['player']:
                        return player['player']['id']
                return None

            # Apply the function to create a new 'player_id' column
            all_game_data['player_id'] = all_game_data['players'].apply(extract_player_id)

            # Convert the 'player_id' column to the appropriate data type (e.g., int)
            all_game_data['player_id'] = all_game_data['player_id'].astype(float).astype(pd.Int64Dtype(), errors='ignore')
            goals = all_game_data.query('eventTypeId == "GOAL"')
            
            return goals  # Return the entire processed DataFrame
        else:
            print(f"Request failed for {game_link} with status code {response.status_code}")
            return None  # Return None to indicate failure
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # Return None to indicate failure
    
    
# Loop through the rows of the games_to_date df
for index, row in games_to_date.iterrows():
    # Extract the API link from the current row
    game_link = row['link']
    
 # Run your game-specific data script and get the entire processed DataFrame
    game_specific_data = extract_game_data(game_link)
    
        # Add a 'game_id' column to the game_specific_data DataFrame
    game_specific_data['game_id'] = row['game_id']
    
    # Store the result in the dictionary with the game ID as the key
    game_data_dict[row['game_id']] = game_specific_data



# In[ ]:


game_data_dict


# In[ ]:


game_data_dict.keys()


# In[ ]:


# # Loop through the rows of the games_to_date df
# for index, row in games_to_date.iterrows():
#     # Extract the API link from the current row
#     game_link = row['link']
    
#     # Run your game-specific data script and get the entire processed DataFrame
#     game_specific_data = extract_game_data(game_link)
    
#     # Add a 'game_id' column to the game_specific_data DataFrame
#     game_specific_data['game_id'] = row['game_id']
    
#     # Store the result in the dictionary with the game ID as the key
#     game_data_dict[row['game_id']] = game_specific_data
# game_data_dict


# ### Game Data (To Date): Every game to run date, goals, player ids, and coordinates

# In[ ]:


# Create an empty dictionary to store the results
goal_location_dict = {}

# Loop through the rows of the games_to_date df
for index, row in games_to_date.iterrows():
    # Extract the API link from the current row
    game_link = row['link']
    
    # Run your game-specific data script and get the entire processed DataFrame
    game_specific_data = extract_game_data(game_link)
    
    # Set the 'game_id' for the game_specific_data using the current row's 'game_id'
    if game_specific_data is not None:
        game_specific_data['game_id'] = row['game_id']
    
    # Store the result in the dictionary with the game ID as the key
    goal_location_dict[row['game_id']] = game_specific_data

# Combine all the game-specific DataFrames in the dictionary into one DataFrame
final_game_data = pd.concat(goal_location_dict.values(), ignore_index=True)
final_game_data=final_game_data[['player_id', 'eventIdx', 'period', 'event', 'x', 'y' ]]


final_game_data.head()


# In[ ]:


final_game_data


# ### Goal Counts: Roster Info (jersey #, ID, Name, Pos, Team) + Game Data(ID, event)

# In[ ]:


final_roster_data.head()


# In[ ]:


#removing goalies
final_roster_data = final_roster_data.query('position != "Goalie"')
final_roster_data.head()


# In[ ]:


#Merging game data and roster data to create goal counts

if 'player_id' in final_roster_data.columns:
    # Merge the coordinates data from final_game_data to goal_counts based on player IDs
    goal_counts = final_roster_data.merge(final_game_data[['player_id', 'event']],on = 'player_id')
else:
    print("Column 'player_id 'does not exist in goal_counts DataFrame.")


# In[ ]:


goal_counts.head()


# In[ ]:


goal_counts = goal_counts.groupby(['jersey_number','player_id', 'player_name', 'position', 'team_name'])['event'].count()
goal_counts=pd.DataFrame(goal_counts).reset_index()
goal_counts = goal_counts.rename(columns={'event': 'goals'})
goal_counts = goal_counts.sort_values(by='player_name')
goal_counts.head()


# ### Goal Locations: Roster Info(Jersey #, ID, Name, Pos, Team) + Game Data (id, event, x, y)

# In[ ]:


if 'player_id' in final_roster_data.columns:
    # Merge the coordinates data from final_game_data to goal_counts based on player IDs
    goal_locations = final_roster_data.merge(final_game_data[['player_id', 'eventIdx', 'period', 'x','y']],on = 'player_id')
else:
    print("Column 'player_id 'does not exist in goal_counts DataFrame.")

goal_locations


# In[ ]:


goal_locations = goal_locations.dropna(subset=['player_id'])
goal_locations = goal_locations.sort_values(['player_name', 'player_id', 'eventIdx'], ascending=[True, True, True])
goal_locations


# In[ ]:


null_check=goal_locations[goal_locations['player_id'].isnull()]
null_check


# In[ ]:


file_path1 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_locations.csv'
goal_locations.to_csv(file_path1, index=False, encoding='utf-8')


# In[ ]:


file_path2 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_counts.csv'
goal_counts.to_csv(file_path2,  index=False, encoding='utf-8')


# In[ ]:


#running count of goal number for each player - make sure this is ordered by game id and time in period
goal_location_df = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_locations.csv'
goal_location_df= pd.read_csv(goal_location_df)
goal_talley =goal_location_df
goal_talley['goal_no'] = goal_talley.groupby('player_id').cumcount()+1

def adjust_coordinates(row):
    x = row['x']
    y = row['y']
    if x < 0:
        adj_x = abs(x)
        adj_y = -y
    else:
        adj_x = x
        adj_y = y
    return pd.Series({'x_adjusted': adj_x, 'y_adjusted': adj_y})

# Apply the function to each row of the DataFrame
goal_talley[['x_adjusted', 'y_adjusted']] = goal_talley.apply(adjust_coordinates, axis=1)
ice_map_data = goal_talley



# In[ ]:


file_path3 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\ice_map_data.csv'
ice_map_data.to_csv(file_path3,  index=False, encoding='utf-8')


# In[ ]:


goal_counts_df = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_counts.csv'
goal_counts_df = pd.read_csv(goal_counts_df)


# In[ ]:


logos = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\logos.csv'
logos = pd.read_csv(logos)
logos=logos[['tri_code', 'id', 'logo']]


# In[ ]:


combined_df.head()


# In[ ]:


#Then I combine all of the dfs in the list by concatenation to create a single df
shot_maps=combined_df
shot_maps = shot_maps.rename(columns={'gamePk': 'game_id'})

shot_maps = shot_maps.sort_values(by='game_id')
shot_maps = shot_maps.reset_index(drop=True)
shot_maps = shot_maps.rename(columns = {'teams.home.team.id':'home_team','teams.away.team.id':'away_team', 
                                                'teams.home.team.name':'home_team_name', 'teams.away.team.name':'away_team_name'})
shot_maps.head()


# In[ ]:


# Create an empty dictionary to store the results
game_data_dict = {}

# Define your extraction script as a function
def extract_game_data(game_link, game_id):
    try:
        # Send an HTTP GET request to the game URL
        response = requests.get(game_link)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            response_text = response.text
            json_game_data = json.loads(response_text)

            # Event data
            event_data = json_game_data['liveData']['plays']['allPlays']
            event_data = pd.DataFrame(event_data)

            # Coordinates
            coordinates = pd.json_normalize(event_data['coordinates'])
            coordinates = pd.DataFrame(coordinates)

            # Teams column data
            teams = pd.json_normalize(event_data['team'])
            teams = pd.DataFrame(teams)
            teams = teams[['id','name', 'triCode']]

            # Game events
            game_events = pd.json_normalize(event_data['result'])
            game_events = pd.DataFrame(game_events)
            game_events = game_events[['event', 'eventCode', 'eventTypeId', 'description']]
            game_events['eventId'] = game_events['eventCode'].str[3:].astype(int)

            # Join team names and coordinates
            game_events = game_events.merge(teams, left_index=True, right_index=True)
            game_events = game_events.merge(coordinates, left_index=True, right_index=True)

            # About game data - adding time into the game info
            about_game = pd.json_normalize(event_data['about'])
            about_game = pd.DataFrame(about_game)
            about_game = about_game[['eventId',  'period', 'periodTime']]

            # Limit results to goals and shots, merge final DataFrames
            game_events = game_events.query('event == "Goal" or event == "Shot"')
            game_events = game_events.merge(about_game, on="eventId")
            game_events = game_events[['event', 'eventId', 'name','id', 'triCode', 'x', 'y', 'period', 'periodTime']]

            # Add 'game_id' column to the DataFrame
            game_events['game_id'] = game_id

            return game_events  # Return the entire processed DataFrame
        else:
            print(f"Request failed for {game_link} with status code {response.status_code}")
            return None  # Return None to indicate failure
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # Return None to indicate failure

game_data_dict = {}

# Loop through the rows of the games_to_date df
for index, row in shot_maps.iterrows():
    # Extract the API link from the current row
    game_link = row['link']
    
    # Extract home team and away team from the current row
    home_team = row['home_team']
    home_team_name = row['home_team_name']
    away_team = row['away_team']
    away_team_name = row['away_team_name']
    game_date = row['gameDate']

    # Run your game-specific data script and get the entire processed DataFrame
    game_specific_data = extract_game_data(game_link, row['game_id'])
    
    # Add home_team and away_team to the game_specific_data DataFrame
    game_specific_data['home_team'] = home_team
    game_specific_data['home_team_name'] = home_team_name
    game_specific_data['away_team'] = away_team
    game_specific_data['away_team_name'] = away_team_name
    game_specific_data['gameDate'] = game_date

    # Store the result in the dictionary with the game ID as the key
    game_data_dict[row['game_id']] = game_specific_data


# In[ ]:


# Combine all the game-specific DataFrames in the dictionary into one DataFrame
final_shot_data = pd.concat(game_data_dict.values(), ignore_index=True)
final_shot_data['gameDate'] = final_shot_data['gameDate'].dt.strftime('%m/%d/%Y')


# In[ ]:


# Convert 'gameDate' to a datetime object
final_shot_data['gameDate'] = pd.to_datetime(final_shot_data['gameDate'])

# Define a custom function to create the desired concatenated value
def create_matchup_date(row):
    home_team_id = row['home_team']
    away_team_id = row['away_team']
    
    # Look up the home and away team names from the 'logos' DataFrame
    home_team_name = logos.loc[logos['id'] == home_team_id, 'tri_code'].values[0]
    away_team_name = logos.loc[logos['id'] == away_team_id, 'tri_code'].values[0]
    
    game_date = row['gameDate'].strftime('%m/%d/%Y')
    return f"{home_team_name} vs {away_team_name}, {game_date}"

# Apply the custom function to create the new 'matchup_date' column
final_shot_data['matchup'] = final_shot_data.apply(create_matchup_date, axis=1)

# Now, the 'matchup_date' column contains the desired concatenated values
final_shot_data.head()


# In[ ]:


final_shot_data['gameDate'] = pd.to_datetime(final_shot_data['gameDate'])


# In[ ]:


final_shot_data = final_shot_data.sort_values(by=['game_id', 'period', 'periodTime'], ascending=[False, True, True])
final_shot_data.head()


# In[ ]:


test = final_shot_data.query('game_id ==2023020062')
goals = test.query('event == "Goal"')
goals


# In[ ]:


file_path4 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\game_matchups.csv'
final_shot_data.to_csv(file_path4,  index=False, encoding='utf-8')


# In[ ]:





# In[ ]:





# In[ ]:




