#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import pandas as pd
from datetime import datetime 
from pprint import pprint 
import pytz
from hockey_rink import NHLRink, RinkImage
import matplotlib.pyplot as plt
import schedule
import time


#if days have been missed, run the full scrape otherwise use the update
#games_data_df = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\final_game_data.csv'
#games_data_df= pd.read_csv(games_data_df)

goal_counts_df = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_counts.csv'
goal_counts_df = pd.read_csv(goal_counts_df)
goal_location_df = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_locations.csv'
goal_location_df= pd.read_csv(goal_location_df)

#def main_script_logic():

#season game results
api_url = "https://statsapi.web.nhl.com/api/v1/schedule"

response = requests.get(api_url )
content = json.loads(response.content)
type(content)


# In[2]:


#goal_counts_df.head()


# In[3]:


#goal_location_df.head()


# ## Games Data

# In[4]:


# Send an HTTP GET request to the specified URL
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # The response content can be accessed using response.text
    response_text = response.text
    #pprint(response_text)
else:
    print(f"Request failed with status code {response.status_code}")


# In[5]:


# Parse the JSON string into a Python dictionary - what are the keys
json_data = json.loads(response_text)
json_data.keys()


# In[6]:


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
#game_test1.head()



# In[7]:


#create a loop to extract all of the data and put it in the dfs list
#the number of games each day changes, so the range has to be set dynamically
end_value = len(game_data.columns)
dfs = {}

# Loop through the iterations (30 times)
for i in range(0, end_value): 
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

# access them using dfs['game_test1'], dfs['game_test2'], etc.
#dfs['game_test2']


# In[8]:


#Then I combine all of the dfs in the list by concatenation to create a single df
combined_df = pd.concat(dfs.values(), ignore_index=True)
combined_df = combined_df[['gamePk', 'gameType', 'gameDate', 'link']]
combined_df=combined_df.convert_dtypes()
combined_df['gameDate'] = pd.to_datetime(combined_df['gameDate'])
#combined_df.head()


# In[9]:


#add to the beginning of the link
prefix = 'https://statsapi.web.nhl.com'
combined_df['link'] = prefix + combined_df['link']
#combined_df.head()


# In[10]:


games_to_date = combined_df
# Assuming your DataFrame is named 'games_to_date'
games_to_date = games_to_date.rename(columns={'gamePk': 'game_id'})
games_to_date = games_to_date.sort_values(by='game_id')
games_to_date = games_to_date.reset_index(drop=True)
#games_to_date


# In[129]:


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
            
            event_data = json_game_data['liveData']['plays']['allPlays']
            event_data = pd.DataFrame(event_data)
            coordinates = pd.json_normalize(event_data['coordinates'])
            coordinates = pd.DataFrame(coordinates)
            game_events = pd.json_normalize(event_data['result'])
            game_events = pd.DataFrame(game_events)
            game_events = game_events[['event', 'eventCode', 'eventTypeId', 'description']]
            game_events['eventId'] = game_events['eventCode'].str[3:].astype(int)
            about_game = pd.json_normalize(event_data['about'])
            about_game = pd.DataFrame(about_game)
            about_game = about_game[['eventId', 'period', 'periodTime', 'periodTimeRemaining']]
            all_game_data = game_events.merge(about_game, on="eventId", how="left")
            all_game_data = all_game_data.merge(coordinates, left_index=True, right_index=True)
            event_players = pd.DataFrame(event_data['players'])
            event_players['eventIdx'] = event_players.index
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
    
    # Store the result in the dictionary with the game ID as the key
    game_data_dict[row['game_id']] = game_specific_data



# In[130]:


game_data_dict.keys()


# In[131]:


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


# In[132]:


# Create an empty dictionary to store the results
game_data_dict = {}

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
    game_data_dict[row['game_id']] = game_specific_data

# Combine all the game-specific DataFrames in the dictionary into one DataFrame
final_game_data = pd.concat(game_data_dict.values(), ignore_index=True)
final_game_data=final_game_data[['player_id', 'eventIdx', 'period', 'event', 'x', 'y' ]]

final_game_data['x'].fillna(0.0, inplace=True)
final_game_data['y'].fillna(0.0, inplace=True)
#final_game_data.head()


# ### Goal Location Data

# In[133]:


#player_goals = final_game_data[['player_id','x','y']]
final_game_data['x'].fillna(0.0, inplace=True)
final_game_data['y'].fillna(0.0, inplace=True)


# ### Current Roster Data

# In[134]:


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


# In[135]:


#removing goalies
final_roster_data = final_roster_data.query('position != "Goalie"')
#final_roster_data.head()


# ### Goal Location Data + Roster Data

# In[136]:


#final_roster_data.head()


# In[137]:


#final_game_data.head()


# In[138]:


if 'player_id' in final_roster_data.columns:
    # Merge the coordinates data from final_game_data to goal_counts based on player IDs
    goal_locations = final_roster_data.merge(final_game_data[['player_id', 'eventIdx', 'period', 'x','y']],on = 'player_id')
else:
    print("Column 'player_id 'does not exist in goal_counts DataFrame.")

#goal_locations


# In[139]:


goal_locations = goal_locations.dropna(subset=['player_id'])
goal_locations = goal_locations.sort_values(['player_name', 'player_id', 'eventIdx'], ascending=[True, True, True])
goal_locations.head()


# In[140]:


null_check=goal_locations[goal_locations['player_id'].isnull()]
#null_check


# In[141]:


#file_path1 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\final_game_data.csv'
#final_game_data.to_csv(file_path1, index=False)

#file_path2 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_location_df.csv'
#goal_location_df.to_csv(file_path1, index=False)


# ### Append goal location data to goal location df

# In[142]:


#running track of goals
#goal_location_df.head()


# In[143]:


goal_location_df=goal_location_df.append(goal_locations)
#goal_location_df.head()


# ### Build Goal Count df: if player IDs match to current csv file, then add new goals, if there is no match, add the name and goals count.

# In[144]:


#Merging game data and roster data to create goal counts

if 'player_id' in final_roster_data.columns:
    # Merge the coordinates data from final_game_data to goal_counts based on player IDs
    goal_counts = final_roster_data.merge(final_game_data[['player_id', 'event']],on = 'player_id')
else:
    print("Column 'player_id 'does not exist in goal_counts DataFrame.")


# In[145]:


goal_counts = goal_counts.groupby(['jersey_number','player_id', 'player_name', 'position', 'team_name'])['event'].count()
goal_counts=pd.DataFrame(goal_counts).reset_index()
goal_counts = goal_counts.rename(columns={'event': 'goals'})
goal_counts = goal_counts.sort_values(by='player_name')
#goal_counts.head()


# In[146]:


#goal_counts_df.head()


# In[147]:


#repeated scorers
repeat_players = pd.merge(goal_counts_df,goal_counts, on ='player_id')
repeat_players = repeat_players.rename(columns = {'jersey_number_x':'jersey_number', 'player_name_x':'player_name', 'position_x':'position',
                                                 'team_name_x':'team_name','goals_x':'goals'})


# In[148]:


#New scorers
new_players = goal_counts[~goal_counts['player_id'].isin(repeat_players['player_id'])]
#new_players


# In[149]:


repeat_players=repeat_players[['jersey_number', 'player_id', 'player_name', 'position', 'team_name', 'goals']]
#repeat_players


# ### append the new players to the list

# In[150]:


goal_counts_df=goal_counts_df.append(new_players)
goal_counts_df = goal_counts_df.append(repeat_players)
goal_counts_df=goal_counts_df.groupby(['jersey_number', 'player_id', 'player_name', 'position', 'team_name'])['goals'].sum()
goal_counts_df.head()


# In[151]:


goal_counts_df = pd.DataFrame(goal_counts_df).reset_index()
#goal_counts_df.head()


# In[155]:


goal_counts_df = goal_counts_df.sort_values(by='player_name')
goal_location_df = goal_location_df.sort_values(by='player_name')


# In[156]:


#goaltracker file path?
file_path1 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_counts.csv'
goal_counts_df.to_csv(file_path1, index=False)


# In[157]:


#goaltracker file path?
file_path2 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\goal_locations.csv'
goal_location_df.to_csv(file_path2, index=False)


# In[ ]:


#running count of goal number for each player - make sure this is ordered by game id and time in period
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

file_path3 = 'C:\\Users\\Karoline Sears\\Documents\\GitHub\\hockey-streamlit\\data\\ice_map_data.csv'
ice_map_data.to_csv(file_path3, index=False)

