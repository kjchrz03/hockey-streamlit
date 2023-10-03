#!/usr/bin/env python
# coding: utf-8

# In[18]:


import requests
import json
import pandas as pd
from datetime import datetime 
from pprint import pprint 
import pytz

import schedule
import time

def main_script_logic():

#season game results
api_url = "https://statsapi.web.nhl.com/api/v1/schedule?season=20232024"

response = requests.get(api_url )
content = json.loads(response.content)
type(content)


# In[19]:


# Send an HTTP GET request to the specified URL
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # The response content can be accessed using response.text
    response_text = response.text
    #pprint(response_text)
else:
    print(f"Request failed with status code {response.status_code}")


# In[20]:


# Parse the JSON string into a Python dictionary - what are the keys
json_data = json.loads(response_text)
json_data.keys()


# In[21]:


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



# In[22]:


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


# In[23]:


#Then I combine all of the dfs in the list by concatenation to create a single df
combined_df = pd.concat(dfs.values(), ignore_index=True)
combined_df = combined_df[['gamePk', 'gameType', 'gameDate', 'link']]
combined_df=combined_df.convert_dtypes()
combined_df['gameDate'] = pd.to_datetime(combined_df['gameDate'])
combined_df.head()


# In[24]:


#add to the beginning of the link
prefix = 'https://statsapi.web.nhl.com'
combined_df['link'] = prefix + combined_df['link']
combined_df.head()


# ##### How to pull out single game data

# In[25]:


#testing out on a specific game
game_url = "https://statsapi.web.nhl.com/api/v1/game/2023010002/feed/live"

# Send an HTTP GET request to the specified URL
response2 = requests.get(game_url)

# Check if the request was successful (status code 200)
if response2.status_code == 200:
    # The response content can be accessed using response.text
    response_text2 = response2.text
    #pprint(response_text)
else:
    print(f"Request failed with status code {response2.status_code}")

    #getting in-game data
json_data2 = json.loads(response_text2)
event_data = json_data2['liveData']['plays']['allPlays']

#convert the dictionary into a dataframe
event_data = pd.DataFrame(event_data)

#Normalize (separates the data in the curly brackets out into separate columns)

#coordinates column data
coordinates=pd.json_normalize(event_data['coordinates'])
coordinates= pd.DataFrame(coordinates)

#results column data
game_events = pd.json_normalize(event_data['result'])
game_events=pd.DataFrame(game_events)
game_events = game_events[['event','eventCode', 'eventTypeId', 'description']]
game_events['eventId'] = game_events['eventCode'].str[3:].astype(int)

#about column data
about_game = pd.json_normalize(event_data['about'])
about_game=pd.DataFrame(about_game)
about_game = about_game[['eventIdx', 'eventId', 'periodTime', 'periodTimeRemaining']]

#players column data - bringing it in as a dictionary first to connect the data to the event id before parsing
event_players = pd.DataFrame(event_data['players'])
event_players['eventIdx'] = event_players.index

#merge all three dfs together to create a new single df, merging left onto game events
all_game_data = game_events.merge(about_game, on="eventId", how = "left")

#adding in coordinates data on the index
all_game_data = all_game_data.merge(coordinates, left_index=True, right_index=True)

#merging in the player data
all_game_data = all_game_data.merge(event_players, left_index=True, right_index=True)

#extracting the player ID data to connect it to the event id
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

all_game_data.head()


# In[26]:


#Extracting the game id from the url
# Split the URL by "game/" and take the last part
#parts = game_url.split("game/")
#if len(parts) > 1:
#    game_id_with_extra = parts[-1]  # This includes "/feed/live"
#    # Remove the "/feed/live" portion
#    game_id = game_id_with_extra.split("/feed/live")[0]


# ##### Creating a roster df - this is for last season bc this season does not have a roster set yet. Must change in october

# In[27]:


stats_url = "https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster&season=20222023"

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

#this is extracting the data from a single team, the number in the [] indicates which team in the database to extract roster info for. Bruins are team 6
# Assuming you have the JSON response as 'response_text'
#json_data = json.loads(response_text)
# Extract 'roster' data from the 'teams' key
#roster_data = json_data['teams'][0]['roster']['roster']
# Create a DataFrame directly from the 'roster' data
#person_df = pd.json_normalize(roster_data)
#person_df
# Assuming you have a list of teams in your data, you can iterate through them
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
final_roster_data.head()


# #### developing the final df

# In[34]:


# Create a timezone object for UTC-4
utc_minus_4 = pytz.timezone('America/New_York')  # Adjust to the correct timezone if necessary

# Get the current date in UTC-4 timezone
today_date_utc_minus_4 = datetime.now(utc_minus_4).date()

# Assuming your 'gameDate' column is already in UTC-4, you can directly compare it
filtered_games = games[games['gameDate'].dt.date < today_date_utc_minus_4]

filtered_games = filtered_games.rename(columns={'gamePk': 'game_id'})

# games_to_date now contains games that have happened prior to today in UTC-4 timezone
games_to_date = filtered_games


# In[35]:


# Assuming your DataFrame is named 'games_to_date'
games_to_date = games_to_date.sort_values(by='game_id')
games_to_date = games_to_date.reset_index(drop=True)
games_to_date


# In[36]:


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
            about_game = about_game[['eventIdx', 'eventId', 'periodTime', 'periodTimeRemaining']]
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



# In[37]:


game_data_dict.keys()


# In[38]:


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
game_data_dict


# In[48]:


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



# #### Creating a df to track players' goals and coordinates

# In[40]:


final_roster_data.head()


# In[52]:


goal_tracker = final_roster_data[['jerseyNumber', 'person.id', 'person.fullName', 'position.name', 'team_name']]
# Check if 'person.id' exists in goal_tracker DataFrame
if 'person.id' in goal_tracker.columns:
    # Merge the coordinates data from final_game_data to goal_tracker based on player IDs
    goal_tracker = goal_tracker.merge(final_game_data[['player_id', 'x', 'y']], left_on='person.id', right_on='player_id', how='left')
else:
    print("Column 'person.id' does not exist in goal_tracker DataFrame.")




# In[ ]:


def schedule_script():
    # Define the schedule for your script
    # For example, run main_script_logic every day at 8:00 AM
    schedule.every().day.at("08:18").do(main_script_logic)

    while True:
        # Run the pending tasks
        schedule.run_pending()
        time.sleep(60)  # Sleep for 60 seconds (adjust as needed)

if __name__ == "__main__":
    # Start scheduling your script
    schedule_script()

