import pandas as pd
import numpy as np
import asyncio
import aiohttp
import nest_asyncio
import requests
import json
import re

def get_player_data():
    # Apply nest_asyncio to allow nested event loops in Jupyter notebooks
    nest_asyncio.apply()

    # Constants
    START_GAME_ID = 2024020001
    END_GAME_ID = 2024021307
    BASE_URL = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="
    PXP_URL = "https://api-web.nhle.com/v1/gamecenter/"
    PXP_SUFFIX = "/play-by-play"
    
    # Create game URLs DataFrame
    game_data = [{"game_id": game_id, "shift_url": f"{BASE_URL}{game_id}", "pxp_url": f"{PXP_URL}{game_id}{PXP_SUFFIX}"} 
                for game_id in range(START_GAME_ID, END_GAME_ID + 1)]
    game_df = pd.DataFrame(game_data)

    # Asynchronous function to fetch shift data with a timeout and error handling
    async def fetch_shift_data(session, shift_url):
        try:
            async with session.get(shift_url, timeout=10) as response:  # Timeout after 10 seconds
                if response.status == 404:  # Stop if a 404 (Not Found) error occurs
                    print("Game not found (404). Stopping further requests.")
                    return None  # Indicate stopping
                response.raise_for_status()  # Raise an error for other bad responses
                json_data = await response.json()
                details = pd.DataFrame(json_data['data'])
                if not details.empty:
                    details['player_name'] = details['firstName'] + " " + details['lastName']
                    mask_505 = details['typeCode'] == 505
                    details.loc[mask_505, 'eventDetails'] = details.loc[mask_505, 'eventDetails'].fillna('unassisted').replace(r'^\s*$', 'unassisted', regex=True)
                    assists = details.loc[mask_505, 'eventDetails'].str.split(', ', expand=True)
                    details.loc[mask_505, ['assist_1', 'assist_2']] = assists
                    return details
                return pd.DataFrame()  # Return empty DataFrame if no data
        except Exception as e:
            print(f"An error occurred in shift data: {e}")
            return pd.DataFrame()

    # Fetch all shift data asynchronously in batches
    async def fetch_all_shift_data(game_df, batch_size=50):
        async with aiohttp.ClientSession() as session:
            all_results = []
            for i in range(0, len(game_df), batch_size):
                batch = game_df.iloc[i:i + batch_size]
                tasks = [fetch_shift_data(session, row['shift_url']) for _, row in batch.iterrows()]
                batch_results = await asyncio.gather(*tasks)
                all_results.extend(batch_results)
            return all_results

    # Run the async tasks and collect the results
    all_shifts_data = asyncio.run(fetch_all_shift_data(game_df))
    all_shifts = pd.concat(all_shifts_data, ignore_index=True)

    # Create a dictionary to make shot data more readable 
    shot_code_dictionary = {
        801: 'slap shot', 802: 'snap shot', 803: 'wrist shot',
        804: 'wrap-around', 805: 'tip-in', 806: 'backhanded shot',
        807: 'deflected in', 808: 'bat shot', 809: 'cradle/Michigan',
        810: 'poke', 811: 'between the legs'
    }
    all_shifts['shot_type'] = all_shifts['detailCode'].map(shot_code_dictionary)
    all_shifts['gameId'] = all_shifts['gameId'].astype(str)

    # Filter and prepare DataFrames for assists and goals
    assist_1 = all_shifts[all_shifts['assist_1'].notna()]
    assist_2 = all_shifts[all_shifts['assist_2'].notna()]

    shifts_df = all_shifts[['gameId', 'period', 'shiftNumber', 'startTime', 'eventNumber', 'playerId', 'player_name', 
                            'teamId', 'teamName', 'typeCode', 'assist_1', 'assist_2', 'shot_type', 'eventDescription']]
    goal_shifts = shifts_df[shifts_df['typeCode'].isin([505])]

    # Prepare totals for goals and assists
    season_goal_totals = goal_shifts.groupby(['playerId', 'player_name', 'gameId'])['eventNumber'].count().reset_index(name='g')
    season_assist1_total = assist_1.groupby(['gameId', 'assist_1', 'eventNumber', 'period']).size().reset_index(name='a1')
    season_assist2_total = assist_2.groupby(['gameId', 'assist_2', 'eventNumber', 'period']).size().reset_index(name='a2')

    # Merge assist totals and goal totals
    season_assist1_total = season_assist1_total.rename(columns={'assist_1': 'player_name'})
    season_assist2_total = season_assist2_total.rename(columns={'assist_2': 'player_name'})

    season_assists = season_assist1_total.merge(season_assist2_total, on=['player_name', 'gameId'], how='outer', suffixes=('_assist1', '_assist2'))
    season_totals = season_goal_totals.merge(season_assists, on=['player_name', 'gameId'], how="outer")

    # Fill NaN values with 0 and calculate aggregates
    season_totals.fillna(0, inplace=True)
    season_totals = season_totals.groupby(['playerId', 'player_name']).agg(
        g=('g', 'sum'),
        gp=('gameId', 'nunique'),
        a1=('a1', 'sum'),
        a2=('a2', 'sum')
    ).reset_index()

    # Calculate goals per game and total points
    season_totals['gpg'] = season_totals['g'] / season_totals['gp']
    season_totals['p'] = season_totals['g'] + season_totals['a1'] + season_totals['a2']
    season_totals=season_totals.sort_values(by='player_name')
    season_totals=season_totals.rename(columns = {'playerId':'player_id'})
    return season_totals

def load_player_data():
    try:
        # Call the function from season_data.py (assuming it returns a DataFrame)
        season_totals = get_player_data()
        return season_totals  # Returning the processed DataFrame
    except Exception as e:
        print(f"Error loading data player data: {e}")
        return None
# Call the function and store the results
season_results = load_player_data()



def get_roster_data():
    
    #Roster Data
    team_url = "https://api.nhle.com/stats/rest/en/team"

    response = requests.get(team_url )
    content = json.loads(response.content)

    # Send an HTTP GET request to the specified URL
    response = requests.get(team_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
 
        response_text = response.text

    else:
        # print(f"Request for player data failed with status code {response.status_code}")

    json_data = json.loads(response_text)

    roster = json_data['data']
    

    df_roster= pd.DataFrame(roster)
    df_roster = df_roster.convert_dtypes()
    df_roster['roster_url'] = 'https://api-web.nhle.com/v1/roster/' + df_roster['triCode'] + '/20232024'
    df_roster = df_roster[['id','fullName', 'triCode', 'roster_url']]
    df_roster = df_roster.rename(columns = {'id':'team_id', 'fullName':'team_name', 'triCode':'tri_code'})
    df_roster=df_roster.sort_values(by='team_id')


    #for 2024-2025 season :
    valid_team_codes = set(range(1, 11)).union(set(range(12, 27))).union(set(range(28, 31))).union(set(range(52, 53))).union(set(range(54, 56))).union(set(range(59, 60)))
    filtered_rosters = df_roster[df_roster['team_id'].isin(valid_team_codes)]


    # Create an empty dictionary to store the results
    roster_dict = {}

    # Define your function to extract player data and avoid repetition
    def process_player_data(player_data, position):
        df = pd.DataFrame(player_data)
        if not df.empty:
            df = df[['id', 'headshot', 'firstName', 'lastName', 'sweaterNumber', 'positionCode', 'shootsCatches']]
            df['firstName'] = df['firstName'].apply(lambda x: x['default'] if isinstance(x, dict) else x)
            df['lastName'] = df['lastName'].apply(lambda x: x['default'] if isinstance(x, dict) else x)
            df['player_name'] = df['firstName'] + " " + df['lastName']
            df['position'] = position  # Add position info
            return df[['id', 'headshot', 'player_name', 'sweaterNumber', 'positionCode', 'shootsCatches']]
        return pd.DataFrame()

    # Function to extract tricode from headshot link
    def extract_tricode(link):
        match = re.search(r'/([A-Z]{3})/', link)
        return match.group(1) if match else None

    # Function to fetch and process roster data
    def extract_roster_data(roster_link):
        try:
            response = requests.get(roster_link)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                forwards_df = process_player_data(json_data['forwards'], 'FWD')
                defense_df = process_player_data(json_data['defensemen'], 'DEF')
                goalies_df = process_player_data(json_data['goalies'], 'GOL')

                # Concatenate all positions into a single DataFrame
                team_roster_df = pd.concat([forwards_df, defense_df, goalies_df], axis=0, ignore_index=True)
                team_roster_df['tri_code'] = team_roster_df['headshot'].apply(extract_tricode)
                return team_roster_df
            else:
                # print(f"Request failed for roster {roster_link} with status code {response.status_code}")
                return None
        except Exception as e:
            # print(f"An error occurred in roster data: {e}")
            return None

    # Loop through the rows of filtered_rosters and extract data
    for index, row in filtered_rosters.iterrows():
        roster_link = row['roster_url']
        team_roster = extract_roster_data(roster_link)
        
        if team_roster is not None:
            team_roster['tri_code'] = row['tri_code']
            roster_dict[row['tri_code']] = team_roster
        else:
            print(f"Skipping row {index} due to failed request or exception in roster data.")

    # Combine all rosters into a single DataFrame
    all_rosters = pd.concat(roster_dict.values(), ignore_index=True)

    # Merge team information with the player roster data
    team_rosters = filtered_rosters[['team_id', 'team_name', 'tri_code']]
    team_rosters = team_rosters.merge(all_rosters, on="tri_code", how="left")
    team_rosters = team_rosters.rename(columns={'id': 'player_id'})
    team_rosters = team_rosters[['player_id', 'team_name', 'positionCode', 'sweaterNumber', 'shootsCatches']]
    # View result
    return team_rosters

def load_roster_data():
    try:
        # Call the function from season_data.py (assuming it returns a DataFrame)
        team_rosters = get_roster_data()
        return team_rosters  # Returning the processed DataFrame
    except Exception as e:
        # print(f"Error loading roster data: {e}")
        return None
# Call the function and store the results
team_rosters = get_roster_data()


def get_season_data(roster_df, season_totals_df):
    # Merge season totals with roster data
    combined_df = pd.merge(roster_df, season_totals_df, on='player_id', how='left')
    return combined_df

# Call functions to get data
team_rosters = get_roster_data()
season_totals = load_player_data()  # Assuming you have this function to get season totals

# Merge roster data and season totals
if team_rosters is not None and season_totals is not None:
    season_data = get_season_data(team_rosters, season_totals)

else:
    # print("Failed to retrieve roster data.")

def load_season_data():
    try:
        # Call the function from season_data.py (assuming it returns a DataFrame)
        season_data = get_season_data()
        return season_data  # Returning the processed DataFrame
    except Exception as e:
        # print(f"Error loading season data: {e}")
        return None
# Call the function and store the results
season_data = get_season_data(team_rosters, season_totals)

# # Display the first few rows of the DataFrame
# if season_data is not None:
#     print(season_data.columns)
# else:
#     print("No season data returned.")

