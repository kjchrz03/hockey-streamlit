import pandas as pd
import numpy as np
import asyncio
import aiohttp
import nest_asyncio
import requests
import json
import re
from datetime import datetime, timedelta, date
import time
import pytz
import math
import traceback


# Use a persistent storage mechanism (like a text file) to track the last game ID
LAST_SHIFT_ID_FILE = "last_shift_id.txt"

def get_last_shift_id():
    """Retrieve the last shift ID from a file."""
    try:
        with open(LAST_SHIFT_ID_FILE, "r") as file:
            return int(file.read().strip())
    except FileNotFoundError:
        return None  # Return None if the file doesn't exist (first run)

def update_last_shift_id(last_shift_id):
    """Update the last shift ID in the file."""
    with open(LAST_SHIFT_ID_FILE, "w") as file:
        file.write(str(last_shift_id))

###### GATHERING DATA FOR GOAL/POINTS/ASSISTS/GP SEASON DATA

def get_season_data():
   # Apply nest_asyncio to allow nested event loops in Jupyter notebooks
    nest_asyncio.apply()

    # Constants
    START_GAME_ID = 2024020001
    END_GAME_ID = 2024021307
    BASE_URL = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="
    PXP_URL = "https://api-web.nhle.com/v1/gamecenter/"
    PXP_SUFFIX = "/play-by-play"
    START_SHIFT_ID = 14387777

        # Get the last game ID from the stored file
    last_shift_id = get_last_shift_id()
    if last_shift_id is None:  # If there's no last game ID, fetch all
        last_shift_id = START_SHIFT_ID - 1  

    # Create game URLs DataFrame
    game_data = [{"game_id": game_id, "shift_url": f"{BASE_URL}{game_id}", "pxp_url": f"{PXP_URL}{game_id}{PXP_SUFFIX}"} 
                for game_id in range(START_GAME_ID, END_GAME_ID + 1)]
    game_df = pd.DataFrame(game_data)

    

# Asynchronous function to fetch shift data with a timeout and error handling
    async def fetch_shift_data(session, shift_url):
        try:
            async with session.get(shift_url, timeout=20) as response:  # Timeout after 10 seconds
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
            print(f"An error occurred: {e}")
            traceback.print_exc()  # Print the full traceback for more details
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

    shifts_df = all_shifts[['id', 'gameId', 'period', 'shiftNumber', 'startTime', 'eventNumber', 'playerId', 'player_name', 
                            'teamId', 'teamName', 'typeCode', 'assist_1', 'assist_2', 'shot_type', 'eventDescription']]
    goal_shifts = shifts_df[shifts_df['typeCode'].isin([505])]

    return goal_shifts

def get_agg_totals():
    try:
        goal_shifts = get_season_data()
            # Filter and prepare DataFrames for assists and goals
        assist_1 = goal_shifts[goal_shifts['assist_1'].notna()]
        assist_2 = goal_shifts[goal_shifts['assist_2'].notna()]
        
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
        season_totals=season_totals.rename(columns={'playerId': 'player_id'})
        return season_totals
    except Exception as e:
        print(f"Error loading final data: {e}")
        return None

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
        print(f"Request for player data failed with status code {response.status_code}")

    json_data = json.loads(response_text)

    roster = json_data['data']
    

    df_roster= pd.DataFrame(roster)
    df_roster = df_roster.convert_dtypes()
    df_roster['roster_url'] = 'https://api-web.nhle.com/v1/roster/' + df_roster['triCode'] + '/20242025'
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
            df['position'] = position
            df['sweaterNumber']=df['sweaterNumber'].astype(str)  
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
        team_rosters = get_roster_data()
        return team_rosters
    except Exception as e:
        print(f"Error loading roster data: {e}")
        return None
 
def load_season_data():
    try:
        season_totals = get_agg_totals()
        team_rosters = get_roster_data()
        combined_df = pd.merge(season_totals, team_rosters, on='player_id', how = 'left') 
        combined_df=combined_df.sort_values(by='player_name')
        return combined_df  # Returning the processed DataFrame
    except Exception as e:
        print(f"Error loading final data: {e}")
        return None
    


######## full shifts data, plays, and loactions
def get_play_data():
    start_game_id = 2024020001
    end_game_id = 2024021307

    # Base URL for the API
    pxp_url = "https://api-web.nhle.com/v1/gamecenter/"
    pxp_suffix = "/play-by-play"

    # Initialize an empty DataFrame to store the results
    game_plays = pd.DataFrame()

    # Function to process a batch of game IDs
    def process_game_batch(game_batch):
        batch_events = []
        
        for item in game_batch:
            url = item['link']
            game_id = item['game_id']

            response = requests.get(url)
            
            # Stop if a 404 (Not Found) error occurs for the first game in the batch
            if response.status_code == 404:
                print(f"Game ID {game_id} not found (404). Stopping further requests.")
                return None  # Stop further processing

            if response.status_code == 200:
                json_data = response.json()
            
            # Directly check the game state
            if 'gameState' in json_data:
                game_state = json_data['gameState']  # No need for indexing, just get the string value

                # Stop if the game state is 'FUT'
                if game_state == "FUT":
                    print(f"Future game found at Game ID {game_id}. Stopping further requests.")
                    return None  # Stop further processing


                # Continue processing if game state is not "FUT"
                game_plays_detail = pd.json_normalize(json_data['plays'])
                game_plays_detail['game_id'] = game_id
                game_plays_detail = game_plays_detail[['game_id'] + [col for col in game_plays_detail.columns if col != 'game_id']]
                print(f"Game ID {game_id} processed.")
                batch_events.append(game_plays_detail)
            else:
                print(f"'gameState' key not found in response for game_id {game_id}")
        else:
            print(f"Request failed with status code {response.status_code} for game_id {game_id}")


        # Combine all the events from this batch into a single DataFrame
        if batch_events:
            batch_plays = pd.concat(batch_events, ignore_index=True)
            batch_plays.dropna(how='all', inplace=True)
            return batch_plays
        return pd.DataFrame()

    # Process in batches of 100
    batch_size = 100
    game_ids = [{'game_id': game_id, 'link': f"{pxp_url}{game_id}{pxp_suffix}"} for game_id in range(start_game_id, end_game_id + 1)]

    for i in range(0, len(game_ids), batch_size):
        game_batch = game_ids[i:i + batch_size]
        batch_plays = process_game_batch(game_batch)
        
        # Stop processing if the first game in the batch returned a 404
        if batch_plays is None:
            break
        
        if not batch_plays.empty:
            game_plays = pd.concat([game_plays, batch_plays], ignore_index=True)

    game_plays = game_plays.rename(columns={
        'periodDescriptor.number': 'period_number',
        'periodDescriptor.periodType': 'period_type', 
        'periodDescriptor.maxRegulationPeriods': 'max_regulation_periods',
        'details.eventOwnerTeamId': 'event_team_id',
        'details.losingPlayerId ': 'losing_player_id',
        'details.winningPlayerId': 'winning_player_id',
        'details.xCoord': 'xCoord',
        'details.yCoord': 'yCoord',
        'details.zoneCode': 'zone_code',
        'details.reason': 'reason',
        'details.hittingPlayerId': 'hitter',
        'details.hitteePlayerId': 'hittee',
        'details.playerId': 'player_id',
        'details.shotType': 'shot_type',
        'details.shootingPlayerId': 'shooting_player',
        'details.goalieInNetId': 'goalie',
        'details.awaySOG': 'away_sog',
        'details.homeSOG': 'home_sog',
        'details.blockingPlayerId': 'blocker',
        'details.scoringPlayerId': 'scoring_player',
        'details.scoringPlayerTotal': 'scoring_player_total',
        'details.assist1PlayerId': 'assist_1',
        'details.assist1PlayerTotal': 'assist1_total',
        'details.assist2PlayerId': 'assist_2',
        'details.assist2PlayerTotal': 'assist2_total',
        'details.awayScore': 'away_score',
        'details.homeScore': 'home_score',
        'details.secondaryReason': 'secondary_reason',
        'details.typeCode': 'type_code',
        'details.descKey': 'desc_key',
        'details.duration': 'duration',
        'details.committedByPlayerId': 'committed_by',
        'details.drawnByPlayerId': 'drawn_by',
        'details.servedByPlayerId': 'served_by',
        'event_team_id': 'team_id'
    })
    
    situation_dictionary = {
        '1551': '5 on 5',
        '1451': '5 on 4',
        '1541': '5 on 4',
        '0651': '6 on 5',
        '1560': '6 on 5',
        '1441': '4 on 4',
        '1331': '3 on 3',
        '1460': '6 on 4',
        '1351': '5 on 3',     
        '0641': '6 on 4',
        '1341': '4 on 3',      
        '0101': '1 on 1',
        '1531': '5 on 3',
        '1010': '1 on 1',
        '1431': '4 on 3',
        '0440': '4 on 4',
        '0541': '5 on 4',
        '1550': '5 on 5',
        '1450': '5 on 4',
        '0551': '5 on 5',
        '0431': '4 on 3',
        '1340': '4 on 3',
        '0451': '5 on 4',
        '0531': '5 on 4',
        '0631': '6 on 3',
        '1360': '6 on 3',
        '1350': '5 on 4',
        '1440': '4 on 4'
    }
# Check if 'situationCode' column exists before applying the map
    if 'situationCode' in game_plays.columns:
        game_plays['situation'] = game_plays['situationCode'].map(situation_dictionary)
        print("Mapped situationCode to situation.")
    
        game_plays['goalie_situation'] = np.where((game_plays['situationCode'].str.startswith('0')) | (game_plays['situationCode'].str[3] == '0'),
        'pulled', 'in net')
    else:
        print("'situationCode' column not found in the game_plays DataFrame.")

    game_plays['game_id'] = game_plays['game_id'].astype(str)

    return game_plays

#### for use with score bug (aka combined_df)
def get_daily_games():
    try:
        # Initialize the DataFrame
        daily_games = pd.DataFrame()
        base_url = "https://api-web.nhle.com/v1/schedule/"
        
         # Set the start date
        start_date = datetime.strptime("2024-10-04", "%Y-%m-%d")
        
        # Set the end date to the current date but cap it at "2025-04-17"
        max_end_date = datetime.strptime("2025-04-17", "%Y-%m-%d")
        current_date = datetime.now()
        end_date = min(current_date, max_end_date)
        
        # Track seen dates
        seen_dates = set()


        while current_date <= end_date:
            formatted_date = current_date.strftime("%Y-%m-%d")
            api_url = f"{base_url}{formatted_date}"

            # Make the API request
            response = requests.get(api_url)
            if response.status_code != 200:
                print(f"Failed to retrieve data for {formatted_date}")
                current_date += timedelta(weeks=1)
                continue

            json_data = response.json()
            game_week = json_data.get('gameWeek', [])
            game_week_df = pd.DataFrame(game_week)

            # Filter out empty rows and duplicate dates
            if not game_week_df.empty and formatted_date not in seen_dates:
                seen_dates.add(formatted_date)
                daily_games = pd.concat([daily_games, game_week_df], ignore_index=True)

            current_date += timedelta(weeks=1)

        if daily_games.empty:
            return None

        # Filter out rows where the 'date' is after the end date
        daily_games['date'] = pd.to_datetime(daily_games['date'])
        daily_games = daily_games[daily_games['date'] <= end_date].reset_index(drop=True)

        # Normalize the games column
        game_week_details = pd.json_normalize(daily_games['games'])

        # Create a dictionary of dataframes for each iteration
        dfs = {
            f'game_test{i}': pd.json_normalize(game_week_details[i])
            for i in range(len(game_week_details.columns))
        }

        # Concatenate all dataframes into one
        combined_df = pd.concat(dfs.values(), ignore_index=True).dropna(how='all')

        # Select relevant columns
        all_daily_games = combined_df[['id', 'season', 'startTimeUTC', 'gameType', 'awayTeam.id', 'awayTeam.abbrev',
                                       'awayTeam.logo', 'homeTeam.id', 'homeTeam.abbrev', 'homeTeam.logo',
                                       'homeTeam.placeName.default', 'awayTeam.placeName.default',
                                       'awayTeam.score', 'homeTeam.score', 'winningGoalScorer.playerId',
                                       'winningGoalie.playerId', 'gameState']]

        # Clean and format the data
        all_daily_games['id'] = all_daily_games['id'].astype(str)
        all_daily_games['link'] = 'https://api-web.nhle.com/v1/gamecenter/' + all_daily_games['id'] + '/play-by-play'
        all_daily_games.dropna(subset=['id'], inplace=True)
        all_daily_games = all_daily_games.query('gameState == "OFF"')
        all_daily_games['startTimeUTC'] = pd.to_datetime(all_daily_games['startTimeUTC'])
        all_daily_games = all_daily_games.rename(columns={'id': 'game_id'}).sort_values('game_id').reset_index(drop=True)

        # Convert startTimeUTC to Eastern Time and format the date
        eastern_timezone = pytz.timezone('America/New_York')
        all_daily_games['game_date'] = all_daily_games['startTimeUTC'].dt.tz_convert(eastern_timezone).dt.strftime('%Y-%m-%d')
        all_daily_games.drop('startTimeUTC', axis=1, inplace=True)

        return all_daily_games

    except Exception as e:
        print(f"Error: {e}")
        return None

# ### GAME LOCATIONS
def get_game_locations_data():
    try:
        all_daily_games = get_daily_games()
        game_location = all_daily_games[['game_id', 'awayTeam.id','awayTeam.abbrev', 'homeTeam.id', 'homeTeam.abbrev']]
        game_location['game_id'] = game_location['game_id'].astype(str)

        return game_location
    
    except Exception as e:
        print(f"Error loading final data: {e}")
        return None
    
def load_play_data():
    try:
        game_location = get_game_locations_data()
        game_plays = get_play_data()
        team_rosters = get_roster_data()
        game_plays_data = game_plays.merge( game_location, how='left',  on='game_id' )
        game_plays_data['event_by_team'] = game_plays_data.apply(
            lambda row: (
                'home' if not pd.isna(row['team_id']) and row['team_id'] == row['homeTeam.id'] else
                ('away' if not pd.isna(row['team_id']) and row['team_id'] == row['awayTeam.id'] else None)
            ),
            axis=1
        )
        game_plays_data = game_plays.merge( game_location, how='left',  on='player_id' )

    except Exception as e:
        print(f"Error loading final data: {e}")
        return None
    
def load_shot_data():
    try:
        game_plays_data=load_play_data()
        shots_df = game_plays_data[game_plays_data['typeCode'].isin([505, 506, 507, 508])]
        shots_df=shots_df[['game_id', 'eventId','timeInPeriod','situationCode', 'typeCode', 'typeDescKey', 'sortOrder', 'period_number',
'xCoord','yCoord', 'reason', 'shot_type', 'shooting_player', 'scoring_player', 'assist_1', 'assist_2', 'type_code', 'situation', 'event_by_team']]
        shots_df = shots_df.sort_values(['player_name', 'player_id', 'game_id', 'sortOrder', 'period_number'], 
                                          ascending=[True, True, True, True, True])
        shots_df['goal_no'] = shots_df.groupby('player_id').cumcount()+1
        #center net
        net_x = 89
        net_y = 0

        def calculate_distance(x, y, net_x=net_x, net_y=net_y):
            return math.sqrt((x - net_x)**2 + (y - net_y)**2)


        shots_df['distance'] = shots_df.apply(lambda row: calculate_distance(row['xCoord'], row['yCoord']), axis=1)
        
        def calculate_shot_angle(row, net_x=89, net_y=0):
            # Calculate the displacement vector
            delta_x = net_x - row['xCoord']
            delta_y = net_y - row['yCoord']
            
            # Calculate the angle in radians and then convert it to degrees
            angle_rad = np.arctan2(delta_y, delta_x)
            angle_deg = np.degrees(angle_rad)
            
            # Ensure the angle is positive and within the range 0-360
            if angle_deg < 0:
                angle_deg += 360
            
            return angle_deg

        # Assuming you have a DataFrame 'season_goals' with columns 'xCoord' and 'yCoord'
        game_plays_data['shot_angle'] = game_plays_data.apply(calculate_shot_angle, axis=1)
    
        return shots_df
    
    except Exception as e:
        print(f"Error loading final data: {e}")
        return None

#Agg of basic player stats
season_results = load_season_data()
daily_games = get_daily_games()
# #All player shifts with goals, assists with shift number
# all_season_results = get_season_data()

# #All play data/events on the ice
# all_play_data = get_play_data()




# Display the first few rows of the DataFrame
if season_results is not None:
    print(daily_games)
    
else:

    print("No data returned.")