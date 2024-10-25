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


###### GATHERING DATA FOR GOAL/POINTS/ASSISTS/GP SEASON DATA

def get_skater_summary():
    try:

        base_url = "https://api.nhle.com/stats/rest/en/skater/summary"
    
        # Parameters for the API request
        params = {
            "isAggregate": "false",
            "isGame": "false",
            "sort": '[{"property":"points","direction":"DESC"},{"property":"goals","direction":"DESC"},{"property":"assists","direction":"DESC"},{"property":"playerId","direction":"ASC"}]',
            "start": 0,  # Starting point for pagination
            "limit": 100,  # Limit per request
            "factCayenneExp": "gamesPlayed>=1",
            "cayenneExp": "gameTypeId=2 and seasonId<=20242025 and seasonId>=20242025"
        }

        all_results = []  # List to store all results

        for i in range(10):  # Loop through 10 times or until no more data
            params['start'] = i * params['limit']  # Update the starting point
            
            response = requests.get(base_url, params=params)
            
            if response.status_code != 200:
                print(f"Failed to retrieve data: {response.status_code}")
                break  # Exit the loop on failure
            
            data = response.json()
            skater_data = data.get('data', [])
            
            if not skater_data:
                print("No more data available.")
                break  # Exit if no more data

            all_results.extend(skater_data)  # Add results to the list

        # Convert results to a DataFrame
        skater_summary = pd.DataFrame(all_results)


        skater_summary = skater_summary.rename(columns={'evGoals': 'ev_goals', 'evPoints':'ev_points', 'faceoffWinPct':'fow_pct', 'gameWinningGoals':'gwg',
                                                        'gamesPlayed':'games_played', 'otGoals':'ot_goals', 'penaltyMinutes':'pims', 
                                                        'playerId':'player_id', 'plusMinus':'plus_minus', 'pointsPerGame':'ppg', 'positionCode': 'position',
                                                        'ppGoals':'pp_goals', 'ppPoints':'pp_points', 'shGoals':'sh_goals', 'shPoints':'sh_points',
                                                        'shootingPct':'shooting_pct', 'skaterFullName':'player_name', 'teamAbbrevs':'team', 'timeOnIcePerGame':'toi'})

        skater_summary['atoi'] = round(skater_summary['toi']/60,2)
        skater_summary['gpg'] = round(skater_summary['goals']/skater_summary['games_played'],2)
        skater_summary=skater_summary.sort_values(by='lastName')
        return skater_summary
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
            # Format the date as 'YYYY-MM-DD'
            formatted_date = current_date.strftime("%Y-%m-%d")
            api_url = f"{base_url}{formatted_date}"
            
            # Make the API request
            response = requests.get(api_url)
            
            if response.status_code == 200:
            # The response content can be accessed using response.text
                response_text = response.text
            #pprint(response_text)
            else:
                print(f"Request failed with status code {response.status_code}")

            json_data = json.loads(response_text)

            game_week = json_data['gameWeek']
            game_week_df = pd.DataFrame(game_week)

            # Filter out empty rows and duplicate dates
            if not game_week_df.empty and formatted_date not in seen_dates:
                seen_dates.add(formatted_date)
                daily_games = pd.concat([daily_games, game_week_df], ignore_index=True)

            # Move to the next week
            current_date += timedelta(weeks=1)
            # Filter out rows where 'date' is after the end date
            daily_games['date'] = pd.to_datetime(daily_games['date'])
            daily_games = daily_games[daily_games['date'] <= end_date]

            # Reset index after filtering
            daily_games.reset_index(drop=True, inplace=True)

            game_week_details = pd.json_normalize(daily_games['games'])

            # Create a dictionary of dataframes for each iteration
            dfs = {
                f'game_test{i}': pd.json_normalize(game_week_details[i])
                for i in range(len(game_week_details.columns))
            }

            # Concatenate all dataframes into one
            combined_df = pd.concat(dfs.values(), ignore_index=True).dropna(how='all')

            # Select relevant columns using `.get()` to avoid KeyErrors for missing fields
            all_daily_games = pd.DataFrame({
                'id': combined_df.get('id', ''),
                'season': combined_df.get('season', ''),
                'startTimeUTC': combined_df.get('startTimeUTC', ''),
                'gameType': combined_df.get('gameType', ''),
                'awayTeam.id': combined_df.get('awayTeam.id', ''),
                'awayTeam.abbrev': combined_df.get('awayTeam.abbrev', ''),
                'awayTeam.logo': combined_df.get('awayTeam.logo', ''),
                'homeTeam.id': combined_df.get('homeTeam.id', ''),
                'homeTeam.abbrev': combined_df.get('homeTeam.abbrev', ''),
                'homeTeam.logo': combined_df.get('homeTeam.logo', ''),
                'homeTeam.placeName.default': combined_df.get('homeTeam.placeName.default', ''),
                'awayTeam.placeName.default': combined_df.get('awayTeam.placeName.default', ''),
                'awayTeam.score': combined_df.get('awayTeam.score', 0),  # Default to 0 if missing
                'homeTeam.score': combined_df.get('homeTeam.score', 0),  # Default to 0 if missing
                'winningGoalScorer.playerId': combined_df.get('winningGoalScorer.playerId', ''),  # Default to empty if missing
                'winningGoalie.playerId': combined_df.get('winningGoalie.playerId', ''),  # Default to empty if missing
                'gameState': combined_df.get('gameState', '')
            })

            # Clean and format the data
            all_daily_games['id'] = all_daily_games['id'].astype(str)
            all_daily_games['link'] = 'https://api-web.nhle.com/v1/gamecenter/' + all_daily_games['id'] + '/play-by-play'
            all_daily_games.dropna(subset=['id'], inplace=True)

            # Set scores to 0 if the game hasn't started yet (you can modify the logic to check gameState if needed)
            all_daily_games['awayTeam.score'] = all_daily_games['awayTeam.score'].fillna(0).astype('Int64').astype(int)
            all_daily_games['homeTeam.score'] = all_daily_games['homeTeam.score'].fillna(0).astype('Int64').astype(int)

            all_daily_games['startTimeUTC'] = pd.to_datetime(all_daily_games['startTimeUTC'])
            all_daily_games = all_daily_games.rename(columns={'id': 'game_id'}).sort_values('game_id').reset_index(drop=True)

            # Convert startTimeUTC to Eastern Time and format the date
            eastern_timezone = pytz.timezone('America/New_York')
 
            # Convert 'startTimeUTC' to Eastern Time
            all_daily_games['game_date_time'] = all_daily_games['startTimeUTC'].dt.tz_convert(eastern_timezone)
    
            all_daily_games['start_time'] = all_daily_games['startTimeUTC'].dt.tz_convert(eastern_timezone).dt.strftime('%I:%M %p').str.lstrip('0').str.lower()
            all_daily_games['game_date'] = all_daily_games['startTimeUTC'].dt.tz_convert(eastern_timezone).dt.strftime('%Y-%m-%d')
            all_daily_games.drop('startTimeUTC', axis=1, inplace=True)

            return all_daily_games

    except Exception as e:
        print(f"Error: {e}")
        return None


######## full shifts data, plays, and loactions
def get_play_data():
    try:
        # Fetch all daily games
        all_daily_games = get_daily_games()
        
        # Filter the games that are live
        live_games = all_daily_games[all_daily_games['gameState'] == "LIVE"]
        
        # If there are no live games, exit early
        if live_games.empty:
            print("No live games at the moment.")
            return None

        # Extract unique live game IDs
        game_ids = live_games['game_id'].unique().tolist()

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

                try:
                    response = requests.get(url)
                    response.raise_for_status()  # Raises an exception for 4xx/5xx errors
                except requests.exceptions.HTTPError as err:
                    print(f"HTTP error occurred for game ID {game_id}: {err}")
                    continue

                if response.status_code == 200:
                    try:
                        json_data = response.json()
                    except ValueError:
                        print(f"Error decoding JSON for game_id {game_id}")
                        continue
                    
                    if 'plays' in json_data:
                        game_plays_detail = pd.json_normalize(json_data['plays'])

                        if not game_plays_detail.empty:
                            game_plays_detail['game_id'] = game_id
                            game_plays_detail = game_plays_detail[['game_id'] + [col for col in game_plays_detail.columns if col != 'game_id']]
                            print(f"Game ID {game_id} processed.")
                            batch_events.append(game_plays_detail)
                        else:
                            print(f"No plays data found for game_id {game_id}")
                    else:
                        print(f"'plays' not found in response for game_id {game_id}")
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
        game_ids = [{'game_id': game_id, 'link': f"{pxp_url}{game_id}{pxp_suffix}"} for game_id in game_ids]  # Use the unique game IDs list

        for i in range(0, len(game_ids), batch_size):
            game_batch = game_ids[i:i + batch_size]
            batch_plays = process_game_batch(game_batch)
            
            # Stop processing if the first game in the batch returned a 404 or batch is empty
            if batch_plays is None or batch_plays.empty:
                print(f"Stopping batch processing at batch starting with game ID {game_batch[0]['game_id']}")
                break
            
            if not batch_plays.empty:
                game_plays = pd.concat([game_plays, batch_plays], ignore_index=True)

        # Check if the 'game_id' column exists before further processing
        if 'game_id' in game_plays.columns:
            # Rename and process as needed
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
            
            # Map 'situationCode' if column exists
            if 'situationCode' in game_plays.columns:
                game_plays['situation'] = game_plays['situationCode'].map(situation_dictionary)
                print("Mapped situationCode to situation.")
            
                game_plays['goalie_situation'] = np.where(
                    (game_plays['situationCode'].str.startswith('0')) | (game_plays['situationCode'].str[3] == '0'),
                    'pulled', 'in net'
                )
            else:
                print("'situationCode' column not found in the game_plays DataFrame.")
            
            game_plays['game_id'] = game_plays['game_id'].astype(str)
        else:
            print("'game_id' column not found in the game_plays DataFrame.")

        return game_plays
    
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
    
def get_standings_data():
    try: 
        api_url = "https://api-web.nhle.com/v1/standings/now"
        response = requests.get(api_url )
        content = json.loads(response.content)


        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # The response content can be accessed using response.text
            response_text = response.text
            #pprint(response_text)
        else:
            print(f"Request failed with status code {response.status_code}")

        json_data = json.loads(response_text)
        standings = json_data['standings']
        standings_df =pd.DataFrame(standings)

        # Extract team names
        standings_df['team'] = standings_df['teamCommonName'].apply(lambda x: x['default'])
        standings_df = standings_df[["conferenceName", "leagueSequence", "divisionName", "gamesPlayed", "teamLogo", "team", 
                                    "winPctg", "conferenceSequence", "date", "divisionSequence", "points", "regulationWins", 
                                    "regulationPlusOtWins", "pointPctg", "goalDifferential", "goalFor"]]
        
        points_per_game = standings_df['points'] / standings_df['gamesPlayed']
        games_remaining = 82 - standings_df['gamesPlayed']
        projected_final_points = standings_df['points']  + (games_remaining * points_per_game)
        standings_df['projected_points'] = round(projected_final_points)

        return standings_df 
    except Exception as e:
        print(f"Error loading standings data: {e}")
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



daily_games = get_daily_games()
league_standings = get_standings_data()

skater_summary = get_skater_summary()

# #All play data/events on the ice
all_play_data = get_play_data()


# Display the first few rows of the DataFrame
if skater_summary is not None:
    print(skater_summary)

else:

    print("No data returned.")