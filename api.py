import requests
import json
import pandas as pd
import time
from sqlalchemy import create_engine
import configparser

def request_player_summary_data():
    
    seasonId = int(input("Enter a season ID (i.e. 20232024): ")) # This needs to be modified to cycle through seasons.
    data = requests.get(f'https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&cayenneExp=seasonId={seasonId}')

    if data.status_code == 200:       
        print("Data collected and saved successfully.")
        
    else:
        print(f"There was an error with the API call, no data created (error: {data.status_code})")
        
    return data.json()


def request_full_player_regular_season_bios_data():  
    year_1 = 1917
    year_2 = 1918
    full_data = []
    
    while year_1 <= 2023 and year_2 <= 2024:
        seasonId = str(year_1)+str(year_2)
        response = requests.get(f'https://api.nhle.com/stats/rest/en/skater/bios?limit=-1&&gameType=2&cayenneExp=seasonId={seasonId}')

        if response.status_code == 200:       
            print(f"Data collected and saved successfully for season {year_1} - {year_2}")
            year_1 += 1
            year_2 += 1
            full_data.append(response.json())
        
        else:
            print(f"There was an error with the API call, no data created (error: {response.status_code})")
            break
        
    return full_data
    
def parse_player_bios_data():
    year_1 = 1917
    year_2 = 1918
    seasonId = str(year_1) + str(year_2)
    data = request_full_player_regular_season_bios_data()
    data_list = []
    
    for season in data:
        for player in season['data']:
            dict_container = {
                'assists': player['assists'],
                'birthCity': player['birthCity'],
                'birthCountryCode': player['birthCountryCode'],
                'birthDate': player['birthDate'],
                'birthStateProvinceCode': player['birthStateProvinceCode'],
                'currentTeamAbbrev': player['currentTeamAbbrev'],
                'currentTeamName': player['currentTeamName'],
                'draftOverall': player['draftOverall'],
                'draftRound': player['draftRound'],
                'draftYear': player['draftYear'],
                'firstSeasonForGameType': player['firstSeasonForGameType'],
                'gamesPlayed': player['gamesPlayed'],
                'goals': player['gamesPlayed'],
                'height': player['height'],
                'isInHallOfFameYn': player['isInHallOfFameYn'],
                'lastName': player['lastName'],
                'nationalityCode': player['nationalityCode'],
                'playerId': player['playerId'],
                'points': player['points'],
                'positionCode': player['positionCode'],
                'shootsCatches': player['shootsCatches'],
                'skaterFullName': player['skaterFullName'],
                'weight': player['weight'],
                'seasonId': seasonId
            }
            
            data_list.append(dict_container)
            
        year_1 += 1
        year_2 += 1
        seasonId = str(year_1) + str(year_2)
            
                
    return data_list
    
def parse_player_summary_data():
    parsed_data = request_player_summary_data()
    data_list = []
    

    for player in parsed_data['data']:
        dict_container = {
            'playerId': player['playerId'],
            'skaterFullName': player['skaterFullName'],
            'lastName': player['lastName'],
            'teamAbbrevs': player['teamAbbrevs'],
            'seasonId': player['seasonId'],
            'assists': player['assists'],
            'evGoals': player['evGoals'],
            'evPoints': player['evPoints'],
            'faceoffWinPct': player['faceoffWinPct'],
            'gameWinningGoals': player['gameWinningGoals'],
            'gamesPlayed': player['gamesPlayed'],
            'goals': player['goals'],
            'otGoals': player['otGoals'],
            'penaltyMinutes': player['penaltyMinutes'],
            'plusMinus': player['plusMinus'],
            'points': player['points'],
            'pointsPerGame': player['pointsPerGame'],
            'positionCode': player['positionCode'],
            'ppGoals': player['ppGoals'],
            'ppPoints': player['ppPoints'],
            'shGoals': player['shGoals'],
            'shPoints': player['shGoals'],
            'shootingPct': player['shootingPct'],
            'shootsCatches': player['shootsCatches'],
            'shots': player['shots'],
            'timeOnIcePerGame': player['timeOnIcePerGame']        
            }
        
        data_list.append(dict_container)
        
    return data_list
    
def toDf(data_list):    
    df = pd.DataFrame(data_list)
    df['ingestionTimestamp'] = time.time()
    df['ingestionTimestamp'] = pd.to_datetime(df['ingestionTimestamp'], unit = 's')
    return df
  


def db_connector(df, username, password, host, database, table_name):
    connection_string = f'mysql+mysqlconnector://{username}:{password}@{host}/{database}'
    
    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=1800)
    
    try:
        with engine.begin() as connection:
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print("DataFrame has been sent to the MySQL database.")
    

    except Exception as e:
        print(f"An error occurred: {e}")
        
        
    finally:
        engine.dispose()
        print("Engine has been disposed.")
        

def parse_config():  
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['database']
    return db_config

def menu():
    print("Welcome to the NHL API Ingestor.\n Please select one of the following:\n 1. Ingest Player Bios \n 2.Ingest Player Summary Stats")
    selection = input("Select of the options above by entering the corresponding number: ")
    
    return selection

def batch_ingestion(data_list):
    batch_counter = 0
    batch_start = -1
    batch_end = 0
    total_expected_batches = round(len(data_list) / 5000, 0)

    while batch_end < len(data_list):
        batch = []
        if len(data_list) - batch_end >= 5000:
            batch_end += 5000
        else: 
            batch_end += len(data_list) - batch_end
            
        batch = data_list[batch_start+1:batch_end]
        
    
        df = toDf(batch)
        db_connector(df, db_config['username'], db_config['password'], db_config['host'], db_config['database'], db_config['players_bios_raw'])
        
        batch_start = batch_end
        batch_counter += 1
        print(f"Batch {batch_counter} completed ({batch_counter}/{total_expected_batches})")
        print(f"batch start is {batch_start}")
        print(f"Batch end is {batch_end}")
        print(f"Len of data_list is {len(data_list)}")
        
        
        
    
db_config = parse_config()
selection = int(menu())

if selection == 1:
    data_list = parse_player_bios_data()
    batch_ingestion(data_list)
    
else: 
    print("Nothing to do for now.")