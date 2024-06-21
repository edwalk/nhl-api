import requests
import json
import pandas as pd
import time
from sqlalchemy import create_engine
import configparser

def import_player_data():
    
    seasonId = int(input("Enter a season ID (i.e. 20232024): "))
    data = requests.get(f'https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&cayenneExp=seasonId={seasonId}')

    if data.status_code == 200:
        

        f_data = json.dumps(data.json(), indent=4)
        with open("data.json", "w") as outfile:
            outfile.write(f_data)
        
        print("Data collected and saved successfully.")
        
    else:
        print(f"There was an error with the API call, no data created (error: {data.status_code})")
        
    return data.json()
    

def parse_data():
    parsed_data = import_player_data()
    data_list = []
    loop_counter = 0 

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
    
    engine = create_engine(connection_string)
    
    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print("DataFrame has been sent to the MySQL database.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        engine.dispose()
        print("Engine has been disposed.")
        
        
config = configparser.ConfigParser()
config.read('config.ini')
db_config = config['database']

data_list = parse_data()
df = toDf(data_list)

db_connector(df, db_config['username'], db_config['password'], db_config['host'], db_config['database'], db_config['players_stats_summary_raw'])