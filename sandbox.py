
# Code below is useful to iterate through all available seasonIds for API calls.

# firstYear = 1917
# secondYear = 1918

# while firstYear <= 2023 and secondYear <= 2024:

#     id = str(firstYear) + str(secondYear)
#     print(id)
#     firstYear += 1
#     secondYear += 1
import requests
import json 
import configparser
import time

def request_full_player_bios_data():  
    year_1 = 1917
    year_2 = 1918
    full_data = []
    
    while year_1 <= 2023 and year_2 <= 2024:
        seasonId = str(year_1)+str(year_2)
        response = requests.get(f'https://api.nhle.com/stats/rest/en/skater/bios?limit=-1&cayenneExp=seasonId={seasonId}')

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
    data = request_full_player_bios_data()
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

data_list = parse_player_bios_data()

batch_counter = 0
batch_start = -1
batch_end = 0

while batch_end <= len(data_list):
    batch = []
    batch_end = 1000
    batch = data_list[batch_start+1:batch_end]
    
    for i in batch:
        df = toDf(data_list)
        db_connector(df, db_config['username'], db_config['password'], db_config['host'], db_config['database'], db_config['players_bios_raw'])