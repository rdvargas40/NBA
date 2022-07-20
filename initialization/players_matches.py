"""
Functions for downloading and populating all the historial data in S3
"""
import configparser
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.library.parameters import SeasonAll
from sql_queries import exec_sql_query
from tqdm import tqdm
from typing import List
import pandas as pd
from warnings import filterwarnings
import boto3

filterwarnings('ignore')

# Load AWS Credentials
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

AWS_KEY = config.get("AWS", "AWS_KEY")
AWS_SECRET = config.get("AWS", "AWS_SECRET")
AWS_REGION = config.get("AWS", "AWS_REGION")

available_player_ids = [player['id'] for player in players.get_players()]

def get_write_player_matches_historical_data(player_id: int) -> bool:
    """
    Asks for the player historial data match by match from the nba_api, then writes it on S3
    Args:
        player_id (int): unique player id from the nba_api
    Returns:
        bool True if the operation was succesfull, False otherwise
    """
    for _ in range(10):
      try:
        # Dowload Player Historical Data
        gamelog_player_all = playergamelog.PlayerGameLog(player_id=player_id, season=SeasonAll.all, timeout=10)

        # Convert Player Historical Data into a DataFrame
        df_player_games_all = gamelog_player_all.get_data_frames()[0]

        # Write Player Historical Data into S3
        player_seasons = df_player_games_all.SEASON_ID.unique()
        for season in player_seasons:
            # Each Seasson is written in an individual file into the player folder 
            df_player_games_season = df_player_games_all[df_player_games_all.SEASON_ID == season]
            df_player_games_season.to_csv(
                f"s3://nba.pipeline/players/matches/{player_id}/{season}.csv", index=False,
                storage_options={'key': AWS_KEY, 'secret': AWS_SECRET}
            )
        return True
        break
      except Exception as e:
        print('Failed to download the data from player with id: ', player_id)
        continue
    return False


def populate_players_match_historical_data_from_list(player_id_list: List=available_player_ids) -> List:
    """
    Get and write the historical data of a list opf players matches.
    Args:
        player_id_list (List=available_player_ids): List of ids from the players whose data is to be downloaded and written into S3
    Returns:
        List of ids from the players at which the frunction failed to complete the operation
    """
    failed_players_ids = []
    for player_id in tqdm(player_id_list):
        success = get_write_player_matches_historical_data(player_id)
        if not success:
            failed_players_ids.append(player_id)
    print(f'Success Rate: {1 - len(failed_players_ids)/len(player_id_list): .0%}')
    return failed_players_ids


def create_table():
    """ Creates the players_matches table """
    exec_sql_query("""--sql
        CREATE TABLE IF NOT EXISTS players_matches (
            player_id INT REFERENCES players(player_id),
            match_id INT,
            season_id VARCHAR,
            minutes INT,
            points INT,
            rebounds INT,
            assists INT,
            steals INT,
            blocks INT,
            turnovers INT,
            PRIMARY KEY (player_id, match_id, season_id)
        );
    """)


def load_file_into_database(df_file: pd.DataFrame):
    """
    Loads a player crude file into the database
    Args:
        df_file (pd.DataFrame): player crude file
    """
    
    df_file = df_file[[
        'Player_ID', 'Game_ID', 'SEASON_ID', 'MIN',
        'PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV'
    ]]

    df_file.columns = [
        'player_id', 'match_id', 'season_id', 'minutes',
        'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers'
    ]

    df_file['player_id'] = df_file['player_id'].astype(int)
    df_file['match_id'] = df_file['match_id'].astype(int)
    df_file['season_id'] = df_file['season_id'].astype(str)

    # Definition of the values to be inserted
    update_many_query = """--sql
        INSERT INTO
            players_matches (player_id, match_id, season_id, minutes, points, rebounds, assists, steals, blocks, turnovers)
        VALUES
    """
    for i in range(len(df_file)):
        doc_string = tuple(df_file.iloc[i].values)
        doc_string = str(doc_string)
        update_many_query += (doc_string + ',\n')
    update_many_query = update_many_query[:-2]

    # Definition of the update rule
    update_many_query += """--sql
        ON CONFLICT (player_id, match_id, season_id)
        DO 
            UPDATE SET (minutes, points, rebounds, assists, steals, blocks, turnovers) = 
            (EXCLUDED.minutes, EXCLUDED.points, EXCLUDED.rebounds, EXCLUDED.assists, EXCLUDED.steals, EXCLUDED.blocks, EXCLUDED.turnovers)
        ;
    """
    
    exec_sql_query(update_many_query)


def populate_database() -> List:
    """
    Populate the database with the players matches files hosted in S3
    Returns:
        List of files that could not be loaded into the database
    """
    
    # Identify the file paths to team match files
    s3_client = boto3.client("s3")
    s3_paginator = s3_client.get_paginator('list_objects_v2')
    s3_pages = s3_paginator.paginate(Bucket='nba.pipeline', Prefix="players/matches")
    s3_file_paths = []
    for s3_page in s3_pages:
        for s3_obj in s3_page['Contents']:
            s3_file_paths.append(s3_obj['Key'])

    failed_files = []
    for file_path in tqdm(s3_file_paths):
        try:
            df_file = pd.read_csv(f"s3://nba.pipeline/{file_path}")
            load_file_into_database(df_file)
        except Exception as e:
            print('Could not load the file: ', file_path)
            print(e)
            failed_files.append(file_path)
    print(f'File Upload Success Ratio: {1 - len(failed_files)/len(s3_file_paths): .0%}')
    return failed_files
