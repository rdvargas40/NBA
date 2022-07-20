"""
Teams matches Table
"""
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from sql_queries import exec_sql_query
from typing import List
from tqdm import tqdm
import pandas as pd
from warnings import filterwarnings
import boto3

filterwarnings('ignore')

available_team_ids = [team['id'] for team in teams.get_teams()]

def get_write_player_matches_historical_data(team_id: int) -> bool:
    """
    Asks for the team historial data match by match from the nba_api, then writes it on S3
    Args:
        team_id (int): unique team id from the nba_api
    Returns:
        bool True if the operation was succesfull, False otherwise
    """

    for _ in range(10):
      try:
        # Dowload Team Historical Data
        df_team_games_all = leaguegamefinder.LeagueGameFinder(team_id_nullable=1610612747).get_data_frames()[0]

        # Write Team Historical Data into S3
        team_seasons = df_team_games_all.SEASON_ID.unique()
        for season in team_seasons:
            # Each Seasson is written in an individual file into the team folder 
            df_team_games_season = df_team_games_all[df_team_games_all.SEASON_ID == season]
            df_team_games_season.to_csv(f"s3://nba.pipeline/teams/matches/{team_id}/{season}.csv", index=False)
        return True
        break
      except Exception as e:
        print('Failed to download the data from team with id: ', team_id)
        continue
    return False

def populate_teams_matches_historical_data(team_id_list: List=available_team_ids) -> List:
    """
    Puplates the teams matches historical data in S3
    Args:
        team_id_list (List=available_team_ids): List of ids from the teams whose data is to be downloaded and written into S3
    Returns:
        List of ids from the teams at which the frunction failed to complete the operation
    """
    failed_teams_ids = []
    for team_id in tqdm(team_id_list):
        success = get_write_player_matches_historical_data(team_id)
        if not success:
            failed_teams_ids.append(team_id)
    print(f'Success Rate: {1 - len(failed_teams_ids)/len(team_id_list): .0%}')
    return failed_teams_ids

def create_table():
    """ Creates the teams_matches table """
    exec_sql_query("""--sql
        CREATE TABLE IF NOT EXISTS teams_matches (
            team_id INT REFERENCES teams(team_id),
            match_id INT,
            season_id VARCHAR,
            game_date DATE,
            opponent VARCHAR,
            result VARCHAR,
            duration INT,
            points INT,
            rebounds INT,
            assists INT,
            steals INT,
            blocks INT,
            turnovers INT,
            PRIMARY KEY (team_id, match_id, season_id)
        );
    """)

def load_file_into_databbase(df_file: pd.DataFrame):
    """
    Loads a team crude file into the database
    Args:
        df_file (pd.DataFrame): team crude file
    """
    
    df_file = df_file[[
        'TEAM_ID', 'GAME_ID', 'SEASON_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN',
        'PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV'
    ]]

    df_file.columns = [
        'team_id', 'match_id', 'season_id', 'game_date', 'opponent', 'result', 'duration',
        'points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers'
    ]

    
    df_file['team_id'] = df_file['team_id'].astype(int)
    df_file['match_id'] = df_file['match_id'].astype(int)
    df_file['season_id'] = df_file['season_id'].astype(str)
    df_file['opponent'] = [x.split()[-1] for x in df_file['opponent']]

    # Definition of the values to be inserted
    update_many_query = """--sql
        INSERT INTO
            teams_matches (team_id, match_id, season_id, game_date, opponent, result, duration, points, rebounds, assists, steals, blocks, turnovers)
        VALUES
    """
    for i in range(len(df_file)):
        doc_string = tuple(df_file.iloc[i].values)
        doc_string = str(doc_string)
        update_many_query += (doc_string + ',\n')
    update_many_query = update_many_query[:-2]

    # Definition of the update rule
    update_many_query += """--sql
        ON CONFLICT (team_id, match_id, season_id)
        DO 
            UPDATE SET (duration, points, rebounds, assists, steals, blocks, turnovers) = 
            (EXCLUDED.duration, EXCLUDED.points, EXCLUDED.rebounds, EXCLUDED.assists, EXCLUDED.steals, EXCLUDED.blocks, EXCLUDED.turnovers)
        ;
    """
    
    exec_sql_query(update_many_query)

def populate_database() -> List:
    """
    Populate the database with the teams matches files hosted in S3
    Returns:
        List of files that could not be loaded into the database
    """
    
    # Identify the file paths to team match files
    s3_client = boto3.client("s3")
    s3_paginator = s3_client.get_paginator('list_objects_v2')
    s3_pages = s3_paginator.paginate(Bucket='nba.pipeline', Prefix="teams/matches")
    s3_file_paths = []
    for s3_page in s3_pages:
        for s3_obj in s3_page['Contents']:
            s3_file_paths.append(s3_obj['Key'])

    failed_files = []
    for file_path in tqdm(s3_file_paths):
        try:
            df_file = pd.read_csv(f"s3://nba.pipeline/{file_path}")
            load_file_into_databbase(df_file)
        except Exception as e:
            print('Could not load the file: ', file_path)
            print(e)
            failed_files.append(file_path)
    print(f'File Upload Success Ratio: {1 - len(failed_files)/len(s3_file_paths): .0%}')
    return failed_files
