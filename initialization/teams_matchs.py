"""
Teams Matchs Table
"""
import configparser
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from sql_queries import exec_sql_query
from typing import List
from tqdm import tqdm

# Load AWS Credentials
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

AWS_KEY = config.get("AWS", "AWS_KEY")
AWS_SECRET = config.get("AWS", "AWS_SECRET")
AWS_REGION = config.get("AWS", "AWS_REGION")

available_team_ids = [team['id'] for team in teams.get_teams()]

def get_write_player_matchs_historical_data(team_id: int) -> bool:
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
            df_team_games_season.to_csv(
                f"s3://nba.pipeline/teams/matchs/{team_id}/{season}.csv", index=False,
                storage_options={'key': AWS_KEY, 'secret': AWS_SECRET}
            )
        return True
        break
      except Exception as e:
        print('Failed to download the data from team with id: ', team_id)
        continue
    return False

def populate_teams_matchs_historical_data(team_id_list: List=available_team_ids) -> List:
    """
    Puplates the teams matchs historical data in S3
    Args:
        team_id_list (List=available_team_ids): List of ids from the teams whose data is to be downloaded and written into S3
    Returns:
        List of ids from the teams at which the frunction failed to complete the operation
    """
    failed_teams_ids = []
    for team_id in tqdm(team_id_list):
        success = get_write_player_matchs_historical_data(team_id)
        if not success:
            failed_teams_ids.append(team_id)
    print(f'Success Rate: {1 - len(failed_teams_ids)/len(team_id_list): .0%}')
    return failed_teams_ids
