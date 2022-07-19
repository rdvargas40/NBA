"""
Functions for downloading and populating all the historial data in S3
"""
import configparser
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.library.parameters import SeasonAll
from tqdm import tqdm
from typing import List

# Load AWS Credentials
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

AWS_KEY = config.get("AWS", "AWS_KEY")
AWS_SECRET = config.get("AWS", "AWS_SECRET")
AWS_REGION = config.get("AWS", "AWS_REGION")

available_player_ids = [player['id'] for player in players.get_teams()]

def get_write_player_matchs_historical_data(player_id: int) -> bool:
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
                f"s3://nba.pipeline/players/matchs/{player_id}/{season}.csv", index=False,
                storage_options={'key': AWS_KEY, 'secret': AWS_SECRET}
            )
        return True
        break
      except:
        continue
    return False


def populate_players_match_historical_data_from_list(player_id_list: List=available_player_ids) -> List:
    """
    Get and write the historical data of a list opf players matchs.
    Args:
        player_id_list (List=available_player_ids): List of ids from the players whose data is to be downloaded and written into S3
    Returns:
        List of ids from the players at which the frunction failed to complete the operation
    """
    failed_players_ids = []
    for player_id in tqdm(player_id_list):
        success = get_write_player_matchs_historical_data(player_id)
        if not success:
            failed_players_ids.append(player_id)
    print(f'Success Rate: {1 - len(failed_players_ids)/len(player_id_list): .0%}')
    return failed_players_ids
