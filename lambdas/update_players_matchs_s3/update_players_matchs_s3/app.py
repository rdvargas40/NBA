import json
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import playergamelog


def update_player_seasson_matchs_s3(player_id):
    player_matchs_update = playergamelog.PlayerGameLog(player_id=player_id, season=SeasonAll.current_season)
    player_matchs_update_df = player_matchs_update.get_data_frames()[0]

    for season in player_matchs_update_df.SEASON_ID.unique():
        player_matchs_season_df = player_matchs_update_df[player_matchs_update_df.SEASON_ID == season]
        player_matchs_season_df.to_csv(f"s3://nba.pipeline/players/matchs/{player_id}/{season}.csv", index=False)


def lambda_handler(event, context):
    """
    Lambda Function that Updates the Players tables in the NBA Database

    Parameters
    ----------
    event: Trigger of the Lambda Function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict
    """
    update_player_seasson_matchs_s3(event['player_id'])
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Succeded at donwloading the data from: {event['first_name']} {event['last_name']}",
        }),
    }
