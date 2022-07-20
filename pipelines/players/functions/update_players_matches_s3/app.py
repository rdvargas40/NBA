import json
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import playergamelog


def update_player_season_matches_s3(player_id):
    player_matchs_update = playergamelog.PlayerGameLog(
        player_id=player_id, season=SeasonAll.current_season
    )
    player_matchs_update_df = player_matchs_update.get_data_frames()[0]

    for season in player_matchs_update_df.SEASON_ID.unique():
        player_matchs_season_df = player_matchs_update_df[
            player_matchs_update_df.SEASON_ID == season
        ]
        player_matchs_season_df.to_csv(
            f"s3://nba.pipeline/players/matchs/{player_id}/{season}.csv",
            index=False,
        )


def lambda_handler(event, context):

    # Update Player Files in S3
    update_player_season_matches_s3(event['id'])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Succeded at donwloading the data from: {event['full_name']}, id: {event['id']}" ,
        }),
    }
