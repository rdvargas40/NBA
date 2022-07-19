import json
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import leaguegamefinder

def update_team_seasson_matchs_s3(team_id):
    team_matchs_update = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, season_nullable=SeasonAll.current_season)
    team_matchs_update_df = team_matchs_update.get_data_frames()[0]

    for season in team_matchs_update_df.SEASON_ID.unique():
        team_matchs_season_df = team_matchs_update_df[team_matchs_update_df.SEASON_ID == season]
        team_matchs_season_df.to_csv(f"s3://nba.pipeline/teams/matchs/{team_id}/{season}.csv", index=False)


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
    update_team_seasson_matchs_s3(event['team_id'])
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Succeded at donwloading the data from: " + event['abbreviation'],
        }),
    }
