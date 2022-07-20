import json
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.endpoints import leaguegamefinder

def update_team_season_matches_s3(team_id):
    team_matchs_update = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, season_nullable=SeasonAll.current_season, timeout=100)
    team_matchs_update_df = team_matchs_update.get_data_frames()[0]

    for season in team_matchs_update_df.SEASON_ID.unique():
        team_matchs_season_df = team_matchs_update_df[team_matchs_update_df.SEASON_ID == season]
        team_matchs_season_df.to_csv(f"s3://nba.pipeline/teams/matches/{team_id}/{season}.csv", index=False)


def lambda_handler(event, context):

    # Update Player Files in S3
    update_team_season_matches_s3(event['id'])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Succeded at donwloading the data from: {event['full_name']}, id: {event['id']}" ,
        }),
    }
