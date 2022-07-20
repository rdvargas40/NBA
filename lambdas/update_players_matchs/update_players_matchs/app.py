import json
import configparser
import psycopg2
import boto3
import pandas as pd
from warnings import filterwarnings

filterwarnings('ignore')

# Load SQL Credentials
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

host = config.get("SQL", "host")
dbname = config.get("SQL", "dbname")
user = config.get("SQL", "user")
password = config.get("SQL", "password")

# S3 Client
s3_client = boto3.client('s3')

def exec_sql_query(query: str):
    """
    Exceutes PostgreSQL Query on the NBA Database
    Args:
        query (str): PostgreSQL Query
    """
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(query)
    conn.close()


def load_file_into_databbase(df_file: pd.DataFrame):
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
            players_matchs (player_id, match_id, season_id, minutes, points, rebounds, assists, steals, blocks, turnovers)
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

    s3_file_path = event['Records'][0]['s3']['object']['key']
    new_file_df = pd.read_csv('s3://nba.pipeline/' + s3_file_path)
    load_file_into_databbase(new_file_df)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Succeded at loading file: " + s3_file_path,
        }),
    }
