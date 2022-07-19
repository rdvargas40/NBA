import json
import configparser
import psycopg2
from nba_api.stats.static import teams

# Load SQL Credentials
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

host = config.get("SQL", "host")
dbname = config.get("SQL", "dbname")
user = config.get("SQL", "user")
password = config.get("SQL", "password")

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


def update_teams_table():
    """ Updates the teams table """
    # Definition of the values to be inserted
    update_many_query = """--sql
        INSERT INTO
            teams (team_id, full_name, nick_name, abbreviation, city, state, year_founded)
        VALUES
    """
    for team in teams.get_teams():
        keys = ['id', 'full_name', 'nickname', 'abbreviation', 'city', 'state', 'year_founded']
        team_string = tuple([team[k] for k in keys])
        team_string = str(team_string)
        update_many_query += (team_string + ',\n')
    update_many_query = update_many_query[:-2]

    # Definition of the update rule
    update_many_query += """--sql
        ON CONFLICT (team_id)
        DO 
            UPDATE SET (full_name, nick_name, abbreviation, city, state, year_founded) =
            (EXCLUDED.full_name, EXCLUDED.nick_name, EXCLUDED.abbreviation, EXCLUDED.city, EXCLUDED.state, EXCLUDED.year_founded)
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
    update_teams_table()
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Succeded",
        }),
    }
