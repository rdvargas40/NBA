import configparser
import psycopg2
from nba_api.stats.static import players

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


def update_players_table():
    """ Updates the players table """
    # Definition of the values to be inserted
    update_many_query = """--sql
        INSERT INTO
            players (player_id, first_name, last_name, is_active)
        VALUES
    """
    for player in players.get_players():
        player_id = player['id']
        first_name = player['first_name'].replace("'", "")
        last_name = player['last_name'].replace("'", "")
        active = str(player['is_active']).upper()
        player_string =  str((player_id, first_name, last_name, active))
        update_many_query += (player_string + ',\n')
    update_many_query = update_many_query[:-2]

    # Definition of the update rule
    update_many_query += """--sql
        ON CONFLICT (player_id)
        DO UPDATE SET is_active = EXCLUDED.is_active
        ;
    """
    
    exec_sql_query(update_many_query)


def lambda_handler(event, context):
    # Updates the players table
    update_players_table()

    # Return updated list of active layers , this is important for the pipeline
    updated_players = players.get_active_players()

    return {"players": updated_players}
