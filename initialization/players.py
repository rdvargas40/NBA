"""
Players Table
"""
from nba_api.stats.static import players
from sql_queries import exec_sql_query


def create_players_table():
    """ Creates the players table """
    exec_sql_query("""--sql
        CREATE TABLE IF NOT EXISTS players (
            player_id INT PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            is_active BOOLEAN NOT NULL
        );
    """)

def populate_players_table():
    """ Populates the players table with historical data """
    # Definition of the values to be inserted
    insert_many_query = """--sql
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
        insert_many_query += (player_string + ',\n')
    insert_many_query = insert_many_query[:-2] + "\n;"
    
    exec_sql_query(insert_many_query)


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
