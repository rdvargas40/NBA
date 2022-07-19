"""
Teams Table
"""
from nba_api.stats.static import teams
from sql_queries import exec_sql_query

def create_teams_table():
    """ Creates the teams table """
    exec_sql_query("""--sql
        CREATE TABLE IF NOT EXISTS teams (
            team_id INT PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            nick_name VARCHAR NOT NULL,
            abbreviation VARCHAR NOT NULL UNIQUE,
            city VARCHAR NOT NULL,
            state VARCHAR NOT NULL,
            year_founded INT NOT NULL
        );
    """)

def populate_teams_table():
    """ Populates the teams table with historical data """
    # Definition of the values to be inserted
    insert_many_query = """--sql
        INSERT INTO
            teams (team_id, full_name, nick_name, abbreviation, city, state, year_founded)
        VALUES
    """
    for team in teams.get_teams():
        keys = ['id', 'full_name', 'nickname', 'abbreviation', 'city', 'state', 'year_founded']
        team_string = tuple([team[k] for k in keys])
        team_string = str(team_string)
        insert_many_query += (team_string + ',\n')
    insert_many_query = insert_many_query[:-2] + "\n;"
    
    exec_sql_query(insert_many_query)


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
