"""
SQL Queries
"""
import configparser
import psycopg2

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


# Creates the players table
create_players_table = """--sql
    CREATE TABLE IF NOT EXISTS players (
        player_id INT PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        is_active BOOLEAN NOT NULL
    );
"""

# Creates the teams table
create_teams_table = """--sql
    CREATE TABLE IF NOT EXISTS teams (
        team_id INT PRIMARY KEY,
        full_name VARCHAR NOT NULL,
        abbreviation VARCHAR,
        nickname VARCHAR,
        city VARCHAR NOT NULL, -- If a city table is to be used, this needs to reference that table
        state VARCHAR NOT NULL, -- The same for this field
        year_founded INT,
    );
"""
