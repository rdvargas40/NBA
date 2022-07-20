"""
SQL Queries
"""
import configparser
import psycopg2
import pandas.io.sql as sqlio
import pandas as pd


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

def read_sql(query: str) -> pd.DataFrame:
    """
    Exceutes PostgreSQL SLECT Query to read and download a DataFrame from the NBA Database
    Args:
        query (str): the SELECT Quqery
    Returns:
        pd.DataFrame the requested data
    """
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
    conn.set_session(autocommit=True)
    df = sqlio.read_sql_query(query, conn)
    conn.close()
    return df
    