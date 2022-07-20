"""
Teams Salaries Table
"""
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from sql_queries import exec_sql_query

salaries_teams_ids = {
    'Cleveland': 1610612739,
    'New York': 1610612752,
    'Detroit': 1610612765,
    'LA Lakers': 1610612747,
    'Atlanta': 1610612737,
    'Dallas': 1610612742,
    'Philadelphia': 1610612755,
    'Milwaukee': 1610612749,
    'Phoenix': 1610612756,
    'Brooklyn': 1610612751,
    'Boston': 1610612738,
    'Portland': 1610612757,
    'Golden State': 1610612744,
    'San Antonio': 1610612759,
    'Indiana': 1610612754,
    'Utah': 1610612762,
    'Oklahoma City': 1610612760,
    'Houston': 1610612745,
    'Charlotte': 1610612766,
    'Denver': 1610612743,
    'LA Clippers': 1610612746,
    'Chicago': 1610612741,
    'Washington': 1610612764,
    'Sacramento': 1610612758,
    'Miami': 1610612748,
    'Minnesota': 1610612750,
    'Orlando': 1610612753,
    'Memphis': 1610612763,
    'Toronto': 1610612761,
    'New Orleans': 1610612740
 }


def create_salaries_table():
    """ Creates the players table """
    exec_sql_query("""--sql
        CREATE TABLE IF NOT EXISTS salaries (
            team_id INT REFERENCES teams(team_id),
            year INT NOT NULL,
            salaries INT NOT NULL,
            PRIMARY KEY(team_id, year)
        );
    """)


def get_season_year_salaries(year):
    year_str = f'{year}-{year+1}'

    # Create an URL object
    url = f'https://hoopshype.com/salaries/{year_str}/'

    # Create object page
    page = requests.get(url)
    # Obtain page's information
    soup = BeautifulSoup(page.text, 'lxml')
    # Obtain information from tag <table>
    table1 = soup.find('table', )
    # Obtain every title of columns with tag <td>
    headers = []
    for i in table1.find_all("td"):
        title = i.text
        headers.append(title)

    new_headers = [s.replace("\n", "").replace("\t", "") for s in headers]
    new_headers = [s.replace(",", "").replace("$", "").replace(".", "") for s in new_headers]
    new_headers_array =np.array(new_headers).reshape((int(len(new_headers)/4),4))

    year_salaries_df = pd.DataFrame(new_headers_array[1:], columns=new_headers_array[0])
    year_salaries_df.set_index('Team', inplace=True)
    year_salaries_ds = year_salaries_df.iloc[:, 1].astype(int)

    year_salaries_ds.index = [salaries_teams_ids[team] for team in year_salaries_ds.index]

    return year_salaries_ds


def update_year_in_database(year):

    year_ds = get_season_year_salaries(year)

    # Definition of the values to be inserted
    update_many_query = """--sql
        INSERT INTO
            salaries (team_id, year, salaries)
        VALUES
    """
    for index, value in year_ds.items():
        team_id = index
        salaries = value
        object_string = str((team_id, year, salaries))
        update_many_query += (object_string + ',\n')
    update_many_query = update_many_query[:-2]

    # Definition of the update rule
    update_many_query += """--sql
        ON CONFLICT (team_id, year)
        DO UPDATE SET salaries = EXCLUDED.salaries
        ;
    """
    
    exec_sql_query(update_many_query)


def populate_database():
    year_span = range(1990, 2022)
    for year in year_span:
        update_year_in_database(year)