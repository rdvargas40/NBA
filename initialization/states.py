"""
Tabla de Información de los Estados
"""
import pandas as pd
from sql_queries import exec_sql_query

def create_states_table():
    """ Creates the states information table """
    exec_sql_query("""--sql
        CREATE TABLE IF NOT EXISTS states (
            state VARCHAR NOT NULL,
            year INT NOT NULL,
            personal_income FLOAT,
            pipc FLOAT,
            population INT,
            PRIMARY KEY(state, year)
        );
    """)

def states_extraction_proccess(path='/data/states.csv'):

    raw_states_df = pd.read_csv(path, skiprows=range(4))

    # Filters
    raw_states_df.dropna(subset=['GeoName', 'Description'], inplace=True)
    raw_states_df = raw_states_df[raw_states_df.GeoName != 'United States']

    # Drop Columns
    raw_states_df.drop(['GeoFips', 'LineCode'], axis=1, inplace=True)

    # Renaming
    raw_states_df.rename(columns={'GeoName': 'state'}, inplace=True)
    raw_states_df.Description.replace({
        'Personal income (millions of dollars)': 'personal_income',
        'Population (persons) 1/': 'population',
        'Per capita personal income (dollars) 2/': 'pipc'
    }, inplace=True)

    # Pivot
    pivoted_states_df = raw_states_df.set_index(['state', 'Description']).stack().unstack(1)
    pivoted_states_df.reset_index(inplace=True)

    # Renaming
    pivoted_states_df.rename(columns={'level_1': 'year'}, inplace=True)

    # Datatypes
    pivoted_states_df.year = pd.to_numeric(pivoted_states_df.year, errors='coerce', downcast='integer')
    pivoted_states_df.personal_income = pd.to_numeric(pivoted_states_df.personal_income, errors='coerce', downcast='float')
    pivoted_states_df.pipc = pd.to_numeric(pivoted_states_df.pipc, errors='coerce', downcast='float')
    pivoted_states_df.population = pd.to_numeric(pivoted_states_df.population, errors='coerce', downcast='integer')

    # Cleaning
    pivoted_states_df.state.replace({'Alaska *': 'Alaska'}, inplace=True)

    return pivoted_states_df


def load_states_data(path='/data/states.csv'):
    states_df = states_extraction_proccess(path)

    # Definition of the values to be inserted
    update_many_query = """--sql
        INSERT INTO
            states (state, year, personal_income, pipc, population)
        VALUES
    """
    states_df.fillna('null_special_code', inplace=True)
    for iter, row in states_df.iterrows():
        keys = ['state', 'year', 'personal_income', 'pipc', 'population']
        state_string = tuple([row[k] for k in keys])
        state_string = str(state_string)
        update_many_query += (state_string + ',\n')
    update_many_query = update_many_query[:-2]
    update_many_query = update_many_query.replace("'null_special_code'", "NULL")
    # Definition of the update rule
    update_many_query += """--sql
        ON CONFLICT (state, year)
        DO 
            UPDATE SET (personal_income, pipc, population) =
            (EXCLUDED.personal_income, EXCLUDED.pipc, EXCLUDED.population)
        ;
    """
    
    exec_sql_query(update_many_query)

