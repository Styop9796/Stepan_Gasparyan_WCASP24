import pytest
import psycopg2
import yaml

# Load SQL queries from config file
def load_sql_queries():
    with open('config/config_SQL_example.yaml', 'r') as file:
        return yaml.safe_load(file)

@pytest.fixture(scope='session')
def db_connection():
    connection = psycopg2.connect(
        dbname='EPAM_DWH',
        user='postgres',
        password='password',
        host='localhost',
        port='5432'

    )
    yield connection.cursor()
    connection.close()

@pytest.fixture(scope='session')
def sql_queries():
    return load_sql_queries()['tests']
