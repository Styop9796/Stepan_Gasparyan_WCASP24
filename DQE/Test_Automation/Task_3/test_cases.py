import pytest
import allure
import yaml

def get_numbers_data(config_name):
    with open(config_name, 'r') as stream:
        config = yaml.safe_load(stream)
    return config['tests']




queries_for_smoke = get_numbers_data('sql_confing.yaml')
queries_for_critical = get_numbers_data('sql_for_critical.yaml')

@pytest.mark.smoke
@pytest.mark.parametrize("query", queries_for_smoke,ids=[query['name'] for query in queries_for_smoke])
def test_existance(db_connection,query):
    input = query['sql']
    expected = query['expected']
    db_connection.execute(input)
    num = db_connection.fetchone()[0]
    assert  num == expected



@pytest.mark.critical
@pytest.mark.parametrize("query", queries_for_critical,ids=[query['name'] for query in queries_for_critical])
def test_duplicates(db_connection,query):
    input = query['sql']
    expected = query['expected']
    db_connection.execute(input)
    num = db_connection.fetchone()[0]
    assert  num == expected