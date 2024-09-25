import pytest
import yaml

def get_numbers_data(config_name):
    with open(config_name, 'r') as stream:
        config = yaml.safe_load(stream)
    return config['cases']

def add_numbers(a, b, c):
    if not all(isinstance(x, (int, float)) for x in [a, b, c]):
        raise TypeError('Please check the parameters. All of them must be numeric')
    return a + b + c



cases = get_numbers_data('config.yaml')

@pytest.mark.smoke
@pytest.mark.parametrize("case", cases, ids=[case['case_name'] for case in cases])
def test_add_numbers(case):
    input_values = case['input']
    expected = case['expected']
    result = add_numbers(*input_values)
    assert result == expected


@pytest.mark.critical
@pytest.mark.parametrize("input_values", [
    ('a', 2, 1),
    (None, 1, 3), 
    ([], 1, 4),  
])
def test_add_invalid_types(input_values):
    with pytest.raises(TypeError, match='Please check the parameters. All of them must be numeric') as exc_info:
        add_numbers(*input_values)
    assert exc_info.type is TypeError
