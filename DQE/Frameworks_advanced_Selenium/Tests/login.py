import os
import sys
import yaml
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pytest
from Pages.incomeStatementsReportPage import IncomeStatementsReportPage


def get_selenium_config(config_name):
    module_dir = os.path.dirname(os.path.abspath(sys.modules[__name__].__file__))
    parent_dir = os.path.dirname(module_dir)
    with open(os.path.join(parent_dir, 'Configs', config_name), 'r') as stream:
        config = yaml.safe_load(stream)
    return config['global']


@pytest.fixture(scope="function")
def open_income_statements_report_webpage():
    report_uri = get_selenium_config('config_selenium.yaml')['report_uri']
    delay = get_selenium_config('config_selenium.yaml')['delay']
    driver = webdriver.Chrome()
    driver.set_window_size(1024, 600)
    driver.get(report_uri)

    income_report = IncomeStatementsReportPage(driver, delay)
    yield income_report
    driver.close()

"""
def test_01_open_decomposition_tree_visualization(open_income_statements_report_webpage):
    report_page = open_income_statements_report_webpage
    report_title = report_page.get_revenue_report_title()
    assert report_title == 'REVENUE (in billions)'
"""


def test_02_title_cost_rev(open_income_statements_report_webpage):
    report_page = open_income_statements_report_webpage
    report_text = report_page.get_cost_of_rev()
    assert report_text == 'Total revenue'

def test_03_random_repot_number(open_income_statements_report_webpage):
    report_page = open_income_statements_report_webpage
    report_num = report_page.get_rand_report_num()
    assert report_num == '$ 15,588'