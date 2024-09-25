from selenium.webdriver.support.ui import WebDriverWait as WDW
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import *


class IncomeStatementsReportPage:
    def __init__(self, driver, delay):
        self.driver = driver
        self.delay = delay

        self.power_bi_button = "//a[@role='tab' and text()='Power BI']"
        self.revenue_report_header = "//div[contains(@title, 'REVENUE')][1]"

        self.cosr_of_rev = "//th[@scope='row' and @class='p2']"
        self.random_report_num = "//td[@class='f-numerical f-sub-categorical' and @headers='subcategoryQuarter subcategoryCurYearQuarter subcategoryItem']"

    def switch_to_report_frame(self):
        iframe = self.driver.find_element(By.ID, "mschart")
        self.driver.switch_to.frame(iframe)

    def get_revenue_report_title(self):
        report_header = self.driver.find_element((By.XPATH,self.revenue_report_header))
        return report_header.get_attribute("title")

    def get_cost_of_rev(self):
        cot_rev_text = self.driver.find_element(By.XPATH,self.cosr_of_rev)
        return cot_rev_text.text


    def get_rand_report_num(self):
            rand_report_num = self.driver.find_element(By.XPATH,self.random_report_num)
            return rand_report_num.text
