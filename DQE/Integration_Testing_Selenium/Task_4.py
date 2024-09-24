from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
import time

browser = webdriver.Chrome()

browser.get('https://google.com')

search_box = browser.find_element(By.NAME, 'q')
search_box.send_keys('Selenium')
search_box.submit()

first_link = WebDriverWait(browser, 10).until(expected_conditions.element_to_be_clickable((By.CSS_SELECTOR, 'h3'))
)
first_link.click()

time.sleep(2)

browser.save_screenshot('screenshot.png')

browser.quit()
