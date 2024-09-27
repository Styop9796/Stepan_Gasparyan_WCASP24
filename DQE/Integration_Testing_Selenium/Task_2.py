from selenium import webdriver

browser = webdriver.Chrome()
browser.get('https://google.com/')
print(browser.title)
browser.quit()