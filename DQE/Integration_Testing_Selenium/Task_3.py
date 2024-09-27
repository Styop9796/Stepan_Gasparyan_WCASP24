from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()

driver.get('https://phptravels.com/demo/')

#by class name
first_name_input = driver.find_element(By.CLASS_NAME,'first_name')
whatsapp_number_input = driver.find_element(By.CLASS_NAME,'whatsapp_number')

print(f"First name input element: \n {first_name_input} \n ")

print(f"Whatsapp number input element: \n {whatsapp_number_input} \n ")

# by id

country_id_input= driver.find_element(By.ID,"country_id")
numb1= driver.find_element(By.ID,"numb1")
numb2= driver.find_element(By.ID,"numb2")

print(f"Country id input element: \n {country_id_input} \n ")
print(f"Num 1 element: \n {numb1} \n ")
print(f"Num 2 element: \n {numb2} \n ")

# by name

last_name_input = driver.find_element(By.NAME,"last_name")
business_name_input = driver.find_element(By.NAME,'business_name')

print(f"Last name input element: \n {last_name_input} \n ")
print(f"Business name input element: \n {business_name_input} \n ")

# by XPATH 

first_name_input_by_xpath = driver.find_element(By.XPATH,"//input[@name='first_name']")
submit_button = driver.find_element(By.XPATH,"//button[@id='demo']")

print(f"First name input by xpath element: \n {first_name_input_by_xpath} \n ")

print(f"Submit button element: \n {submit_button} \n ")

# by CSS selector

driver.get('https://phptravels.com/blog/')


post_article = driver.find_element(By.CSS_SELECTOR,'#post-2992')
main_div = driver.find_element(By.CSS_SELECTOR,'.posts-featured')

print(f"Post article element: \n {post_article} \n ")
print(f"Main div element: \n {main_div} \n ")
