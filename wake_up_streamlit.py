from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from streamlit_app import STREAMLIT_APPS
import datetime

# Set up Selenium webdriver
options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome(options=options)

# Initialize log file
with open("wakeup_log.txt", "a") as log_file:
    log_file.write(f"Execution started at: {datetime.datetime.now()}\n")

    # Iterate through each URL in the list
    for url in STREAMLIT_APPS:
        try:
            driver.get(url)
            
            # Wait for the page to load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    
            # Check if the wake up button exists
            try:
                button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, '_restartButton_') and contains(text(), 'Yes, get this app back up!')]"))
                )
                button.click()
                
                # Wait for the app to start (adjust the timeout and condition as needed)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'stApp')]"))
                )
                
                log_file.write(f"[{datetime.datetime.now()}] Successfully woke up app at: {url}\n")
            except TimeoutException:
                log_file.write(f"[{datetime.datetime.now()}] Button not found or app didn't start for: {url}\n")
        
        except Exception as e:
            log_file.write(f"[{datetime.datetime.now()}] Error for app at {url}: {str(e)}\n")

# Close the browser
driver.quit()
