from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
import time
import random
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from Scraping_darty import get_driver, simulate_human_behavior
from selenium.webdriver import Chrome, Firefox
import undetected_chromedriver as uc
#Pour Linux:
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
import os
from webdriver_manager.firefox import GeckoDriverManager

chrome_options = Options()
chrome_options.add_argument("--disable-gpu")  # desactiver les GPU hardware acceleration
chrome_options.add_argument("--headless") 


path="Rakuten.xlsx"
url = "https://fr.shopping.rakuten.com/mfp/12362131/apple-iphone-16?pid=13159484162"

CHROME_DATA_DIR = "C:/Users/zoero/AppData/Local/Google/Chrome/User Data/Default"
rakuten = "https://fr.shopping.rakuten.com/"

df = pd.read_excel(path)

def get_driver(CHROME_DATA_DIR):
    """
    Initializes and returns a Chrome WebDriver instance with specific options.

    The WebDriver is configured with the following options:
    - Disables GPU usage.
    - Disables the sandbox environment.
    - Disables Blink features that are controlled by automation.
    - Starts the browser maximized.
    - Disables browser extensions.
    - Uses a specific user data directory for Chrome.
    - Sets a custom user agent string.

    Returns:
        webdriver.Chrome: An instance of Chrome WebDriver with the specified options.
    """
    options = uc.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")

    options.add_argument(f"user-data-dir={CHROME_DATA_DIR}")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    driver = uc.Chrome(options=options)
    return driver

def getData(driver, url, excel_path):
    df = pd.read_excel(path)
    driver.get(url)
    date= datetime.today().strftime('%Y-%m-%d %H:%M')
    mainProductTitle = driver.find_element(By.CLASS_NAME, "detailHeadline").text
    price = driver.find_element(By.CLASS_NAME, "price").text
    neuf_occas = driver.find_element(By.CLASS_NAME, "offerType").text
    seller = driver.find_element(By.CLASS_NAME, "nameSeller").text
    noteSeller = driver.find_element(By.CLASS_NAME, "sellerRating").text
    
    #ajout dans fichier excel
    main_product_row = pd.DataFrame({'Date': [date], 'Nom': [mainProductTitle], 'Prix': [price], 
                            'Vendeur':[seller], 'Etat':[neuf_occas],
                            'note_Vendeur':[noteSeller]                            
                            })
    
    df = pd.concat([df, main_product_row], ignore_index=True)
    
    #autres offres
    items = driver.find_elements(By.CLASS_NAME, 'itemlist')
    otherOffersPrices = []
    etats = []
    partnerNames = []
    sellerNames = []
    notesSellers = []
    
    for item in items:
        try:
            try:
        # Try to find the price using the first class
                price = item.find_element(By.CLASS_NAME, 'PriceInformation-module_actualPrice__0vJzI').text
            except:
                try:
            # If the first fails, try the 'price' class
                    price = item.find_element(By.CLASS_NAME, 'price').text
                except Exception as e:
                    print("Price not found with exception ", e)
            try:       
                etat = item.find_element(By.CLASS_NAME,'V5Box-root').text
            except:
                try:
            # If the first fails, try the 'etat' class
                    etat = item.find_element(By.CLASS_NAME, 'label.state').text
                except Exception as e:
                    print("State not found with exception ", e)
            partnerName = item.find_element(By.CLASS_NAME, 'PartnerInformation-module_partnerInformationContainer__Pu3LX').text
            sellerName = item.find_element(By.CLASS_NAME, 'nameSeller').text
            noteSeller = driver.find_element(By.CLASS_NAME, "sellerRating").text
            
            
            otherOffersPrices.append(price)
            etats.append(etat)
            notesSellers.append(noteSeller)
            partnerNames.append(partnerName)
            sellerNames.append(sellerName)
            
        except:
            print("Data not found")
    print('main prod',main_product_row, '\n')
    print('autre:\n')
    print(otherOffersPrices)
    print(etats)
    print(notesSellers)
    print(partnerNames)
    print(sellerNames)
    
    



def simulate_human_behavior(driver):
    """
    Simulates human-like interactions with the web page.

    This function randomly scrolls the web page up and down to mimic the behavior of
    a human user, helping to avoid detection by anti-bot mechanisms on the website.

    Args:
        driver (webdriver.Chrome): The WebDriver instance used to interact with the browser.

    Returns:
        None
    """
    logging.info("Simulation de comportement humain...")
    try:
        num_scrolls = random.randint(3, 6)
        
        for _ in range(num_scrolls):
            scroll_start = random.randint(0, 500)
            scroll_end = scroll_start + random.randint(500, 1000)
            driver.execute_script(f"window.scrollTo({scroll_start}, {scroll_end});")
            
            time.sleep(random.randint(1, 5))
            
            scroll_start = random.randint(0, 500)
            scroll_end = scroll_start - random.randint(500, 1000)
            driver.execute_script(f"window.scrollTo({scroll_start}, {scroll_end});")
            
            time.sleep(random.randint(1, 5))
        
        logging.info("Comportement humain simulé.")
    except Exception as e:
        logging.error(f"Erreur lors de la simulation de comportement humain : {e}")

def main():
    nb=0
    while nb<1:
        driver = get_driver(CHROME_DATA_DIR)
        
        if driver:
            try:
                getData(driver,url,path)
                #action
                #df.to_excel(path, index=False)
                print('Requête ', nb,'terminée avec succès')
        
            except Exception as e:
                print(f"Erreur lors du scraping : {str(e)}")
               
            finally:
                driver.quit()
        else:
            print("Impossible de lancer un navigateur.")
            
        nb+=1
        #délais entre deux prises en secondes
        time.sleep(5)


if __name__=="__main__":
    try:
        main()
    except Exception as e:
         print(f"Erreur lors du scraping : {str(e)}")