from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from Scraping_darty import get_driver, simulate_human_behavior
from selenium.webdriver import Chrome, Firefox

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

#Pour Windows:
CHROME_DATA_DIR ="C:/Users/ANIMATEUR/AppData/Local/Google/Chrome/User Data/Default"

url_xiaomi = "https://fr.shopping.rakuten.com/offer/buy/12835760342/xiaomi-redmi-13c-17-1-cm-6-74-double-sim-android-13-4g-usb-type-c.html"
#Xiaomi Redmi 13C 17,1 cm noir 256Go
url_iphone14 = "https://fr.shopping.rakuten.com/mfp/shop/8450779/apple-iphone-14-pro-max?pid=9176573234&sellerLogin=tsxy&fbbaid=16433862867&rd=1"
#Apple iPhone 14 Pro Max Noir Sideral 128 Go

df = pd.read_excel(path)

def get_firefox_driver():
    try:
        firefox_options = Options()
        firefox_options.add_argument("--disable-gpu")
        firefox_options.add_argument("--headless")  
        service = Service(executable_path=GeckoDriverManager().install())

        # Lancer Firefox
        driver = webdriver.Firefox(service=service, options=firefox_options)
        return driver
    except Exception as e:
        print(f"Échec avec Firefox: {str(e)}")
        return None

def addInfoToFile(url, driver):
    driver.get(url)
    simulate_human_behavior(driver)
    date= datetime.today().strftime('%Y-%m-%d %H:%M')
    price = driver.find_element(By.CLASS_NAME, "price").text
    name = driver.find_element(By.CLASS_NAME,"detailHeadline").text
    seller = driver.find_element(By.CLASS_NAME,"nameSeller").text
    livraison_prix = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.shipping li span.value")))
    
    new_row = pd.DataFrame({'Date': [date], 'Name': [name], 'Price': [price], 
                            'Vendeur':[seller], 'Prix de livraison':[livraison_prix],
                            
                            })
    simulate_human_behavior(driver)
    global df
    df = pd.concat([df, new_row], ignore_index=True)
    

def getMoreOffers(url):
    driver = get_driver()
    driver.get(url)
    moreAnnouncementButton = driver.find_element(By.CLASS_NAME,"moreAnnouncementLink")
    moreAnnouncementButton.click()


def main():
    nb=0
    while nb<1:
        driver = get_driver(CHROME_DATA_DIR)
        
        if driver is None:
            print("Lancement avec Firefox...")
            driver = get_firefox_driver()
        
        if driver:
            try:
                addInfoToFile(url_iphone14, driver)
                df.to_excel(path, index=False)
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
    main()