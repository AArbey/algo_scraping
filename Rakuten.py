from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from Scraping_darty import get_driver, simulate_human_behavior

chrome_options = Options()
chrome_options.add_argument("--disable-gpu")  # desactiver les GPU hardware acceleration
chrome_options.add_argument("--headless") 


path="Rakuten.xlsx"
url_xiaomi = "https://fr.shopping.rakuten.com/offer/buy/12835760342/xiaomi-redmi-13c-17-1-cm-6-74-double-sim-android-13-4g-usb-type-c.html"
#Xiaomi Redmi 13C 17,1 cm noir 256Go
url_iphone14 = "https://fr.shopping.rakuten.com/mfp/shop/8450779/apple-iphone-14-pro-max?pid=9176573234&sellerLogin=tsxy&fbbaid=16433862867&rd=1"
#Apple iPhone 14 Pro Max Noir Sideral 128 Go

df = pd.read_excel(path)

def addInfoToFile(url):
    service = Service('chromedriver.exe')
    driver = get_driver()
    driver.get(url)
    simulate_human_behavior()
    date= datetime.today().strftime('%Y-%m-%d %H:%M')
    price = driver.find_element(By.CLASS_NAME, "price").text
    name = driver.find_element(By.CLASS_NAME,"detailHeadline").text
    seller = driver.find_element(By.CLASS_NAME,"nameSeller").text
    livraison_prix = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.shipping li span.value")))
    
    new_row = pd.DataFrame({'Date': [date], 'Name': [name], 'Price': [price], 
                            'Vendeur':[seller], 'Prix de livraison':[livraison_prix],
                            
                            })
    simulate_human_behavior()
    global df
    df = pd.concat([df, new_row], ignore_index=True)
    driver.quit()

def getMoreOffers(url):
    driver = get_driver()
    driver.get(url)
    moreAnnouncementButton = driver.find_element(By.CLASS_NAME,"moreAnnouncementLink")
    moreAnnouncementButton.click()


def main():
    nb=0
    while nb<1:
        addInfoToFile(url_iphone14)
        df.to_excel(path, index=False)
        print('done')
        nb+=1
        #délais entre deux prises en secondes
        time.sleep(5)


if __name__=="__main__":
    main()