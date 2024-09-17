import csv
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL = "https://www.e.leclerc/"

HTML_SELECTORS = {
    "accept_condition": "didomi-notice-agree-button",
    "search_bar": "input.search-input.input-padding.ng-untouched.ng-pristine.ng-valid",
    "product": "d-flex.d-sm-block.flex-row",
    "name": ".product-block-title.clamp.clamp-2.product-block-title-short",
    "price": "price-unit ng-star-inserted",
    "currency": "price-symbol",
    "cents": "price-cents",
    "seller": "fw-500",
    "more_offers_link": "//button[@class='btn btn-secondary btn-tran' and contains(text(), 'Consulter')]"
}

def accept_condition(driver):
    driver.get(URL)
    time.sleep(5)
    try:
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"]))
        ).click()
    except Exception as e:
        print(f"Erreur lors de l'acceptation des conditions : {e}")

def search_product(driver, search_query):
    try:
        search_bar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, HTML_SELECTORS["search_bar"]))
        )
        search_bar.click()
        search_bar.send_keys(search_query)
        search_bar.send_keys(Keys.RETURN)
    except Exception as e:
        print(f"Erreur lors de la recherche: {e}")

def get_product_url(driver):
    try:
        product_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["product"]))
        )
        product_link.click()
        time.sleep(2)
        return driver.current_url
    except Exception as e:
        print(f"Erreur lors de la récupération du produit: {e}")
        return None

def scrape_product(driver, product_url):
    try:
        driver.get(product_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        soup = BeautifulSoup(driver.page_source, 'lxml')
        product_name_element = soup.select_one(HTML_SELECTORS["name"])
        if not product_name_element:
            print("Nom du produit manquant.")
            return None

        product_data = {
            "Platform": "E.Leclerc",
            "name": product_name_element.get_text(strip=True),
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        }
        return product_data
    except Exception as e:
        print(f"Erreur pendant le scraping du produit: {e}")
        return None

def click_more_offers(driver):
    try:
        more_offers_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, HTML_SELECTORS["more_offers_link"]))
        )
        more_offers_button.click()
        time.sleep(2)
        return driver.current_url
    except Exception as e:
        print(f"Erreur lors du clic sur 'Consulter': {e}")
        return None

def fetch_data_from_pages(driver, url, html_selector, data_type):
    if not url:
        print(f"URL non valide pour récupérer {data_type}.")
        return []

    fetched_data = []
    seen_urls = set()
    while url and url not in seen_urls:
        seen_urls.add(url)
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            soup = BeautifulSoup(driver.page_source, 'lxml')

            if data_type == 'sellers':
                sellers = soup.find_all('a', class_=HTML_SELECTORS[html_selector])
                fetched_data.extend([s.get_text(strip=True) for s in sellers])
            elif data_type == 'prices':
                prices = soup.find_all('div', class_=HTML_SELECTORS["price"])
                currencies = soup.find_all('span', class_=HTML_SELECTORS["currency"])
                cents = soup.find_all('span', class_=HTML_SELECTORS["cents"])
                fetched_data.extend(
                    [f"{prices[i].get_text(strip=True)}.{cents[i].get_text(strip=True)} {currencies[i].get_text(strip=True)}"
                     for i in range(min(len(prices), len(currencies), len(cents)))]
                )
            time.sleep(5)
        except Exception as e:
            print(f"Erreur lors de la récupération des {data_type}: {e}")
            break
    return fetched_data

def write_combined_data_to_csv(data, sellers, prices, csv_file="scraping_data.csv"):
    if not data:
        print("Aucune donnée de produit à écrire.")
        return
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        if not file_exists:
            writer.writerow(["Platform", "Product Name", "Price", "Seller", "Timestamp"])

        if sellers and prices:
            min_length = min(len(sellers), len(prices))
            for i in range(min_length):
                writer.writerow([
                    data["Platform"], data["name"], prices[i], sellers[i], datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                ])
        writer.writerow(["----------------------------------------------------------------------------------------------------------"])

def main():
    chrome_options = Options()
    chrome_options.binary_location = '/usr/bin/google-chrome'
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        accept_condition(driver)
        search_product(driver, '6941812757383')

        product_url = get_product_url(driver)
        if product_url:
            product_data = scrape_product(driver, product_url)
            if product_data:
                more_offers = click_more_offers(driver)
                sellers, prices = [], []
                if more_offers:
                    sellers = fetch_data_from_pages(driver, more_offers, 'seller', 'sellers')
                    prices = fetch_data_from_pages(driver, more_offers, 'price', 'prices')
                    write_combined_data_to_csv(product_data, sellers, prices)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()