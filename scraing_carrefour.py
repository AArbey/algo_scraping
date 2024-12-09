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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

URL = "https://www.carrefour.fr/"

HTML_SELECTORS = {
    "accept_condition": "onetrust-accept-btn-handler",
    "search_bar": "header-search-bar",
    "product": "c-text.c-text--size-m.c-text--style-p.c-text--bold.c-text--spacing-default.product-card-title__text.product-card-title__text--hoverable",
    "name": "product-title__title c-text c-text--size-m c-text--style-h3 c-text--spacing-default",
    "price": "product-price__content c-text c-text--size-m c-text--style-subtitle c-text--bold c-text--spacing-default",
    "cents": "product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "currency":"product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "seller": "c-link c-link--size-s c-link--tone-main",
    "delivery_info": "delivery-infos__time c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "more_offers_button": "//button[contains(text(), 'offres')]",
    "side_panel": "c-modal__container c-modal__container--position-right",
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
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["search_bar"]))
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
        print("Loading product page...")
        driver.get(product_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "product-title__title")))
        soup = BeautifulSoup(driver.page_source, 'lxml')
        product_name_element = soup.find('h1', class_=HTML_SELECTORS["name"])
        if not product_name_element:
            print("Nom du produit introuvable.")
            return None
        print("Product name found.")
        return {
            "Platform": "Carrefour",
            "name": product_name_element.get_text(strip=True),
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        }
    except Exception as e:
        print(f"Erreur pendant le scraping du produit : {e}")
        return None

def click_more_offers(driver):
    try:
        print("Attempting to click 'More Offers' button...")
        time.sleep(2)
        more_offers_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, HTML_SELECTORS["more_offers_button"]))
        )
        time.sleep(1)
        driver.execute_script("arguments[0].click();", more_offers_button)
        print("Successfully clicked 'More Offers' button.")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, HTML_SELECTORS["side_panel"]))
        )
        return driver.current_url
    except TimeoutException:
        print("Timeout while waiting for 'More Offers' button or offers to load.")
    except Exception as e:
        print(f"Error clicking 'More Offers' button: {e}")
    return None

def fetch_data_from_side_panel(driver):
    try:
        print("Scraping data from side panel...")
        soup = BeautifulSoup(driver.page_source, 'lxml')

        sellers = soup.find_all('a', class_=HTML_SELECTORS["seller"])
        delivery_infos = soup.find_all('p', class_="delivery-infos__time c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default")
        prices = soup.find_all('p', class_="product-price__content c-text c-text--size-m c-text--style-subtitle c-text--bold c-text--spacing-default")
        cents = soup.find_all('p', class_="product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default")
        seller_ratings = soup.find_all('span', class_="rating-stars__slot c-text c-text--size-m c-text--style-p c-text--spacing-default")

        fetched_data = []
        for i, seller in enumerate(sellers):
            price_text = ""
            if i < len(prices):
                price_text += prices[i].get_text(strip=True).replace("€", "")
            if i < len(cents):
                price_text += cents[i].get_text(strip=True).replace("€", "")
            price_text += "€"
            rating = seller_ratings[i].get_text(strip=True) if i < len(seller_ratings) else "Non spécifié"
            fetched_data.append({
                "seller": seller.get_text(strip=True),
                "delivery_info": delivery_infos[i].get_text(strip=True) if i < len(delivery_infos) else "Non spécifié",
                "price": price_text,
                "seller_rating": rating
            })
        return fetched_data
    except Exception as e:
        print(f"Erreur lors de la récupération des données du panneau latéral : {e}")
        return []

def write_combined_data_to_csv(data, sellers_data, csv_file="D:\\carrefour.csv"):
    if not data:
        print("Aucune donnée de produit à écrire.")
        return
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        if not file_exists:
            writer.writerow(["Platform", "Product Name", "Seller", "Delivery Info", "Price", "Seller Rating", "Timestamp"])
        for seller in sellers_data:
            writer.writerow([
                data["Platform"], data["name"], seller["seller"],
                seller["delivery_info"], seller["price"], seller["seller_rating"],
                data["timestamp"]
            ])
        writer.writerow(["----------------------------------------------------------------------------------------------------------"])

def main():
    chrome_options = Options()
    chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
    service = Service('C:\\Users\\nsoulie\\Downloads\\chromedriver-win64 (1)\\chromedriver-win64\chromedriver.exe')
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    product_ids = ['0195949822865', '0195949821899', '0195949821899', '0195949724169', '0195949723216', '0195949722264', '0195949773488']

    accept_condition(driver)
    for product_id in product_ids:
        try:
            print(f"Scraping product with ID: {product_id}")
            search_product(driver, product_id)
            product_url = get_product_url(driver)
            data = scrape_product(driver, product_url)
            if data:
                click_more_offers(driver)
                sellers_data = fetch_data_from_side_panel(driver)
                write_combined_data_to_csv(data, sellers_data)
        except Exception as e:
            print(f"Erreur pour le produit {product_id}: {e}")

    driver.quit()

if __name__ == "__main__":
    main()
