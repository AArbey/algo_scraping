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
import subprocess

# Configuration
URL = "https://www.carrefour.fr/"
HTML_SELECTORS = {
    "accept_condition": "onetrust-accept-btn-handler",
    "search_bar": "header-search-bar",
    "product": "c-text.c-text--size-m.c-text--style-p.c-text--bold.c-text--spacing-default.product-card-title__text.product-card-title__text--hoverable",
    "name": "product-title__title c-text c-text--size-m c-text--style-h3 c-text--spacing-default",
    "price": "product-price__content c-text c-text--size-m c-text--style-subtitle c-text--bold c-text--spacing-default",
    "cents": "product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "currency": "product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "seller": "c-link c-link--size-s c-link--tone-main",
    "delivery_info": "delivery-infos__time c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "more_offers_button": "//button[contains(text(), 'offres')]",
    "side_panel": "c-modal__container c-modal__container--position-right",
}


def start_xvfb():
    xvfb_process = subprocess.Popen(
        ["Xvfb", ":98", "-screen", "0", "1920x1080x24"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    os.environ["DISPLAY"] = ":98"
    return xvfb_process


def accept_condition(driver):
    driver.get(URL)
    try:
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"]))
        ).click()
    except Exception as e:
        print(f"Erreur accept condition : {e}")


def search_product(driver, search_query):
    try:
        search_bar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["search_bar"]))
        )
        search_bar.click()
        search_bar.send_keys(search_query)
        search_bar.send_keys(Keys.RETURN)
    except Exception as e:
        print(f"Erreur recherche : {e}")


def get_product_url(driver):
    try:
        product_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["product"]))
        )
        product_link.click()
        time.sleep(2)
        return driver.current_url
    except Exception as e:
        print(f"Erreur URL produit : {e}")
        return None


def scrape_product(driver, product_url):
    try:
        driver.get(product_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "product-title__title"))
        )
        soup = BeautifulSoup(driver.page_source, 'lxml')

        name_elem = soup.find('h1', class_=HTML_SELECTORS["name"])
        if not name_elem:
            print("Nom produit introuvable")
            return None

        seller_elem = soup.find('a', class_=HTML_SELECTORS["seller"])
        price_elem = soup.find('p', class_=HTML_SELECTORS["price"])
        cents_elem = soup.find('p', class_=HTML_SELECTORS["cents"])
        delivery_elem = soup.find('p', class_=HTML_SELECTORS["delivery_info"])

        main_offer = {
            "seller": seller_elem.get_text(strip=True) if seller_elem else "Non spécifié",
            "price": f"{price_elem.get_text(strip=True)}{cents_elem.get_text(strip=True)}€" if price_elem and cents_elem else "Non spécifié",
            "delivery_info": delivery_elem.get_text(strip=True) if delivery_elem else "Non spécifié",
            "seller_rating": "Non spécifié"
        }

        return {
            "Platform": "Carrefour",
            "name": name_elem.get_text(strip=True),
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "main_offer": main_offer
        }

    except Exception as e:
        print(f"Erreur scraping produit : {e}")
        return None


def click_more_offers(driver):
    try:
        time.sleep(2)
        more_offers_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, HTML_SELECTORS["more_offers_button"]))
        )
        driver.execute_script("arguments[0].click();", more_offers_button)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, HTML_SELECTORS["side_panel"]))
        )
        return driver.current_url
    except Exception as e:
        print(f"Erreur bouton offres : {e}")
    return None


def fetch_data_from_side_panel(driver):
    try:
        full_soup = BeautifulSoup(driver.page_source, 'lxml')
        side_panel = full_soup.find('div', class_=HTML_SELECTORS["side_panel"])
        if not side_panel:
            return []

        sellers = side_panel.find_all('a', class_=HTML_SELECTORS["seller"])
        delivery_infos = side_panel.find_all('p', class_=HTML_SELECTORS["delivery_info"])
        prices = side_panel.find_all('p', class_=HTML_SELECTORS["price"])
        cents = side_panel.find_all('p', class_=HTML_SELECTORS["cents"])
        seller_ratings = side_panel.find_all('span', class_="rating-stars__slot c-text c-text--size-m c-text--style-p c-text--spacing-default")

        data = []
        for i, seller in enumerate(sellers):
            euros = prices[i].get_text(strip=True).replace("€", "") if i < len(prices) else ""
            centimes = cents[i].get_text(strip=True).replace("€", "") if i < len(cents) else ""
            price_text = f"{euros}{centimes}€" if euros or centimes else "Non spécifié"

            data.append({
                "seller": seller.get_text(strip=True),
                "delivery_info": delivery_infos[i].get_text(strip=True) if i < len(delivery_infos) else "Non spécifié",
                "price": price_text,
                "seller_rating": seller_ratings[i].get_text(strip=True) if i < len(seller_ratings) else "Non spécifié"
            })

        return data
    except Exception as e:
        print(f"Erreur scraping panel : {e}")
        return []


def write_combined_data_to_csv(data, sellers_data, csv_file="scraping_carrefour.csv"):
    if not data:
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
        writer.writerow(["-" * 100])


def main():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.binary_location = '/usr/bin/google-chrome'
    chrome_options.add_argument("--user-data-dir=/tmp/chrome_user_data_vm")
    service = Service('/usr/local/bin/chromedriver-linux64/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    product_ids = ['0195949822865', '0195949821899', '0195949724169']

    accept_condition(driver)

    for product_id in product_ids:
        try:
            print(f"Scraping ID: {product_id}")
            search_product(driver, product_id)
            product_url = get_product_url(driver)
            data = scrape_product(driver, product_url)
            if data:
                click_more_offers(driver)
                sellers_data = [data["main_offer"]] + fetch_data_from_side_panel(driver)
                write_combined_data_to_csv(data, sellers_data)
        except Exception as e:
            print(f"Erreur produit {product_id} : {e}")

    driver.quit()


if __name__ == "__main__":
    xvfb = start_xvfb()
    try:
        main()
    finally:
        xvfb.terminate()
        xvfb.wait()
        if os.path.exists("/tmp/.X98-lock"):
            os.remove("/tmp/.X98-lock")
