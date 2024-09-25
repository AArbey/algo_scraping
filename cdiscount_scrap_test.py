import requests
import sys
import csv
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from twocaptcha import TwoCaptcha
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL = "https://www.cdiscount.com/"

HTML_SELECTORS = {
    "accept_condition": "footer_tc_privacy_button_2",
    "search_bar": "c-form-input.type--search.js-search__input",
    "first_product": "alt-h4.u-line-clamp--2",
    "first_product_name": "h2 u-truncate",
    "first_product_price": "c-price c-price--promo c-price--xs",
    "first_product_seller": "a[aria-controls='SellerLayer']",
    "more_offers_link": "offres neuves",
    "seller_name": "slrName",
    "seller_status": "slrType",
    "get_price": "c-price c-price--xl c-price--promo",
    "shipping_country": "c-shipping__country",
    "product_condition": "c-productCondition"
}

def solve_hcaptcha_and_submit_form(driver, captcha_page_url, site_key, two_captcha_api_key):
    sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    os.getenv(two_captcha_api_key, site_key)


def accept_condition(driver):
    driver.implicitly_wait(30)
    print("------------------accept_condition--------------------")
    try:
        driver.get(URL)
        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"]))).click()
        print("Conditions accepted.")
    except TimeoutException:
        print("Condition acceptance button not found.")

def search_product(driver, search_query):
    print("------------------search_product--------------------")
    try:
        search_bar = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["search_bar"])))
        search_bar.click()
        search_bar.send_keys(search_query)
        search_bar.send_keys(Keys.RETURN)
    except Exception as e:
        print(f"Error in searching product: {e}")

def get_first_product_url(driver):
    print("------------------get_first_product_url--------------------")
    try:
        product_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["first_product"])))
        product_link.click()
        return driver.current_url
    except Exception as e:
        print(f"Error in retrieving first product: {e}")
        return None

def scrape_product_details(driver, product_url):
    print("------------------scrape_product_details--------------------")
    try:
        driver.get(product_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))

        soup = BeautifulSoup(driver.page_source, 'lxml')
        product_name = soup.find('div', class_=HTML_SELECTORS["first_product_name"]).get_text(strip=True)
        product_price = soup.find('span', class_=HTML_SELECTORS["first_product_price"]).get_text(strip=True)
        product_seller = soup.select_one(HTML_SELECTORS["first_product_seller"]).get_text(strip=True)
        return {"Platform": "Cdiscount", "name": product_name or "N/A", "price": product_price or "N/A", "seller": product_seller or "N/A", "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
    except Exception as e:
        print(f"Error scraping product details: {e}")
        return None

def get_more_offers_page(driver):
    print("------------------get_more_offers_page--------------------")
    try:
        more_offers_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, HTML_SELECTORS["more_offers_link"])))
        more_offers_link.click()
        time.sleep(5)
        return driver.current_url
    except Exception as e:
        print(f"Error in getting more offers page: {e}")
        return None

def fetch_data_from_pages(driver, url, html_selector, data_type):

    if not url:
        print(f"No valid URL for fetching {data_type}.")
        return []

    fetched_data = []
    while url:
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))

            soup = BeautifulSoup(driver.page_source, 'lxml')
            if data_type == 'sellers':
                sellers = soup.find_all('a', class_=HTML_SELECTORS[html_selector])
                seller_statuses = soup.find_all('span', class_=HTML_SELECTORS["seller_status"])
                fetched_data.extend([(sellers[i].get_text(strip=True), seller_statuses[i].get_text(strip=True)) for i in range(len(sellers))])
            else:
                elements = soup.find_all('p', class_=HTML_SELECTORS[html_selector])
                fetched_data.extend([elem.get_text(strip=True) for elem in elements])

            next_page = soup.find('a', string='Next')
            url = next_page.get('href') if next_page else None
            time.sleep(5)
        except Exception as e:
            print(f"Error fetching {data_type}: {e}")
            break

    return fetched_data

def write_combined_data_to_csv(sellers, prices, product_name, csv_file="scraping_data.csv"):

    if not sellers or not prices:
        print("No sellers or prices to write.")
        return

    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        if not file_exists:
            writer.writerow(["Platform", "Product Name", "Price", "Seller", "Timestamp"])
    min_length = min(len(sellers), len(prices))
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["Platform", "Product Name", "Price", "Seller", "Seller Status", "Timestamp"])
        for i in range(min_length):
            seller_name, seller_status = sellers[i]
            writer.writerow(["Cdiscount", product_name, prices[i], seller_name, seller_status, datetime.now().strftime("%d/%m/%Y %H:%M:%S")])
        writer.writerow(["----------------------------------------------------------------------------------------------------------"])
    print(f"Combined data written to {csv_file}")


def main():

    chrome_options = Options()
    chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
    chrome_options.add_argument("--disable-gpu")
    ##chrome_options.add_argument("--headless")
    service = Service('chromedriver.exe')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        captcha_page_url = URL
        site_key = "f6af350b-e1f0-4be9-847a-de731e69489a"
        two_captcha_api_key = "48769c3dfb7194a2639f7f5627378bad"

        solve_hcaptcha_and_submit_form(driver, captcha_page_url, site_key, two_captcha_api_key)
        time.sleep(5)
        accept_condition(driver)

        product_to_search = 'xia1699956501953'
        search_product(driver, product_to_search)
        product_url = get_first_product_url(driver)
        if product_url:
            product_data = scrape_product_details(driver, product_url)
            other_offers_url = get_more_offers_page(driver)
            if other_offers_url:
                sellers = fetch_data_from_pages(driver, other_offers_url, 'seller_name', 'sellers')
                prices = fetch_data_from_pages(driver, other_offers_url, 'get_price', 'prices')

                write_combined_data_to_csv(sellers, prices, product_data["name"])
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
