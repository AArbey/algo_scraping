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
API_KEY = "48769c3dfb7194a2639f7f5627378bad"
SITE_KEY = "f6af350b-e1f0-4be9-847a-de731e69489a"

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
    "seller_rating": "//*[@id='fpmContent']/div/div[1]/div/div/span",
    "get_price": "c-price c-price--xl c-price--promo",
    "delivery_fee": "priceColor",
    "delivery_date": "//*[@id='fpmContent']/div/div[3]/table/tbody/tr[4]/td[2]/span"
}

def get_hcaptcha_solution():
    response = requests.post("https://2captcha.com/in.php",
            {'key': API_KEY, 'method': 'hcaptcha', 'sitekey': SITE_KEY, 'pageurl': URL})
    captcha_id = response.text.split('|')[1]
    time.sleep(20)
    response = requests.get(f'https://2captcha.com/res.php?key={API_KEY}&action=get&id=({captcha_id}')
    while 'CAPTCHA_NOT_READY' in response.text:
        time.sleep(5)
        response = requests.get(f'https://2captcha.com/res.php?key={API_KEY}&action=get&id=({captcha_id}')
    return response.text.split('|')[1]

def solve_captcha_if_present(driver):
    print("------------------solve_captcha_if_present--------------------")
    try:
        captcha_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-sitekey]')))
        if captcha_element:
            print("CAPTCHA detected. Solving...")
            captcha_solution = get_hcaptcha_solution()
            captcha_input = driver.find_element(By.ID, "h-captcha-response")
            driver.execute_script("arguments[0].value = arguments[1];", captcha_input, captcha_solution)

            submit_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
            driver.execute_script("arguments[0].click();", submit_button)

            WebDriverWait(driver, 10).until_not(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-sitekey]')))
            print("CAPTCHA solved.")
        else:
            print("No CAPTCHA found.")
    except TimeoutException:
        print("No CAPTCHA detected, continuing with the next step.")
    except Exception as e:
        print(f"Error solving CAPTCHA: {e}")

def accept_condition(driver):
    print("------------------accept_condition--------------------")
    try:
        driver.get(URL)
        accept_button = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"])))
        driver.execute_script("arguments[0].scrollIntoView(true);", accept_button)
        accept_button.click()
        print("Conditions accepted.")
    except TimeoutException:
        print("Condition acceptance button not found.")

def search_product(driver, search_query):
    print("------------------search_product--------------------")
    try:
        search_bar = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["search_bar"])))
        driver.execute_script("arguments[0].scrollIntoView(true);", search_bar)
        search_bar.click()
        search_bar.send_keys(search_query)
        search_bar.send_keys(Keys.RETURN)
    except Exception as e:
        print(f"Error in searching product: {e}")

def get_first_product_url(driver):
    print("------------------get_first_product_url--------------------")
    try:
        product_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["first_product"])))
        driver.execute_script("arguments[0].scrollIntoView(true);", product_link)
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
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(1)
        more_offers_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, HTML_SELECTORS["more_offers_link"]))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_offers_link)
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
    delivery_fee = []
    seller_ratings = []
    delivery_dates = []

    while url:
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))

            soup = BeautifulSoup(driver.page_source, 'lxml')

            if data_type == 'sellers':
                sellers = soup.find_all('a', class_=HTML_SELECTORS[html_selector])
                seller_statuses = soup.find_all('span', class_=HTML_SELECTORS["seller_status"])
                delivery_fee = soup.find_all('span', class_=HTML_SELECTORS["delivery_fee"])
                ratings_elements = driver.find_elements(By.XPATH, HTML_SELECTORS["seller_rating"])
                seller_ratings = [rating.text.strip() for rating in ratings_elements]
                delivery_date_elements = driver.find_elements(By.XPATH, HTML_SELECTORS["delivery_date"])
                delivery_dates = [date.text.strip() for date in delivery_date_elements]

                fetched_data.extend([
                    (sellers[i].get_text(strip=True), seller_statuses[i].get_text(strip=True), seller_ratings[i], delivery_fee[i].get_text(strip=True), delivery_dates[i]) 
                    for i in range(len(sellers))
                ])
            else:
                delivery_fee = soup.find_all('span', class_=HTML_SELECTORS["delivery_fee"])
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
            writer.writerow(["Platform", "Product Name", "Price", "Seller", "Seller Status", "Seller Rating", "Delivery Fee", "Delivery Date", "Timestamp"])

        min_length = min(len(sellers), len(prices))
        writer.writerow(["Platform", "Product Name", "Price", "Seller", "Seller Status", "Seller Rating", "Delivery Fee", "Delivery Date", "Timestamp"])
        for i in range(min_length):
            seller_name, seller_status, seller_rating, delivery_fee, delivery_date = sellers[i]
            writer.writerow(["Cdiscount", product_name, prices[i], seller_name, seller_status, seller_rating, delivery_fee, delivery_date, datetime.now().strftime("%d/%m/%Y %H:%M:%S")])
        writer.writerow(["----------------------------------------------------------------------------------------------------------"])

    print(f"Combined data written to {csv_file}")

def main():

    chrome_options = Options()
    chrome_options.binary_location = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
    chrome_options.add_argument("--disable-gpu")
    service = Service('chromedriver.exe')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        solve_captcha_if_present(driver)
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