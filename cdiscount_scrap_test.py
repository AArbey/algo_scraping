import requests
import csv
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
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
    "more_offers_link": ["offres neuves", "offres d'occasion"],
    "seller_name": "slrName",
    "seller_status": "u-ml-sm",
    "seller_rating": "c-stars-rating__note",
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
        solve_captcha_if_present(driver)
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
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "p")))

        soup = BeautifulSoup(driver.page_source, 'lxml')

        product_name_element = soup.find('div', class_=HTML_SELECTORS["first_product_name"])
        product_name = product_name_element.get_text(strip=True) if product_name_element else "N/A"

        product_price_element = soup.find('span', class_=HTML_SELECTORS["first_product_price"])
        product_price = product_price_element.get_text(strip=True) if product_price_element else "N/A"

        product_seller_element = soup.select_one(HTML_SELECTORS["first_product_seller"])
        product_seller = product_seller_element.get_text(strip=True) if product_seller_element else "N/A"
        return {"Platform": "Cdiscount", "name": product_name, "price": product_price, "seller": product_seller, "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
    except Exception as e:
        print(f"Error scraping product details: {e}")
        return None

def get_more_offers_page(driver):
    print("------------------get_more_offers_page--------------------")
    try:
        more_offers_link = None
        for offer_text in HTML_SELECTORS["more_offers_link"]:
            try:
                more_offers_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, offer_text))
                )
                if more_offers_link:
                    break
            except TimeoutException:
                continue
        if more_offers_link:
            driver.execute_script("arguments[0].scrollIntoView(true);", more_offers_link)
            more_offers_link.click()
            time.sleep(5)
            return driver.current_url
        else:
            print("No more offers link found.")
            return None
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
                delivery_fee = soup.find_all('span', class_=HTML_SELECTORS["delivery_fee"])
                ratings_elements = soup.find_all('span', class_=HTML_SELECTORS["seller_rating"])

                print("Sellers Found:", [seller.get_text(strip=True) for seller in sellers])
                print("Seller Statuses Found:", [status.get_text(strip=True) for status in seller_statuses])
                
                seller_ratings = [rating.get_text(strip=True) for rating in ratings_elements]
                print("Seller Ratings Found:", seller_ratings)
                print("Delivery Fees Found:", [fee.get_text(strip=True) for fee in delivery_fee])

                for i in range(len(sellers)):
                    seller_name = sellers[i].get_text(strip=True)
                    seller_status = seller_statuses[i].get_text(strip=True) if i < len(seller_statuses) else "N/A"
                    seller_rating = seller_ratings[i] if i < len(seller_ratings) else "N/A"
                    delivery_fee_text = delivery_fee[i].get_text(strip=True) if i < len(delivery_fee) else "N/A"
                    fetched_data.append((seller_name, seller_status, seller_rating, delivery_fee_text))
                
                print("Fetched Sellers Data:", fetched_data)

            else:
                delivery_fee = soup.find_all('span', class_=HTML_SELECTORS["delivery_fee"])
                elements = soup.find_all('p', class_=HTML_SELECTORS[html_selector])
                fetched_data.extend([elem.get_text(strip=True) for elem in elements])
                print("Fetched Prices Data:", fetched_data)

            next_page = soup.find('a', class_='next')
            if next_page and 'href' in next_page.attrs:
                url = next_page['href']
            else:
                break
            time.sleep(5)
        except Exception as e:
            print(f"Error fetching {data_type}: {e}")
            break

    return fetched_data

def write_combined_data_to_csv(sellers, prices, product_data, csv_file="D:\scraping_data.csv", write_product_details=True):
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

        if write_product_details:
            writer.writerow(["Platform", "Product Name", "Price", "Seller", "Timestamp"])
            writer.writerow([product_data["Platform"], product_data["name"], product_data["price"], "Cdiscount", product_data["timestamp"]])
            print("Product details (without additional offers) written to CSV.")

        if sellers and prices:
            min_length = min(len(sellers), len(prices))
            writer.writerow(["Platform", "Product Name", "Price", "Seller", "Seller Status", "Seller Rating", "Delivery Fee", "Timestamp"])
            for i in range(min_length):
                seller_data = sellers[i]
                writer.writerow(["Cdiscount", product_data["name"], prices[i], seller_data[0], seller_data[1], seller_data[2], seller_data[3], datetime.now().strftime("%d/%m/%Y %H:%M:%S")])
        writer.writerow(["----------------------------------------------------------------------------------------------------------"])

    print(f"Data written to {csv_file}")

def main():

    chrome_options = Options()
    chrome_options.binary_location = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
    service = Service('C:\\Users\\nsoulie\\Downloads\\chromedriver-win64 (1)\\chromedriver-win64\chromedriver.exe')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    products_to_search = ['ip16512black', 'ip16256black', 'ip16p512black',
                          'ip16p256black', 'ip16p128black', 'ip16pro1tbblack',
                          'ip16prom1tbbla', 'ip15512black', 'ip15128black']

    try:
        accept_condition(driver)

        for product_to_search in products_to_search:
            search_product(driver, product_to_search)
            product_url = get_first_product_url(driver)
            if product_url:
                product_data = scrape_product_details(driver, product_url)
                other_offers_url = get_more_offers_page(driver)

                if other_offers_url:
                    print("More offers found, scraping second page offers only.")
                    sellers = fetch_data_from_pages(driver, other_offers_url, 'seller_name', 'sellers')
                    prices = fetch_data_from_pages(driver, other_offers_url, 'get_price', 'prices')
                    write_combined_data_to_csv(sellers, prices, product_data, write_product_details=False)
                else:
                    print(f"No additional offers found for {product_to_search}. Writing product details only.")
                    write_combined_data_to_csv([], [], product_data, write_product_details=True)
            else:
                print(f"Product not found for {product_to_search}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
