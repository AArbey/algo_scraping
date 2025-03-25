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
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Démarrer Xvfb sur l'écran virtuel :99 (ou un autre numéro)
os.system("Xvfb :99 -screen 0 1920x1080x24 &")
os.environ["DISPLAY"] = ":99"

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
    "delivery_fees": "sue-text-green-dark fw-500 text-uppercase ng-star-inserted",
    "delivery_date": "date ng-star-inserted",
    "product_state": "mb-0.state-text.fw-500.ng-tns",
    "more_offers_link": "//button[@class='btn btn-secondary btn-tran' and contains(text(), 'Comparer') or contains(text(), 'Consulter')]"
}

def accept_condition(driver):
    driver.get(URL)
    time.sleep(5)
    try:
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"]))
        ).click()
        print("Conditions acceptées.")
    except TimeoutException:
        print("Aucun bouton d'acceptation des conditions détecté.")
    except Exception as e:
        print(f"Erreur lors de l'acceptation des conditions : {e}")

def close_popup_if_present(driver):
    try:
        popup_close_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "ab_widget_container_popin-image_close_button"))
        )
        popup_close_button.click()
    except Exception:
        print("No popup present.")

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
            EC.visibility_of_element_located((By.XPATH, HTML_SELECTORS["more_offers_link"]))
        )
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(1)
        more_offers_button.click()
        time.sleep(2)
        return driver.current_url
    except TimeoutException:
        try:
            alternative_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "choice-box-tab d-flex align-items-center justify-content-center align-self-stretch choice-box-market border-top-right ng-star-inserted"))
            )
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(1)
            alternative_button.click()
            time.sleep(2)
            more_offers_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, HTML_SELECTORS["more_offers_link"]))
            )
            more_offers_button.click()
            time.sleep(2)
            return driver.current_url
        except Exception as e:
            print(f"Erreur lors de la tentative de clic sur l'alternative: {e}")
            return None
    except Exception as e:
        print(f"Erreur lors du clic sur 'Consulter': {e}")
        return None

def fetch_data_from_pages(driver, url, data_type):
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
                sellers = soup.find_all('a', class_=HTML_SELECTORS["seller"])
                delivery_fees = soup.find_all('span', class_=HTML_SELECTORS["delivery_fees"])
                delivery_dates = soup.find_all('span', class_=HTML_SELECTORS["delivery_date"])
                
                product_state_elements = driver.find_elements(
                    By.XPATH,
                    "//app-product-detail-offers//app-product-offer-list-item//div/div[1]/p"
                )
                product_states = []
                for elem in product_state_elements:
                    state_text = elem.text.strip()
                    if "NEUF" in state_text:
                        product_states.append("NEUF")
                    elif state_text.startswith("OCCASION -"):
                        product_states.append(state_text)

                for i in range(len(sellers)):
                    product_state_text = product_states[i] if i < len(product_states) else ""
                    print(f"Product State: {product_state_text}")
                    fetched_data.append({
                        "seller": sellers[i].get_text(strip=True) if i < len(sellers) else "",
                        "delivery_fees": delivery_fees[i].get_text(strip=True) if i < len(delivery_fees) else "",
                        "delivery_date": delivery_dates[i].get_text(strip=True) if i < len(delivery_dates) else "",
                        "product_state": product_state_text
                    })

            elif data_type == 'prices':
                prices = soup.find_all('div', class_=HTML_SELECTORS["price"])
                currencies = soup.find_all('span', class_=HTML_SELECTORS["currency"])
                cents = soup.find_all('span', class_=HTML_SELECTORS["cents"])

                fetched_data = [
                    f"{prices[i].get_text(strip=True)}.{cents[i].get_text(strip=True)} {currencies[i].get_text(strip=True)}"
                    for i in range(min(len(prices), len(currencies), len(cents)))
                ]

            time.sleep(5)
        except Exception as e:
            print(f"Erreur lors de la récupération des {data_type}: {e}")
            break

    return fetched_data


def write_combined_data_to_csv(data, sellers_data, prices, csv_file = "/home/scraping/algo-scraping/scraping_leclerc.csv"):
    if not data:
        print("Aucune donnée de produit à écrire.")
        return
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        if not file_exists:
            writer.writerow(["Platform", "Product Name", "Price", "Seller", "Delivery Fees", "Delivery Date", "Product State", "Timestamp"])

        if sellers_data and prices:
            min_length = min(len(sellers_data), len(prices))
            for i in range(min_length):
                writer.writerow([
                    data["Platform"], data["name"], prices[i], sellers_data[i]["seller"],
                    sellers_data[i]["delivery_fees"], sellers_data[i]["delivery_date"],
                    sellers_data[i].get("product_state", ""), datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                ])
        writer.writerow(["----------------------------------------------------------------------------------------------------------"])

def main():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")  # Empêcher les erreurs liées au GPU
    chrome_options.add_argument("--window-size=1920,1080")  # Simuler un affichage normal
    chrome_options.add_argument("--user-data-dir=/tmp/chrome_user_data_vm")
    chrome_options.binary_location = '/usr/bin/google-chrome'
    service = Service('/usr/local/bin/chromedriver-linux64/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)

    product_codes = ['0195949823763', '0195949806384', '0195949771774', '0195949773860','0195949722264',
                     '0195949036064', '0195949042539', '0195949041631', '0195949040733', '0195949020735',
                     '0195949049699']    
    

    try:
        accept_condition(driver)
        close_popup_if_present(driver)

        for product_code in product_codes:
            search_product(driver, product_code)

            product_url = get_product_url(driver)
            if product_url:
                product_data = scrape_product(driver, product_url)
                if product_data:
                    more_offers = click_more_offers(driver)
                    sellers_data, prices = [], []
                    if more_offers:
                        sellers_data = fetch_data_from_pages(driver, more_offers, 'sellers')
                        prices = fetch_data_from_pages(driver, more_offers, 'prices')
                        write_combined_data_to_csv(product_data, sellers_data, prices)
                    else:
                        print("non")
            else:
                print("non")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
