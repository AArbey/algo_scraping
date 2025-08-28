from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import logging
import os
import pwd

def drop_privileges():
    if os.getuid() != 0:
        return  # Déjà exécuté en tant qu'utilisateur normal
    
    # Identifiants de l'utilisateur scraping
    user_name = "scraping"
    user_info = pwd.getpwnam(user_name)
    user_uid = user_info.pw_uid
    user_gid = user_info.pw_gid
    
    # Définir le groupe
    os.setgid(user_gid)
    # Définir l'utilisateur
    os.setuid(user_uid)
    # Définir le HOME
    os.environ['HOME'] = f'/home/{user_name}'

# Changer d'utilisateur
drop_privileges()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("leclerc_scraper.log"),
        logging.StreamHandler()
    ]
)

# Nouveau délimiteur CSV
CSV_DELIMITER = ';'  # Utilisation du point-virgule comme séparateur CSV

HTML_SELECTORS = {
    "Product Name": "h1[class^='cbBiP clamp']",
    "Price": ".vcEUR",
    "Cents": ".bYgjT",
    "Currency": ".fwwLV",
    "Seller": "a[class^='other-offer__seller-name link-primary']",
    "Delivery Fees": ".vEteb",
    "Delivery Date": ".ebbbH",
    "Product State": "span[class^='fWkBF']",
    "Seller Rating": ".etxDh",
}

def fetch_html(url, html="page_content.html"):
    try:
        logging.info(f"Fetching HTML for URL: {url}")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.binary_location = '/usr/bin/chromium-browser'

        driver = webdriver.Chrome(options=options)
        driver.set_window_size(1920, 1080)
        driver.get(url)
        
        # More robust element finding
        try:
            check_input = driver.find_element(By.CLASS_NAME, "p.buybox-header--title")
            check_input.click()
            time.sleep(3)
        except Exception as e:
            logging.warning(f"Could not find/click check input: {str(e)}")
        
        html_content = driver.page_source
        with open(html, 'w', encoding='utf-8') as file:
            file.write(html_content)
        logging.info(f"HTML content saved for URL: {url}")

        driver.quit()
        return html_content
    except Exception as e:
        logging.error(f"Error in fetch_html: {str(e)}")
        raise

def get_sellers(soup):
    try:
        logging.info("Extracting sellers")
        # Get all marketplace sellers
        marketplace_sellers = soup.select(HTML_SELECTORS["Seller"])
        marketplace_names = [seller.get_text(strip=True) for seller in marketplace_sellers]
        
        # Get E.Leclerc seller separately
        leclerc_seller = soup.select_one(".shop-infos.fw-500.ng-tns-c183-2.ng-star-inserted")
        leclerc_name = leclerc_seller.get_text(strip=True).replace("Vendeur : ", "") if leclerc_seller else "E.Leclerc"
        
        # Combine lists (Leclerc first, then marketplace)
        sellers = [leclerc_name] + marketplace_names
        
        logging.info(f"Found {len(sellers)} sellers: {sellers}")
        return sellers
    except Exception as e:
        logging.error(f"Error in get_sellers: {str(e)}")
        return ["E.Leclerc"]

def get_prices(soup):
    try:
        logging.info("Extracting prices")
        prices = soup.select(HTML_SELECTORS["Price"])
        cents = soup.select(HTML_SELECTORS["Cents"])
        currencies = soup.select(HTML_SELECTORS["Currency"])

        product_prices = []
        for i in range(len(prices)):
            price = prices[i].get_text(strip=True) if i < len(prices) else ""
            cent = cents[i].get_text(strip=True) if i < len(cents) else "00"
            currency = currencies[i].get_text(strip=True) if i < len(currencies) else "€"
            product_prices.append(f"{price}.{cent} {currency}".strip())
        
        logging.info(f"Found {len(product_prices)} prices: {product_prices}")
        return product_prices
    except Exception as e:
        logging.error(f"Error in get_prices: {str(e)}")
        return []

def get_product_states(soup):
    try:
        logging.info("Extracting product states")
        states = soup.select(HTML_SELECTORS["Product State"])
        product_states = [state.get_text(strip=True) for state in states]
        
        # Handle case where Leclerc state might be missing
        if len(product_states) < len(soup.select(".offer-block")):
            product_states = ["Neuf"] + product_states
            
        logging.info(f"Found {len(product_states)} states: {product_states}")
        return product_states
    except Exception as e:
        logging.error(f"Error in get_product_states: {str(e)}")
        return ["Neuf"]

def extract_product_details(soup, sellers, prices, product_states):
    try:
        logging.info("Extracting product details")
        num_offers = min(len(sellers), len(prices), len(product_states))
        products = []
        
        for i in range(num_offers):
            product_details = {
                "Product Name": soup.select_one(HTML_SELECTORS["Product Name"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Product Name"]) else "Non trouvé",
                "Price": prices[i] if i < len(prices) else "Non trouvé",
                "Seller": sellers[i] if i < len(sellers) else "E.Leclerc",
                "Product State": product_states[i] if i < len(product_states) else "Neuf",
                "Delivery Fees": soup.select_one(HTML_SELECTORS["Delivery Fees"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Delivery Fees"]) else "Non trouvé",
                "Delivery Date": soup.select_one(HTML_SELECTORS["Delivery Date"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Delivery Date"]) else "Non trouvé",
                "Seller Rating": soup.select_one(HTML_SELECTORS["Seller Rating"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Seller Rating"]) else "Non trouvé",
                "Platform": "E.Leclerc",
                "Timestamp": datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            }
            products.append(product_details)
            
            # Log each product detail
            logging.debug(f"Product {i+1}: {product_details}")
        
        logging.info(f"Extracted {len(products)} product offers")
        return products
    except Exception as e:
        logging.error(f"Error in extract_product_details: {str(e)}")
        return []

def extract_info(soup):
    try:
        logging.info("Starting info extraction")
        sellers = get_sellers(soup)
        prices = get_prices(soup)
        product_states = get_product_states(soup)
        
        # Log list lengths for alignment check
        logging.info(f"Alignment check: Sellers({len(sellers)}) | Prices({len(prices)}) | States({len(product_states)})")
        
        # Log first 3 elements of each list for comparison
        logging.debug(f"Sellers sample: {sellers[:3]}")
        logging.debug(f"Prices sample: {prices[:3]}")
        logging.debug(f"States sample: {product_states[:3]}")
        
        products = extract_product_details(soup, sellers, prices, product_states)
        return products
    except Exception as e:
        logging.error(f"Error in extract_info: {str(e)}")
        return []

def get_initial_batch_id(csv_file='product_details.csv'):
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if not lines:
                return 0
            last_line = lines[-1].rstrip('\n')
            parts = [p.strip() for p in last_line.split(CSV_DELIMITER)]
            if parts and parts[-1].isdigit():
                logging.info(f"Last batch_id found in CSV: {parts[-1]}")
                return int(parts[-1]) + 1
    except FileNotFoundError:
        logging.info("CSV file not found, starting with batch_id 0")
        return 0
    except Exception:
        return 0
    return 0

# Initialise batch_id au démarrage
batch_id = get_initial_batch_id()

def write_to_csv(products):
    try:
        logging.info("Writing to CSV")
        with open('scraping_leclerc.csv', "a", newline="", encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=CSV_DELIMITER)
            if file.tell() == 0:
                writer.writerow([
                    "Platform", "Product Name", "Seller", "Price",
                    "Delivery Fees", "Delivery Date", "Product State",
                    "Seller Rating", "Timestamp", "Batch ID"
                ])
            for product in products:
                writer.writerow([
                    product.get("Platform", ''),
                    product.get('Product Name', ''),
                    product.get('Seller', ''),
                    product.get('Price', ''),
                    product.get('Delivery Fees', ''),
                    product.get('Delivery Date', ''),
                    product.get('Product State', ''),
                    product.get('Seller Rating', ''),
                    product.get('Timestamp', ''),
                    batch_id
                ])
        logging.info(f"Wrote {len(products)} products to CSV with delimiter '{CSV_DELIMITER}'")
    except Exception as e:
        logging.error(f"Error writing to CSV: {str(e)}")

def main():
    urls = [
        "https://www.e.leclerc/of/apple-iphone-16-15-5-cm-6-1-double-sim-ios-18-5g-usb-type-c-512-go-noir-0195949823763",
        "https://www.e.leclerc/of/apple-iphone-16-15-5-cm-6-1-double-sim-ios-18-5g-usb-type-c-256-go-noir-0195949822865",
        "https://www.e.leclerc/fp/apple-iphone-16-15-5-cm-6-1-double-sim-ios-18-5g-usb-type-c-128-go-noir-0195949821967",
        "https://www.e.leclerc/of/apple-iphone-16-plus-17-cm-6-7-double-sim-ios-18-5g-usb-type-c-512-go-noir-0195949724169",
        "https://www.e.leclerc/of/apple-iphone-16-plus-17-cm-6-7-double-sim-ios-18-5g-usb-type-c-256-go-noir-0195949723216",
        "https://www.e.leclerc/of/apple-iphone-16-plus-17-cm-6-7-double-sim-ios-18-5g-usb-type-c-128-go-noir-0195949722264",
        "https://www.e.leclerc/fp/apple-iphone-16-pro-16-cm-6-3-double-sim-ios-18-5g-usb-type-c-128-go-noir-0195949771200",
        "https://www.e.leclerc/fp/apple-iphone-16-pro-16-cm-6-3-double-sim-ios-18-5g-usb-type-c-256-go-noir-0195949771965",
        "https://www.e.leclerc/fp/apple-iphone-16-pro-16-cm-6-3-double-sim-ios-18-5g-usb-type-c-512-go-noir-0195949772726",
        "https://www.e.leclerc/of/apple-iphone-16-pro-16-cm-6-3-double-sim-ios-18-5g-usb-type-c-1-to-noir-0195949773488",
        "https://www.e.leclerc/fp/apple-iphone-16-pro-max-17-5-cm-6-9-double-sim-ios-18-5g-usb-type-c-256-go-noir-0195949805813",
        "https://www.e.leclerc/fp/apple-iphone-16-pro-max-17-5-cm-6-9-double-sim-ios-18-5g-usb-type-c-512-go-noir-0195949806575",
        "https://www.e.leclerc/fp/apple-iphone-16-pro-max-17-5-cm-6-9-double-sim-ios-18-5g-usb-type-c-1-to-noir-0195949807336",
        "https://www.e.leclerc/fp/apple-iphone-15-15-5-cm-6-1-double-sim-ios-17-5g-usb-type-c-512-go-noir-0195949037795?offer_id=72931002",
        "https://www.e.leclerc/of/smartphone-apple-iphone-15-256gb-noir-0195949036965",
        "https://www.e.leclerc/of/smartphone-apple-iphone-15-128gb-noir-0195949036064",
        "https://www.e.leclerc/fp/smartphone-apple-iphone-15-pro-128gb-noir-titanium-0195949018572",
        "https://www.e.leclerc/fp/smartphone-apple-iphone-15-pro-max-256gb-noir-titanium-0195949048258",
        "https://www.e.leclerc/of/apple-iphone-14-15-5-cm-6-1-double-sim-ios-17-5g-512-go-noir-0194253411550",
        "https://www.e.leclerc/of/smartphone-apple-iphone-14-256go-noir-midnight-0194253409908",
        "https://www.e.leclerc/fp/apple-iphone-14-15-5-cm-6-1-double-sim-ios-17-5g-128-go-noir-0194253408253",
        "https://www.e.leclerc/fp/smartphone-apple-iphone-14-plus-128go-noir-midnight-0194253373476",
        "https://www.e.leclerc/fp/apple-iphone-14-plus-17-cm-6-7-double-sim-ios-17-5g-256-go-noir-0194253374626",
        "https://www.e.leclerc/fp/apple-iphone-14-plus-17-cm-6-7-double-sim-ios-17-5g-512-go-noir-0194253375777",
        "https://www.e.leclerc/fp/apple-iphone-14-pro-15-5-cm-6-1-double-sim-ios-16-5g-256-go-noir-0194253402497",
        "https://www.e.leclerc/fp/apple-iphone-14-pro-max-17-cm-6-7-double-sim-ios-17-5g-128-go-noir-0194253379942",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-15-8-cm-6-2-double-sim-android-15-5g-usb-type-c-12-go-128-go-4000-mah-argent-8806095851792",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-15-8-cm-6-2-double-sim-android-15-5g-usb-type-c-12-go-256-go-4000-mah-argent-8806095851488",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-15-8-cm-6-2-double-sim-android-15-5g-usb-type-c-12-go-512-go-4000-mah-argent-8806095851433",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-17-cm-6-7-double-sim-android-15-5g-usb-type-c-12-go-512-go-4900-mah-argent-8806095857602",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-ultra-17-5-cm-6-9-double-sim-android-15-5g-usb-type-c-12-go-256-go-5000-mah-gris-titane-8806095860237",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-ultra-17-5-cm-6-9-double-sim-android-15-5g-usb-type-c-12-go-512-go-5000-mah-gris-titane-8806095859934",
        "https://www.e.leclerc/fp/samsung-galaxy-s25-ultra-17-5-cm-6-9-double-sim-android-15-5g-usb-type-c-12-go-1-to-5000-mah-gris-titane-8806095859880",
        "https://www.e.leclerc/fp/samsung-galaxy-s24-15-8-cm-6-2-double-sim-android-14-5g-usb-type-c-8-go-128-go-4000-mah-gris-couleur-marbre-8806095299822",
        "https://www.e.leclerc/fp/samsung-galaxy-s24-smartphone-256gb-argent-8806095299808",
        "https://www.e.leclerc/fp/samsung-galaxy-s24-smartphone-256gb-argent-8806095306810",
        "https://www.e.leclerc/fp/samsung-galaxy-s24-ultra-smartphone-256gb-gris-8806095309446"
    ]

    for url in urls:
        try:
            logging.info(f"Processing URL: {url}")
            html_content = fetch_html(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            products = extract_info(soup)
            write_to_csv(products)
        except Exception as e:
            logging.error(f"Error processing URL {url}: {str(e)}")

def run_indefinitely(cycle_interval=600):
    global batch_id
    while True:
        try:
            logging.info(f"Starting new scraping cycle")
            main()
            batch_id += 1
            logging.info(f"Cycle completed. Next batch_id={batch_id}. Waiting {cycle_interval/60} minutes.")
        except Exception as e:
            logging.error(f"Critical error in main cycle: {str(e)}")
        time.sleep(cycle_interval)

if __name__ == "__main__":
    run_indefinitely()