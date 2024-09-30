import time
import csv
import logging
import random
from datetime import datetime
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# CONSTANTS

CHROME_DATA_DIR = "C:/Users/thoma/AppData/Local/Google/Chrome/User Data/Default"
SCRAPE_INTERVAL = 60 # En secondes

URL = "https://www.darty.com/nav/extra/offres?codic=7663854"

CSV_FILE = "darty_offers.csv"

HTML_SELECTORS = {
    "seller": ".mkp_choicebox_seller__text",
    "price": ".product-price__price.price_ir",
    "rating": ".grade",
    "product_state": ".product_state",
    "delivery_date": ".promise-text.promise-date",
    "darty_seller": "p.mkp_choicebox_seller > b"
}

# Logger

logging.basicConfig(
    filename='log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# FUNCTIONS

def get_driver():
    """
    Initializes and returns a Chrome WebDriver instance with specific options.

    The WebDriver is configured with the following options:
    - Disables GPU usage.
    - Disables the sandbox environment.
    - Disables Blink features that are controlled by automation.
    - Starts the browser maximized.
    - Disables browser extensions.
    - Uses a specific user data directory for Chrome.
    - Sets a custom user agent string.

    Returns:
        webdriver.Chrome: An instance of Chrome WebDriver with the specified options.
    """
    options = uc.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")

    options.add_argument(f"user-data-dir={CHROME_DATA_DIR}")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    driver = uc.Chrome(options=options)
    return driver

def verify_detection(driver):
    """
    Verifies whether the WebDriver is detectable by the website.

    This function checks various browser properties that can indicate automation,
    such as navigator.webdriver, navigator.userAgent, window.chrome, navigator.plugins,
    and navigator.mimeTypes. It prints out the values of these properties for inspection.

    Args:
        driver (webdriver.Chrome): The WebDriver instance used to interact with the browser.

    Returns:
        None
    """
    logging.info("Vérification des propriétés de détection...")
    try:
        webdriver_property = driver.execute_script("return navigator.webdriver")
        logging.info(f"Valeur de navigator.webdriver : {webdriver_property}")
        
        user_agent = driver.execute_script("return navigator.userAgent")
        logging.info(f"User-Agent actuel : {user_agent}")
        
        chrome_property = driver.execute_script("return window.chrome")
        logging.info(f"Valeur de window.chrome : {chrome_property}")

        plugins = driver.execute_script("return navigator.plugins.length")
        logging.info(f"Nombre de plugins disponibles : {plugins}")

        mime_types = driver.execute_script("return navigator.mimeTypes.length")
        logging.info(f"Nombre de mime types disponibles : {mime_types}")
        
    except Exception as e:
        logging.error(f"Erreur lors de la vérification des propriétés : {e}")

def simulate_human_behavior(driver):
    """
    Simulates human-like interactions with the web page.

    This function randomly scrolls the web page up and down to mimic the behavior of
    a human user, helping to avoid detection by anti-bot mechanisms on the website.

    Args:
        driver (webdriver.Chrome): The WebDriver instance used to interact with the browser.

    Returns:
        None
    """
    logging.info("Simulation de comportement humain...")
    try:
        num_scrolls = random.randint(3, 6)
        
        for _ in range(num_scrolls):
            scroll_start = random.randint(0, 500)
            scroll_end = scroll_start + random.randint(500, 1000)
            driver.execute_script(f"window.scrollTo({scroll_start}, {scroll_end});")
            
            time.sleep(random.randint(1, 5))
            
            scroll_start = random.randint(0, 500)
            scroll_end = scroll_start - random.randint(500, 1000)
            driver.execute_script(f"window.scrollTo({scroll_start}, {scroll_end});")
            
            time.sleep(random.randint(1, 5))
        
        logging.info("Comportement humain simulé.")
    except Exception as e:
        logging.error(f"Erreur lors de la simulation de comportement humain : {e}")

def scrape_darty_product_info(url):
    """
    Scrapes product information from a given Darty product page.

    This function navigates to the specified URL, simulates human behavior, and then extracts
    information about product offers such as the seller name, price, rating, product state, 
    and delivery date. It returns a list of dictionaries, each containing the extracted information
    for a product offer.

    Args:
        url (str): The URL of the Darty product page to scrape.

    Returns:
        list of dict: A list of dictionaries, each containing information about a product offer.
    """
    driver = get_driver()
    time.sleep(10)
    driver.get(url)
    logging.info("Page chargée")

    verify_detection(driver)
    simulate_human_behavior(driver)

    try:
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "mkp_item")))

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        products = soup.select(".mkp_item")
        logging.info(f"{len(products)} offres trouvées")

        product_list = []
        request_timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        for index, product in enumerate(products):
            logging.info(f"Scraping de l'offre {index + 1}")
            try:
                darty_vendeur = product.select_one(HTML_SELECTORS["darty_seller"])
                if darty_vendeur:
                    nom_vendeur = "Darty"
                    note_vendeur = "Non applicable"
                else:
                    nom_vendeur = product.select_one(HTML_SELECTORS["seller"]).get_text(strip=True)
                    try:
                        note_vendeur = product.select_one(HTML_SELECTORS["rating"]).get_text(strip=True)
                    except:
                        note_vendeur = "Non spécifié"
            except Exception as e:
                nom_vendeur = "Non spécifié"
                note_vendeur = "Non spécifié"
                logging.error(f"Erreur de récupération du nom du vendeur: {e}")
            
            try:
                prix = product.select_one(HTML_SELECTORS["price"]).get_text(strip=True).replace('€', '').strip()
                prix = prix.replace(',', '.')
            except Exception as e:
                prix = "Non spécifié"
                logging.error(f"Erreur de récupération du prix: {e}")
            
            try:
                etat_produit = product.select_one(HTML_SELECTORS["product_state"]).get_text(strip=True).replace('Etat du produit : ', '')
            except Exception as e:
                etat_produit = "Non spécifié"
                logging.error(f"Erreur de récupération de l'état du produit: {e}")
            
            try:
                date_livraison_full = product.select_one(HTML_SELECTORS["delivery_date"]).get_text(strip=True)
                date_livraison = date_livraison_full.replace('Livrédès le ', '').strip()
            except Exception as e:
                date_livraison = "Non spécifié"
                logging.error(f"Erreur de récupération de la date de livraison: {e}")

            product_info = {
                "Nom du Vendeur": nom_vendeur,
                "Prix (€)": prix,
                "Note du Vendeur": note_vendeur,
                "État du Produit": etat_produit,
                "Date de Livraison": date_livraison,
                "Horodatage": request_timestamp
            }
            logging.info(f"Infos récupérées pour l'offre {index + 1} : Nom du Vendeur - {nom_vendeur}, État du Produit - {etat_produit}, Date de Livraison - {date_livraison}")
            product_list.append(product_info)
        
        return product_list
    except Exception as e:
        logging.error(f"Erreur lors du scraping : {e}")
        return []
    finally:
        try:
            driver.quit()
            logging.info("Navigateur fermé")
        except Exception as e:
            logging.error(f"Erreur lors de la fermeture du navigateur : {e}")

def save_to_csv(data, filename):
    """
    Saves scraped product information to a CSV file.

    This function appends the provided data to a CSV file. If the file does not exist or is empty,
    it writes a header row based on the keys of the data dictionaries.

    Args:
        data (list of dict): The list of dictionaries containing product information to save.
        filename (str): The name of the CSV file where the data will be saved.

    Returns:
        None
    """
    keys = data[0].keys() if data else []
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        if file.tell() == 0:
            writer.writeheader()
        writer.writerows(data)
    logging.info(f"Données enregistrées dans {filename}")

# MAIN

def main():
    while True:
        try:
            product_info_list = scrape_darty_product_info(URL)
            if product_info_list:
                save_to_csv(product_info_list, CSV_FILE)

            driver = get_driver()
            driver.get(URL)
            simulate_human_behavior(driver)
            time.sleep(SCRAPE_INTERVAL)
            driver.quit()

            logging.info(f"Attente de {SCRAPE_INTERVAL} secondes avant le prochain scraping...")
            time.sleep(SCRAPE_INTERVAL)

        except Exception as e:
            logging.error(f"Erreur dans le main : {e}")
            break

if __name__ == "__main__":
    main()
