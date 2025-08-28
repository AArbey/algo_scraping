import csv
import os, sys
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
import os
import pwd
import random
import atexit
import signal

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

# Configuration
URL = "https://www.carrefour.fr/"
HTML_SELECTORS = {
    "accept_condition": "onetrust-accept-btn-handler",
    "search_bar": "header-search-bar",
    "product": "c-text.c-text--size-m.c-text--style-p.c-text--bold.c-text--spacing-default.product-card-title__text.product-card-title__text--hoverable",
    "name": "product-title__title c-text c-text--size-m c-text--style-h3 c-text--spacing-default",
    "price": "product-price__content c-text c-text--size-m c-text--style-subtitle c-text--bold c-text--spacing-default",
    "cents": "product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "seller_main": "delivery-choice__title",
    "delivery_popup_button": "c-button__loader__container",
    "delivery_popup_text": "delivery-method-description__delay",
    "seller": "c-link c-link--size-s c-link--tone-main",
    "delivery_info": "delivery-infos__time c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default",
    "more_offers_button": "//button[contains(text(), 'offres')]",
    "side_panel": "c-modal__container c-modal__container--position-right",
}



def start_xvfb():
    xvfb_process = subprocess.Popen(
        ["Xvfb", ":103", "-screen", "0", "1920x1080x24"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    os.environ["DISPLAY"] = ":103"
    return xvfb_process

lockfile = "/tmp/scraping_carrefour.lock"

# register cleanup to always remove the lockfile
def remove_lockfile():
    try:
        if os.path.exists(lockfile):
            os.remove(lockfile)
            print("Lockfile removed.")
    except Exception as e:
        print(f"Error removing lockfile: {e}")

def signal_handler(signum, frame):
    remove_lockfile()
    sys.exit(0)

atexit.register(remove_lockfile)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if os.path.exists(lockfile):
    print("Script déjà en cours. Abandon.")
    sys.exit()

# Créer le fichier lock
with open(lockfile, 'w') as f:
    f.write(str(os.getpid()))


def accept_condition(driver):
    driver.get(URL)
    try:
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"]))
        ).click()
    except TimeoutException:
        print("L'élément d'acceptation conditions n'a pas été trouvé. Peut-être que ce popup ne s'affiche pas toujours.")
    except Exception as e:
        print(f"Erreur accept condition : {e}")

def close_all_modals(driver):
    try:
        time.sleep(5)
        modals = driver.find_elements(By.CSS_SELECTOR, "dialog[open].c-modal, div.c-modal[open]")
        for modal in modals:
            try:
                close_btn = modal.find_element(By.CSS_SELECTOR, "[aria-label='Fermer'], button")
                driver.execute_script("arguments[0].click();", close_btn)
                print("Popup fermée.")
                time.sleep(1)
            except Exception as e:
                print(f"Impossible de fermer une popup : {e}")

        # Close promotional popups using multiple possible selectors
        popup_selectors = ["button.by_close.by-js-close", ".by_popin_overlay.by-js-close"]
        for sel in popup_selectors:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in elems:
                try:
                    driver.execute_script("arguments[0].click();", el)
                    print(f"Popup promotionnelle fermée ({sel}).")
                    time.sleep(1)
                except Exception as e:
                    print(f"Impossible de cliquer sur popup promotionnelle ({sel}): {e}")



    except Exception as e:
        print(f"Erreur en cherchant les modals : {e}")



def search_product(driver, search_query, retry=True):
    try:
        close_all_modals(driver)
        print("Waiting for search bar to be clickable...")
        search_bar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, HTML_SELECTORS["search_bar"]))
        )
        print("Search bar found, entering search query...")
        search_bar.click()
        search_bar.send_keys(search_query)
        print("Search query entered, submitting...")
        search_bar.send_keys(Keys.RETURN)
    except Exception as e:
        if retry:
            print("Trying again to search")
            time.sleep(2)
            search_product(driver, search_query, retry=False)  # Réessayer la recherche
        print(f"Erreur recherche : {e}")




def get_product_url(driver):
    try:
        close_all_modals(driver)
        try:
            overlay = driver.find_element(By.CSS_SELECTOR, ".by_popin_overlay.by-js-close")
            driver.execute_script("arguments[0].click();", overlay)
            time.sleep(1)
        except Exception:
            pass
        # 3) Récupérer le lien produit et cliquer via JS
        product_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, HTML_SELECTORS["product"]))
        )
        
        driver.execute_script("arguments[0].scrollIntoView(true);", product_link)
        driver.execute_script("arguments[0].click();", product_link)
        time.sleep(2)
        current_url = driver.current_url
        if "https://www.carrefour.fr/" in current_url:
            return current_url
        else:
            print("URL anormale ou non chargée.")
            return None
    except Exception as e:
        print(f"Erreur lors de la récupération du produit: {e}")
        driver.get(URL)  # Revenir à la page d'accueil
        close_all_modals(driver)
        return None

def scrape_product(driver, product_url):
    try:
        print("Loading product page...")
        if not product_url:
            raise ValueError("URL vide")

        driver.get(product_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "product-title__title"))
        )
        soup = BeautifulSoup(driver.page_source, 'lxml')

        name_elem = soup.find('h1', class_=HTML_SELECTORS["name"])
        if not name_elem:
            print("Nom produit introuvable")
            return None
        print("Product name found.")
        seller_elem = soup.find('div', class_=HTML_SELECTORS["seller_main"])
        euros_elem = soup.find('p', class_=HTML_SELECTORS["price"])
        cents_elem = soup.find('p', class_=HTML_SELECTORS["cents"])

        euros = euros_elem.get_text(strip=True).replace("€", "") if euros_elem else "0"
        cents = cents_elem.get_text(strip=True).replace("€", "") if cents_elem else "00"
        full_price = f"{euros}{cents}€"

        
        livr_panel = soup.find('div', class_="non-food-delivery-modalities-modal__wrap")
        if livr_panel:livr_panel = livr_panel.find('div', class_="c-text c-text--size-m c-text--style-p c-text--spacing-default")

        if livr_panel:
            li_elem = livr_panel.find("li")  
            if li_elem:
                lignes = li_elem.get_text(separator="\n", strip=True).split("\n")
                for ligne in lignes:
                    if ligne.startswith("Frais de livraison"):
                        delivery_info=ligne
        else:
            delivery_info ="Non spécifié"
        link = seller_elem.find('a', class_="c-link c-link--size-s c-link--tone-main")
        if link:
            vendeur=link.get_text(strip=True)
        else:
            vendeur=seller_elem.get_text(strip=True) if seller_elem else "Non spécifié"

        seller_ratings = soup.find('span', class_="rating-stars__slot c-text c-text--size-m c-text--style-p c-text--spacing-default")


        main_offer = {
            "seller": vendeur,
            "price": full_price,
            "delivery_info": delivery_info,
            "seller_rating": seller_ratings.get_text(strip=True) if seller_ratings.get_text(strip=True) else "Non spécifié"
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
        print("Attempting to click 'More Offers' button...")
        time.sleep(2)
        more_offers_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, HTML_SELECTORS["more_offers_button"]))
        )
        driver.execute_script("arguments[0].click();", more_offers_button)
        print("Successfully clicked 'More Offers' button.")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, HTML_SELECTORS["side_panel"]))
        )
        return driver.current_url
    except Exception as e:
        print(f"Erreur bouton offres : {e}")
    return None


def fetch_data_from_side_panel(driver, main_data):
    try:
        print("Scraping data from side panel...")
        full_soup = BeautifulSoup(driver.page_source, 'lxml')
        side_panel = full_soup.find('div', class_=HTML_SELECTORS["side_panel"])
        if not side_panel:
            return []

        sellers = side_panel.find_all('a', class_=HTML_SELECTORS["seller"])
        delivery_infos = side_panel.find_all('p', class_=HTML_SELECTORS["delivery_info"])
        
        seller_ratings = side_panel.find_all('span', class_="rating-stars__slot c-text c-text--size-m c-text--style-p c-text--spacing-default")
        product_blocks = side_panel.find_all("div", class_="product-price__amounts")
        data = []
        for i, block in enumerate(product_blocks):
            main_price_block = block.find("div", class_="product-price__amount product-price__amount--main")
            if not main_price_block:
                print("Pas de bloc prix principal")
                continue

            euros_elem = main_price_block.find("p", class_="product-price__content c-text c-text--size-m c-text--style-h4 c-text--bold c-text--spacing-default")
            
            # On récupère tous les p en size-s, pour avoir les centimes et ignorer le "€"
            size_s_elems = main_price_block.find_all("p", class_="product-price__content c-text c-text--size-s c-text--style-p c-text--bold c-text--spacing-default")
            
            # On garde le premier avec une virgule pour les centimes (ça évite d’avoir "€")
            centimes = ",00"
            for elem in size_s_elems:
                txt = elem.get_text(strip=True)
                if "," in txt:
                    centimes = txt
                    break

            euros = euros_elem.get_text(strip=True).replace("€", "") if euros_elem else ""
            price=f"{euros}{centimes}€"

            print("EUROS:", euros)
            print("CENTIMES:", centimes)
            print("PRIX:", price)
            vendeur=sellers[i].get_text(strip=True)
            if vendeur not in main_data['seller']:
                
                data.append({
                    "seller": sellers[i].get_text(strip=True),
                    "delivery_info": delivery_infos[i].get_text(strip=True) if i < len(delivery_infos) else "Non spécifié",
                    "price": price,
                    "seller_rating": seller_ratings[i].get_text(strip=True) if i < len(seller_ratings) else "Non spécifié"
                })
                print("data ajoutée")

        return data
    except Exception as e:
        print(f"Erreur scraping panel : {e}")
        return []


def write_combined_data_to_csv(data, sellers_data, batch_id, csv_file="/home/scraping/algo_scraping/CARREFOUR/scraping_carrefour.csv"):
    if not data:
        print("Aucune donnée de produit à écrire.")
        return
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, delimiter=';')
        if not file_exists:
            writer.writerow([
                "Platform", "Product Name", "Seller", "Delivery Info",
                "Price", "Seller Rating", "Timestamp", "Batch ID"
            ])
        for seller in sellers_data:
            writer.writerow([
                data["Platform"], data["name"], seller["seller"],
                seller["delivery_info"], seller["price"],
                seller["seller_rating"], data["timestamp"], batch_id
            ])
        #writer.writerow(["-" * 100])


def main():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.binary_location = '/usr/bin/chromium-browser'
    #chrome_options.add_argument("--user-data-dir=/tmp/chrome_user_data_vm_carrefour")
    driver = webdriver.Chrome(options=chrome_options)

    #product_ids = ['0195949822865', '0195949821899', '0195949724169','0195949723216', '0195949722264', '0195949773488', 
    #               '0195949807336', '0195949037863', '0195949036965', '0195949036064', '0195949042539', '0195949041631', 
    #               '0195949040733', '0195949020735', '019594904969']

    product_ids = ['0195949823763','0195949822865','0195949821967',
'0195949724169','0195949723216','0195949722264','0195949773488',
'0195949807336','0195949037863','0195949036965','0195949036064',
'0195949042539','0195949041631','0195949040733','0195949020735',
'0195949049699','0194253408253','0194253409809','0194253411550',
'0194253373476','0194253374626','0194253375777','0194253401179',
'0194253403814','0194253405139','0194253379942','0194253381686',
'8806095851792','8806095851488','8806095857725','8806095857602',
'8806095860107','8806095860237','8806095859934','8806095851433',
'8806095859910','8806095859743','8806095859880','8806095299785',
'8806095306797','8806095307633','8806095414485','8806095414478'
]

    accept_condition(driver)
    sellers_data=[]

    

    for product_id in product_ids:
        try:
            # Waiting a random time before each product to avoid being blocked
            time.sleep(random.uniform(2, 5))

            print(f"Scraping ID: {product_id}")
            search_product(driver, product_id)
            product_url = get_product_url(driver)
            print(f"➡️ URL récupérée : {product_url}")
            data = scrape_product(driver, product_url)
            
            if data:
                #click_more_offers(driver)
                #plus besoin de click_more_offers car on cherche direct dans le side panel même s'il est pas ouvert
                #mais je garde la fonction au cas où
                sellers_data = [data["main_offer"]] + fetch_data_from_side_panel(driver, data["main_offer"])
                write_combined_data_to_csv(data, sellers_data, batch_id)
        except Exception as e:
            print(f"Erreur produit {product_id} : {e}")

    driver.quit()


if __name__ == "__main__":
    xvfb = start_xvfb()
    try:
        csv_file = "/home/scraping/algo_scraping/CARREFOUR/scraping_carrefour.csv"
        batch_id = 0
        if os.path.exists(csv_file):
            try:
                with open(csv_file, newline="") as f:
                    reader = csv.reader(f, delimiter=';')
                    last_row = None
                    for last_row in reader:
                        pass
                if last_row and len(last_row) > 0:
                    last_col = last_row[-1]
                    if last_col.isdigit():
                        batch_id = int(last_col) + 1
            except Exception:
                batch_id = 0


        while True:
            main()
            print("Attente avant la prochaine exécution dans 20 min…")
            batch_id += 1
            time.sleep(1200)
    finally:
        xvfb.terminate()
        xvfb.wait()
        if os.path.exists(lockfile):
            os.remove(lockfile)
