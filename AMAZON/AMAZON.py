"""
Script de scraping pour Amazon
------------------------------

Ce script scrape par itération une liste de pages de produits Amazon.
Pour chaque produit (défini par un ASIN), il récupère les informations sur l'offre principale 
ainsi que les offres supplémentaires via des requêtes AJAX. Les informations extraites sont ensuite 
converties et enregistrées dans un fichier Parquet.
Les requêtes sont effectuées de manière répartie sur un intervalle de 1 heure, puis le script recommence la liste à l'infini.

Détails :
- Le script effectue une requête GET sur un URL Amazon pour récupérer les informations de l'offre principale, 
  et utilise des requêtes AJAX pour récupérer les offres supplémentaires.
- À chaque itération, les nouvelles données sont ajoutées dans un fichier Parquet ('amazon_offers.parquet').
- Les requêtes sont effectuées de manière aléatoire pour éviter le blocage, en utilisant un intervalle défini de temps entre chaque produit.
- Une fois que tous les produits de la liste sont scrappés, le script attend quelques minutes et recommence à l'infini.

Variables :
- EXCEL_FILE : Chemin vers le fichier Excel contenant les ASINs, ids, et noms des produits.
- PARQUET_FILE : Nom du fichier Parquet où les offres seront enregistrées.
- SCRAPE_INTERVAL : Interval entre chaque cycle de scraping

Auteur : Vanessa KENNICHE SANOCKA, Thomas FERNANDES
Date : 28-10-2024
Version : 2.0

"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime
import os
import time
import re
import math

BASE_URL_TEMPLATE = 'https://www.amazon.fr/dp/{asin}'
MAIN_OFFER_URL_TEMPLATE = 'https://www.amazon.fr/gp/product/ajax/ref=dp_aod_ALL_mbc?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&experienceId=aodAjaxMain'
AJAX_URL_TEMPLATE = 'https://www.amazon.fr/gp/product/ajax/ref=aod_page_{page}?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&isonlyrenderofferlist=true&pageno={page}&experienceId=aodAjaxMain'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.amazon.fr/"
}
SCRAPE_INTERVAL = 1 * 60 * 60  # 1 heure en secondes
MAX_RETRY = 5
EXCEL_FILE = './../lien.xlsx'
PARQUET_FILE = "amazon_offers.parquet"
ZIP_FILE = "JSON_Amazon.zip"

logging.basicConfig(
    filename='log_amazon.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def clean_text(text):
    if text:
        return re.sub(r'\s+', ' ', text.strip())
    return 'N/A'

def scrape_main_offer(asin, idsmartphone, phone_name):
    offers = []
    main_offer_url = MAIN_OFFER_URL_TEMPLATE.format(asin=asin)
    logging.info(f"Scraping main offer for ASIN {asin}")

    session = requests.Session()
    try:
        response = session.get(main_offer_url, headers=HEADERS, timeout=10)
    except Exception as e:
        logging.error(f"Erreur lors de la requête principale pour ASIN {asin} : {e}")
        return offers

    logging.info(f"Main offer response status code: {response.status_code}")

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        price_block = soup.find('div', class_='a-section a-spacing-none aok-align-center aok-relative')
        if price_block:
            logging.info("Price block found in main offer")
            price_whole = price_block.find('span', class_='a-price-whole')
            price_fraction = price_block.find('span', class_='a-price-fraction')

            price_whole_text = price_whole.get_text(strip=True) if price_whole else '0'
            price_fraction_text = price_fraction.get_text(strip=True) if price_fraction else '00'

            price_value = clean_text(f"{price_whole_text}.{price_fraction_text}")
            price_value = re.sub(r'[^\d.]', '', price_value)
            try:
                price_value = float(price_value)
            except ValueError:
                logging.warning(f"Impossible de convertir le prix '{price_value}' en float pour ASIN {asin}.")
                price_value = pd.NA
        else:
            logging.warning("Price block not found in main offer")
            price_value = pd.NA

        expediteur_name = 'N/A'
        seller_name = 'N/A'

        expediteur_block = soup.find('div', id='aod-offer-shipsFrom')
        if expediteur_block:
            expediteur_span = expediteur_block.find('span', class_='a-size-small a-color-base')
            expediteur_name = clean_text(expediteur_span.get_text()) if expediteur_span else 'N/A'
            logging.info(f"Expediteur trouvé: {expediteur_name}")
        else:
            logging.warning("Expediteur block not found in main offer")

        seller_block = soup.find('div', id='aod-offer-soldBy')
        if seller_block:
            seller_span = seller_block.find('span', class_='a-size-small a-color-base')
            seller_name = clean_text(seller_span.get_text()) if seller_span else 'N/A'
            logging.info(f"Vendeur trouvé: {seller_name}")
        else:
            logging.warning("Seller block not found in main offer")

        product_state = 'Neuf'

        offer_details = {
            'pfid': "AMAZ",
            'idsmartphone': idsmartphone,
            'url': BASE_URL_TEMPLATE.format(asin=asin),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Price': price_value,
            'shipcost': pd.NA,
            'seller': seller_name,
            'rating': pd.NA,
            'ratingnb': pd.NA,
            'offertype': product_state,
            'offerdetails': pd.NA,
            'shipcountry': pd.NA,
            'sellercountry': pd.NA,
            'descriptsmartphone': phone_name
        }
        offers.append(offer_details)
        logging.info("Main offer retrieved")
    else:
        logging.error(f"Error retrieving main offer for ASIN {asin}: {response.status_code}")
        logging.debug(f"Response content: {response.text}")

    return offers

def scrape_amazon_offers(asin, idsmartphone, phone_name, start_page=1, max_pages=20):
    """
    Scrape les offres supplémentaires pour un produit Amazon.

    Parameters:
    - asin (str): L'ASIN du produit.
    - idsmartphone (str): L'identifiant unique du smartphone.
    - phone_name (str): Le nom du smartphone.
    - start_page (int): La page de départ pour le scraping (par défaut 1).
    - max_pages (int): Le nombre maximum de pages à scraper (par défaut 20).

    Returns:
    - offers (list): Une liste de dictionnaires contenant les détails des offres.
    """
    offers = []
    page = start_page

    while True:
        logging.info(f"Scraping page {page} for ASIN {asin}")
        ajax_url = AJAX_URL_TEMPLATE.format(page=page, asin=asin)
        try:
            response = requests.get(ajax_url, headers=HEADERS, timeout=10)
        except Exception as e:
            logging.error(f"Erreur lors de la requête AJAX pour ASIN {asin}, page {page} : {e}")
            break

        logging.info(f"Page {page} response status code: {response.status_code}")

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            offers_on_page = 0
            offer_blocks = soup.find_all('div', class_='a-section a-spacing-none a-padding-base aod-information-block aod-clear-float')

            if not offer_blocks:
                logging.info(f"No offer blocks found on page {page} for ASIN {asin}.")
                break

            for offer_block in offer_blocks:
                price_whole = offer_block.find('span', class_='a-price-whole')
                price_fraction = offer_block.find('span', class_='a-price-fraction')
                price_whole_text = price_whole.get_text(strip=True) if price_whole else '0'
                price_fraction_text = price_fraction.get_text(strip=True) if price_fraction else '00'
                price_value = clean_text(f"{price_whole_text}.{price_fraction_text}")
                price_value = re.sub(r'[^\d.]', '', price_value)
                try:
                    price_value = float(price_value)
                except ValueError:
                    logging.warning(f"Impossible de convertir le prix '{price_value}' en float pour ASIN {asin}.")
                    price_value = pd.NA

                seller_block = offer_block.find('div', {'id': 'aod-offer-soldBy'})
                if seller_block:
                    seller_name_element = seller_block.find('a', class_='a-size-small a-link-normal', role='link')
                    if seller_name_element:
                        seller_name = clean_text(seller_name_element.get_text())
                    else:
                        seller_name_span = seller_block.find('span', class_='a-size-small a-color-base')
                        seller_name = clean_text(seller_name_span.get_text() if seller_name_span else 'N/A')
                    logging.info(f"Vendeur trouvé: {seller_name}")
                else:
                    seller_name = 'N/A'
                    logging.warning("Seller block not found in offer block.")

                expediteur_block = offer_block.find('div', {'id': 'aod-offer-shipsFrom'})
                if expediteur_block:
                    expediteur_name_element = expediteur_block.find('div', class_='a-fixed-left-grid-col a-col-right')
                    if expediteur_name_element:
                        expediteur_span = expediteur_name_element.find('span', class_='a-size-small a-color-base')
                        expediteur_name = clean_text(expediteur_span.get_text() if expediteur_span else 'N/A')
                        logging.info(f"Expediteur trouvé: {expediteur_name}")
                    else:
                        expediteur_name = 'N/A'
                        logging.warning("Aucun inner <span> trouvé pour expediteur.")
                else:
                    expediteur_name = 'N/A'
                    logging.warning("Expediteur block not found in offer block.")

                state_element = offer_block.find('div', {'id': 'aod-offer-heading'})
                if state_element:
                    h5 = state_element.find('h5')
                    product_state = clean_text(h5.get_text()) if h5 else 'N/A'
                else:
                    product_state = 'N/A'
                    logging.warning("State block not found in offer block.")

                seller_rating_block = offer_block.find('div', id='aod-offer-seller-rating')
                if seller_rating_block:
                    rating_icon = seller_rating_block.find('i', class_=re.compile('a-icon-star-mini'))
                    if rating_icon and 'class' in rating_icon.attrs:
                        rating_classes = rating_icon['class']
                        rating_class = next((cls for cls in rating_classes if cls.startswith('a-star-mini-')), None)
                        if rating_class:
                            match = re.match(r'a-star-mini-(\d+)(?:-(\d))?', rating_class)
                            if match:
                                major = float(match.group(1))
                                minor = float(match.group(2)) / 10 if match.group(2) else 0.0
                                seller_rating = major + minor
                            else:
                                seller_rating = pd.NA
                                logging.warning(f"Regex non trouvée pour rating_class: '{rating_class}'")
                        else:
                            seller_rating = pd.NA
                            logging.warning("Aucun rating_class trouvé dans les classes du rating_icon.")
                    else:
                        seller_rating = pd.NA
                        logging.warning("Aucun rating_icon trouvé ou 'class' manquant.")
                else:
                    seller_rating = pd.NA
                    logging.warning("Seller rating block not found.")

                if 'amazon' not in seller_name.lower():
                    ratingnb = extract_ratingnb(offer_block, seller_name)
                else:
                    ratingnb = pd.NA
                    logging.info(f"Vendeur '{seller_name}' contient 'amazon', donc ratingnb ignoré.")

                offer_details = {
                    'pfid': "AMAZ",
                    'idsmartphone': idsmartphone,
                    'url': BASE_URL_TEMPLATE.format(asin=asin),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Price': price_value,
                    'shipcost': pd.NA,
                    'seller': seller_name,
                    'rating': seller_rating,
                    'ratingnb': ratingnb,
                    'offertype': product_state,
                    'offerdetails': pd.NA,
                    'shipcountry': pd.NA,
                    'sellercountry': pd.NA,
                    'descriptsmartphone': phone_name
                }
                offers.append(offer_details)
                logging.info("Other offers retrieved")

                offers_on_page += 1

            if offers_on_page < 10 or page >= max_pages:
                logging.info(f"Moins de 10 offres trouvées sur la page {page} pour ASIN {asin}. Fin du scraping.")
                break

            page += 1
            time.sleep(1)

        else:
            logging.error(f"Erreur lors de la récupération de la page {page} pour ASIN {asin}: {response.status_code}")
            logging.debug(f"Response content: {response.text}")
            break

    return offers

def extract_ratingnb(offer_block, seller_name):
    """
    Extrait le nombre d'évaluations du vendeur à partir du bloc d'offre.

    Parameters:
    - offer_block (BeautifulSoup object): Le bloc HTML de l'offre.
    - seller_name (str): Le nom du vendeur.

    Returns:
    - ratingnb (int or pd.NA): Le nombre d'évaluations ou pd.NA si non disponible.
    """
    span_tags = offer_block.find_all('span', id=re.compile(r'^seller-rating-count-'), class_='a-size-small a-color-base')

    for span in span_tags:
        inner_span = span.find('span')
        if inner_span:
            text = inner_span.get_text(strip=True)
            match = re.search(r'\(?(\d+)\s*évaluations\)?', text)
            if match:
                ratingnb_str = match.group(1)
                try:
                    ratingnb = int(ratingnb_str.replace('\xa0', '').replace(' ', ''))
                    return ratingnb
                except ValueError:
                    return pd.NA
        else:
            logging.warning(f"Aucun inner <span> trouvé dans la balise <span> pour vendeur {seller_name}.")

    logging.info(f"Aucun ratingnb trouvé pour vendeur {seller_name}.")
    return pd.NA

def save_offers_to_parquet(offers, filename='AMAZON.parquet'):
    if not offers:
        logging.info("Aucune offre à enregistrer.")
        return

    df = pd.DataFrame(offers)

    if 'ratingnb' in df.columns:
        df['ratingnb'] = df['ratingnb'].astype('Int64')
    if 'Price' in df.columns:
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    if 'rating' in df.columns:
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')

    try:
        if os.path.isfile(filename):
            existing_df = pd.read_parquet(filename, engine='pyarrow')
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(filename, engine='pyarrow', index=False)
        else:
            df.to_parquet(filename, engine='pyarrow', index=False)
        logging.info(f"Les données ont été ajoutées au fichier Parquet '{filename}' avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde en Parquet : {e}")

def scrape_amazon_product(asin, idsmartphone, phone_name):
    main_offers = scrape_main_offer(asin, idsmartphone, phone_name)
    time.sleep(1)
    other_offers = scrape_amazon_offers(asin, idsmartphone, phone_name)

    if main_offers is None:
        main_offers = []
        logging.warning(f"scrape_main_offer a retourné None pour ASIN {asin}.")
    if other_offers is None:
        other_offers = []
        logging.warning(f"scrape_amazon_offers a retourné None pour ASIN {asin}.")

    all_offers = main_offers + other_offers
    logging.info(f"Total des offres collectées pour ASIN {asin}: {len(all_offers)}")

    save_offers_to_parquet(all_offers, PARQUET_FILE)

if __name__ == "__main__":
    while True:
        try:
            try:
                excel_data = pd.read_excel(EXCEL_FILE, sheet_name='AMAZON', dtype={"idsmartphone": str})
                
                if 'Link_ID' in excel_data.columns:
                    excel_data.rename(columns={'Link_ID': 'ASIN'}, inplace=True)
                else:
                    time.sleep(600)
                    continue
                
                required_columns = {'ASIN', 'idsmartphone', 'Phone'}
                if not required_columns.issubset(excel_data.columns):
                    logging.error(f"Le fichier Excel doit contenir les colonnes suivantes : {required_columns}")
                    time.sleep(600)
                    continue
                asins = excel_data[['ASIN', 'idsmartphone', 'Phone']].dropna().values.tolist()
                logging.info(f"{len(asins)} ASINs chargés depuis le fichier Excel.")
            except Exception as e:
                logging.error(f"Erreur lors de la lecture du fichier Excel: {e}")
                asins = []

            if asins:
                num_asins = len(asins)
                sleep_time = math.ceil(SCRAPE_INTERVAL / num_asins)
                logging.info(f"Temps d'attente entre chaque ASIN: {sleep_time} secondes.")

                for idx, (asin, idsmartphone, phone_name) in enumerate(asins):
                    logging.info(f"Traitement de l'ASIN {asin} ({idx+1}/{num_asins}) avec l'ID {idsmartphone} et le téléphone {phone_name}")
                    scrape_amazon_product(asin, idsmartphone, phone_name)

                    if idx < num_asins - 1:
                        logging.info(f"Attente de {sleep_time} secondes avant le prochain ASIN.")
                        time.sleep(sleep_time)

                logging.info("Fin d'un cycle de scraping pour tous les ASINs. Recommence après une pause de 5 minutes.")
                time.sleep(300)
            else:
                logging.info("Aucun ASIN à traiter. Attente de 10 minutes avant de vérifier à nouveau.")
                time.sleep(600)

        except Exception as e:
            logging.error(f"Erreur dans le main : {e}")
            break
