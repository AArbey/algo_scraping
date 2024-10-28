"""
Script de scraping pour Amazon
------------------------------

Ce script scrape par itération une liste de pages de produits Amazon.
Pour chaque produit (défini par un ASIN), il récupère les informations sur l'offre principale 
ainsi que les offres supplémentaires via des requêtes AJAX. Les informations extraites sont ensuite 
converties et enregistrées dans un fichier CSV.
Les requêtes sont effectuées de manière répartie sur un intervalle de 1 heures, puis le script recommence la liste à l'infini.

Détails :
- Le script effectue une requête GET sur un URL Amazon pour récupérer les informations de l'offre principale, 
  et utilise des requêtes AJAX pour récupérer les offres supplémentaires.
- À chaque itération, les nouvelles données sont ajoutées dans un fichier CSV ('amazon_offers.csv').
- Les requêtes sont effectuées de manière aléatoire pour éviter le blocage, en utilisant un intervalle défini de temps entre chaque produit.
- Une fois que tous les produits de la liste sont scrappés, le script attend quelques minutes et recommence à l'infini.

Variables :
- EXCEL_FILE : Chemin vers le fichier Excel contenant les ASINs, IDs et noms des produits.
- CSV_FILE : Nom du fichier CSV où les offres seront enregistrées.
- SCRAPE_INTERVAL : Interval entre chaque cycle de scraping

Auteur : Vanessa KENNICHE SANOCKA, Thomas FERNANDES
Date : 28-10-2024
Version : 1.0

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

# CONSTANTS
BASE_URL_TEMPLATE = 'https://www.amazon.fr/dp/{asin}'
MAIN_OFFER_URL_TEMPLATE = 'https://www.amazon.fr/gp/product/ajax/ref=dp_aod_ALL_mbc?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&experienceId=aodAjaxMain'
AJAX_URL_TEMPLATE = 'https://www.amazon.fr/gp/product/ajax/ref=aod_page_{page}?asin={asin}&m=&qid=&smid=&sourcecustomerorglistid=&sourcecustomerorglistitemid=&sr=&pc=dp&isonlyrenderofferlist=true&pageno={page}&experienceId=aodAjaxMain'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.amazon.fr/"
}
SCRAPE_INTERVAL = 1 * 60 * 60 # 1h

def clean_text(text):
    if text:
        return re.sub(r'\s+', ' ', text.strip())
    return 'N/A'

def scrape_main_offer(asin, phone_id, phone_name):
    offers = []
    main_offer_url = MAIN_OFFER_URL_TEMPLATE.format(asin=asin)
    logging.info(f"Scraping main offer for ASIN {asin}")

    session = requests.Session()
    response = session.get(main_offer_url, headers=HEADERS)

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
        else:
            logging.warning("Price block not found in main offer")
            price_value = 'N/A'

        expediteur_name = 'N/A'
        seller_name = 'N/A'

        expediteur_block = soup.find('div', id='aod-offer-shipsFrom')
        if expediteur_block:
            expediteur_span = expediteur_block.find('span', class_='a-size-small a-color-base')
            expediteur_name = clean_text(expediteur_span.get_text()) if expediteur_span else 'N/A'
        else:
            logging.warning("Expediteur block not found in main offer")

        seller_block = soup.find('div', id='aod-offer-soldBy')
        if seller_block:
            seller_span = seller_block.find('span', class_='a-size-small a-color-base')
            seller_name = clean_text(seller_span.get_text()) if seller_span else 'N/A'
        else:
            logging.warning("Seller block not found in main offer")

        seller_rating = 'N/A'

        product_state = 'Neuf'

        offer_details = {
            'ID': phone_id,
            'ASIN': asin,
            'Phone': phone_name,
            'Page': 0,  # 0 est la main offer
            'Prix': price_value,
            'Vendeur': seller_name,
            'Expéditeur': expediteur_name,
            'Etat': product_state,
            'Note Vendeur': seller_rating,
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        offers.append(offer_details)
        logging.info(f"Main offer retrieved: {offer_details}")
    else:
        logging.error(f"Error retrieving main offer for ASIN {asin}: {response.status_code}")
        logging.debug(f"Response content: {response.text}")

    return offers

def scrape_amazon_offers(asin, phone_id, phone_name, start_page=1, max_pages=20):
    """
    Scrape les offres supplémentaires pour un produit Amazon.
    
    Parameters:
    - asin (str): L'ASIN du produit.
    - phone_id (str): L'identifiant unique du téléphone.
    - phone_name (str): Le nom du téléphone.
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
        response = requests.get(ajax_url, headers=HEADERS)

        logging.info(f"Page {page} response status code: {response.status_code}")

        # Vérifier si la requête a réussi
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            # Initialiser le compteur pour les offres trouvées sur cette page
            offers_on_page = 0

            # Trouver tous les blocs d'offres sur la page
            offer_blocks = soup.find_all('div', class_='a-section a-spacing-none a-padding-base aod-information-block aod-clear-float')

            # Si aucun bloc d'offres n'est trouvé, arrêter le scraping
            if not offer_blocks:
                logging.info(f"No offer blocks found on page {page} for ASIN {asin}.")
                break

            # Parcourir chaque bloc d'offre trouvé
            for offer_block in offer_blocks:
                # Extraire les informations de prix
                price_whole = offer_block.find('span', class_='a-price-whole')
                price_fraction = offer_block.find('span', class_='a-price-fraction')
                price_whole_text = price_whole.get_text(strip=True) if price_whole else '0'
                price_fraction_text = price_fraction.get_text(strip=True) if price_fraction else '00'
                price_value = clean_text(f"{price_whole_text}.{price_fraction_text}")
                price_value = re.sub(r'[^\d.]', '', price_value)

                # Extraire le nom du vendeur
                seller_block = offer_block.find('div', {'id': 'aod-offer-soldBy'})
                if seller_block:
                    seller_name_element = seller_block.find('a', class_='a-size-small a-link-normal', role='link')
                    if seller_name_element:
                        seller_name = clean_text(seller_name_element.get_text())
                    else:
                        seller_name_span = seller_block.find('span', class_='a-size-small a-color-base')
                        seller_name = clean_text(seller_name_span.get_text() if seller_name_span else 'N/A')
                else:
                    seller_name = 'N/A'

                # Extraire le nom de l'expéditeur
                expediteur_block = offer_block.find('div', {'id': 'aod-offer-shipsFrom'})
                if expediteur_block:
                    expediteur_name_element = expediteur_block.find('div', class_='a-fixed-left-grid-col a-col-right').find('span', class_='a-size-small a-color-base')
                    expediteur_name = clean_text(expediteur_name_element.get_text() if expediteur_name_element else 'N/A')
                else:
                    expediteur_name = 'N/A'

                # Extraire l'état du produit
                state_element = offer_block.find('div', {'id': 'aod-offer-heading'}).find('h5')
                product_state = clean_text(state_element.get_text() if state_element else 'N/A')

                # Extraire la note du vendeur
                seller_rating_block = offer_block.find('div', id='aod-offer-seller-rating')
                if seller_rating_block:
                    rating_icon = seller_rating_block.find('i', class_=re.compile('a-icon-star-mini'))
                    if rating_icon and 'class' in rating_icon.attrs:
                        rating_classes = rating_icon['class']
                        rating_class = next((cls for cls in rating_classes if cls.startswith('a-star-mini-')), None)
                        seller_rating = rating_class.replace('a-star-mini-', '') if rating_class else 'N/A'
                    else:
                        seller_rating = 'N/A'
                else:
                    seller_rating = 'N/A'

                # Stocker les détails de l'offre
                offer_details = {
                    'ID': phone_id,
                    'ASIN': asin,
                    'Phone': phone_name,
                    'Page': page,
                    'Prix': price_value,
                    'Vendeur': seller_name,
                    'Expéditeur': expediteur_name,
                    'Etat': product_state,
                    'Note Vendeur': seller_rating,
                    'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                offers.append(offer_details)
                logging.info(f"Récupéré : {offer_details}")

                # Incrémenter le compteur d'offres pour cette page
                offers_on_page += 1

            # Si moins de 10 offres sont trouvées ou que la limite de pages est atteinte, arrêter le scraping
            if offers_on_page < 10 or page >= max_pages:
                logging.info(f"Moins de 10 offres trouvées sur la page {page} pour ASIN {asin}. Fin du scraping.")
                break

            # Passer à la page suivante
            page += 1
            time.sleep(1)  # Pause pour éviter le blocage

        else:
            # En cas d'erreur, afficher le code de réponse et le contenu pour debug
            logging.error(f"Erreur lors de la récupération de la page {page} pour ASIN {asin}: {response.status_code}")
            logging.debug(f"Response content: {response.text}")
            break

    return offers

def save_offers_to_csv(offers, filename='amazon_offers.csv'):
    df = pd.DataFrame(offers)
    file_exists = os.path.isfile(filename)
    df.to_csv(filename, mode='a', header=not file_exists, index=False)
    logging.info(f"Les données ont été enregistrées dans {filename}.")

# MAIN
if __name__ == "__main__":
    logging.basicConfig(
        filename='log_amazon.txt',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8'
    )

    while True:
        try:
            df_links = pd.read_excel('lien.xlsx', sheet_name='AMAZON')
            asins = df_links[['ID', 'Link_ID', 'Phone']].dropna().values.tolist()
        except Exception as e:
            logging.error(f"Erreur lors de la lecture du fichier Excel: {e}")
            asins = []

        if asins:
            num_asins = len(asins)
            sleep_time = math.ceil(SCRAPE_INTERVAL / num_asins)
            logging.info(f"Temps d'attente entre chaque ASIN: {sleep_time} secondes.")

            for idx, (phone_id, asin, phone_name) in enumerate(asins):
                logging.info(f"Traitement de l'ASIN {asin} ({idx+1}/{num_asins}) avec l'ID {phone_id} et le téléphone {phone_name}")

                main_offer = scrape_main_offer(asin, phone_id, phone_name)
                time.sleep(1)

                other_offers = scrape_amazon_offers(asin, phone_id, phone_name)

                all_offers = main_offer + other_offers

                if all_offers:
                    save_offers_to_csv(all_offers)
                else:
                    logging.info(f"Aucune offre récupérée pour l'ASIN {asin}.")

                if idx < num_asins - 1:
                    logging.info(f"Attente de {sleep_time} secondes avant le prochain ASIN.")
                    time.sleep(sleep_time)

            logging.info("Fin d'un cycle de scraping pour tous les ASINs. Recommence après une pause de 5 minutes.")
            time.sleep(300)

        else:
            logging.info("Aucun ASIN à traiter. Attente de 10 minutes avant de vérifier à nouveau.")
            time.sleep(600)
