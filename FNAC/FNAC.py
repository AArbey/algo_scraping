"""
Script de scraping pour la FNAC
--------------------------------

Ce script scrape par itération une liste de pages de produits FNAC. 
Pour chaque page, il récupère les informations du produit via une requête GET, 
et récupère les données JSON de la page.
Les informations extraites sont ensuite converties et enregistrées dans un fichier Parquet.
De plus, les fichiers JSON sont archivés dans un fichier ZIP pour garder une trace de toutes les requêtes.

Détails :
- Le script effectue une requête GET sur un URL FNAC et parse le contenu pour récupérer les informations du produit.
- À chaque itération, les nouvelles données sont ajoutées dans un fichier Parquet ('fnac_offers.parquet').
- Les fichiers JSON générés sont archivés dans un fichier ZIP ('JSON_FNAC.zip').
- Les requêtes sont effectuées de manière répartie sur un intervalle de 2 heures.
- Le script parcourt tous les produits de la liste une fois, puis recommence la liste à l'infini pour chaque produit à nouveau.

Auteur : Vanessa KENNICHE SANOCKA, Thomas FERNANDES
Date : 09-12-2024
Version : 2.0

"""

import time
import logging
import random
import requests
import json
import pandas as pd
import os
import zipfile
from datetime import datetime
from bs4 import BeautifulSoup

# CONSTANTS
EXCEL_FILE = './../lien.xlsx'
PARQUET_FILE = "FNAC.parquet"
ZIP_FILE = "JSON_FNAC.zip"
SCRAPE_INTERVAL = 2 * 60 * 60  # 2 heures en secondes
MAX_RETRY = 5

# Charger les données depuis le fichier Excel
excel_data = pd.read_excel(EXCEL_FILE, sheet_name="FNAC", dtype={"idsmartphone": str})
links = excel_data["Link"].tolist()
phones = excel_data["Phone"].tolist()
idsmartphones = excel_data["idsmartphone"].tolist()

# Liste de User-Agents, pour éviter le blocage
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
]

# LOGGER
logging.basicConfig(
    filename='log_fnac.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# FUNCTIONS
def scrape_fnac_product_info(url, phone_name, idsmartphone):
    retry_count = 0
    while retry_count < MAX_RETRY:
        try:
            user_agent = random.choice(user_agents)
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                "cache-control": "max-age=0",
                "dnt": "1",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                logging.info("Page chargée avec succès avec User-Agent : %s", user_agent)
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extraction du JSON
                script_tag = soup.find('script', {'id': 'digitalData'})
                if script_tag:
                    json_data = json.loads(script_tag.string)
                    if 'user' in json_data:
                        del json_data['user']
                    if 'subscriptionplans' in json_data:
                        del json_data['subscriptionplans']

                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    json_filename = f'fnac_digitalData_{timestamp}.json'
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=4)

                    logging.info(f"Le fichier JSON '{json_filename}' a été créé avec succès.")
                    add_json_to_zip(json_filename)

                    # Extraire userRating
                    product_attributes = json_data['product'][0].get('attributes', {})
                    user_rating = product_attributes.get('userRating', pd.NA)

                    # Extraire ratingnb depuis le HTML
                    seller_ratings = extract_seller_ratings(soup)

                    convert_offers_to_parquet(json_data, timestamp, phone_name, idsmartphone, url, user_rating, seller_ratings)
                else:
                    logging.error("Le script avec id 'digitalData' n'a pas été trouvé.")
                break  # Sort de la boucle si la requête est un succès

            else:
                logging.warning(f"Erreur lors de la requête (code {response.status_code}) avec User-Agent {user_agent}. Réessai...")
                retry_count += 1
                continue  # Essaye avec un autre User-Agent

        except Exception as e:
            logging.error(f"Erreur lors de l'extraction des données : {e}")
            retry_count += 1

    if retry_count >= MAX_RETRY:
        logging.error(f"Échec de la récupération des données après {MAX_RETRY} tentatives.")

def add_json_to_zip(json_filename):
    try:
        with zipfile.ZipFile(ZIP_FILE, 'a') as zipf:
            zipf.write(json_filename, os.path.basename(json_filename))
        logging.info(f"Le fichier JSON '{json_filename}' a été ajouté au fichier ZIP '{ZIP_FILE}' avec succès.")
        
        os.remove(json_filename)
    except Exception as e:
        logging.error(f"Erreur lors de l'ajout du fichier JSON au ZIP : {e}")

def extract_seller_ratings(soup):
    """
    Extrait les noms des vendeurs et leur nombre d'avis depuis le HTML.
    Retourne un dictionnaire avec les noms normalisés des vendeurs comme clés et le nombre d'avis comme valeurs.
    """
    seller_ratings = {}
    
    # Trouver toutes les sections de vendeurs
    seller_sections = soup.find_all('div', class_='f-faMpSeller__label')
    
    for section in seller_sections:
        # Extraire le nom du vendeur
        seller_name_tag = section.find('strong', class_='f-faMpSeller__name')
        if seller_name_tag:
            seller_name = seller_name_tag.get_text(strip=True)
            normalized_seller_name = normalize_string(seller_name)
            
            # Trouver le nombre d'avis correspondant dans la section suivante
            rating_section = section.find_next_sibling('div', class_='f-faMpSeller__rating')
            if rating_section:
                rating_num_tag = rating_section.find('span', class_='f-rating__labelNum')
                if rating_num_tag:
                    rating_num_text = rating_num_tag.get_text(strip=True)
                    # Convertir "2 897" en "2897"
                    rating_num = int(rating_num_text.replace('\xa0', '').replace(' ', ''))
                    seller_ratings[normalized_seller_name] = rating_num
    return seller_ratings

def normalize_string(s):
    """
    Normalise une chaîne de caractères en supprimant les espaces et en mettant en minuscules.
    """
    return ''.join(s.lower().split())

def convert_offers_to_parquet(json_data, timestamp, phone_name, idsmartphone, page_url, user_rating, seller_ratings):
    try:
        product_data = json_data['product'][0]
        offers = product_data['attributes'].get('offer', [])
        
        offers_list = []
        for offer in offers:
            # Extraction des données disponibles
            shipcost = offer['price'].get('shipping', 0.0)
            # Assurer que shipcost est numérique
            if not isinstance(shipcost, (int, float)):
                try:
                    shipcost = float(shipcost)
                except ValueError:
                    shipcost = 0.0  # Valeur par défaut si conversion échoue

            seller_name = offer.get('seller', 'N/A')
            normalized_seller_name = normalize_string(seller_name)
            rating_nb = seller_ratings.get(normalized_seller_name, pd.NA)
            
            offer_details = {
                "pfid": "FNAC",
                "idsmartphone": idsmartphone,  # Utilisation de 'idsmartphone' depuis Excel
                "url": page_url,
                "timestamp": timestamp,
                "Price": offer['price'].get('basePrice', pd.NA),
                "shipcost": shipcost,
                "seller": seller_name,
                "rating": user_rating,
                "ratingnb": rating_nb,  # Nombre d'avis extrait du HTML
                "offertype": offer.get('condition', pd.NA),
                "offerdetails": pd.NA,  # Laisser vide pour le moment
                "shipcountry": pd.NA,    # Laisser vide pour le moment
                "sellercountry": offer.get('sellerLocation', pd.NA),
                "descriptsmartphone": phone_name
            }
            offers_list.append(offer_details)

        if offers_list:
            offers_df = pd.DataFrame(offers_list)
            
            # Convertir les colonnes en types appropriés
            if 'ratingnb' in offers_df.columns:
                offers_df['ratingnb'] = offers_df['ratingnb'].astype('Int64')  # Nullable integer

            # Vérifier si le fichier Parquet existe déjà
            if os.path.isfile(PARQUET_FILE):
                # Charger le fichier existant
                existing_df = pd.read_parquet(PARQUET_FILE, engine='pyarrow')
                # Concatenation des DataFrames
                combined_df = pd.concat([existing_df, offers_df], ignore_index=True)
                # Écrire le DataFrame combiné dans le fichier Parquet
                combined_df.to_parquet(PARQUET_FILE, engine='pyarrow', index=False)
            else:
                # Écrire directement le DataFrame dans un nouveau fichier Parquet
                offers_df.to_parquet(PARQUET_FILE, engine='pyarrow', index=False)
            
            logging.info(f"Les données ont été ajoutées au fichier Parquet '{PARQUET_FILE}' avec succès.")
    
    except Exception as e:
        logging.error(f"Erreur lors de la conversion en Parquet : {e}")

# MAIN
if __name__ == "__main__":
    while True:
        try:
            num_links = len(links)
            if num_links == 0:
                logging.warning("Aucun lien trouvé dans le fichier Excel. Attente de 2 heures avant de réessayer.")
                time.sleep(SCRAPE_INTERVAL)
                continue

            # Calculer l'intervalle entre chaque requête pour répartir uniformément sur 2 heures
            interval_between_requests = SCRAPE_INTERVAL / num_links
            
            for i, link in enumerate(links):
                scrape_fnac_product_info(link, phones[i], idsmartphones[i])
                logging.info(f"Attente de {interval_between_requests:.2f} secondes avant la prochaine requête...")
                time.sleep(interval_between_requests)

            logging.info(f"Cycle complet terminé, reprise dans {SCRAPE_INTERVAL} secondes...")
        except Exception as e:
            logging.error(f"Erreur dans le main : {e}")
            break
