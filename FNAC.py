"""
Script de scraping pour la FNAC
--------------------------------

Ce script scrape par itération une liste de pages de produits FNAC. 
Pour chaque page, il récupère les informations du produit via une requête GET, 
et récupère les données JSON de la page.
Les informations extraites sont ensuite converties et enregistrées dans un fichier CSV.
De plus, les fichiers JSON sont archivés dans un fichier ZIP pour garder une trace de toutes les requêtes.

Détails :
- Le script effectue une requête GET sur un URL FNAC et parse le contenu pour récupérer les informations du produit.
- À chaque itération, les nouvelles données sont ajoutées dans un fichier CSV ('fnac_offers.csv').
- Les fichiers JSON générés sont archivés dans un fichier ZIP ('JSON_FNAC.zip').
- Les requêtes sont effectuées de manière répartie sur un intervalle de 2 heures.
- Le script parcourt tous les produits de la liste une fois, puis recommence la liste à l'infini pour chaque produit à nouveau.

Auteur : Vanessa KENNICHE SANOCKA, Thomas FERNANDES
Date : 26-10-2024
Version : 1.1

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
EXCEL_FILE = './lien.xlsx'
CSV_FILE = "fnac_offers.csv"
ZIP_FILE = "JSON_FNAC.zip"
SCRAPE_INTERVAL = 2 * 60 * 60  # 2 heures en secondes
MAX_RETRY = 5

# Charger les données depuis le fichier Excel
excel_data = pd.read_excel(EXCEL_FILE, sheet_name="FNAC", dtype={"ID": str})
links = excel_data["Link"].tolist()
phones = excel_data["Phone"].tolist()
ids = excel_data["ID"].tolist()

# Liste de User-Agents, pour éviter le blocage
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
]

# LOGGER
logging.basicConfig(
    filename='log_fnac.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# FUNCTIONS
def scrape_fnac_product_info(url, phone_name, product_id):
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
                    convert_offers_to_csv(json_data, timestamp, phone_name, product_id)
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

def convert_offers_to_csv(json_data, timestamp, phone_name, product_id):
    try:
        product_data = json_data['product'][0]
        offers = product_data['attributes']['offer']
        
        offers_list = []
        for offer in offers:
            offer_details = {
                "Phone": phone_name,
                "ID": product_id,
                "Horodatage": timestamp,
                "Nom du Vendeur": offer.get('seller', 'N/A'),
                "Type de Vendeur": offer.get('sellerType', 'N/A'),
                "Prix": offer['price'].get('basePrice', 'N/A'),
                "Prix TTC": offer['price'].get('basePriceWithTax', 'N/A'),
                "Condition": offer.get('condition', 'N/A'),
                "Disponibilité": offer.get('fulfillment', 'N/A'),
                "Offre URL": offer.get('offerURL', 'N/A')
            }
            offers_list.append(offer_details)

        offers_df = pd.DataFrame(offers_list)
        file_exists = os.path.isfile(CSV_FILE)
        offers_df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)

        logging.info(f"Les données ont été ajoutées au fichier CSV '{CSV_FILE}' avec succès.")
    
    except Exception as e:
        logging.error(f"Erreur lors de la conversion en CSV : {e}")

# MAIN
if __name__ == "__main__":
    while True:
        try:
            num_links = len(links)
            # Calculer l'intervalle entre chaque requête pour répartir uniformément sur 2 heures
            interval_between_requests = SCRAPE_INTERVAL / num_links
            
            for i, link in enumerate(links):
                scrape_fnac_product_info(link, phones[i], ids[i])
                logging.info(f"Attente de {interval_between_requests:.2f} secondes avant la prochaine requête...")
                time.sleep(interval_between_requests)

            logging.info(f"Cycle complet terminé, reprise dans {SCRAPE_INTERVAL} secondes...")
        except Exception as e:
            logging.error(f"Erreur dans le main : {e}")
            break
