"""
Script de scraping pour Rakuten
--------------------------------

Ce script effectue une extraction des offres de smartphones à partir d'une liste de liens Rakuten.
Pour chaque lien, il envoie une requête HTTP pour récupérer les informations du produit,
puis extrait et enregistre les données dans un fichier CSV. Les requêtes sont étalées
uniformément dans un intervalle défini.

Détails :
- Le script charge un fichier Excel contenant des identifiants et des URL de produits.
- Pour chaque produit, il envoie une requête, récupère les données JSON et extrait les informations
  pertinentes telles que le prix, le coût de livraison et l'état de l'offre.
- Les données sont ajoutées dans un fichier CSV ('Rakuten_data.csv').
- Un fichier de log ('log_rakuten.txt') est utilisé pour suivre les erreurs et les informations de suivi.

Auteur : Thomas FERNANDES
Date : 05-11-2024
Version : 1.1
"""

import requests
import json
import csv
import time
import logging
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

EXCEL_FILE = "ID_EXCEL.xlsx"
CSV_FILE = "Rakuten_data.csv"
LOG_FILE = "log_rakuten.txt"
INTERVAL = 60 * 20

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

FIELDNAMES = [
    "pfid", "idsmartphone", "url", "timestamp", "price", "shipcost", "rating",
    "ratingnb", "offertype", "offerdetails", "shipcountry", "sellercountry"
]

def load_excel_data():
    try:
        df = pd.read_excel(EXCEL_FILE, skiprows=7)
        return df.iloc[:, [2, 14]]
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier Excel : {e}")
        return pd.DataFrame()

def init_csv():
    try:
        with open(CSV_FILE, mode='x', newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
    except FileExistsError:
        pass

def scrape_rakuten_product(idsmartphone, url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logging.info(f"Requête réussie pour {url}")
            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.find("script", {"type": "application/ld+json", "id": "ggrc", "data-qa": "md_product"})
            if script_tag:
                data_json = json.loads(script_tag.string)
                process_json_data(data_json, idsmartphone)
            else:
                logging.warning("La balise JSON demandée n'a pas été trouvée.")
        else:
            logging.error(f"Échec de la requête pour {url}. Code de statut : {response.status_code}")
    except Exception as e:
        logging.error(f"Erreur lors du scraping du produit {idsmartphone} : {e}")

def process_json_data(data_json, idsmartphone):
    offers = data_json.get("offers", {}).get("offers", [])
    product_url = data_json.get("url", "")
    timestamp = datetime.now().strftime("%Y/%m/%d/%H:%M")

    for offer in offers:
        row_data = {
            "pfid": "RAK",
            "idsmartphone": idsmartphone,
            "url": product_url,
            "timestamp": timestamp,
            "price": offer.get("price", ""),
            "shipcost": offer.get("shippingDetails", {}).get("shippingRate", {}).get("value", ""),
            "rating": "",  # Valeur vide
            "ratingnb": "",  # Valeur vide
            "offertype": offer.get("itemCondition", ""),
            "offerdetails": "",  # Valeur vide
            "shipcountry": "",  # Valeur vide
            "sellercountry": ""  # Valeur vide
        }
        save_to_csv(row_data)

def save_to_csv(row_data):
    try:
        with open(CSV_FILE, mode='a', newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writerow(row_data)
    except Exception as e:
        logging.error(f"Erreur lors de l'écriture des données dans le CSV : {e}")

def main():
    while True:
        start_time = time.time()
        df_links = load_excel_data()
        num_telephones = len(df_links)
        if num_telephones == 0:
            logging.warning("Aucun téléphone à scrapper. Attente avant le prochain cycle.")
            time.sleep(INTERVAL)
            continue

        request_interval = INTERVAL / num_telephones

        init_csv()
        
        for index, row in df_links.iterrows():
            idsmartphone = row.iloc[0]
            url = str(row.iloc[1])

            if "rakuten" not in url.lower():
                logging.info("Lien invalide détecté, redémarrage de la liste.")
                break

            request_start_time = time.time()
            scrape_rakuten_product(idsmartphone, url)
            request_end_time = time.time()

            elapsed_time = request_end_time - request_start_time
            sleep_time = max(0, request_interval - elapsed_time)
            time.sleep(sleep_time)

        total_cycle_time = time.time() - start_time
        if total_cycle_time < INTERVAL:
            remaining_time = INTERVAL - total_cycle_time
            logging.info(f"Cycle complet terminé, attente de {remaining_time:.2f} secondes avant le prochain cycle...")
            time.sleep(remaining_time)
        else:
            logging.info("Cycle complet terminé, démarrage immédiat du prochain cycle.")


if __name__ == "__main__":
    main()
