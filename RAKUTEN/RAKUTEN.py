"""
Script de scraping pour Rakuten
--------------------------------

Version : 2.1

Ce script effectue une extraction des offres de smartphones à partir d'une liste de liens Rakuten.
Pour chaque lien, il envoie une requête HTTP pour récupérer les informations du produit,
puis extrait et enregistre les données dans un fichier Parquet. Les requêtes sont étalées
uniformément dans un intervalle défini.

Fonctionnalités :
- Charge un fichier Excel contenant des identifiants et des URL de produits.
- Pour chaque produit, envoie une requête pour récupérer les données JSON et extrait les informations
  pertinentes telles que le prix, le coût de livraison et l'état de l'offre.
- Récupère les ratings des vendeurs en scrappant les pages des boutiques des vendeurs.
- Utilise un cache pour stocker les informations des vendeurs et éviter des requêtes redondantes.
- Enregistre les données scrappées dans un fichier Parquet ('Rakuten_data.parquet').
- Utilise un fichier de log ('log_rakuten.log') pour suivre les erreurs et les informations de suivi.
- Gère les délais et les intervalles entre les requêtes pour minimiser le risque de blocage.
- Sauvegarde de secours en CSV en cas d'échec de la sauvegarde en Parquet.

Configuration :
- Les chemins des fichiers et les paramètres de scraping peuvent être ajustés dans la section de configuration.
- Les préfixes des vendeurs à ignorer sont définis dans `SELLER_PREFIXES_TO_SKIP`.

Auteur : Thomas FERNANDES
Date : 05-11-2024
"""

import requests
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import os
import time
import re
from urllib.parse import urlparse, parse_qs
import random

# -----------------------------------------------------------------------------
# Configuration des fichiers et paramètres
# -----------------------------------------------------------------------------
EXCEL_FILE = "../ID_EXCEL.xlsx"
PARQUET_FILE = "Rakuten_data.parquet"
LOG_FILE = "log_rakuten.log"
SELLER_CACHE_FILE = "seller_cache.parquet"
INTERVAL = 60 * 30  # 30 minutes
CACHE_EXPIRY = timedelta(hours=24)  # 24 heures
CACHE_MAX_AGE = timedelta(days=30)  # Supprimer les entrées non mises à jour depuis 30 jours
SELLER_PREFIXES_TO_SKIP = ["Club_R_", "ClubR_"]  # Liste des préfixes à exclure

# Configuration du logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Liste des User-Agents pour la rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
]

# -----------------------------------------------------------------------------
# Fonctions utilitaires
# -----------------------------------------------------------------------------
def get_random_user_agent():
    return random.choice(USER_AGENTS)

def extract_pid_cid(url):
    """Extrait les paramètres pid et cid à partir de l'URL."""
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        pid = query_params.get('pid', [None])[0]
        path_parts = parsed_url.path.split('/')
        cid = path_parts[2] if len(path_parts) > 2 else pd.NA
        logging.debug(f"PID: {pid}, CID: {cid} extraits de l'URL: {url}")
        return pid, cid
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction de pid et cid de l'URL {url} : {e}")
        return pd.NA, pd.NA

# -----------------------------------------------------------------------------
# Fonctions de création / chargement / sauvegarde du cache vendeur
# -----------------------------------------------------------------------------
def load_seller_cache():
    """Charge le cache des vendeurs depuis un fichier Parquet."""
    if os.path.exists(SELLER_CACHE_FILE):
        try:
            df_cache = pd.read_parquet(SELLER_CACHE_FILE)
            logging.debug(f"Cache des vendeurs chargé avec {len(df_cache)} entrées.")
            if df_cache['last_scraped'].dtype == object:
                df_cache['last_scraped'] = pd.to_datetime(df_cache['last_scraped'])
            return df_cache
        except Exception as e:
            logging.error(f"Erreur lors du chargement du cache des vendeurs : {e}")
            return pd.DataFrame(columns=["seller_name", "rating", "ratingnb", "shipcountry", "sellercountry", "last_scraped"])
    else:
        logging.debug("Aucun cache des vendeurs trouvé. Création d'un nouveau cache vide.")
        return pd.DataFrame(columns=["seller_name", "rating", "ratingnb", "shipcountry", "sellercountry", "last_scraped"])

def save_seller_cache(df_cache):
    """Sauvegarde le cache des vendeurs dans un fichier Parquet."""
    try:
        df_cache.to_parquet(SELLER_CACHE_FILE, engine='pyarrow', index=False, compression='snappy')
        logging.debug(f"Cache des vendeurs sauvegardé avec {len(df_cache)} entrées.")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde du cache des vendeurs : {e}")

def clean_seller_cache(df_cache):
    """Nettoie le cache des vendeurs en supprimant les entrées non mises à jour depuis CACHE_MAX_AGE."""
    if not df_cache.empty:
        cutoff_time = datetime.now() - CACHE_MAX_AGE
        initial_length = len(df_cache)
        df_cache = df_cache[df_cache['last_scraped'] >= cutoff_time]
        final_length = len(df_cache)
        if final_length < initial_length:
            logging.debug(f"Nettoyé le cache des vendeurs : {initial_length - final_length} entrées supprimées.")
    return df_cache

# -----------------------------------------------------------------------------
# Fonctions de gestion des données Excel et Parquet
# -----------------------------------------------------------------------------
def load_excel_data():
    """Charge les données depuis le fichier Excel."""
    try:
        df = pd.read_excel(EXCEL_FILE, skiprows=7)
        logging.debug(f"Excel chargé avec {len(df)} lignes.")
        df_selected = df.iloc[:, [2, 14]].dropna()
        logging.debug(f"{len(df_selected)} enregistrements valides après suppression des NA.")
        return df_selected
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier Excel : {e}")
        return pd.DataFrame()

def save_to_parquet(data, filename=PARQUET_FILE):
    """Enregistre les données dans un fichier Parquet avec gestion des types."""
    try:
        if not data:
            logging.info("Aucune donnée à enregistrer pour ce cycle.")
            return

        df_new = pd.DataFrame(data)
        logging.debug(f"{len(df_new)} nouvelles offres à enregistrer dans le Parquet.")

        # Définir les types de données
        dtypes = {
            'pfid': 'string',
            'idsmartphone': 'string',
            'url': 'string',
            'timestamp': 'string',
            'price': 'float64',
            'shipcost': 'float64',
            'rating': 'float64',
            'ratingnb': 'Int64',
            'offertype': 'string',
            'offerdetails': 'string',
            'shipcountry': 'string',
            'sellercountry': 'string',
            'seller': 'string'
        }

        # Appliquer les types de données
        for col, dtype in dtypes.items():
            if col in df_new.columns:
                try:
                    if dtype == 'string':
                        df_new[col] = df_new[col].astype('string')
                    elif dtype == 'float64':
                        df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
                    elif dtype == 'Int64':
                        df_new[col] = pd.to_numeric(df_new[col], errors='coerce').astype('Int64')
                except Exception as e:
                    logging.warning(f"Erreur lors de la conversion de la colonne {col} : {e}")
                    if dtype == 'float64':
                        df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
                    elif dtype == 'Int64':
                        df_new[col] = pd.to_numeric(df_new[col], errors='coerce').astype('Int64')
                    else:
                        df_new[col] = df_new[col].astype('string')

        # Charger les données existantes si le fichier existe
        if os.path.exists(filename):
            logging.debug(f"Fichier Parquet existant trouvé : {filename}")
            try:
                df_existing = pd.read_parquet(filename)
                logging.debug(f"{len(df_existing)} offres existantes chargées.")
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            except Exception as e:
                logging.error(f"Erreur lors du chargement du fichier Parquet existant : {e}")
                df_combined = df_new
        else:
            df_combined = df_new
            logging.debug(f"Création d'un nouveau fichier Parquet avec {len(df_combined)} offres.")

        # Sauvegarder les données combinées
        df_combined.to_parquet(filename, engine='pyarrow', index=False, compression='snappy')
        logging.info(f"Données sauvegardées avec succès dans {filename}")
    except Exception as e:
        logging.error(f"Erreur lors de l'enregistrement en Parquet : {e}")
        try:
            backup_file = filename.replace('.parquet', '_backup.csv')
            df_new.to_csv(backup_file, index=False)
            logging.info(f"Sauvegarde de secours effectuée dans {backup_file}")
        except Exception as backup_err:
            logging.error(f"Échec de la sauvegarde de secours : {backup_err}")

# -----------------------------------------------------------------------------
# Fonctions de scraping et gestion des vendeurs
# -----------------------------------------------------------------------------
def parse_seller_page(seller_name, session):
    """
    Scrape les informations du vendeur à partir de sa page boutique.
    Retourne un tuple (seller_info, success_flag).
    """
    prefixes_to_remove = ["Club_R_", "ClubR_"]
    seller_url_part = seller_name

    # Retirer les préfixes exclus
    for prefix in prefixes_to_remove:
        if isinstance(seller_name, str) and seller_name.startswith(prefix):
            seller_url_part = seller_name[len(prefix):]
            break

    seller_url = f"https://fr.shopping.rakuten.com/boutique/{seller_url_part}"
    headers = {
        "User-Agent": get_random_user_agent(),
        "Referer": "https://fr.shopping.rakuten.com/"
    }
    try:
        response = session.get(seller_url, headers=headers, timeout=10)
        logging.debug(f"Requête envoyée à {seller_url} avec le statut {response.status_code}.")

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # Extraire le JSON contenant les informations du vendeur
            script_tag = soup.find("script", string=re.compile(r'window\.INITIAL_STORE\.navandsearch'))
            if script_tag:
                try:
                    script_content = script_tag.string
                    json_str_match = re.search(r'window\.INITIAL_STORE\.navandsearch\s*=\s*(\{.*\});', script_content, re.DOTALL)
                    if json_str_match:
                        json_str = json_str_match.group(1)
                        data_json = json.loads(json_str)

                        result = data_json.get("result", {})
                        if not result:
                            logging.warning(f"'result' non trouvé dans le JSON pour {seller_name}.")
                            return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False

                        # Extraction des informations du vendeur
                        seller_info_data = result.get("eshopInfo", {})
                        rating_str = seller_info_data.get("sellerRating", pd.NA)
                        if isinstance(rating_str, str):
                            try:
                                rating = float(rating_str.replace(',', '.').replace('\xa0', '').strip())
                                logging.debug(f"Rating extrait pour {seller_name}: {rating}")
                            except ValueError:
                                logging.warning(f"Impossible de convertir rating '{rating_str}' en float pour {seller_name}.")
                                rating = pd.NA
                        else:
                            rating = pd.NA
                            logging.warning(f"'sellerRating' non disponible pour {seller_name}.")

                        number_of_sale_str = seller_info_data.get("numberOfSale", "0")
                        number_of_sale_clean = re.sub(r'\s+', '', number_of_sale_str)
                        if number_of_sale_clean.isdigit():
                            ratingnb = int(number_of_sale_clean)
                            logging.debug(f"Ratingnb extrait pour {seller_name}: {ratingnb}")
                        else:
                            logging.warning(f"Impossible de convertir numberOfSale '{number_of_sale_str}' en int pour {seller_name}.")
                            ratingnb = pd.NA

                        legal_notice = seller_info_data.get("legalNotice", {})
                        shipcountry = legal_notice.get("address", {}).get("countryName", pd.NA)
                        logging.debug(f"ShipCountry extrait pour {seller_name}: {shipcountry}")

                        eshop_legal_notice = result.get("eshopLegalNotice", {})
                        sellercountry = eshop_legal_notice.get("address", {}).get("countryName", pd.NA)
                        logging.debug(f"SellerCountry extrait pour {seller_name}: {sellercountry}")

                        return {
                            "rating": rating,
                            "ratingnb": ratingnb,
                            "shipcountry": shipcountry,
                            "sellercountry": sellercountry
                        }, True
                    else:
                        logging.warning(f"Impossible d'extraire le JSON pour {seller_name}.")
                        return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False
                except json.JSONDecodeError as je:
                    logging.error(f"Erreur de décodage JSON pour {seller_name}: {je}")
                    return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False
            else:
                logging.warning(f"Aucune balise script JSON contenant window.INITIAL_STORE.navandsearch trouvée pour {seller_name}.")
                return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False

        elif response.status_code == 403:
            logging.error(f"403 Forbidden lors de la requête pour la page du vendeur {seller_name}.")
            return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False

        else:
            logging.error(f"Échec de la requête pour la page du vendeur {seller_name}. Status: {response.status_code}")
            return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False

    except requests.exceptions.Timeout:
        logging.error(f"Timeout lors de la requête pour la page du vendeur: {seller_url}.")
        return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Erreur lors du scraping de la page du vendeur {seller_name} : {req_err}")
        return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, False

def get_seller_info(seller_name, session, df_cache):
    """
    Récupère les informations d'un vendeur soit depuis le cache,
    soit en scrapant la page si le cache est expiré ou absent.
    Met à jour le cache si une nouvelle requête est effectuée et réussie.
    """
    if pd.isna(seller_name):
        logging.debug("Nom du vendeur est NA. Retour des valeurs manquantes.")
        return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, df_cache

    # Vérifier si le vendeur doit être ignoré
    if isinstance(seller_name, str) and any(seller_name.startswith(prefix) for prefix in SELLER_PREFIXES_TO_SKIP):
        logging.debug(f"Vendeur '{seller_name}' commence par l'un des préfixes exclus {SELLER_PREFIXES_TO_SKIP}. Enregistrement avec des NA.")
        new_entry = {
            "seller_name": seller_name,
            "rating": pd.NA,
            "ratingnb": pd.NA,
            "shipcountry": pd.NA,
            "sellercountry": pd.NA,
            "last_scraped": datetime.now()
        }
        if seller_name not in df_cache['seller_name'].values:
            df_new_entry = pd.DataFrame([new_entry], columns=df_cache.columns)
            df_cache = pd.concat([df_cache, df_new_entry], ignore_index=True)
        else:
            df_cache.loc[df_cache['seller_name'] == seller_name, ['rating', 'ratingnb', 'shipcountry', 'sellercountry', 'last_scraped']] = [
                pd.NA, pd.NA, pd.NA, pd.NA, new_entry["last_scraped"]
            ]
        return {"rating": pd.NA, "ratingnb": pd.NA, "shipcountry": pd.NA, "sellercountry": pd.NA}, df_cache

    # Vérifier si le vendeur est dans le cache
    seller_row = df_cache[df_cache['seller_name'] == seller_name]
    now = datetime.now()
    if not seller_row.empty:
        last_scraped = seller_row.iloc[0]['last_scraped']
        if now - last_scraped < CACHE_EXPIRY:
            logging.debug(f"Utilisation des données du cache pour le vendeur '{seller_name}'.")
            return {
                "rating": seller_row.iloc[0]['rating'],
                "ratingnb": seller_row.iloc[0]['ratingnb'],
                "shipcountry": seller_row.iloc[0]['shipcountry'],
                "sellercountry": seller_row.iloc[0]['sellercountry']
            }, df_cache
        else:
            logging.debug(f"Données du cache expirées pour le vendeur '{seller_name}'. Scraping nécessaire.")
    else:
        logging.debug(f"Vendeur '{seller_name}' non trouvé dans le cache. Scraping nécessaire.")

    # Ajouter un délai aléatoire avant de scraper la page du vendeur
    delay = random.uniform(5, 10)
    logging.debug(f"Pause de {delay:.2f} secondes avant de scraper la page du vendeur '{seller_name}'.")
    time.sleep(delay)

    # Scraper la page du vendeur
    seller_info, success = parse_seller_page(seller_name, session)

    if success:
        new_entry = {
            "seller_name": seller_name,
            "rating": seller_info.get("rating", pd.NA),
            "ratingnb": seller_info.get("ratingnb", pd.NA),
            "shipcountry": seller_info.get("shipcountry", pd.NA),
            "sellercountry": seller_info.get("sellercountry", pd.NA),
            "last_scraped": now
        }

        if not seller_row.empty:
            df_cache.loc[df_cache['seller_name'] == seller_name, ['rating', 'ratingnb', 'shipcountry', 'sellercountry', 'last_scraped']] = [
                new_entry["rating"], new_entry["ratingnb"], new_entry["shipcountry"], new_entry["sellercountry"], new_entry["last_scraped"]
            ]
        else:
            df_new_entry = pd.DataFrame([new_entry], columns=df_cache.columns)
            df_cache = pd.concat([df_cache, df_new_entry], ignore_index=True)

        logging.debug(f"Cache mis à jour pour le vendeur '{seller_name}'.")
    else:
        logging.debug(f"Scraping échoué pour le vendeur '{seller_name}'. Aucune mise à jour du cache.")

    return seller_info, df_cache

def scrape_main_page(data_json, idsmartphone):
    """Scrape les offres depuis la page principale."""
    offers = data_json.get("offers", {}).get("offers", [])
    product_url = data_json.get("url", pd.NA)
    timestamp = datetime.now().strftime("%Y/%m/%d %H:%M")

    logging.debug(f"{len(offers)} offres trouvées sur la page principale pour le produit {idsmartphone}.")

    if not offers:
        logging.warning(f"Aucune offre trouvée sur la page principale pour le produit {idsmartphone}.")

    processed_offers = []

    for offer in offers:
        seller_info = offer.get("seller", {})
        seller_name = seller_info.get("name", pd.NA)
        row_data = {
            "pfid": "RAK",
            "idsmartphone": idsmartphone,
            "url": product_url,
            "timestamp": timestamp,
            "price": offer.get("price", pd.NA),
            "shipcost": offer.get("shippingDetails", {}).get("shippingRate", {}).get("value", pd.NA),
            "rating": pd.NA,
            "ratingnb": pd.NA,
            "offertype": offer.get("itemCondition", pd.NA),
            "offerdetails": pd.NA,
            "shipcountry": pd.NA,
            "sellercountry": pd.NA,
            "seller": seller_name
        }
        processed_offers.append(row_data)
        logging.debug(f"Offre traitée pour {idsmartphone} : {row_data}")

    logging.debug(f"{len(processed_offers)} offres traitées sur la page principale pour {idsmartphone}.")
    return processed_offers

# -----------------------------------------------------------------------------
# Fonction principale du script de scraping
# -----------------------------------------------------------------------------
def main():
    """Fonction principale du script de scraping."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": get_random_user_agent(),
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Referer": "https://fr.shopping.rakuten.com/"
    })

    # Charger et nettoyer le cache des vendeurs
    df_seller_cache = load_seller_cache()
    df_seller_cache = clean_seller_cache(df_seller_cache)

    while True:
        start_time = time.time()
        df_links = load_excel_data()
        num_telephones = len(df_links)
        logging.debug(f"{num_telephones} téléphones à scrapper.")

        if num_telephones == 0:
            logging.warning("Aucun téléphone à scrapper. Attente avant le prochain cycle.")
            time.sleep(INTERVAL)
            continue

        request_interval = INTERVAL / num_telephones
        logging.debug(f"Intervalle entre chaque requête principale : {request_interval:.2f} secondes.")

        for index, row in df_links.iterrows():
            idsmartphone = row.iloc[0]
            url = str(row.iloc[1])

            if "rakuten" not in url.lower():
                logging.warning(f"Lien invalide détecté ({url}), redémarrage de la liste.")
                break

            pid, cid = extract_pid_cid(url)
            if pd.isna(pid) or pd.isna(cid):
                logging.warning(f"PID ou CID manquant pour l'URL {url}. Skipping.")
                continue

            scrape_start_time = time.time()
            logging.debug(f"Début du scraping pour ID {idsmartphone} à l'URL {url}.")

            try:
                response = session.get(url, timeout=10)
                logging.debug(f"Requête envoyée à {url} avec le statut {response.status_code}.")

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    script_tag = soup.find("script", {"type": "application/ld+json", "id": "ggrc", "data-qa": "md_product"})

                    if script_tag:
                        try:
                            data_json = json.loads(script_tag.string)
                            logging.debug(f"Données JSON parsées pour {url}.")

                            main_offers = scrape_main_page(data_json, idsmartphone)
                            total_offers = len(main_offers)
                            logging.debug(f"Nombre total d'offres trouvées : {total_offers}")
                        except json.JSONDecodeError as je:
                            logging.error(f"Erreur de décodage JSON pour {url} : {je}")
                            main_offers = []
                            total_offers = 0

                        if total_offers > 0:
                            sellers_processed = {}

                            for offer in main_offers:
                                seller_name = offer.get("seller", pd.NA)
                                if not pd.isna(seller_name):
                                    if seller_name not in sellers_processed:
                                        seller_info, df_seller_cache = get_seller_info(seller_name, session, df_seller_cache)
                                        sellers_processed[seller_name] = seller_info
                                        logging.debug(f"Informations mises à jour pour le vendeur '{seller_name}': {seller_info}")

                                    seller_info = sellers_processed[seller_name]
                                    offer["rating"] = seller_info.get("rating", pd.NA)
                                    offer["ratingnb"] = seller_info.get("ratingnb", pd.NA)
                                    offer["shipcountry"] = seller_info.get("shipcountry", pd.NA)
                                    offer["sellercountry"] = seller_info.get("sellercountry", pd.NA)
                                else:
                                    logging.debug(f"Aucun vendeur valide pour l'offre: {offer}")

                            if main_offers:
                                save_to_parquet(main_offers)
                                logging.info(f"Données sauvegardées pour {idsmartphone} avec {len(main_offers)} offres")

                    else:
                        logging.warning(f"Balise script JSON non trouvée pour {url}")
                else:
                    logging.error(f"Échec de la requête principale pour {url}. Status: {response.status_code}")

            except Exception as e:
                logging.error(f"Erreur lors du traitement de {url}: {str(e)}")

            scrape_end_time = time.time()
            elapsed_time = scrape_end_time - scrape_start_time
            additional_delay = random.uniform(10, 20)
            sleep_time = max(0, request_interval - elapsed_time) + additional_delay
            logging.debug(f"Temps de scraping: {elapsed_time:.2f}s. Pause de {sleep_time:.2f}s avant la suite.")
            time.sleep(sleep_time)

        # Nettoyer et sauvegarder le cache des vendeurs
        df_seller_cache = clean_seller_cache(df_seller_cache)
        save_seller_cache(df_seller_cache)

        total_cycle_time = time.time() - start_time
        logging.debug(f"Temps total du cycle : {total_cycle_time:.2f} secondes")

        if total_cycle_time < INTERVAL:
            remaining_time = INTERVAL - total_cycle_time
            logging.info(f"Cycle terminé, attente de {remaining_time:.2f}s avant le prochain cycle")
            time.sleep(remaining_time)
        else:
            logging.info("Cycle terminé, démarrage immédiat du prochain cycle")

if __name__ == "__main__":
    main()
