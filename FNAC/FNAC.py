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

Auteur : Vanessa KENNICHE SANOCKA, Thomas FERNANDES, Ambroise Arbey
Date : 09-12-2024
Version : 3.0

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
CSV_FILE = "fnac_offers.csv"
ZIP_FILE = "JSON_FNAC.zip"
SCRAPE_INTERVAL = 2 * 60 * 60  # 2 heures en secondes
MAX_RETRY = 2

# Charger les données depuis le fichier Excel
excel_data = pd.read_excel(EXCEL_FILE, sheet_name="FNAC", dtype={"idsmartphone": str})
links = excel_data["Link"].tolist()
phones = excel_data["Phone"].tolist()
idsmartphones = excel_data["idsmartphone"].tolist()

# Liste de User-Agents, pour éviter le blocage
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1"
]

def generate_headers(user_agent):
    return {
        "authority": "www.fnac.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "dnt": "1",
        "sec-ch-ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": user_agent,
        "referer": "https://www.fnac.com/",
        "cookie": "PFNAC=1;",  # Cookie minimal pour éviter les blocages
    }

# LOGGER
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# FUNCTIONS
def scrape_fnac_product_info(url, phone_name, idsmartphone, batch_id):
    retry_count = 0
    while retry_count < MAX_RETRY:
        try:
            user_agent = random.choice(user_agents)
            headers = generate_headers(user_agent)
            
            # Utiliser une session avec gestion avancée des cookies
            session = requests.Session()
            session.headers.update(headers)
            
            # Ajouter un délai aléatoire avant la requête
            delay = random.uniform(5, 15)
            logging.info(f"Attente de {delay:.2f}s avant la requête...")
            time.sleep(delay)


            response = session.get(url, timeout=10)

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

                    cur_time = datetime.now()
                    url_timestamp = cur_time.strftime('%Y%m%d_%H%M%S')
                    timestamp = cur_time.strftime("%d/%m/%Y %H:%M:%S")
                    json_filename = f'fnac_digitalData_{url_timestamp}.json'
                    with open(json_filename, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=4)

                    logging.info(f"Le fichier JSON '{json_filename}' a été créé avec succès.")
                    add_json_to_zip(json_filename)

                    # Extraire userRating
                    product_attributes = json_data['product'][0].get('attributes', {})
                    user_rating = product_attributes.get('userRating', pd.NA)

                    convert_offers_to_csv(json_data, timestamp, phone_name, idsmartphone, url, user_rating, session, batch_id)
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
    seller_sections = soup.find_all('div', class_='f-offerList__list')
    
    for section in seller_sections:
        seller_name = section.find('div', class_='f-offerList__sellerName').text.strip()

        print(f"Seller Name: {seller_name}")

        # Extract seller rating
        seller_rating = section.find('span', class_='f-rating__average').text.strip()
        print(f"Seller Rating: {seller_rating}")

        # Extract amount of reviews
        reviews_text = section.find('div', class_='f-offerList__sellerNameReview').text.strip()
        amount_of_reviews = reviews_text.split(' ')[0]  # Getting the number before 'avis'
        seller_ratings[normalize_string(seller_name)] = {
            "rating": seller_rating,
            "reviews": amount_of_reviews
        }

    return seller_ratings

def normalize_string(s):
    """
    Normalise une chaîne de caractères en supprimant les espaces et en mettant en minuscules.
    """
    return ''.join(s.lower().split())

def convert_offers_to_csv(json_data, timestamp, phone_name, idsmartphone, page_url, user_rating, session, batch_id):
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
            price = offer['price'].get('basePrice', pd.NA)
            seller_url = offer.get('offerURL', pd.NA)

            # seller_url renvoie la page du vendeur, on va donc aller dessus pour récuper toutes les informations
            # il faut d'abord vérifier que le vendeur n'est pas la FNAC, auquel cas on ne récupère pas d'info
            logging.info(f"Vérification du vendeur {seller_name} pour le téléphone {phone_name}...")
            # On récupère les 4 premiers caractères de du normalized_seller_name pour vérifier que ce n'est pas FNAC ou Fnac
            if normalized_seller_name.startswith("fnac") or normalized_seller_name.startswith("Fnac"):
                logging.info(f"Le vendeur de {phone_name} est {normalized_seller_name}, pas besoin de récupérer les infos du vendeur.")
                seller_rating = pd.NA
                rating_count = pd.NA
                ship_country = "France"
                sales_count = pd.NA
                pass
            else:
                # On va chercher les informations du vendeur sur sa page
                logging.info(f"Récupération des informations du vendeur {seller_name}...")
                try:
                    seller_response = session.get(seller_url, timeout=10)
                    logging.info(f"Page {seller_url} renvoie le code {seller_response.status_code}.")
                    seller_soup = BeautifulSoup(seller_response.text, 'html.parser')
                    logging.info(f"Page du vendeur {seller_name} chargée avec succès.")
                    
                    # On récupère le nom du vendeur
                    check_seller_name = seller_soup.find('h1', class_='f-sellerHeader__name').text.strip()
                    if check_seller_name != seller_name:
                        logging.warning(f"Le nom du vendeur récupéré ({check_seller_name}) ne correspond pas au nom attendu ({seller_name}).")

                    # On récupère la note du vendeur
                    # elle est dans une balise b avec la classe customerReviewsRating__score
                    seller_rating = seller_soup.find('b', class_='customerReviewsRating__score').text.strip()

                    # On récupère le nombre d'avis du vendeur
                    # Il faut d'abord récupérer la balise div dont la classe est customerReviewsRating__countTotal
                    rating_count_info = seller_soup.find('div', class_="customerReviewsRating__countTotal")
                    # Ensuite on récupère dedans le contenu de la balise span
                    if rating_count_info :
                        rating_count = rating_count_info.find('span').text.strip()
                        # On récupère uniquement le nombre qui est avant "avis"
                        rating_count = int(rating_count.split(' ')[0])
                    else:
                        rating_count = pd.NA

                    # On récupère aussi le nombre de ventes ainsi que le pays d'expédition
                    # On récupère déjà le balise div dont la classe est f-sellerHeader__properties
                    properties_div = seller_soup.find('div', class_='f-sellerHeader__properties')
                    if properties_div:
                        # Il doit y avoir 3 balises dl, avec dans chachune une balise dt et une balise dd
                        # La balise dt contient le nom de la propriété et la balise dd contient la valeur
                        properties = properties_div.find_all('dl')
                        for prop in properties:
                            dt = prop.find('dt')
                            dd = prop.find('dd')
                            if dt and dd:
                                if 'ventes' in dt.text:
                                    sales_count = dd.text.strip()
                                    logging.info(f"Nombre de ventes du vendeur récupéré : {sales_count}")
                                elif 'Pays d\'expédition' in dt.text:
                                    ship_country = dd.text.strip()
                                    logging.info(f"Pays d'expédition du vendeur récupéré : {ship_country}")
                                else:
                                    continue
                            else:
                                sales_count = pd.NA
                                ship_country = pd.NA
                except Exception as e:
                    logging.error(f"Erreur lors de la récupération des informations du vendeur {seller_name} : {e}")
                    seller_rating = pd.NA
                    rating_count = pd.NA
                    ship_country = pd.NA
                    sales_count = pd.NA
            
            
            # Si seller = "FNAC.COM", et si price finit pas .93, on rajoute 3.07 qui correspond à l'éco part DEEE qui n'es pas incluse de base
            if normalized_seller_name == "fnac.com" and isinstance(price, (int, float)) and str(price).endswith('.93'):
                price += 3.07

            offer_details = {
                "pfid": "FNAC",
                "idsmartphone": idsmartphone,  # Utilisation de 'idsmartphone' depuis Excel
                "url": page_url,
                "timestamp": timestamp,
                "Price": price,
                "shipcost": shipcost,
                "product_rating": user_rating,
                "seller": seller_name,
                "seller_rating": seller_rating if 'seller_rating' in locals() else pd.NA,
                "seller_sales_count": sales_count if 'sales_count' in locals() else pd.NA,
                "seller_rating_count": rating_count if 'rating_count' in locals() else pd.NA,
                "offertype": offer.get('condition', pd.NA),
                "offerdetails": pd.NA,  # Laisser vide pour le moment
                "shipcountry": ship_country, 
                "sellercountry": offer.get('sellerLocation', pd.NA),
                "descriptsmartphone": phone_name,
                "batch_id": batch_id
            }
            logging.info(f"Offre extraite : {offer_details}")
            offers_list.append(offer_details)

        if offers_list:
            offers_df = pd.DataFrame(offers_list)
            
            # Convertir les colonnes en types appropriés
            if 'ratingnb' in offers_df.columns:
                offers_df['ratingnb'] = offers_df['ratingnb'].astype('Int64')  # Nullable integer

            # Ajouter les nouvelles lignes au fichier CSV sans recharger tout le fichier
            if os.path.isfile(CSV_FILE):
                offers_df.to_csv(CSV_FILE, mode='a', header=False, index=False)
            else:
                offers_df.to_csv(CSV_FILE, mode='w', header=True, index=False)
            
            logging.info(f"Les données ont été ajoutées au fichier CSV '{CSV_FILE}' avec succès.")
    
    except Exception as e:
        logging.error(f"Erreur lors de la conversion en csv : {e}")

# MAIN
if __name__ == "__main__":
    batch_id = 0
    print("Initializing batch_id from CSV file...")
    if os.path.isfile(CSV_FILE):
        try:
            with open(CSV_FILE, "r", newline="") as f:
                lines = [line for line in f if line.strip()]
            # Scan backwards for the last numeric batch_id
            for line in reversed(lines):
                parts = line.strip().split(",")
                last_col = parts[-1]
                if last_col.isdigit():
                    batch_id = int(last_col) + 1
                    print(f"Starting with batch_id: {batch_id}, found from CSV.")
                    break
        except:
            print("Error reading the CSV file, starting with batch_id 0.")
            pass
    while True:
        logging.info(f"Début du cycle de scraping pour le batch {batch_id}...")
        try:
            num_links = len(links)
            if num_links == 0:
                logging.warning("Aucun lien trouvé dans le fichier Excel. Attente de 1 heure avant de réessayer.")
                time.sleep(SCRAPE_INTERVAL)
                continue

            # Calculer l'intervalle entre chaque requête pour répartir uniformément sur 1 heure
            interval_between_requests = SCRAPE_INTERVAL / num_links
            
            for i, link in enumerate(links):
                logging.info(f"Scraping du produit {i + 1}/{num_links} : {phones[i]} (link: {link})")
                scrape_fnac_product_info(link, phones[i], idsmartphones[i], batch_id)
                logging.info(f"Attente de {interval_between_requests:.2f} secondes avant la prochaine requête...")
                time.sleep(interval_between_requests)

            logging.info(f"Cycle complet terminé, reprise dans {SCRAPE_INTERVAL} secondes...")
            batch_id += 1
        except Exception as e:
            logging.error(f"Erreur dans le main : {e}")
            break
