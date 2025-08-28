import re
import requests
import csv
import os, sys
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import subprocess
import random
from base64 import b64decode
import hashlib

URL = "https://www.cdiscount.com/"
API_KEY = "48769c3dfb7194a2639f7f5627378bad"
SITE_KEY = "f6af350b-e1f0-4be9-847a-de731e69489a"

HTML_SELECTORS = {
    "accept_condition": "footer_tc_privacy_button_2",
    "search_bar": "sc-1drly4b-0 aXCZZ sc-1hfr0re-0 cjoXKP sc-1qy5nu7-9 bxlOqQ searchBar",
    "first_product": "alt-h4.u-line-clamp--2",
    "first_product_name": "h2 u-truncate",
    "first_product_price": "c-price c-price--promo c-price--xs",
    "first_product_seller": "a[aria-controls='SellerLayer']",
    "more_offers_link": ["offres neuves", "offres d'occasion"],
    "seller_name": "slrName",
    "seller_status": "u-ml-sm",
    "seller_rating": "c-stars-rating__note",
    "seller_rating_number": "c-stars-rating__label",
    "seller_sales_number": "u-text--body-small",
    "get_price": "c-price c-price--xl c-price--promo",
    "delivery_fee": "priceColor",
    "delivery_date": "//*[@id='fpmContent']/div/div[3]/table/tbody/tr[4]/td[2]/span"
    }

def start_xvfb():
    xvfb_process = subprocess.Popen(
        ["Xvfb", ":104", "-screen", "0", "1920x1080x24"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    os.environ["DISPLAY"] = ":104"
    return xvfb_process

def solve_altcha_captcha(driver):
    print("Detected ALTCHA CAPTCHA, solving...")
    
    # Extract challenge parameters
    challenge_url = driver.execute_script("""
        return document.querySelector('altcha-widget').getAttribute('challengeurl')
    """)
    captcha_token = re.search(r'captcha_token=([^&]+)', challenge_url).group(1)
    
    # Get challenge data
    challenge_data = requests.get(
        f"https://www.cdiscount.com/.well-known/baleen/captcha/generate?captcha_token={captcha_token}"
    ).json()
    
    # Solve proof-of-work challenge
    # This requires finding a nonce that satisfies: 
    # hash(nonce + salt) starts with specified number of zeros
    algorithm = challenge_data["algorithm"]
    salt = b64decode(challenge_data["salt"]).hex()
    challenge = challenge_data["challenge"]
    max_number = challenge_data["maxNumber"]
    signature = ""
    nonce = 0
    
    while nonce < max_number:
        test = f"{nonce}{salt}".encode()
        hash_result = hashlib.sha256(test).hexdigest()
        if hash_result.startswith("0" * challenge_data["complexity"]):
            signature = hash_result
            break
        nonce += 1
    
    if not signature:
        raise Exception("Failed to solve ALTCHA challenge")
    
    # Submit solution
    driver.execute_script(f"""
        document.querySelector('altcha-widget').setAttribute('signature', '{signature}')
        document.querySelector('altcha-widget').setAttribute('nonce', '{nonce}')
        document.querySelector('#altcha').dispatchEvent(new CustomEvent('statechange', {{
            detail: {{
                state: 'verified',
                payload: JSON.stringify({{
                    algorithm: "{algorithm}",
                    challenge: "{challenge}",
                    nonce: "{nonce}",
                    signature: "{signature}"
                }})
            }}
        }}))
    """)
    
    print("CAPTCHA solution submitted")
    time.sleep(3)  # Wait for redirect


def get_hcaptcha_solution():
    response = requests.post("https://2captcha.com/in.php",
            {'key': API_KEY, 'method': 'hcaptcha', 'sitekey': SITE_KEY, 'pageurl': URL})
    captcha_id = response.text.split('|')[1]
    time.sleep(20)
    response = requests.get(f'https://2captcha.com/res.php?key={API_KEY}&action=get&id=({captcha_id}')
    while 'CAPTCHA_NOT_READY' in response.text:
        time.sleep(5)
        response = requests.get(f'https://2captcha.com/res.php?key={API_KEY}&action=get&id=({captcha_id}')
    return response.text.split('|')[1]

def solve_captcha_if_present(driver):
    print("------------------solve_captcha_if_present--------------------")
    try:
        time.sleep(10)  # Wait for the page to load
        captcha_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-sitekey]')))
        if captcha_element:
            print("CAPTCHA detected. Solving...")
            captcha_solution = get_hcaptcha_solution()
            captcha_input = driver.find_element(By.ID, "h-captcha-response")
            driver.execute_script("arguments[0].value = arguments[1];", captcha_input, captcha_solution)

            submit_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
            driver.execute_script("arguments[0].click();", submit_button)

            WebDriverWait(driver, 10).until_not(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-sitekey]')))
            print("CAPTCHA solved.")
        else:
            print("No CAPTCHA found.")
    except TimeoutException:
        print("No CAPTCHA detected, continuing with the next step.")
    except Exception as e:
        print(f"Error solving CAPTCHA: {e}")

def accept_condition(driver):
    print("------------------accept_condition--------------------")
    try:
        driver.get(URL)
        solve_captcha_if_present(driver)
        accept_button = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, HTML_SELECTORS["accept_condition"])))
        driver.execute_script("arguments[0].scrollIntoView(true);", accept_button)
        accept_button.click()
        print("Conditions accepted.")
    except TimeoutException:
        print("Condition acceptance button not found.")

def search_product(driver, search_query):
    print("------------------search_product--------------------")
    try:
        print("Searching for product:", search_query)
        # Wait for search bar to be clickable
        search_bar = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Qu’est-ce qui vous ferait plaisir ?']"))
        )
        print("Search bar found")
        
        # Scroll into view and focus
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", search_bar)
        time.sleep(0.5)
        
        # Clear existing text efficiently
        driver.execute_script("arguments[0].value = '';", search_bar)
        time.sleep(0.3)
        
        # Type search query
        print("Entering search query:", search_query)
        search_bar.send_keys(search_query)
        time.sleep(0.5)
        print("Search query entered")
        
        # Submit search
        # Click on the search icon instead of pressing Enter
        search_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[@id='stickyHeader']/div/div[2]/div/button")  # Adjusted XPath to match the search button
            )
        )
        driver.execute_script("arguments[0].click();", search_button)
        print("Search submitted via button")

        # Sometimes the search doesn't load properly, so we just manually go to the search results page
        driver.get(f"https://www.cdiscount.com/search/10/{search_query}.html")

        time.sleep(10)  # Wait for the search results to load
        # Save the html source for debugging
        with open("cdiscount_search_result.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        # print the current URL with the search query
        
    except Exception as e:
        print(f"Error in searching product: {e}")
        # Fallback: Use JavaScript to submit search
        try:
            driver.execute_script(f"""
                document.querySelector("input[placeholder='Qu’est-ce qui vous ferait plaisir ?']").value = '{search_query}';
            """)
            search_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[@id='stickyHeader']/div/div[2]/div/button")  # Adjusted XPath to match the search button
            )
            )
            driver.execute_script("arguments[0].click();", search_button)

            print("Used JavaScript fallback for search")
        except Exception as js_error:
            print(f"JavaScript fallback failed: {js_error}")

def filter_products_apple(driver, dont_stop=False):
    print("------------------filter_products--------------------")
    try:
        # Attendre que la page se charge
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))


    except Exception as e:
        print(f"Error in waiting for page to load: {e}")
        raise
    
    try:

        # Appuyer sur le filtre "Apple", pas besoin de cliquer sur un bouton "+ de choix"
        
        print("Applying filter for 'Apple'...")
        # We find the list of checkboxes and look for the one with value "Marque/apple"
        apple_checkbox = None
        for i in range(1, 10):  # On essaie de trouver la checkbox dans les indices 1 à 9
            marque_checkboxes = driver.find_elements(By.NAME, f"FacetForm.SelectedFacets[{i}]")
            for cbox in marque_checkboxes:
                cbox_v = cbox.get_attribute("value")
                if cbox_v == "Marque/apple":
                    apple_checkbox = cbox
                    break
        if not apple_checkbox:
            raise ValueError("Filtre 'Apple' non disponible.")

        print("Apple checkbox found, scrolling into view...")

        driver.execute_script("arguments[0].scrollIntoView(true);", apple_checkbox)

        print("Clicking on Apple checkbox...")
        # force‐click via JS to avoid “element not interactable”
        driver.execute_script("arguments[0].click();", apple_checkbox)

        print("Filtre 'Apple' appliqué.")
    except Exception as e:
        print(f"Error in filtering products with filter 'Apple': {e}")

    try:
        # Attendre un temps aléatoire entre 1 et 5 secondes
        time.sleep(1 + 4 * random.random())

        # Appuyer sur le filtre "Noir"
        noir_checkbox = None
        for i in range(1, 15):  # On essaie de trouver la checkbox dans les indices 1 à 14
            couleurs_checkboxes = driver.find_elements(By.NAME, f"FacetForm.SelectedFacets[{i}]")
            print(f"Searching for 'Noir' checkbox in FacetForm.SelectedFacets[{i}]...")
            print(f"Found {len(couleurs_checkboxes)} checkboxes in this facet.")
            k=0
            for cbox in couleurs_checkboxes:
                cbox_v = cbox.get_attribute("value")
                if cbox_v == "Couleur/noir":
                    noir_checkbox = cbox
                    print("Found 'Noir' checkbox")
                    break
                if k > 10:
                    print("Too many checkboxes found, breaking the loop to avoid performance issues.")
                    break
                k+=1

        if not noir_checkbox:
            # Raise an exception if the Noir checkbox is not found
            raise ValueError("Filtre 'Noir' non disponible.")

        print("Applying filter for 'Noir'...")
        driver.execute_script("arguments[0].scrollIntoView(true);", noir_checkbox)
        print("Clicking on Noir checkbox...")
        driver.execute_script("arguments[0].click();", noir_checkbox)
        print("Filtre 'Noir' appliqué.")

    except Exception as e:
        print(f"Error in filtering products with filter 'Noir': {e}")
        if not dont_stop:
            raise

def filter_products_samsung(driver, dont_stop=False):
    print("------------------filter_products--------------------")
    try:
        # Attendre que la page se charge
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))
    except Exception as e:
        print(f"Error in waiting for page to load: {e}")
        raise
    
    try:
        # On appuie sur le bouton spécifiant qu'on cherche dans la catégorie "Smartphone"
        # <label class=" ">        Smartphone <span class="pCount">(3 947)</span><input type="checkbox" name="NavigationForm.SelectedNavigationPath" value="categorycodepath/07|0703|070302" class="initialized"></label>

        print("Applying filter for 'Smartphone' category...")

        smartphone_checkbox = None
        marque_checkboxes = driver.find_elements(By.NAME, "NavigationForm.SelectedNavigationPath")
        for cbox in marque_checkboxes:
            cbox_v = cbox.get_attribute("value")
            if cbox_v == "categorycodepath/07|0703|070302":
                smartphone_checkbox = cbox
                break
        if not smartphone_checkbox:
            raise ValueError("Filtre 'Smartphone' non disponible.")
    
    except Exception as e:
        print(f"Error in filtering products with filter 'Smartphone': {e}")
        if not dont_stop:
            raise

    try:

        time.sleep(3 + 4 * random.random())

        # Appuyer sur le filtre "Samsung"

        print("Applying filter for 'Samsung'...")
        # We find the list of checkboxes and look for the one with value "Marque/samsung"
        samsung_checkbox = None
        for i in range(1, 15):  # On essaie de trouver la checkbox dans les indices 1 à 9
            marque_checkboxes = driver.find_elements(By.NAME, f"FacetForm.SelectedFacets[{i}]")
            for cbox in marque_checkboxes:
                cbox_v = cbox.get_attribute("value")
                if cbox_v == "Marque/samsung":
                    samsung_checkbox = cbox
                    break
        if not samsung_checkbox:
            raise ValueError("Filtre 'Samsung' non disponible.")

        print("Samsung checkbox found, scrolling into view...")

        driver.execute_script("arguments[0].scrollIntoView(true);", samsung_checkbox)

        print("Clicking on Samsung checkbox...")
        # force‐click via JS to avoid “element not interactable”
        driver.execute_script("arguments[0].click();", samsung_checkbox)

        print("Filtre 'Samsung' appliqué.")
    except Exception as e:
        print(f"Error in filtering products with filter 'Samsung': {e}")
        raise

    # Spécifique à samsung, on précise qu'il est compatible 5g pour enlever les vieux modèles ou bas de gamme

    #try:
    #    # Attendre un temps aléatoire entre 1 et 5 secondes
    #    time.sleep(1 + 4 * random.random())
    #    # Appuyer sur le filtre "5g"
    #    cinq_g_checkbox = None
    #    for i in range(1, 10):  # On essaie de trouver la checkbox dans les indices 1 à 9
    #        couleurs_checkboxes = driver.find_elements(By.NAME, f"FacetForm.SelectedFacets[{i}]")
    #        print(f"Searching for '5g' checkbox in FacetForm.SelectedFacets[{i}]...")
    #        print(f"Found {len(couleurs_checkboxes)} checkboxes in this facet.")
    #        k=0
    #        for cbox in couleurs_checkboxes:
    #            cbox_v = cbox.get_attribute("value")
    #            if cbox_v == "Réseau compatible/5g":
    #                cinq_g_checkbox = cbox
    #                print("Found '5g' checkbox")
    #                break
    #            if k > 10:
    #                print("Too many checkboxes found, breaking the loop to avoid performance issues.")
    #                break
    #            k+=1
    #    if not cinq_g_checkbox:
    #        # Raise an exception if the 5g checkbox is not found
    #        raise ValueError("Filtre '5g' non disponible.")
    #    print("Applying filter for '5g'...")
    #    driver.execute_script("arguments[0].scrollIntoView(true);", cinq_g_checkbox)
    #    print("Clicking on 5g checkbox...")
    #    driver.execute_script("arguments[0].click();", cinq_g_checkbox)
    #    print("Filtre '5g' appliqué.")
#
    #except Exception as e:
    #    print(f"Error in filtering products with filter '5g': {e}")
    #    if not dont_stop: #for debugging
    #        raise

    try:
        # Attendre un temps aléatoire entre 3 et 7 secondes
        time.sleep(3 + 4 * random.random())

        # Appuyer sur le filtre "Gris"
        gris_checkbox = None
        for i in range(0, 15):  # On essaie de trouver la checkbox dans les indices 1 à 9
            couleurs_checkboxes = driver.find_elements(By.NAME, f"FacetForm.SelectedFacets[{i}]")
            k=0
            print(f"Now looking at checkbox {couleurs_checkboxes}")
            for cbox in couleurs_checkboxes:
                cbox_v = cbox.get_attribute("value")
                print(f"Current checkbox value: {cbox_v}")
                if cbox_v == "Couleur/gris":
                    gris_checkbox = cbox
                    print("Found 'Gris' checkbox")
                    break
                if k > 10:
                    print("Too many checkboxes found, breaking the loop to avoid performance issues.")
                    break
                k+=1

        # Saves the html for debugging with timestamp
        #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        #filename = f"cdiscount_filter_samsung_{timestamp}.html"
        #with open(filename, "w", encoding="utf-8") as f:
        #    f.write(driver.page_source)
        #print(f"Filter page HTML saved: {filename}")

        if not gris_checkbox:
            # Raise an exception if the Gris checkbox is not found
            raise ValueError("Filtre 'Gris' non disponible.")
		
        print("Applying filter for 'Gris'...")
        driver.execute_script("arguments[0].scrollIntoView(true);", gris_checkbox)
        print("Clicking on Gris checkbox...")
        driver.execute_script("arguments[0].click();", gris_checkbox)
        print("Filtre 'Gris' appliqué.")
        

    except Exception as e:
        print(f"Error in filtering products with filter 'Gris': {e}")
        #if not dont_stop:
        #    raise
    

def get_products_url(driver, marque):
    print("------------------get_products_url--------------------")

    # On cherche ici à récupérer tout les liens de produits sur chaque page de résultats

    keep_going = True
    product_urls = []
    good_product_urls = []
    page_number = 1
    try:
        while keep_going:
            print(f"Récupération des liens de produits pour la page {page_number}...")
            # Attendre la présence d'au moins un lien produit

            # Il faut vérifier que les éléments sont dans l'élément :
            # ul id=lpBlocInline class="l-card-list jslpInline u-mt-md" data-cs-override-id="Offres-LR"
            product_list = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'lpBlocInline'))
            )
            product_links = product_list.find_elements(By.CSS_SELECTOR, 'a.o-card__link')

            print(f"Nombre de produits trouvés : {len(product_links)}")

            # On rajoute les liens des produits à la liste

            product_urls.extend([link.get_attribute("href") for link in product_links])
            print(f"Nombre de produits dans la liste complète : {len(product_urls)}")

            # On essaie de chercher le bouton correspondant à la page {page_number + 1}

            # <ul id="PaginationForm_ul" name="PaginationForm.ul">
            #<li><a href="#" class="current">1</a></li><li>
            # <a href="#">2</a></li>
            # <li><a href="#">3</a></li>
            # <li><a href="#">4</a></li>
            # <li><a href="#">5</a></li>
            # <li><a href="#">6</a></li>
            # <li><a href="#">7</a></li></ul>
            try:
                next_page_link = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, f"//ul[@id='PaginationForm_ul']/li/a[text()='{page_number + 1}']"))
                )
                print(f"Page suivante trouvée...")
                # On scroll jusqu'au lien de la page suivante
                driver.execute_script("arguments[0].scrollIntoView(true);", next_page_link)
                # On clique sur le lien de la page suivante
                driver.execute_script("arguments[0].click();", next_page_link)
                time.sleep(5)  # Attendre que la page se charge
                page_number += 1
            except TimeoutException:
                print(f"Pas de page suivante trouvée pour la page {page_number}, on arrête la récupération des liens.")
                keep_going = False

        
        print("Il y a " + str(len(product_urls)) + " produits trouvés, on va les filtrer.")
        print("Marque recherchée :", marque)
        if marque == "apple":
            for product_url in product_urls:
                # On vérifie que le lien correspond à un modèle cherché et de la taille de stockage voulue
                # les liens doivent correspondre au regex suivant :
                # ^https:\/\/www\.cdiscount\.com\/telephonie\/telephone-mobile\/(apple-)?iphone-(14|15|16|16e)-(plus-|pro-|pro-max-)?(128gb|256gb|512gb|1tb)-(black|midnight)-.*$
                # Si le lien correspond au regex :
                if re.match(r"^https:\/\/www\.cdiscount\.com\/telephonie\/telephone-mobile\/(apple-)?iphone-(14|15|16|16e)-(plus-|pro-|pro-max-)?(128gb|256gb|512gb|1tb)-(noir|black|midnight|space-black)(?:[-/].*)?$", product_url):
                    print(f"➡️ Produit trouvé : {product_url}")
                    good_product_urls.append(product_url)
                else:
                    print(f"❌ Produit non valide : {product_url}, on le supprime de la liste.")
            return good_product_urls
        elif marque == "galaxy":
            for product_url in product_urls:
                if re.match(r"^https:\/\/www\.cdiscount\.com\/telephonie\/telephone-mobile\/(samsung-)?galaxy-(s23|s24|s25)(?:[-/].*)?$", product_url):
                    print(f"➡️ Produit trouvé : {product_url}")
                    good_product_urls.append(product_url)
                else:
                    print(f"❌ Produit non valide : {product_url}, on le supprime de la liste.")
            return good_product_urls
        else:
            print("ERREUR, marque non reconnue :", marque)
    except Exception as e:
        print(f"Error in retrieving product URLs: {e}")
        return None
    
def get_product_seller(soup):
    # Cas 1 : vendeur tiers (lien)
    seller_link = soup.select_one("a[aria-controls='SellerLayer']")
    if seller_link:
        return seller_link.get_text(strip=True)

    # Cas 2 : vendu par Cdiscount (logo)
    cdiscount_span = soup.find("span", class_="o-logoCDS")
    if cdiscount_span:
        return "Cdiscount"

    # Si aucun trouvé
    return "N/A"

def scrape_product_details(driver, product_url):
    print("------------------scrape_product_details--------------------")
    try:
        driver.get(product_url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "p")))

        soup = BeautifulSoup(driver.page_source, 'lxml')

        product_name_element = soup.find('div', class_=HTML_SELECTORS["first_product_name"])
        product_name = product_name_element.get_text(strip=True) if product_name_element else "N/A"
        print("Product Name:", product_name)

        # Si il y a "Tiroir Carte" dans le nom du produit, on ignore le produit
        if "Tiroir Carte" in product_name:
            return None

        product_price_element = soup.find('span', class_=HTML_SELECTORS["first_product_price"])
        product_price = product_price_element.get_text(strip=True) if product_price_element else "N/A"

        product_seller = get_product_seller(soup)
        if product_seller == "Cdiscount":
            seller_status = "Vendu par Cdiscount"  # Valeur par défaut
        else:
            seller_status = "N/A"
        seller_rating = "N/A"  # Valeur par défaut
        seller_rating_number = "N/A"  # Valeur par défaut
        seller_sales_number = "N/A"  # Valeur par défaut
        delivery_fee = "Livraison à domicile ou en point retrait"  # Valeur par défaut
        return {"Platform": "Cdiscount", "name": product_name, "price": product_price, "seller": product_seller, "seller_status":seller_status, "seller_rating":seller_rating, "seller_rating_number": seller_rating_number, "seller_sales_number": seller_sales_number, "delivery_fee":delivery_fee, "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
    except Exception as e:
        print(f"Error scraping product details: {e}")
        return None

def get_more_offers_page(driver):
    print("------------------get_more_offers_page--------------------")
    try:
        more_offers_link = None
        for offer_text in HTML_SELECTORS["more_offers_link"]:
            try:
                more_offers_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, offer_text))
                )
                if more_offers_link:
                    break
            except TimeoutException:
                print(f"Link with text {offer_text} not found.")
                continue

        if more_offers_link:
            print("Found 'More Offers' link, scrolling...")
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", more_offers_link)
            time.sleep(1) 
            driver.execute_script("arguments[0].scrollIntoView(true);", more_offers_link)
            time.sleep(2)
            driver.execute_script("arguments[0].click();", more_offers_link)
            print("Clicked on 'More Offers' link.")
            time.sleep(5)
            return driver.current_url
        else:
            print("No more offers link found.")
            return None
    except Exception as e:
        print(f"Error in getting more offers page: {e}")
        return None

def submit_next_page(driver):
    # 1) Read current and total page count
    current = int(driver.find_element(By.ID, "PaginationCurrentPageNumber")
                     .get_attribute("value"))
    print("Current page number:", current)
    total   = int(driver.find_element(By.NAME, "Pagination.TotalPageCount")
                     .get_attribute("value"))
    print("Total page count:", total)
    if current < total:
        next_page = current + 1
        print(f"Submitting next page: {next_page}")
        
        try: 
            # Obligatoire de faire comme ça sinon la page ne s'actualise pas
            driver.execute_script("""
                document.getElementById('PaginationCurrentPageNumber').value = arguments[0];
            """, next_page)  # :contentReference[oaicite:2]{index=2}
            next_btn = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR,
                     "#OfferListPaginationForm a[data-page].mpNext")
                )
            )
            driver.execute_script("arguments[0].click();", next_btn)

        except Exception as e:
            print(f"Error submitting next page: {e}")
            return False
        new_pagination = driver.find_element(By.ID, "PaginationCurrentPageNumber").get_attribute("value")
        print(f"The value of PaginationCurrentPageNumber is now set to {new_pagination}.")
        print(f"Next page {next_page} submitted successfully.")
        print("Waiting for the page to load...")
        time.sleep(15)  # Wait for the page to load

        return True
    else:
        print("Already on the last page, no next page to submit.")
        raise Exception("Already on the last page, no next page to submit.")

def fetch_data_from_pages(driver, url, html_selector, data_type):
    if not url:
        print(f"No valid URL for fetching {data_type}.")
        return []

    fetched_data = []
    go_to_next_page = False
    
    while url:
        try:
            if not go_to_next_page:
                driver.get(url)
            else:
                new_pagination = driver.find_element(By.ID, "PaginationCurrentPageNumber").get_attribute("value")
                print(f"The value of PaginationCurrentPageNumber is now set to {new_pagination}.")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "p")))

            soup = BeautifulSoup(driver.page_source, 'lxml')

            if data_type == 'sellers':
                sellers = soup.find_all('a', class_=HTML_SELECTORS[html_selector])
                seller_statuses = soup.find_all('span', class_=HTML_SELECTORS["seller_status"])
                delivery_fee = soup.find_all('span', class_=HTML_SELECTORS["delivery_fee"])
                ratings_elements = soup.find_all('span', class_=HTML_SELECTORS["seller_rating"])
                seller_rating_numbers = soup.find_all('span', class_=HTML_SELECTORS["seller_rating_number"])
                if seller_rating_numbers:
                    seller_rating_numbers = [rating.get_text(strip=True).split()[0] for rating in seller_rating_numbers]
                print("Seller Rating Number Found after cleaning:", seller_rating_numbers)

                seller_sales_numbers = soup.find_all('p', class_=HTML_SELECTORS["seller_sales_number"])
                print("Seller Sales Number Found before cleaning:", seller_sales_numbers)
                if seller_sales_numbers:
                    seller_sales_numbers = [sales.get_text(strip=True).split()[-1] for sales in seller_sales_numbers]
                # Il faut encore enlever le : devant le chiffre, actuellement c'est encore ":3998"
                seller_sales_numbers = [sales.replace(":", "") for sales in seller_sales_numbers] # type: ignore
                print("Seller Sales Number Found after cleaning:", seller_sales_numbers)

                # On vérifie que ratings_elements ne contient pas la note du produit en lui même
                # On cherche si il y a un rating dans le span avec class=c-product-reviews__item
                product_rating_info = soup.find('span', class_='c-product-reviews__item')

                #On récupère ensuite la note précise dont la class est c-stars-rating__note dans product_rating_info
                if product_rating_info:
                    product_rating = product_rating_info.find('span', class_='c-stars-rating__note') 
                    print("Product rating found:", product_rating.get_text(strip=True))
                    # Si il y a un rating pour le produit, on enlève le premier élément de ratings_elements
                    if ratings_elements and ratings_elements[0].get_text(strip=True) == product_rating.get_text(strip=True):
                        print("On pop " + ratings_elements[0].get_text(strip=True) + " from ratings_elements")
                        ratings_elements.pop(0)
                    product_rating_number = product_rating_info.find('span', class_='c-stars-rating__label')
                    product_rating_number = product_rating_number.get_text(strip=True).split()[0] if product_rating_number else "N/A"
                    
                    # Même chose pour le nombre d'avis, on enlève le premier élément de seller_rating_numbers
                    if seller_rating_numbers and seller_rating_numbers[0] == product_rating_number:
                        print("On pop " + seller_rating_numbers[0] + " from seller_rating_numbers")
                        seller_rating_numbers.pop(0)

                

                cds_logo_imgs = soup.find_all('img', alt='Cdiscount')
                for img in cds_logo_imgs:
                    parent = img.find_parent()
                    if parent:
                        sellers.append(parent)  # On ajoute le parent comme "bloc vendeur" générique

                print("Sellers Found:", [seller.get_text(strip=True) for seller in sellers])
                print("Seller Statuses Found:", [status.get_text(strip=True) for status in seller_statuses])
                
                seller_ratings = [rating.get_text(strip=True) for rating in ratings_elements]
                print("Seller Ratings Found:", seller_ratings)
                print("Number of seller ratings Found:", seller_rating_numbers)
                print("Delivery Fees Found:", [fee.get_text(strip=True) for fee in delivery_fee])

                for i in range(len(sellers)):
                    seller_block = sellers[i]
    
                    # 🛠️ Cas spécial : Cdiscount avec logo
                    if seller_block.find('img', alt='Cdiscount'):
                        seller_name = "Cdiscount"
                        seller_status = "Vendu par Cdiscount"
                        seller_rating = "N/A"  # Pas de rating pour Cdiscount
                        seller_sales_number = "N/A"  # Pas de nombre d'avis pour Cdiscount
                    else:
                        seller_name = seller_block.get_text(strip=True)
                        seller_status = seller_statuses[i].get_text(strip=True) if i < len(seller_statuses) else "N/A"
                        seller_sales_number = seller_sales_numbers[i] if i < len(seller_sales_numbers) else "N/A"
                        
                        if seller_status == "NOUVEAU VENDEUR":
                            seller_rating = "N/A"  # Pas de rating pour les nouveaux vendeurs
                            seller_rating_number = "N/A"  # Pas de nombre d'avis pour les nouveaux vendeurs
                            # On rajoute dans la liste seller_rating "N/A" pour ce vendeur, en décalant les autres
                            if i < len(seller_ratings):
                                seller_ratings.insert(i, "N/A")
                                seller_rating_numbers.insert(i, "N/A")
                        else:
                            seller_rating = seller_ratings[i] if i < len(seller_ratings) else "N/A"
                            seller_rating_number = seller_rating_numbers[i] if i < len(seller_rating_numbers) else "N/A"
                            

                    
                    delivery_fee_text = delivery_fee[i].get_text(strip=True) if i < len(delivery_fee) else "N/A"
                    print(seller_name, seller_status, seller_rating, seller_rating_number, seller_sales_number, delivery_fee_text)
                    fetched_data.append((seller_name, seller_status, seller_rating, seller_rating_number, seller_sales_number, delivery_fee_text))

                print("Fetched Sellers Data:", fetched_data)

            else:
                delivery_fee = soup.find_all('span', class_=HTML_SELECTORS["delivery_fee"])
                elements = soup.find_all('p', class_=HTML_SELECTORS[html_selector])
                fetched_data.extend([elem.get_text(strip=True) for elem in elements])
                print("Fetched Prices Data:", fetched_data)


            try:
                # Check if next page link exists
                submit_next_page(driver)
                go_to_next_page = True
                time.sleep(3)  # Add short delay for stability
                
            except Exception as e:
                break  # No more pages


        except Exception as e:
            print(f"Error fetching {data_type}: {e}")
            break

    return fetched_data

def write_combined_data_to_csv(sellers, prices, product_data, csv_file="/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv", write_product_details=True, batch_id=0):    
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, "a", newline="") as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        if write_product_details:
            # add Batch ID column + seller details
            #writer.writerow([
            #    "Platform", "Product Name", "Price",
            #    "Seller", "Seller Status", "Seller Rating", "Delivery Fee",
            #    "Timestamp", "Batch ID"
            #])

            # Définition de l'état du produit
            if "reconditionné" in product_data["name"].lower():
                match = re.search(r"reconditionné - (.+?) état", product_data["name"].lower())
                product_state = match.group(1) if match else "Reconditionné"
            else:
                product_state = "Neuf"
            writer.writerow([
                product_data["Platform"],
                product_data["name"],
                product_data["price"],
                product_state, 
                product_data.get("seller", ""),        # ex: "Cdiscount"
                product_data.get("seller_status", ""), # ex: "N/A"
                product_data.get("seller_rating", ""), # ex: "N/A"
                product_data.get("seller_rating_number", ""), # ex: "N/A"
                product_data.get("seller_sales_number", ""),  # ex: "N/A"
                product_data.get("delivery_fee", ""),  # ex: "N/A"
                product_data["timestamp"],
                batch_id
            ])
            print("Product details (without additional offers) written to CSV.")

        if sellers and prices:
            min_length = min(len(sellers), len(prices))
            # writer.writerow(["Platform", "Product Name", "Price", "Seller", "Seller Status", "Seller Rating", "Delivery Fee", "Timestamp","Batch ID"])
            # Définition de l'état du produit
            if "reconditionné" in product_data["name"].lower():
                match = re.search(r"reconditionné - (.+?) état", product_data["name"].lower())
                product_state = match.group(1) if match else "Reconditionné"
                #We want to print "Reconditionné - état" in the CSV
                product_state = f"Reconditionné - {product_state}"
            else:
                product_state = "Neuf"
            for i in range(min_length):
                s = sellers[i]
                print("for product", product_data["name"], "found seller info :", s)
                writer.writerow([
                    "Cdiscount",
                    product_data["name"],
                    prices[i],
                    product_state,
                    s[0], s[1], s[2], s[3], s[4], s[5],
                    datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                    batch_id
                ])
        #writer.writerow(["-"*100])

    print(f"Data written to {csv_file}")

def main():
    # Initialize batch_id from last numeric entry in CSV
    csv_file = "/home/scraping/algo_scraping/CDISCOUNT/scraping_cdiscount.csv"
    batch_id = 0
    print("Initializing batch_id from CSV file...")
    if os.path.isfile(csv_file):
        try:
            with open(csv_file, "r", newline="") as f:
                lines = [line for line in f if line.strip()]
            for line in reversed(lines):
                parts = line.strip().split(";")
                last_col = parts[-1]
                if last_col.isdigit():
                    batch_id = int(last_col) + 1
                    print(f"Starting with batch_id: {batch_id}, found from CSV.")
                    break
        except:
            print("Error reading the CSV file, starting with batch_id 0.")
            pass
    else:
        print("CSV file does not exist, starting with batch_id 0.")
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                "Platform", "Product Name", "Price", "Product state",
                "Seller", "Seller Status", "Seller Rating",
                "Seller Rating Number", "Seller Sales Number",
                "Delivery Fee",
                "Timestamp", "Batch ID"
            ])
            print("CSV file created with header.")
        

    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # 🟢 Accepter une seule fois les conditions
        accept_condition(driver)
          
        # 🔐 Vérification du driver
        if not driver.session_id:
            print("Session invalide, on stoppe.")
            return
        
        ##########################################################################################################
        #####################################       APPLE IPHONES       ##########################################
        ##########################################################################################################
        print(f"Recherche de tout les iphones...")
        search_product(driver, "iphone")

        # Délai aléatoire entre 1 et 5 secondes
        time.sleep(1 + 4 * random.random())

        # Filtrer les produits
        print(f"Filtrage des produits... ")

        # On applique les filtres requis, cette fonction va raise une error si il n'y a pas de checkbox pour la couleur
        try:
            filter_products_apple(driver)
        except Exception as e:
            print(f"Erreur lors du filtrage des produits: {e}")
            raise


        # Délai aléatoire entre 1 et 5 secondes
        time.sleep(1 + 4 * random.random())


        product_urls_apple = get_products_url(driver, "apple")
        print(f"Nombre de produits trouvés pour apple : {len(product_urls_apple)}")

        ##########################################################################################################
        ##################################       Samsung Galaxy S       ##########################################
        ##########################################################################################################


        print(f"Recherche de tout les samsung...")
        search_product(driver, "samsung")

        # Délai aléatoire entre 1 et 5 secondes
        time.sleep(1 + 4 * random.random())

        # Filtrer les produits
        print(f"Filtrage des produits... ")

        # On applique les filtres requis, cette fonction va raise une error si il n'y a pas de checkbox pour la couleur
        try:
            filter_products_samsung(driver)
        except Exception as e:
            print(f"Erreur lors du filtrage des produits: {e}")
            raise


        # Délai aléatoire entre 1 et 5 secondes
        time.sleep(1 + 4 * random.random())


        product_urls_samsung = get_products_url(driver, "galaxy")
        nb_products = len(product_urls_samsung)
        print(f"Nombre de produits trouvés pour Samsung : {nb_products}")

        # On combine les deux listes de produits
        product_urls = product_urls_apple + product_urls_samsung
        total_products = len(product_urls)
        print(f"Nombre total de produits trouvés : {total_products}")
        # On mélange les produits pour éviter de toujours traiter les mêmes produits en premier
        random.shuffle(product_urls)

        k=0
        for product_url in product_urls :
            print(f"Now at product {k} / {len(product_urls)}")
            k+=1
            product_data = scrape_product_details(driver, product_url)
            other_offers_url = get_more_offers_page(driver)
            if other_offers_url:
                sellers = fetch_data_from_pages(driver, other_offers_url, 'seller_name', 'sellers')
                prices = fetch_data_from_pages(driver, other_offers_url, 'get_price', 'prices')
                write_combined_data_to_csv(sellers, prices, product_data, csv_file, write_product_details=False, batch_id=batch_id)
            else:
                write_combined_data_to_csv([], [], product_data, csv_file, write_product_details=True, batch_id=batch_id)

            time.sleep(random.uniform(2, 7))  # Délai aléatoire entre 1 et 3 secondes entre chaque produit 

        # end of one full cycle
        print("Cycle fini, pour le batch_id", batch_id)
        batch_id += 1
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()


if __name__ == "__main__":
    xvfb = start_xvfb()
    try:
        while True:
            main()
            print("Attente de 20 minutes avant le prochain cycle...")
            time.sleep(20 * 60)
    finally:
        xvfb.terminate()
        xvfb.wait()
