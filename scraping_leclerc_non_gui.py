from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import csv
from datetime import datetime

HTML_SELECTORS = {
    "Product Name": ".product-content-title.clamp.clamp-2",
    "Price": ".price-unit.ng-star-inserted",
    "Cents": ".price-cents",
    "Currency": ".price-symbol",
    "Seller": "[class^=\"fw-500 mr-2 ng-tns-c183-\"]",
    "Delivery Fees": ".sue-text-green-dark.fw-500.text-uppercase.ng-star-inserted",
    "Delivery Date": ".date.ng-star-inserted",
    "Product State": "p[class^='mb-0 state-text fw-500 ng-tns-c183-']",
}

def fetch_html(url, html="page_content.html"):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(1920, 1080)
    driver.get(url)
    time.sleep(5)
    html_content = driver.page_source
    with open(html, 'w', encoding='utf-8') as file:
        file.write(html_content)
    print(f"HTML content saved to: {html}")

    driver.quit()
    return html_content

def get_sellers(soup):
    sellers = soup.select(HTML_SELECTORS["Seller"])
    leclerc_seller = soup.select_one(".shop-infos.fw-500.ng-tns-c183-2.ng-star-inserted")

    if leclerc_seller:
        leclerc_name = leclerc_seller.get_text(strip=True).replace("Vendeur : ", "")
        sellers.insert(0, leclerc_name)

    seller_names = []
    for seller in sellers:
        if isinstance(seller, str):
            seller_names.append(seller.strip())
        else:
            seller_names.append(seller.get_text(strip=True))
    return seller_names

def get_prices(soup):
    prices = soup.select(HTML_SELECTORS["Price"])
    cents = soup.select(HTML_SELECTORS["Cents"])
    currencies = soup.select(HTML_SELECTORS["Currency"])

    product_prices = []
    for i in range(len(prices)):
        price = prices[i].get_text(strip=True) if i < len(prices) else ""
        cent = cents[i].get_text(strip=True) if i < len(cents) else ""
        currency = currencies[i].get_text(strip=True) if i < len(currencies) else ""
        product_prices.append(f"{price}{cent} {currency}".strip())

    return product_prices

def get_product_states(soup):
    states = soup.select(HTML_SELECTORS["Product State"])
    product_states = [state.get_text(strip=True) if state else "Non trouvé" for state in states]
    return product_states

def extract_product_details(soup, sellers, prices, product_states):
    products = []
    num_sellers = max(len(sellers), len(prices), len(product_states))

    for i in range(num_sellers):
        product_details = {}

        product_details["Product Name"] = soup.select_one(HTML_SELECTORS["Product Name"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Product Name"]) else "Non trouvé"
        product_details["Price"] = prices[i] if i < len(prices) else "Non trouvé"
        product_details["Seller"] = sellers[i] if i < len(sellers) else "E.Leclerc"
        product_details["Product State"] = product_states[i] if i < len(product_states) else "Non trouvé"

        product_details["Delivery Fees"] = soup.select_one(HTML_SELECTORS["Delivery Fees"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Delivery Fees"]) else "Non trouvé"
        product_details["Delivery Date"] = soup.select_one(HTML_SELECTORS["Delivery Date"]).get_text(strip=True) if soup.select_one(HTML_SELECTORS["Delivery Date"]) else "Non trouvé"

        product_details["Seller Rating"] = "0"
        product_details["Platform"] = "E.Leclerc"
        product_details["Timestamp"] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

        products.append(product_details)
    
    return products

def extract_info(soup):
    sellers = get_sellers(soup)
    prices = get_prices(soup)
    product_states = get_product_states(soup)

    products = extract_product_details(soup, sellers, prices, product_states)

    return products

def write_to_csv(products):
    with open('product_details.csv', "a", newline="", encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Platform", "Product Name", "Seller", "Price", "Delivery Fees", "Delivery Date", "Product State", "Seller Rating", "Timestamp"])

        for product in products:
            writer.writerow([product.get("Platform", ''), product.get('Product Name', ''), product.get('Seller', ''), product.get('Price', ''), product.get('Delivery Fees', ''), product.get('Delivery Date', ''), product.get('Product State', ''), product.get('Seller Rating', ''), product.get('Timestamp', '')])
        writer.writerow(["------------------------------------------------------------------------------------------------------------------------------------"])
    print("Product details written to CSV.")

def main():
    urls = [
        "https://www.e.leclerc/of/apple-iphone-16-15-5-cm-6-1-double-sim-ios-18-5g-usb-type-c-512-go-noir-0195949823763",
        "https://www.e.leclerc/of/apple-iphone-16-15-5-cm-6-1-double-sim-ios-18-5g-usb-type-c-256-go-noir-0195949822865",
        "https://www.e.leclerc/fp/apple-iphone-16-15-5-cm-6-1-double-sim-ios-18-5g-usb-type-c-128-go-noir-0195949821967",
        "https://www.e.leclerc/of/apple-iphone-16-plus-17-cm-6-7-double-sim-ios-18-5g-usb-type-c-512-go-noir-0195949724169",
        "https://www.e.leclerc/of/apple-iphone-16-plus-17-cm-6-7-double-sim-ios-18-5g-usb-type-c-256-go-noir-0195949723216",
        "https://www.e.leclerc/of/apple-iphone-16-plus-17-cm-6-7-double-sim-ios-18-5g-usb-type-c-128-go-noir-0195949722264",
        "https://www.e.leclerc/of/apple-iphone-16-pro-16-cm-6-3-double-sim-ios-18-5g-usb-type-c-1-to-noir-0195949773488",
        "https://www.e.leclerc/fp/apple-iphone-15-15-5-cm-6-1-double-sim-ios-17-5g-usb-type-c-512-go-noir-0195949037795?offer_id=72931002",
        "https://www.e.leclerc/of/smartphone-apple-iphone-15-256gb-noir-0195949036965",
        "https://www.e.leclerc/of/smartphone-apple-iphone-15-128gb-noir-0195949036064",
        "https://www.e.leclerc/of/apple-iphone-14-15-5-cm-6-1-double-sim-ios-17-5g-512-go-noir-0194253411550",
        "https://www.e.leclerc/of/smartphone-apple-iphone-14-256go-noir-midnight-0194253409908"
    ]

    for url in urls:
        print(f"Processing URL: {url}")

        fetch_html(url)
        filepath = "page_content.html"

        with open(filepath, 'r', encoding='utf-8') as file:
            html_content = file.read()
        soup = BeautifulSoup(html_content, 'html.parser')

        products = extract_info(soup)
        write_to_csv(products)

if __name__ == "__main__":
    main()
