import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
from pathlib import Path
import sys

# Correct import of get_model_code (module is one directory up)
parent_exploration_dir = Path(__file__).resolve().parent.parent  # /exploration
if str(parent_exploration_dir) not in sys.path:
    sys.path.insert(0, str(parent_exploration_dir))
try:
    from get_unique_model import get_model_code
except ModuleNotFoundError as e:
    raise ImportError(f"Impossible d'importer get_model_code depuis {parent_exploration_dir}: {e}")

# Configuration
CSV_PATH = "/home/scraping/algo_scraping/CARREFOUR/scraping_carrefour.csv"
EXPORT_PATH = "/home/scraping/algo_scraping/exploration/data_cleaned_version2kk.csv" #data_cleaned3 lorsque je change la définition d'un algo_suspect à 3 chgt par jour
FIG_DIR = "/home/scraping/algo_scraping/exploration/figures_carrefour/boites_moustache_carrefour"
os.makedirs(FIG_DIR, exist_ok=True)

# 1. Chargement et nettoyage des données
def load_and_clean_data(path):
    # --- Detect header & delimiter ---
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline().strip()

    has_header = bool(re.search(r'platform', first_line, re.IGNORECASE))
    # Prefer ';' if present (sample provided), else fallback ','
    sep = ';' if (';' in first_line and first_line.lower().count('platform;')) else ','

    read_kwargs = dict(sep=sep, on_bad_lines='skip', encoding='utf-8', dtype=str)

    if has_header:
        df = pd.read_csv(path, **read_kwargs)
    else:
        df = pd.read_csv(
            path,
            **read_kwargs,
            header=None,
            names=["Platform", "Product", "Seller", "Delivery", "Price", "Rating", "Timestamp"]
        )

    # --- Normalize column names ---
    rename_map = {}
    for col in df.columns:
        norm = col.strip().lower()
        if norm in ("product name", "product", "nom produit"):
            rename_map[col] = "Product"
        elif norm in ("delivery info", "delivery", "livraison", "shipping"):
            rename_map[col] = "Delivery"
        elif norm in ("seller rating", "rating", "seller_rating", "note vendeur"):
            rename_map[col] = "Rating"
        elif norm in ("timestamp", "date", "datetime"):
            rename_map[col] = "Timestamp"
        elif norm == "platform":
            rename_map[col] = "Platform"
        elif norm in ("seller", "vendeur"):
            rename_map[col] = "Seller"
        elif norm in ("price", "prix"):
            rename_map[col] = "Price"
        elif norm in ("batch id", "batch_id", "batch"):
            rename_map[col] = "BatchID"

    df = df.rename(columns=rename_map)

    required = ["Platform", "Product", "Seller", "Delivery", "Price", "Rating", "Timestamp", "BatchID"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes après normalisation: {missing}. Colonnes présentes: {list(df.columns)}")

    # --- Clean Price robustly ---
    original_len = len(df)
    df["Price"] = df["Price"].astype("string")

    # Remove currency symbols, NBSP, thin spaces; unify decimal comma to dot
    df["Price"] = (
        df["Price"]
        .str.replace(r'[\u00A0\u202F]', '', regex=True)   # non‑breaking / thin spaces
        .str.replace(r'[€]', '', regex=True)
        .str.replace(',', '.', regex=False)
        .str.replace(r'[^0-9.\-]', '', regex=True)        # keep digits . -
        .str.strip()
    )

    def normalize_decimal(s):
        if s is None or pd.isna(s) or s == "":
            return np.nan
        parts = s.split('.')
        if len(parts) <= 2:
            return s
        return ''.join(parts[:-1]) + '.' + parts[-1]

    df["Price"] = df["Price"].apply(normalize_decimal)
    df["Price"] = pd.to_numeric(df["Price"], errors='coerce')
    df = df[df["Price"].notna()]

    # --- Clean Rating ---
    df["Rating"] = (
        df["Rating"]
        .astype("string")
        .str.strip()
        .replace({"Non spécifié": None, "Non specifie": None, "nan": None, "": None})
    )
    df["Rating"] = pd.to_numeric(df["Rating"], errors='coerce')

    # --- Parse Timestamp ---
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", dayfirst=True)
    df = df[df["Timestamp"].notna()]
    df["Day"] = df["Timestamp"].dt.floor("D")

    print(f"Delimiter détecté: '{sep}' | Header détecté: {has_header}")
    print(f"Lignes initiales: {original_len} | Après nettoyage: {len(df)} | Supprimées: {original_len - len(df)}")
    print("Dates uniques dans le fichier :")
    print(df["Day"].value_counts())

    return df

# 2. Génération des identifiants lisibles
def extract_model_code(product_name):
    # (Legacy function now unused; kept for fallback/reference)
    name = product_name.lower().replace("apple", "").replace("iphone", "ip")

    # Génération (ip14, ip15, ip16, etc.)
    gen_match = re.search(r'ip\s?(\d{2})', name)
    gen = gen_match.group(1) if gen_match else "unk"

    # Version (plus, pro max, pro, max)
    version = ""
    if "pro max" in name:
        version = "promax"
    elif "pro" in name:
        version = "pro"
    elif "max" in name:
        version = "max"
    elif "plus" in name:
        version = "plus"

    # Capacité (128go, 1to, etc.)
    cap_match = re.search(r'(\d{2,4})\s?(go|to)', name)
    if cap_match:
        capacity = cap_match.group(1)
        if cap_match.group(2) == "to":
            try:
                capacity = str(int(capacity) * 1000)  # To → Go
            except:
                pass
    else:
        capacity = "unk"

    # Assemblage du code
    model_code = f"ip{gen}{version}{capacity}"
    model_code = re.sub(r'(unk)+', 'unk', model_code)
    return model_code


def create_ids(df):
    df["PlatformCode"] = "car"
    df["SellerCode"] = df["Seller"].str.lower().str.replace(r'\W+', '', regex=True)
    # New: use get_model_code; fallback to 'unk' if not detectable
    df["ModelCode"] = df["Product"].astype(str).apply(lambda x: get_model_code(x) or "unk")
    df["StateCode"] = "n"
    df["ReadableID"] = df["PlatformCode"] + "_" + df["SellerCode"] + "_" + df["ModelCode"] + df["StateCode"]
    return df

# 3. Suppression des doublons
def deduplicate(df):
    return df.sort_values("Price").drop_duplicates(subset=["ReadableID", "Timestamp"], keep="first")

"""# 4. Comptage des changements pour suspicion de tarification algo
def add_algo_flags(df):
    changes = df.groupby("ReadableID").agg(
        n_dates=("Day", "nunique"),
        n_prices=("Price", "nunique")
    )
    
    changes["algo_suspect"] = changes["n_prices"] >= 3
    return df.merge(changes, on="ReadableID", how="left")"""

# VERSION 2 
def add_algo_flags(df):
    # 1. Nombre de jours uniques et de prix uniques par ReadableID
    base_stats = df.groupby("ReadableID").agg(
        n_dates=("Day", "nunique"),
        n_prices=("Price", "nunique")
    )

    # 2. Nombre de changements de prix par jour pour chaque ReadableID
    daily_changes = (
        df.groupby(["ReadableID", "Day"])["Price"]
        .nunique()
        .reset_index(name="n_changes_per_day")
    )

    # 3. Statistiques supplémentaires sur les changements par jour
    extra_stats = daily_changes.groupby("ReadableID").agg(
        median_changes=("n_changes_per_day", "median"),
        max_changes=("n_changes_per_day", "max"),
        nb_days_ge_4=("n_changes_per_day", lambda x: (x >= 4).sum())
    )

    # 4. Détection algo_suspect selon la nouvelle règle
    extra_stats["algo_suspect"] = (
        (extra_stats["median_changes"] > 2) & (extra_stats["max_changes"] >= 6)
    ) | (extra_stats["nb_days_ge_4"] >= 3)

    # 5. Fusion des stats
    combined = base_stats.join(extra_stats[["algo_suspect"]])

    # 6. Fusion finale avec le DataFrame original
    return df.merge(combined, on="ReadableID", how="left")



def generate_structured_numeric_id(df):
    # Codes simples et reproductibles
    platform_map = {name: i for i, name in enumerate(sorted(df["Platform"].unique()))}
    seller_map = {name: i for i, name in enumerate(sorted(df["Seller"].str.lower().unique()))}
    model_map = {name: i for i, name in enumerate(sorted(df["ModelCode"].unique()))}

    df["PlatformCodeNum"] = df["Platform"].map(platform_map)
    df["SellerCodeNum"] = df["Seller"].str.lower().map(seller_map)
    df["ModelCodeNum"] = df["ModelCode"].map(model_map)

    # CodeID composé, ex : 1_027_132 → car_big_ip16256n
    df["CodeID"] = (
        df["PlatformCodeNum"] * 1_000_000 +
        df["SellerCodeNum"] * 1_000 +
        df["ModelCodeNum"]
    )
    print("Entrées avec CodeID = 0 :")
    print(df[df["CodeID"] == 0][["Platform", "Seller", "ModelCode", "ReadableID"]].drop_duplicates())

    return df


# 5. Visualisations utiles
def plot_boxplot(df):
    top_models = df["ModelCode"].value_counts().nlargest(6).index
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df[df["ModelCode"].isin(top_models)], x="ModelCode", y="Price")
    plt.title("Distribution des prix par modèle, Marketplace Carrefour")
    plt.savefig(os.path.join(FIG_DIR, "boxplot_prices2.png"))
    plt.close()

def plot_daily_counts(df):
    counts = df.groupby("Day").size()
    plt.figure(figsize=(10, 5))
    counts.plot(kind="bar")
    plt.title("Nombre de prix observés par jour, Marketplace Carrefour")
    plt.ylabel("Nombre")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, "count_per_day.png"))
    plt.close()


# 1. Boxplot par fournisseur pour un modèle donné
def plot_boxplot_by_seller_for_model(df, model_code):
    # Filtrer les données pour un modèle donné
    model_data = df[df["ModelCode"] == model_code]
    
    # Calcul du nombre de changements de prix par jour pour chaque fournisseur et pour ce modèle spécifique
    changes = model_data.groupby(["Seller", "Day"]).agg(
        n_prices=("Price", "nunique")  # Nombre de prix uniques par jour pour chaque fournisseur
    ).reset_index()

    # Boxplot pour les changements de prix par jour pour chaque fournisseur
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=changes, x="Seller", y="n_prices")
    plt.title(f"Distribution des changements de prix par jour pour le modèle {model_code}, par fournisseur")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"boxplot_price_changes_by_seller_for_model_{model_code}.png"))
    plt.close()

# 2. Créer plusieurs boxplots pour les 5 modèles les plus fréquents
def plot_boxplots_for_top_models(df):
    # Identifier les 5 modèles les plus fréquents
    top_models = df["ModelCode"].value_counts().nlargest(5).index
    
    # Créer un boxplot pour chaque modèle
    for model_code in top_models:
        plot_boxplot_by_seller_for_model(df, model_code)


# Boxplot par jour pour un modèle donné
def plot_boxplot_by_day_for_model(df, model_code):
    # Filtrer les données pour un modèle spécifique
    model_data = df[df["ModelCode"] == model_code]
    
    # Boxplot pour les prix par jour pour ce modèle spécifique
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=model_data, x="Day", y="Price")
    plt.title(f"Distribution des prix par jour pour le modèle {model_code}")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"boxplot_by_day_for_model_{model_code}.png"))
    plt.close()

# Créer plusieurs boxplots pour les 5 modèles les plus fréquents par jour
def plot_boxplots_for_top_models_by_day(df):
    # Identifier les 5 modèles les plus fréquents
    top_models = df["ModelCode"].value_counts().nlargest(5).index
    
    # Créer un boxplot pour chaque modèle
    for model_code in top_models:
        plot_boxplot_by_day_for_model(df, model_code)

# Boxplot par fréquence de changement de prix (selon le nombre de changements par jour)
def plot_boxplot_by_price_change_frequency_for_model(df, model_code, threshold=6):
    # Filtrer les données pour un modèle donné
    model_data = df[df["ModelCode"] == model_code]
    
    # Ajouter la colonne de fréquence de changement de prix
    changes = model_data.groupby("ReadableID").agg(
        n_prices=("Price", "nunique")
    )
    changes["price_change_frequency"] = changes["n_prices"]
    model_data = model_data.merge(changes[["price_change_frequency"]], on="ReadableID", how="left")
    
    # Filtrer les fournisseurs avec des changements fréquents de prix
    model_data['PriceChangeCategory'] = model_data['price_change_frequency'].apply(lambda x: 'High' if x >= threshold else 'Low')

    # Boxplot pour les prix selon la fréquence de changement de prix
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=model_data, x="PriceChangeCategory", y="Price")
    plt.title(f"Distribution des prix selon la fréquence de changement de prix pour le modèle {model_code}")
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"boxplot_by_price_change_frequency_for_model_{model_code}.png"))
    plt.close()

# Créer plusieurs boxplots pour les 5 modèles les plus fréquents selon la fréquence de changement de prix
def plot_boxplots_for_top_models_by_price_change_frequency(df):
    # Identifier les 5 modèles les plus fréquents
    top_models = df["ModelCode"].value_counts().nlargest(5).index
    
    # Créer un boxplot pour chaque modèle
    for model_code in top_models:
        plot_boxplot_by_price_change_frequency_for_model(df, model_code)


def plot_true_price_changes_boxplot(df, model_code):
    model_data = df[df["ModelCode"] == model_code].copy()
    if model_data.empty:
        print(f"Aucune donnée pour le modèle {model_code}")
        return

    # Trier pour détecter changements successifs
    model_data = model_data.sort_values(["Seller", "Day", "Timestamp"])
    model_data["PrevPrice"] = model_data.groupby(["Seller", "Day"])["Price"].shift()
    model_data["PriceChanged"] = model_data["Price"] != model_data["PrevPrice"]

    # Compter les changements par vendeur et jour
    change_counts = model_data.groupby(["Seller", "Day"])["PriceChanged"].sum().reset_index()
    change_counts.rename(columns={"PriceChanged": "n_price_changes"}, inplace=True)

    # Tracer
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=change_counts, x="Seller", y="n_price_changes")
    plt.title(f"Changements de prix réels par jour – modèle {model_code}")
    plt.ylabel("Nombre de changements dans la journée")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_DIR, f"TRUE_boxplot_changes_{model_code}.png"))
    plt.close()


def plot_true_price_changes_boxplot_for_top_models(df):
    top_n=5
    top_models = df["ModelCode"].value_counts().nlargest(top_n).index
    for model_code in top_models:
        plot_true_price_changes_boxplot(df, model_code)


# 6. Pipeline principal
def main():
    
    df = load_and_clean_data(CSV_PATH)
    
    df = create_ids(df)
    df = generate_structured_numeric_id(df)
    #df = deduplicate(df)
    df = add_algo_flags(df)
    df = df[["Timestamp", "Seller", "Day", "ModelCode", "CodeID", "ReadableID", "Price", "Rating", "algo_suspect", "n_prices", "n_dates"]]
    df.to_csv(EXPORT_PATH, index=False)
    plot_true_price_changes_boxplot_for_top_models(df)
    #plot_boxplot(df)
    #plot_daily_counts(df)
    #plot_boxplots_for_top_models(df)
    #plot_boxplots_for_top_models_by_day(df)
    #plot_boxplots_for_top_models_by_price_change_frequency(df)

    print(f"Fichier nettoyé exporté : {EXPORT_PATH}")
    print(f"Graphiques enregistrés dans `{FIG_DIR}`.")
    print(f"Fournisseurs suspects (tarification algo présumée) :")
    #print(df[df["algo_suspect"]].groupby("ReadableID")[["n_prices", "n_dates"]].first().sort_values("n_prices", ascending=False).head(10))
    # Assure-toi que les colonnes "n_prices" et "n_dates" existent bien dans le DataFrame
    cols_to_show = ["ReadableID", "n_prices", "n_dates", "algo_suspect"]
    if not all(col in df.columns for col in cols_to_show):
        raise ValueError("Les colonnes nécessaires ne sont pas présentes dans df")

    # Filtrer les suspects et afficher les 10 premiers triés par nombre de prix uniques décroissant
    top_suspects = (
        df[df["algo_suspect"]]
        .drop_duplicates("ReadableID")
        .sort_values("n_prices", ascending=False)
        [["ReadableID", "n_prices", "n_dates"]]
        .head(10)
    )

    print(top_suspects)
if __name__ == "__main__":
    main()