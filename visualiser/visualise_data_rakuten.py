import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

# Path to the CSV file
csv_file = '/home/scraping/algo_scraping/RAKUTEN/Rakuten_data.csv'

# Chemin vers le fichier lien.csv
smartphone_csv_file = '/home/scraping/algo_scraping/lien.csv'

# Define column names for the new CSV structure
columns = [
    "pfid", "idsmartphone", "url", "timestamp", "price", "shipcost", 
    "rating", "ratingnb", "offertype", "offerdetails", 
    "shipcountry", "sellercountry", "seller"
]

# Function to load and clean the data
def load_and_clean_data():
    try:
        # Load the CSV file and let pandas infer column names
        data = pd.read_csv(csv_file, sep=",", engine='python', on_bad_lines='skip')
        
        # Check if the file is empty
        if data.empty:
            print("The CSV file is empty.")
            return pd.DataFrame(columns=columns)
        
        # Ensure the DataFrame has the expected columns
        missing_columns = [col for col in columns if col not in data.columns]
        if missing_columns:
            print(f"Missing columns in the CSV file: {missing_columns}")
            return pd.DataFrame(columns=columns)
        
        # Clean the data
        data['price'] = data['price'].astype(str).str.replace('€', '').str.replace(',', '.').str.strip()
        data = data[data['price'].notnull()]
        data = data[data['price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
        data['price'] = data['price'].astype(float)
        
        # Handle missing values in 'shipcost'
        data['shipcost'] = data['shipcost'].fillna(0).astype(float)
        
        # Convert 'rating' to numeric, fill missing with NaN
        data['rating'] = pd.to_numeric(data['rating'], errors='coerce')
        
        # Convert 'timestamp' to datetime using the specified format
        data['timestamp'] = pd.to_datetime(data['timestamp'], format='%Y/%m/%d %H:%M', errors='coerce')
        data = data[data['timestamp'].notnull()]
        
        # Round timestamps to the nearest 30 minutes
        data['Rounded_Timestamp'] = data['timestamp'].dt.round('30min')
        
        # Fill missing seller names with "Unknown"
        data['seller'] = data['seller'].fillna("Unknown")
        
        # Merge with smartphone models to replace idsmartphone with Phone
        smartphone_models = load_smartphone_models(smartphone_csv_file)
        data = data.merge(smartphone_models, on='idsmartphone', how='left')
        
        # Replace idsmartphone with Phone for visualization
        data['idsmartphone'] = data['Phone']
        data.drop(columns=['Phone'], inplace=True)  # Remove the Phone column after replacement
        
        return data
    except Exception as e:
        print(f"Error loading and cleaning data: {e}")
        return pd.DataFrame(columns=columns)

def load_smartphone_models(csv_file_path):
    """
    Charge les modèles de smartphones depuis le fichier lien.csv.
    
    Args:
        csv_file_path (str): Chemin vers le fichier lien.csv.
    
    Returns:
        pd.DataFrame: DataFrame contenant les colonnes 'Phone' et 'idsmartphone'.
    """
    try:
        # Charger le fichier CSV
        data = pd.read_csv(csv_file_path)
        
        # Vérifier si les colonnes nécessaires sont présentes
        required_columns = ['Phone', 'idsmartphone']
        if not all(col in data.columns for col in required_columns):
            print(f"Les colonnes nécessaires {required_columns} sont absentes du fichier.")
            return pd.DataFrame(columns=required_columns)
        
        # Extraire les colonnes nécessaires
        smartphone_models = data[['Phone', 'idsmartphone']].dropna()
        print(f"{len(smartphone_models)} modèles de smartphones chargés depuis {csv_file_path}.")
        return smartphone_models
    
    except Exception as e:
        print(f"Erreur lors du chargement des modèles de smartphones : {e}")
        return pd.DataFrame(columns=['Phone', 'idsmartphone'])

# Charger les modèles de smartphones
smartphone_models = load_smartphone_models(smartphone_csv_file)

# Afficher les modèles chargés (pour vérification)
print(smartphone_models.head())

# Initialize the Dash app
app = Dash(__name__)

# Initial data load
data = load_and_clean_data()

# Function to create the figure
def create_figure(data):
    return px.line(
        data,
        x="Rounded_Timestamp",
        y="price",
        color="seller",
        line_group="idsmartphone",
        facet_col="idsmartphone",
        facet_col_wrap=3,
        line_shape="spline",
        title="Price Trends for Smartphones Over Time",
        labels={
            "Rounded_Timestamp": "Date",
            "price": "Price (€)",
            "seller": "Seller",
            "idsmartphone": "Smartphone ID"
        },
        height=800
    )

# Define the layout of the app
app.layout = html.Div([
    html.H1("Price Trends for Smartphones Over Time", style={'textAlign': 'center'}),
    dcc.Graph(id='price-trends-graph')  # Dynamic graph
])

# Callback to update the graph when data changes
@app.callback(
    Output('price-trends-graph', 'figure'),
    Input('price-trends-graph', 'id')  # Dummy input to trigger updates
)
def update_graph(_):
    global data
    return create_figure(data)

# File watcher to reload data when the CSV file changes
class CSVFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        global data
        if event.src_path == csv_file:
            print(f"File {csv_file} changed, reloading data...")
            data = load_and_clean_data()

# Start the file watcher in a separate thread
def start_file_watcher():
    event_handler = CSVFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=csv_file, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# Run the app and file watcher
if __name__ == '__main__':
    threading.Thread(target=start_file_watcher, daemon=True).start()
    app.run(debug=True, host='157.159.195.72', port=8052)
