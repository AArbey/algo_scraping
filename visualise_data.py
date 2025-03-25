import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

# Path to the CSV file
csv_file = '/home/scraping/algo_scraping/scraping_carrefour.csv'

# Define column names for the CSV
columns = [
    "Store", "Product", "Seller", "Delivery", "Price", "Rating", "Timestamp"
]

# Function to load and clean the data
def load_and_clean_data():
    data = pd.read_csv(csv_file, sep=",", names=columns, skiprows=1, engine='python', on_bad_lines='skip')
    # Clean the data
    data['Price'] = data['Price'].str.replace('€', '').str.replace(',', '.').str.strip()
    data = data[data['Price'].notnull()]
    data = data[data['Price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
    data['Price'] = data['Price'].astype(float)
    data = data[data['Rating'] != "Non spécifié"]
    data['Rating'] = data['Rating'].astype(float)
    data['Timestamp'] = pd.to_datetime(data['Timestamp'], errors='coerce', dayfirst=True)
    data = data[data['Timestamp'].notnull()]
    data['Rounded_Timestamp'] = data['Timestamp'].dt.round('30min')
    return data

# Initialize the Dash app
app = Dash(__name__)

# Initial data load
data = load_and_clean_data()

# Function to create the figure
def create_figure(data):
    return px.line(
        data,
        x="Rounded_Timestamp",
        y="Price",
        color="Seller",
        line_group="Product",
        facet_col="Product",
        facet_col_wrap=3,
        line_shape="spline",
        title="Price Trends for Smartphones Over Time",
        labels={"Rounded_Timestamp": "Date", "Price": "Prix (€)", "Seller": "Vendeur"},
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
    app.run_server(debug=True, host='157.159.195.72', port=8050)
