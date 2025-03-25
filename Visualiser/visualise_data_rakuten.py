import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

# Path to the CSV file
csv_file = '/home/scraping/algo_scraping/LECLERC/product_details.csv'

# Define column names for the new CSV structure
columns = [
    "pfid", "idsmartphone", "url", "timestamp", "price", "shipcost", 
    "rating", "ratingnb", "offertype", "offerdetails", 
    "shipcountry", "sellercountry", "seller"
]

# Function to load and clean the data
def load_and_clean_data():
    data = pd.read_csv(csv_file, sep=",", names=columns, skiprows=1, engine='python', on_bad_lines='skip')
    
    # Clean the data
    data['price'] = data['price'].astype(str).str.replace('€', '').str.replace(',', '.').str.strip()
    data = data[data['price'].notnull()]
    data = data[data['price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
    data['price'] = data['price'].astype(float)
    
    # Handle missing values in 'shipcost'
    data['shipcost'] = data['shipcost'].fillna(0).astype(float)
    
    # Convert 'rating' to numeric, fill missing with NaN
    data['rating'] = pd.to_numeric(data['rating'], errors='coerce')
    
    # Convert 'timestamp' to datetime, drop rows with invalid timestamps
    data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce', dayfirst=True)
    data = data[data['timestamp'].notnull()]
    
    # Round timestamps to the nearest 30 minutes
    data['Rounded_Timestamp'] = data['timestamp'].dt.round('30min')
    
    # Fill missing seller names with "Unknown"
    data['seller'] = data['seller'].fillna("Unknown")
    
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
    app.run(debug=True, host='157.159.195.72', port=8050)
