import pandas as pd
import plotly.express as px

# Load the CSV file
csv_file = '/home/scraping/algo_scraping/scraping_carrefour.csv'

# Define column names for the CSV
columns = [
    "Store", "Product", "Seller", "Delivery", "Price", "Rating", "Timestamp"
]

# Load the data into a DataFrame
data = pd.read_csv(csv_file, sep=",", names=columns, skiprows=1, engine='python', on_bad_lines='skip')

# Clean the data
# Remove rows with missing or invalid prices
data['Price'] = data['Price'].str.replace('€', '').str.replace(',', '.').str.strip()
data = data[data['Price'].notnull()]  # Ensure no null values
data = data[data['Price'].apply(lambda x: x.replace('.', '', 1).isdigit())]
data['Price'] = data['Price'].astype(float)

# Remove rows with missing or invalid ratings
data = data[data['Rating'] != "Non spécifié"]
data['Rating'] = data['Rating'].astype(float)

# Convert the 'Timestamp' column to a datetime format
data['Timestamp'] = pd.to_datetime(data['Timestamp'], errors='coerce', dayfirst=True)

# Remove rows with invalid or missing timestamps
data = data[data['Timestamp'].notnull()]

# Round timestamps to the nearest hour
data['Rounded_Timestamp'] = data['Timestamp'].dt.round('30min')

# Create an interactive line chart with smoothing
fig = px.line(
    data,
    x="Rounded_Timestamp",
    y="Price",
    color="Seller",
    line_group="Product",
    facet_col="Product",
    facet_col_wrap=3,
    line_shape="spline",  # Smooth the lines
    title="Price Trends for Smartphones Over Time",
    labels={"Rounded_Timestamp": "Date", "Price": "Prix (€)", "Seller": "Vendeur"},
    height=800
)

# Update layout for better readability
fig.update_layout(
    xaxis=dict(tickangle=45),
    legend_title="Vendeur",
    margin=dict(t=50, l=50, r=50, b=50)
)

# Show the plot
fig.show()
