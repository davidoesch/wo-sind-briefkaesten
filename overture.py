import pandas as pd
import geopandas as gpd
import duckdb as db
from shapely import wkt
import ast
import requests
import re

# Example: Flatten the 'addresses' column to extract 'freeform' fields
def extract_freeform(addresses):
    """
    Extracts 'freeform' fields from a list of address dictionaries.

    This function takes a list of dictionaries, each representing an address,
    and extracts the value associated with the 'freeform' key from each dictionary.
    If the input is a JSON string, it will be parsed into a list of dictionaries first.
    The extracted 'freeform' values are then concatenated into a single string,
    separated by commas.

    Args:
        addresses (str or list): A JSON string or a list of dictionaries, where each
                                 dictionary contains address information.

    Returns:
        str: A comma-separated string of 'freeform' values, or None if an error occurs.
    """
    try:
        # Parse the JSON if it's in string format
        if isinstance(addresses, str):
            addresses = json.loads(addresses)
        # Extract 'freeform' fields from the list of dictionaries
        return ', '.join([addr.get('freeform', '') for addr in addresses if 'freeform' in addr])
    except Exception as e:
        return None

def clean_df(df, column_name):
    """
    Converts list-like strings in a specified column to comma-separated strings.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to clean.

    Returns:
        pd.DataFrame: The modified DataFrame with the cleaned column.
    """
    def convert_to_comma_separated(value):
        try:
            # Safely evaluate the string as a list
            items = ast.literal_eval(value)
            # Join the items with a comma
            return ",".join(items)
        except (ValueError, SyntaxError):
            # Return the original value if conversion fails
            return value

    df[column_name] = df[column_name].apply(convert_to_comma_separated)
    return df

# Initial setup
con = db.connect()

# To perform spatial operations, the spatial extension is required.
# src - https://duckdb.org/docs/api/python/overview.html#loading-and-installing-extensions

con.install_extension("spatial")
con.load_extension("spatial")

# To load a Parquet file from S3, the httpfs extension is required.
# src - https://duckdb.org/docs/guides/import/s3_import.html

con.install_extension("httpfs")
con.load_extension("httpfs")

# Tell DuckDB which S3 region to find Overture's data bucket in
# src - https://github.com/OvertureMaps/data/blob/main/README.md#how-to-access-overture-maps-data
con.sql("SET s3_region='us-west-2'")

# Overture structure
# https://github.com/OvertureMaps/data/blob/main/README.md#how-to-access-overture-maps-data

# Fetch the latest release information from the Overture Maps GitHub repository
response = requests.get("https://docs.overturemaps.org/release/latest/")
html_content = response.text

# Use regular expression to find the release date in the title
match = re.search(r'data-rh=true>(\d{4}-\d{2}-\d{2}\.\d+)', html_content)
if match:
    release_date = match.group(1)
    print(release_date)
else:
    print("Release date not found")

# Construct the parquet path using the latest release date
parquet_path = f"s3://overturemaps-us-west-2/release/{release_date}/theme=places/type=*/*"



polygon_wkt = "POLYGON((7.407129 46.903032, 7.426751 46.903032, 7.426751 46.916736, 7.407129 46.916736, 7.407129 46.903032))"
breakpoint()
query = f"""
SELECT
    id,
    names.primary AS primary_name,
    json_extract_string(categories, 'primary') AS category,
    json_extract_string(categories, 'alternate') AS category_alt,
    addresses AS addresses,
    ST_AsText(geometry) as geometry
FROM
    read_parquet('{parquet_path}', filename=true, hive_partitioning=1)
WHERE
    ST_Intersects(geometry, ST_GeomFromText('{polygon_wkt}'))
"""


result_df = con.execute(query).fetchdf()

# explicitly close the connection
con.close()

# Extract place names and addresses
place_and_address_df = result_df[['primary_name', 'addresses','category','category_alt']].dropna()

# Apply the extract freeform function to the 'addresses' column
place_and_address_df['flattened_addresses'] = place_and_address_df['addresses'].apply(extract_freeform)

# Drop the original 'addresses' column for cleaner output (optional)
place_and_address_df = place_and_address_df.drop(columns=['addresses'])

# converting dict to list
place_and_address_df = clean_df(place_and_address_df, 'category_alt')

# Display the resulting DataFrame
print(place_and_address_df)
num_frames = len(place_and_address_df)
print(f"Anzahl der Frames: {num_frames}")

# categories_with_no_mailbox = [
#     "accommodation", "automotive", "arts_and_entertainment", "attractions_and_activities",
#     "beauty_and_spa", "education", "financial_service", "private_establishments_and_corporates",
#     "retail", "health_and_medical", "pets", "business_to_business", "public_service_and_government",
#     "religious_organization", "real_estate", "travel", "mass_media", "home_service",
#     "professional_services"
# ]

# # Erstellen einer Filterbedingung
# condition = place_and_address_df['category'].isin(categories_with_mailbox) | \
#             place_and_address_df['category_alt'].isin(categories_with_mailbox)

# # Anwenden des Filters auf den DataFrame
# filtered_df = place_and_address_df[condition]

# # Anzeigen des gefilterten DataFrames
# print(filtered_df)

# # Ausgabe der Anzahl der gefilterten Frames
# print(f"\nAnzahl der gefilterten Frames: {len(filtered_df)}")
print("ende")