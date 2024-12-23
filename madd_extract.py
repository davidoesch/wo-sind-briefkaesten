import requests
import geopandas as gpd
import pandas as pd
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon, box
from shapely.ops import split
from collections import defaultdict
import json
import numpy as np
from swiftshadow import QuickProxy
from shapely import wkt
import duckdb as db
import ast
import requests
import re
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

global building_codes
building_codes = {
    1010: {"CODE": 1010, "KAT": "GKAT", "BESCHREIBUNG": "Provisorische Unterkunft"},
    1020: {"CODE": 1020, "KAT": "GKAT", "BESCHREIBUNG": "Gebäude mit ausschliesslicher Wohnnutzung"},
    1030: {"CODE": 1030, "KAT": "GKAT", "BESCHREIBUNG": "Andere Wohngebäude (Wohngebäude mit Nebennutzung)"},
    1040: {"CODE": 1040, "KAT": "GKAT", "BESCHREIBUNG": "Gebäude mit teilweiser Wohnnutzung"},
    1060: {"CODE": 1060, "KAT": "GKAT", "BESCHREIBUNG": "Gebäude ohne Wohnnutzung"},
    1110: {"CODE": 1110, "KAT": "GKLAS", "BESCHREIBUNG": "Gebäude mit einer Wohnung"},
    1121: {"CODE": 1121, "KAT": "GKLAS", "BESCHREIBUNG": "Gebäude mit zwei Wohnungen"},
    1122: {"CODE": 1122, "KAT": "GKLAS", "BESCHREIBUNG": "Gebäude mit drei oder mehr Wohnungen"},
    1130: {"CODE": 1130, "KAT": "GKLAS", "BESCHREIBUNG": "Wohngebäude für Gemeinschaften"},
    1211: {"CODE": 1211, "KAT": "GKLAS", "BESCHREIBUNG": "Hotelgebäude"},
    1212: {"CODE": 1212, "KAT": "GKLAS", "BESCHREIBUNG": "Andere Gebäude für kurzfristige Beherbergung"},
    1220: {"CODE": 1220, "KAT": "GKLAS", "BESCHREIBUNG": "Bürogebäude"},
    1230: {"CODE": 1230, "KAT": "GKLAS", "BESCHREIBUNG": "Gross-und Einzelhandelsgebäude"},
    1231: {"CODE": 1231, "KAT": "GKLAS", "BESCHREIBUNG": "Restaurants und Bars in Gebäuden ohne Wohnnutzung"},
    1241: {"CODE": 1241, "KAT": "GKLAS", "BESCHREIBUNG": "Gebäude des Verkehrs- und Nachrichtenwesens ohne Garagen"},
    1251: {"CODE": 1251, "KAT": "GKLAS", "BESCHREIBUNG": "Industriegebäude"},
    1261: {"CODE": 1261, "KAT": "GKLAS", "BESCHREIBUNG": "Gebäude für Kultur- und Freizeitzwecke"},
    1262: {"CODE": 1262, "KAT": "GKLAS", "BESCHREIBUNG": "Museen und Bibliotheken"},
    1263: {"CODE": 1263, "KAT": "GKLAS", "BESCHREIBUNG": "Schul- und Hochschulgebäude"},
    1264: {"CODE": 1264, "KAT": "GKLAS", "BESCHREIBUNG": "Krankenhäuser und Facheinrichtungen des Gesundheitswesens"},
    1275: {"CODE": 1275, "KAT": "GKLAS", "BESCHREIBUNG": "Andere Gebäude für die kollektive Unterkunft"},
}

def extract_overture(polygon):        # Initial setup
        """
        Extracts place names and addresses from Overture Maps data within a specified polygon.
        This function connects to a DuckDB database, installs and loads necessary extensions,
        fetches the latest release information from the Overture Maps GitHub repository, constructs
        the parquet path using the latest release date, and performs a spatial query to extract
        place names and addresses within the specified polygon. It checks foralready existing  possible addresses and categories from the global variable gwrgeschaefte_by_streetnr and adds them to the DataFrame. The function then aggregates the place names and addresses by flattened addresses and returns the number of frames (rows) in the resulting DataFrame, the DataFrame containing the extracted place names, categories, and flattened addresses, and a list of lists containing a flattened address and the count of places associated with that address.
        Args:
            polygon (shapely.geometry.Polygon): The polygon within which to extract place names and addresses.
        Returns:
            tuple: A tuple containing:
                - num_frames (int): The number of frames (rows) in the resulting DataFrame.
                - place_and_address_df (pandas.DataFrame): A DataFrame containing the extracted place names,
                    categories, and flattened addresses.
                - total_places_pro_adresse (list): A list of lists, where each inner list contains a flattened address
                    and the count of places associated with that address.
        """
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
            print("Overture release date: "+ release_date)
        else:
            print("Release date not found, taking 2024-12-18.0")
            release_date = "2024-12-18.0"

        # Construct the parquet path using the latest release date
        parquet_path = f"s3://overturemaps-us-west-2/release/{release_date}/theme=places/type=*/*"


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
            ST_Intersects(geometry, ST_GeomFromText('{polygon.wkt}'))
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


        #check if the variable gwrgeschaefte_by_streetnr exists
        if 'gwrgeschaefte_by_streetnr' in globals() and len(gwrgeschaefte_by_streetnr) !=0:
            #convert gwrgechaeft_by_streetnr to a dataframe with geopandas
            gwrgeschaefte_by_streetnr_df = pd.DataFrame(gwrgeschaefte_by_streetnr)

            #add from gwrgeschaefte_by_streetnr_df the columns 'address', 'category' and 'category_alt' to place_and_address_df. Set the value of 'primary_name' to 'Unbekannt', use the cokumn 'address' as 'flattened_addresses', the column 'category' as 'category' and the column 'category_alt' as 'category_alt'
            place_and_address_df_new = pd.concat([place_and_address_df, gwrgeschaefte_by_streetnr_df[['address', 'category', 'category_alt']]], ignore_index=True)

            # Create a new DataFrame with the required columns and values
            new_df = pd.DataFrame({
                'primary_name': 'Unbekannt',
                'flattened_addresses': gwrgeschaefte_by_streetnr_df['address'],
                'category': gwrgeschaefte_by_streetnr_df['category'],
                'category_alt': gwrgeschaefte_by_streetnr_df['category_alt']
            })

            # Filter out rows where 'flattened_addresses' are already present in place_and_address_df
            new_df = new_df[~new_df['flattened_addresses'].isin(place_and_address_df['flattened_addresses'])]

            # Concatenate the new DataFrame to place_and_address_df
            place_and_address_df = pd.concat([place_and_address_df, new_df], ignore_index=True)


        #print(place_and_address_df)
        num_frames = len(place_and_address_df)
        #print(f"Anzahl der Frames: {num_frames}")

        #aggregate place_and_address_df by flattened_addresses
        total_places_pro_adresse_df = place_and_address_df.groupby('flattened_addresses').size().reset_index(name='count')
        total_places_pro_adresse_df =total_places_pro_adresse_df.rename(columns={'flattened_addresses': 'Adresse', 'count': 'Geschäfte'})

        #sortiere total_places_pro_adresse_df nach 'Adresse' absteigend
        total_places_pro_adresse_df = total_places_pro_adresse_df.sort_values(by='Geschäfte', ascending=False)

        #ändere in place_and_address_df den Namen der Spalte 'flattened_addresses' in 'Adresse' und die Spalte 'category' in 'Kategorie' und 'category_alt' in 'Kategorie_Alternative' und 'primary_name' in 'Geschäft'
        place_and_address_df = place_and_address_df.rename(columns={'flattened_addresses': 'Adresse', 'primary_name': 'Geschäft', 'category': 'Kategorie', 'category_alt': 'Kategorie_Alternative'})

        #re-order: erste Spalte in place_and_address_df ist die Adresse, die zweite Spalte ist das Geschäft, die dritte Spalte ist die Kategorie und die vierte Spalte ist die Kategorie_Alternative
        place_and_address_df = place_and_address_df[['Adresse', 'Geschäft', 'Kategorie', 'Kategorie_Alternative']]

        #Sortiere place_and_address_df nach 'Adresse' absteigend
        place_and_address_df = place_and_address_df.sort_values(by='Adresse', ascending=False)


        #total_places_pro_adresse = total_places_pro_adresse_df.values.tolist()

        return num_frames, place_and_address_df,total_places_pro_adresse_df,release_date

def get_latest_release_date(repo_url):
    # Construct the releases page URL
    releases_url = f"{repo_url}/releases"

    # Send a GET request to the releases page
    response = requests.get(releases_url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch the releases page: {response.status_code}")

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the latest release tag (usually it's the first `a` with the class `Link--primary` in the releases list)
    latest_release_tag = soup.find('a', {'class': 'Link--primary'})

    if not latest_release_tag:
        raise Exception("Could not find any releases on the page.")

    # Extract the release version text
    latest_release = latest_release_tag['href'].split('/')[-1]


    # Find the release date (usually it's in a `relative-time` tag within the release tag)
    release_date_tag = soup.find('relative-time')

    if not release_date_tag:
        raise Exception("Could not find the release date on the page.")

    # Extract the release date text
    release_date = release_date_tag['datetime']

    return latest_release, release_date



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

def resolve_kml_url(shortened_url):
    """Löst eine gekürzte URL auf, extrahiert die KML-URL und entfernt '&featureInfo=default'."""
    print(f"Aufruf shortened  public.geo.admin.ch ... ")
    response = requests.head(shortened_url, allow_redirects=True, timeout=15)
    print(f"... erhalten ")
    if response.status_code != 200:
        raise ValueError(f"Fehler beim Auflösen der URL: {response.status_code}")

    if "https://public.geo.admin.ch/api/kml" in response.url:
        # Parse the URL
        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.fragment)

        # Extract the 'layers' parameter and split to get the desired part
        layers_param = query_params.get('layers', [''])[0]
        clean_url = layers_param.replace('KML|', '', 1)
        return clean_url

    raise ValueError("Keine gültige Zeichnung gefunden.")

def load_kml_polygon_directly(kml_url):
    """Parst ein KML-Polygon direkt aus einer URL und gibt es als Shapely-Polygon zurück."""
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
    resolved_url = resolve_kml_url(kml_url)

    print(f"Aufruf public.geo.admin.ch ... ")
    response = requests.get(resolved_url, timeout=30)
    print(f"... erhalten ")
    if response.status_code != 200:
        raise ValueError(f"Fehler beim Laden der KML-Datei: {response.status_code}")

    root = ET.fromstring(response.content)
    namespace = {"kml": "http://www.opengis.net/kml/2.2"}

    coordinates = root.find(".//kml:coordinates", namespace).text.strip()
    coords = [
        tuple(map(float, coord.split(",")[:2]))
        for coord in coordinates.split()
    ]

    return Polygon(coords)

def split_polygon(polygon, max_area, export_gpkg=False, gpkg_path="grid_output.gpkg"):
    """Teilt ein Polygon in kleinere Polygone auf, deren Fläche max_area nicht überschreitet. Optionaler Export als GeoPackage."""
    bounds = polygon.bounds  # (minx, miny, maxx, maxy)
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]

    num_x = int(np.ceil(width / np.sqrt(max_area)))
    num_y = int(np.ceil(height / np.sqrt(max_area)))

    x_step = width / num_x
    y_step = height / num_y

    sub_polygons = []
    for i in range(num_x):
        for j in range(num_y):
            minx = bounds[0] + i * x_step
            miny = bounds[1] + j * y_step
            maxx = minx + x_step
            maxy = miny + y_step

            grid_cell = box(minx, miny, maxx, maxy)
            intersection = polygon.intersection(grid_cell)

            if not intersection.is_empty:
                sub_polygons.append(intersection)

    if export_gpkg:
        # Exportiere die Sub-Polygone als GeoPackage
        gdf = gpd.GeoDataFrame(geometry=sub_polygons, crs="EPSG:4326")
        gdf.to_file(gpkg_path, driver="GPKG")
        print(f"GeoPackage wurde exportiert nach: {gpkg_path}")

    return sub_polygons

def query_geoadmin_with_polygon(polygon, sr=4326):
    """Sendet eine Anfrage an die GeoAdmin API mit einem Polygon."""
    endpoint = "https://api3.geo.admin.ch/rest/services/api/MapServer/identify"

    polygon_coords = [[x, y] for x, y in polygon.exterior.coords]
    polygon_geometry = {
        "rings": [polygon_coords],
        "spatialReference": {"wkid": sr}
    }

    params = {
        "geometryType": "esriGeometryPolygon",
        "geometry": json.dumps(polygon_geometry),
        "tolerance": 0,
        "layers": "all:ch.bfs.gebaeude_wohnungs_register",
        "imageDisplay": "500,600,96",
        "sr": sr,
        "limit": 1000,
        "returnGeometry": False
    }



    try:
        print(f"call api.geo.admin.ch ... ")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }

        prox=QuickProxy()
        #print(prox)
        #response = requests.get(endpoint, params=params, headers=headers,proxies = {prox[0]:prox[1]}, timeout=15)
        response = requests.get(endpoint, params=params, timeout=15)

        print(f" ... responded ")
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to api.geo.admin.ch: {e}")
        return


def extract_wohnungen_and_counts(result):
    global total_adressen
    total_wohnungen = 0
    wohnungen_by_streetnr = defaultdict(int)
    wohnungen_by_street = defaultdict(int)
    total_features = 0

    if result and 'results' in result:
        total_features = len(result['results'])

        if total_features == 0:
            print("Keine Adressen gefunden.")
            return 0, {}, {}

        for feature in result['results']:
            attributes = feature.get('attributes', {})
            ganzwhg = attributes.get('ganzwhg', 0) or 0

            # Check if ganzwhg is 0 and apply the additional checks
            if ganzwhg == 0:
                gkat = attributes.get('gkat')
                gklas = attributes.get('gklas')

                # Check if either gkat or gklas matches the specified values in building_codes
                if any(building_codes[code]["CODE"] == gkat for code in building_codes if "CODE" in building_codes[code]) or any(building_codes[code]["KAT"] == gklas for code in building_codes if "KAT" in building_codes[code]):

                    #prüf ob der dict gwrgeschaefte_by_streetnr schon besteht, falls nihct setzte eine globale variable gwrgeschaefte_by_streetnr
                    if 'gwrgeschaefte_by_streetnr' not in globals():
                        global gwrgeschaefte_by_streetnr
                        gwrgeschaefte_by_streetnr = []

                    # Create a new record
                    new_record = {
                        'address': attributes.get('strname_deinr', "Unbekannt"),
                        'category': building_codes.get(gkat, {}).get("BESCHREIBUNG", "Code not found"),
                        'category_alt': building_codes.get(gklas, {}).get("BESCHREIBUNG", "Code not found")
                    }

                    # Append the new record to the list if category or category_alt is not "Code not found"
                    if new_record['category_alt'] != "Code not found":
                        gwrgeschaefte_by_streetnr.append(new_record)


            strnamenr = attributes.get('strname_deinr', "Unbekannt")
            strname = ", ".join(attributes.get('strname', "Unbekannt"))
            total_wohnungen += ganzwhg
            wohnungen_by_streetnr[strnamenr] += ganzwhg
            wohnungen_by_street[strname] += ganzwhg

        print(f"Anzahl der gefundenen Adressen: {total_features}")
        total_adressen += total_features

        if total_features >= 200:
            print("***************")
            print("Warnung: Mehr als 200 Adressen. Bitte unterteilen Sie die Zeichnung in kleinere Abschnitte und führen Sie die Abfrage mehrfach aus.")
            print("***************")
            wohnungen_by_streetnr = defaultdict(int)
            total_wohnungen = 0

    else:
        print("Keine Ergebnisse gefunden.")
        return 0, {}, {}

    return total_wohnungen, wohnungen_by_streetnr, wohnungen_by_street

# Hauptprogramm
if __name__ == "__main__":
    # Beispiel-KML-Link (ersetzen Sie diesen durch Ihren eigenen Link)
    #kml_url = "https://s.geo.admin.ch/3lwgn09kgqha" #Schliern
    #kml_url = "https://s.geo.admin.ch/p56ijeogsuta" #SChliern gross
    #kml_url = "https://s.geo.admin.ch/jpve0fg64vai" #köniz
    #kml_url = "https://s.geo.admin.ch/nct5odun6mkp"
    #kml_url = "https://s.geo.admin.ch/aemkr12m23lk" #bumpliiz
    kml_url = "https://s.geo.admin.ch/j8mzmz9oou1n" #sterinhlzli
    #kml_url="https://s.geo.admin.ch/l2eyovbqgimd"#stadelhofen
    #kml_url = input("Bitte Link zur Zeichnung eingeben: ")
    # print(f"Link zur Zeichnung ist: {kml_url}")

    polygon = load_kml_polygon_directly(kml_url)

    max_area = 0.000005  # 130m x 130m
    sub_polygons = split_polygon(polygon, max_area,export_gpkg=True)

    total_adressen = 0
    total_wohnungen = 0
    aggregated_wohnungen_by_streetnr = defaultdict(int)
    aggregated_wohnungen_by_street = defaultdict(int)


    for i, sub_polygon in enumerate(sub_polygons):
        print(f"Verarbeite Subpolygon {i + 1} von {len(sub_polygons)}...")
        result = query_geoadmin_with_polygon(sub_polygon)

        if result:
            sub_total_wohnungen, sub_wohnungen_by_streetnr,sub_wohnungen_by_street= extract_wohnungen_and_counts(result)
            total_wohnungen += sub_total_wohnungen

            for street, count in sub_wohnungen_by_streetnr.items():
                aggregated_wohnungen_by_streetnr[street] += count

            for street, count in sub_wohnungen_by_street.items():
                aggregated_wohnungen_by_street[street] += count

    print("-------------------------------------------------------")
    print("Wohnungen nach Adressen")
    print("-------------------------------------------------------")
    for strnamenr, count in sorted(aggregated_wohnungen_by_streetnr.items()):
        print(f"  {strnamenr}: {count}")

    # Berechnung der Summe der Wohnungen pro Straße
    print("-------------------------------------------------------")
    print("Wohnungen nach Strassen:")
    print("-------------------------------------------------------")
    total_wohnungen_pro_strasse = defaultdict(int)

    for strname, count in aggregated_wohnungen_by_street.items():
        total_wohnungen_pro_strasse[strname] += count

    # Ausgabe der Summe der Wohnungen pro Straße
    for strname, total_count in sorted(total_wohnungen_pro_strasse.items()):
        print(f"  {strname}: {total_count}")

    print("-------------------------------------------------------")
    print(f"Gesamtanzahl Adressen im Polygon: {total_adressen}")
    print("-------------------------------------------------------")

    print("-------------------------------------------------------")
    print(f"Gesamtanzahl Wohnungen im Polygon: {total_wohnungen}")
    print("-------------------------------------------------------")


    total_geschaefte, place_and_address_df, total_places_pro_adresse_df, release_date = extract_overture(polygon)
    print("-------------------------------------------------------")
    print(f"Anzahl der Geschäfte im Polygon: {total_geschaefte}")
    print("-------------------------------------------------------")


    print("ende")
