"""
Breifkästen  Query Tool

Diese Streamlit-Anwendung ermöglicht die Berechnung der Gesamtanzahl von Wohnungen innerhalb eines benutzerdefinierten Polygons auf einer Karte.
Benutzer können Polygone zeichnen, die durch Unterteilung in kleinere Polygone verarbeitet werden, um API-Limits von 200 Adressen einzuhalten.

Hauptmerkmale:
- Integration der GeoAdmin API für Datenabfragen.
- Verwendung von Folium zur interaktiven Kartenanzeige.
- Ergebnisanzeige der berechneten Wohnungsdaten.

Benötigte Bibliotheken:
- streamlit
- requests
- geopandas
- shapely
- numpy
- pandas
- folium
- streamlit_folium
"""

import streamlit as st
import requests
import geopandas as gpd
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon, box
from shapely.ops import split
from collections import defaultdict
import json
import numpy as np
import pandas as pd
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
import duckdb as db
import ast
import requests
import re
from bs4 import BeautifulSoup

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


def split_polygon(polygon, max_area, export_gpkg=False, gpkg_path="grid_output.gpkg"):
    """Teilt ein Polygon in kleinere Polygone, deren Fläche eine vorgegebene Maximalgröße nicht überschreitet.

    Args:
        polygon (shapely.geometry.Polygon): Das zu teilende Polygon.
        max_area (float): Maximale Fläche eines Teilpolygons.
        export_gpkg (bool, optional): Gibt an, ob die Teilpolygone als GeoPackage exportiert werden sollen. Standard: False.
        gpkg_path (str, optional): Pfad zur Ausgabe des GeoPackages. Standard: "grid_output.gpkg".

    Returns:
        list: Liste der generierten Teilpolygone.
    """
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
    """Sendet eine Anfrage an die GeoAdmin API mit einem gegebenen Polygon.

    Args:
        polygon (shapely.geometry.Polygon): Das Polygon für die Anfrage.
        sr (int, optional): Raumbezugssystem (Spatial Reference). Standard: 4326 (WGS84).

    Returns:
        dict: Das Antwort-JSON der API.
    """
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

        #prox=QuickProxy()
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
                # Check if either gkat or gklas matches the specified values
                if gkat in {1010, 1020, 1030, 1040,1060} or gklas in {1110,1121,1122,1130,1211,1212,1220,1230,1231,1241,1242,1251,1261,1262,1263,1264,1275}:
                    ganzwhg = 1

            strnamenr = attributes.get('strname_deinr', "Unbekannt")
            strname = ", ".join(attributes.get('strname', "Unbekannt"))
            total_wohnungen += ganzwhg
            wohnungen_by_streetnr[strnamenr] += ganzwhg
            wohnungen_by_street[strname] += ganzwhg

        print(f"Anzahl der gefundenen Adressen: {total_features}")
        total_adressen = total_adressen + total_features

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


def create_map(center, zoom):
    """Erstellt eine interaktive Karte mit Zeichentools.

    Args:
        center (list): Mittelpunkt der Karte [Breitengrad, Längengrad].
        zoom (int): Zoomstufe der Karte.

    Returns:
        folium.Map: Eine Folium-Karte mit Zeichentools.
    """
    m = folium.Map(location=center,
        zoom_start=zoom,
        control_scale=True,
        tiles="https://wmts.geo.admin.ch/1.0.0/ch.swisstopo.pixelkarte-farbe/default/current/3857/{z}/{x}/{y}.jpeg",
        attr='Map data: &copy; <a href="https://www.swisstopo.ch" target="_blank" rel="noopener noreferrer">swisstopo</a>, <a href="https://www.housing-stat.ch/" target="_blank" rel="noopener noreferrer">BFS</a>',
        )

    Draw(
        export=False,
        position="topleft",
        draw_options={
            "polyline": False,
            "rectangle": False,
            "circle": False,
            "marker": False,
            "circlemarker": False,
            "polygon": {
                "shapeOptions": {
                    "color": "#ff0000"
                },
            },
        },
        edit_options={
            "edit": False
        }
    ).add_to(m)
    return m

def extract_overture(polygon):        # Initial setup
        """
        Extracts place names and addresses from Overture Maps data within a specified polygon.
        This function connects to a DuckDB database, installs and loads necessary extensions,
        fetches the latest release information from the Overture Maps GitHub repository, constructs
        the parquet path using the latest release date, and performs a spatial query to extract
        place names and addresses within the specified polygon.
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

        # Extract place names and addresses
        place_and_address_df = result_df[['primary_name', 'addresses','category','category_alt']].dropna()

        # Apply the extract freeform function to the 'addresses' column
        place_and_address_df['flattened_addresses'] = place_and_address_df['addresses'].apply(extract_freeform)

        # Drop the original 'addresses' column for cleaner output (optional)
        place_and_address_df = place_and_address_df.drop(columns=['addresses'])

        # converting dict to list
        place_and_address_df = clean_df(place_and_address_df, 'category_alt')

        # Display the resulting DataFrame
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


# Hauptprogramm
# Streamlit app
release_date = "-"
gh_release= "-"
gh_date= "-"

st.set_page_config(
    page_title="Briefkasten",
    page_icon="📮",
)
st.title("Wieviele Briefkästen gibt es ?")
st.markdown("""
Hier können Sie schnell und einfach die Anzahl der Zulieferadressen / Briefkästen in einem Gebiet berechnen. Zeichnen Sie einfach ein Polygon auf der Karte, und wir analysieren die Daten für Sie:

1. **Zeichnen Sie ein Gebiet auf der Karte ein.**
2. **Klicken Sie auf *Berechnen*.**
3. **Erhalten Sie die Anzahl der Zustelladressen.**

""")

m = create_map(center=[46.8182, 8.2275], zoom=8)  # Centered on Switzerland
output = st_folium(m, width=700, height=500)

if st.button("Berechnen", key="calculate_button"):
    if output["last_active_drawing"]:
        drawn_polygon = output["last_active_drawing"]["geometry"]["coordinates"][0]
        polygon = Polygon(drawn_polygon)
        #st.write(f"polyarea: {polygon.area }")
        if polygon.area > 0.001:  # 0.001 entspricht ungefähr 10 km²
            st.warning("Das gezeichnete Polygon ist grösser als 10 km². Die Berechnung kann sehr lange dauern und möglicherweise aufgrund von API-Limitierungen von geo.admin.ch abbrechen.")

        # Use the drawn polygon for calculations
        max_area = 0.000005
        sub_polygons = split_polygon(polygon, max_area)

        total_adressen = 0
        total_wohnungen = 0
        aggregated_wohnungen_by_streetnr = defaultdict(int)
        aggregated_wohnungen_by_street = defaultdict(int)

        progress_bar = st.progress(0)
        progress_text = st.empty()  # Platzhalter für Fortschrittsanzeige

        # Iteriere über Subsets mit Countdown
        for i, sub_polygon in enumerate(sub_polygons):
            progress_text.text(f"Auslesen Wohnungen(subset): {len(sub_polygons) - i}")
            result = query_geoadmin_with_polygon(sub_polygon)
            if result:
                sub_total_wohnungen, sub_wohnungen_by_streetnr, sub_wohnungen_by_street = extract_wohnungen_and_counts(result)
                total_wohnungen += sub_total_wohnungen
                for street, count in sub_wohnungen_by_streetnr.items():
                    aggregated_wohnungen_by_streetnr[street] += count
                for street, count in sub_wohnungen_by_street.items():
                    aggregated_wohnungen_by_street[street] += count
            progress_bar.progress((i + 1) / len(sub_polygons))
        progress_text.text("Auslesen Wohnungen abgeschlossen")

        # Geschäfte extraktion
        with st.spinner('Auslesen Geschäfte (dauert ca 1 min)...'):
            total_geschaefte, place_and_address_df, total_places_pro_adresse_df, release_date = extract_overture(polygon)
        print(f"Anzahl der Geschäfte: {total_geschaefte}")


        # Briefkästen direkt anzeigen
        total_briefkaesten = total_wohnungen + total_geschaefte
        st.subheader(f"Briefkästen: {total_briefkaesten}")
        st.markdown(f"Entspricht der Summe der [Wohnungen](https://github.com/davidoesch/wo-sind-briefkaesten/tree/master?tab=readme-ov-file#wohnungen): {total_wohnungen}  und der Summe der [Geschäfte](https://github.com/davidoesch/wo-sind-briefkaesten/tree/master?tab=readme-ov-file#geschäfte) {total_geschaefte} im Polygon")

        # Details als Tabellen anzeigen
        with st.expander("Details: Wohnungen nach Adressen"):
            adressen_df = pd.DataFrame(
                [{"Adresse": adr, "Wohnungen": count} for adr, count in aggregated_wohnungen_by_streetnr.items()]
            )
            if not adressen_df.empty:
                adressen_df_sorted = adressen_df.sort_values("Adresse")
                st.write(adressen_df_sorted)
            else:
                st.write("Keine Adressen gefunden.")


        with st.expander("Details: Wohnungen nach Strassen"):
            strassen_df = pd.DataFrame(
                [{"Strasse": street, "Wohnungen": count} for street, count in aggregated_wohnungen_by_street.items()]
            )
            if not strassen_df.empty:
                strassen_df_sorted = strassen_df.sort_values("Strasse")
                st.write(strassen_df_sorted)
            else:
                st.write("Keine Strassen gefunden.")


        with st.expander("Details: Adressen"):
            st.write(f"Gesamtanzahl Adressen im Polygon: {total_adressen}")

        #Tabelle mit total_places_pro_adresse anzeigen
        with st.expander("Details: Geschäfte nach Adressen"):
            if not total_places_pro_adresse_df.empty:
                st.write(total_places_pro_adresse_df)
            else:
                st.write("Keine Geschäfte gefunden.")

        # Tabelle mit place_and_address_df anzeigen
        with st.expander("Details: Geschäfte"):
            if not place_and_address_df.empty:
                st.write(place_and_address_df)
            else:
                st.write("Keine Geschäfte gefunden.")


else:
    st.warning("Bitte zeichnen Sie zuerst ein Polygon auf der Karte.")

st.write("")
st.write("")


gh_release,gh_date=get_latest_release_date("https://github.com/davidoesch/wo-sind-briefkaesten/")

st.markdown("---")
st.write(
    f"🏠 **Wohnungs-Briefkasten-Analyse** © 2024 David Oesch, [Overture Maps Foundation](https://overturemaps.org), Overture  Release {release_date},  Anwendung Version: {gh_release} vom {gh_date}"
)
st.markdown(
    "Mehr infos und :star: unter [github.com/davidoesch/wo-sind-briefkaesten](https://github.com/davidoesch/wo-sind-briefkaesten)"
)