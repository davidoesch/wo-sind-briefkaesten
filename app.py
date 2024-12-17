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
            "edit": False,
            "remove": False
        }
    ).add_to(m)
    return m




# Hauptprogramm
# Streamlit app

st.set_page_config(
    page_title="Briefkasten",
    page_icon="📮",
)
st.title("Wieviele Briefkästen gibt es ?")

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
            progress_text.text(f"Verbleibende Subsets: {len(sub_polygons) - i}")
            result = query_geoadmin_with_polygon(sub_polygon)
            if result:
                sub_total_wohnungen, sub_wohnungen_by_streetnr, sub_wohnungen_by_street = extract_wohnungen_and_counts(result)
                total_wohnungen += sub_total_wohnungen
                for street, count in sub_wohnungen_by_streetnr.items():
                    aggregated_wohnungen_by_streetnr[street] += count
                for street, count in sub_wohnungen_by_street.items():
                    aggregated_wohnungen_by_street[street] += count
            progress_bar.progress((i + 1) / len(sub_polygons))
        progress_text.text("Analyse erfolgreich abgeschlossen")

        # Briefkästen direkt anzeigen
        st.subheader(f"Briefkästen: {total_wohnungen}")
        st.write(f"Entspricht der Gesamtanzahl Wohnungen  im Polygon")

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
else:
    st.warning("Bitte zeichnen Sie zuerst ein Polygon auf der Karte.")

st.write("")
st.write("")


st.markdown("---")
st.write(
    "🏠 **Wohnungs-Briefkasten-Analyse** © 2024 David Oesch "
)
st.markdown(
    "Mehr infos und :star: unter [github.com/davidoesch/wo-sind-briefkaesten](https://github.com/davidoesch/wo-sind-briefkaesten)"
)