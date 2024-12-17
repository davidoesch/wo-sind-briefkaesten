import requests
import geopandas as gpd
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon, box
from shapely.ops import split
from collections import defaultdict
import json
import numpy as np
from swiftshadow import QuickProxy


def resolve_kml_url(shortened_url):
    """Löst eine gekürzte URL auf, extrahiert die KML-URL und entfernt '&featureInfo=default'."""
    print(f"Aufruf shortened  public.geo.admin.ch ... ")
    response = requests.head(shortened_url, allow_redirects=True, timeout=15)
    print(f"... erhalten ")
    if response.status_code != 200:
        raise ValueError(f"Fehler beim Auflösen der URL: {response.status_code}")

    if "https://public.geo.admin.ch/api/kml" in response.url:
        start_index = response.url.find("https://public.geo.admin.ch/api/kml")
        clean_url = response.url[start_index:]

        if "&featureInfo=default" in clean_url:
            clean_url = clean_url.split("&featureInfo=default")[0]

        return clean_url

    raise ValueError("Keine gültige Zeichnung gefunden.")

def load_kml_polygon_directly(kml_url):
    """Parst ein KML-Polygon direkt aus einer URL und gibt es als Shapely-Polygon zurück."""
    resolved_url = resolve_kml_url(kml_url)
    print(f"Aufruf public.geo.admin.ch ... ")
    response = requests.get(resolved_url, timeout=15)
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
                # Check if either gkat or gklas matches the specified values
                if gkat in {1010, 1020, 1030, 1040,1060} or gklas in {1110,1121,1122,1130,1211,1212,1220,1230,1231,1241,1242,1251,1261,1262,1263,1264,1275}:
                    ganzwhg = 1

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
    kml_url = input("Bitte Link zur Zeichnung eingeben: ")
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
