# Notebook: Abfrage der Anzahl Wohnungen für ein Polygon über die GeoAdmin API

# Benötigte Bibliotheken
import requests
import geopandas as gpd
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon
from collections import defaultdict
import json

# 1. Laden des KML-Polygons direkt aus der URL
def resolve_kml_url(shortened_url):
    """Löst eine gekürzte URL auf, extrahiert die KML-URL und entfernt '&featureInfo=default'."""
    response = requests.head(shortened_url, allow_redirects=True)
    if response.status_code != 200:
        raise ValueError(f"Fehler beim Auflösen der URL: {response.status_code}")

    # Suche nach einer URL, die mit 'https://public.geo.admin.ch/api/kml' beginnt
    if "https://public.geo.admin.ch/api/kml" in response.url:
        start_index = response.url.find("https://public.geo.admin.ch/api/kml")
        clean_url = response.url[start_index:]

        # Entfernen von '&featureInfo=default', falls vorhanden
        if "&featureInfo=default" in clean_url:
            clean_url = clean_url.split("&featureInfo=default")[0]

        return clean_url

    raise ValueError("Keine gültige Zeichnung  gefunden.")




def load_kml_polygon_directly(kml_url):
    """Parst ein KML-Polygon direkt aus einer URL und gibt es als GeoDataFrame zurück."""
    resolved_url = resolve_kml_url(kml_url)
    response = requests.get(resolved_url)
    if response.status_code != 200:
        raise ValueError(f"Fehler beim Laden der KML-Datei: {response.status_code}")

    # XML-Inhalt parsen
    root = ET.fromstring(response.content)
    namespace = {"kml": "http://www.opengis.net/kml/2.2"}

    # Koordinaten extrahieren
    coordinates = root.find(".//kml:coordinates", namespace).text.strip()
    coords = [
        tuple(map(float, coord.split(",")[:2]))
        for coord in coordinates.split()
    ]

    # Polygon erstellen
    polygon = Polygon(coords)
    return polygon

# Beispiel-KML-Link (ersetzen Sie diesen durch Ihren eigenen Link)
#kml_url = "https://s.geo.admin.ch/3lwgn09kgqha" #Schliern
# kml_url = "https://s.geo.admin.ch/p56ijeogsuta" #SChliern gross
#kml_url = "https://s.geo.admin.ch/jpve0fg64vai" #köniz
#kml_url = "https://s.geo.admin.ch/nct5odun6mkp"
kml_url = input("Bitte Link zur Zeichnung eingeben: ")
print(f"Link zur Zeichnung ist: {kml_url}")


polygon = load_kml_polygon_directly(kml_url)

# 2. Abfrage an die GeoAdmin API senden
def query_geoadmin_with_polygon(polygon, sr=4326):
    """Sendet eine Anfrage an die GeoAdmin API mit einem Polygon."""
    endpoint = "https://api3.geo.admin.ch/rest/services/api/MapServer/identify"

    # Polygon in das API-Format konvertieren
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

    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# Anfrage an die GeoAdmin API mit Polygon
try:
    geoadmin_result = query_geoadmin_with_polygon(polygon)
    print("Abfrage erfolgreich!")
except Exception as e:
    print(f"Fehler bei der Abfrage: {e}")

# 3. Verarbeitung der Ergebnisse und Filterung nach Polygon
def extract_wohnungen_and_counts(result):
    """Extrahiert die Gesamtanzahl der Wohnungen, die Anzahl pro Straßennamen und die Anzahl der Features."""
    total_wohnungen = 0
    wohnungen_by_street = defaultdict(int)
    total_features = 0  # Variable zum Zählen der Features

    if result and 'results' in result:
        total_features = len(result['results'])  # Anzahl der Features bestimmen
        for feature in result['results']:
            attributes = feature.get('attributes', {})
            ganzwhg = attributes.get('ganzwhg', 0) or 0
            strname = attributes.get('strname_deinr', "Unbekannt")

            total_wohnungen += ganzwhg
            wohnungen_by_street[strname] += ganzwhg

    # Ausgabe der Anzahl der gefundenen Features
    print(f"Anzahl der gefundenen Adressen: {total_features}")

    # Warnung ausgeben, wenn die Anzahl der Features 200 oder mehr beträgt
    if total_features >= 200:
        print("***************")
        print("Warnung: Mehr als 200 Adressen. Bitte unterteilen Sie die Zeichnung in kleinere Abschnitte und führen Sie die Abfrage mehrfach aus.")
        print("***************")
        wohnungen_by_street = defaultdict(int)
        total_wohnungen = 0


    return total_wohnungen, wohnungen_by_street



# Gesamtanzahl der Wohnungen und Verteilung berechnen
if geoadmin_result:
    anzahl_wohnungen, wohnungen_pro_strasse = extract_wohnungen_and_counts(geoadmin_result)

    print("Wohnungen nach Straßennamen:")
    for strname, count in sorted(wohnungen_pro_strasse.items()):
        print(f"  {strname}: {count}")
    print("-------------------------------------------------------")
    print(f"Gesamtanzahl Wohnungen im Polygon: {anzahl_wohnungen}")
    print("-------------------------------------------------------")
else:
    print("Keine Ergebnisse verfügbar.")
