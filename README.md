  # How many delivery addresses / mailboxes are there?

  An interactive web application to estimate the total number of delivery addresses / mailboxes (Zustelladressen / Briefkästen )for a user-defined perimeter in Switzerland based on the number of apartments according to the [Federal Building and Housing Register (GWR) of the Federal Statistical Office BFS](https://www.bfs.admin.ch/bfs/de/home/register/gebaeude-wohnungsregister.html) and the number of businesses according to <em>places</em> from the [Overture Maps Foundation](https://overturemaps.org). This tool is ideal for target group analysis, e.g., for planning marketing measures such as flyer distribution in neighborhoods.

  -> application website (DE): [How many mailboxes are there?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

  ## Basic assumption:
  ### Apartments
  Assumption: Each apartment 'ganzwhg' according to [Feature Catalog 4.2 of the GWR](https://www.housing-stat.ch/de/help/42.html) also has a mailbox as a delivery address.

  ### Businesses
  Assumption: The delivery address / mailbox corresponds to the [<em>Overture places</em> ](https://docs.overturemaps.org/guides/places/) dataset. Categories such as *park* etc. can be filtered out from the result list. Additionally, at least one delivery address / mailbox is added for addresses with the following CODES according to [Feature Catalog 4.2 of the GWR](https://www.housing-stat.ch/de/help/42.html):

  | CODE | KAT   | BESCHREIBUNG                                              |
  | ---- | ----- | --------------------------------------------------------- |
  | 1010 | GKAT  | Provisorische Unterkunft                                  |
  | 1020 | GKAT  | Gebäude mit ausschliesslicher Wohnnutzung                 |
  | 1030 | GKAT  | Andere Wohngebäude (Wohngebäude mit Nebennutzung)         |
  | 1040 | GKAT  | Gebäude mit teilweiser Wohnnutzung                        |
  | 1060 | GKAT  | Gebäude ohne Wohnnutzung                                  |
  | 1110 | GKLAS | Gebäude mit einer Wohnung                                 |
  | 1121 | GKLAS | Gebäude mit zwei Wohnungen                                |
  | 1122 | GKLAS | Gebäude mit drei oder mehr Wohnungen                      |
  | 1130 | GKLAS | Wohngebäude für Gemeinschaften                            |
  | 1211 | GKLAS | Hotelgebäude                                              |
  | 1212 | GKLAS | Andere Gebäude für kurzfristige Beherbergung              |
  | 1220 | GKLAS | Bürogebäude                                               |
  | 1230 | GKLAS | Gross-und Einzelhandelsgebäude                            |
  | 1231 | GKLAS | Restaurants und Bars in Gebäuden ohne Wohnnutzung         |
  | 1241 | GKLAS | Gebäude des Verkehrs- und Nachrichtenwesens ohne Garagen  |
  | 1242 | GKLAS | Garagengebäude                                            |
  | 1251 | GKLAS | Industriegebäude                                          |
  | 1261 | GKLAS | Gebäude für Kultur- und Freizeitzwecke                    |
  | 1262 | GKLAS | Museen und Bibliotheken                                   |
  | 1263 | GKLAS | Schul- und Hochschulgebäude                               |
  | 1264 | GKLAS | Krankenhäuser und Facheinrichtungen des Gesundheitswesens |
  | 1275 | GKLAS | Andere Gebäude für die kollektive Unterkunft              |

  ## Main features
  - **Polygon drawing function:** Users can draw polygons on an interactive map or generate from a map.geo.admin.ch link.
  - **Automatic subdivision of large polygons:** API limitations are circumvented by dividing into smaller polygons.
  - **GeoAdmin API integration:** Precise data queries from the daily updated Swiss Building and Housing Register of the BFS.
  - **Overture Maps Foundation query integration:** Precise data queries from the OSM / Overture Maps, monthly (?) updated places directory.
  - **Result display:** Presentation of aggregated apartment/business data by address and street.
  - **Export option:** Results can be displayed as a table and further processed.

  ## Requirements
  ### Python libraries
  - `streamlit`
  - `requests`
  - `geopandas`
  - `shapely`
  - `numpy`
  - `pandas`
  - `folium`
  - `streamlit_folium`
  - `duckdb`

  Install the required libraries with:
  ```bash
  pip install -r requirements.txt
  ```

  ### Files
  The project consists of three main files:
  1. **app.py**: Main application for interactive use via streamlit.io [How many mailboxes are there?](https://wieviele-briefkaesten-gibt-es.streamlit.app)
  2. **madd_extract.py**: Python functions as used in the main application.
  3. **overture.py**: Python function to access overturemaps via DUCKDB.

  ## Functions
  ### app.py
  - **Map display with drawing tools:**
    Interactive map that allows drawing polygons.
  - **Polygon validation:**
    Warning for large polygons (>10 km² and >150 km²) that may lead to long loading times.
  - **API query:**
    Automated queries with support for subdividing large polygons.
  - **Result display:**
    - Total number of apartments
    - Details by address and street
  - **Progress bar:**
    Shows the progress of processing multiple subsets.

  ### madd_extract.py
  - **split_polygon:**
    Splits a large polygon into smaller polygons based on a maximum area.
  - **query_geoadmin_with_polygon:**
    Sends API queries to GeoAdmin based on a given polygon.
  - **extract_wohnungen_and_counts:**
    Aggregates apartment information from the API response.
  - **create_map:**
    Creates an interactive map with Folium and drawing tools.

  ### overture.py
  - **extract_freeform:**
    Extracts 'freeform' fields from a list of address dictionaries or a JSON string.

  - **clean_df:**
    Cleans and transforms a DataFrame by processing certain columns.

  - **fetch_latest_release_date:**
    Retrieves the release date of the latest version of Overture Maps from the website.

  ## Usage
  ### Local execution
  1. Clone the repository:
    ```bash
    git clone https://github.com/davidoesch/wo-sind-briefkaesten.git
    ```
  2. Change to the directory:
    ```bash
    cd wo-sind-briefkaesten
    ```
  3. Start the Streamlit application:
    ```bash
    streamlit run app.py
    ```

  ### Interactive use

  website: [How many mailboxes are there?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

  1. Draw a polygon on the map.
  2. Click the "Calculate" button.
  3. View the aggregated results directly in the app.

  ## Results
  - **Total number of mailboxes:**
    Number of apartments in the drawn polygon.
  - **Apartment/business details by address and street:**
    Tables with sorted data.
  - **Warnings for API limits:**
    Notes on reducing the size of the polygons.

  ## Example
  [How many mailboxes are there?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

  ## License
  © 2024 David Oesch. This project is licensed under the [MIT License](LICENSE.txt).

  ## Contact
  Do you have questions or suggestions for improvement? [David Oesch on GitHub](https://github.com/davidoesch).
