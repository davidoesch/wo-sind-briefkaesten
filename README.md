  # Wie viele  Zustelladressen / Briefkästen gibt es?

  Eine interaktive Web-Anwendung zur Schätzung der Gesamtanzahl von  Zustelladressen / Briefkästen für einen benutzerdefinierten Perimeter basierend auf der Anzahl Wohnungen gemäss [Eidg. Gebäude- und Wohnungsregister (GWR) des Bundesamt für Statistik BFS](https://www.bfs.admin.ch/bfs/de/home/register/gebaeude-wohnungsregister.html) und Anzahl Geschäfte gemäss <em>places</em> von [Overture Maps Foundation](https://overturemaps.org). Dieses Tool eignet sich ideal für Zielgruppenanalysen, z. B. zur Planung von Marketingmassnahmen wie Flyer-Verteilung in Quartieren.

  -> website der Anwendung: [Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

  ## Grundannahme:
  ### Wohnungen
  Annahme: Jede Wohnung 'ganzwhg' gemäss [Merkmalskatalog 4.2 des GWR](https://www.housing-stat.ch/de/help/42.html) verfügt auch über einen Briefkasten als Zustelladresse. 

  ### Geschäfte
  Annahme: Die Zustelladresse / Briefkasten entsprechen dem [<em>Overture places</em> ](https://docs.overturemaps.org/guides/places/) Datensatz. Kategorien wie *park* etc können über die Resultateliste gefiltert werden. Zusätzlich werden
  für Adressen mit folgenden CODES gemäss [Merkmalskatalog 4.2 des GWR](https://www.housing-stat.ch/de/help/42.html) mindestens eine ustelladresse / Briefkasten hinzugefügt:

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


  ## Hauptmerkmale
  - **Polygon-Zeichenfunktion:** Benutzer können Polygone auf einer interaktiven Karte zeichnen oder aus einem map.geo.admin.ch link generieren
  - **Automatische Unterteilung großer Polygone:** API-Limitierungen werden durch die Aufteilung in kleinere Polygone umgangen.
  - **GeoAdmin API-Integration:** Präzise Datenabfragen aus dem täglich aktualisierten schweizerischen Gebäude- und Wohnungsregister des BFS.
  - **Overture Maps Foundation Abfrage-Integration: :** Präzise Datenabfragen aus dem OSM / Overture Maps, monatlich (?) aktualisierten Places verzeichnis.
  - **Ergebnisanzeige:** Darstellung der aggregierten Wohnungs/Geschäfstdaten nach Adressen und Strassen.
  - **Exportoption:** Ergebnisse können als Tabelle angezeigt und weiterverarbeitet werden.

  ## Anforderungen
  ### Python-Bibliotheken
  - `streamlit`
  - `requests`
  - `geopandas`
  - `shapely`
  - `numpy`
  - `pandas`
  - `folium`
  - `streamlit_folium`
  - `duckdb`

  Installieren Sie die benötigten Bibliotheken mit:
  ```bash
  pip install -r requirements.txt
  ```

  ### Dateien
  Das Projekt besteht aus zwei Hauptdateien:
  1. **app.py**: Hauptanwendung für die interaktive Nutzung via streamlit.io [Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)
  2. **madd_extract.py**: python Funktionen, wie sie in der Hauptanwendung verwendet werden.
  3. **overture.py**: python Funktion um via DUCKDB auf overturemaps zuzugreifen.

  ## Funktionen
  ### app.py
  - **Kartenanzeige mit Zeichentools:**
    Interaktive Karte, die das Zeichnen von Polygonen erlaubt.
  - **Polygon-Validierung:**
    Warnung bei großen Polygonen (>10 km²), die zu langen Ladezeiten führen können.
  - **API-Abfrage:**
    Automatisierte Abfragen mit Unterstützung zur Unterteilung großer Polygone.
  - **Ergebnisanzeige:**
    - Gesamtanzahl der Wohnungen
    - Details nach Adresse und Straße
  - **Progressbar:**
    Zeigt den Fortschritt der Verarbeitung bei mehreren Subsets an.

  ### madd_extract.py
  - **split_polygon:**
    Teilt ein großes Polygon in kleinere Polygone basierend auf einer maximalen Fläche.
  - **query_geoadmin_with_polygon:**
    Sendet API-Abfragen an GeoAdmin basierend auf einem gegebenen Polygon.
  - **extract_wohnungen_and_counts:**
    Aggregiert Wohnungsinformationen aus der API-Antwort.
  - **create_map:**
    Erstellt eine interaktive Karte mit Folium und Zeichenwerkzeugen.

  ### overture.py
  - **extract_freeform:**
  Extrahiert 'freeform'-Felder aus einer Liste von Adresswörterbüchern oder einem JSON-String.

  - **clean_df:**
  Bereinigt und transformiert einen DataFrame, indem bestimmte Spalten verarbeitet werden.

  - **fetch_latest_release_date:**
  Ruft das Veröffentlichungsdatum der neuesten Version von Overture Maps von der Website ab.



  ## Nutzung
  ### Lokale Ausführung
  1. Klonen Sie das Repository:
    ```bash
    git clone https://github.com/davidoesch/wo-sind-briefkaesten.git
    ```
  2. Wechseln Sie in das Verzeichnis:
    ```bash
    cd wo-sind-briefkaesten
    ```
  3. Starten Sie die Streamlit-Anwendung:
    ```bash
    streamlit run app.py
    ```

  ### Interaktive Nutzung

  website: [Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

  1. Zeichnen Sie ein Polygon auf der Karte.
  2. Klicken Sie auf den "Berechnen"-Button.
  3. Sehen Sie sich die aggregierten Ergebnisse direkt in der App an.

  ## Ergebnisse
  - **Gesamtanzahl der Briefkästen:**
    Anzahl der Wohnungen im gezeichneten Polygon.
  - **Wohnungs/ Geschäftsdetails nach Adresse und Straße:**
    Tabellen mit sortierten Daten.
  - **Warnungen bei API-Limits:**
    Hinweise zur Verkleinerung der Polygone.

  ## Beispiel
  [Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

  ## Lizenz
  © 2024 David Oesch. Dieses Projekt ist unter der [MIT-Lizenz](LICENSE.txt) lizenziert.

  ## Kontakt
  Haben Sie Fragen oder Verbesserungsvorschläge? [David Oesch auf GitHub](https://github.com/davidoesch).
