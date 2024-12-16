# Wie viele Briefkästen gibt es?

Eine interaktive Web-Anwendung zur Analyse der Gesamtanzahl von Wohnungen und darausfolgend Briefkästen in einem benutzerdefinierten Gebiet in der Schweiz. Dieses Tool eignet sich ideal für Zielgruppenanalysen, z. B. zur Planung von Marketingmassnahmen wie Flyer-Verteilung in Quartieren.
Grundannahme: jede Wohnung verfügt auch über einen Briefkasten..

-> website der Anwendung: [Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

## Hauptmerkmale
- **Polygon-Zeichenfunktion:** Benutzer können Polygone auf einer interaktiven Karte zeichnen oder aus einem map.geo.admin.ch link generieren
- **Automatische Unterteilung großer Polygone:** API-Limitierungen werden durch die Aufteilung in kleinere Polygone umgangen.
- **GeoAdmin API-Integration:** Präzise Datenabfragen aus dem täglich aktualisierten schweizerischen Gebäude- und Wohnungsregister des BFS.
- **Ergebnisanzeige:** Darstellung der aggregierten Wohnungsdaten nach Adressen und Strassen.
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

Installieren Sie die benötigten Bibliotheken mit:
```bash
pip install -r requirements.txt
```

### Dateien
Das Projekt besteht aus zwei Hauptdateien:
1. **app.py**: Hauptanwendung für die interaktive Nutzung via streamlit.io [Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)
2. **madd_extract.py**: Hilfsfunktionen, die von der Hauptanwendung verwendet werden.

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
- **Wohnungsdetails nach Adresse und Straße:**
  Tabellen mit sortierten Daten.
- **Warnungen bei API-Limits:**
  Hinweise zur Verkleinerung der Polygone.

## Beispiel
[Wieviele Briefkästen gibt es?](https://wieviele-briefkaesten-gibt-es.streamlit.app)

## Lizenz
© 2024 David Oesch. Dieses Projekt ist unter der [MIT-Lizenz](LICENSE.txt) lizenziert.

## Kontakt
Haben Sie Fragen oder Verbesserungsvorschläge? [David Oesch auf GitHub](https://github.com/davidoesch).
