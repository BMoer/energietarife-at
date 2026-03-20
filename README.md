# energietarife-at — Österreichische Energietarife (Strom & Gas)

Scraper und offene Daten für alle Strom- und Gas-Tarife in Österreich, direkt von der [E-Control](https://www.e-control.at/tarifkalkulator).

> 171 aktuelle Tarife · 22 Netzgebiete · historische Daten · JSON API · keine Registrierung nötig

## Was ist das?

Die E-Control ist Österreichs Energieregulator und veröffentlicht alle verfügbaren Strom- und Gas-Tarife. Dieses Projekt scraped diese Daten regelmäßig und stellt sie als **offene JSON-Dateien** und **REST API** zur Verfügung.

- **Scraper**: Pure Python (stdlib only, keine Dependencies), speichert in SQLite
- **JSON Export**: Aktuelle und historische Tarife als statische JSON-Dateien
- **REST API**: Gehostet auf Vercel, frei nutzbar, CORS-enabled

## Schnellstart

### Daten direkt nutzen (kein Setup nötig)

```bash
# Aktuelle Stromtarife abrufen
curl "https://energietarife-at.vercel.app/api/v1/current?energy_type=POWER"

# Gastarife filtern
curl "https://energietarife-at.vercel.app/api/v1/current?energy_type=GAS&brand=Wien%20Energie"

# Historische Tarife mit Preisfilter
curl "https://energietarife-at.vercel.app/api/v1/tariffs?energy_type=POWER&max_price=20"
```

### Selbst scrapen

```bash
git clone https://github.com/BMoer/energietarife-at.git
cd energietarife-at

# Alle Tarife scrapen (Strom + Gas) → tarife.db
python3 scrape_tarife.py

# Nur Strom
python3 scrape_tarife.py --energy-type POWER

# Datenbank abfragen
python3 query_tarife.py summary
python3 query_tarife.py cheapest --energy-type POWER -n 10
python3 query_tarife.py brands
python3 query_tarife.py compare 1060 --energy-type POWER
```

Benötigt **Python 3.12+**, keine externen Pakete.

## API Referenz

Basis-URL: `https://energietarife-at.vercel.app/api/v1`

### `GET /current`

Aktuelle Tarife aller Anbieter.

| Parameter | Beschreibung |
|-----------|-------------|
| `energy_type` | `POWER` oder `GAS` |
| `brand` | Anbieter-Name (Teilstring) |
| `limit` | Max. Ergebnisse (default: 1000, max: 5000) |
| `offset` | Pagination |

### `GET /tariffs`

Historische Tarife mit erweiterten Filtern.

| Parameter | Beschreibung |
|-----------|-------------|
| `energy_type` | `POWER` oder `GAS` |
| `brand` | Anbieter-Name (Teilstring) |
| `from` / `to` | Zeitraum (ISO-Datum) |
| `min_price` / `max_price` | Preisfilter (ct/kWh) |
| `limit` / `offset` | Pagination |

### `GET /brands`

Alle Anbieter mit Tarifanzahl.

### `GET /stats`

Zusammenfassung: Anzahl Tarife, Anbieter, Preisspanne.

## Daten-Pipeline

```
scrape_tarife.py          → tarife.db (aktuelle Tarife, alle Netzgebiete)
scrape_all_regions.py     → tarife.db (historische Tarife, 22 Netzgebiete)
scrape_plz_mapping.py     → tarife.db (PLZ → Netzgebiet Mapping)
build_site.py             → public/data/*.json (Export für API)
```

## Datenformat

Jeder Tarif enthält:

```json
{
  "product_id": 1171774,
  "product_name": "ED Flex 1.0",
  "brand_name": "EnergieDirect Austria GmbH",
  "energy_type": "POWER",
  "grid_operator_name": "Wiener Netze GmbH",
  "zip_code": "1010",
  "annual_eur": 904.33,
  "energy_eur": 274.47,
  "base_rate_eur": 36.00,
  "annual_consumption_kwh": 3500,
  "is_certified_green": 0,
  "price_guarantee_type": null,
  "scraped_at": "2026-03-11T14:52:39+00:00"
}
```

## Verwandte Projekte

- **[energietools](https://github.com/BMoer/energietools)** — Python-Toolkit für den österreichischen Energiemarkt (Tarifvergleich, Lastprofil-Analyse, PV/Batterie-Simulation)

## Datenquelle

Alle Daten stammen von der [E-Control](https://www.e-control.at), Österreichs Regulierungsbehörde für Energie. Die E-Control stellt diese Informationen öffentlich über ihren Tarifkalkulator zur Verfügung.

## Lizenz

MIT
