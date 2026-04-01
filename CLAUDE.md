# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Austrian energy tariff scraper and query tool for the E-Control Tarifkalkulator (e-control.at). Scrapes electricity (POWER) and gas (GAS) tariffs via the E-Control REST API and stores them in a SQLite database. Pure Python 3.12+ with no external dependencies (stdlib only).

## Commands

```bash
# Scrape all tariffs (POWER + GAS) into tarife.db
python3 scrape_tarife.py

# Scrape only one energy type
python3 scrape_tarife.py --energy-type POWER
python3 scrape_tarife.py --energy-type GAS

# Use a different database file
python3 scrape_tarife.py --db my_tarife.db

# Query the database
python3 query_tarife.py summary
python3 query_tarife.py cheapest --energy-type POWER -n 20 --kwh 4000
python3 query_tarife.py brands --energy-type GAS
python3 query_tarife.py grid-operators
python3 query_tarife.py search "Verbund"
python3 query_tarife.py compare 1010 --energy-type POWER --kwh 2500
```

## Architecture

Scripts sharing a SQLite database (`tarife.db`):

- **`scrape_tarife.py`** — Scraper that hits the E-Control REST API. Discovers grid operators by querying ~300 sample ZIP codes, then fetches brands and per-grid-area product rates. Stores **per-unit pricing** (ct/kWh + base fee) so costs can be calculated for any consumption.

- **`scrape_all_regions.py`** — Scrapes historical tariffs for all Austrian grid areas (22 areas). Uses its own `historical_products` table.

- **`scrape_plz_mapping.py`** — Builds complete PLZ → grid area mapping by querying E-Control for all Austrian PLZs (1010–9999). Stores in `plz_grid_area_mapping` table.

- **`build_site.py`** — Exports SQLite data to JSON files in `public/data/` for the static site. Run after any scraping. Exports `current.json`, `grid-rates.json`, `historical.json`, `brands.json`, `stats.json`, `plz-mapping.json`.

- **`query_tarife.py`** — Read-only CLI for querying the scraped data. Supports `--kwh` flag for custom consumption.

## Data Pipeline

```
scrape_tarife.py / scrape_all_regions.py → tarife.db
scrape_plz_mapping.py → tarife.db (plz_grid_area_mapping)
build_site.py → public/data/*.json → Vercel (static + serverless)
```

## Database Schema (SQLite)

WAL journal mode. Foreign keys enabled.

### Core pricing tables

**`product_rates`** — Per-unit energy pricing per product per grid area. All prices in **cents netto**.

| Column | Type | Description |
|--------|------|-------------|
| `energy_ct_kwh` | REAL | Energy price per kWh (netto, cents) — derived: `energyRateTotal / consumption` |
| `energy_base_ct_year` | REAL | Annual energy base/fixed fee (netto, cents) |
| `energy_discount_ct_year` | REAL | Discount (netto, cents, 0 or negative) |
| `energy_fees_ct_year` | REAL | Product fees like Gebrauchsabgabe (netto, cents) |
| `reference_consumption_kwh` | INT | Consumption used for scraping (3500/15000) |
| `energy_total_netto_ct` | REAL | API total for verification |
| `annual_total_brutto_ct` | REAL | API annual gross total for verification |

**`grid_area_rates`** — Network costs per grid area (same for all suppliers). All prices in **cents netto**.

| Column | Type | Description |
|--------|------|-------------|
| `grid_ct_kwh` | REAL | Grid usage price per kWh (netto, cents) |
| `grid_base_ct_year` | REAL | Annual grid base fee (netto, cents) |
| `grid_loss_ct_year` | REAL | Grid loss rate (netto, cents) |
| `meter_ct_year` | REAL | Meter rate (netto, cents) |
| `grid_fees_ct_year` | REAL | Grid fees: Elektrizitätsabgabe, Erneuerbaren-Förderung etc. (netto, cents) |

### Cost calculation formula

For any consumption X kWh:

```
energie_netto = energy_ct_kwh × X + energy_base_ct_year
netz_netto    = grid_ct_kwh × X + grid_base_ct_year + grid_loss_ct_year + meter_ct_year + grid_fees_ct_year
total_brutto  = (energie_netto + netz_netto) × 1.2 / 100  [EUR]
```

### Other tables

- `scrape_runs` — Scrape run metadata
- `grid_operators` — Grid operator master data
- `grid_operator_zip_codes` — Which PLZ belong to which grid operator
- `brands` — Brand/supplier master data
- `products` — Product master data (name, brand, green cert, price model etc.)
- `plz_grid_area_mapping` — Complete PLZ → grid area mapping (2168 PLZ)
- `historical_products` — Historical tariff snapshots (from `scrape_all_regions.py`)

## API Details

- Base URL: `https://www.e-control.at/o/rc-public-rest`
- Grid operator discovery: GET `/rate-calculator/grid-operators?zipCode=...&energyType=...`
- Brands: GET `/brands/energy-type/{type}/customer-group/{group}`
- Rate calculation: POST `/rate-calculator/energy-type/{type}/rate` (different payload shapes for POWER vs GAS)
- Reference consumption: 3500 kWh (POWER), 15000 kWh (GAS) — used to derive ct/kWh
- For POWER: both `CLASSIC` and `SPOT_MARKET` price models are scraped

## Key Conventions

- All UI text is in German (Austrian context)
- Energy types: `"POWER"` (electricity) and `"GAS"`
- Customer groups: `"HOME"` and `"BUSINESS"` (rates currently scraped for HOME only)
- **All prices in DB are cents netto** — multiply by 1.2 for brutto (20% Austrian VAT)
- Timestamps are UTC ISO format
- Spot/flex tariffs have `energy_ct_kwh = 0` (price depends on spot market, only base fee stored)
- Grid costs are per grid area, not per product (same Netzkosten for all suppliers in a region)
