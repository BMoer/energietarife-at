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
python3 query_tarife.py cheapest --energy-type POWER -n 20
python3 query_tarife.py brands --energy-type GAS
python3 query_tarife.py grid-operators
python3 query_tarife.py search "Verbund"
python3 query_tarife.py compare 1010 --energy-type POWER
```

## Architecture

Scripts sharing a SQLite database (`tarife.db`):

- **`scrape_tarife.py`** — Scraper that hits the E-Control REST API. Discovers grid operators by querying ~300 sample ZIP codes, then fetches brands and per-grid-area product rates.

- **`scrape_all_regions.py`** — Scrapes historical tariffs for all Austrian grid areas (22 areas).

- **`scrape_plz_mapping.py`** — Builds complete PLZ → grid area mapping by querying E-Control for all Austrian PLZs (1010–9999). Stores in `plz_grid_area_mapping` table.

- **`build_site.py`** — Exports SQLite data to JSON files in `public/data/` for the static site. Run after any scraping.

- **`query_tarife.py`** — Read-only CLI for querying the scraped data.

## Data Pipeline

```
scrape_tarife.py / scrape_all_regions.py → tarife.db
scrape_plz_mapping.py → tarife.db (plz_grid_area_mapping)
build_site.py → public/data/*.json → Vercel (static + serverless)
```

## Database Schema (SQLite)

Key tables: `scrape_runs`, `grid_operators`, `grid_operator_zip_codes`, `brands`, `products`, `product_rates`, `historical_products`, `plz_grid_area_mapping`. WAL journal mode is used. Foreign keys are enabled.

## API Details

- Base URL: `https://www.e-control.at/o/rc-public-rest`
- Grid operator discovery: GET `/rate-calculator/grid-operators?zipCode=...&energyType=...`
- Brands: GET `/brands/energy-type/{type}/customer-group/{group}`
- Rate calculation: POST `/rate-calculator/energy-type/{type}/rate` (different payload shapes for POWER vs GAS)
- Default annual consumption: 3500 kWh (POWER), 15000 kWh (GAS)

## Key Conventions

- All UI text is in German (Austrian context)
- Energy types: `"POWER"` (electricity) and `"GAS"`
- Customer groups: `"HOME"` and `"BUSINESS"` (rates currently scraped for HOME only)
- Monetary values in the DB are in cents (divide by 100 for EUR)
- Timestamps are UTC ISO format
