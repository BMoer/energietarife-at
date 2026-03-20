#!/usr/bin/env python3
"""
E-Control Historical Tariff Scraper
=====================================
Scrapt historische Produkte und deren Tarifdetails (Energiepreis, Grundgebühr)
für alle Marken aus dem E-Control Tarifkalkulator.

Ergänzt die aktuelle Tarif-Datenbank um historische Datenpunkte.

Usage:
    python3 scrape_historical.py [--db tarife.db] [--energy-type POWER|GAS|BOTH]
                                 [--zip 1010] [--brand-id 6251]
"""

import argparse
import json
import logging
import sqlite3
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CONTEXT = None

BASE_URL = "https://www.e-control.at/o/rc-public-rest"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "EControl-Tarif-Scraper/1.0",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def api_request(path: str, method: str = "GET", data: dict | None = None) -> dict | list | None:
    """Make an API request to the E-Control REST API."""
    url = f"{BASE_URL}/{path}"
    body = json.dumps(data).encode("utf-8") if data else None
    req = Request(url, data=body, headers=HEADERS, method=method)

    for attempt in range(3):
        try:
            with urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited, waiting %ds...", wait)
                time.sleep(wait)
                continue
            error_body = e.read().decode("utf-8", errors="replace")
            logger.error("HTTP %d for %s: %s", e.code, path, error_body[:200])
            return None
        except (URLError, TimeoutError) as e:
            logger.warning("Request failed (attempt %d): %s", attempt + 1, e)
            time.sleep(2)

    return None


def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database with historical products schema."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS historical_products (
            product_id INTEGER NOT NULL,
            association_id INTEGER,
            brand_id INTEGER NOT NULL,
            brand_name TEXT NOT NULL,
            supplier_name TEXT,
            product_name TEXT NOT NULL,
            energy_type TEXT NOT NULL,
            customer_group TEXT NOT NULL,
            grid_area_id INTEGER NOT NULL,
            zip_code TEXT NOT NULL,
            product_validity_from TEXT,
            regular_customers_from TEXT,
            energy_rate_ct_kwh REAL,
            energy_rate_high_ct_kwh REAL,
            energy_rate_low_ct_kwh REAL,
            base_rate_cents REAL,
            base_rate_type TEXT,
            rate_type TEXT,
            rate_zoning_type TEXT,
            min_contract_term_months INTEGER,
            notice_period_months INTEGER,
            accounting_type TEXT,
            locations TEXT,
            scraped_at TEXT NOT NULL,
            PRIMARY KEY (product_id, grid_area_id)
        );

        CREATE INDEX IF NOT EXISTS idx_hist_brand ON historical_products(brand_id);
        CREATE INDEX IF NOT EXISTS idx_hist_energy ON historical_products(energy_type);
        CREATE INDEX IF NOT EXISTS idx_hist_validity ON historical_products(product_validity_from);
        CREATE INDEX IF NOT EXISTS idx_hist_brand_name ON historical_products(brand_name);
    """)

    conn.commit()
    return conn


def ms_to_iso(ms: int | None) -> str | None:
    """Convert millisecond timestamp to ISO date string."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def fetch_brands(energy_type: str, customer_group: str) -> list[dict]:
    """Fetch all brands for a given energy type and customer group."""
    result = api_request(f"brands/energy-type/{energy_type}/customer-group/{customer_group}")
    return result if isinstance(result, list) else []


def fetch_grid_operators(zip_code: str, energy_type: str) -> list[dict]:
    """Fetch grid operators for a ZIP code."""
    result = api_request(
        f"rate-calculator/grid-operators?zipCode={zip_code}&energyType={energy_type}"
    )
    if not result or not result.get("isZipCodeValid"):
        return []
    return result.get("gridOperators", [])


def fetch_brand_products(brand_id: int, energy_type: str, customer_group: str,
                         zip_code: str, grid_operator_id: int, grid_area_id: int,
                         consumption: int) -> list[dict]:
    """Fetch all historical products for a brand in a grid area."""
    energy_path = "power" if energy_type == "POWER" else "gas"

    if energy_type == "POWER":
        payload = {
            "customerGroup": customer_group,
            "firstMeterOptions": {"standardConsumption": consumption},
            "zipCode": zip_code,
            "gridOperatorId": grid_operator_id,
            "gridAreaId": grid_area_id,
            "moveHome": False,
        }
    else:
        payload = {
            "customerGroup": customer_group,
            "gasRequestOptions": {
                "annualConsumption": consumption,
                "measurementMode": "UNMETERED",
            },
            "zipCode": zip_code,
            "gridOperatorId": grid_operator_id,
            "gridAreaId": grid_area_id,
            "moveHome": False,
        }

    result = api_request(
        f"brands/{brand_id}/products/{energy_path}/search?includeSmartMeter=false",
        method="POST",
        data=payload,
    )
    if not result:
        return []
    return result.get("productData", [])


def fetch_product_details(product_id: int, zip_code: str) -> dict | None:
    """Fetch detailed tariff data for a specific product."""
    return api_request(
        "products/product-details",
        method="POST",
        data={"productId": product_id, "zipCode": zip_code},
    )


def save_historical_product(conn: sqlite3.Connection, product: dict,
                            details: dict, brand: dict, energy_type: str,
                            customer_group: str, grid_area_id: int,
                            zip_code: str):
    """Save a historical product with its tariff details."""
    now = datetime.now(timezone.utc).isoformat()

    # Extract energy rate (first tier)
    energy_rate = None
    energy_rate_high = None
    energy_rate_low = None
    for er in details.get("energyRates", []):
        energy_rate = er.get("standardRate")
        energy_rate_high = er.get("highRate")
        energy_rate_low = er.get("lowRate")
        break  # first stagger tier

    # Extract base rate (first tier)
    base_rate = None
    base_rate_type = None
    for br in details.get("baseRates", []):
        base_rate = br.get("rate")
        base_rate_type = br.get("baseRateType")
        break

    locations = ", ".join(details.get("locations", []))

    conn.execute("""
        INSERT INTO historical_products (
            product_id, association_id, brand_id, brand_name, supplier_name,
            product_name, energy_type, customer_group,
            grid_area_id, zip_code,
            product_validity_from, regular_customers_from,
            energy_rate_ct_kwh, energy_rate_high_ct_kwh, energy_rate_low_ct_kwh,
            base_rate_cents, base_rate_type,
            rate_type, rate_zoning_type,
            min_contract_term_months, notice_period_months,
            accounting_type, locations, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id, grid_area_id) DO UPDATE SET
            energy_rate_ct_kwh=excluded.energy_rate_ct_kwh,
            base_rate_cents=excluded.base_rate_cents,
            scraped_at=excluded.scraped_at
    """, (
        product["mainId"],
        product.get("mainAssociationId"),
        brand["id"],
        brand["brandName"],
        details.get("supplierName") or brand.get("supplierName"),
        product["name"],
        energy_type,
        customer_group,
        grid_area_id,
        zip_code,
        ms_to_iso(product.get("productValidityFrom")),
        ms_to_iso(product.get("regularCustomersFrom")),
        energy_rate,
        energy_rate_high,
        energy_rate_low,
        base_rate,
        base_rate_type,
        details.get("rateType"),
        details.get("rateZoningType"),
        details.get("minContractTermHousehold"),
        details.get("noticePeriodHousehold"),
        details.get("accountingType"),
        locations or None,
        now,
    ))


def scrape_historical(conn: sqlite3.Connection, energy_type: str,
                      zip_code: str, customer_group: str,
                      consumption: int, brand_filter: int | None = None):
    """Scrape historical products for all brands in a grid area."""
    logger.info("=== Scraping historical %s products (PLZ %s) ===", energy_type, zip_code)

    # 1. Get grid operator for this ZIP
    grid_ops = fetch_grid_operators(zip_code, energy_type)
    if not grid_ops:
        logger.error("No grid operators found for PLZ %s / %s", zip_code, energy_type)
        return

    go = grid_ops[0]
    go_id = go["id"]
    grid_area_id = go["gridAreaId"]
    logger.info("Grid operator: %s (area %d)", go["name"], grid_area_id)

    # 2. Get all brands
    brands = fetch_brands(energy_type, customer_group)
    logger.info("Found %d brands for %s %s", len(brands), energy_type, customer_group)
    time.sleep(0.3)

    if brand_filter:
        brands = [b for b in brands if b["id"] == brand_filter]
        logger.info("Filtered to brand ID %d: %d brands", brand_filter, len(brands))

    # 3. For each brand, get historical products and their details
    total_products = 0
    total_brands_with_products = 0

    for i, brand in enumerate(brands):
        brand_id = brand["id"]
        brand_name = brand["brandName"]

        if i > 0 and i % 20 == 0:
            logger.info("  Progress: %d/%d brands, %d products so far",
                        i, len(brands), total_products)

        products = fetch_brand_products(
            brand_id, energy_type, customer_group,
            zip_code, go_id, grid_area_id, consumption,
        )
        time.sleep(0.15)

        if not products:
            continue

        total_brands_with_products += 1
        logger.info("  %s: %d historical products", brand_name, len(products))

        for prod in products:
            product_id = prod["mainId"]
            details = fetch_product_details(product_id, zip_code)
            time.sleep(0.15)

            if not details:
                logger.warning("    No details for product %d (%s)", product_id, prod["name"])
                continue

            save_historical_product(
                conn, prod, details, brand, energy_type,
                customer_group, grid_area_id, zip_code,
            )
            total_products += 1

        conn.commit()

    logger.info("=== %s historical scrape complete: %d brands -> %d products ===",
                energy_type, total_brands_with_products, total_products)


def main():
    parser = argparse.ArgumentParser(description="E-Control Historical Tariff Scraper")
    parser.add_argument("--db", default="tarife.db", help="SQLite database path")
    parser.add_argument(
        "--energy-type", choices=["POWER", "GAS", "BOTH"], default="BOTH",
        help="Which energy type to scrape",
    )
    parser.add_argument("--zip", default="1010", help="ZIP code to query")
    parser.add_argument("--brand-id", type=int, default=None,
                        help="Scrape only a specific brand ID")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    logger.info("Database: %s", db_path)

    conn = init_db(str(db_path))

    customer_group = "HOME"
    energy_types = ["POWER", "GAS"] if args.energy_type == "BOTH" else [args.energy_type]

    for et in energy_types:
        consumption = 3500 if et == "POWER" else 15000
        scrape_historical(conn, et, args.zip, customer_group, consumption, args.brand_id)

    # Summary
    total = conn.execute("SELECT COUNT(*) FROM historical_products").fetchone()[0]
    by_energy = conn.execute(
        "SELECT energy_type, COUNT(*) FROM historical_products GROUP BY energy_type"
    ).fetchall()
    logger.info("Database total: %d historical products", total)
    for row in by_energy:
        logger.info("  %s: %d", row[0], row[1])

    conn.close()


if __name__ == "__main__":
    main()
