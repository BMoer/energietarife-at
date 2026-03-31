#!/usr/bin/env python3
"""
E-Control Tarifkalkulator Scraper
==================================
Scrapt alle aktuellen Strom- und Gas-Tarife aus dem E-Control Tarifkalkulator
(https://www.e-control.at/tarifkalkulator) via deren REST API.

Speichert die Ergebnisse in einer SQLite-Datenbank.

Usage:
    python3 scrape_tarife.py [--db tarife.db] [--energy-type POWER|GAS|BOTH]
"""

import argparse
import json
import logging
import sqlite3
import ssl
import time
from dataclasses import dataclass
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

# Representative ZIP codes covering all Austrian grid areas
# Each Bundesland has multiple Netzbetreiber, so we sample many PLZ
SAMPLE_ZIP_CODES = [
    # Wien
    "1010", "1020", "1030", "1040", "1050", "1060", "1070", "1080", "1090",
    "1100", "1110", "1120", "1130", "1140", "1150", "1160", "1170", "1180",
    "1190", "1200", "1210", "1220", "1230",
    # Niederösterreich
    "2000", "2100", "2130", "2170", "2201", "2230", "2320", "2340", "2353",
    "2380", "2460", "2500", "2620", "2700", "2700", "2753", "2801", "2840",
    "3100", "3180", "3200", "3250", "3300", "3340", "3382", "3400", "3430",
    "3500", "3550", "3580", "3601", "3631", "3680", "3700", "3730",
    "3800", "3830", "3910", "3950",
    # Burgenland
    "7000", "7011", "7022", "7033", "7100", "7121", "7132", "7141", "7202",
    "7210", "7301", "7350", "7400", "7423", "7431", "7471", "7501", "7540",
    # Oberösterreich
    "4010", "4020", "4030", "4040", "4050", "4060", "4070", "4100", "4150",
    "4174", "4190", "4210", "4240", "4261", "4293", "4320", "4360", "4400",
    "4470", "4501", "4540", "4560", "4600", "4614", "4650", "4663", "4690",
    "4710", "4722", "4752", "4780", "4810", "4820", "4840", "4860", "4890",
    "4910", "4950",
    # Salzburg
    "5020", "5061", "5071", "5082", "5101", "5110", "5142", "5163", "5202",
    "5230", "5280", "5301", "5310", "5340", "5400", "5441", "5500", "5524",
    "5541", "5550", "5570", "5580", "5600", "5620", "5630", "5651", "5672",
    "5700", "5710", "5741", "5751",
    # Steiermark
    "8010", "8020", "8036", "8041", "8051", "8055", "8063", "8071", "8101",
    "8112", "8130", "8141", "8160", "8200", "8230", "8240", "8261", "8280",
    "8311", "8330", "8342", "8350", "8380", "8401", "8430", "8452", "8461",
    "8480", "8501", "8530", "8541", "8552", "8570", "8580", "8600", "8605",
    "8630", "8650", "8670", "8680", "8700", "8720", "8740", "8750", "8761",
    "8770", "8790", "8800", "8820", "8840", "8850", "8861", "8900", "8911",
    "8920", "8940", "8950", "8960", "8970", "8990",
    # Kärnten
    "9020", "9061", "9073", "9100", "9121", "9150", "9170", "9210", "9220",
    "9241", "9300", "9311", "9330", "9341", "9360", "9400", "9431", "9462",
    "9500", "9520", "9546", "9560", "9580", "9601", "9620", "9640", "9653",
    "9701", "9710", "9753", "9771", "9800", "9813", "9853", "9871", "9900",
    # Tirol
    "6010", "6020", "6060", "6080", "6100", "6112", "6130", "6150", "6162",
    "6170", "6176", "6200", "6210", "6230", "6250", "6263", "6271", "6290",
    "6300", "6330", "6341", "6351", "6365", "6380", "6395", "6401", "6410",
    "6422", "6432", "6450", "6460", "6471", "6500", "6511", "6521", "6531",
    "6541", "6551", "6561", "6571", "6580", "6600", "6611", "6621", "6631",
    "6642", "6651", "6652", "6653", "6654", "6655", "6670", "6677", "6691",
    # Vorarlberg
    "6700", "6710", "6713", "6719", "6721", "6731", "6741", "6751", "6764",
    "6774", "6780", "6793", "6800", "6811", "6820", "6830", "6833", "6840",
    "6845", "6850", "6858", "6861", "6863", "6870", "6874", "6881", "6884",
    "6890", "6900", "6911", "6922", "6932", "6941", "6951", "6952", "6960",
    "6971", "6991",
]

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
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scrape_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            energy_type TEXT,
            zip_codes_queried INTEGER DEFAULT 0,
            grid_areas_found INTEGER DEFAULT 0,
            products_found INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS grid_operators (
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            energy_type TEXT NOT NULL,
            grid_area_id INTEGER NOT NULL,
            tenant_id INTEGER,
            status TEXT,
            brand_home INTEGER,
            brand_business INTEGER,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            PRIMARY KEY (id, energy_type)
        );

        CREATE TABLE IF NOT EXISTS grid_operator_zip_codes (
            grid_operator_id INTEGER NOT NULL,
            zip_code TEXT NOT NULL,
            energy_type TEXT NOT NULL,
            PRIMARY KEY (grid_operator_id, zip_code, energy_type)
        );

        CREATE TABLE IF NOT EXISTS brands (
            id INTEGER NOT NULL,
            brand_name TEXT NOT NULL,
            supplier_name TEXT,
            ecad_id TEXT,
            energy_type TEXT NOT NULL,
            customer_group TEXT NOT NULL,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            PRIMARY KEY (id, energy_type, customer_group)
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            association_id INTEGER,
            product_name TEXT NOT NULL,
            brand_id INTEGER NOT NULL,
            brand_name TEXT NOT NULL,
            product_type TEXT,
            energy_type TEXT NOT NULL,
            customer_group TEXT NOT NULL,
            price_guarantee_type TEXT,
            price_model TEXT,
            contract_conclusion_type TEXT,
            is_online_product INTEGER,
            is_certified_green INTEGER,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS product_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_run_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            association_id INTEGER,
            grid_area_id INTEGER NOT NULL,
            grid_operator_name TEXT,
            zip_code TEXT NOT NULL,
            energy_type TEXT NOT NULL,
            customer_group TEXT NOT NULL,
            annual_consumption_kwh INTEGER NOT NULL,
            annual_gross_rate_cents REAL,
            annual_saving_cents REAL,
            base_rate_with_tax_cents REAL,
            avg_energy_price_cent_kwh REAL,
            avg_total_price_cent_kwh REAL,
            energy_rate_net_sum REAL,
            energy_rate_total REAL,
            base_rate REAL,
            discount_net_sum REAL,
            product_fee_net_sum REAL,
            grid_rate_net_sum REAL,
            grid_rate_total REAL,
            grid_base_rate REAL,
            taxes_and_levies_total REAL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (scrape_run_id) REFERENCES scrape_runs(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE INDEX IF NOT EXISTS idx_product_rates_product ON product_rates(product_id);
        CREATE INDEX IF NOT EXISTS idx_product_rates_grid_area ON product_rates(grid_area_id);
        CREATE INDEX IF NOT EXISTS idx_product_rates_scraped ON product_rates(scraped_at);
        CREATE INDEX IF NOT EXISTS idx_product_rates_energy ON product_rates(energy_type);
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand_id);
        CREATE INDEX IF NOT EXISTS idx_products_energy ON products(energy_type);
    """)

    conn.commit()
    return conn


def discover_grid_operators(zip_codes: list[str], energy_type: str) -> dict[int, dict]:
    """Discover all grid operators by querying multiple ZIP codes."""
    grid_ops = {}

    for i, plz in enumerate(zip_codes):
        if i > 0 and i % 50 == 0:
            logger.info("  Checked %d/%d ZIP codes, found %d grid areas so far",
                        i, len(zip_codes), len(grid_ops))
            time.sleep(0.5)

        result = api_request(
            f"rate-calculator/grid-operators?zipCode={plz}&energyType={energy_type}"
        )
        if not result or not result.get("isZipCodeValid"):
            continue

        for go in result.get("gridOperators", []):
            go_id = go["id"]
            if go_id not in grid_ops:
                grid_ops[go_id] = {**go, "zip_codes": set()}
            grid_ops[go_id]["zip_codes"].add(plz)

        time.sleep(0.1)

    return grid_ops


def fetch_brands(energy_type: str, customer_group: str) -> list[dict]:
    """Fetch all brands for a given energy type and customer group."""
    result = api_request(f"brands/energy-type/{energy_type}/customer-group/{customer_group}")
    return result if isinstance(result, list) else []


def fetch_rates(energy_type: str, customer_group: str,
                zip_code: str, grid_operator: dict,
                consumption: int,
                search_price_model: str = "CLASSIC") -> dict | None:
    """Fetch tariff rates for a specific grid area.

    Args:
        search_price_model: "CLASSIC" for standard tariffs,
                           "SPOT_MARKET" for spot/float tariffs (POWER only).
    """
    grid_op_id = grid_operator["id"]
    grid_area_id = grid_operator["gridAreaId"]

    if energy_type == "POWER":
        # SPOT_MARKET requires a different priceView
        if search_price_model == "SPOT_MARKET":
            price_view = "SPOT_MARKET_MARGIN"
        else:
            price_view = "EUR_PER_YEAR"

        payload = {
            "customerGroup": customer_group,
            "energyType": "POWER",
            "zipCode": zip_code,
            "gridOperatorId": grid_op_id,
            "gridAreaId": grid_area_id,
            "moveHome": True,
            "includeSwitchingDiscounts": False,
            "firstMeterOptions": {
                "standardConsumption": consumption,
                "rateZoningType": "STANDARD",
                "productType": "MAIN",
                "smartMeterRequestOptions": {"smartMeterSearch": False},
            },
            "comparisonOptions": {},
            "priceView": price_view,
            "referencePeriod": "ONE_YEAR",
            "searchPriceModel": search_price_model,
        }
    else:
        payload = {
            "customerGroup": customer_group,
            "energyType": "GAS",
            "zipCode": zip_code,
            "gridOperatorId": grid_op_id,
            "gridAreaId": grid_area_id,
            "moveHome": True,
            "includeSwitchingDiscounts": False,
            "gasRequestOptions": {
                "annualConsumption": consumption,
                "measurementMode": "UNMETERED",
            },
            "comparisonOptions": {},
            "priceView": "EUR_PER_YEAR",
            "referencePeriod": "ONE_YEAR",
        }

    return api_request(
        f"rate-calculator/energy-type/{energy_type}/rate",
        method="POST",
        data=payload,
    )


def save_grid_operators(conn: sqlite3.Connection, grid_ops: dict, energy_type: str):
    """Save grid operators to database."""
    now = datetime.now(timezone.utc).isoformat()

    for go_id, go in grid_ops.items():
        conn.execute("""
            INSERT INTO grid_operators (id, name, energy_type, grid_area_id, tenant_id,
                                        status, brand_home, brand_business, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, energy_type) DO UPDATE SET
                name=excluded.name, status=excluded.status, last_seen=excluded.last_seen
        """, (
            go_id, go["name"], energy_type, go["gridAreaId"],
            go.get("tenantId"), go.get("status"),
            go.get("brandHome"), go.get("brandBusiness"),
            now, now,
        ))

        for plz in go.get("zip_codes", []):
            conn.execute("""
                INSERT OR IGNORE INTO grid_operator_zip_codes
                (grid_operator_id, zip_code, energy_type) VALUES (?, ?, ?)
            """, (go_id, plz, energy_type))

    conn.commit()


def save_brands(conn: sqlite3.Connection, brands: list[dict],
                energy_type: str, customer_group: str):
    """Save brands to database."""
    now = datetime.now(timezone.utc).isoformat()

    for b in brands:
        conn.execute("""
            INSERT INTO brands (id, brand_name, supplier_name, ecad_id,
                                energy_type, customer_group, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, energy_type, customer_group) DO UPDATE SET
                brand_name=excluded.brand_name, supplier_name=excluded.supplier_name,
                last_seen=excluded.last_seen
        """, (
            b["id"], b["brandName"], b.get("supplierName"),
            b.get("ecadId"), energy_type, customer_group, now, now,
        ))

    conn.commit()


def save_rates(conn: sqlite3.Connection, run_id: int, rate_data: dict,
               energy_type: str, customer_group: str,
               zip_code: str, grid_area_id: int, consumption: int):
    """Save rate results to database."""
    now = datetime.now(timezone.utc).isoformat()
    grid_op_name = rate_data.get("gridOperatorName", "")

    for p in rate_data.get("ratedProducts", []):
        # Save/update product
        conn.execute("""
            INSERT INTO products (id, association_id, product_name, brand_id, brand_name,
                                  product_type, energy_type, customer_group,
                                  price_guarantee_type, price_model,
                                  is_online_product, is_certified_green,
                                  first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                product_name=excluded.product_name, last_seen=excluded.last_seen
        """, (
            p["id"], p.get("associationId"), p["productName"],
            p["brandId"], p["brandName"], p.get("productType"),
            energy_type, customer_group,
            p.get("priceGuaranteeType"), p.get("priceModel"),
            1 if p.get("isOnlineProduct") else 0,
            1 if p.get("isCertifiedGreenPower") else 0,
            now, now,
        ))

        # Extract cost breakdown
        energy_costs = p.get("calculatedProductEnergyCosts", {}) or {}
        grid_costs = p.get("calculatedGridCosts", {}) or {}
        taxes = p.get("calculatedTaxesAndLevies", {}) or {}

        conn.execute("""
            INSERT INTO product_rates (
                scrape_run_id, product_id, association_id,
                grid_area_id, grid_operator_name, zip_code,
                energy_type, customer_group, annual_consumption_kwh,
                annual_gross_rate_cents, annual_saving_cents,
                base_rate_with_tax_cents, avg_energy_price_cent_kwh,
                avg_total_price_cent_kwh,
                energy_rate_net_sum, energy_rate_total, base_rate,
                discount_net_sum, product_fee_net_sum,
                grid_rate_net_sum, grid_rate_total, grid_base_rate,
                taxes_and_levies_total,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id, p["id"], p.get("associationId"),
            grid_area_id, grid_op_name, zip_code,
            energy_type, customer_group, consumption,
            p.get("annualGrossRate"), p.get("annualSaving"),
            p.get("baseRateWithTax"), p.get("averageEnergyPriceInCentKWh"),
            p.get("averageTotalPriceInCentKWh"),
            energy_costs.get("energyRateNetSum"), energy_costs.get("energyRateTotal"),
            energy_costs.get("baseRate"),
            energy_costs.get("discountNetSum"), energy_costs.get("productFeeNetSum"),
            grid_costs.get("gridRateNetSum"), grid_costs.get("gridRateTotal"),
            grid_costs.get("baseRate"),
            taxes.get("totalPrice"),
            now,
        ))

    conn.commit()


def scrape_energy_type(conn: sqlite3.Connection, run_id: int,
                       energy_type: str, zip_codes: list[str]):
    """Scrape all tariffs for one energy type."""
    customer_group = "HOME"
    default_consumption = 3500 if energy_type == "POWER" else 15000

    logger.info("=== Scraping %s tariffs ===", energy_type)

    # 1. Discover grid operators
    logger.info("Discovering grid operators for %s...", energy_type)
    grid_ops = discover_grid_operators(zip_codes, energy_type)
    logger.info("Found %d grid operators for %s", len(grid_ops), energy_type)

    save_grid_operators(conn, grid_ops, energy_type)

    conn.execute(
        "UPDATE scrape_runs SET grid_areas_found = grid_areas_found + ? WHERE id = ?",
        (len(grid_ops), run_id),
    )
    conn.execute(
        "UPDATE scrape_runs SET zip_codes_queried = zip_codes_queried + ? WHERE id = ?",
        (len(zip_codes), run_id),
    )
    conn.commit()

    # 2. Fetch brands
    logger.info("Fetching brands for %s...", energy_type)
    for cg in ["HOME", "BUSINESS"]:
        brands = fetch_brands(energy_type, cg)
        logger.info("  %s %s: %d brands", energy_type, cg, len(brands))
        save_brands(conn, brands, energy_type, cg)
        time.sleep(0.3)

    # 3. Fetch rates for each grid area
    # For POWER we query both CLASSIC and SPOT_MARKET price models
    price_models = ["CLASSIC", "SPOT_MARKET"] if energy_type == "POWER" else ["CLASSIC"]

    total_products = 0
    queried_grid_areas = set()

    for go_id, go in grid_ops.items():
        grid_area_id = go["gridAreaId"]

        if grid_area_id in queried_grid_areas:
            continue
        queried_grid_areas.add(grid_area_id)

        sample_plz = sorted(go["zip_codes"])[0]

        for price_model in price_models:
            model_label = f" [{price_model}]" if len(price_models) > 1 else ""
            logger.info("  Fetching rates for %s (grid area %d, PLZ %s)%s...",
                         go["name"], grid_area_id, sample_plz, model_label)

            rate_data = fetch_rates(
                energy_type, customer_group, sample_plz, go, default_consumption,
                search_price_model=price_model,
            )

            if rate_data and "ratedProducts" in rate_data:
                n_products = len(rate_data["ratedProducts"])
                total_products += n_products
                logger.info("    -> %d products", n_products)
                save_rates(
                    conn, run_id, rate_data, energy_type,
                    customer_group, sample_plz, grid_area_id, default_consumption,
                )
            else:
                if price_model == "CLASSIC":
                    logger.warning("    -> No results for grid area %d", grid_area_id)

            time.sleep(0.5)

    conn.execute(
        "UPDATE scrape_runs SET products_found = products_found + ? WHERE id = ?",
        (total_products, run_id),
    )
    conn.commit()

    logger.info("=== %s complete: %d grid areas, %d product rates ===",
                energy_type, len(queried_grid_areas), total_products)


def main():
    parser = argparse.ArgumentParser(description="E-Control Tarifkalkulator Scraper")
    parser.add_argument("--db", default="tarife.db", help="SQLite database path")
    parser.add_argument(
        "--energy-type", choices=["POWER", "GAS", "BOTH"], default="BOTH",
        help="Which energy type to scrape",
    )
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    logger.info("Database: %s", db_path)

    conn = init_db(str(db_path))

    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "INSERT INTO scrape_runs (started_at, energy_type) VALUES (?, ?)",
        (now, args.energy_type),
    )
    run_id = cursor.lastrowid
    conn.commit()

    energy_types = ["POWER", "GAS"] if args.energy_type == "BOTH" else [args.energy_type]

    for et in energy_types:
        scrape_energy_type(conn, run_id, et, SAMPLE_ZIP_CODES)

    conn.execute(
        "UPDATE scrape_runs SET finished_at = ? WHERE id = ?",
        (datetime.now(timezone.utc).isoformat(), run_id),
    )
    conn.commit()

    # Print summary
    row = conn.execute(
        "SELECT zip_codes_queried, grid_areas_found, products_found FROM scrape_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    logger.info("DONE! Run #%d: %d ZIP codes -> %d grid areas -> %d product rates",
                run_id, row[0], row[1], row[2])

    total_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    total_rates = conn.execute("SELECT COUNT(*) FROM product_rates").fetchone()[0]
    total_brands = conn.execute("SELECT COUNT(*) FROM brands").fetchone()[0]
    logger.info("Database totals: %d brands, %d products, %d rate entries",
                total_brands, total_products, total_rates)

    conn.close()


if __name__ == "__main__":
    main()
