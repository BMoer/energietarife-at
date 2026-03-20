#!/usr/bin/env python3
"""
Scrape all Austrian grid areas by discovering unique Netzgebiete
via PLZ sampling, then scraping historical tariffs for each.
"""

import json
import logging
import sqlite3
import ssl
import sys
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

DB_PATH = Path("tarife.db").resolve()

# Representative PLZ per Austrian region (covers all Bundesländer + sub-regions)
# We'll also scan ranges to catch smaller grid areas
SAMPLE_PLZS = [
    # Wien
    "1010", "1020", "1030", "1040", "1050", "1060", "1070", "1080", "1090",
    "1100", "1110", "1120", "1130", "1140", "1150", "1160", "1170", "1180",
    "1190", "1200", "1210", "1220", "1230",
    # Niederösterreich
    "2000", "2020", "2100", "2130", "2170", "2230", "2320", "2340", "2380",
    "2460", "2500", "2540", "2620", "2700", "2753", "2801", "2860", "2880",
    "3001", "3100", "3130", "3200", "3250", "3300", "3340", "3380", "3400",
    "3430", "3500", "3580", "3601", "3680", "3701", "3730", "3800", "3830",
    "3900", "3910", "3950",
    # Burgenland
    "7000", "7011", "7071", "7100", "7122", "7132", "7201", "7210", "7301",
    "7350", "7400", "7431", "7501", "7540",
    # Steiermark
    "8010", "8020", "8051", "8101", "8130", "8160", "8200", "8230", "8280",
    "8301", "8330", "8350", "8380", "8401", "8430", "8462", "8501", "8530",
    "8570", "8600", "8630", "8680", "8700", "8720", "8740", "8750", "8770",
    "8790", "8800", "8820", "8850", "8900", "8920", "8940", "8960", "8990",
    # Kärnten
    "9020", "9100", "9130", "9170", "9201", "9220", "9300", "9330", "9360",
    "9400", "9433", "9462", "9500", "9520", "9560", "9580", "9601", "9620",
    "9640", "9710", "9754", "9800", "9821", "9851", "9900",
    # Oberösterreich
    "4010", "4020", "4030", "4040", "4050", "4060", "4070", "4100", "4150",
    "4170", "4201", "4210", "4230", "4240", "4261", "4280", "4300", "4320",
    "4400", "4470", "4481", "4501", "4522", "4540", "4560", "4580", "4600",
    "4614", "4616", "4631", "4650", "4663", "4690", "4710", "4730", "4753",
    "4780", "4800", "4810", "4820", "4830", "4840", "4850", "4860", "4880",
    "4901", "4910", "4950",
    # Salzburg
    "5010", "5020", "5061", "5081", "5101", "5111", "5131", "5142", "5162",
    "5201", "5230", "5280", "5301", "5340", "5360", "5400", "5431", "5451",
    "5500", "5541", "5580", "5600", "5620", "5640", "5651", "5660", "5671",
    "5700", "5710", "5730", "5741", "5751", "5760",
    # Tirol
    "6010", "6020", "6060", "6067", "6100", "6112", "6130", "6150", "6166",
    "6170", "6176", "6200", "6210", "6215", "6220", "6230", "6232", "6240",
    "6250", "6260", "6263", "6271", "6280", "6290", "6300", "6311", "6320",
    "6330", "6340", "6353", "6361", "6370", "6380", "6382", "6384", "6385",
    "6388", "6390", "6391", "6395", "6401", "6410", "6421", "6432", "6444",
    "6450", "6460", "6464", "6471", "6500", "6511", "6521", "6531", "6533",
    "6534", "6541", "6542", "6543", "6551", "6553", "6555", "6561", "6562",
    "6563", "6571", "6580", "6591", "6600", "6610", "6621", "6631", "6632",
    "6633", "6642", "6644", "6645", "6650", "6652", "6654", "6655", "6670",
    "6672", "6673", "6675", "6677", "6683", "6691",
    # Vorarlberg
    "6700", "6710", "6713", "6714", "6719", "6721", "6731", "6741", "6751",
    "6761", "6764", "6771", "6773", "6774", "6780", "6787", "6791", "6793",
    "6800", "6811", "6820", "6822", "6824", "6830", "6832", "6833", "6840",
    "6842", "6844", "6845", "6850", "6858", "6861", "6863", "6867", "6870",
    "6874", "6881", "6882", "6883", "6884", "6886", "6888", "6890", "6900",
    "6911", "6912", "6921", "6922", "6923", "6932", "6941", "6942", "6943",
    "6951", "6952", "6960", "6971", "6973", "6974", "6991", "6992", "6993",
]


def api_request(path, method="GET", data=None):
    url = f"{BASE_URL}/{path}"
    body = json.dumps(data).encode("utf-8") if data else None
    req = Request(url, data=body, headers=HEADERS, method=method)
    for attempt in range(3):
        try:
            with urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** (attempt + 1))
                continue
            return None
        except (URLError, TimeoutError):
            time.sleep(2)
    return None


def fetch_grid_operators(zip_code, energy_type):
    result = api_request(
        f"rate-calculator/grid-operators?zipCode={zip_code}&energyType={energy_type}"
    )
    if not result or not result.get("isZipCodeValid"):
        return []
    return result.get("gridOperators", [])


def discover_grid_areas():
    """Discover all unique grid areas by sampling PLZ codes."""
    logger.info("=== Phase 1: Discovering grid areas ===")

    grid_areas = {}  # grid_area_id -> {info}

    for i, plz in enumerate(SAMPLE_PLZS):
        if i > 0 and i % 50 == 0:
            logger.info("  Sampled %d/%d PLZ, found %d unique grid areas so far",
                        i, len(SAMPLE_PLZS), len(grid_areas))

        for energy_type in ["POWER", "GAS"]:
            ops = fetch_grid_operators(plz, energy_type)
            for op in ops:
                area_id = op["gridAreaId"]
                key = (area_id, energy_type)
                if key not in grid_areas:
                    grid_areas[key] = {
                        "grid_area_id": area_id,
                        "grid_operator_id": op["id"],
                        "grid_operator_name": op["name"],
                        "energy_type": energy_type,
                        "sample_plz": plz,
                    }
            time.sleep(0.1)

    logger.info("=== Discovery complete: %d unique grid areas ===", len(grid_areas))
    for key, info in sorted(grid_areas.items()):
        logger.info("  %s area %d: %s (PLZ %s)",
                     info["energy_type"], info["grid_area_id"],
                     info["grid_operator_name"], info["sample_plz"])

    return list(grid_areas.values())


def ms_to_iso(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def fetch_brands(energy_type, customer_group="HOME"):
    result = api_request(f"brands/energy-type/{energy_type}/customer-group/{customer_group}")
    return result if isinstance(result, list) else []


def fetch_brand_products(brand_id, energy_type, zip_code, go_id, grid_area_id, consumption):
    energy_path = "power" if energy_type == "POWER" else "gas"
    if energy_type == "POWER":
        payload = {
            "customerGroup": "HOME",
            "firstMeterOptions": {"standardConsumption": consumption},
            "zipCode": zip_code,
            "gridOperatorId": go_id,
            "gridAreaId": grid_area_id,
            "moveHome": False,
        }
    else:
        payload = {
            "customerGroup": "HOME",
            "gasRequestOptions": {
                "annualConsumption": consumption,
                "measurementMode": "UNMETERED",
            },
            "zipCode": zip_code,
            "gridOperatorId": go_id,
            "gridAreaId": grid_area_id,
            "moveHome": False,
        }
    result = api_request(
        f"brands/{brand_id}/products/{energy_path}/search?includeSmartMeter=false",
        method="POST", data=payload,
    )
    return result.get("productData", []) if result else []


def fetch_product_details(product_id, zip_code):
    return api_request("products/product-details", method="POST",
                       data={"productId": product_id, "zipCode": zip_code})


def init_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
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


def save_product(conn, product, details, brand, energy_type, grid_area_id, zip_code):
    now = datetime.now(timezone.utc).isoformat()
    energy_rate = energy_rate_high = energy_rate_low = None
    for er in details.get("energyRates", []):
        energy_rate = er.get("standardRate")
        energy_rate_high = er.get("highRate")
        energy_rate_low = er.get("lowRate")
        break
    base_rate = base_rate_type = None
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
        product["mainId"], product.get("mainAssociationId"),
        brand["id"], brand["brandName"],
        details.get("supplierName") or brand.get("supplierName"),
        product["name"], energy_type, "HOME",
        grid_area_id, zip_code,
        ms_to_iso(product.get("productValidityFrom")),
        ms_to_iso(product.get("regularCustomersFrom")),
        energy_rate, energy_rate_high, energy_rate_low,
        base_rate, base_rate_type,
        details.get("rateType"), details.get("rateZoningType"),
        details.get("minContractTermHousehold"),
        details.get("noticePeriodHousehold"),
        details.get("accountingType"),
        locations or None, now,
    ))


def scrape_grid_area(conn, area_info, brands_cache):
    energy_type = area_info["energy_type"]
    grid_area_id = area_info["grid_area_id"]
    go_id = area_info["grid_operator_id"]
    go_name = area_info["grid_operator_name"]
    plz = area_info["sample_plz"]
    consumption = 3500 if energy_type == "POWER" else 15000

    # Check if already scraped (skip if >50 products already exist)
    existing = conn.execute(
        "SELECT COUNT(*) FROM historical_products WHERE grid_area_id=? AND energy_type=?",
        (grid_area_id, energy_type)
    ).fetchone()[0]
    if existing > 50:
        logger.info("  SKIP %s area %d (%s) — already %d products",
                     energy_type, grid_area_id, go_name, existing)
        return 0

    brands = brands_cache.get(energy_type)
    if not brands:
        brands = fetch_brands(energy_type)
        brands_cache[energy_type] = brands
        time.sleep(0.3)

    total = 0
    for bi, brand in enumerate(brands):
        if bi > 0 and bi % 20 == 0:
            logger.info("    Brands: %d/%d, products: %d", bi, len(brands), total)
        products = fetch_brand_products(
            brand["id"], energy_type, plz, go_id, grid_area_id, consumption
        )
        time.sleep(0.15)
        if not products:
            continue

        for prod in products:
            details = fetch_product_details(prod["mainId"], plz)
            time.sleep(0.15)
            if not details:
                continue
            try:
                save_product(conn, prod, details, brand, energy_type, grid_area_id, plz)
                total += 1
            except sqlite3.Error as e:
                logger.warning("    DB error for product %d: %s", prod["mainId"], e)

        conn.commit()

    return total


def main():
    logger.info("Database: %s", DB_PATH)

    # Phase 1: Discover all grid areas
    grid_areas = discover_grid_areas()

    if not grid_areas:
        logger.error("No grid areas found!")
        return

    # Phase 2: Scrape each grid area
    conn = init_db()
    brands_cache = {}

    total_areas = len(grid_areas)
    total_products = 0
    completed = 0

    logger.info("=== Phase 2: Scraping %d grid areas ===", total_areas)

    for area in sorted(grid_areas, key=lambda a: (a["energy_type"], a["grid_area_id"])):
        completed += 1
        et = area["energy_type"]
        ga = area["grid_area_id"]
        name = area["grid_operator_name"]

        logger.info("[%d/%d] %s area %d: %s (PLZ %s)",
                     completed, total_areas, et, ga, name, area["sample_plz"])

        count = scrape_grid_area(conn, area, brands_cache)
        total_products += count

        if count > 0:
            logger.info("  -> %d products saved (total: %d)", count, total_products)

    # Summary
    total = conn.execute("SELECT COUNT(*) FROM historical_products").fetchone()[0]
    areas = conn.execute(
        "SELECT energy_type, COUNT(DISTINCT grid_area_id), COUNT(*) "
        "FROM historical_products GROUP BY energy_type"
    ).fetchall()

    logger.info("=== COMPLETE ===")
    logger.info("Total: %d products in DB", total)
    for row in areas:
        logger.info("  %s: %d grid areas, %d products", row[0], row[1], row[2])

    conn.close()


if __name__ == "__main__":
    main()
