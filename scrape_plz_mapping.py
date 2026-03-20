#!/usr/bin/env python3
"""
Scrape complete PLZ → grid area mapping for all Austrian PLZs.

Queries the E-Control API for every PLZ from 1010 to 9999 and stores
which grid areas (POWER + GAS) serve each valid PLZ.

Usage:
    python3 scrape_plz_mapping.py [--db tarife.db]
"""

import json
import logging
import sqlite3
import ssl
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CONTEXT = None

BASE_URL = "https://www.e-control.at/o/rc-public-rest"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "EControl-Tarif-Scraper/1.0",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def api_request(path):
    url = f"{BASE_URL}/{path}"
    req = Request(url, headers=HEADERS)
    for attempt in range(3):
        try:
            ctx = SSL_CONTEXT if SSL_CONTEXT else None
            with urlopen(req, timeout=15, context=ctx) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** (attempt + 1))
                continue
            return None
        except (URLError, TimeoutError):
            time.sleep(1)
    return None


def fetch_grid_areas(zip_code, energy_type):
    result = api_request(
        f"rate-calculator/grid-operators?zipCode={zip_code}&energyType={energy_type}"
    )
    if not result or not result.get("isZipCodeValid"):
        return []
    return [
        {
            "grid_area_id": op["gridAreaId"],
            "grid_operator_id": op["id"],
            "grid_operator_name": op["name"],
        }
        for op in result.get("gridOperators", [])
    ]


def init_db(db_path):
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS plz_grid_area_mapping (
            zip_code TEXT NOT NULL,
            energy_type TEXT NOT NULL,
            grid_area_id INTEGER NOT NULL,
            grid_operator_id INTEGER NOT NULL,
            grid_operator_name TEXT NOT NULL,
            PRIMARY KEY (zip_code, energy_type, grid_area_id)
        );
        CREATE INDEX IF NOT EXISTS idx_plz_mapping_zip
            ON plz_grid_area_mapping(zip_code);
        CREATE INDEX IF NOT EXISTS idx_plz_mapping_grid_area
            ON plz_grid_area_mapping(grid_area_id);
    """)
    conn.commit()
    return conn


def main():
    db_path = Path(sys.argv[1] if len(sys.argv) > 1 else "tarife.db").resolve()
    conn = init_db(db_path)

    # Check what we already have
    existing = set()
    for row in conn.execute("SELECT DISTINCT zip_code FROM plz_grid_area_mapping"):
        existing.add(row[0])

    logger.info("Database: %s (already have %d PLZs)", db_path, len(existing))

    # Austrian PLZs: 1010-9999
    all_plzs = [str(plz) for plz in range(1010, 10000)]
    remaining = [p for p in all_plzs if p not in existing]

    logger.info("Scanning %d PLZs (%d remaining)", len(all_plzs), len(remaining))

    valid_count = len(existing)
    batch = []

    for i, plz in enumerate(remaining):
        if i > 0 and i % 200 == 0:
            conn.executemany(
                """INSERT OR IGNORE INTO plz_grid_area_mapping
                   (zip_code, energy_type, grid_area_id, grid_operator_id, grid_operator_name)
                   VALUES (?, ?, ?, ?, ?)""",
                batch,
            )
            conn.commit()
            batch = []
            logger.info(
                "  Progress: %d/%d scanned, %d valid PLZs found",
                i, len(remaining), valid_count,
            )

        for energy_type in ("POWER", "GAS"):
            areas = fetch_grid_areas(plz, energy_type)
            for area in areas:
                batch.append((
                    plz,
                    energy_type,
                    area["grid_area_id"],
                    area["grid_operator_id"],
                    area["grid_operator_name"],
                ))
            if areas:
                valid_count += 1

        time.sleep(0.08)

    # Final batch
    if batch:
        conn.executemany(
            """INSERT OR IGNORE INTO plz_grid_area_mapping
               (zip_code, energy_type, grid_area_id, grid_operator_id, grid_operator_name)
               VALUES (?, ?, ?, ?, ?)""",
            batch,
        )
        conn.commit()

    # Summary
    total = conn.execute("SELECT COUNT(DISTINCT zip_code) FROM plz_grid_area_mapping").fetchone()[0]
    rows = conn.execute("SELECT COUNT(*) FROM plz_grid_area_mapping").fetchone()[0]
    logger.info("=== COMPLETE: %d valid PLZs, %d total mappings ===", total, rows)

    for row in conn.execute(
        "SELECT energy_type, COUNT(DISTINCT zip_code), COUNT(DISTINCT grid_area_id) "
        "FROM plz_grid_area_mapping GROUP BY energy_type"
    ).fetchall():
        logger.info("  %s: %d PLZs, %d grid areas", row[0], row[1], row[2])

    conn.close()


if __name__ == "__main__":
    main()
