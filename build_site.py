#!/usr/bin/env python3
"""
Build script: exports tariff data from SQLite to JSON for the static site.

Usage:
    python3 build_site.py [--db tarife.db]
"""

import json
import sqlite3
import sys
from pathlib import Path


def export_historical(conn: sqlite3.Connection) -> list[dict]:
    """Export historical products as JSON.

    Includes both historical_products AND current product_rates (mapped to the
    same schema) so that recently-added tariffs (e.g. spot/float) appear even
    before a full historical re-scrape.
    """
    conn.row_factory = sqlite3.Row

    # Check if historical_products table exists (only created by scrape_all_regions.py)
    has_historical = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='historical_products'"
    ).fetchone()[0] > 0

    if has_historical:
        rows = conn.execute("""
            SELECT product_id, product_name, brand_name, supplier_name, energy_type,
                   product_validity_from, regular_customers_from,
                   energy_rate_ct_kwh, energy_rate_high_ct_kwh, energy_rate_low_ct_kwh,
                   base_rate_cents, base_rate_type, rate_type, rate_zoning_type,
                   locations, grid_area_id, zip_code
            FROM historical_products
            WHERE energy_rate_ct_kwh IS NOT NULL AND product_validity_from IS NOT NULL

            UNION ALL

            SELECT
                p.id AS product_id,
                p.product_name,
                p.brand_name,
                p.supplier_name,
                p.energy_type,
                substr(pr.scraped_at, 1, 10) AS product_validity_from,
                NULL AS regular_customers_from,
                pr.energy_ct_kwh AS energy_rate_ct_kwh,
                NULL AS energy_rate_high_ct_kwh,
                NULL AS energy_rate_low_ct_kwh,
                pr.energy_base_ct_year AS base_rate_cents,
                NULL AS base_rate_type,
                NULL AS rate_type,
                NULL AS rate_zoning_type,
                NULL AS locations,
                pr.grid_area_id,
                pr.zip_code
            FROM product_rates pr
            JOIN products p ON pr.product_id = p.id
            WHERE pr.energy_ct_kwh > 0
              AND NOT EXISTS (
                  SELECT 1 FROM historical_products hp
                  WHERE hp.product_id = p.id AND hp.grid_area_id = pr.grid_area_id
              )

            ORDER BY product_validity_from
        """).fetchall()
    else:
        # Fallback: only export current product_rates as historical data
        rows = conn.execute("""
            SELECT
                p.id AS product_id,
                p.product_name,
                p.brand_name,
                p.supplier_name,
                p.energy_type,
                substr(pr.scraped_at, 1, 10) AS product_validity_from,
                NULL AS regular_customers_from,
                pr.energy_ct_kwh AS energy_rate_ct_kwh,
                NULL AS energy_rate_high_ct_kwh,
                NULL AS energy_rate_low_ct_kwh,
                pr.energy_base_ct_year AS base_rate_cents,
                NULL AS base_rate_type,
                NULL AS rate_type,
                NULL AS rate_zoning_type,
                NULL AS locations,
                pr.grid_area_id,
                pr.zip_code
            FROM product_rates pr
            JOIN products p ON pr.product_id = p.id
            WHERE pr.energy_ct_kwh > 0
            ORDER BY product_validity_from
        """).fetchall()
    return [dict(r) for r in rows]


def export_current(conn: sqlite3.Connection) -> list[dict]:
    """Export current product rates as JSON with per-unit pricing."""
    conn.row_factory = sqlite3.Row

    # Get latest run ID
    run_id = conn.execute(
        "SELECT id FROM scrape_runs WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not run_id:
        return []
    run_id = run_id["id"]

    rows = conn.execute("""
        SELECT
            pr.product_id, p.product_name, p.brand_name, p.supplier_name,
            p.energy_type,
            p.price_guarantee_type, p.is_online_product, p.is_certified_green,
            pr.grid_operator_name, pr.grid_area_id, pr.zip_code,
            pr.energy_ct_kwh,
            pr.energy_base_ct_year / 100.0 AS energy_base_eur_year,
            pr.energy_fees_ct_year / 100.0 AS energy_fees_eur_year,
            pr.energy_discount_ct_year / 100.0 AS energy_discount_eur_year,
            pr.reference_consumption_kwh,
            pr.annual_total_brutto_ct / 100.0 AS annual_total_brutto_eur,
            pr.scraped_at
        FROM product_rates pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.scrape_run_id = ? AND pr.energy_ct_kwh > 0
        ORDER BY (pr.energy_ct_kwh * pr.reference_consumption_kwh + pr.energy_base_ct_year)
    """, (run_id,)).fetchall()
    return [dict(r) for r in rows]


def export_grid_rates(conn: sqlite3.Connection) -> list[dict]:
    """Export grid area rates as JSON."""
    conn.row_factory = sqlite3.Row

    run_id = conn.execute(
        "SELECT id FROM scrape_runs WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not run_id:
        return []
    run_id = run_id["id"]

    rows = conn.execute("""
        SELECT
            grid_area_id, grid_operator_name, energy_type,
            grid_ct_kwh,
            grid_base_ct_year / 100.0 AS grid_base_eur_year,
            grid_loss_ct_year / 100.0 AS grid_loss_eur_year,
            meter_ct_year / 100.0 AS meter_eur_year,
            grid_fees_ct_year / 100.0 AS grid_fees_eur_year,
            reference_consumption_kwh,
            scraped_at
        FROM grid_area_rates
        WHERE scrape_run_id = ?
    """, (run_id,)).fetchall()
    return [dict(r) for r in rows]


def export_brands(conn: sqlite3.Connection) -> list[dict]:
    """Export brands list."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT brand_name, supplier_name, energy_type
        FROM brands ORDER BY brand_name
    """).fetchall()
    return [dict(r) for r in rows]


def export_stats(conn: sqlite3.Connection) -> dict:
    """Export summary statistics."""
    conn.row_factory = sqlite3.Row

    has_historical = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='historical_products'"
    ).fetchone()[0] > 0

    stats = {"energy_types": {}}
    for et in ["POWER", "GAS"]:
        if has_historical:
            hist_count = conn.execute(
                "SELECT COUNT(*) as c FROM historical_products WHERE energy_type = ?", (et,)
            ).fetchone()["c"]
            hist_brands = conn.execute(
                "SELECT COUNT(DISTINCT brand_name) as c FROM historical_products WHERE energy_type = ?", (et,)
            ).fetchone()["c"]
            date_range = conn.execute(
                "SELECT MIN(product_validity_from) as earliest, MAX(product_validity_from) as latest "
                "FROM historical_products WHERE energy_type = ?", (et,)
            ).fetchone()
        else:
            hist_count = 0
            hist_brands = 0
            date_range = {"earliest": None, "latest": None}

        stats["energy_types"][et] = {
            "historical_products": hist_count,
            "brands": hist_brands,
            "earliest": date_range["earliest"],
            "latest": date_range["latest"],
        }

    stats["total_historical"] = conn.execute(
        "SELECT COUNT(*) as c FROM historical_products"
    ).fetchone()["c"] if has_historical else 0

    return stats


def export_plz_mapping(conn: sqlite3.Connection, out_dir: Path):
    """Export PLZ → grid area mapping as JSON. Format: { "1010": { "POWER": [651], "GAS": [1001] }, ... }"""
    conn.row_factory = sqlite3.Row

    has_table = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='plz_grid_area_mapping'"
    ).fetchone()[0] > 0
    if not has_table:
        print("  plz-mapping.json: skipped (table not found)")
        return

    rows = conn.execute(
        "SELECT zip_code, energy_type, grid_area_id, grid_operator_name "
        "FROM plz_grid_area_mapping ORDER BY zip_code"
    ).fetchall()

    mapping = {}
    operator_names = {}
    for r in rows:
        plz = r["zip_code"]
        et = r["energy_type"]
        ga = r["grid_area_id"]
        if plz not in mapping:
            mapping[plz] = {}
        if et not in mapping[plz]:
            mapping[plz][et] = []
        mapping[plz][et].append(ga)
        operator_names[ga] = r["grid_operator_name"]

    output = {"mapping": mapping, "gridOperatorNames": operator_names}
    (out_dir / "plz-mapping.json").write_text(
        json.dumps(output, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    print(f"  plz-mapping.json: {len(mapping)} PLZs, {len(operator_names)} grid operators")


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "tarife.db"
    out_dir = Path("public/data")
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    # Historical tariffs (main dataset for visualization)
    historical = export_historical(conn)
    (out_dir / "historical.json").write_text(
        json.dumps(historical, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  historical.json: {len(historical)} records")

    # Current rates (with per-unit pricing)
    current = export_current(conn)
    (out_dir / "current.json").write_text(
        json.dumps(current, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  current.json: {len(current)} records")

    # Grid area rates
    grid_rates = export_grid_rates(conn)
    (out_dir / "grid-rates.json").write_text(
        json.dumps(grid_rates, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  grid-rates.json: {len(grid_rates)} records")

    # Brands
    brands = export_brands(conn)
    (out_dir / "brands.json").write_text(
        json.dumps(brands, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  brands.json: {len(brands)} records")

    # Stats
    stats = export_stats(conn)
    (out_dir / "stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"  stats.json: written")

    # PLZ → grid area mapping
    export_plz_mapping(conn, out_dir)

    conn.close()
    print(f"Done! Files in {out_dir}/")


if __name__ == "__main__":
    main()
