#!/usr/bin/env python3
"""
Query utility for the E-Control tariff database.

Usage:
    python3 query_tarife.py [--db tarife.db] <command> [options]

Commands:
    summary          - Show database summary
    cheapest         - Show cheapest tariffs per grid area
    brands           - List all brands/suppliers
    grid-operators   - List all grid operators
    search           - Search products by name
    compare          - Compare tariffs for a ZIP code
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_summary(conn: sqlite3.Connection):
    print("=== E-Control Tarif-Datenbank ===\n")

    for energy in ["POWER", "GAS"]:
        label = "Strom" if energy == "POWER" else "Gas"
        n_brands = conn.execute(
            "SELECT COUNT(DISTINCT id) FROM brands WHERE energy_type = ?", (energy,)
        ).fetchone()[0]
        n_products = conn.execute(
            "SELECT COUNT(*) FROM products WHERE energy_type = ?", (energy,)
        ).fetchone()[0]
        n_grid_ops = conn.execute(
            "SELECT COUNT(*) FROM grid_operators WHERE energy_type = ?", (energy,)
        ).fetchone()[0]
        n_rates = conn.execute(
            "SELECT COUNT(*) FROM product_rates WHERE energy_type = ?", (energy,)
        ).fetchone()[0]

        print(f"{label}:")
        print(f"  Anbieter (Brands): {n_brands}")
        print(f"  Produkte:          {n_products}")
        print(f"  Netzbetreiber:     {n_grid_ops}")
        print(f"  Tarifeinträge:     {n_rates}")
        print()

    runs = conn.execute(
        "SELECT * FROM scrape_runs ORDER BY id DESC LIMIT 5"
    ).fetchall()
    if runs:
        print("Letzte Scrape-Runs:")
        for r in runs:
            print(f"  #{r['id']}: {r['started_at'][:19]} | "
                  f"{r['energy_type']} | "
                  f"{r['zip_codes_queried']} PLZ -> "
                  f"{r['grid_areas_found']} Netzgebiete -> "
                  f"{r['products_found']} Tarife")


def cmd_cheapest(conn: sqlite3.Connection, energy_type: str, n: int = 10):
    label = "Strom" if energy_type == "POWER" else "Gas"
    print(f"\n=== Top {n} günstigste {label}-Tarife ===\n")

    rows = conn.execute("""
        SELECT
            pr.product_id,
            p.product_name,
            p.brand_name,
            pr.grid_operator_name,
            pr.zip_code,
            pr.annual_gross_rate_cents / 100.0 AS annual_eur,
            pr.avg_total_price_cent_kwh,
            pr.annual_consumption_kwh,
            p.price_guarantee_type,
            pr.scraped_at
        FROM product_rates pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.energy_type = ?
        ORDER BY pr.annual_gross_rate_cents ASC
        LIMIT ?
    """, (energy_type, n)).fetchall()

    for i, r in enumerate(rows, 1):
        print(f"{i:2d}. {r['brand_name']}")
        print(f"    Produkt:       {r['product_name']}")
        print(f"    Jahreskosten:  {r['annual_eur']:.2f} EUR ({r['annual_consumption_kwh']} kWh)")
        if r["avg_total_price_cent_kwh"]:
            print(f"    Gesamtpreis:   {r['avg_total_price_cent_kwh']:.2f} ct/kWh")
        print(f"    Netzbetreiber: {r['grid_operator_name']} (PLZ {r['zip_code']})")
        if r["price_guarantee_type"]:
            print(f"    Preisgarantie: {r['price_guarantee_type']}")
        print()


def cmd_brands(conn: sqlite3.Connection, energy_type: str | None = None):
    print("\n=== Anbieter ===\n")

    query = "SELECT DISTINCT brand_name, supplier_name, energy_type FROM brands"
    params = ()
    if energy_type:
        query += " WHERE energy_type = ?"
        params = (energy_type,)
    query += " ORDER BY brand_name"

    rows = conn.execute(query, params).fetchall()
    for r in rows:
        label = "⚡" if r["energy_type"] == "POWER" else "🔥"
        supplier = f" ({r['supplier_name']})" if r["supplier_name"] != r["brand_name"] else ""
        print(f"  {label} {r['brand_name']}{supplier}")

    print(f"\nGesamt: {len(rows)} Anbieter")


def cmd_grid_operators(conn: sqlite3.Connection, energy_type: str | None = None):
    print("\n=== Netzbetreiber ===\n")

    query = """
        SELECT go.*, COUNT(gz.zip_code) as n_zips
        FROM grid_operators go
        LEFT JOIN grid_operator_zip_codes gz ON go.id = gz.grid_operator_id AND go.energy_type = gz.energy_type
    """
    params = ()
    if energy_type:
        query += " WHERE go.energy_type = ?"
        params = (energy_type,)
    query += " GROUP BY go.id, go.energy_type ORDER BY go.name"

    rows = conn.execute(query, params).fetchall()
    for r in rows:
        label = "⚡" if r["energy_type"] == "POWER" else "🔥"
        print(f"  {label} {r['name']:45s} | Netzgebiet {r['grid_area_id']:5d} | {r['n_zips']} PLZ")

    print(f"\nGesamt: {len(rows)} Netzbetreiber")


def cmd_search(conn: sqlite3.Connection, term: str):
    print(f"\n=== Suche: '{term}' ===\n")

    rows = conn.execute("""
        SELECT DISTINCT p.product_name, p.brand_name, p.energy_type,
               p.customer_group, p.price_guarantee_type
        FROM products p
        WHERE p.product_name LIKE ? OR p.brand_name LIKE ?
        ORDER BY p.brand_name, p.product_name
    """, (f"%{term}%", f"%{term}%")).fetchall()

    for r in rows:
        label = "⚡" if r["energy_type"] == "POWER" else "🔥"
        print(f"  {label} {r['brand_name']:35s} | {r['product_name']:35s} | "
              f"{r['customer_group']} | {r['price_guarantee_type'] or '-'}")

    print(f"\n{len(rows)} Ergebnisse")


def cmd_compare(conn: sqlite3.Connection, zip_code: str, energy_type: str):
    label = "Strom" if energy_type == "POWER" else "Gas"
    print(f"\n=== {label}-Tarife für PLZ {zip_code} ===\n")

    rows = conn.execute("""
        SELECT
            p.brand_name,
            p.product_name,
            pr.annual_gross_rate_cents / 100.0 AS annual_eur,
            pr.avg_total_price_cent_kwh,
            pr.annual_consumption_kwh,
            p.price_guarantee_type,
            pr.energy_rate_total / 100.0 AS energy_eur,
            pr.grid_rate_total / 100.0 AS grid_eur,
            pr.taxes_and_levies_total / 100.0 AS taxes_eur
        FROM product_rates pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.zip_code = ? AND pr.energy_type = ?
        ORDER BY pr.annual_gross_rate_cents ASC
    """, (zip_code, energy_type)).fetchall()

    if not rows:
        # Try finding rates for the same grid area
        go = conn.execute("""
            SELECT grid_area_id, grid_operator_id FROM grid_operator_zip_codes gz
            JOIN grid_operators go ON gz.grid_operator_id = go.id AND gz.energy_type = go.energy_type
            WHERE gz.zip_code = ? AND gz.energy_type = ?
        """, (zip_code, energy_type)).fetchone()

        if go:
            rows = conn.execute("""
                SELECT
                    p.brand_name, p.product_name,
                    pr.annual_gross_rate_cents / 100.0 AS annual_eur,
                    pr.avg_total_price_cent_kwh,
                    pr.annual_consumption_kwh,
                    p.price_guarantee_type,
                    pr.energy_rate_total / 100.0 AS energy_eur,
                    pr.grid_rate_total / 100.0 AS grid_eur,
                    pr.taxes_and_levies_total / 100.0 AS taxes_eur
                FROM product_rates pr
                JOIN products p ON pr.product_id = p.id
                WHERE pr.grid_area_id = ? AND pr.energy_type = ?
                ORDER BY pr.annual_gross_rate_cents ASC
            """, (go["grid_area_id"], energy_type)).fetchall()

    for i, r in enumerate(rows, 1):
        guarantee = {"GUARANTEE": "Fix", "NO_GUARANTEE": "Variabel",
                     "ADJUSTING": "Anpassend", "DYNAMIC": "Dynamisch"}.get(
            r["price_guarantee_type"], r["price_guarantee_type"] or "-")
        print(f"{i:3d}. {r['annual_eur']:>8.2f} EUR/a | "
              f"{r['brand_name']:30s} | {r['product_name']:35s} | {guarantee}")

    print(f"\n{len(rows)} Tarife ({rows[0]['annual_consumption_kwh'] if rows else '?'} kWh/a)")


def main():
    parser = argparse.ArgumentParser(description="E-Control Tarif-Datenbank Abfrage")
    parser.add_argument("--db", default="tarife.db", help="SQLite database path")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("summary")

    p_cheap = sub.add_parser("cheapest")
    p_cheap.add_argument("--energy-type", choices=["POWER", "GAS"], default="POWER")
    p_cheap.add_argument("-n", type=int, default=10)

    p_brands = sub.add_parser("brands")
    p_brands.add_argument("--energy-type", choices=["POWER", "GAS"])

    p_go = sub.add_parser("grid-operators")
    p_go.add_argument("--energy-type", choices=["POWER", "GAS"])

    p_search = sub.add_parser("search")
    p_search.add_argument("term")

    p_compare = sub.add_parser("compare")
    p_compare.add_argument("zip_code")
    p_compare.add_argument("--energy-type", choices=["POWER", "GAS"], default="POWER")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    conn = get_conn(args.db)

    match args.command:
        case "summary":
            cmd_summary(conn)
        case "cheapest":
            cmd_cheapest(conn, args.energy_type, args.n)
        case "brands":
            cmd_brands(conn, args.energy_type)
        case "grid-operators":
            cmd_grid_operators(conn, args.energy_type)
        case "search":
            cmd_search(conn, args.term)
        case "compare":
            cmd_compare(conn, args.zip_code, args.energy_type)

    conn.close()


if __name__ == "__main__":
    main()
