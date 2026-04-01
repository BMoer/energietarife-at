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
    compare          - Compare tariffs for a ZIP code and custom kWh
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _get_latest_run_id(conn: sqlite3.Connection) -> int | None:
    """Get the latest completed scrape run ID."""
    row = conn.execute(
        "SELECT id FROM scrape_runs WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row["id"] if row else None


def _get_grid_area_for_plz(conn: sqlite3.Connection, zip_code: str, energy_type: str) -> int | None:
    """Resolve PLZ → grid_area_id via plz_grid_area_mapping or grid_operator_zip_codes."""
    # Try plz_grid_area_mapping first (most complete)
    row = conn.execute(
        "SELECT grid_area_id FROM plz_grid_area_mapping WHERE zip_code = ? AND energy_type = ? LIMIT 1",
        (zip_code, energy_type),
    ).fetchone()
    if row:
        return row["grid_area_id"]

    # Fallback: grid_operator_zip_codes → grid_operators
    row = conn.execute("""
        SELECT go.grid_area_id
        FROM grid_operator_zip_codes gz
        JOIN grid_operators go ON gz.grid_operator_id = go.id AND gz.energy_type = go.energy_type
        WHERE gz.zip_code = ? AND gz.energy_type = ?
        LIMIT 1
    """, (zip_code, energy_type)).fetchone()
    return row["grid_area_id"] if row else None


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
        n_grid_rates = conn.execute(
            "SELECT COUNT(*) FROM grid_area_rates WHERE energy_type = ?", (energy,)
        ).fetchone()[0]

        print(f"{label}:")
        print(f"  Anbieter (Brands): {n_brands}")
        print(f"  Produkte:          {n_products}")
        print(f"  Netzbetreiber:     {n_grid_ops}")
        print(f"  Tarif-Rates:       {n_rates}")
        print(f"  Netz-Rates:        {n_grid_rates}")
        print()

    n_plz = conn.execute("SELECT COUNT(DISTINCT zip_code) FROM plz_grid_area_mapping").fetchone()[0]
    print(f"PLZ-Mapping: {n_plz} PLZ\n")

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


def cmd_cheapest(conn: sqlite3.Connection, energy_type: str, n: int = 10, kwh: int = 3500):
    label = "Strom" if energy_type == "POWER" else "Gas"
    run_id = _get_latest_run_id(conn)
    if not run_id:
        print("Keine Scrape-Daten vorhanden.")
        return

    print(f"\n=== Top {n} günstigste {label}-Tarife ({kwh} kWh/a) ===\n")

    # Calculate energy cost for given kWh, add grid costs, apply 20% VAT
    rows = conn.execute("""
        SELECT
            p.brand_name,
            p.product_name,
            pr.energy_ct_kwh,
            pr.energy_base_ct_year,
            pr.energy_fees_ct_year,
            pr.grid_area_id,
            p.price_guarantee_type,
            p.is_certified_green
        FROM product_rates pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.scrape_run_id = ? AND pr.energy_type = ? AND pr.energy_ct_kwh > 0
        ORDER BY (pr.energy_ct_kwh * ? + pr.energy_base_ct_year) ASC
        LIMIT ?
    """, (run_id, energy_type, kwh, n)).fetchall()

    for i, r in enumerate(rows, 1):
        energy_netto = r["energy_ct_kwh"] * kwh + r["energy_base_ct_year"]
        energy_brutto = energy_netto * 1.2 / 100

        # Get grid costs for this area
        grid = conn.execute("""
            SELECT grid_ct_kwh, grid_base_ct_year, grid_loss_ct_year,
                   meter_ct_year, grid_fees_ct_year
            FROM grid_area_rates
            WHERE scrape_run_id = ? AND grid_area_id = ? AND energy_type = ?
            LIMIT 1
        """, (run_id, r["grid_area_id"], energy_type)).fetchone()

        grid_brutto = 0
        if grid:
            grid_netto = (grid["grid_ct_kwh"] * kwh + grid["grid_base_ct_year"]
                          + grid["grid_loss_ct_year"] + grid["meter_ct_year"]
                          + grid["grid_fees_ct_year"])
            grid_brutto = grid_netto * 1.2 / 100

        total = energy_brutto + grid_brutto

        guarantee = {"GUARANTEE": "Fix", "NO_GUARANTEE": "Var",
                     "ADJUSTING": "Anp", "DYNAMIC": "Dyn"}.get(
            r["price_guarantee_type"], "?")
        green = "🌿" if r["is_certified_green"] else "  "

        print(f"{i:3d}. {total:>8.2f} EUR/a {green} | "
              f"{r['energy_ct_kwh']:>6.2f} ct/kWh + {r['energy_base_ct_year']/100:>5.2f}€ Basis | "
              f"Netz {grid_brutto:>6.2f}€ | "
              f"{r['brand_name'][:25]:<25s} | {r['product_name'][:30]:<30s} | {guarantee}")

    print(f"\n{len(rows)} Tarife | Preise brutto (inkl. 20% USt)")


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


def cmd_compare(conn: sqlite3.Connection, zip_code: str, energy_type: str, kwh: int = 3500):
    label = "Strom" if energy_type == "POWER" else "Gas"
    run_id = _get_latest_run_id(conn)
    if not run_id:
        print("Keine Scrape-Daten vorhanden.")
        return

    grid_area_id = _get_grid_area_for_plz(conn, zip_code, energy_type)
    if not grid_area_id:
        print(f"PLZ {zip_code} nicht gefunden. Bitte scrape_plz_mapping.py ausführen.")
        return

    print(f"\n=== {label}-Tarife für PLZ {zip_code} ({kwh} kWh/a) ===\n")

    # Get grid costs for this area
    grid = conn.execute("""
        SELECT grid_ct_kwh, grid_base_ct_year, grid_loss_ct_year,
               meter_ct_year, grid_fees_ct_year, grid_operator_name
        FROM grid_area_rates
        WHERE scrape_run_id = ? AND grid_area_id = ? AND energy_type = ?
        LIMIT 1
    """, (run_id, grid_area_id, energy_type)).fetchone()

    grid_brutto = 0
    if grid:
        grid_netto = (grid["grid_ct_kwh"] * kwh + grid["grid_base_ct_year"]
                      + grid["grid_loss_ct_year"] + grid["meter_ct_year"]
                      + grid["grid_fees_ct_year"])
        grid_brutto = grid_netto * 1.2 / 100
        print(f"Netzbetreiber: {grid['grid_operator_name']}")
        print(f"Netzkosten:    {grid_brutto:.2f} EUR/a brutto "
              f"({grid['grid_ct_kwh']:.4f} ct/kWh + {grid['grid_base_ct_year']/100:.2f}€ Basis)\n")

    # Get all tariffs for this grid area, sorted by calculated cost
    rows = conn.execute("""
        SELECT
            p.brand_name,
            p.product_name,
            pr.energy_ct_kwh,
            pr.energy_base_ct_year,
            p.price_guarantee_type,
            p.is_certified_green
        FROM product_rates pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.scrape_run_id = ? AND pr.grid_area_id = ? AND pr.energy_type = ?
              AND pr.energy_ct_kwh > 0
        ORDER BY (pr.energy_ct_kwh * ? + pr.energy_base_ct_year) ASC
    """, (run_id, grid_area_id, energy_type, kwh)).fetchall()

    for i, r in enumerate(rows, 1):
        energy_netto = r["energy_ct_kwh"] * kwh + r["energy_base_ct_year"]
        energy_brutto = energy_netto * 1.2 / 100
        total = energy_brutto + grid_brutto

        guarantee = {"GUARANTEE": "Fix", "NO_GUARANTEE": "Var",
                     "ADJUSTING": "Anp", "DYNAMIC": "Dyn"}.get(
            r["price_guarantee_type"], "?")
        green = "🌿" if r["is_certified_green"] else "  "

        print(f"{i:3d}. {total:>8.2f} EUR/a {green} | "
              f"Energie {energy_brutto:>7.2f}€ | "
              f"{r['energy_ct_kwh']:>6.2f} ct/kWh + {r['energy_base_ct_year']/100:>5.2f}€ | "
              f"{r['brand_name'][:25]:<25s} | {r['product_name'][:30]} | {guarantee}")

    print(f"\n{len(rows)} Tarife | Alle Preise brutto (inkl. 20% USt)")


def main():
    parser = argparse.ArgumentParser(description="E-Control Tarif-Datenbank Abfrage")
    parser.add_argument("--db", default="tarife.db", help="SQLite database path")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("summary")

    p_cheap = sub.add_parser("cheapest")
    p_cheap.add_argument("--energy-type", choices=["POWER", "GAS"], default="POWER")
    p_cheap.add_argument("-n", type=int, default=10)
    p_cheap.add_argument("--kwh", type=int, default=3500, help="Jahresverbrauch in kWh")

    p_brands = sub.add_parser("brands")
    p_brands.add_argument("--energy-type", choices=["POWER", "GAS"])

    p_go = sub.add_parser("grid-operators")
    p_go.add_argument("--energy-type", choices=["POWER", "GAS"])

    p_search = sub.add_parser("search")
    p_search.add_argument("term")

    p_compare = sub.add_parser("compare")
    p_compare.add_argument("zip_code")
    p_compare.add_argument("--energy-type", choices=["POWER", "GAS"], default="POWER")
    p_compare.add_argument("--kwh", type=int, default=3500, help="Jahresverbrauch in kWh")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    conn = get_conn(args.db)

    match args.command:
        case "summary":
            cmd_summary(conn)
        case "cheapest":
            cmd_cheapest(conn, args.energy_type, args.n, args.kwh)
        case "brands":
            cmd_brands(conn, args.energy_type)
        case "grid-operators":
            cmd_grid_operators(conn, args.energy_type)
        case "search":
            cmd_search(conn, args.term)
        case "compare":
            cmd_compare(conn, args.zip_code, args.energy_type, args.kwh)

    conn.close()


if __name__ == "__main__":
    main()
