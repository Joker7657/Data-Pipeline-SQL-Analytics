"""
Simple end-to-end pipeline: ingest CSVs, build a small warehouse in DuckDB,
and run complex analytics queries.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict, List, Optional

import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
WAREHOUSE_PATH = BASE_DIR / "data" / "warehouse.duckdb"
QUERIES_FILE = BASE_DIR / "sql" / "complex_queries.sql"


def ensure_data_dirs() -> None:
    """Guarantee that the data folders and database file exist."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    ensure_data_dirs()
    return duckdb.connect(str(WAREHOUSE_PATH), read_only=read_only)


def ingest_raw(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE staging.customers AS
        SELECT * FROM read_csv_auto('{RAW_DIR / 'customers.csv'}');
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE staging.products AS
        SELECT * FROM read_csv_auto('{RAW_DIR / 'products.csv'}');
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE staging.orders AS
        SELECT * FROM read_csv_auto('{RAW_DIR / 'orders.csv'}');
        """
    )


def transform(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    conn.execute(
        """
        CREATE OR REPLACE TABLE mart.dim_customers AS
        SELECT DISTINCT
            CAST(customer_id AS INTEGER) AS customer_id,
            name,
            country,
            CAST(signup_date AS DATE) AS signup_date
        FROM staging.customers;
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE mart.dim_products AS
        SELECT DISTINCT
            CAST(product_id AS INTEGER) AS product_id,
            name,
            category,
            CAST(unit_price AS DOUBLE) AS base_price
        FROM staging.products;
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE mart.fact_orders AS
        SELECT
            CAST(order_id AS INTEGER) AS order_id,
            CAST(customer_id AS INTEGER) AS customer_id,
            CAST(product_id AS INTEGER) AS product_id,
            CAST(order_timestamp AS TIMESTAMP) AS order_ts,
            LOWER(status) AS status,
            CAST(quantity AS INTEGER) AS quantity,
            CAST(unit_price AS DOUBLE) AS unit_price,
            quantity * unit_price AS gross_revenue
        FROM staging.orders
        WHERE LOWER(status) NOT IN ('cancelled', 'refunded');
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE mart.metrics_daily AS
        SELECT
            CAST(order_ts AS DATE) AS order_date,
            SUM(gross_revenue) AS revenue,
            SUM(quantity) AS units,
            COUNT(*) AS orders,
            APPROX_QUANTILE(gross_revenue, 0.95) AS p95_revenue
        FROM mart.fact_orders
        GROUP BY 1;
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE mart.customer_rollups AS
        SELECT
            customer_id,
            COUNT(*) AS order_count,
            SUM(gross_revenue) AS total_revenue,
            MIN(order_ts) AS first_order_ts,
            MAX(order_ts) AS last_order_ts,
            AVG(gross_revenue) AS avg_ticket,
            SUM(quantity) AS units,
            ROW_NUMBER() OVER (ORDER BY SUM(gross_revenue) DESC) AS revenue_rank
        FROM mart.fact_orders
        GROUP BY 1;
        """
    )

    # Collect statistics to help the optimizer
    conn.execute("ANALYZE;")


def run_etl(verbose: bool = False) -> None:
    with get_connection() as conn:
        t0 = time.time()
        ingest_raw(conn)
        transform(conn)
        elapsed = time.time() - t0
        if verbose:
            print(f"ETL finished in {elapsed:.3f}s -> {WAREHOUSE_PATH}")


def parse_named_queries(path: Path) -> Dict[str, str]:
    queries: Dict[str, str] = {}
    current_name: Optional[str] = None
    buffer: List[str] = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip().startswith("-- name:"):
                if current_name and buffer:
                    queries[current_name] = "".join(buffer).strip()
                    buffer = []
                current_name = line.split(":", 1)[1].strip()
            else:
                buffer.append(line)

    if current_name and buffer:
        queries[current_name] = "".join(buffer).strip()

    return queries


def run_queries(selected: Optional[str] = None, explain: bool = False) -> None:
    queries = parse_named_queries(QUERIES_FILE)
    if selected and selected not in queries:
        available = ", ".join(sorted(queries))
        raise SystemExit(f"Query '{selected}' not found. Available: {available}")

    with get_connection(read_only=True) as conn:
        for name, sql in queries.items():
            if selected and name != selected:
                continue

            heading = f"\n-- {name} --"
            print(heading)
            plan_prefix = "EXPLAIN ANALYZE " if explain else ""
            result = conn.execute(plan_prefix + sql)

            if explain:
                print(result.fetchall())
            else:
                for row in result.fetchall():
                    print(row)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ETL and analytics queries")
    sub = parser.add_subparsers(dest="command", required=True)

    etl_cmd = sub.add_parser("etl", help="Run ingestion and transforms")
    etl_cmd.add_argument("--verbose", action="store_true", help="Print timing info")

    queries_cmd = sub.add_parser("queries", help="Execute analytics queries")
    queries_cmd.add_argument("--name", help="Run only one named query")
    queries_cmd.add_argument("--explain", action="store_true", help="Show EXPLAIN ANALYZE output")

    full_cmd = sub.add_parser("full", help="Run ETL then queries")
    full_cmd.add_argument("--explain", action="store_true", help="Show EXPLAIN ANALYZE output")

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.command == "etl":
        run_etl(verbose=args.verbose)
    elif args.command == "queries":
        run_queries(selected=args.name, explain=args.explain)
    elif args.command == "full":
        run_etl(verbose=True)
        run_queries(explain=args.explain)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
