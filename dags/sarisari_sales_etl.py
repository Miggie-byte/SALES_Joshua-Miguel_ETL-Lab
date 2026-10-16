"""Sari-sari store sales ETL.

landing CSV -> data lake raw zone -> data lake staged zone (Parquet)
-> PostgreSQL staging -> star schema (dw) -> quality checks
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

LANDING = Path("/opt/airflow/data/landing")
RAW = Path("/opt/airflow/datalake/raw/sales")
STAGED = Path("/opt/airflow/datalake/staged/sales")
DWH_CONN_ID = "dwh_postgres"  # comes from AIRFLOW_CONN_DWH_POSTGRES in docker-compose

STG_COLUMNS = [
    "order_id", "order_date", "store_id", "store_name", "barangay", "city",
    "product_id", "product_name", "category", "quantity", "unit_price",
    "line_total", "batch_id",
]

# Staging -> star schema. Every statement is an upsert, so re-running is safe.
LOAD_DW_SQL = """
INSERT INTO dw.dim_date (date_key, full_date, day_of_week, is_weekend,
                         month, month_name, quarter, year)
SELECT DISTINCT
    TO_CHAR(order_date, 'YYYYMMDD')::INT,
    order_date,
    TRIM(TO_CHAR(order_date, 'Day')),
    EXTRACT(ISODOW FROM order_date) IN (6, 7),
    EXTRACT(MONTH FROM order_date)::INT,
    TRIM(TO_CHAR(order_date, 'Month')),
    EXTRACT(QUARTER FROM order_date)::INT,
    EXTRACT(YEAR FROM order_date)::INT
FROM staging.stg_sales
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dw.dim_store (store_id, store_name, barangay, city)
SELECT DISTINCT ON (store_id) store_id, store_name, barangay, city
FROM staging.stg_sales
ORDER BY store_id
ON CONFLICT (store_id) DO UPDATE
SET store_name = EXCLUDED.store_name,
    barangay   = EXCLUDED.barangay,
    city       = EXCLUDED.city;

INSERT INTO dw.dim_product (product_id, product_name, category)
SELECT DISTINCT ON (product_id) product_id, product_name, category
FROM staging.stg_sales
ORDER BY product_id
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category;

INSERT INTO dw.fact_sales (order_id, date_key, store_key, product_key,
                           quantity, unit_price, line_total, batch_id)
SELECT s.order_id,
       TO_CHAR(s.order_date, 'YYYYMMDD')::INT,
       ds.store_key,
       dp.product_key,
       s.quantity, s.unit_price, s.line_total, s.batch_id
FROM staging.stg_sales s
JOIN dw.dim_store ds   ON ds.store_id   = s.store_id
JOIN dw.dim_product dp ON dp.product_id = s.product_id
ON CONFLICT (order_id, product_key) DO UPDATE
SET date_key   = EXCLUDED.date_key,
    store_key  = EXCLUDED.store_key,
    quantity   = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_total = EXCLUDED.line_total,
    batch_id   = EXCLUDED.batch_id;
"""


@dag(
    dag_id="sarisari_sales_etl",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["cselec1c", "etl"],
)
def sarisari_sales_etl():
    @task
    def extract() -> dict:
        """E: copy landing files, untouched, into a new batch folder in the raw zone."""
        files = sorted(LANDING.glob("sales_*.csv"))
        if not files:
            raise FileNotFoundError(f"No sales_*.csv files found in {LANDING}")

        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        target = RAW / f"ingest_date={batch_id[:8]}" / f"batch={batch_id}"
        target.mkdir(parents=True, exist_ok=True)

        copied = []
        for f in files:
            shutil.copy2(f, target / f.name)
            copied.append(str(target / f.name))

        print(f"Copied {len(copied)} file(s) to {target}")
        return {"batch_id": batch_id, "files": copied}

    @task
    def transform(raw: dict) -> dict:
        """T: clean and type the raw rows, write Parquet to the staged zone."""
        df = pd.concat(
            (pd.read_csv(p, dtype=str) for p in raw["files"]), ignore_index=True
        )
        rows_in = len(df)

        df = df.apply(lambda col: col.str.strip())            # stray spaces
        df["category"] = df["category"].str.title()           # 'canned goods' -> 'Canned Goods'
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

        required = ["order_id", "order_date", "store_id", "product_id",
                    "quantity", "unit_price"]
        df = df.dropna(subset=required)                       # missing values
        df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)]  # impossible values
        df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")  # duplicates

        df["quantity"] = df["quantity"].astype(int)
        df["line_total"] = (df["quantity"] * df["unit_price"]).round(2)
        df["batch_id"] = raw["batch_id"]

        out = STAGED / f"batch={raw['batch_id']}" / "sales_clean.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df[STG_COLUMNS].to_parquet(out, index=False)

        print(f"Rows in: {rows_in}, rows kept: {len(df)}, rejected: {rows_in - len(df)}")
        return {"batch_id": raw["batch_id"], "path": str(out),
                "rows_in": rows_in, "rows_out": len(df)}

    @task
    def load(staged: dict) -> dict:
        """L: staged Parquet -> staging table -> dimensions and fact."""
        df = pd.read_parquet(staged["path"])
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)

        hook.run("TRUNCATE staging.stg_sales;")
        # astype(object) turns numpy numbers into plain Python values psycopg2 understands
        rows = df[STG_COLUMNS].astype(object).itertuples(index=False, name=None)
        hook.insert_rows(table="staging.stg_sales", rows=rows,
                         target_fields=STG_COLUMNS, commit_every=1000)

        hook.run(LOAD_DW_SQL)
        fact_rows = hook.get_first("SELECT COUNT(*) FROM dw.fact_sales")[0]
        print(f"Loaded {len(df)} staged rows; fact_sales now has {fact_rows} rows")
        return {"batch_id": staged["batch_id"], "rows_loaded": len(df)}

    @task
    def quality_check(loaded: dict) -> None:
        """Fail the run loudly if the warehouse breaks a business rule."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        checks = {
            "fact table is not empty":
                "SELECT COUNT(*) > 0 FROM dw.fact_sales",
            "every staged row reached the fact table":
                "SELECT (SELECT COUNT(*) FROM staging.stg_sales) = "
                "(SELECT COUNT(*) FROM dw.fact_sales WHERE batch_id = %(b)s)",
            "no non-positive quantities":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales WHERE quantity <= 0",
            "line_total equals quantity x unit_price":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales "
                "WHERE line_total <> ROUND(quantity * unit_price, 2)",
            "no sale dated in the future":
                "SELECT COUNT(*) = 0 FROM dw.fact_sales "
                "WHERE date_key > TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INT",
        }
        failed = [
            name for name, sql in checks.items()
            if not hook.get_first(sql, parameters={"b": loaded["batch_id"]})[0]
        ]
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}")
        print(f"All {len(checks)} checks passed for batch {loaded['batch_id']}")

    quality_check(load(transform(extract())))


sarisari_sales_etl()
