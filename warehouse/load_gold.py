import subprocess
from pathlib import Path

import duckdb


# =========================================================
# CONFIGURATION
# =========================================================

DB_PATH = "warehouse/retail.duckdb"

GOLD_S3_PATH = "s3://retailanalyticss/gold/"
LOCAL_GOLD_PATH = Path("data/gold")


GOLD_TABLES = {
    "sales_summary": "data/gold/sales_summary/*.parquet",
    "product_performance": "data/gold/product_performance/*.parquet",
    "customer_sales": "data/gold/customer_sales/*.parquet",
    "store_performance": "data/gold/store_performance/*.parquet",
    "category_performance": "data/gold/category_performance/*.parquet",
}


# =========================================================
# 1. SYNC GOLD FROM S3
# =========================================================

print("\n" + "=" * 60)
print("STEP 1: SYNCING GOLD DATA FROM S3")
print("=" * 60)

LOCAL_GOLD_PATH.mkdir(parents=True, exist_ok=True)

subprocess.run(
    [
        "aws",
        "s3",
        "sync",
        GOLD_S3_PATH,
        str(LOCAL_GOLD_PATH),
        "--delete",
    ],
    check=True,
)

print("✓ S3 Gold sync completed.")


# =========================================================
# 2. CONNECT TO DUCKDB
# =========================================================

print("\n" + "=" * 60)
print("STEP 2: CONNECTING TO DUCKDB")
print("=" * 60)

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(DB_PATH)

print(f"✓ DuckDB connected: {DB_PATH}")


# =========================================================
# 3. LOAD GOLD TABLES
# =========================================================

print("\n" + "=" * 60)
print("STEP 3: LOADING GOLD → DUCKDB")
print("=" * 60)


for table_name, parquet_path in GOLD_TABLES.items():

    print(f"\nLoading: {table_name}")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT *
        FROM read_parquet('{parquet_path}');
        """
    )

    count = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name};
        """
    ).fetchone()[0]

    print(f"✓ {table_name}: {count:,} rows")


# =========================================================
# 4. VERIFY DUCKDB TABLES
# =========================================================

print("\n" + "=" * 60)
print("STEP 4: VERIFYING DUCKDB TABLES")
print("=" * 60)

tables = con.execute(
    """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name;
    """
).fetchall()


for table in tables:
    table_name = table[0]

    count = con.execute(
        f"SELECT COUNT(*) FROM {table_name};"
    ).fetchone()[0]

    print(f"✓ {table_name}: {count:,} rows")


# =========================================================
# 5. CLOSE CONNECTION
# =========================================================

con.close()


# =========================================================
# FINISH
# =========================================================

print("\n" + "=" * 60)
print("GOLD → DUCKDB COMPLETED SUCCESSFULLY")
print("=" * 60)
