import duckdb

DB_PATH = "warehouse/retail.duckdb"

con = duckdb.connect(DB_PATH)

print("DuckDB warehouse created successfully!")

con.close()