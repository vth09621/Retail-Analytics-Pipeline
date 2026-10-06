
from pyspark.sql import SparkSession

# 1. Create Spark session
spark = (
    SparkSession.builder
    .appName("RetailDataIngestion")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0"
    )
    .config(
        "fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
    )
    .config(
        "fs.s3a.endpoint.region",
        "ap-south-1"
    )
    .getOrCreate()
)

# 2. S3 paths
raw_path = "s3a://retailanalyticss/raw"
bronze_path = "s3a://retailanalyticss/bronze"

# 3. Files to process
files = [
    "customers.csv",
    "categories.csv",
    "products.csv",
    "orders.csv",
    "shipments.csv",
    "promotions.csv",
    "suppliers.csv",
    "payments.csv",
    "order_items.csv",
    "employees.csv",
    "stores.csv",
    "returns.csv"
]

# 4. Raw → Bronze
for file_name in files:

    input_path = f"{raw_path}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    table_name = file_name.replace(".csv", "")
    output_path = f"{bronze_path}/{table_name}"

    print("\n" + "=" * 60)
    print(f"PROCESSING: {file_name}")
    print(f"ROWS: {df.count()}")
    print(f"COLUMNS: {len(df.columns)}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"BRONZE WRITTEN: {output_path}")

spark.stop()

print("\n" + "=" * 60)
print("RAW → BRONZE COMPLETED SUCCESSFULLY")
print("=" * 60)
