from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date

spark = (
    SparkSession.builder
    .appName("RetailBronzeToSilver")
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

bronze_path = "s3a://retailanalyticss/bronze"
silver_path = "s3a://retailanalyticss/silver"


def clean_dataframe(df):
    """
    Basic cleaning applied to all datasets:
    - Remove completely empty rows
    - Remove duplicate rows
    - Trim whitespace from string columns
    """
    df = df.dropna(how="all")
    df = df.dropDuplicates()

    for column_name, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(
                column_name,
                trim(col(column_name))
            )

    return df


def process_table(table_name):
    input_path = f"{bronze_path}/{table_name}"
    output_path = f"{silver_path}/{table_name}"

    print("\n" + "=" * 60)
    print(f"PROCESSING: {table_name}")

    df = spark.read.parquet(input_path)

    print(f"BRONZE ROWS: {df.count()}")

    df = clean_dataframe(df)

    # Date columns
    if table_name == "customers":
        df = df.withColumn(
            "signup_date",
            to_date(col("signup_date"))
        )

    elif table_name == "orders":
        df = df.withColumn(
            "order_date",
            to_date(col("order_date"))
        )

    # Numeric columns
    if table_name == "products":
        df = df.withColumn("price", col("price").cast("double"))

    elif table_name == "payments":
        df = df.withColumn("amount", col("amount").cast("double"))

    elif table_name == "order_items":
        df = (
            df.withColumn("qty", col("qty").cast("integer"))
              .withColumn("price", col("price").cast("double"))
        )

    elif table_name == "employees":
        df = df.withColumn("salary", col("salary").cast("double"))

    elif table_name == "returns":
        df = df.withColumn("refund", col("refund").cast("double"))

    elif table_name == "promotions":
        df = df.withColumn("discount", col("discount").cast("double"))

    # Remove rows where important IDs are missing
    id_columns = {
        "customers": ["customer_id"],
        "categories": ["category_id"],
        "products": ["product_id"],
        "orders": ["order_id", "customer_id"],
        "shipments": ["shipment_id", "order_id"],
        "promotions": ["promotion_id"],
        "suppliers": ["supplier_id"],
        "payments": ["payment_id", "order_id"],
        "order_items": ["order_item_id", "order_id", "product_id"],
        "employees": ["employee_id", "store_id"],
        "stores": ["store_id"],
        "returns": ["return_id", "order_item_id"],
    }

    if table_name in id_columns:
        df = df.dropna(subset=id_columns[table_name])

    silver_count = df.count()

    print(f"SILVER ROWS: {silver_count}")
    print(f"COLUMNS: {len(df.columns)}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"SILVER WRITTEN: {output_path}")


tables = [
    "customers",
    "categories",
    "products",
    "orders",
    "shipments",
    "promotions",
    "suppliers",
    "payments",
    "order_items",
    "employees",
    "stores",
    "returns"
]


for table in tables:
    process_table(table)


spark.stop()

print("\n" + "=" * 60)
print("BRONZE -> SILVER COMPLETED SUCCESSFULLY")
print("=" * 60)
