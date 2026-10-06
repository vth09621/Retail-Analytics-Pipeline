from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    avg,
    round
)

spark = (
    SparkSession.builder
    .appName("RetailSilverToGold")
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

silver_path = "s3a://retailanalyticss/silver"
gold_path = "s3a://retailanalyticss/gold"


# =========================================================
# READ SILVER DATA
# =========================================================

orders = spark.read.parquet(f"{silver_path}/orders")
order_items = spark.read.parquet(f"{silver_path}/order_items")
products = spark.read.parquet(f"{silver_path}/products")
customers = spark.read.parquet(f"{silver_path}/customers")
stores = spark.read.parquet(f"{silver_path}/stores")
categories = spark.read.parquet(f"{silver_path}/categories")

print("\nSilver data loaded successfully.")


# =========================================================
# SALES DETAIL
# =========================================================

sales_detail = (
    order_items
    .join(
        orders.select(
            "order_id",
            "customer_id",
            "store_id",
            "order_date"
        ),
        on="order_id",
        how="inner"
    )
    .join(
        products.select(
            "product_id",
            "category_id"
        ),
        on="product_id",
        how="left"
    )
    .withColumn(
        "revenue",
        round(col("qty") * col("price"), 2)
    )
)


# =========================================================
# 1. SALES SUMMARY
# =========================================================

sales_summary = (
    sales_detail
    .groupBy("order_date")
    .agg(
        sum("revenue").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        sum("qty").alias("total_quantity")
    )
    .orderBy("order_date")
)

sales_summary.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/sales_summary")

print("GOLD CREATED: sales_summary")


# =========================================================
# 2. PRODUCT PERFORMANCE
# =========================================================

product_performance = (
    sales_detail
    .groupBy(
        "product_id"
    )
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("qty").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        avg("price").alias("average_price")
    )
    .join(
        products.select(
            "product_id",
            "category_id"
        ),
        on="product_id",
        how="left"
    )
)

product_performance.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/product_performance")

print("GOLD CREATED: product_performance")


# =========================================================
# 3. CUSTOMER SALES
# =========================================================

customer_sales = (
    sales_detail
    .groupBy("customer_id")
    .agg(
        sum("revenue").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        sum("qty").alias("total_quantity"),
        avg("revenue").alias("average_order_item_revenue")
    )
    .join(
        customers.select(
            "customer_id",
            "city",
            "signup_date"
        ),
        on="customer_id",
        how="left"
    )
)

customer_sales.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/customer_sales")

print("GOLD CREATED: customer_sales")


# =========================================================
# 4. STORE PERFORMANCE
# =========================================================

store_performance = (
    sales_detail
    .groupBy("store_id")
    .agg(
        sum("revenue").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        sum("qty").alias("total_quantity")
    )
    .join(
        stores.select(
            "store_id",
            "city"
        ),
        on="store_id",
        how="left"
    )
)

store_performance.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/store_performance")

print("GOLD CREATED: store_performance")


# =========================================================
# 5. CATEGORY PERFORMANCE
# =========================================================

category_performance = (
    sales_detail
    .groupBy("category_id")
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("qty").alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("product_id").alias("total_products")
    )
    .join(
        categories.select(
            "category_id",
            "category_name"
        ),
        on="category_id",
        how="left"
    )
)

category_performance.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/category_performance")

print("GOLD CREATED: category_performance")


# =========================================================
# FINISH
# =========================================================

spark.stop()

print("\n" + "=" * 60)
print("SILVER -> GOLD COMPLETED SUCCESSFULLY")
print("=" * 60)
