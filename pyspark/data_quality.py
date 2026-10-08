from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("RetailDataQuality")
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


SILVER_PATH = "s3a://retailanalyticss/silver"


def read_table(table_name):
    return spark.read.parquet(f"{SILVER_PATH}/{table_name}")


print("\n" + "=" * 60)
print("RETAIL DATA QUALITY CHECKS")
print("=" * 60)


# --------------------------------------------------
# Load Silver tables
# --------------------------------------------------

order_items = read_table("order_items")
products = read_table("products")
customers = read_table("customers")
orders = read_table("orders")
stores = read_table("stores")
categories = read_table("categories")
suppliers = read_table("suppliers")
promotions = read_table("promotions")
shipments = read_table("shipments")
payments = read_table("payments")
returns = read_table("returns")


# --------------------------------------------------
# CHECK 1: Order item quantities
# --------------------------------------------------

invalid_qty = order_items.filter(col("qty") <= 0)

print(
    "Invalid quantity rows:",
    invalid_qty.count()
)


# --------------------------------------------------
# CHECK 2: Product prices
# --------------------------------------------------

invalid_price = products.filter(col("price") <= 0)

print(
    "Invalid price rows:",
    invalid_price.count()
)


# --------------------------------------------------
# CHECK 3: Order items -> Products
# --------------------------------------------------

invalid_products = (
    order_items
    .join(
        products,
        order_items.product_id == products.product_id,
        "left_anti"
    )
)

print(
    "Order items with invalid product_id:",
    invalid_products.count()
)


# --------------------------------------------------
# CHECK 4: Orders -> Customers
# --------------------------------------------------

invalid_customers = (
    orders
    .join(
        customers,
        orders.customer_id == customers.customer_id,
        "left_anti"
    )
)

print(
    "Orders with invalid customer_id:",
    invalid_customers.count()
)


# --------------------------------------------------
# CHECK 5: Orders -> Stores
# --------------------------------------------------

invalid_stores = (
    orders
    .join(
        stores,
        orders.store_id == stores.store_id,
        "left_anti"
    )
)

print(
    "Orders with invalid store_id:",
    invalid_stores.count()
)


# --------------------------------------------------
# CHECK 6: Products -> Categories
# --------------------------------------------------

invalid_categories = (
    products
    .join(
        categories,
        products.category_id == categories.category_id,
        "left_anti"
    )
)

print(
    "Products with invalid category_id:",
    invalid_categories.count()
)


# --------------------------------------------------
# CHECK 7: Products -> Suppliers
# --------------------------------------------------

invalid_suppliers = (
    products
    .join(
        suppliers,
        products.supplier_id == suppliers.supplier_id,
        "left_anti"
    )
)

print(
    "Products with invalid supplier_id:",
    invalid_suppliers.count()
)


# --------------------------------------------------
# CHECK 8: Orders -> Promotions
# --------------------------------------------------

invalid_promotions = (
    orders
    .filter(col("promotion_id").isNotNull())
    .join(
        promotions,
        orders.promotion_id == promotions.promotion_id,
        "left_anti"
    )
)

print(
    "Orders with invalid promotion_id:",
    invalid_promotions.count()
)


# --------------------------------------------------
# CHECK 9: Shipments -> Orders
# --------------------------------------------------

invalid_shipments = (
    shipments
    .join(
        orders,
        shipments.order_id == orders.order_id,
        "left_anti"
    )
)

print(
    "Shipments with invalid order_id:",
    invalid_shipments.count()
)


# --------------------------------------------------
# CHECK 10: Payments -> Orders
# --------------------------------------------------

invalid_payments = (
    payments
    .join(
        orders,
        payments.order_id == orders.order_id,
        "left_anti"
    )
)

print(
    "Payments with invalid order_id:",
    invalid_payments.count()
)


# --------------------------------------------------
# CHECK 11: Returns -> Order Items
# --------------------------------------------------

invalid_returns = (
    returns
    .join(
        order_items,
        returns.order_item_id == order_items.order_item_id,
        "left_anti"
    )
)

print(
    "Returns with invalid order_item_id:",
    invalid_returns.count()
)


spark.stop()


print("\n" + "=" * 60)
print("DATA QUALITY CHECKS COMPLETED")
print("=" * 60)
