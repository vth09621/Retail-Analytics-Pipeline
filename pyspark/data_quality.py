from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("RetailDataQuality")
    .getOrCreate()
)

# Read Silver order_items
df = spark.read.parquet("data/processed/order_items")

# Find invalid quantities
invalid_qty = df.filter(col("qty") <= 0)

print("Invalid quantity rows:", invalid_qty.count())


# Check invalid product prices
products = spark.read.parquet("data/processed/products")

invalid_price = products.filter(col("price") <= 0)

# Check whether every order item refers to a valid product

products = spark.read.parquet("data/processed/products")
order_items = spark.read.parquet("data/processed/order_items")

invalid_products = (
    order_items
    .join(
        products,
        order_items.product_id == products.product_id,
        "left_anti"
    )
)

print("Order items with invalid product_id:", invalid_products.count())

print("Invalid price rows:", invalid_price.count())


# Check whether every order refers to a valid customer

customers = spark.read.parquet("data/processed/customers")
orders = spark.read.parquet("data/processed/orders")

invalid_customers = (
    orders
    .join(
        customers,
        orders.customer_id == customers.customer_id,
        "left_anti"
    )
)

print("Orders with invalid customer_id:", invalid_customers.count())

# Check whether every order refers to a valid store

stores = spark.read.parquet("data/processed/stores")

invalid_stores = (
    orders
    .join(
        stores,
        orders.store_id == stores.store_id,
        "left_anti"
    )
)

print("Orders with invalid store_id:", invalid_stores.count())

# --------------------------------------------------
# CHECK 6: Products -> Categories
# --------------------------------------------------

categories = spark.read.parquet("data/processed/categories")

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

suppliers = spark.read.parquet("data/processed/suppliers")

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

promotions = spark.read.parquet("data/processed/promotions")

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

shipments = spark.read.parquet("data/processed/shipments")

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

payments = spark.read.parquet("data/processed/payments")

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

returns = spark.read.parquet("data/processed/returns")

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