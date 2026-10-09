from spark_session import get_spark_session
from schemas import *

def ingest_bronze(spark=None):
    if spark is None:
        spark = get_spark_session("BronzeIngestion")
    
    # Orders
    spark.read.csv("data/raw/olist_orders_dataset.csv", header=True, schema=orders_schema) \
        .write.mode("overwrite").parquet("data/bronze/orders/")
        
    # Items
    spark.read.csv("data/raw/olist_order_items_dataset.csv", header=True, schema=items_schema) \
        .write.mode("overwrite").parquet("data/bronze/items/")
        
    # Payments
    spark.read.csv("data/raw/olist_order_payments_dataset.csv", header=True, schema=payments_schema) \
        .write.mode("overwrite").parquet("data/bronze/payments/")
        
    # Reviews (Includes multiLine fix)
    spark.read.csv("data/raw/olist_order_reviews_dataset.csv", header=True, schema=reviews_schema, multiLine=True, escape='"') \
        .write.mode("overwrite").parquet("data/bronze/reviews/")
        
    # Customers
    spark.read.csv("data/raw/olist_customers_dataset.csv", header=True, schema=customers_schema) \
        .write.mode("overwrite").parquet("data/bronze/customers/")
        
    # Sellers
    spark.read.csv("data/raw/olist_sellers_dataset.csv", header=True, schema=sellers_schema) \
        .write.mode("overwrite").parquet("data/bronze/sellers/")
        
    # Products
    spark.read.csv("data/raw/olist_products_dataset.csv", header=True, schema=products_schema) \
        .write.mode("overwrite").parquet("data/bronze/products/")
        
    # Geolocation
    spark.read.csv("data/raw/olist_geolocation_dataset.csv", header=True, schema=geolocation_schema) \
        .write.mode("overwrite").parquet("data/bronze/geolocation/")
        
    # Translation 
    spark.read.csv("data/raw/product_category_name_translation.csv", header=True, schema=translation_schema) \
        .write.mode("overwrite").parquet("data/bronze/translation/")

    print("Bronze ingestion complete!")

if __name__ == "__main__":
    ingest_bronze()