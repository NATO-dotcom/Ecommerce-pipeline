from spark_session import get_spark_session
from pyspark.sql.functions import *

def clean_orders(spark):
    # 1. Read from Bronze
    df = spark.read.parquet("data/bronze/orders/")
    
    # 2. Cast timestamp columns
    timestamp_cols = [
        "order_purchase_timestamp", 
        "order_approved_at", 
        "order_delivered_carrier_date", 
        "order_delivered_customer_date", 
        "order_estimated_delivery_date"
    ]
    for c in timestamp_cols:
        df = df.withColumn(c, to_timestamp(col(c)))
        
    # 3. Add is_delivered flag[cite: 3]
    df = df.withColumn(
        "is_delivered", 
        when(col("order_status") == "delivered", True).otherwise(False)
    )
    
    # 4. Write to Silver[cite: 3]
    df.write.mode("overwrite").parquet("data/silver/orders/")
    print("Orders cleaned and written to Silver.")

def clean_reviews(spark):
    df = spark.read.parquet("data/bronze/reviews/")
    
    # 1. Drop duplicate reviews[cite: 3]
    df = df.dropDuplicates(["review_id"])
    
    # 2. Cast timestamps
    df = df.withColumn("review_creation_date", to_timestamp(col("review_creation_date")))
    df = df.withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))
    
    df.write.mode("overwrite").parquet("data/silver/reviews/")
    print("Reviews cleaned and written to Silver.")
    
def clean_items(spark):
    df = spark.read.parquet("data/bronze/items/")
    
    # 1. Remove items with price <= 0
    df = df.filter(col("price") > 0)
    
    # 2. Cast timestamp
    df = df.withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
    
    df.write.mode("overwrite").parquet("data/silver/items/")
    print("Items cleaned and written to Silver.")

def clean_customers(spark):
    df = spark.read.parquet("data/bronze/customers/")
    
    # 1. Drop duplicate customers
    df = df.dropDuplicates(["customer_id"])
    
    # 2. Trim and standardize text columns (city names lowercase)
    df = df.withColumn("customer_city", lower(trim(col("customer_city"))))
    
    df.write.mode("overwrite").parquet("data/silver/customers/")
    print("Customers cleaned and written to Silver.")

def clean_products(spark):
    products_df = spark.read.parquet("data/bronze/products/")
    translation_df = spark.read.parquet("data/bronze/translation/")
    
    # 1. Join translation table to add English category name
    # A 'left' join ensures we keep products even if they don't have a match in the translation table.
    df = products_df.join(translation_df, on="product_category_name", how="left")
    
    # 2. Fill missing categories with "unknown"
    df = df.fillna({"product_category_name_english": "unknown"})
    
    df.write.mode("overwrite").parquet("data/silver/products/")
    print("Products cleaned and written to Silver.")
    
def clean_payments(spark):
    df = spark.read.parquet("data/bronze/payments/")
    df.write.mode("overwrite").parquet("data/silver/payments/")
    print("Payments written to Silver.")

def clean_sellers(spark):
    df = spark.read.parquet("data/bronze/sellers/")
    # Standardize city names
    df = df.withColumn("seller_city", lower(trim(col("seller_city"))))
    df.write.mode("overwrite").parquet("data/silver/sellers/")
    print("Sellers cleaned and written to Silver.")

def clean_geolocation(spark):
    df = spark.read.parquet("data/bronze/geolocation/")
    # Standardize city names
    df = df.withColumn("geolocation_city", lower(trim(col("geolocation_city"))))
    df.write.mode("overwrite").parquet("data/silver/geolocation/")
    print("Geolocation cleaned and written to Silver.")

if __name__ == "__main__":
    spark = get_spark_session("SilverCleaning")
    clean_orders(spark)
    clean_reviews(spark)
    clean_items(spark)
    clean_customers(spark)
    clean_products(spark)
    clean_payments(spark)
    clean_sellers(spark)
    clean_geolocation(spark)