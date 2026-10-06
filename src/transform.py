from spark_session import get_spark_session
from pyspark.sql.functions import *

#Example of a fact table implemented in the Gold layer following the Kimball architecture for D.M

def build_daily_revenue(spark):
    # 1. Read required Silver tables
    orders_df = spark.read.parquet("data/silver/orders/")
    items_df = spark.read.parquet("data/silver/items/")
    
    # 2. Join orders and items
    df = orders_df.join(items_df, on="order_id", how="inner")
    
    # 3. Apply business logic: only count delivered orders
    # If we didn't filter this, canceled orders would falsely inflate our revenue
    df = df.filter(col("is_delivered") == True)
    
    # 4. Group by purchase date and calculate revenue
    # to_date strips the hours/minutes away so we can group by the exact day
    revenue_df = df.groupBy(to_date(col("order_purchase_timestamp")).alias("date")) \
        .agg(
            sum("price").alias("total_item_revenue"),
            sum("freight_value").alias("total_freight_revenue")
        ) \
        .orderBy("date")
    
    # 5. Write to Gold
    revenue_df.write.mode("overwrite").parquet("data/gold/daily_revenue/")
    print("Daily revenue table built and written to Gold.")
    
    
    #Example of dimensional table combined with aggregated facts
def build_seller_performance(spark):
    # 1. Read required Silver tables
    items_df = spark.read.parquet("data/silver/items/")
    orders_df = spark.read.parquet("data/silver/orders/")
    reviews_df = spark.read.parquet("data/silver/reviews/")
    
    # 2. Join tables together
    # Inner join items and orders to verify delivery status
    # Left join reviews because some orders might not have a review yet
    df = items_df.join(orders_df, on="order_id", how="inner") \
                 .join(reviews_df, on="order_id", how="left")
    
    # 3. Filter for successfully completed sales
    df = df.filter(col("is_delivered") == True)
    
    # 4. Group by seller and aggregate the facts
    seller_df = df.groupBy("seller_id").agg(
        count("order_item_id").alias("total_items_sold"),
        sum("price").alias("total_revenue"),
        round(avg("review_score"), 2).alias("avg_review_score")
    ).orderBy(col("total_revenue").desc())
    
    # 5. Write to Gold
    seller_df.write.mode("overwrite").parquet("data/gold/seller_performance/")
    print("Seller performance table built and written to Gold.")

if __name__ == "__main__":
    spark = get_spark_session("GoldTransform")
    build_daily_revenue(spark)
    build_seller_performance(spark)