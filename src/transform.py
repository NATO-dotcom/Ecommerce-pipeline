from spark_session import get_spark_session
import sys
from pyspark.sql.functions import *
from pyspark.sql.window import *

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
    
    
    

def build_customer_orders_ranked(spark):
    orders_df = spark.read.parquet("data/silver/orders/")
    customers_df = spark.read.parquet("data/silver/customers/")
    items_df = spark.read.parquet("data/silver/items/")

    # 1. Calculate total spend per order first
    order_spend_df = items_df.groupBy("order_id").agg(
        round(sum("price"), 2).alias("order_total_price")
    )

    # 2. Join orders with customer_unique_id (tracks actual repeat humans) and spend
    df = orders_df.join(customers_df, on="customer_id", how="inner") \
                  .join(order_spend_df, on="order_id", how="inner") \
                  .filter(col("is_delivered") == True)

    # 3. Define the Window: Group by unique customer, order chronologically
    customer_window = Window.partitionBy("customer_unique_id").orderBy(col("order_purchase_timestamp").asc())

    # 4. Apply row_number() over the window to rank their 1st, 2nd, 3rd order
    ranked_df = df.withColumn("order_sequence_rank", row_number().over(customer_window)) \
                    .select(
                        "customer_unique_id",
                        "order_id",
                        "order_purchase_timestamp",
                        "order_total_price",
                        "order_sequence_rank"
                    )

    ranked_df.write.mode("overwrite").parquet("data/gold/customer_orders_ranked/")
    print("Customer orders ranked table (Window) built and written to Gold.")


def build_monthly_cumulative_revenue(spark):
    daily_rev_df = spark.read.parquet("data/gold/daily_revenue/")

    # 1. Aggregate daily revenue into monthly buckets (YYYY-MM)
    monthly_df = daily_rev_df.withColumn("year_month", date_format(col("date"), "yyyy-MM")) \
        .groupBy("year_month") \
        .agg(round(sum("total_item_revenue"), 2).alias("monthly_revenue"))

    # 2. Define an cumulative Window ordered by month from the beginning of time to current row
    cumulative_window = Window.orderBy("year_month") \
                              .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # 3. Calculate running total
    cum_rev_df = monthly_df.withColumn(
        "cumulative_revenue",
        round(sum("monthly_revenue").over(cumulative_window), 2)
    ).orderBy("year_month")

    cum_rev_df.write.mode("overwrite").parquet("data/gold/monthly_cumulative_revenue/")
    print("Monthly cumulative revenue table (Window) built and written to Gold.")
    
    

def run_quality_checks(spark):
    print("\n--- Running Data Quality Checks ---")
    
    daily_rev_df = spark.read.parquet("data/gold/daily_revenue/")
    seller_perf_df = spark.read.parquet("data/gold/seller_performance/")
    
    # Check 1: Row counts > 0
    if daily_rev_df.count() == 0 or seller_perf_df.count() == 0:
        print("FAIL: One of the Gold tables is empty!")
        sys.exit(1)
        
    # Check 2: No negative revenue
    neg_rev_count = daily_rev_df.filter(col("total_item_revenue") < 0).count()
    if neg_rev_count > 0:
        print(f"FAIL: Found {neg_rev_count} days with negative revenue!")
        sys.exit(1)
        
    # Check 3: No null seller_ids in seller_performance
    null_sellers = seller_perf_df.filter(col("seller_id").isNull()).count()
    if null_sellers > 0:
        print(f"FAIL: Found {null_sellers} null seller_ids!")
        sys.exit(1)

    # Check 4: Gold total revenue equals Silver item prices for delivered orders
    orders_silver = spark.read.parquet("data/silver/orders/").filter(col("is_delivered") == True)
    items_silver = spark.read.parquet("data/silver/items/")
    
    silver_total = orders_silver.join(items_silver, on="order_id", how="inner") \
        .agg(round(sum("price"), 2).alias("total")).collect()[0]["total"]
        
    gold_total = daily_rev_df.agg(round(sum("total_item_revenue"), 2).alias("total")).collect()[0]["total"]
    
    if silver_total != gold_total:
        print(f"FAIL: Revenue mismatch! Silver: {silver_total} vs Gold: {gold_total}")
        sys.exit(1)
        
    print(f"PASS: All quality checks passed! Verified Total Revenue: ${gold_total:,.2f}")

if __name__ == "__main__":
    spark = get_spark_session("GoldTransform")
    build_daily_revenue(spark)
    build_seller_performance(spark)
    build_customer_orders_ranked(spark)
    build_monthly_cumulative_revenue(spark)
    run_quality_checks(spark)