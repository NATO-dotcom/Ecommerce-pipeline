from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F

# Define the exact schema that matches your producer's JSON payload
event_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("purchase_timestamp", StringType(), True)
])

def main():
    # Initialize Spark and automatically download the Kafka connector
    spark = SparkSession.builder \
        .appName("OlistOrdersConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()

    # Suppress verbose Spark INFO logs so we can see our data clearly
    spark.sparkContext.setLogLevel("WARN")
    print("Spark initialized. Connecting to Kafka...")

    # Read the raw binary stream from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "olist.orders") \
        .option("startingOffsets", "earliest") \
        .load()

    # # Parse the binary 'value' column into structured columns
    # parsed_df = raw_df \
    #     .selectExpr("CAST(value AS STRING)") \
    #     .select(F.from_json(F.col("value"), event_schema).alias("data")) \
    #     .select("data.*")
    
    # # REAL-TIME AGGREGATION: Group by state and sum the revenue
    # revenue_by_state_df = parsed_df \
    #     .groupBy('customer_state') \
    #     .agg(F.sum('total_price').alias('cumulative_revenue'))
    
    # Parse the JSON and cast the timestamp string to a true Spark Timestamp
    parsed_df = raw_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("purchase_timestamp", F.to_timestamp("purchase_timestamp"))
    
    # REAL-TIME AGGREGATION: Windowing and Watermarking
    revenue_by_state_df = parsed_df \
        .withWatermark("purchase_timestamp", "10 minutes") \
        .groupBy(
            F.window("purchase_timestamp", "1 hour"),
            "customer_state"
        ) \
        .agg(F.sum("total_price").alias("hourly_revenue"))

    # Write the stream to the console for testing
    query = revenue_by_state_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    def write_batch_to_parquet(batch_df, batch_id):
        print(f"Writing Batch {batch_id} to Parquet...")
        batch_df.coalesce(1).write \
            .mode("overwrite") \
            .parquet("data/streaming/hourly_revenue")

    # Write the stream using foreachBatch
    query = revenue_by_state_df.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_batch_to_parquet) \
        .start()

    print("Listening for messages...")
    query.awaitTermination()

if __name__ == "__main__":
    main()