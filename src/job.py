import time
import sys
from spark_session import get_spark_session
from ingest import ingest_bronze
from clean import (
    clean_orders, clean_reviews, clean_items, clean_customers,
    clean_products, clean_payments, clean_sellers, clean_geolocation
)
from transform import (
    build_daily_revenue, build_seller_performance, 
    build_customer_orders_ranked, build_monthly_cumulative_revenue,
    run_quality_checks
)

def run_pipeline():
    spark = get_spark_session("OlistMedallionPipeline")
    pipeline_start = time.time()
    
    try:
        print("==========================================")
        print("🚀 Starting Olist E-Commerce Pipeline")
        print("==========================================")

        # --- STAGE 1: BRONZE ---
        t0 = time.time()
        print("\n[Stage 1/4] Running Bronze Ingestion...")
        ingest_bronze(spark)
        print(f"✅ Bronze completed in {time.time() - t0:.2f}s")

        # --- STAGE 2: SILVER ---
        t0 = time.time()
        print("\n[Stage 2/4] Running Silver Transformations...")
        clean_orders(spark)
        clean_reviews(spark)
        clean_items(spark)
        clean_customers(spark)
        clean_products(spark)
        clean_payments(spark)
        clean_sellers(spark)
        clean_geolocation(spark)
        print(f"✅ Silver completed in {time.time() - t0:.2f}s")

        # --- STAGE 3: GOLD ---
        t0 = time.time()
        print("\n[Stage 3/4] Building Gold Analytical Tables...")
        build_daily_revenue(spark)
        build_seller_performance(spark)
        build_customer_orders_ranked(spark)
        build_monthly_cumulative_revenue(spark)
        print(f"✅ Gold completed in {time.time() - t0:.2f}s")

        # --- STAGE 4: QUALITY CHECKS ---
        t0 = time.time()
        print("\n[Stage 4/4] Executing Data Quality Checks...")
        run_quality_checks(spark)
        print(f"✅ Quality Checks completed in {time.time() - t0:.2f}s")

        total_time = time.time() - pipeline_start
        print("\n==========================================")
        print(f"🎉 Pipeline finished successfully in {total_time:.2f}s!")
        print("==========================================")

    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()