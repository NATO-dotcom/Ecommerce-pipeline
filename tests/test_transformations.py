import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

# This "fixture" spins up a tiny, temporary Spark engine just for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("PytestFixture") \
        .getOrCreate()

def test_city_name_standardization(spark):
    # 1. Create messy mock data (Simulating Bronze)
    mock_data = [
        ("1", " Sao Paulo "), 
        ("2", "RIO DE JANEIRO"), 
        ("3", "belo horizonte")
    ]
    df = spark.createDataFrame(mock_data, ["customer_id", "city"])
    
    # 2. Apply your exact Silver layer logic
    cleaned_df = df.withColumn("city", lower(trim(col("city"))))
    results = cleaned_df.collect()
    
    # 3. Assert the outputs match perfectly
    assert results[0]["city"] == "sao paulo"
    assert results[1]["city"] == "rio de janeiro"
    assert results[2]["city"] == "belo horizonte"

def test_negative_price_filter(spark):
    # 1. Create mock data with a corrupt negative price
    mock_data = [("item1", 50.0), ("item2", -10.0), ("item3", 0.0)]
    df = spark.createDataFrame(mock_data, ["item_id", "price"])
    
    # 2. Apply your Silver layer filter
    filtered_df = df.filter(col("price") > 0)
    
    # 3. Assert only the valid row remains
    assert filtered_df.count() == 1
    assert filtered_df.collect()[0]["item_id"] == "item1"