from pyspark.sql import SparkSession

def get_spark_session(app_name="OlistPipeline"):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()