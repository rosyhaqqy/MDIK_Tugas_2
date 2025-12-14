#!/usr/bin/env python3
"""
Kafka Consumer -> Delta Lake (Bronze Layer)
Membaca streaming data dari Kafka dan menyimpan ke Delta Lake format
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# Konfigurasi Spark dengan Delta Lake
def create_spark_session():
    builder = SparkSession.builder \
        .appName("KafkaToDeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def consume_sales_to_bronze(spark):
    """Consume sales data dari Kafka ke Bronze Delta Lake"""
    
    print("📥 Consuming Superstore Sales from Kafka...")
    
    # Schema untuk sales data
    sales_schema = StructType([
        StructField("row_id", IntegerType()),
        StructField("order_id", StringType()),
        StructField("order_date", StringType()),
        StructField("ship_date", StringType()),
        StructField("ship_mode", StringType()),
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("segment", StringType()),
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("postal_code", StringType()),
        StructField("region", StringType()),
        StructField("product_id", StringType()),
        StructField("category", StringType()),
        StructField("sub_category", StringType()),
        StructField("product_name", StringType()),
        StructField("sales", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("discount", DoubleType()),
        StructField("profit", DoubleType()),
        StructField("ingestion_timestamp", StringType())
    ])
    
    # Read dari Kafka (batch mode untuk demo)
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "superstore-sales") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON value
    df_sales = df_kafka.select(
        from_json(col("value").cast("string"), sales_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Add metadata columns
    df_sales_bronze = df_sales \
        .withColumn("bronze_load_date", current_timestamp()) \
        .withColumn("data_source", lit("kafka_superstore"))
    
    # Write to Delta Lake Bronze
    bronze_path = "/lakehouse/bronze/sales"
    df_sales_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(bronze_path)
    
    print(f"✅ Sales data written to Delta Lake Bronze: {bronze_path}")
    print(f"   Total records: {df_sales_bronze.count()}")
    
    return bronze_path

def consume_tweets_to_bronze(spark):
    """Consume tweets data dari Kafka ke Bronze Delta Lake"""
    
    print("📥 Consuming Customer Tweets from Kafka...")
    
    tweets_schema = StructType([
        StructField("tweet_id", StringType()),
        StructField("author_id", StringType()),
        StructField("text", StringType()),
        StructField("response_tweet_id", StringType()),
        StructField("in_response_to_tweet_id", StringType()),
        StructField("created_at", StringType()),
        StructField("ingestion_timestamp", StringType())
    ])
    
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "customer-tweets") \
        .option("startingOffsets", "earliest") \
        .load()
    
    df_tweets = df_kafka.select(
        from_json(col("value").cast("string"), tweets_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    df_tweets_bronze = df_tweets \
        .withColumn("bronze_load_date", current_timestamp()) \
        .withColumn("data_source", lit("kafka_tweets"))
    
    bronze_path = "/lakehouse/bronze/tweets"
    df_tweets_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(bronze_path)
    
    print(f"✅ Tweets data written to Delta Lake Bronze: {bronze_path}")
    print(f"   Total records: {df_tweets_bronze.count()}")
    
    return bronze_path

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # Consume both topics
        sales_path = consume_sales_to_bronze(spark)
        tweets_path = consume_tweets_to_bronze(spark)
        
        print("\n" + "="*60)
        print("🎉 Bronze Layer Creation Completed!")
        print(f"Sales: {sales_path}")
        print(f"Tweets: {tweets_path}")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()
