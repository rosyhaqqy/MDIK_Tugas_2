#!/usr/bin/env python3
"""
Delta Lake Silver & Gold Layer Processing
- Silver: Data cleaning & NLP processing
- Gold: Business analytics & aggregations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import re

def create_spark_session():
    builder = SparkSession.builder \
        .appName("SilverGoldProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

# ========== UDF untuk NLP Processing ==========

@udf(StringType())
def categorize_complaint(text):
    """Kategorisasi keluhan berdasarkan keyword"""
    if not text:
        return "Unknown"
    
    text_lower = text.lower()
    
    # Shipping/Delivery Issues
    shipping_keywords = ['delivery', 'ship', 'late', 'delayed', 'arrive', 'tracking', 'package', 'courier']
    if any(kw in text_lower for kw in shipping_keywords):
        return "Shipping Issue"
    
    # Product Quality/Defect
    quality_keywords = ['broken', 'damage', 'defect', 'quality', 'wrong', 'missing', 'faulty']
    if any(kw in text_lower for kw in quality_keywords):
        return "Product Defect"
    
    # Customer Service
    service_keywords = ['service', 'support', 'help', 'response', 'rude', 'staff', 'contact']
    if any(kw in text_lower for kw in service_keywords):
        return "Customer Service"
    
    # Refund/Payment
    payment_keywords = ['refund', 'money', 'charge', 'payment', 'bill', 'price', 'expensive']
    if any(kw in text_lower for kw in payment_keywords):
        return "Payment Issue"
    
    return "General Inquiry"

@udf(StringType())
def extract_product_category(text):
    """Extract kategori produk dari text tweet"""
    if not text:
        return "Unknown"
    
    text_lower = text.lower()
    
    # Furniture keywords
    furniture_kw = ['chair', 'table', 'desk', 'furniture', 'cabinet', 'shelf', 'bookcase']
    if any(kw in text_lower for kw in furniture_kw):
        return "Furniture"
    
    # Technology keywords
    tech_kw = ['phone', 'computer', 'laptop', 'tablet', 'printer', 'machine', 'technology', 'tech']
    if any(kw in text_lower for kw in tech_kw):
        return "Technology"
    
    # Office Supplies keywords
    office_kw = ['paper', 'pen', 'binder', 'envelope', 'supplies', 'office']
    if any(kw in text_lower for kw in office_kw):
        return "Office Supplies"
    
    return "Unknown"

@udf(StringType())
def sentiment_analysis_simple(text):
    """Simple sentiment analysis berdasarkan keyword"""
    if not text:
        return "Neutral"
    
    text_lower = text.lower()
    
    negative_words = ['bad', 'terrible', 'worst', 'awful', 'poor', 'horrible', 'disappointing', 
                      'broken', 'damage', 'late', 'never', 'hate', 'angry', 'frustrated']
    positive_words = ['good', 'great', 'excellent', 'love', 'best', 'perfect', 'amazing', 'thank']
    
    # Ganti sum() dengan len() + list comprehension
    neg_count = len([1 for word in negative_words if word in text_lower])
    pos_count = len([1 for word in positive_words if word in text_lower])
    
    if neg_count > pos_count:
        return "Negative"
    elif pos_count > neg_count:
        return "Positive"
    else:
        return "Neutral"

# ========== SILVER LAYER PROCESSING ==========

def process_sales_silver(spark):
    """Transform Sales Bronze -> Silver"""
    print("🔄 Processing Sales Silver Layer...")
    
    # Read dari Bronze
    df_bronze = spark.read.format("delta").load("/lakehouse/bronze/sales")
    
    # Data Cleaning & Transformation
    df_silver = df_bronze \
        .filter(col("sales").isNotNull()) \
        .filter(col("sales") > 0) \
        .withColumn("order_date", to_date(col("order_date"))) \
        .withColumn("ship_date", to_date(col("ship_date"))) \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_quarter", quarter(col("order_date"))) \
        .withColumn("profit_margin", 
                    when(col("sales") > 0, col("profit") / col("sales") * 100).otherwise(0)) \
        .withColumn("is_profitable", when(col("profit") > 0, lit(True)).otherwise(lit(False))) \
        .withColumn("silver_processed_date", current_timestamp()) \
        .dropDuplicates(["order_id", "product_id"])
    
    # Write to Silver
    silver_path = "/lakehouse/silver/sales"
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .save(silver_path)
    
    print(f"✅ Sales Silver written to: {silver_path}")
    return df_silver

def process_tweets_silver(spark):
    """Transform Tweets Bronze -> Silver dengan NLP"""
    print("🔄 Processing Tweets Silver Layer dengan NLP...")
    
    df_bronze = spark.read.format("delta").load("/lakehouse/bronze/tweets")
    
    # NLP Processing
    df_silver = df_bronze \
        .filter(col("text").isNotNull()) \
        .filter(length(col("text")) > 10) \
        .withColumn("complaint_category", categorize_complaint(col("text"))) \
        .withColumn("product_category", extract_product_category(col("text"))) \
        .withColumn("sentiment", sentiment_analysis_simple(col("text"))) \
        .withColumn("text_length", length(col("text"))) \
        .withColumn("word_count", size(split(col("text"), " "))) \
        .withColumn("created_date", to_date(col("created_at"))) \
        .withColumn("silver_processed_date", current_timestamp()) \
        .dropDuplicates(["tweet_id"])
    
    # Write to Silver
    silver_path = "/lakehouse/silver/tweets"
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)
    
    print(f"✅ Tweets Silver written to: {silver_path}")
    return df_silver

# ========== GOLD LAYER PROCESSING ==========

def create_gold_analytics(spark, df_sales, df_tweets):
    """Create Gold Layer: Business Analytics"""
    print("🏆 Creating Gold Layer Analytics...")
    
    # === ANALITIK 1: Menghubungkan Kategori Produk dengan Keluhan ===
    print("  📊 Analytics 1: Product Category vs Complaint Type")
    
    gold_1 = df_tweets \
        .filter(col("product_category") != "Unknown") \
        .groupBy("product_category", "complaint_category") \
        .agg(
            count("*").alias("total_complaints"),
            countDistinct("author_id").alias("unique_customers")
        ) \
        .orderBy("product_category", col("total_complaints").desc())
    
    gold_1.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/lakehouse/gold/product_complaints")
    
    print(f"    ✓ Saved: /lakehouse/gold/product_complaints")
    
    # === ANALITIK 2: Tren Permasalahan per Region ===
    print("  📊 Analytics 2: Complaint Trends by Region & Category")
    
    # Agregasi sales per region
    sales_by_region = df_sales \
        .groupBy("region", "category") \
        .agg(
            sum("sales").alias("total_sales"),
            sum("profit").alias("total_profit"),
            count("*").alias("total_orders"),
            avg("profit_margin").alias("avg_profit_margin")
        )
    
    # Agregasi complaints (simulasi region dari text analysis)
    # Untuk demo, kita assign random region atau bisa enhance NLP
    tweets_enriched = df_tweets \
        .withColumn("estimated_region",
                    when(col("product_category") == "Furniture", lit("West"))
                    .when(col("product_category") == "Technology", lit("Central"))
                    .otherwise(lit("East")))
    
    complaints_by_region = tweets_enriched \
        .groupBy("estimated_region", "product_category", "complaint_category") \
        .agg(
            count("*").alias("total_complaints"),
            countDistinct("author_id").alias("unique_complainers")
        )
    
    gold_2 = sales_by_region.join(
        complaints_by_region,
        (sales_by_region.region == complaints_by_region.estimated_region) &
        (sales_by_region.category == complaints_by_region.product_category),
        "left"
    ).select(
        sales_by_region["region"],
        sales_by_region["category"],
        "total_sales",
        "total_profit",
        "total_orders",
        "avg_profit_margin",
        coalesce("complaint_category", lit("No Complaints")).alias("complaint_category"),
        coalesce("total_complaints", lit(0)).alias("total_complaints"),
        coalesce("unique_complainers", lit(0)).alias("unique_complainers")
    )
    
    gold_2.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/lakehouse/gold/regional_analysis")
    
    print(f"    ✓ Saved: /lakehouse/gold/regional_analysis")
    
    # === ANALITIK 3: Integrasi Sales Performance vs Complaints ===
    print("  📊 Analytics 3: Sales Performance vs Complaint Rate")
    
    # Agregasi sales per kategori produk
    sales_summary = df_sales \
        .groupBy("category") \
        .agg(
            sum("sales").alias("total_sales"),
            sum("profit").alias("total_profit"),
            count("*").alias("order_count"),
            avg("discount").alias("avg_discount"),
            countDistinct("customer_id").alias("unique_customers")
        )
    
    # Agregasi complaints per kategori
    complaints_summary = df_tweets \
        .filter(col("product_category") != "Unknown") \
        .groupBy("product_category") \
        .agg(
            count("*").alias("total_complaints"),
            countDistinct("author_id").alias("complaint_customers"),
            sum(when(col("sentiment") == "Negative", 1).otherwise(0)).alias("negative_complaints"),
            sum(when(col("complaint_category") == "Product Defect", 1).otherwise(0)).alias("defect_complaints")
        )
    
    # Join structured + unstructured
    gold_3 = sales_summary.join(
        complaints_summary,
        sales_summary.category == complaints_summary.product_category,
        "left"
    ).select(
        sales_summary["category"],
        "total_sales",
        "total_profit",
        "order_count",
        "avg_discount",
        "unique_customers",
        coalesce("total_complaints", lit(0)).alias("total_complaints"),
        coalesce("complaint_customers", lit(0)).alias("complaint_customers"),
        coalesce("negative_complaints", lit(0)).alias("negative_complaints"),
        coalesce("defect_complaints", lit(0)).alias("defect_complaints")
    ).withColumn("complaint_rate", 
                 when(col("order_count") > 0, 
                      col("total_complaints") / col("order_count") * 100)
                 .otherwise(0)) \
     .withColumn("profit_per_order", col("total_profit") / col("order_count"))
    
    gold_3.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/lakehouse/gold/sales_vs_complaints")
    
    print(f"    ✓ Saved: /lakehouse/gold/sales_vs_complaints")
    
    # === ANALITIK 4: Detail Tweets untuk Drill-Down ===
    df_tweets \
        .select("tweet_id", "text", "complaint_category", "product_category", 
                "sentiment", "created_at") \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .save("/lakehouse/gold/tweets_detail")
    
    print(f"    ✓ Saved: /lakehouse/gold/tweets_detail")
    
    print("✅ Gold Layer Analytics Completed!")

# ========== MAIN EXECUTION ==========

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("\n" + "="*60)
        print("🚀 Starting Silver & Gold Layer Processing")
        print("="*60 + "\n")
        
        # Process Silver Layer
        df_sales_silver = process_sales_silver(spark)
        df_tweets_silver = process_tweets_silver(spark)
        
        # Process Gold Layer
        create_gold_analytics(spark, df_sales_silver, df_tweets_silver)
        
        print("\n" + "="*60)
        print("🎉 Silver & Gold Processing Completed Successfully!")
        print("="*60)
        
        # Show sample results
        print("\n📋 Sample Gold Analytics:")
        spark.read.format("delta").load("/lakehouse/gold/sales_vs_complaints").show(10, truncate=False)
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
