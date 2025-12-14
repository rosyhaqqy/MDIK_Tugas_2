#!/usr/bin/env python3
"""
Create Hive External Tables dari Delta Lake Gold Layer
Untuk query engine dan BI tools
"""
from pyspark.sql import SparkSession

def create_spark_with_hive():
    return SparkSession.builder \
        .appName("CreateHiveTables") \
        .config("spark.sql.warehouse.dir", "/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

def create_hive_tables(spark):
    """Create Hive external tables pointing to Delta Gold"""
    
    print("🗄️ Creating Hive Tables dari Delta Gold Layer...")
    
    # Drop existing database if exists
    spark.sql("DROP DATABASE IF EXISTS retail_analytics CASCADE")
    spark.sql("CREATE DATABASE retail_analytics")
    spark.sql("USE retail_analytics")
    
    # Table 1: Product Complaints
    print("  Creating table: product_complaints")
    spark.sql("""
        CREATE EXTERNAL TABLE product_complaints (
            product_category STRING,
            complaint_category STRING,
            total_complaints BIGINT,
            unique_customers BIGINT
        )
        USING DELTA
        LOCATION '/lakehouse/gold/product_complaints'
    """)
    
    # Table 2: Regional Analysis
    print("  Creating table: regional_analysis")
    spark.sql("""
        CREATE EXTERNAL TABLE regional_analysis (
            region STRING,
            category STRING,
            total_sales DOUBLE,
            total_profit DOUBLE,
            total_orders BIGINT,
            avg_profit_margin DOUBLE,
            complaint_category STRING,
            total_complaints BIGINT,
            unique_complainers BIGINT
        )
        USING DELTA
        LOCATION '/lakehouse/gold/regional_analysis'
    """)
    
    # Table 3: Sales vs Complaints (Main Analytics)
    print("  Creating table: sales_vs_complaints")
    spark.sql("""
        CREATE EXTERNAL TABLE sales_vs_complaints (
            category STRING,
            total_sales DOUBLE,
            total_profit DOUBLE,
            order_count BIGINT,
            avg_discount DOUBLE,
            unique_customers BIGINT,
            total_complaints BIGINT,
            complaint_customers BIGINT,
            negative_complaints BIGINT,
            defect_complaints BIGINT,
            complaint_rate DOUBLE,
            profit_per_order DOUBLE
        )
        USING DELTA
        LOCATION '/lakehouse/gold/sales_vs_complaints'
    """)
    
    # Table 4: Tweets Detail
    print("  Creating table: tweets_detail")
    spark.sql("""
        CREATE EXTERNAL TABLE tweets_detail (
            tweet_id STRING,
            text STRING,
            complaint_category STRING,
            product_category STRING,
            sentiment STRING,
            created_at STRING
        )
        USING DELTA
        LOCATION '/lakehouse/gold/tweets_detail'
    """)
    
    print("\n✅ Hive Tables Created Successfully!")
    
    # Show tables
    print("\n📋 Tables in retail_analytics database:")
    spark.sql("SHOW TABLES").show()
    
    # Sample query
    print("\n📊 Sample Data from sales_vs_complaints:")
    spark.sql("""
        SELECT category, 
               ROUND(total_sales, 2) as sales,
               total_complaints,
               ROUND(complaint_rate, 2) as complaint_pct
        FROM sales_vs_complaints
        ORDER BY total_sales DESC
    """).show()

if __name__ == "__main__":
    spark = create_spark_with_hive()
    
    try:
        create_hive_tables(spark)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        spark.stop()
