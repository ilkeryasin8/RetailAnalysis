#!/usr/bin/env python
# coding: utf-8

"""
MongoDB Upload Script
--------------------
This script loads processed retail data and uploads it to MongoDB using Spark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def initialize_spark():
    """Initialize Spark session with MongoDB connector"""
    spark = SparkSession.builder \
        .appName("RetailDataMongoUpload") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .config("spark.mongodb.output.uri", "mongodb+srv://bigdata:JyRgEeuQ3X0Uz29g@retaildb.e9pmb.mongodb.net/RetailDB.transactions") \
        .getOrCreate()
    return spark

def load_processed_data(spark):
    """Load processed data from parquet file"""
    if not os.path.exists("processed_retail_data.parquet"):
        raise FileNotFoundError("Processed data file not found. Please run eda.py first.")
    
    df = spark.read.parquet("processed_retail_data.parquet")
    print(f"Loaded {df.count()} records from processed data file")
    return df

def upload_to_mongodb(df):
    """Upload data to MongoDB using Spark MongoDB connector"""
    print("Starting upload to MongoDB...")
    
    # Write data to MongoDB
    df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "RetailDB") \
        .option("collection", "transactions") \
        .save()
    
    print("Data successfully uploaded to MongoDB!")

def main():
    """Main function to run the upload process"""
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # Load processed data
        print("Loading processed data...")
        df = load_processed_data(spark)
        
        # Upload to MongoDB
        print("Uploading to MongoDB...")
        upload_to_mongodb(df)
        
    finally:
        # Stop Spark session
        spark.stop()
        print("Upload process complete!")

if __name__ == "__main__":
    main()