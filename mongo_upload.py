#!/usr/bin/env python
# coding: utf-8

"""
MongoDB Upload Script
--------------------
This script loads processed retail data from CSV and uploads it to MongoDB using Spark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

# MongoDB connection string - with added parameters
MONGO_URI = "mongodb+srv://bigdata:JyRgEeuQ3X0Uz29g@retaildb.e9pmb.mongodb.net/?retryWrites=true&w=majority"

def initialize_spark():
    """Initialize Spark session with MongoDB connector"""
    spark = SparkSession.builder \
        .appName("RetailDataMongoUpload") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .config("spark.mongodb.output.database", "RetailDB") \
        .config("spark.mongodb.output.collection", "transactions") \
        .config("spark.mongodb.output.connection.ssl", "true") \
        .config("spark.mongodb.output.connection.ssl.allowInvalidHostnames", "true") \
        .getOrCreate()
    return spark

def load_processed_data(spark):
    """Load processed data from CSV file"""
    if not os.path.exists("new_cleaned_data.csv"):
        raise FileNotFoundError("CSV data file not found. Please make sure new_cleaned_data.csv exists.")
    
    df = spark.read.csv("new_cleaned_data.csv", header=True, inferSchema=True)
    print(f"Loaded {df.count()} records from CSV data file")
    return df

def test_mongodb_connection():
    """Test MongoDB connection before proceeding"""
    print("Testing MongoDB connection...")
    try:
        client = MongoClient(MONGO_URI, 
                            serverSelectionTimeoutMS=20000,
                            connectTimeoutMS=30000,
                            socketTimeoutMS=45000)
        client.server_info()  # Will throw an exception if cannot connect
        print("MongoDB connection successful!")
        return True
    except ServerSelectionTimeoutError as e:
        print(f"Error: Could not connect to MongoDB: {e}")
        print("Please check your internet connection and MongoDB Atlas settings.")
        print("Make sure your IP address is whitelisted in MongoDB Atlas.")
        return False
    except Exception as e:
        print(f"Unexpected error connecting to MongoDB: {e}")
        return False
    finally:
        client.close()

def clear_mongodb_collection():
    """Delete existing data from MongoDB collection"""
    print("Clearing existing MongoDB collection...")
    try:
        client = MongoClient(MONGO_URI, 
                           serverSelectionTimeoutMS=30000,
                           connectTimeoutMS=40000, 
                           socketTimeoutMS=60000)
        db = client["RetailDB"]
        collection = db["transactions"]
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} records from MongoDB collection")
        client.close()
        return True
    except Exception as e:
        print(f"Error while clearing MongoDB collection: {e}")
        print("Will proceed with upload anyway using overwrite mode.")
        return False

def upload_to_mongodb(df):
    """Upload data to MongoDB using Spark MongoDB connector"""
    print("Starting upload to MongoDB...")
    
    # Write data to MongoDB
    df.write \
        .format("mongo") \
        .mode("overwrite") \
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
        
        # Test MongoDB connection
        if test_mongodb_connection():
            # Try to clear MongoDB collection
            clear_mongodb_collection()
            
            # Upload to MongoDB
            print("Uploading to MongoDB...")
            upload_to_mongodb(df)
        else:
            print("Skipping MongoDB upload due to connection issues.")
            
    finally:
        # Stop Spark session
        spark.stop()
        print("Upload process complete!")

if __name__ == "__main__":
    main()