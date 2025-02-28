#!/usr/bin/env python
# coding: utf-8

"""
MongoDB Read Script
------------------
This script reads retail data from MongoDB using Spark, performs basic analysis,
and presents the results in a more readable format both visually and in the console.
Additionally, a sample of the data is saved to CSV.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, month
from tabulate import tabulate  # pip install tabulate

def initialize_spark():
    """Initialize Spark session with MongoDB connector"""
    spark = SparkSession.builder \
        .appName("RetailDataMongoRead") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .config("spark.mongodb.input.uri", "mongodb+srv://bigdata:JyRgEeuQ3X0Uz29g@retaildb.e9pmb.mongodb.net/RetailDB.transactions") \
        .getOrCreate()
    return spark

def read_from_mongodb(spark):
    """Read data from MongoDB using Spark MongoDB connector"""
    print("Reading data from MongoDB...")
    
    # Read data from MongoDB
    df = spark.read \
        .format("mongo") \
        .option("database", "RetailDB") \
        .option("collection", "transactions") \
        .load()
    
    print(f"Successfully read {df.count()} records from MongoDB")
    return df

def analyze_data(df):
    """Perform basic analysis on the data read from MongoDB and print results nicely"""
    print("\n--- Basic Analysis of Data from MongoDB ---\n")
    
    # 1. Product Category Distribution
    prod_cat_df = df.groupBy("Product_Category") \
                    .count() \
                    .orderBy("count", ascending=False)
    prod_cat_pdf = prod_cat_df.toPandas()
    print("Product Category Distribution:")
    print(tabulate(prod_cat_pdf, headers='keys', tablefmt='psql', showindex=False))
    print("\n")
    
    # 2. Average Amount by Payment Method
    payment_df = df.groupBy("Payment_Method") \
                   .agg(round(avg("Amount"), 2).alias("Avg_Amount"),
                        count("Transaction_ID").alias("Transaction_Count")) \
                   .orderBy("Avg_Amount", ascending=False)
    payment_pdf = payment_df.toPandas()
    print("Average Amount by Payment Method:")
    print(tabulate(payment_pdf, headers='keys', tablefmt='psql', showindex=False))
    print("\n")
    
    # 3. Total Sales by Customer Segment
    segment_df = df.groupBy("Customer_Segment") \
                   .agg(round(sum("Total_Amount"), 2).alias("Total_Sales"),
                        count("Transaction_ID").alias("Transaction_Count")) \
                   .orderBy("Total_Sales", ascending=False)
    segment_pdf = segment_df.toPandas()
    print("Total Sales by Customer Segment:")
    print(tabulate(segment_pdf, headers='keys', tablefmt='psql', showindex=False))
    print("\n")
    
    # 4. Show Sample Data
    print("Sample Data from MongoDB:")
    df.select("Transaction_ID", "Customer_ID", "Date", "Amount", "Product_Category") \
      .orderBy("Date") \
      .show(5, truncate=False)

    # Optionally: You can also create visualizations here using matplotlib/seaborn
    # For example, a bar chart for product category distribution:
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        sns.set(style='whitegrid')
        prod_cat_pdf_sorted = prod_cat_pdf.sort_values("count", ascending=False)
        plt.figure(figsize=(10,6))
        sns.barplot(data=prod_cat_pdf_sorted, x="count", y="Product_Category", palette="viridis")
        plt.title("Product Category Distribution")
        plt.xlabel("Count")
        plt.ylabel("Product Category")
        plt.tight_layout()
        plt.savefig("mongo_product_category_distribution.png")
        plt.show()
        print("Bar chart saved as 'mongo_product_category_distribution.png'")
    except Exception as e:
        print(f"Visualization error: {e}")

def save_to_csv(df):
    """Save a sample of the data to CSV for verification"""
    # Eğer _id sütunu varsa, drop edelim
    if "_id" in df.columns:
        df = df.drop("_id")
    sample_df = df.limit(1000)
    sample_df.write.mode("overwrite").option("header", "true").csv("mongodb_sample_data.csv")
    print("Sample data saved to 'mongodb_sample_data.csv'")

def main():
    """Main function to run the read process"""
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # Read data from MongoDB
        df = read_from_mongodb(spark)
        
        # Analyze the data (with nicely formatted console output)
        analyze_data(df)
        
        # Save sample to CSV
        save_to_csv(df)
        
    finally:
        # Stop Spark session
        spark.stop()
        print("Read process complete!")

if __name__ == "__main__":
    main()
